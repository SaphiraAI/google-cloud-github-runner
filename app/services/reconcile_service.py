"""
Reconciler: closes the gap between GitHub's queued jobs and the manager's runner VMs.

The webhook path is best effort: a dropped delivery leaves a queued job with no VM, a runner
that never registers leaves a VM with no job, and a job cancelled while queued leaves both
GitHub and Compute Engine waiting for an event that never comes. Run periodically (Cloud
Scheduler -> POST /reconcile) this service:

* creates a VM for every queued job on a ``gcp-`` label that has waited longer than
  ``RECONCILE_STUCK_MINUTES`` with no VM for its job id (same path as the webhook);
* deletes every runner VM older than ``RECONCILE_STUCK_MINUTES`` whose job is not running,
  deregistering its runner from GitHub first so it cannot pick up a job mid-delete;
* never deletes a VM that GitHub reports as running a job (by runner name or busy flag).

Every decision is logged with the job id, VM name, zone and reason, and returned in the report.
"""
import concurrent.futures
import datetime
import logging
import os
import uuid

from app.clients import GitHubClient, GCloudClient
from app.clients.gcloud_client import JOB_ID_LABEL, LIVE_INSTANCE_STATUSES
from app.services.webhook_service import WebhookService

logger = logging.getLogger(__name__)

DEFAULT_STUCK_MINUTES = 10
DEFAULT_MAX_CREATES = 20
DEFAULT_CREATE_WORKERS = 4
# Suffix for VMs the reconciler creates, so a late webhook for the same job never races on
# the same instance name in a different zone; both VMs carry the same gha-job-id label.
RECONCILE_NAME_SUFFIX = '-r'


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r, using default %s", name, os.environ.get(name), default)
        return default


def parse_github_time(value):
    """Parse a GitHub API timestamp (RFC 3339, ``Z`` suffix) into an aware datetime."""
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return None


def template_label_for(labels):
    """Return the job label the manager maps to a template, using the webhook's rule."""
    for label in labels or []:
        if isinstance(label, str) and (label.startswith('gcp-') or label.lower() == 'dependabot'):
            return label
    return None


class ReconcileService:
    """One reconciliation pass over every repository the GitHub App installation can see."""

    def __init__(
        self,
        github_client=None,
        gcloud_client=None,
        webhook_service=None,
        stuck_minutes=None,
        max_creates=None,
        create_workers=None,
        now=None,
    ):
        self.github_client = github_client or GitHubClient()
        self.gcloud_client = gcloud_client or GCloudClient()
        self.webhook_service = webhook_service or WebhookService(
            github_client=self.github_client, gcloud_client=self.gcloud_client
        )
        self.stuck_minutes = stuck_minutes if stuck_minutes is not None \
            else _env_int('RECONCILE_STUCK_MINUTES', DEFAULT_STUCK_MINUTES)
        self.max_creates = max_creates if max_creates is not None \
            else _env_int('RECONCILE_MAX_CREATES', DEFAULT_MAX_CREATES)
        self.create_workers = create_workers if create_workers is not None \
            else _env_int('RECONCILE_CREATE_WORKERS', DEFAULT_CREATE_WORKERS)
        self._now = now
        self.run_id = f"reconcile-{uuid.uuid4().hex[:8]}"

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def now(self):
        return self._now or datetime.datetime.now(datetime.timezone.utc)

    def _is_stuck(self, timestamp):
        """True when ``timestamp`` is at least ``stuck_minutes`` in the past (unknown = not stuck)."""
        if timestamp is None:
            return False
        return self.now() - timestamp >= datetime.timedelta(minutes=self.stuck_minutes)

    @staticmethod
    def _scope_for_repo(repo):
        """Runner registration scope: ('org', login) for organization repos, else ('repo', full_name)."""
        owner = repo.get('owner') or {}
        if (owner.get('type') or '').lower() == 'organization':
            return ('org', owner.get('login'))
        return ('repo', repo.get('full_name'))

    def _list_runners(self, scope, token):
        kind, name = scope
        if kind == 'org':
            return self.github_client.list_runners(org_name=name, token=token)
        return self.github_client.list_runners(repo_name=name, token=token)

    def _delete_runner(self, scope, runner_id, token):
        kind, name = scope
        if kind == 'org':
            return self.github_client.delete_runner(runner_id, org_name=name, token=token)
        return self.github_client.delete_runner(runner_id, repo_name=name, token=token)

    # ------------------------------------------------------------------
    # main entry point
    # ------------------------------------------------------------------

    def run(self, dry_run=False):
        """
        Execute one pass.

        Args:
            dry_run (bool): compute and log every decision but do not create or delete anything.

        Returns:
            dict: report with the lists ``deleted``, ``created``, ``kept``, ``skipped``, ``errors``.
        """
        report = {
            'run_id': self.run_id,
            'dry_run': dry_run,
            'stuck_minutes': self.stuck_minutes,
            'repositories': [],
            'queued_jobs': 0,
            'live_vms': 0,
            'deleted': [],
            'created': [],
            'kept': [],
            'skipped': [],
            'errors': [],
        }
        logger.info("Reconcile %s starting (dry_run=%s, stuck_minutes=%s)", self.run_id, dry_run, self.stuck_minutes)

        token = self.github_client.get_installation_access_token()
        repos = self.github_client.list_installation_repositories(token=token)
        report['repositories'] = [repo.get('full_name') for repo in repos]

        # --- GitHub view: jobs of every run that is queued or in progress -------------------
        queued_jobs = []           # (repo, job) for queued jobs on a template label
        running_runner_names = set()
        jobs_by_id = {}
        listing_failed = False
        for repo in repos:
            full_name = repo.get('full_name')
            try:
                jobs = self.github_client.list_workflow_jobs(full_name, token=token)
            except Exception as e:
                listing_failed = True
                logger.error("Reconcile %s: could not list jobs of %s: %s", self.run_id, full_name, e)
                report['errors'].append({'repo': full_name, 'error': f"list jobs: {e}"})
                continue
            for job in jobs:
                jobs_by_id[str(job.get('id'))] = (repo, job)
                status = job.get('status')
                if status == 'in_progress' and job.get('runner_name'):
                    running_runner_names.add(job['runner_name'])
                elif status == 'queued' and template_label_for(job.get('labels')):
                    queued_jobs.append((repo, job))
        report['queued_jobs'] = len(queued_jobs)

        # --- Compute view ---------------------------------------------------------------
        vms = self.gcloud_client.list_runner_instances()
        live_vms = [vm for vm in vms if vm.get('status') in LIVE_INSTANCE_STATUSES]
        report['live_vms'] = len(live_vms)
        for vm in vms:
            if vm not in live_vms:
                logger.info("Reconcile %s: ignoring VM %s in zone %s with status %s",
                            self.run_id, vm['name'], vm['zone'], vm.get('status'))

        # --- Runner registrations, per scope ---------------------------------------------
        scope_by_owner = {}
        scope_by_repo = {}
        for repo in repos:
            scope = self._scope_for_repo(repo)
            owner_login = ((repo.get('owner') or {}).get('login') or '').lower()
            scope_by_repo[(repo.get('full_name') or '').lower()] = scope
            if scope[0] == 'org':
                scope_by_owner[owner_login] = scope
        runners_by_scope = {}
        runners_failed = False
        for scope in set(scope_by_repo.values()):
            try:
                runners_by_scope[scope] = {r['name']: r for r in self._list_runners(scope, token)}
            except Exception as e:
                runners_failed = True
                logger.error("Reconcile %s: could not list runners for %s: %s", self.run_id, scope, e)
                report['errors'].append({'scope': list(scope), 'error': f"list runners: {e}"})

        # --- Delete phase ---------------------------------------------------------------
        deleted_job_ids = set()
        if listing_failed or runners_failed:
            # Without a complete picture of what is running we cannot prove a VM is idle.
            logger.error("Reconcile %s: skipping the delete phase because a GitHub listing failed", self.run_id)
            report['skipped'].append({'phase': 'delete', 'reason': 'incomplete GitHub view'})
        else:
            for vm in live_vms:
                self._reconcile_vm(
                    vm, token, running_runner_names, runners_by_scope, scope_by_owner, scope_by_repo,
                    jobs_by_id, deleted_job_ids, report, dry_run,
                )

        # --- Create phase ---------------------------------------------------------------
        self._create_missing(queued_jobs, live_vms, deleted_job_ids, report, dry_run)

        logger.info(
            "Reconcile %s finished: %d queued job(s), %d live VM(s), %d deleted, %d created, %d kept, "
            "%d skipped, %d error(s)",
            self.run_id, report['queued_jobs'], report['live_vms'], len(report['deleted']),
            len(report['created']), len(report['kept']), len(report['skipped']), len(report['errors']),
        )
        return report

    # ------------------------------------------------------------------
    # delete phase
    # ------------------------------------------------------------------

    def _reconcile_vm(self, vm, token, running_runner_names, runners_by_scope, scope_by_owner, scope_by_repo,
                      jobs_by_id, deleted_job_ids, report, dry_run):
        name = vm['name']
        zone = vm['zone']
        labels = vm.get('labels') or {}
        job_id = labels.get(JOB_ID_LABEL)
        entry = {'vm': name, 'zone': zone, 'job_id': job_id}

        def keep(reason):
            logger.info("Reconcile %s: keeping VM %s (zone %s, job %s): %s", self.run_id, name, zone, job_id, reason)
            report['kept'].append({**entry, 'reason': reason})

        def skip(reason):
            logger.warning("Reconcile %s: skipping VM %s (zone %s, job %s): %s", self.run_id, name, zone, job_id, reason)
            report['skipped'].append({**entry, 'reason': reason})

        if vm.get('created_at') is None:
            return skip('unknown creation time')
        if not self._is_stuck(vm['created_at']):
            return keep(f"younger than {self.stuck_minutes} minutes")
        if name in running_runner_names:
            return keep('GitHub reports an in_progress job on this runner name')

        owner = (labels.get('gha-owner') or '').lower()
        repo_label = (labels.get('gha-repo') or '').lower()
        scope = scope_by_owner.get(owner) or scope_by_repo.get(f"{owner}/{repo_label}")
        if scope is None:
            return skip('owner/repo labels do not match any installed repository')
        runner = runners_by_scope.get(scope, {}).get(name)
        if runner and runner.get('busy'):
            return keep('GitHub reports the runner as busy')

        if job_id:
            repo_entry = jobs_by_id.get(job_id, (None, None))[0] or {}
            repo_full_name = repo_entry.get('full_name') or f"{owner}/{repo_label}"
            try:
                job = self.github_client.get_workflow_job(repo_full_name, job_id, token=token)
            except Exception as e:
                return skip(f"could not re-check job {job_id}: {e}")
            status = (job or {}).get('status')
            if status == 'in_progress':
                return keep('job re-checked as in_progress')
            if status == 'queued':
                if runner and runner.get('status') == 'online':
                    return keep('job still queued and runner is registered and online')
                reason = f"runner never registered after {self.stuck_minutes} minutes; job still queued"
            elif job is None:
                reason = 'job not found on GitHub'
            else:
                reason = f"job is {status}" + (f"/{job.get('conclusion')}" if job.get('conclusion') else '')
        else:
            if runner is None:
                reason = 'no job label and no registered runner'
            elif runner.get('status') == 'online':
                reason = 'no job label and the runner is idle'
            else:
                reason = f"no job label and the runner is {runner.get('status')}"

        if self._retire_vm(vm, runner, scope, token, reason, report, dry_run) and job_id:
            deleted_job_ids.add(str(job_id))

    def _retire_vm(self, vm, runner, scope, token, reason, report, dry_run):
        """Deregister the runner (if any) and delete the VM. Returns True when the VM delete was issued."""
        name = vm['name']
        zone = vm['zone']
        job_id = (vm.get('labels') or {}).get(JOB_ID_LABEL)
        entry = {'vm': name, 'zone': zone, 'job_id': job_id, 'reason': reason}
        if dry_run:
            logger.warning("Reconcile %s (dry run): would delete VM %s in zone %s (job %s): %s",
                           self.run_id, name, zone, job_id, reason)
            report['deleted'].append({**entry, 'dry_run': True})
            return True
        if runner:
            # Removing the registration first means the runner cannot take a job while we delete.
            try:
                self._delete_runner(scope, runner['id'], token)
                logger.info("Reconcile %s: deregistered runner %s (id %s) from %s", self.run_id, name, runner['id'], scope)
            except Exception as e:
                logger.warning("Reconcile %s: not deleting VM %s: GitHub refused to remove runner %s: %s",
                               self.run_id, name, runner['id'], e)
                report['skipped'].append({**entry, 'reason': f"runner removal failed: {e}"})
                return False
        try:
            self.gcloud_client.delete_runner_instance(name, delivery_id=self.run_id, zone=zone)
        except Exception as e:
            logger.error("Reconcile %s: failed to delete VM %s in zone %s: %s", self.run_id, name, zone, e)
            report['errors'].append({**entry, 'error': str(e)})
            return False
        logger.warning("Reconcile %s: deleted VM %s in zone %s (job %s): %s", self.run_id, name, zone, job_id, reason)
        report['deleted'].append(entry)
        return True

    # ------------------------------------------------------------------
    # create phase
    # ------------------------------------------------------------------

    def _create_missing(self, queued_jobs, live_vms, deleted_job_ids, report, dry_run):
        covered = {str((vm.get('labels') or {}).get(JOB_ID_LABEL)) for vm in live_vms}
        candidates = []
        for repo, job in queued_jobs:
            job_id = str(job.get('id'))
            created_at = parse_github_time(job.get('created_at'))
            entry = {'job_id': job_id, 'repo': repo.get('full_name'), 'job': job.get('name')}
            if job_id in deleted_job_ids:
                report['skipped'].append({**entry, 'reason': 'its VM was deleted in this pass; retry next pass'})
                continue
            if job_id in covered:
                continue
            if not self._is_stuck(created_at):
                continue
            candidates.append((created_at, repo, job))
        candidates.sort(key=lambda item: item[0])
        for _, repo, job in candidates[self.max_creates:]:
            report['skipped'].append({'job_id': str(job.get('id')), 'repo': repo.get('full_name'),
                                      'reason': f"deferred: more than {self.max_creates} creations in one pass"})
        candidates = candidates[:self.max_creates]
        if not candidates:
            return

        if dry_run:
            for created_at, repo, job in candidates:
                logger.warning("Reconcile %s (dry run): would create a VM for job %s (%s, %s) queued since %s",
                               self.run_id, job.get('id'), repo.get('full_name'), job.get('name'), created_at)
                report['created'].append({'job_id': str(job.get('id')), 'repo': repo.get('full_name'),
                                          'job': job.get('name'), 'dry_run': True})
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, self.create_workers)) as pool:
            futures = {pool.submit(self._provision, repo, job): (repo, job) for _, repo, job in candidates}
            for future in concurrent.futures.as_completed(futures):
                repo, job = futures[future]
                entry = {'job_id': str(job.get('id')), 'repo': repo.get('full_name'), 'job': job.get('name')}
                try:
                    instance_name = future.result()
                except Exception as e:
                    logger.error("Reconcile %s: failed to create a VM for job %s (%s): %s",
                                 self.run_id, job.get('id'), repo.get('full_name'), e)
                    report['errors'].append({**entry, 'error': str(e)})
                    continue
                if instance_name is None:
                    report['skipped'].append({**entry, 'reason': 'no matching instance template'})
                    continue
                logger.warning("Reconcile %s: created VM %s for job %s (%s, %s) queued since %s",
                               self.run_id, instance_name, job.get('id'), repo.get('full_name'), job.get('name'),
                               job.get('created_at'))
                report['created'].append({**entry, 'vm': instance_name})

    def _provision(self, repo, job):
        owner = repo.get('owner') or {}
        org_name = owner.get('login') if (owner.get('type') or '').lower() == 'organization' else None
        return self.webhook_service.provision_runner(
            template_label_for(job.get('labels')),
            repo.get('html_url'),
            owner.get('html_url'),
            repo.get('full_name'),
            org_name,
            job_id=job.get('id'),
            delivery_id=self.run_id,
            name_suffix=RECONCILE_NAME_SUFFIX,
        )
