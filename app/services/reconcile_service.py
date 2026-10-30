"""
Reconciler: closes the gap between GitHub's queued jobs and the manager's runner VMs.

The webhook path is best effort: a dropped delivery leaves a queued job with no VM, a runner
that never registers leaves a VM with no job, and a job cancelled while queued leaves both
GitHub and Compute Engine waiting for an event that never comes. Run periodically (Cloud
Scheduler -> POST /reconcile) this service:

* creates VMs per label for the queued jobs that the label's free runners cannot take. GitHub
  hands a queued job to any runner with its label, so a VM created for one job often runs
  another: jobs are counted against VMs by label, not matched by job id. A job queued less than
  ``RECONCILE_QUEUE_GRACE_MINUTES`` is left to its own webhook task, and the VM made for it is
  not counted as free for older jobs;
* deletes every runner VM older than ``RECONCILE_STUCK_MINUTES`` that is not running a job:
  one whose runner never registered or has gone, or an idle runner beyond the label's queued
  demand, deregistering the runner from GitHub first so it cannot pick up a job mid-delete;
* never deletes a VM that GitHub reports as running a job (by runner name or busy flag).

Every decision is logged with the job id, VM name, zone and reason, and returned in the report.
"""
import concurrent.futures
import datetime
import json
import logging
import os
import sys
import uuid

from app.clients import GitHubClient, GCloudClient
from app.clients.gcloud_client import JOB_ID_LABEL, LIVE_INSTANCE_STATUSES, label_value
from app.services.webhook_service import WebhookService

logger = logging.getLogger(__name__)

DEFAULT_STUCK_MINUTES = 10
# A queued job younger than this is left to the VM its webhook task is creating.
DEFAULT_QUEUE_GRACE_MINUTES = 3
DEFAULT_MAX_CREATES = 20
DEFAULT_CREATE_WORKERS = 4
# Parallel GitHub listings per pass (well under GitHub's 100-concurrent secondary limit).
LISTING_WORKERS = 8
# A job queued longer than this is retried at a reduced rate (once per RECONCILE_SLOW_RETRY_MINUTES)
# instead of every pass, so a template whose runner never registers does not churn a VM every
# five minutes. It is never given up on: GitHub's own 24 h queued-job timeout is the only floor.
DEFAULT_SLOW_RETRY_HOURS = 6
DEFAULT_SLOW_RETRY_MINUTES = 60
# How often the pass runs (must match the Cloud Scheduler schedule); used to gate slow retries.
DEFAULT_INTERVAL_MINUTES = 5
# Stable marker for the heartbeat log line the monitoring alert reads.
HEARTBEAT_EVENT = 'reconcile_heartbeat'
HEARTBEAT_MARKER = 'RECONCILE_HEARTBEAT'
STUCK_JOB_EVENT = 'reconcile_stuck_job'
# Suffix for VMs the reconciler creates, so a late webhook for the same job never races on
# the same instance name in a different zone; both VMs carry the same gha-job-id label.
RECONCILE_NAME_SUFFIX = '-r'


def emit_structured(event, message, **fields):
    """
    Write one JSON line to stdout. Cloud Run turns JSON stdout lines into structured log entries
    (jsonPayload), which log-based metrics and alerts can read; the plain logger line stays too.
    """
    record = {'severity': 'INFO', 'message': message, 'event': event, **fields}
    sys.stdout.write(json.dumps(record, default=str) + '\n')  # one write, so lines cannot interleave
    sys.stdout.flush()
    logger.info("%s", message)


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
        queue_grace_minutes=None,
        max_creates=None,
        create_workers=None,
        now=None,
        exclude_repositories=None,
        slow_retry_hours=None,
    ):
        self.github_client = github_client or GitHubClient()
        self.gcloud_client = gcloud_client or GCloudClient()
        self.webhook_service = webhook_service or WebhookService(
            github_client=self.github_client, gcloud_client=self.gcloud_client
        )
        self.stuck_minutes = stuck_minutes if stuck_minutes is not None \
            else _env_int('RECONCILE_STUCK_MINUTES', DEFAULT_STUCK_MINUTES)
        self.queue_grace_minutes = queue_grace_minutes if queue_grace_minutes is not None \
            else _env_int('RECONCILE_QUEUE_GRACE_MINUTES', DEFAULT_QUEUE_GRACE_MINUTES)
        self.max_creates = max_creates if max_creates is not None \
            else _env_int('RECONCILE_MAX_CREATES', DEFAULT_MAX_CREATES)
        self.create_workers = create_workers if create_workers is not None \
            else _env_int('RECONCILE_CREATE_WORKERS', DEFAULT_CREATE_WORKERS)
        self.exclude_repositories = os.environ.get('RECONCILE_EXCLUDE_REPOSITORIES', '') \
            if exclude_repositories is None else exclude_repositories
        self.slow_retry_hours = slow_retry_hours if slow_retry_hours is not None \
            else _env_int('RECONCILE_SLOW_RETRY_HOURS', DEFAULT_SLOW_RETRY_HOURS)
        self.slow_retry_minutes = max(1, _env_int('RECONCILE_SLOW_RETRY_MINUTES', DEFAULT_SLOW_RETRY_MINUTES))
        self.interval_minutes = max(1, _env_int('RECONCILE_INTERVAL_MINUTES', DEFAULT_INTERVAL_MINUTES))
        self._now = now
        self._repo_names = {}  # sanitised owner/repo label key -> repository full name
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
    def repo_key(owner, repo_name):
        """Key a repository the way create_runner_instance labels VMs (sanitised owner/repo)."""
        return f"{label_value(owner)}/{label_value(repo_name)}"

    def _select_repositories(self, repos):
        """
        Every repository the App installation can see, minus archived/disabled ones (they cannot
        hold a queued job) and RECONCILE_EXCLUDE_REPOSITORIES (comma-separated owner/repo). There
        is deliberately no include list: a repository the App is installed on is covered without
        configuration. Excluded repositories are neither provisioned for nor cleaned up.

        Returns:
            tuple(list, set): the repositories to scan, and the label keys of excluded ones.
        """
        excluded = {name.strip().lower() for name in self.exclude_repositories.split(',') if name.strip()}
        selected, dropped, excluded_keys, seen = [], [], set(), set()
        for repo in repos:
            full_name = (repo.get('full_name') or '')
            owner_login = (repo.get('owner') or {}).get('login') or ''
            seen.add(full_name.lower())
            if full_name.lower() in excluded:
                dropped.append(full_name)
                excluded_keys.add(self.repo_key(owner_login, full_name.split('/', 1)[-1]))
            elif repo.get('archived') or repo.get('disabled'):
                logger.info("Reconcile %s: skipping %s repository %s", self.run_id,
                            'archived' if repo.get('archived') else 'disabled', full_name)
            else:
                selected.append(repo)
        if dropped:
            logger.info("Reconcile %s: excluding %s by configuration", self.run_id, ', '.join(dropped))
        unknown = sorted(excluded - seen)
        if unknown:
            logger.warning("Reconcile %s: RECONCILE_EXCLUDE_REPOSITORIES names repositories the App cannot see "
                           "(renamed or removed?): %s", self.run_id, ', '.join(unknown))
        return selected, excluded_keys

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
            'oldest_queued_job_age_seconds': 0,
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
        repos, excluded_keys = self._select_repositories(repos)
        report['repositories'] = [repo.get('full_name') for repo in repos]

        # --- GitHub view: jobs of every run that is queued or in progress -------------------
        queued_jobs = []           # (repo, job) for queued jobs on a template label
        running_runner_names = set()
        jobs_by_id = {}
        failed_repo_keys = set()  # repositories whose jobs could not be listed: their VMs are left alone

        def list_jobs(repo):
            return repo, self.github_client.list_workflow_jobs(repo.get('full_name'), token=token)

        with concurrent.futures.ThreadPoolExecutor(max_workers=LISTING_WORKERS) as pool:
            futures = {pool.submit(list_jobs, repo): repo for repo in repos}
            results = []
            for future in concurrent.futures.as_completed(futures):
                repo = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    full_name = repo.get('full_name') or ''
                    owner_login = (repo.get('owner') or {}).get('login') or ''
                    failed_repo_keys.add(self.repo_key(owner_login, full_name.split('/', 1)[-1]))
                    logger.error("Reconcile %s: could not list jobs of %s: %s", self.run_id, full_name, e)
                    report['errors'].append({'repo': full_name, 'error': f"list jobs: {e}"})
        # Deterministic order (by repository name) so logs and reports are stable.
        results.sort(key=lambda item: item[0].get('full_name') or '')
        for repo, jobs in results:
            for job in jobs:
                jobs_by_id[str(job.get('id'))] = (repo, job)
                status = job.get('status')
                if status == 'in_progress' and job.get('runner_name'):
                    running_runner_names.add(job['runner_name'])
                elif status == 'queued' and template_label_for(job.get('labels')):
                    queued_jobs.append((repo, job))
        report['queued_jobs'] = len(queued_jobs)
        # Queued jobs per (sanitised) label: idle runners of a label are kept while it has demand.
        queued_by_label = {}
        for _, job in queued_jobs:
            key = label_value(template_label_for(job.get('labels')))
            queued_by_label[key] = queued_by_label.get(key, 0) + 1
        ages = [self.now() - parse_github_time(job.get('created_at')) for _, job in queued_jobs
                if parse_github_time(job.get('created_at')) is not None]
        report['oldest_queued_job_age_seconds'] = int(max(ages).total_seconds()) if ages else 0

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
            owner_login = (repo.get('owner') or {}).get('login') or ''
            repo_name = (repo.get('full_name') or '/').split('/', 1)[1]
            # Keyed the way create_runner_instance writes the labels (sanitised GCE label values).
            scope_by_repo[self.repo_key(owner_login, repo_name)] = scope
            self._repo_names[self.repo_key(owner_login, repo_name)] = repo.get('full_name')
            if scope[0] == 'org':
                scope_by_owner[label_value(owner_login)] = scope
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
        if runners_failed:
            # Without the runner registrations we cannot prove any VM is idle.
            logger.error("Reconcile %s: skipping the delete phase because a runner listing failed", self.run_id)
            report['skipped'].append({'phase': 'delete', 'reason': 'incomplete GitHub view'})
        else:
            if failed_repo_keys:
                logger.error("Reconcile %s: leaving VMs of %d repositor%s alone because their jobs could not be listed",
                             self.run_id, len(failed_repo_keys), 'y' if len(failed_repo_keys) == 1 else 'ies')
            idle_budget = dict(queued_by_label)
            # Newest first, so the idle VMs kept for queued jobs are the ones furthest from their
            # maximum run duration; the oldest surplus is retired.
            epoch = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
            for vm in sorted(live_vms, key=lambda v: v.get('created_at') or epoch, reverse=True):
                self._reconcile_vm(
                    vm, token, running_runner_names, runners_by_scope, scope_by_owner, scope_by_repo,
                    deleted_job_ids, idle_budget, report, dry_run, failed_repo_keys | excluded_keys,
                )

        # --- Create phase ---------------------------------------------------------------
        busy_names = set(running_runner_names)
        for runners in runners_by_scope.values():
            busy_names.update(name for name, r in runners.items() if r.get('busy'))
        deleted_names = {entry['vm'] for entry in report['deleted']}
        self._create_missing(queued_jobs, live_vms, deleted_job_ids, busy_names, deleted_names, report, dry_run)

        logger.info(
            "Reconcile %s finished: %d queued job(s), %d live VM(s), %d deleted, %d created, %d kept, "
            "%d skipped, %d error(s)",
            self.run_id, report['queued_jobs'], report['live_vms'], len(report['deleted']),
            len(report['created']), len(report['kept']), len(report['skipped']), len(report['errors']),
        )
        # Heartbeat: one structured line per completed pass; the monitoring alerts read it
        # (absence = reconciler down, oldest_queued_job_age_seconds = stuck jobs).
        emit_structured(
            HEARTBEAT_EVENT,
            f"{HEARTBEAT_MARKER} run_id={self.run_id} queued_jobs={report['queued_jobs']} "
            f"oldest_queued_job_age_seconds={report['oldest_queued_job_age_seconds']} live_vms={report['live_vms']} "
            f"created={len(report['created'])} deleted={len(report['deleted'])} errors={len(report['errors'])}",
            run_id=self.run_id,
            dry_run=dry_run,
            queued_jobs=report['queued_jobs'],
            oldest_queued_job_age_seconds=report['oldest_queued_job_age_seconds'],
            live_vms=report['live_vms'],
            created=len(report['created']),
            deleted=len(report['deleted']),
            errors=len(report['errors']),
        )
        return report

    # ------------------------------------------------------------------
    # delete phase
    # ------------------------------------------------------------------

    def _reconcile_vm(self, vm, token, running_runner_names, runners_by_scope, scope_by_owner, scope_by_repo,
                      deleted_job_ids, idle_budget, report, dry_run, untouchable_repo_keys=frozenset()):
        """Keep or retire one VM by what its own runner is doing. The job id on its label says only
        which job it was created for: GitHub may have given it any job with the label."""
        name = vm['name']
        zone = vm['zone']
        labels = vm.get('labels') or {}
        job_id = labels.get(JOB_ID_LABEL)
        label = labels.get('gha-runner') or ''
        entry = {'vm': name, 'zone': zone, 'job_id': job_id}

        def keep(reason):
            logger.info("Reconcile %s: keeping VM %s (zone %s, job %s): %s", self.run_id, name, zone, job_id, reason)
            report['kept'].append({**entry, 'reason': reason})

        def skip(reason):
            logger.warning("Reconcile %s: skipping VM %s (zone %s, job %s): %s", self.run_id, name, zone, job_id, reason)
            report['skipped'].append({**entry, 'reason': reason})

        def idle_decision(runner):
            """An idle registered runner is kept while its label has queued jobs it can take."""
            if idle_budget.get(label, 0) > 0:
                idle_budget[label] -= 1
                return 'runner is registered and online, idle, and a job is queued for its label', True
            return 'runner is registered and online, idle, and no job is queued for its label', False

        if vm.get('created_at') is None:
            return skip('unknown creation time')
        if not self._is_stuck(vm['created_at']):
            return keep(f"younger than {self.stuck_minutes} minutes")
        if name in running_runner_names:
            return keep('GitHub reports an in_progress job on this runner name')

        owner = (labels.get('gha-owner') or '').lower()
        repo_label = (labels.get('gha-repo') or '').lower()
        if f"{owner}/{repo_label}" in untouchable_repo_keys:
            # Excluded by configuration, or its jobs could not be listed this pass: no safe decision.
            return skip('its repository is excluded or its jobs could not be listed this pass')
        scope = scope_by_owner.get(owner) or scope_by_repo.get(f"{owner}/{repo_label}")
        if scope is None:
            return skip('owner/repo labels do not match any installed repository')
        runner = runners_by_scope.get(scope, {}).get(name)
        if runner and runner.get('busy'):
            return keep('GitHub reports the runner as busy')

        if runner is None:
            # The runner listing was taken at pass start; a runner that registered since could be
            # busy by now. Re-list the scope so the deregister step (and GitHub's refusal to remove a
            # busy runner) always gates the delete.
            try:
                fresh = {r['name']: r for r in self._list_runners(scope, token)}
            except Exception as e:
                return skip(f"could not re-list runners before deleting: {e}")
            runner = fresh.get(name)
            if runner is not None and runner.get('busy'):
                return keep('runner registered since the listing and is busy')
        if runner is None:
            # Nothing to deregister, so GitHub's refusal to remove a busy runner cannot gate this
            # delete. Re-check the VM's own job in case it started here after the listings.
            if not job_id:
                reason = 'no job label and no registered runner'
            else:
                repo_full_name = self._repo_names.get(f"{owner}/{repo_label}", f"{owner}/{repo_label}")
                try:
                    job = self.github_client.get_workflow_job(repo_full_name, job_id, token=token)
                except Exception as e:
                    return skip(f"could not re-check job {job_id}: {e}")
                status = (job or {}).get('status')
                if status == 'in_progress' and (job or {}).get('runner_name') == name:
                    return keep('job re-checked as in_progress on this runner')
                if job is None:
                    reason = 'job not found on GitHub'
                elif status == 'queued':
                    reason = f"runner never registered after {self.stuck_minutes} minutes; job still queued"
                elif status == 'in_progress':
                    reason = 'no registered runner; its job is running on another runner'
                else:
                    reason = f"job is {status}" + (f"/{job.get('conclusion')}" if job.get('conclusion') else '')
        elif runner.get('status') == 'online':
            reason, kept = idle_decision(runner)
            if kept:
                return keep(reason)
        else:
            reason = f"runner is {runner.get('status')} and not busy"

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

    def _create_missing(self, queued_jobs, live_vms, deleted_job_ids, busy_names, deleted_names, report, dry_run):
        """Create, per label, as many VMs as the label's queued jobs exceed its free VMs.

        Demand: queued jobs older than the grace period. Supply: live VMs of the label not running
        a job (booting, or registered and idle), not deleted this pass, and not created for a job
        still inside the grace period (that VM is its own job's). VMs are named after the job they
        are created for; a job whose own VM took another job gets ``-r``, ``-r2`` and so on.
        """
        live_names = {vm['name'] for vm in live_vms}
        by_label = {}
        for repo, job in queued_jobs:
            by_label.setdefault(label_value(template_label_for(job.get('labels'))), []).append((repo, job))
        grace = datetime.timedelta(minutes=self.queue_grace_minutes)
        candidates = []
        templates = None
        provisionable = {}  # label -> bool, resolved once per pass against the template list
        for label_key, jobs in by_label.items():
            reserved = {str(job.get('id')) for _, job in jobs
                        if parse_github_time(job.get('created_at')) is not None
                        and self.now() - parse_github_time(job.get('created_at')) < grace}
            supply = [vm for vm in live_vms
                      if (vm.get('labels') or {}).get('gha-runner') == label_key
                      and vm['name'] not in busy_names and vm['name'] not in deleted_names
                      and str((vm.get('labels') or {}).get(JOB_ID_LABEL)) not in reserved]
            covered = {str((vm.get('labels') or {}).get(JOB_ID_LABEL)) for vm in live_vms
                       if (vm.get('labels') or {}).get('gha-runner') == label_key}
            waiting = []
            for repo, job in jobs:
                job_id = str(job.get('id'))
                created_at = parse_github_time(job.get('created_at'))
                entry = {'job_id': job_id, 'repo': repo.get('full_name'), 'job': job.get('name')}
                if created_at is None or job_id in reserved:
                    continue
                if job_id in deleted_job_ids:
                    logger.warning("Reconcile %s: job %s (%s): its VM was deleted in this pass; retrying next pass",
                                   self.run_id, job_id, repo.get('full_name'))
                    report['skipped'].append({**entry, 'reason': 'its VM was deleted in this pass; retry next pass'})
                    continue
                if not self._slow_retry_due(repo, job, created_at, entry, report):
                    continue
                waiting.append((created_at, repo, job))
            deficit = len(waiting) - len(supply)
            if deficit <= 0:
                continue
            # Jobs with no VM of their own first, then the oldest.
            waiting.sort(key=lambda item: (str(item[2].get('id')) in covered, item[0]))
            label = template_label_for(waiting[0][2].get('labels'))
            if label not in provisionable:
                if templates is None:
                    templates = self.gcloud_client.list_templates()
                provisionable[label] = self.gcloud_client.has_template_for(label, templates=templates)
            if not provisionable[label]:
                for _, repo, job in waiting:
                    logger.warning("Reconcile %s: job %s (%s, %s) is queued on label %s but no instance template matches it",
                                   self.run_id, job.get('id'), repo.get('full_name'), job.get('name'), label)
                    report['skipped'].append({'job_id': str(job.get('id')), 'repo': repo.get('full_name'),
                                              'job': job.get('name'),
                                              'reason': f"no matching instance template for label {label}"})
                continue
            for created_at, repo, job in waiting[:deficit]:
                candidates.append((created_at, repo, job, self._name_suffix(str(job.get('id')), live_names)))
        candidates = self._share_budget(candidates)
        for _, repo, job, _ in candidates[self.max_creates:]:
            logger.warning("Reconcile %s: job %s (%s) deferred to the next pass (more than %d creations)",
                           self.run_id, job.get('id'), repo.get('full_name'), self.max_creates)
            report['skipped'].append({'job_id': str(job.get('id')), 'repo': repo.get('full_name'),
                                      'reason': f"deferred: more than {self.max_creates} creations in one pass"})
        candidates = candidates[:self.max_creates]
        if not candidates:
            return

        if dry_run:
            for created_at, repo, job, _ in candidates:
                logger.warning("Reconcile %s (dry run): would create a VM for job %s (%s, %s) queued since %s",
                               self.run_id, job.get('id'), repo.get('full_name'), job.get('name'), created_at)
                report['created'].append({'job_id': str(job.get('id')), 'repo': repo.get('full_name'),
                                          'job': job.get('name'), 'dry_run': True})
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, self.create_workers)) as pool:
            futures = {pool.submit(self._provision, repo, job, suffix): (repo, job) for _, repo, job, suffix in candidates}
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

    def _slow_retry_due(self, repo, job, created_at, entry, report):
        """A job queued longer than slow_retry_hours is retried once per slow interval, not every pass."""
        if self.now() - created_at < datetime.timedelta(hours=self.slow_retry_hours):
            return True
        job_id = str(job.get('id'))
        age_minutes = int((self.now() - created_at).total_seconds() // 60)
        # Two pass intervals wide so a pass that runs a few seconds late cannot miss the window.
        due = age_minutes % self.slow_retry_minutes < 2 * self.interval_minutes
        logger.warning(
            "Reconcile %s: job %s (%s, %s) has been queued for %d h with no runner; %s",
            self.run_id, job_id, repo.get('full_name'), job.get('name'), age_minutes // 60,
            "retrying now" if due else
            f"retrying at most every {self.slow_retry_minutes} min (next in about "
            f"{self.slow_retry_minutes - age_minutes % self.slow_retry_minutes} min)",
        )
        emit_structured(STUCK_JOB_EVENT, f"stuck job {job_id} queued for {age_minutes} min",
                        run_id=self.run_id, job_id=job_id, repo=repo.get('full_name'),
                        queued_minutes=age_minutes, retry_now=due)
        if not due:
            report['skipped'].append({**entry, 'reason': (f"queued for more than {self.slow_retry_hours} hours; "
                                                          f"retried at most every {self.slow_retry_minutes} min")})
        return due

    @staticmethod
    def _name_suffix(job_id, live_names):
        """``-r``, or ``-r2``, ``-r3``... when a VM of that name is still alive."""
        n = 1
        while True:
            suffix = RECONCILE_NAME_SUFFIX if n == 1 else f"{RECONCILE_NAME_SUFFIX}{n}"
            names = (f"gcp-runner-{job_id}{suffix}", f"gcp-runner-dependabot-{job_id}{suffix}")
            if not any(name in live_names for name in names):
                return suffix
            n += 1

    @staticmethod
    def _share_budget(candidates):
        """
        Order candidates round-robin across repositories, oldest job first within each repository
        and repositories ordered by their oldest job, so one repository's fan-out cannot consume the
        whole per-pass budget while another repository's single job waits.
        """
        by_repo = {}
        for item in sorted(candidates, key=lambda item: item[0]):
            by_repo.setdefault(item[1].get('full_name'), []).append(item)
        queues = list(by_repo.values())  # insertion order = by oldest job
        ordered = []
        while queues:
            remaining = []
            for queue in queues:
                ordered.append(queue.pop(0))
                if queue:
                    remaining.append(queue)
            queues = remaining
        return ordered

    def _provision(self, repo, job, name_suffix=RECONCILE_NAME_SUFFIX):
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
            name_suffix=name_suffix,
        )
