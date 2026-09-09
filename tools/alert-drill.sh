#!/usr/bin/env bash

# Drill for the two Terraform-managed alert policies (gcp/monitoring.tf).
#
#   stuck      write a synthetic reconcile heartbeat whose oldest_queued_job_age_seconds is above
#              the stuck-job threshold; policy "GitHub runners: job queued too long without a
#              runner" must open an incident within a few minutes.
#   heartbeat  pause the reconcile Cloud Scheduler job (asks for confirmation) so no heartbeat is
#              written; policy "GitHub runners: reconciler heartbeat missing" must open an incident
#              after the configured window (15 min by default). Run `resume` afterwards.
#   resume     resume the scheduler job.
#
# Every subcommand is a cloud mutation (a log entry, or the scheduler state); run only with the
# project owner's approval. Usage:
#   tools/alert-drill.sh <project> <region> stuck [age_seconds]
#   tools/alert-drill.sh <project> <region> heartbeat
#   tools/alert-drill.sh <project> <region> resume

set -euo pipefail

if [ $# -lt 3 ]; then
	echo "usage: $0 <project> <region> stuck [age_seconds] | heartbeat | resume" >&2
	exit 2
fi

PROJECT="$1"
REGION="$2"
MODE="$3"
AGE="${4:-3600}"

# The scheduler job is github-runners-reconcile-<region short name>; look it up instead of guessing.
JOB="$(gcloud scheduler jobs list --project="${PROJECT}" --location="${REGION}" --format='value(name.basename())' \
	--filter='name ~ github-runners-reconcile-' 2>/dev/null | head -n 1 || true)"
CONSOLE="https://console.cloud.google.com/monitoring/alerting/incidents?project=${PROJECT}"

case "$MODE" in
	stuck)
		echo "Writing a synthetic heartbeat with oldest_queued_job_age_seconds=${AGE} to project ${PROJECT}..."
		gcloud logging write reconcile-alert-drill \
			"{\"event\": \"reconcile_heartbeat\", \"message\": \"RECONCILE_HEARTBEAT run_id=drill oldest_queued_job_age_seconds=${AGE}\", \"run_id\": \"drill\", \"oldest_queued_job_age_seconds\": ${AGE}, \"queued_jobs\": 1, \"live_vms\": 0, \"dry_run\": true}" \
			--payload-type=json --severity=INFO --project="${PROJECT}"
		echo "Entry written. Confirm ingestion:"
		gcloud logging read 'logName:"reconcile-alert-drill" AND jsonPayload.event="reconcile_heartbeat"' \
			--project="${PROJECT}" --limit=1 --freshness=10m --format='value(timestamp,jsonPayload.oldest_queued_job_age_seconds)'
		echo "Watch for the incident (log-based metrics take 1-3 minutes to appear): ${CONSOLE}"
		echo "Note: this entry also counts as a heartbeat for the next 15 minutes."
		;;
	heartbeat)
		[ -n "$JOB" ] || { echo "no github-runners-reconcile-* scheduler job found in ${PROJECT}/${REGION}" >&2; exit 1; }
		echo "This pauses Cloud Scheduler job ${JOB} in ${PROJECT}/${REGION}; the reconciler stops until 'resume'."
		read -r -p "Type PAUSE to continue: " answer
		[ "$answer" = "PAUSE" ] || { echo "aborted"; exit 1; }
		gcloud scheduler jobs pause "${JOB}" --location="${REGION}" --project="${PROJECT}"
		echo "Paused at $(date -u +%Y-%m-%dT%H:%M:%SZ). The heartbeat-missing policy should open an incident once the window elapses: ${CONSOLE}"
		echo "Then run: $0 ${PROJECT} ${REGION} resume"
		;;
	resume)
		[ -n "$JOB" ] || { echo "no github-runners-reconcile-* scheduler job found in ${PROJECT}/${REGION}" >&2; exit 1; }
		gcloud scheduler jobs resume "${JOB}" --location="${REGION}" --project="${PROJECT}"
		echo "Resumed. The next pass writes a heartbeat and the incident should close."
		;;
	*)
		echo "unknown mode: ${MODE}" >&2
		exit 2
		;;
esac
