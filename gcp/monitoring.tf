# Alert policies for the runner manager (no notification channels: alerts only, by design).
# Both read the structured heartbeat the reconciler writes after every completed pass
# (jsonPayload.event = "reconcile_heartbeat", see app/services/reconcile_service.py).
# The filters match on the event key only, so a synthetic entry written with
# `gcloud logging write ... --payload-type=json` (tools/alert-drill.sh) exercises the same path.

locals {
  reconcile_heartbeat_filter = "jsonPayload.event=\"reconcile_heartbeat\""
}

# One count per completed reconcile pass
# https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/logging_metric
resource "google_logging_metric" "github-runners-reconcile-heartbeat" {
  project     = module.project.project_id
  name        = "github_runners/reconcile_heartbeat"
  description = "Completed reconcile passes of the GitHub Actions Runners manager (Terraform managed)"
  filter      = local.reconcile_heartbeat_filter

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

# Age of the oldest queued job with a gcp- label, sampled once per pass
resource "google_logging_metric" "github-runners-oldest-queued-job-age" {
  project         = module.project.project_id
  name            = "github_runners/oldest_queued_job_age_seconds"
  description     = "Oldest queued GitHub Actions job waiting for a runner, in seconds (Terraform managed)"
  filter          = local.reconcile_heartbeat_filter
  value_extractor = "EXTRACT(jsonPayload.oldest_queued_job_age_seconds)"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "s"
  }

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 64
      growth_factor      = 2
      scale              = 1
    }
  }
}

# (a) Reconciler heartbeat missing: no completed pass for github_runners_alert_heartbeat_missing_seconds
# https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/monitoring_alert_policy
resource "google_monitoring_alert_policy" "github-runners-reconciler-heartbeat-missing" {
  project      = module.project.project_id
  display_name = "GitHub runners: reconciler heartbeat missing"
  combiner     = "OR"
  severity     = "ERROR"

  documentation {
    content   = "No reconcile pass has completed in ${var.github_runners_alert_heartbeat_missing_seconds} s. Check the Cloud Scheduler job ${local.github_runners_manager_name} (github-runners-reconcile-*) and the manager's logs for 'Reconcile ... failed'. Queued jobs whose webhook was dropped will not self-heal until it runs again."
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "reconcile_heartbeat absent"

    condition_absent {
      filter   = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.github-runners-reconcile-heartbeat.name}\""
      duration = "${var.github_runners_alert_heartbeat_missing_seconds}s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = []
  user_labels = {
    component = "github-runners-manager"
    managed   = "terraform"
  }

  depends_on = [
    google_logging_metric.github-runners-reconcile-heartbeat
  ]
}

# (b) Stuck job: a job has waited for a runner longer than github_runners_alert_stuck_job_seconds
resource "google_monitoring_alert_policy" "github-runners-stuck-job" {
  project      = module.project.project_id
  display_name = "GitHub runners: job queued too long without a runner"
  combiner     = "OR"
  severity     = "WARNING"

  documentation {
    content   = "A workflow job on a gcp- label has been queued for more than ${var.github_runners_alert_stuck_job_seconds} s. The reconciler keeps retrying; look for 'queued for ... h with no runner' and 'capacity error' / 'no matching instance template' in the manager's logs (region-wide stockout, deleted template, or a label with no template)."
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "oldest_queued_job_age_seconds above threshold"

    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.github-runners-oldest-queued-job-age.name}\""
      comparison      = "COMPARISON_GT"
      threshold_value = var.github_runners_alert_stuck_job_seconds
      duration        = "0s"

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_PERCENTILE_99"
        cross_series_reducer = "REDUCE_MAX"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = []
  user_labels = {
    component = "github-runners-manager"
    managed   = "terraform"
  }

  depends_on = [
    google_logging_metric.github-runners-oldest-queued-job-age
  ]
}
