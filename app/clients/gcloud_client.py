"""
Google Cloud Client for managing GCE instances.
"""
import concurrent.futures
import logging
import os
import re
import uuid
import shlex
import google.cloud.compute_v1 as compute_v1
from google.api_core import exceptions as gapi_exceptions

logger = logging.getLogger(__name__)

# How long to wait for an instances.insert operation before giving up (seconds).
DEFAULT_INSERT_TIMEOUT_SECONDS = 120

# Operation error codes that mean "this zone cannot serve the request right now".
# https://cloud.google.com/compute/docs/troubleshooting/troubleshooting-vm-creation
RETRY_ELSEWHERE_CODE_PREFIXES = (
    'ZONE_RESOURCE_POOL_EXHAUSTED',
    'QUOTA_EXCEEDED',
)
# Message fragments that indicate a stockout even when the code is generic.
RETRY_ELSEWHERE_MESSAGE_MARKERS = (
    'stockout',
    'does not have enough resources',
    'currently unavailable',
)


class InstanceCreationError(Exception):
    """The instances.insert operation did not succeed. Never swallowed."""


class ZoneCapacityError(InstanceCreationError):
    """The insert failed for a capacity reason (stockout/quota); another zone may work."""

    def __init__(self, message, code=None, zone=None):
        super().__init__(message)
        self.code = code
        self.zone = zone


def classify_operation_errors(errors):
    """
    Classify the ``error.errors`` list of a finished Compute operation.

    Args:
        errors: iterable of compute_v1.Errors (or anything with .code/.message/.error_details).

    Returns:
        tuple(bool, str): (retry_elsewhere, human readable summary "CODE: message; ...").
    """
    retry_elsewhere = False
    parts = []
    for error in errors or []:
        code = str(getattr(error, 'code', '') or '')
        message = str(getattr(error, 'message', '') or '')
        reason = ''
        zone = ''
        for detail in getattr(error, 'error_details', None) or []:
            error_info = getattr(detail, 'error_info', None)
            if error_info is not None:
                reason = reason or str(getattr(error_info, 'reason', '') or '')
                metadatas = getattr(error_info, 'metadatas', None) or {}
                zone = zone or str(metadatas.get('zone', '') if hasattr(metadatas, 'get') else '')
        summary = f"{code}: {message}"
        if reason:
            summary += f" (reason={reason}"
            summary += f", zone={zone})" if zone else ")"
        parts.append(summary)
        haystack = f"{message} {reason}".lower()
        if code.startswith(RETRY_ELSEWHERE_CODE_PREFIXES) or reason.lower() == 'stockout' \
                or any(marker in haystack for marker in RETRY_ELSEWHERE_MESSAGE_MARKERS):
            retry_elsewhere = True
    return retry_elsewhere, '; '.join(parts)


class GCloudClient:
    """Client for interacting with Google Cloud Compute Engine API."""

    def __init__(self):
        """Initialize GCloudClient with project and zone configuration."""
        self.project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        self.zone = os.environ.get('GOOGLE_CLOUD_ZONE', 'us-central1-a')
        self.github_runner_group = os.environ.get('GITHUB_RUNNER_GROUP', '').strip()
        self.region = '-'.join(self.zone.split('-')[:-1])
        self.insert_timeout = int(os.environ.get('GCE_INSERT_TIMEOUT_SECONDS', DEFAULT_INSERT_TIMEOUT_SECONDS))

        if not self.project_id:
            logger.warning("GOOGLE_CLOUD_PROJECT not set. GCloudClient will not work correctly.")

        # https://docs.cloud.google.com/python/docs/reference/compute/latest/google.cloud.compute_v1.services.instances.InstancesClient
        self.instance_client = compute_v1.InstancesClient()
        # Create a RegionInstanceTemplatesClient for retrieving templates in a specific region
        # https://docs.cloud.google.com/python/docs/reference/compute/latest/google.cloud.compute_v1.services.region_instance_templates
        self.instance_templates_client = compute_v1.RegionInstanceTemplatesClient()

    def _get_template_name(self, template_name):
        """
        Find a matching instance template by name prefix.

        Args:
            template_name (str): The name prefix to search for.

        Returns:
            google.cloud.compute_v1.InstanceTemplate or None: The matching template resource.
        """
        # Replace dots with dashes for template name, so gcp-ubuntu-24.04 matches gcp-ubuntu-24-04
        prefix = template_name.replace('.', '-')
        # logger.info(f"Prefix: {prefix}")
        # Create regex pattern: prefix followed by dash, at least 12 digits, and optional alphanumeric characters
        pattern = re.compile(f"^{re.escape(prefix)}-\\d{{14,}}[a-z0-9]*$")
        try:
            # List all templates to find one that matches the pattern
            for template in self.instance_templates_client.list(project=self.project_id, region=self.region):
                # logger.info(f"Template: {template.name}")
                if pattern.match(template.name):
                    return template
            return None
        except Exception:
            return None

    def create_runner_instance(
        self,
        registration_token,
        repo_url,
        template_name,
        instance_label=None,
        delivery_id=None,
    ):
        """
        Create a new GCE instance for a GitHub Actions runner.

        Args:
            registration_token (str): The GitHub Actions runner registration token.
            repo_url (str): The URL of the repository or organization.
            template_name (str): The name of the instance template to use.
            instance_label (str): Label to add to the Instance for Cost Tracking.
            delivery_id (str): The GitHub webhook delivery ID for log correlation.

        Returns:
            str: The name of the created instance.
        """
        instance_template_resource = self._get_template_name(template_name)
        if instance_template_resource:
            logger.info(
                "Found matching instance template: %s, delivery_id: %s",
                instance_template_resource.name,
                delivery_id,
            )
        else:
            logger.warning(
                "No matching instance template found for label '%s' in region %s. "
                "Skipping instance creation. delivery_id: %s",
                template_name,
                self.region,
                delivery_id,
            )
            return None

        # Name must start with a lowercase letter followed by up to 62 lowercase letters,
        # numbers, or hyphens, and cannot end with a hyphen.
        instance_uuid = uuid.uuid4().hex[:16]
        if instance_template_resource.name.startswith("dependabot"):
            instance_name = f"gcp-runner-dependabot-{instance_uuid}"
        else:
            instance_name = f"gcp-runner-{instance_uuid}"

        logger.info(
            "Creating GCE instance %s with template %s, delivery_id: %s",
            instance_name,
            instance_template_resource.self_link,
            delivery_id,
        )

        # Set instance name
        instance_resource = compute_v1.Instance()  # google.cloud.compute_v1.types.Instance
        instance_resource.name = instance_name

        if instance_label is not None:
            owner, repo = instance_label.split("/")
            instance_resource.labels = {
                "gha-owner": owner.lower(),
                "gha-repo": repo.lower(),
                "gha-runner": template_name
            }

        # Set metadata (startup script) - use shlex.quote to prevent command injection
        runner_group_flag = ""
        if self.github_runner_group:
            runner_group_flag = f" --runnergroup {shlex.quote(self.github_runner_group)}"

        startup_script = (
            "cd /actions-runner && "
            f"sudo -u runner ./config.sh --url {shlex.quote(repo_url)} "
            f"--token {shlex.quote(registration_token)} "
            f"--name {shlex.quote(instance_name)} "
            f"--labels {shlex.quote(template_name)} "
            f"{runner_group_flag} "
            "--ephemeral "
            "--unattended "
            "--no-default-labels "
            "--disableupdate && "
            "sudo -u runner ./run.sh"
        )
        metadata = compute_v1.Metadata()
        metadata.items = [
            compute_v1.Items(key="startup-script", value=startup_script),
            compute_v1.Items(key="vmDnsSetting", value="ZonalOnly"),
            compute_v1.Items(key="block-project-ssh-keys", value="true"),
        ]
        instance_resource.metadata = metadata

        # Create the request
        # https://docs.cloud.google.com/python/docs/reference/compute/latest/google.cloud.compute_v1.types.InsertInstanceRequest
        request = compute_v1.InsertInstanceRequest(
            project=self.project_id,
            zone=self.zone,
            instance_resource=instance_resource,
            source_instance_template=instance_template_resource.self_link
        )

        try:
            # https://docs.cloud.google.com/compute/docs/reference/rest/v1/instances/insert
            operation = self.instance_client.insert(request=request)
            logger.info(
                "Instance creation operation started: %s, delivery_id: %s",
                operation.name,
                delivery_id,
            )
        except Exception as e:
            logger.error(
                "Failed to create instance: %s, delivery_id: %s", e, delivery_id
            )
            raise

        # Wait for the operation: capacity errors (stockout, quota) are operation-level
        # errors, so returning here would report success for a VM that never existed.
        self._wait_for_insert(operation, instance_name, self.zone, delivery_id=delivery_id)
        logger.info(
            "Instance %s created in zone %s, delivery_id: %s",
            instance_name,
            self.zone,
            delivery_id,
        )
        return instance_name

    def _wait_for_insert(self, operation, instance_name, zone, delivery_id=None):
        """
        Block until the insert operation finishes and raise if it did not succeed.

        Raises:
            ZoneCapacityError: the zone is out of capacity/quota; another zone may work.
            InstanceCreationError: any other failure, including a timed-out wait.
        """
        try:
            operation.result(timeout=self.insert_timeout)
        except concurrent.futures.TimeoutError:
            # The operation may still complete; do NOT retry elsewhere or we could double-provision.
            message = (
                f"Instance creation for {instance_name} in zone {zone} timed out after "
                f"{self.insert_timeout}s (operation {getattr(operation, 'name', '?')}); not retrying"
            )
            logger.error("%s, delivery_id: %s", message, delivery_id)
            raise InstanceCreationError(message)
        except gapi_exceptions.GoogleAPICallError as e:
            errors = self._operation_errors(operation)
            retry_elsewhere, summary = classify_operation_errors(errors)
            if not summary:
                summary = str(e)
            code = str(getattr(errors[0], 'code', '') or '') if errors else ''
            if retry_elsewhere:
                logger.warning(
                    "Instance creation for %s in zone %s failed with a capacity error: %s, delivery_id: %s",
                    instance_name,
                    zone,
                    summary,
                    delivery_id,
                )
                raise ZoneCapacityError(
                    f"Zone {zone} has no capacity for {instance_name}: {summary}", code=code, zone=zone
                ) from e
            logger.error(
                "Instance creation for %s in zone %s failed: %s, delivery_id: %s",
                instance_name,
                zone,
                summary,
                delivery_id,
            )
            raise InstanceCreationError(
                f"Instance creation for {instance_name} in zone {zone} failed: {summary}"
            ) from e

        # Defensive: a DONE operation that carries errors but did not raise is still a failure.
        errors = self._operation_errors(operation)
        if errors:
            _, summary = classify_operation_errors(errors)
            logger.error(
                "Instance creation for %s in zone %s finished with errors: %s, delivery_id: %s",
                instance_name,
                zone,
                summary,
                delivery_id,
            )
            raise InstanceCreationError(
                f"Instance creation for {instance_name} in zone {zone} finished with errors: {summary}"
            )

    @staticmethod
    def _operation_errors(operation):
        """Return the list of compute_v1.Errors attached to an operation (empty if none)."""
        try:
            error = getattr(operation, 'error', None)
            errors = list(getattr(error, 'errors', None) or [])
        except Exception:
            return []
        # Guard against MagicMock-like objects in tests returning non-Errors items.
        return [e for e in errors if isinstance(getattr(e, 'code', None), str)]

    def delete_runner_instance(self, instance_name, delivery_id=None):
        """
        Delete a GCE instance.

        Args:
            instance_name (str): The name of the instance to delete.
            delivery_id (str): The GitHub webhook delivery ID for log correlation.
        """
        logger.info(
            "Deleting GCE instance %s, delivery_id: %s", instance_name, delivery_id
        )
        try:
            operation = self.instance_client.delete(
                project=self.project_id,
                zone=self.zone,
                instance=instance_name
            )
            logger.info(
                "Instance deletion operation started: %s, delivery_id: %s",
                operation.name,
                delivery_id,
            )
        except Exception as e:
            logger.error(
                "Failed to delete instance %s: %s, delivery_id: %s",
                instance_name,
                e,
                delivery_id,
            )
            raise
