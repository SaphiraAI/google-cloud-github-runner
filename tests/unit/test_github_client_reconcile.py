"""
Tests for the GitHub client helpers the reconciler relies on.
"""
from unittest.mock import MagicMock, patch

import pytest
import requests

from app.clients.github_client import GitHubClient


def _response(json_body, status=200, next_url=None):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = json_body
    response.links = {'next': {'url': next_url}} if next_url else {}
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f'{status}', response=response)
    else:
        response.raise_for_status.return_value = None
    return response


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv('GITHUB_APP_ID', '1')
    monkeypatch.setenv('GITHUB_INSTALLATION_ID', '2')
    monkeypatch.setenv('GITHUB_PRIVATE_KEY', 'key')
    return GitHubClient()


class TestListWorkflowJobs:
    @patch('app.clients.github_client.requests')
    def test_scans_queued_and_in_progress_runs_and_dedupes(self, mock_requests, client):
        """A run with one running job and queued siblings has status in_progress, not queued."""
        calls = {}

        def fake_get(url, headers=None, params=None, timeout=None):
            calls.setdefault(url, []).append(params)
            if url.endswith('/actions/runs'):
                if params['status'] == 'queued':
                    return _response({'workflow_runs': [{'id': 10}]})
                return _response({'workflow_runs': [{'id': 10}, {'id': 11}]})
            if url.endswith('/runs/10/jobs'):
                return _response({'jobs': [{'id': 100, 'status': 'queued'}]})
            if url.endswith('/runs/11/jobs'):
                return _response({'jobs': [{'id': 110, 'status': 'in_progress', 'runner_name': 'gcp-runner-110'},
                                           {'id': 111, 'status': 'queued'}]})
            raise AssertionError(url)

        mock_requests.get.side_effect = fake_get
        jobs = client.list_workflow_jobs('o/r', token='tok')

        assert sorted(j['id'] for j in jobs) == [100, 110, 111]
        assert len(calls['https://api.github.com/repos/o/r/actions/runs/10/jobs']) == 1, 'run 10 fetched once'
        assert calls['https://api.github.com/repos/o/r/actions/runs/10/jobs'][0]['filter'] == 'latest'
        statuses = [p['status'] for p in calls['https://api.github.com/repos/o/r/actions/runs']]
        assert statuses == ['queued', 'in_progress']

    @patch('app.clients.github_client.requests')
    def test_follows_link_pagination(self, mock_requests, client):
        page2 = 'https://api.github.com/repos/o/r/actions/runs?status=queued&page=2'

        def fake_get(url, headers=None, params=None, timeout=None):
            if url == 'https://api.github.com/repos/o/r/actions/runs':
                assert params['per_page'] == 100
                return _response({'workflow_runs': [{'id': 1}]}, next_url=page2)
            if url == page2:
                assert params is None
                return _response({'workflow_runs': [{'id': 2}]})
            if url.endswith('/jobs'):
                return _response({'jobs': [{'id': int(url.split('/')[-2]) * 10}]})
            raise AssertionError(url)

        mock_requests.get.side_effect = fake_get
        jobs = client.list_workflow_jobs('o/r', token='tok', run_statuses=('queued',))
        assert sorted(j['id'] for j in jobs) == [10, 20]

    @patch('app.clients.github_client.requests')
    def test_http_error_propagates(self, mock_requests, client):
        mock_requests.get.return_value = _response({'message': 'nope'}, status=403)
        with pytest.raises(requests.HTTPError):
            client.list_workflow_jobs('o/r', token='tok')


class TestGetWorkflowJob:
    @patch('app.clients.github_client.requests')
    def test_returns_job(self, mock_requests, client):
        mock_requests.get.return_value = _response({'id': 5, 'status': 'in_progress'})
        assert client.get_workflow_job('o/r', 5, token='tok')['status'] == 'in_progress'
        url = mock_requests.get.call_args.args[0]
        assert url == 'https://api.github.com/repos/o/r/actions/jobs/5'

    @patch('app.clients.github_client.requests')
    def test_404_returns_none(self, mock_requests, client):
        mock_requests.get.return_value = _response({'message': 'Not Found'}, status=404)
        assert client.get_workflow_job('o/r', 5, token='tok') is None


class TestRunners:
    @patch('app.clients.github_client.requests')
    def test_org_runners_endpoint(self, mock_requests, client):
        mock_requests.get.return_value = _response({'runners': [
            {'id': 1, 'name': 'gcp-runner-1', 'status': 'online', 'busy': True, 'labels': []}]})
        runners = client.list_runners(org_name='example-org', token='tok')
        assert runners == [{'id': 1, 'name': 'gcp-runner-1', 'status': 'online', 'busy': True}]
        assert mock_requests.get.call_args.args[0] == 'https://api.github.com/orgs/example-org/actions/runners'

    @patch('app.clients.github_client.requests')
    def test_repo_runners_endpoint(self, mock_requests, client):
        mock_requests.get.return_value = _response({'runners': []})
        client.list_runners(repo_name='o/r', token='tok')
        assert mock_requests.get.call_args.args[0] == 'https://api.github.com/repos/o/r/actions/runners'

    def test_runners_require_a_scope(self, client):
        with pytest.raises(ValueError):
            client.list_runners(token='tok')

    @patch('app.clients.github_client.requests')
    def test_delete_runner_success_and_gone(self, mock_requests, client):
        mock_requests.delete.return_value = _response(None, status=204)
        assert client.delete_runner(7, org_name='example-org', token='tok') is True
        assert mock_requests.delete.call_args.args[0] == 'https://api.github.com/orgs/example-org/actions/runners/7'
        mock_requests.delete.return_value = _response({'message': 'Not Found'}, status=404)
        assert client.delete_runner(7, repo_name='o/r', token='tok') is True

    @patch('app.clients.github_client.requests')
    def test_delete_busy_runner_raises(self, mock_requests, client):
        mock_requests.delete.return_value = _response({'message': 'Runner is busy'}, status=422)
        with pytest.raises(requests.HTTPError):
            client.delete_runner(7, org_name='example-org', token='tok')


class TestInstallationRepositories:
    @patch('app.clients.github_client.requests')
    def test_shapes_repositories(self, mock_requests, client):
        mock_requests.get.return_value = _response({'total_count': 1, 'repositories': [{
            'full_name': 'example-org/example-repo', 'html_url': 'https://github.com/example-org/example-repo',
            'owner': {'login': 'example-org', 'type': 'Organization', 'html_url': 'https://github.com/example-org'},
            'private': True,
        }]})
        repos = client.list_installation_repositories(token='tok')
        assert repos == [{
            'full_name': 'example-org/example-repo', 'html_url': 'https://github.com/example-org/example-repo',
            'owner': {'login': 'example-org', 'type': 'Organization', 'html_url': 'https://github.com/example-org'},
        }]
        assert mock_requests.get.call_args.args[0] == 'https://api.github.com/installation/repositories'


class TestReadRetries:
    @patch('app.clients.github_client.time.sleep')
    @patch('app.clients.github_client.requests')
    def test_502_is_retried_then_succeeds(self, mock_requests, sleep, client):
        mock_requests.get.side_effect = [_response({'message': 'Bad Gateway'}, status=502),
                                         _response({'id': 5, 'status': 'queued'})]
        assert client.get_workflow_job('o/r', 5, token='tok')['status'] == 'queued'
        assert mock_requests.get.call_count == 2
        sleep.assert_called_once()

    @patch('app.clients.github_client.time.sleep')
    @patch('app.clients.github_client.requests')
    def test_persistent_5xx_raises_after_retries(self, mock_requests, sleep, client):
        mock_requests.get.return_value = _response({'message': 'Bad Gateway'}, status=502)
        with pytest.raises(requests.HTTPError):
            client.list_runners(org_name='example-org', token='tok')
        assert mock_requests.get.call_count == 3

    @patch('app.clients.github_client.time.sleep')
    @patch('app.clients.github_client.requests')
    def test_4xx_is_not_retried(self, mock_requests, sleep, client):
        mock_requests.get.return_value = _response({'message': 'Forbidden'}, status=403)
        with pytest.raises(requests.HTTPError):
            client.list_runners(org_name='example-org', token='tok')
        assert mock_requests.get.call_count == 1
        sleep.assert_not_called()
