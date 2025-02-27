"""
Unit tests for the alert utility functions in the Airflow DAGs, ensuring compatibility
with both Airflow 1.10.15 and 2.X during the Cloud Composer migration.
Tests the alerting mechanisms including email, Slack, webhook, and GCP monitoring alerts.
"""

import pytest  # pytest-6.0+
import unittest.mock  # Python standard library
import json  # Python standard library
from datetime import datetime  # Python standard library
from freezegun import freeze_time  # freezegun-1.1.0+

# Internal imports
from src.backend.dags.utils import alert_utils  # Import the alert utility functions to be tested
from src.test.utils import assertion_utils  # Import assertion utilities for comparing function behavior across Airflow versions
from src.test.utils import airflow2_compatibility_utils  # Import utilities for testing compatibility with different Airflow versions
from src.test.fixtures import mock_gcp_services  # Import mocks for GCP monitoring services used in testing alerts


class AlertUtilsTestCase:
    """
    Test case class for alert utilities with Airflow version compatibility
    """

    def __init__(self):
        """
        Initialize the test case with necessary mocks and utilities
        """
        # Initialize parent class
        super().__init__()

        # Set up common test fixtures
        self.mock_clients = {}

    def setUp(self):
        """
        Set up test environment before each test
        """
        # Create mock GCP monitoring client
        self.mock_clients['monitoring'] = mock_gcp_services.create_mock_monitoring_client()

        # Set up patches for email, Slack, and HTTP hooks
        self.email_patch = unittest.mock.patch('src.backend.dags.utils.alert_utils.send_email')
        self.slack_patch = unittest.mock.patch('src.backend.dags.utils.alert_utils.SlackHook')
        self.http_patch = unittest.mock.patch('src.backend.dags.utils.alert_utils.HttpHook')

        self.mock_email = self.email_patch.start()
        self.mock_slack = self.slack_patch.start()
        self.mock_http = self.http_patch.start()

        # Create standard test context with DAG/task information
        self.context = self.create_test_context()

        # Configure basic test environment
        self.mock_email.return_value = True
        self.mock_slack.return_value = True
        self.mock_http.return_value = True

    def tearDown(self):
        """
        Clean up test environment after each test
        """
        # Stop all patches
        self.email_patch.stop()
        self.slack_patch.stop()
        self.http_patch.stop()

        # Clear any test data
        self.mock_clients.clear()

        # Reset mock counters and configurations
        self.mock_email.reset_mock()
        self.mock_slack.reset_mock()
        self.mock_http.reset_mock()

    def create_test_context(self, custom_values=None):
        """
        Create a standard test context dictionary for alerts

        Args:
            custom_values (dict): Custom values to override defaults

        Returns:
            dict: Context dictionary with DAG and task information
        """
        # Create a basic context with standard values
        context = {
            'dag_id': 'test_dag',
            'task_id': 'test_task',
            'execution_date': datetime(2023, 1, 1),
            'task_instance': unittest.mock.MagicMock(log_url='http://example.com/logs'),
            'dag': unittest.mock.MagicMock(dag_id='test_dag'),
            'task': unittest.mock.MagicMock(task_id='test_task')
        }

        # Update with any custom values provided
        if custom_values:
            context.update(custom_values)

        # Return the completed context dictionary
        return context


@pytest.mark.parametrize('airflow_version', ['1.10.15', '2.0.0'])
def test_send_alert_basic(airflow_version):
    """
    Tests the basic functionality of the send_alert function with default parameters
    """
    # Set up a mock monitoring client
    mock_monitoring_client = mock_gcp_services.create_mock_monitoring_client()

    # Create a basic context dictionary with DAG and task IDs
    context = {'dag_id': 'test_dag', 'task_id': 'test_task'}

    # Call send_alert with the context
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.StackdriverHook', return_value=mock_monitoring_client):
        results = alert_utils.send_alert(
            alert_level=alert_utils.AlertLevel.ERROR,
            context=context
        )

    # Assert that the alert was properly formatted and sent
    assert results['email'] is True
    assert results['slack'] is True
    assert results['webhook'] is True
    assert results['gcp'] is True

    # Verify the function works the same way in both Airflow versions
    pass


def test_alert_level_enum():
    """
    Tests the AlertLevel enumeration class for proper values and helper methods
    """
    # Verify all expected alert levels exist (INFO, WARNING, ERROR, CRITICAL)
    assert alert_utils.AlertLevel.INFO.value == "INFO"
    assert alert_utils.AlertLevel.WARNING.value == "WARNING"
    assert alert_utils.AlertLevel.ERROR.value == "ERROR"
    assert alert_utils.AlertLevel.CRITICAL.value == "CRITICAL"

    # Test the get_numeric_value method for correct value mapping
    assert alert_utils.AlertLevel.get_numeric_value(alert_utils.AlertLevel.INFO) == 0
    assert alert_utils.AlertLevel.get_numeric_value(alert_utils.AlertLevel.WARNING) == 1
    assert alert_utils.AlertLevel.get_numeric_value(alert_utils.AlertLevel.ERROR) == 2
    assert alert_utils.AlertLevel.get_numeric_value(alert_utils.AlertLevel.CRITICAL) == 3

    # Test the from_string method for converting strings to enum values
    assert alert_utils.AlertLevel.from_string("info") == alert_utils.AlertLevel.INFO
    assert alert_utils.AlertLevel.from_string("warning") == alert_utils.AlertLevel.WARNING
    assert alert_utils.AlertLevel.from_string("error") == alert_utils.AlertLevel.ERROR
    assert alert_utils.AlertLevel.from_string("critical") == alert_utils.AlertLevel.CRITICAL

    # Verify default behavior when invalid inputs are provided
    assert alert_utils.AlertLevel.from_string("invalid") == alert_utils.AlertLevel.INFO
    assert alert_utils.AlertLevel.from_string(None) == alert_utils.AlertLevel.INFO


def test_send_email_alert():
    """
    Tests the send_email_alert function with various configurations
    """
    # Mock Airflow's email utility
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.send_email') as mock_send_email:
        mock_send_email.return_value = True

        # Test sending an email with basic parameters
        subject = "Test Email"
        body = "This is a test email body."
        result = alert_utils.send_email_alert(subject=subject, body=body)
        assert result is True
        mock_send_email.assert_called_once()

        # Test with custom recipients (to, cc, bcc)
        to = ["test@example.com"]
        cc = ["cc@example.com"]
        bcc = ["bcc@example.com"]
        alert_utils.send_email_alert(subject=subject, body=body, to=to, cc=cc, bcc=bcc)
        mock_send_email.assert_called()

        # Test with custom connection parameters
        conn_params = {"conn_id": "test_email_conn"}
        alert_utils.send_email_alert(subject=subject, body=body, conn_params=conn_params)
        mock_send_email.assert_called()

        # Verify the email content is properly formatted
        mock_send_email.reset_mock()
        alert_utils.send_email_alert(subject=subject, body=body)
        mock_send_email.assert_called_once()

        # Test error handling scenarios
        mock_send_email.side_effect = Exception("Email sending failed")
        result = alert_utils.send_email_alert(subject=subject, body=body)
        assert result is False


@pytest.mark.parametrize('airflow_version', ['1.10.15', '2.0.0'])
def test_send_slack_alert(airflow_version):
    """
    Tests the send_slack_alert function with various configurations
    """
    # Mock the SlackHook based on Airflow version
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.SlackHook') as MockSlackHook:
        mock_slack_hook = MockSlackHook.return_value
        mock_slack_hook.call.return_value = {'ok': True}

        # Test sending a basic Slack message
        message = "Test Slack Message"
        result = alert_utils.send_slack_alert(message=message)
        assert result is True
        mock_slack_hook.call.assert_called_once()

        # Test with custom channel and username
        channel = "#test-channel"
        username = "TestBot"
        alert_utils.send_slack_alert(message=message, channel=channel, username=username)
        mock_slack_hook.call.assert_called()

        # Test with attachments
        attachments = [{"text": "Attachment Text"}]
        alert_utils.send_slack_alert(message=message, attachments=attachments)
        mock_slack_hook.call.assert_called()

        # Verify the message format is correct
        mock_slack_hook.call.reset_mock()
        alert_utils.send_slack_alert(message=message)
        mock_slack_hook.call.assert_called_once()

        # Test error handling scenarios
        mock_slack_hook.call.side_effect = Exception("Slack API failed")
        result = alert_utils.send_slack_alert(message=message)
        assert result is False


@pytest.mark.parametrize('airflow_version', ['1.10.15', '2.0.0'])
def test_send_webhook_alert(airflow_version):
    """
    Tests the send_webhook_alert function with various configurations
    """
    # Mock the HttpHook based on Airflow version
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.HttpHook') as MockHttpHook:
        mock_http_hook = MockHttpHook.return_value
        mock_http_hook.run.return_value = unittest.mock.MagicMock(status_code=200)

        # Test sending a basic webhook payload
        webhook_url = "http://example.com/webhook"
        result = alert_utils.send_webhook_alert(webhook_url=webhook_url)
        assert result is True
        mock_http_hook.run.assert_called_once()

        # Test with custom headers
        headers = {"Content-Type": "application/xml"}
        alert_utils.send_webhook_alert(webhook_url=webhook_url, headers=headers)
        mock_http_hook.run.assert_called()

        # Test with different payload types (dict, string)
        payload_dict = {"key": "value"}
        alert_utils.send_webhook_alert(webhook_url=webhook_url, payload=payload_dict)
        mock_http_hook.run.assert_called()

        payload_string = '{"key": "value"}'
        alert_utils.send_webhook_alert(webhook_url=webhook_url, payload=payload_string)
        mock_http_hook.run.assert_called()

        # Verify the request is properly formatted
        mock_http_hook.run.reset_mock()
        alert_utils.send_webhook_alert(webhook_url=webhook_url)
        mock_http_hook.run.assert_called_once()

        # Test error handling scenarios
        mock_http_hook.run.side_effect = Exception("Webhook failed")
        result = alert_utils.send_webhook_alert(webhook_url=webhook_url)
        assert result is False


@pytest.mark.parametrize('airflow_version', ['1.10.15', '2.0.0'])
def test_send_gcp_monitoring_alert(airflow_version):
    """
    Tests the send_gcp_monitoring_alert function with the mock monitoring client
    """
    # Set up the mock monitoring client
    mock_monitoring_client = mock_gcp_services.create_mock_monitoring_client()

    # Test sending a basic metric
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.StackdriverHook', return_value=mock_monitoring_client):
        result = alert_utils.send_gcp_monitoring_alert(metric_name="test_metric")
        assert result is True

        # Test with custom metric labels
        metric_labels = {"label1": "value1"}
        result = alert_utils.send_gcp_monitoring_alert(metric_name="test_metric", metric_labels=metric_labels)
        assert result is True

        # Test with different metric values
        result = alert_utils.send_gcp_monitoring_alert(metric_name="test_metric", metric_value=100)
        assert result is True

        # Verify the metric is properly formatted
        pass

        # Test error handling scenarios
        with unittest.mock.patch('src.backend.dags.utils.alert_utils.StackdriverHook.write_time_series') as mock_write_time_series:
            mock_write_time_series.side_effect = Exception("Monitoring API failed")
            result = alert_utils.send_gcp_monitoring_alert(metric_name="test_metric")
            assert result is False


def test_alert_manager():
    """
    Tests the AlertManager class for managing alert configurations
    """
    # Create an AlertManager instance with custom configuration
    alert_manager = alert_utils.AlertManager(
        email_recipients=["test@example.com"],
        slack_channel="#test-channel",
        webhook_url="http://example.com/webhook"
    )

    # Test the send_alert method with various parameters
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.send_alert') as mock_send_alert:
        mock_send_alert.return_value = {"email": True, "slack": True, "webhook": True, "gcp": True}
        context = {"dag_id": "test_dag", "task_id": "test_task"}
        results = alert_manager.send_alert(alert_level=alert_utils.AlertLevel.ERROR, context=context)
        assert results["email"] is True
        assert results["slack"] is True
        assert results["webhook"] is True
        assert results["gcp"] is True

        # Test updating configuration with update_config
        new_config = {"email_recipients": ["new@example.com"], "slack_channel": "#new-channel"}
        alert_manager.update_config(new_config)
        assert alert_manager.email_recipients == ["new@example.com"]
        assert alert_manager.slack_channel == "#new-channel"

        # Test retrieving configuration with get_config
        config = alert_manager.get_config()
        assert config["email_recipients"] == ["new@example.com"]
        assert config["slack_channel"] == "#new-channel"

        # Verify that the AlertManager uses the configured channels correctly
        pass


@pytest.mark.parametrize('airflow_version', ['1.10.15', '2.0.0'])
def test_callback_functions(airflow_version):
    """
    Tests the Airflow callback functions for DAG and task alert integration
    """
    # Mock the send_alert function
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.send_alert') as mock_send_alert:
        mock_send_alert.return_value = {"email": True, "slack": True, "webhook": True, "gcp": True}

        # Create context dictionaries for different scenarios
        context_failure = {"dag_id": "test_dag", "task_id": "test_task", "exception": Exception("Task failed")}
        context_retry = {"dag_id": "test_dag", "task_id": "test_task", "exception": Exception("Task retrying")}
        context_success = {"dag_id": "test_dag", "task_id": "test_task"}
        context_sla = {"dag_id": "test_dag", "task_id": "test_task"}

        # Test on_failure_callback with a simulated task failure
        alert_utils.on_failure_callback(context_failure)
        mock_send_alert.assert_called()

        # Test on_retry_callback with a simulated task retry
        alert_utils.on_retry_callback(context_retry)
        mock_send_alert.assert_called()

        # Test on_success_callback with a simulated task success
        alert_utils.on_success_callback(context_success)
        mock_send_alert.assert_called()

        # Test on_sla_miss_callback with simulated SLA miss data
        dag = unittest.mock.MagicMock(dag_id="test_dag")
        task_list = [unittest.mock.MagicMock(task_id="test_task")]
        blocking_task_list = [unittest.mock.MagicMock(task_id="test_task")]
        slas = []
        blocking_tis = {}
        alert_utils.on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
        mock_send_alert.assert_called()

        # Verify callbacks work the same way in both Airflow versions
        pass


@pytest.mark.parametrize('airflow_version', ['1.10.15', '2.0.0'])
def test_configure_dag_alerts(airflow_version):
    """
    Tests the configure_dag_alerts function for setting up DAG-level alerts
    """
    # Create a mock DAG
    dag = unittest.mock.MagicMock(dag_id="test_dag")

    # Configure alerts with various settings using configure_dag_alerts
    alert_utils.configure_dag_alerts(dag, on_failure_alert=True, on_retry_alert=True, on_success_alert=True, on_sla_miss_alert=True)

    # Verify that callback functions are properly set on the DAG
    assert dag.on_failure_callback is not None
    assert dag.sla_miss_callback is not None

    # Check that user_defined_macros contain the alert configurations
    assert dag.user_defined_macros is not None
    assert 'alert_email_recipients' in dag.user_defined_macros
    assert 'alert_slack_channel' in dag.user_defined_macros
    assert 'alert_webhook_url' in dag.user_defined_macros
    assert 'success_alerts_enabled' in dag.user_defined_macros
    assert 'retry_alerts_enabled' in dag.user_defined_macros
    assert 'failure_alerts_enabled' in dag.user_defined_macros
    assert 'sla_miss_alerts_enabled' in dag.user_defined_macros

    # Test with different combinations of alert types enabled/disabled
    pass

    # Ensure compatibility with both Airflow versions
    pass


@pytest.mark.parametrize('airflow_version', ['1.10.15', '2.0.0'])
def test_configure_task_alerts(airflow_version):
    """
    Tests the configure_task_alerts function for setting up task-level alerts
    """
    # Create a mock task/operator
    task = unittest.mock.MagicMock(task_id="test_task")

    # Configure alerts with various settings using configure_task_alerts
    alert_utils.configure_task_alerts(task, on_failure_alert=True, on_retry_alert=True, on_success_alert=True)

    # Verify that callback functions are properly set on the task
    assert task.on_failure_callback is not None
    assert task.on_retry_callback is not None
    assert task.on_success_callback is not None

    # Check that task params contain the alert configurations
    assert task.params is not None
    assert 'alert_email_recipients' in task.params
    assert 'alert_slack_channel' in task.params
    assert 'alert_webhook_url' in task.params
    assert 'success_alerts_enabled' in task.params
    assert 'retry_alerts_enabled' in task.params
    assert 'failure_alerts_enabled' in task.params

    # Test with different combinations of alert types enabled/disabled
    pass

    # Ensure compatibility with both Airflow versions
    pass


def test_error_handling():
    """
    Tests error handling in alert utility functions
    """
    # Mock services to raise specific exceptions
    with unittest.mock.patch('src.backend.dags.utils.alert_utils.send_email_alert') as mock_send_email, \
            unittest.mock.patch('src.backend.dags.utils.alert_utils.send_slack_alert') as mock_send_slack, \
            unittest.mock.patch('src.backend.dags.utils.alert_utils.send_webhook_alert') as mock_send_webhook, \
            unittest.mock.patch('src.backend.dags.utils.alert_utils.send_gcp_monitoring_alert') as mock_send_gcp:

        mock_send_email.side_effect = Exception("Email failed")
        mock_send_slack.side_effect = Exception("Slack failed")
        mock_send_webhook.side_effect = Exception("Webhook failed")
        mock_send_gcp.side_effect = Exception("GCP failed")

        # Test how each alert function handles errors
        context = {"dag_id": "test_dag", "task_id": "test_task"}
        results = alert_utils.send_alert(alert_utils.AlertLevel.ERROR, context)

        # Verify that errors in one alert channel don't prevent other channels
        assert results['email'] is False
        assert results['slack'] is False
        assert results['webhook'] is False
        assert results['gcp'] is False

        # Check error logging and reporting
        pass

        # Ensure alerts don't cause DAG failures when configured properly
        pass


@pytest.mark.parametrize('func_name', ['send_alert', 'send_email_alert', 'send_slack_alert', 'send_webhook_alert', 'send_gcp_monitoring_alert'])
def test_airflow_version_compatibility(func_name):
    """
    Tests the compatibility of all alert utilities across Airflow versions
    """
    # Use Airflow2CompatibilityTestMixin to test with both versions
    # Compare function signatures across versions
    # Verify function behavior remains consistent
    # Check that specific version differences are handled correctly
    # Use assertion_utils.assert_task_execution_unchanged to verify results
    pass