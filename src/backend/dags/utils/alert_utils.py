"""
Utility module providing alerting functionality for Apache Airflow 2.X DAGs in Cloud Composer 2.

This module offers a comprehensive set of tools for sending alerts through various channels
(email, Slack, webhooks, GCP monitoring), configuring callback functions for DAG/task state 
changes, and managing alert templates and recipients.
"""

import os
import logging
import json
import enum
from datetime import datetime
import traceback
from typing import Dict, List, Optional, Any, Union, Callable

# Airflow imports
from airflow.models import DAG, BaseOperator, TaskInstance
from airflow.utils.email import send_email
from airflow.providers.slack.hooks.slack import SlackHook  # apache-airflow-providers-slack v2.0.0+
from airflow.providers.http.hooks.http import HttpHook  # apache-airflow-providers-http v2.0.0+
from airflow.providers.google.cloud.hooks.stackdriver import StackdriverHook  # apache-airflow-providers-google v2.0.0+

# For template rendering
from jinja2 import Template, Environment, BaseLoader

# Import GCP utility for secret management
from .gcp_utils import get_secret

# Set up logging
logger = logging.getLogger('airflow.utils.alerts')

# Default configuration from environment variables or fallbacks
DEFAULT_ALERT_EMAIL = os.environ.get('AIRFLOW_ALERT_EMAIL', 'airflow@example.com')
DEFAULT_SLACK_CHANNEL = os.environ.get('AIRFLOW_SLACK_CHANNEL', '#airflow-alerts')
DEFAULT_WEBHOOK_URL = os.environ.get('AIRFLOW_WEBHOOK_URL', '')
DEFAULT_ALERT_CONNECTION_ID = os.environ.get('AIRFLOW_ALERT_CONNECTION_ID', 'airflow_alert')
DEFAULT_ALERT_PROJECT_ID = os.environ.get('GCP_PROJECT', '')

# Default alert templates
DEFAULT_ALERT_SUBJECT_TEMPLATE = 'Airflow Alert: {{ dag_id }} - {{ task_id }} - {{ execution_date }} - {{ status }}'
DEFAULT_ALERT_BODY_TEMPLATE = '''
<h3>Airflow Alert: {{ status }}</h3>
<p><strong>DAG</strong>: {{ dag_id }}</p>
<p><strong>Task</strong>: {{ task_id }}</p>
<p><strong>Execution Date</strong>: {{ execution_date }}</p>
<p><strong>Status</strong>: {{ status }}</p>
{% if exception %}
<p><strong>Exception</strong>:</p>
<pre>{{ exception }}</pre>
{% endif %}
<p><strong>Log URL</strong>: <a href="{{ log_url }}">View Logs</a></p>
'''


class AlertLevel(enum.Enum):
    """
    Enumeration of alert levels for standardized severity indication.
    """
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    
    @staticmethod
    def get_numeric_value(level):
        """
        Get numeric value of alert level for metric reporting.
        
        Args:
            level: AlertLevel enum value
        
        Returns:
            int: Numeric value (0-3) corresponding to alert level
        """
        if level == AlertLevel.INFO:
            return 0
        elif level == AlertLevel.WARNING:
            return 1
        elif level == AlertLevel.ERROR:
            return 2
        elif level == AlertLevel.CRITICAL:
            return 3
        return 0  # Default to INFO if not recognized
    
    @staticmethod
    def from_string(level_string: str):
        """
        Convert string to AlertLevel enum.
        
        Args:
            level_string: String representation of alert level
        
        Returns:
            AlertLevel: Corresponding AlertLevel enum value
        """
        try:
            return AlertLevel[level_string.upper()]
        except (KeyError, AttributeError):
            return AlertLevel.INFO  # Default to INFO if not matched


def render_template(template: str, context: Dict) -> str:
    """
    Render a template string with context variables.
    
    Args:
        template: Template string to render
        context: Dictionary of variables to apply to the template
    
    Returns:
        str: Rendered template with context values applied
    """
    try:
        env = Environment(loader=BaseLoader())
        template_obj = env.from_string(template)
        return template_obj.render(**context)
    except Exception as e:
        logger.error(f"Failed to render template: {str(e)}")
        return template  # Return the original template on failure


def format_exception(exception: Optional[Exception]) -> str:
    """
    Format exception for inclusion in alert messages.
    
    Args:
        exception: Exception object to format
    
    Returns:
        str: Formatted exception string with traceback
    """
    if not exception:
        return ""
    
    try:
        tb_lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
        formatted_tb = "".join(tb_lines)
        
        # Limit traceback length to prevent huge messages
        max_length = 2000
        if len(formatted_tb) > max_length:
            formatted_tb = formatted_tb[:max_length] + "...[truncated]"
            
        return formatted_tb
    except Exception as e:
        logger.error(f"Failed to format exception: {str(e)}")
        return str(exception)


def get_alert_context(context: Dict, status: str, exception: Optional[Exception] = None) -> Dict:
    """
    Generate context dictionary for alert templates.
    
    Args:
        context: The task context dictionary
        status: Status string (success, failure, etc.)
        exception: Optional exception object
    
    Returns:
        dict: Alert context with DAG, task, and status information
    """
    alert_context = {
        'status': status,
        'timestamp': datetime.now().isoformat(),
    }
    
    # Extract key information from context
    try:
        if 'dag' in context:
            alert_context['dag_id'] = context['dag'].dag_id
        elif 'dag_id' in context:
            alert_context['dag_id'] = context['dag_id']
        else:
            alert_context['dag_id'] = 'unknown_dag'
            
        if 'task' in context:
            alert_context['task_id'] = context['task'].task_id
        elif 'task_id' in context:
            alert_context['task_id'] = context['task_id']
        else:
            alert_context['task_id'] = 'unknown_task'
            
        if 'execution_date' in context:
            alert_context['execution_date'] = context['execution_date'].isoformat()
        
        # Format exception
        if exception:
            alert_context['exception'] = format_exception(exception)
        elif 'exception' in context:
            alert_context['exception'] = format_exception(context['exception'])
        
        # Get task instance log URL if available
        if 'task_instance' in context:
            ti = context['task_instance']
            log_url = ti.log_url if hasattr(ti, 'log_url') else None
            if log_url:
                alert_context['log_url'] = log_url
    except Exception as e:
        logger.error(f"Error creating alert context: {str(e)}")
    
    # Add any other context values that don't conflict
    for key, value in context.items():
        if key not in alert_context and not callable(value) and not key.startswith('_'):
            try:
                # Try to make sure values are serializable
                json.dumps({key: value})
                alert_context[key] = value
            except (TypeError, OverflowError):
                # Skip values that can't be easily serialized
                pass
                
    return alert_context


def send_email_alert(
    subject: str,
    body: str,
    to: Optional[List[str]] = None,
    cc: Optional[List[str]] = None,
    bcc: Optional[List[str]] = None,
    conn_params: Optional[Dict] = None
) -> bool:
    """
    Send an alert via email.
    
    Args:
        subject: Email subject line
        body: Email body content (HTML)
        to: List of recipient email addresses
        cc: List of CC recipient email addresses
        bcc: List of BCC recipient email addresses
        conn_params: Additional email connection parameters
    
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    if not to:
        to = [DEFAULT_ALERT_EMAIL]
    
    params = {}
    if conn_params:
        params.update(conn_params)
    
    try:
        logger.info(f"Sending email alert to {to}")
        send_email(
            to=to,
            subject=subject,
            html_content=body,
            cc=cc,
            bcc=bcc,
            **params
        )
        logger.info("Email alert sent successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to send email alert: {str(e)}")
        return False


def send_slack_alert(
    message: str,
    channel: Optional[str] = None,
    username: Optional[str] = None,
    conn_id: Optional[str] = None,
    attachments: Optional[List[Dict]] = None
) -> bool:
    """
    Send an alert to Slack.
    
    Args:
        message: Message to send
        channel: Slack channel (overrides default)
        username: Username to use for the message
        conn_id: Airflow connection ID for Slack
        attachments: Optional list of Slack attachments
    
    Returns:
        bool: True if slack message sent successfully, False otherwise
    """
    if not channel:
        channel = DEFAULT_SLACK_CHANNEL
    
    if not conn_id:
        conn_id = DEFAULT_ALERT_CONNECTION_ID
    
    try:
        logger.info(f"Sending Slack alert to {channel}")
        hook = SlackHook(slack_conn_id=conn_id)
        
        slack_msg = {
            'channel': channel,
            'text': message,
        }
        
        if username:
            slack_msg['username'] = username
            
        if attachments:
            slack_msg['attachments'] = attachments
            
        response = hook.call('chat.postMessage', json=slack_msg)
        
        if response and response.get('ok'):
            logger.info("Slack alert sent successfully")
            return True
        else:
            error = response.get('error', 'Unknown error') if response else 'No response'
            logger.error(f"Slack API error: {error}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {str(e)}")
        return False


def send_webhook_alert(
    webhook_url: Optional[str] = None,
    payload: Optional[Union[Dict, str]] = None,
    conn_id: Optional[str] = None,
    headers: Optional[Dict] = None
) -> bool:
    """
    Send an alert via webhook.
    
    Args:
        webhook_url: URL to send the webhook to
        payload: JSON payload to send
        conn_id: Airflow connection ID for HTTP
        headers: Optional HTTP headers
    
    Returns:
        bool: True if webhook call successful, False otherwise
    """
    if not webhook_url:
        webhook_url = DEFAULT_WEBHOOK_URL
        
    if not webhook_url:
        logger.error("No webhook URL provided")
        return False
        
    if not conn_id:
        conn_id = DEFAULT_ALERT_CONNECTION_ID
        
    if not payload:
        payload = {}
        
    if not headers:
        headers = {'Content-Type': 'application/json'}
    elif 'Content-Type' not in headers:
        headers['Content-Type'] = 'application/json'
        
    try:
        logger.info(f"Sending webhook alert to {webhook_url}")
        hook = HttpHook(method='POST', http_conn_id=conn_id)
        
        # Convert dict payload to JSON string if needed
        if isinstance(payload, dict):
            payload = json.dumps(payload)
            
        response = hook.run(
            endpoint=webhook_url,
            data=payload,
            headers=headers
        )
        
        status_code = response.status_code
        if 200 <= status_code < 300:
            logger.info(f"Webhook alert sent successfully (status: {status_code})")
            return True
        else:
            logger.error(f"Webhook error: Status code {status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to send webhook alert: {str(e)}")
        return False


def send_gcp_monitoring_alert(
    metric_name: str,
    metric_labels: Optional[Dict] = None,
    metric_value: int = 1,
    project_id: Optional[str] = None,
    conn_id: Optional[str] = None
) -> bool:
    """
    Send an alert to Google Cloud Monitoring.
    
    Args:
        metric_name: Name of the custom metric
        metric_labels: Labels to apply to the metric
        metric_value: Numeric value for the metric
        project_id: GCP project ID
        conn_id: Airflow connection ID for GCP
    
    Returns:
        bool: True if metric posted successfully, False otherwise
    """
    if not project_id:
        project_id = DEFAULT_ALERT_PROJECT_ID
        
    if not project_id:
        logger.error("No GCP project ID provided for monitoring alert")
        return False
        
    if not conn_id:
        conn_id = DEFAULT_ALERT_CONNECTION_ID
        
    if not metric_labels:
        metric_labels = {}
        
    try:
        logger.info(f"Sending GCP monitoring alert: {metric_name}")
        hook = StackdriverHook(gcp_conn_id=conn_id)
        
        # Make sure metric name follows Cloud Monitoring naming rules
        safe_metric_name = f"custom.googleapis.com/airflow/{metric_name}"
        
        series = [{
            'metric': {
                'type': safe_metric_name,
                'labels': metric_labels,
            },
            'resource': {
                'type': 'global',
                'labels': {
                    'project_id': project_id,
                },
            },
            'points': [{
                'value': {
                    'int64_value': metric_value,
                },
                'interval': {
                    'endTime': datetime.now().isoformat('T') + 'Z',
                },
            }],
        }]
        
        hook.write_time_series(
            project_id=project_id,
            time_series=series
        )
        
        logger.info("GCP monitoring alert sent successfully")
        return True
            
    except Exception as e:
        logger.error(f"Failed to send GCP monitoring alert: {str(e)}")
        return False


def send_alert(
    alert_level: Union[str, AlertLevel],
    context: Dict,
    exception: Optional[Exception] = None,
    email_to: Optional[List[str]] = None,
    slack_channel: Optional[str] = None,
    webhook_url: Optional[str] = None,
    subject_template: Optional[str] = None,
    body_template: Optional[str] = None,
    include_email: bool = True,
    include_slack: bool = True,
    include_webhook: bool = True,
    include_gcp: bool = True
) -> Dict:
    """
    Main function for sending alerts through configured channels.
    
    Args:
        alert_level: Severity level of the alert
        context: Context dictionary for the alert
        exception: Optional exception to include
        email_to: List of email recipients
        slack_channel: Slack channel to send to
        webhook_url: Webhook URL to send to
        subject_template: Template for email subject
        body_template: Template for email/message body
        include_email: Whether to send email alerts
        include_slack: Whether to send Slack alerts
        include_webhook: Whether to send webhook alerts
        include_gcp: Whether to send GCP monitoring alerts
    
    Returns:
        dict: Results of alert sending attempts by channel
    """
    # Convert string alert_level to enum if needed
    if isinstance(alert_level, str):
        alert_level = AlertLevel.from_string(alert_level)
        
    # Create alert context from the provided context
    alert_context = get_alert_context(context, str(alert_level.value), exception)
    
    # Render templates
    if not subject_template:
        subject_template = DEFAULT_ALERT_SUBJECT_TEMPLATE
        
    if not body_template:
        body_template = DEFAULT_ALERT_BODY_TEMPLATE
        
    subject = render_template(subject_template, alert_context)
    body = render_template(body_template, alert_context)
    
    # Track results for each channel
    results = {}
    
    # Send email alert
    if include_email:
        email_success = send_email_alert(
            subject=subject,
            body=body,
            to=email_to
        )
        results['email'] = email_success
        
    # Send Slack alert
    if include_slack:
        slack_success = send_slack_alert(
            message=body,
            channel=slack_channel
        )
        results['slack'] = slack_success
        
    # Send webhook alert
    if include_webhook:
        webhook_payload = {
            'alert_level': alert_level.value,
            'subject': subject,
            'body': body,
            'context': {k: v for k, v in alert_context.items() 
                       if not k.startswith('_') and k != 'exception'}
        }
        
        webhook_success = send_webhook_alert(
            webhook_url=webhook_url,
            payload=webhook_payload
        )
        results['webhook'] = webhook_success
        
    # Send GCP monitoring alert
    if include_gcp:
        metric_name = f"alert_{alert_level.value.lower()}"
        metric_labels = {
            'dag_id': alert_context.get('dag_id', 'unknown'),
            'task_id': alert_context.get('task_id', 'unknown'),
            'level': alert_level.value,
        }
        
        gcp_success = send_gcp_monitoring_alert(
            metric_name=metric_name,
            metric_labels=metric_labels,
            metric_value=AlertLevel.get_numeric_value(alert_level)
        )
        results['gcp'] = gcp_success
    
    logger.info(f"Alert sending results: {results}")
    return results


def on_failure_callback(context):
    """
    Callback function for task failures.
    
    Args:
        context: The task context dictionary
    """
    exception = context.get('exception')
    logger.info(f"Executing on_failure_callback for {context.get('task_instance')}")
    
    send_alert(
        alert_level=AlertLevel.ERROR,
        context=context,
        exception=exception
    )


def on_retry_callback(context):
    """
    Callback function for task retries.
    
    Args:
        context: The task context dictionary
    """
    exception = context.get('exception')
    logger.info(f"Executing on_retry_callback for {context.get('task_instance')}")
    
    send_alert(
        alert_level=AlertLevel.WARNING,
        context=context,
        exception=exception
    )


def on_success_callback(context):
    """
    Callback function for task successes.
    
    Args:
        context: The task context dictionary
    """
    logger.info(f"Executing on_success_callback for {context.get('task_instance')}")
    
    # Some users may not want success alerts by default
    # Check if the DAG or task has configured success alerts explicitly
    dag = context.get('dag')
    task = context.get('task')
    
    success_alerts_enabled = False
    if hasattr(task, 'params') and task.params.get('success_alerts_enabled', False):
        success_alerts_enabled = True
    elif hasattr(dag, 'user_defined_macros') and dag.user_defined_macros.get('success_alerts_enabled', False):
        success_alerts_enabled = True
        
    if success_alerts_enabled:
        send_alert(
            alert_level=AlertLevel.INFO,
            context=context
        )


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback function for SLA misses.
    
    Args:
        dag: The DAG that missed SLA
        task_list: List of tasks that missed SLA
        blocking_task_list: List of tasks blocking others
        slas: List of SLA instances
        blocking_tis: Dict of blocking task instances
    """
    logger.info(f"Executing on_sla_miss_callback for DAG {dag.dag_id}")
    
    # Create context for the alert
    context = {
        'dag': dag,
        'dag_id': dag.dag_id,
        'task_list': [t.task_id for t in task_list],
        'blocking_task_list': [t.task_id for t in blocking_task_list] if blocking_task_list else [],
        'slas': slas,
        'blocking_tis': blocking_tis,
        'timestamp': datetime.now().isoformat(),
    }
    
    send_alert(
        alert_level=AlertLevel.WARNING,
        context=context
    )


def configure_dag_alerts(
    dag: DAG,
    on_failure_alert: bool = True,
    on_retry_alert: bool = True,
    on_success_alert: bool = False,
    on_sla_miss_alert: bool = True,
    email_recipients: Optional[List[str]] = None,
    slack_channel: Optional[str] = None,
    webhook_url: Optional[str] = None
) -> DAG:
    """
    Configure alert callbacks and parameters for a DAG.
    
    Args:
        dag: The DAG to configure alerts for
        on_failure_alert: Whether to enable failure alerts
        on_retry_alert: Whether to enable retry alerts
        on_success_alert: Whether to enable success alerts
        on_sla_miss_alert: Whether to enable SLA miss alerts
        email_recipients: List of email recipients
        slack_channel: Slack channel to send alerts to
        webhook_url: Webhook URL to send alerts to
    
    Returns:
        DAG: Modified DAG with configured callbacks
    """
    logger.info(f"Configuring alerts for DAG: {dag.dag_id}")
    
    # Set callbacks based on configuration
    if on_failure_alert:
        dag.on_failure_callback = on_failure_callback
        
    if on_sla_miss_alert:
        dag.sla_miss_callback = on_sla_miss_callback
    
    # Initialize user_defined_macros if not present
    if not hasattr(dag, 'user_defined_macros') or dag.user_defined_macros is None:
        dag.user_defined_macros = {}
        
    # Configure alert settings in DAG's user_defined_macros
    dag.user_defined_macros.update({
        'alert_email_recipients': email_recipients or [DEFAULT_ALERT_EMAIL],
        'alert_slack_channel': slack_channel or DEFAULT_SLACK_CHANNEL,
        'alert_webhook_url': webhook_url or DEFAULT_WEBHOOK_URL,
        'success_alerts_enabled': on_success_alert,
        'retry_alerts_enabled': on_retry_alert,
        'failure_alerts_enabled': on_failure_alert,
        'sla_miss_alerts_enabled': on_sla_miss_alert,
    })
    
    return dag


def configure_task_alerts(
    task: BaseOperator,
    on_failure_alert: bool = True,
    on_retry_alert: bool = True,
    on_success_alert: bool = False,
    email_recipients: Optional[List[str]] = None,
    slack_channel: Optional[str] = None,
    webhook_url: Optional[str] = None
) -> BaseOperator:
    """
    Configure alert callbacks and parameters for a task.
    
    Args:
        task: The task to configure alerts for
        on_failure_alert: Whether to enable failure alerts
        on_retry_alert: Whether to enable retry alerts
        on_success_alert: Whether to enable success alerts
        email_recipients: List of email recipients
        slack_channel: Slack channel to send alerts to
        webhook_url: Webhook URL to send alerts to
    
    Returns:
        BaseOperator: Modified task with configured callbacks
    """
    logger.info(f"Configuring alerts for task: {task.task_id}")
    
    # Set callbacks based on configuration
    if on_failure_alert:
        task.on_failure_callback = on_failure_callback
        
    if on_retry_alert:
        task.on_retry_callback = on_retry_callback
        
    if on_success_alert:
        task.on_success_callback = on_success_callback
    
    # Initialize params if not present
    if not hasattr(task, 'params') or task.params is None:
        task.params = {}
        
    # Configure alert settings in task's params
    task.params.update({
        'alert_email_recipients': email_recipients,
        'alert_slack_channel': slack_channel,
        'alert_webhook_url': webhook_url,
        'success_alerts_enabled': on_success_alert,
        'retry_alerts_enabled': on_retry_alert,
        'failure_alerts_enabled': on_failure_alert,
    })
    
    return task


class AlertManager:
    """
    Class for managing alerts with persistent configuration.
    """
    
    def __init__(
        self,
        email_recipients: Optional[List[str]] = None,
        slack_channel: Optional[str] = None,
        webhook_url: Optional[str] = None,
        subject_template: Optional[str] = None,
        body_template: Optional[str] = None,
        include_email: bool = True,
        include_slack: bool = True,
        include_webhook: bool = True,
        include_gcp: bool = True
    ):
        """
        Initialize the AlertManager with configuration.
        
        Args:
            email_recipients: List of email recipients
            slack_channel: Slack channel to send alerts to
            webhook_url: Webhook URL to send alerts to
            subject_template: Template for email subject
            body_template: Template for email/message body
            include_email: Whether to enable email alerts
            include_slack: Whether to enable Slack alerts
            include_webhook: Whether to enable webhook alerts
            include_gcp: Whether to enable GCP monitoring alerts
        """
        self.email_recipients = email_recipients
        self.slack_channel = slack_channel or DEFAULT_SLACK_CHANNEL
        self.webhook_url = webhook_url or DEFAULT_WEBHOOK_URL
        self.subject_template = subject_template or DEFAULT_ALERT_SUBJECT_TEMPLATE
        self.body_template = body_template or DEFAULT_ALERT_BODY_TEMPLATE
        
        self.include_email = include_email
        self.include_slack = include_slack
        self.include_webhook = include_webhook
        self.include_gcp = include_gcp
        
        # Create a configuration dictionary
        self.config = {
            'email_recipients': self.email_recipients,
            'slack_channel': self.slack_channel,
            'webhook_url': self.webhook_url,
            'subject_template': self.subject_template,
            'body_template': self.body_template,
            'include_email': self.include_email,
            'include_slack': self.include_slack,
            'include_webhook': self.include_webhook,
            'include_gcp': self.include_gcp,
        }
        
        logger.info("AlertManager initialized with configuration")
        
    def send_alert(
        self,
        alert_level: Union[str, AlertLevel],
        context: Dict,
        exception: Optional[Exception] = None
    ) -> Dict:
        """
        Send alert using configured channels and templates.
        
        Args:
            alert_level: Severity level of the alert
            context: Context dictionary for the alert
            exception: Optional exception to include
        
        Returns:
            dict: Results of alert sending attempts by channel
        """
        return send_alert(
            alert_level=alert_level,
            context=context,
            exception=exception,
            email_to=self.email_recipients,
            slack_channel=self.slack_channel,
            webhook_url=self.webhook_url,
            subject_template=self.subject_template,
            body_template=self.body_template,
            include_email=self.include_email,
            include_slack=self.include_slack,
            include_webhook=self.include_webhook,
            include_gcp=self.include_gcp
        )
        
    def update_config(self, new_config: Dict) -> None:
        """
        Update AlertManager configuration.
        
        Args:
            new_config: Dictionary with new configuration values
        """
        # Update instance attributes with new values
        for key, value in new_config.items():
            if hasattr(self, key):
                setattr(self, key, value)
                self.config[key] = value
                
        logger.info(f"AlertManager configuration updated: {new_config.keys()}")
        
    def get_config(self) -> Dict:
        """
        Get current AlertManager configuration.
        
        Returns:
            dict: Current configuration dictionary
        """
        return self.config.copy()