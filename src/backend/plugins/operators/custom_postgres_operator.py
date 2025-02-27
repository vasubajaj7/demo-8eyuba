"""
Custom PostgreSQL operator for Apache Airflow 2.X that extends the standard PostgresOperator with enhanced 
functionality for SQL execution, transaction management, error handling, and alerting to support the 
migration from Airflow 1.10.15 to Cloud Composer 2.
"""

import os
import logging
from typing import Dict, List, Optional, Union, Any

import sqlparse  # sqlparse v0.4.2
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type  # tenacity v6.2.0+

from airflow.models.baseoperator import BaseOperator  # airflow v2.0.0+
from airflow.providers.postgres.operators.postgres import PostgresOperator  # airflow.providers.postgres v2.0.0+
from airflow.exceptions import AirflowException  # airflow v2.0.0+

# Internal imports
from ..hooks.custom_postgres_hook import CustomPostgresHook, DEFAULT_POSTGRES_CONN_ID
from ...dags.utils.validation_utils import validate_input
from ...dags.utils.alert_utils import send_alert, AlertLevel

# Configure logging
logger = logging.getLogger('airflow.operators.custom_postgres_operator')

# Global constants
MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_SCHEMA = 'public'


def _validate_sql_syntax(sql: str) -> bool:
    """
    Validates SQL syntax using sqlparse.
    
    Args:
        sql: SQL query to validate
        
    Returns:
        bool: True if SQL is valid, raises ValueError otherwise
    """
    if sql is None or not isinstance(sql, str):
        raise ValueError("SQL query must be a non-empty string")
    
    try:
        # Parse the SQL to check for syntax errors
        parsed = sqlparse.parse(sql)
        
        # Check if parsing was successful
        if not parsed or len(parsed) == 0:
            raise ValueError("SQL parsing failed: empty or invalid SQL statement")
        
        return True
    except Exception as e:
        error_msg = f"SQL validation error: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)


class CustomPostgresOperator(PostgresOperator):
    """
    Enhanced PostgreSQL operator with additional functionality for SQL execution, 
    error handling, and alerting.
    
    This operator extends the standard PostgresOperator to provide improved error handling,
    SQL validation, transaction support, and alerting capabilities.
    
    Attributes:
        sql (str): SQL query to execute
        postgres_conn_id (str): Connection ID for PostgreSQL
        schema (str): Database schema
        parameters (dict): Parameters for SQL query
        autocommit (bool): Whether to autocommit SQL
        use_transaction (bool): Whether to use a transaction
        validate_sql (bool): Whether to validate SQL syntax
        retry_count (int): Number of retries on failure
        retry_delay (float): Delay between retries
        alert_on_error (bool): Whether to send alerts on errors
    """
    
    template_fields = ["sql", "parameters"]
    
    def __init__(
        self,
        sql: str,
        postgres_conn_id: str = DEFAULT_POSTGRES_CONN_ID,
        schema: str = DEFAULT_SCHEMA,
        parameters: Optional[Dict] = None,
        autocommit: bool = False,
        use_transaction: bool = False,
        validate_sql: bool = False,
        retry_count: int = MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        alert_on_error: bool = False,
        **kwargs
    ) -> None:
        """
        Initialize the CustomPostgresOperator with enhanced configurations.
        
        Args:
            sql: SQL query to execute
            postgres_conn_id: Connection ID for PostgreSQL
            schema: Database schema
            parameters: Parameters for SQL query
            autocommit: Whether to autocommit SQL
            use_transaction: Whether to use a transaction
            validate_sql: Whether to validate SQL syntax
            retry_count: Number of retries on failure
            retry_delay: Delay between retries in seconds
            alert_on_error: Whether to send alerts on errors
            **kwargs: Additional parameters to pass to BaseOperator
        """
        super().__init__(
            sql=sql,
            postgres_conn_id=postgres_conn_id,
            **kwargs
        )
        
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id or DEFAULT_POSTGRES_CONN_ID
        self.schema = schema or DEFAULT_SCHEMA
        self.parameters = parameters or {}
        self.autocommit = autocommit
        self.use_transaction = use_transaction
        self.validate_sql = validate_sql
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.alert_on_error = alert_on_error
        self.hook = None
        
        logger.info(f"Initialized CustomPostgresOperator with conn_id={postgres_conn_id}, "
                   f"schema={schema}, autocommit={autocommit}, use_transaction={use_transaction}")
    
    def get_hook(self) -> CustomPostgresHook:
        """
        Get a CustomPostgresHook instance.
        
        Returns:
            CustomPostgresHook: Configured PostgreSQL hook
        """
        if self.hook is None:
            self.hook = CustomPostgresHook(
                postgres_conn_id=self.postgres_conn_id,
                schema=self.schema
            )
        
        return self.hook
    
    def pre_execute(self, context: Dict) -> None:
        """
        Tasks to perform before executing SQL.
        
        Args:
            context: Task execution context
        """
        try:
            # Validate required parameters
            if not self.sql:
                raise ValueError("SQL query is required")
            
            hook = self.get_hook()
            
            # Test connection before executing
            if not hook.test_connection():
                raise AirflowException(f"Connection test failed for {self.postgres_conn_id}")
            
            # Validate SQL syntax if enabled
            if self.validate_sql:
                _validate_sql_syntax(self.sql)
            
            logger.info("Pre-execute validation completed successfully")
            
        except Exception as e:
            error_msg = f"Pre-execution validation failed: {str(e)}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
    
    def execute(self, context: Dict) -> List:
        """
        Execute the SQL query with enhanced error handling and features.
        
        Args:
            context: Task execution context
            
        Returns:
            List: Query results if any, otherwise None
            
        Raises:
            AirflowException: If execution fails
        """
        try:
            hook = self.get_hook()
            
            logger.info(f"Executing SQL: {self.sql}")
            
            # Validate SQL syntax if enabled
            if self.validate_sql:
                _validate_sql_syntax(self.sql)
            
            # Use transaction if enabled, otherwise regular execution
            if self.use_transaction:
                result = hook.run_transaction(
                    statements=[self.sql],
                    parameters=[self.parameters] if self.parameters else None
                )
                # run_transaction returns a list of results for each statement
                return result[0] if result else []
            else:
                result = hook.execute_query(
                    sql=self.sql,
                    parameters=self.parameters,
                    autocommit=self.autocommit
                )
                return result
                
        except Exception as e:
            error_msg = f"Failed to execute SQL: {str(e)}"
            logger.error(error_msg)
            
            # Send alert if enabled
            if self.alert_on_error:
                send_alert(
                    alert_level=AlertLevel.ERROR,
                    context=context,
                    exception=e
                )
            
            # Include task context in exception for better debugging
            task_info = {
                "task_id": self.task_id,
                "dag_id": context.get("dag").dag_id if context.get("dag") else "Unknown DAG",
                "execution_date": context.get("execution_date", "Unknown")
            }
            
            raise AirflowException(f"{error_msg}. Task: {task_info}")
    
    def post_execute(self, context: Dict, result: Optional[List] = None) -> None:
        """
        Tasks to perform after executing SQL.
        
        Args:
            context: Task execution context
            result: Query result
        """
        logger.info("Starting post-execution phase")
        
        # Log execution statistics
        if result:
            row_count = len(result)
            logger.info(f"Query executed successfully, returned {row_count} rows")
        else:
            logger.info("Query executed successfully, no results returned")
        
        logger.info("Post-execution completed")
    
    def on_kill(self) -> None:
        """
        Handle task instance being killed.
        """
        logger.info(f"Received kill signal for task {self.task_id}")
        
        if self.hook:
            # Close any open connections
            try:
                self.hook.close_conn()
                logger.info("Database connections closed")
            except Exception as e:
                logger.error(f"Error closing connections: {str(e)}")
        
        # Send alert about cancellation if alerts are enabled
        if self.alert_on_error:
            try:
                context = {
                    "task_id": self.task_id,
                    "status": "KILLED"
                }
                send_alert(
                    alert_level=AlertLevel.WARNING,
                    context=context
                )
            except Exception as e:
                logger.error(f"Failed to send kill alert: {str(e)}")
        
        logger.info("Kill operation completed")