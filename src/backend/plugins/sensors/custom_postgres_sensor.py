"""
Custom PostgreSQL sensors for Apache Airflow 2.X that provide enhanced database monitoring capabilities.
These sensors extend the standard Airflow sensors with advanced features for database table existence checking,
row counting, and value validation while ensuring compatibility with Cloud Composer 2.
"""

import logging
import re
from typing import Dict, Optional, Union

# Third-party imports
from airflow.exceptions import AirflowException  # apache-airflow v2.0.0+
from airflow.sensors.base import BaseSensorOperator  # apache-airflow v2.0.0+
from airflow.utils.decorators import apply_defaults  # apache-airflow v2.0.0+

# Internal module imports
from ...dags.utils import alert_utils  # Import alert_utils module
from ...dags.utils import db_utils  # Import db_utils module
from ...dags.utils import validation_utils  # Import validation_utils module
from ..hooks.custom_postgres_hook import CustomPostgresHook  # Import CustomPostgresHook class
from alert_utils import AlertLevel  # Import AlertLevel class
from alert_utils import send_alert  # Import send_alert function
from db_utils import DEFAULT_SCHEMA  # Import DEFAULT_SCHEMA constant
from validation_utils import validate_sensor_args  # Import validate_sensor_args function

# Configure logger
logger = logging.getLogger('airflow.sensors.custom_postgres_sensor')

# Global constants
DEFAULT_POSTGRES_CONN_ID = 'postgres_default'
DEFAULT_SCHEMA = db_utils.DEFAULT_SCHEMA


def _get_hook(postgres_conn_id: str, schema: str, use_persistent_connection: bool) -> CustomPostgresHook:
    """
    Helper function to instantiate a CustomPostgresHook with appropriate parameters.

    Args:
        postgres_conn_id (str): Airflow connection ID for PostgreSQL.
        schema (str): Database schema to use.
        use_persistent_connection (bool): Whether to use a persistent database connection.

    Returns:
        CustomPostgresHook: Instantiated PostgreSQL hook with specified parameters.
    """
    hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id, schema=schema,
                              use_persistent_connection=use_persistent_connection)
    logger.info(f"Created CustomPostgresHook with connection ID '{postgres_conn_id}' and schema '{schema}'")
    return hook


class CustomPostgresSensor(BaseSensorOperator):
    """
    Sensor that polls a PostgreSQL database and executes a custom SQL check.
    """

    template_fields = ('sql', 'params',)

    @apply_defaults
    def __init__(
            self,
            *,
            sql: str,
            postgres_conn_id: str = DEFAULT_POSTGRES_CONN_ID,
            schema: str = DEFAULT_SCHEMA,
            params: Optional[dict] = None,
            fail_on_error: bool = False,
            alert_on_error: bool = False,
            use_persistent_connection: bool = False,
            **kwargs: Dict,
    ) -> None:
        """
        Initialize the CustomPostgresSensor.

        Args:
            sql (str): SQL query to execute.
            postgres_conn_id (str): Airflow connection ID for PostgreSQL.
            schema (str): Database schema to use.
            params (Optional[dict]): Query parameters.
            fail_on_error (bool): Whether to fail the sensor on error.
            alert_on_error (bool): Whether to send an alert on error.
            use_persistent_connection (bool): Whether to use a persistent database connection.
            **kwargs (Dict): Additional keyword arguments for BaseSensorOperator.
        """
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.params = params or {}
        self.fail_on_error = fail_on_error
        self.alert_on_error = alert_on_error
        self.use_persistent_connection = use_persistent_connection

        # Validate arguments
        validate_sensor_args(fail_on_error=self.fail_on_error, alert_on_error=self.alert_on_error)

        logger.info(
            f"Initializing CustomPostgresSensor with sql='{self.sql}', "
            f"postgres_conn_id='{self.postgres_conn_id}', schema='{self.schema}', "
            f"params='{self.params}', fail_on_error={self.fail_on_error}, "
            f"alert_on_error={self.alert_on_error}, use_persistent_connection={self.use_persistent_connection}"
        )

    def get_hook(self) -> CustomPostgresHook:
        """
        Get a PostgreSQL hook.

        Returns:
            CustomPostgresHook: Configured PostgreSQL hook instance.
        """
        return _get_hook(postgres_conn_id=self.postgres_conn_id, schema=self.schema,
                         use_persistent_connection=self.use_persistent_connection)

    @apply_defaults
    def poke(self, context: Dict) -> bool:
        """
        Execute the SQL query and check if it returns records.

        Args:
            context (Dict): Airflow context dictionary.

        Returns:
            bool: True if records found, False otherwise.
        """
        hook = self.get_hook()
        try:
            logger.info(f"Executing SQL query: {self.sql} with params: {self.params}")
            records = hook.execute_query(sql=self.sql, parameters=self.params)
            if records:
                logger.info(f"Query returned {len(records)} records.")
                return True
            else:
                logger.info("Query returned no records.")
                return False
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error executing query: {error_message}")
            self.send_error_alert(exception=e, context=context)
            if self.fail_on_error:
                raise
            return False

    def send_error_alert(self, exception: Exception, context: Dict) -> None:
        """
        Send an alert when sensor encounters an error.

        Args:
            exception (Exception): The exception that occurred.
            context (Dict): Airflow context dictionary.
        """
        error_message = f"Sensor {self.task_id} in DAG {context['dag_id']} failed: {str(exception)}"
        dag_id = context.get('dag_id')
        task_id = context.get('task_instance').task_id if context.get('task_instance') else self.task_id

        send_alert(
            alert_level=AlertLevel.ERROR,
            context=context,
            exception=exception
        )


class CustomPostgresTableExistenceSensor(CustomPostgresSensor):
    """
    Sensor that checks if a PostgreSQL table exists.
    """

    @apply_defaults
    def __init__(
            self,
            *,
            table_name: str,
            postgres_conn_id: str = DEFAULT_POSTGRES_CONN_ID,
            schema: str = DEFAULT_SCHEMA,
            fail_on_error: bool = False,
            alert_on_error: bool = False,
            **kwargs: Dict,
    ) -> None:
        """
        Initialize the CustomPostgresTableExistenceSensor.

        Args:
            table_name (str): Name of the table to check.
            postgres_conn_id (str): Airflow connection ID for PostgreSQL.
            schema (str): Database schema to use.
            fail_on_error (bool): Whether to fail the sensor on error.
            alert_on_error (bool): Whether to send an alert on error.
            **kwargs (Dict): Additional keyword arguments for BaseSensorOperator.
        """
        self.table_name = table_name
        sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM   pg_catalog.pg_class c
                JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE  n.nspname = '{schema}'
                AND    c.relname = '{table_name}'
                AND    c.relkind = 'r'    
            );
        """
        super().__init__(sql=sql, postgres_conn_id=postgres_conn_id, schema=schema,
                         fail_on_error=fail_on_error, alert_on_error=alert_on_error, **kwargs)
        logger.info(f"Initializing CustomPostgresTableExistenceSensor for table '{table_name}'")

    @apply_defaults
    def poke(self, context: Dict) -> bool:
        """
        Check if the specified table exists.

        Args:
            context (Dict): Airflow context dictionary.

        Returns:
            bool: True if table exists, False otherwise.
        """
        table_exists = super().poke(context)
        logger.info(f"Table '{self.table_name}' exists: {table_exists}")
        return table_exists


class CustomPostgresRowCountSensor(CustomPostgresSensor):
    """
    Sensor that checks if a PostgreSQL table has at least N rows.
    """

    @apply_defaults
    def __init__(
            self,
            *,
            table_name: str,
            min_rows: int = 1,
            where_clause: str = "",
            postgres_conn_id: str = DEFAULT_POSTGRES_CONN_ID,
            schema: str = DEFAULT_SCHEMA,
            fail_on_error: bool = False,
            alert_on_error: bool = False,
            **kwargs: Dict,
    ) -> None:
        """
        Initialize the CustomPostgresRowCountSensor.

        Args:
            table_name (str): Name of the table to check.
            min_rows (int): Minimum number of rows required.
            where_clause (str): Optional WHERE clause to filter rows.
            postgres_conn_id (str): Airflow connection ID for PostgreSQL.
            schema (str): Database schema to use.
            fail_on_error (bool): Whether to fail the sensor on error.
            alert_on_error (bool): Whether to send an alert on error.
            **kwargs (Dict): Additional keyword arguments for BaseSensorOperator.
        """
        self.table_name = table_name
        self.min_rows = min_rows
        self.where_clause = where_clause

        sql = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"

        super().__init__(sql=sql, postgres_conn_id=postgres_conn_id, schema=schema,
                         fail_on_error=fail_on_error, alert_on_error=alert_on_error, **kwargs)
        logger.info(
            f"Initializing CustomPostgresRowCountSensor for table '{table_name}' with min_rows={min_rows}"
        )

    @apply_defaults
    def poke(self, context: Dict) -> bool:
        """
        Check if the table has at least min_rows rows.

        Args:
            context (Dict): Airflow context dictionary.

        Returns:
            bool: True if enough rows exist, False otherwise.
        """
        hook = self.get_hook()
        try:
            row_count = hook.execute_query(sql=self.sql)[0][0]
            has_enough_rows = row_count >= self.min_rows
            logger.info(
                f"Table '{self.table_name}' has {row_count} rows, "
                f"required minimum is {self.min_rows}: {has_enough_rows}"
            )
            return has_enough_rows
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error executing query: {error_message}")
            self.send_error_alert(exception=e, context=context)
            if self.fail_on_error:
                raise
            return False


class CustomPostgresValueCheckSensor(CustomPostgresSensor):
    """
    Sensor that checks if a value from a PostgreSQL query matches an expected value.
    """

    @apply_defaults
    def __init__(
            self,
            *,
            sql: str,
            expected_value: Union[str, int, float, bool],
            exact_match: bool = True,
            postgres_conn_id: str = DEFAULT_POSTGRES_CONN_ID,
            schema: str = DEFAULT_SCHEMA,
            params: Optional[dict] = None,
            fail_on_error: bool = False,
            alert_on_error: bool = False,
            **kwargs: Dict,
    ) -> None:
        """
        Initialize the CustomPostgresValueCheckSensor.

        Args:
            sql (str): SQL query to execute.
            expected_value (Union[str, int, float, bool]): Expected value to match.
            exact_match (bool): Whether to perform an exact match or containment check.
            postgres_conn_id (str): Airflow connection ID for PostgreSQL.
            schema (str): Database schema to use.
            params (Optional[dict]): Query parameters.
            fail_on_error (bool): Whether to fail the sensor on error.
            alert_on_error (bool): Whether to send an alert on error.
            **kwargs (Dict): Additional keyword arguments for BaseSensorOperator.
        """
        self.expected_value = expected_value
        self.exact_match = exact_match
        super().__init__(sql=sql, postgres_conn_id=postgres_conn_id, schema=schema, params=params,
                         fail_on_error=fail_on_error, alert_on_error=alert_on_error, **kwargs)
        logger.info(
            f"Initializing CustomPostgresValueCheckSensor with sql='{sql}', "
            f"expected_value='{expected_value}', exact_match={exact_match}"
        )

    @apply_defaults
    def poke(self, context: Dict) -> bool:
        """
        Check if the query result matches the expected value.

        Args:
            context (Dict): Airflow context dictionary.

        Returns:
            bool: True if value matches expectation, False otherwise.
        """
        hook = self.get_hook()
        try:
            records = hook.execute_query(sql=self.sql, parameters=self.params)
            if not records:
                logger.info("Query returned no records.")
                return False

            actual_value = records[0][0]  # Assuming first column of first row

            if self.exact_match:
                value_matches = actual_value == self.expected_value
            else:
                # Perform containment check (e.g., substring)
                value_matches = str(self.expected_value) in str(actual_value)

            logger.info(
                f"Query returned value '{actual_value}', "
                f"expected value is '{self.expected_value}', match: {value_matches}"
            )
            return value_matches
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error executing query: {error_message}")
            self.send_error_alert(exception=e, context=context)
            if self.fail_on_error:
                raise
            return False