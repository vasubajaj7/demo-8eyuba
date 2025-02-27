import logging
from airflow.decorators import task

# Set up module logger
logger = logging.getLogger(__name__)

# Export list to control what is publicly accessible
__all__ = ["example_taskflow", "example_gcp", "example_http", "example_postgres", "ExampleDAGTemplate"]


@task
def example_taskflow(param1: str, param2: int) -> dict:
    """Example function that demonstrates TaskFlow API usage in Airflow 2.X.
    
    This function shows how to create a task using the TaskFlow API, which is
    a new feature in Airflow 2.0 that simplifies the process of creating tasks.
    
    Args:
        param1: A string parameter for demonstration
        param2: An integer parameter for demonstration
        
    Returns:
        A dictionary containing processed results
    """
    logger.info("Starting TaskFlow function execution with params: %s, %d", param1, param2)
    
    # Example business logic
    result = {
        "input_received": {
            "param1": param1,
            "param2": param2
        },
        "processed_value": param1 * param2,
        "status": "success"
    }
    
    logger.info("TaskFlow function execution completed successfully")
    return result


@task
def example_gcp(bucket: str, object_name: str) -> dict:
    """Example function that demonstrates GCP integration using TaskFlow API.
    
    This function shows how to interact with Google Cloud Storage services
    using the TaskFlow API in Airflow 2.X.
    
    Args:
        bucket: The GCS bucket name
        object_name: The object name within the bucket
        
    Returns:
        A dictionary containing operation results and status
    """
    logger.info("Starting GCP operation for bucket: %s, object: %s", bucket, object_name)
    
    # Validate input parameters
    if not bucket or not object_name:
        raise ValueError("Both bucket and object_name must be provided")
    
    # Example GCP operation (in actual implementation, would use GCP hooks)
    # from airflow.providers.google.cloud.hooks.gcs import GCSHook
    # gcs_hook = GCSHook()
    # object_data = gcs_hook.download(bucket_name=bucket, object_name=object_name)
    
    # For demonstration purposes
    result = {
        "operation": "download",
        "bucket": bucket,
        "object": object_name,
        "status": "success",
        "bytes_processed": 1024  # Example value
    }
    
    logger.info("GCP operation completed successfully")
    return result


@task
def example_http(endpoint: str, headers: dict = None) -> dict:
    """Example function that demonstrates HTTP operations using TaskFlow API.
    
    This function shows how to perform HTTP operations using the
    TaskFlow API in Airflow 2.X.
    
    Args:
        endpoint: The HTTP endpoint to call
        headers: Optional HTTP headers as a dictionary
        
    Returns:
        A dictionary containing HTTP operation results
    """
    logger.info("Starting HTTP operation for endpoint: %s", endpoint)
    
    # Validate input parameters
    if not endpoint:
        raise ValueError("Endpoint must be provided")
    
    # Default headers if not provided
    if headers is None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    # Example HTTP operation (in actual implementation, would use HTTP hook)
    # from airflow.providers.http.hooks.http import HttpHook
    # http_hook = HttpHook(method='GET')
    # response = http_hook.run(endpoint, headers=headers)
    
    # For demonstration purposes
    result = {
        "endpoint": endpoint,
        "method": "GET",
        "headers": headers,
        "status_code": 200,  # Example value
        "response_size": 512  # Example value
    }
    
    logger.info("HTTP operation completed successfully")
    return result


@task
def example_postgres(table: str, data: dict) -> dict:
    """Example function that demonstrates PostgreSQL operations using TaskFlow API.
    
    This function shows how to interact with PostgreSQL databases
    using the TaskFlow API in Airflow 2.X.
    
    Args:
        table: The database table name
        data: The data to insert/update as a dictionary
        
    Returns:
        A dictionary containing database operation results
    """
    logger.info("Starting PostgreSQL operation for table: %s", table)
    
    # Validate input parameters
    if not table or not data:
        raise ValueError("Both table and data must be provided")
    
    # Example PostgreSQL operation (in actual implementation, would use PostgreSQL hook)
    # from airflow.providers.postgres.hooks.postgres import PostgresHook
    # pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # insert_sql = f"INSERT INTO {table} (column1, column2) VALUES (%s, %s)"
    # pg_hook.run(insert_sql, parameters=[data['value1'], data['value2']])
    
    # For demonstration purposes
    result = {
        "operation": "insert",
        "table": table,
        "rows_affected": 1,  # Example value
        "data_sample": data,
        "status": "success"
    }
    
    logger.info("PostgreSQL operation completed successfully")
    return result


class ExampleDAGTemplate:
    """Example class that demonstrates how to build DAG template patterns in Airflow 2.X.
    
    This class provides a reusable template for creating DAGs with consistent
    configuration and task patterns, showcasing best practices for Airflow 2.X.
    """
    
    def __init__(self, dag_id: str, description: str = None, default_args: dict = None,
                 schedule_interval: str = '@daily', catchup: bool = False, tags: list = None):
        """Initialize a new DAG template with configurable properties.
        
        Args:
            dag_id: Unique identifier for the DAG
            description: Optional description for the DAG
            default_args: Default arguments for tasks in the DAG
            schedule_interval: Schedule interval for the DAG
            catchup: Whether to catch up on missed DAG runs
            tags: Tags to categorize the DAG
        """
        self.dag_id = dag_id
        self.description = description or f"DAG template for {dag_id}"
        self.default_args = default_args or {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1
        }
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.tags = tags or ['example', 'template']
        
        logger.info("Created DAG template for %s", self.dag_id)
    
    def create_dag(self):
        """Creates a new DAG instance based on the template configuration.
        
        Returns:
            Configured DAG instance ready for use
        """
        from airflow import DAG
        from datetime import datetime
        
        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            description=self.description,
            schedule_interval=self.schedule_interval,
            start_date=datetime(2021, 1, 1),
            catchup=self.catchup,
            tags=self.tags
        ) as dag:
            # Example placeholder task - will be extended by other methods
            from airflow.operators.dummy import DummyOperator
            start = DummyOperator(task_id='start')
            end = DummyOperator(task_id='end')
            
            start >> end
        
        logger.info("Created DAG: %s", self.dag_id)
        return dag
    
    def add_example_taskflow(self, dag):
        """Adds example TaskFlow tasks to the DAG.
        
        Args:
            dag: The DAG to add TaskFlow tasks to
            
        Returns:
            DAG with added TaskFlow tasks
        """
        # Find the start and end tasks
        start_task = None
        end_task = None
        
        for task in dag.tasks:
            if task.task_id == 'start':
                start_task = task
            elif task.task_id == 'end':
                end_task = task
        
        if not start_task or not end_task:
            raise ValueError("DAG must have 'start' and 'end' tasks")
        
        # Add example TaskFlow tasks
        task1 = example_taskflow(param1="example", param2=5)
        task2 = example_gcp(bucket="example-bucket", object_name="example-object.txt")
        task3 = example_http(endpoint="https://api.example.com/data")
        task4 = example_postgres(table="example_table", data={"column1": "value1", "column2": "value2"})
        
        # Configure task dependencies
        start_task >> task1 >> task2 >> task3 >> task4 >> end_task
        
        logger.info("Added TaskFlow tasks to DAG: %s", dag.dag_id)
        return dag