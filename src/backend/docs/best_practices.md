# Best Practices for Airflow 2.X on Cloud Composer 2

## Introduction

### Purpose

This document outlines the best practices for developing, migrating, and maintaining Directed Acyclic Graphs (DAGs) on Apache Airflow 2.X within the Cloud Composer 2 environment. It aims to provide clear, actionable guidance for developers, DevOps engineers, and administrators to ensure efficient, reliable, and secure workflow orchestration.

### Audience

This guide is intended for:

*   Airflow developers
*   DevOps engineers
*   System architects
*   QA engineers
*   Cloud administrators

### Document Scope

This document covers the following key areas:

*   Code structure and organization
*   TaskFlow API usage
*   Testing strategies
*   Security implementation
*   Deployment practices
*   Performance optimization
*   Monitoring techniques
*   Migration from Airflow 1.X

It also references other relevant documents such as:

*   [Migration Guide](./migration_guide.md): For migration-specific details and checklists.
*   [Composer 2 Architecture](./composer2_architecture.md): For architectural performance considerations and scaling strategies.
*   [Security Guidelines](./security_guidelines.md): For security best practices and implementation guidelines.
*   [Deployment Guide](./deployment_guide.md): For deployment practices and CI/CD integration guidance.

## Code Structure Best Practices

### DAG File Organization

*   **Single DAG per file:** Each DAG should be defined in its own Python file. This improves readability and simplifies debugging.
*   **Descriptive file names:** Use meaningful file names that reflect the DAG's purpose (e.g., `etl_pipeline.py`, `data_sync.py`).
*   **Consistent directory structure:** Organize DAG files into logical directories based on business function or team ownership.

### Import Patterns

*   **Use provider packages:** Import operators, hooks, and sensors from the appropriate provider packages (e.g., `apache-airflow-providers-google`, `apache-airflow-providers-http`).
*   **Avoid wildcard imports:** Use explicit imports to improve code clarity and avoid naming conflicts.
*   **Group imports:** Organize imports into standard library, third-party, and internal modules.

### DAG Definition

*   **Use context managers:** Define DAGs using the `with DAG(...) as dag:` context manager. This ensures proper DAG registration and simplifies task definition.
*   **Set descriptive DAG IDs:** Use clear and concise DAG IDs that follow a consistent naming convention.
*   **Configure default arguments:** Set appropriate default arguments for tasks, including `owner`, `start_date`, `retries`, and `retry_delay`.
*   **Use tags:** Apply tags to DAGs for better organization and filtering in the Airflow UI.
*   **Set `catchup=False`:** Explicitly set `catchup=False` to prevent backfilling of missed DAG runs.

Example:

````python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Use context manager for DAG definition
with DAG(
    dag_id='example_modern_dag',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'modern'],
) as dag:
    # Task definitions go here
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: print('Starting workflow')
    )
    
    # Task dependencies can be defined outside the context manager
    # but still within the file scope

# No need to explicitly pass the dag object to operators
````

### Task Definition

*   **Use appropriate operators:** Select the most suitable operator for each task based on its function (e.g., `PythonOperator`, `BashOperator`, `BigQueryExecuteQueryOperator`).
*   **Set descriptive task IDs:** Use clear and concise task IDs that follow a consistent naming convention.
*   **Define task dependencies explicitly:** Use the `>>` and `<<` operators to define task dependencies.
*   **Configure retries and timeouts:** Set appropriate retry policies and execution timeouts for tasks.
*   **Use typing hints:** Use Python typing hints to improve code clarity and enable static analysis.

### Error Handling

*   **Implement try-except blocks:** Use `try-except` blocks to handle potential exceptions within tasks.
*   **Use XComs to capture errors:** Push error messages to XComs for downstream tasks to handle.
*   **Set appropriate trigger rules:** Use trigger rules to control task execution based on the success or failure of upstream tasks (e.g., `TriggerRule.ALL_SUCCESS`, `TriggerRule.ONE_FAILED`).
*   **Implement alerting:** Configure alerts for task failures using the `on_failure_callback` parameter.

Example:

````python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

def task_that_might_fail(**context):
    # Implementation with proper error handling
    try:
        # Task logic here
        pass
    except Exception as e:
        context['ti'].xcom_push(key='error', value=str(e))
        raise

def handle_failure(**context):
    error = context['ti'].xcom_pull(key='error', task_ids='risky_task')
    print(f"Handling failure: {error}")
    # Recovery or cleanup logic

with DAG(
    'error_handling_pattern',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    start = DummyOperator(task_id='start')
    
    risky_task = PythonOperator(
        task_id='risky_task',
        python_callable=task_that_might_fail,
        retries=3,
        retry_delay=60,
        provide_context=True,
    )
    
    handle_error = PythonOperator(
        task_id='handle_error',
        python_callable=handle_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )
    
    success_path = DummyOperator(task_id='success_path')
    end = DummyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    start >> risky_task >> success_path >> end
    risky_task >> handle_error >> end
````

### Idempotency

*   **Ensure idempotent task execution:** Design tasks to be idempotent, meaning that repeated execution produces the same result. This is crucial for handling retries and ensuring data consistency.
*   **Use unique identifiers:** Use unique identifiers for processing data to prevent duplicate processing.
*   **Implement versioning:** Implement versioning for data and code to ensure consistency and reproducibility.

### Dynamic DAG Generation

*   **Use dynamic DAG generation carefully:** Dynamically generating DAGs can be useful for certain use cases, but it can also increase complexity and reduce visibility.
*   **Cache DAG definitions:** Cache DAG definitions to improve performance and reduce DAG parsing time.
*   **Use a consistent DAG generation pattern:** Follow a consistent pattern for generating DAGs to improve maintainability.

### Modular Code Design

*   **Organize code into reusable modules:** Break down complex DAGs into smaller, reusable modules and utilities.
*   **Use functions and classes:** Encapsulate logic into functions and classes to improve code organization and testability.
*   **Follow DRY principle:** Avoid repeating code by creating reusable components.

## TaskFlow API Best Practices

### When to Use TaskFlow API

*   **Python-centric workflows:** TaskFlow API is well-suited for workflows that primarily involve Python code.
*   **Simplified XCom management:** TaskFlow API simplifies data exchange between tasks by automatically handling XComs.
*   **Improved code readability:** TaskFlow API can improve code readability by reducing boilerplate code.

### TaskFlow Function Design

*   **Use the `@task` decorator:** Decorate Python functions with the `@task` decorator to define tasks.
*   **Define clear inputs and outputs:** Use function parameters and return values to define task inputs and outputs.
*   **Use typing hints:** Use Python typing hints to improve code clarity and enable static analysis.

### Multiple Outputs

*   **Use `multiple_outputs=True`:** Use the `multiple_outputs=True` parameter when a task function returns multiple values. This allows you to access individual return values in downstream tasks.

### XCom Usage

*   **Avoid explicit XCom push/pull:** TaskFlow API automatically handles XComs, so avoid using explicit `xcom_push` and `xcom_pull` calls.
*   **Use function parameters to access XComs:** Access XCom values in downstream tasks by using function parameters with the same names as the return values of upstream tasks.

### TaskFlow Dependencies

*   **Define dependencies explicitly:** Define task dependencies by calling task functions within the DAG definition.
*   **Use the `>>` and `<<` operators:** Use the `>>` and `<<` operators to define task dependencies when mixing TaskFlow API with traditional operators.

### Error Handling in TaskFlow

*   **Use `try-except` blocks:** Use `try-except` blocks to handle potential exceptions within TaskFlow functions.
*   **Reraise exceptions:** Reraise exceptions to ensure that Airflow marks the task as failed.
*   **Use `on_failure_callback`:** Configure alerts for task failures using the `on_failure_callback` parameter.

### Mixing TaskFlow and Traditional Operators

*   **Use TaskGroups:** Use TaskGroups to organize tasks and improve DAG structure when mixing TaskFlow API with traditional operators.
*   **Define dependencies explicitly:** Define dependencies between TaskFlow functions and traditional operators using the `>>` and `<<` operators.

### TaskFlow Performance Considerations

*   **Avoid large data transfers:** Avoid transferring large amounts of data between tasks using XComs. Consider using a shared storage location instead.
*   **Optimize task function execution time:** Optimize the execution time of task functions to improve overall DAG performance.
*   **Use appropriate resource allocation:** Allocate appropriate resources to tasks based on their requirements.

## Testing Best Practices

### DAG Validation Testing

*   **Validate DAG structure:** Validate that DAGs have a valid structure, including no cycles and correct task dependencies.
*   **Validate task parameters:** Validate that task parameters are correctly configured and compatible with Airflow 2.X.
*   **Validate import statements:** Validate that import statements are correct and follow the recommended patterns.

### Unit Testing Tasks

*   **Write unit tests for custom operators:** Write unit tests for custom operators to ensure they function correctly.
*   **Mock external dependencies:** Mock external dependencies to isolate tasks and improve testability.
*   **Test task logic:** Test the core logic of tasks to ensure they produce the expected results.

### Integration Testing

*   **Test DAG execution:** Test the execution of entire DAGs to ensure that all tasks run successfully and produce the expected results.
*   **Test data flow:** Test the flow of data between tasks to ensure that XComs are correctly configured.
*   **Test external system integrations:** Test integrations with external systems to ensure that they function correctly.

### Mocking External Services

*   **Use mock objects:** Use mock objects to simulate external services and dependencies.
*   **Control mock behavior:** Control the behavior of mock objects to simulate different scenarios and edge cases.
*   **Verify mock interactions:** Verify that tasks interact with mock objects as expected.

### Test Fixtures

*   **Create reusable test fixtures:** Create reusable test fixtures to reduce code duplication and improve test maintainability.
*   **Use dependency injection:** Use dependency injection to provide test fixtures to tasks.
*   **Manage test data:** Manage test data effectively to ensure consistency and reproducibility.

### CI Integration

*   **Integrate tests with CI pipeline:** Integrate DAG tests with the CI pipeline to automatically validate code changes.
*   **Run tests on every commit:** Run tests on every commit to ensure that code changes do not introduce regressions.
*   **Use a dedicated test environment:** Use a dedicated test environment to isolate tests from production systems.

### Test Environment Setup

*   **Use Docker containers:** Use Docker containers to create isolated test environments.
*   **Configure Airflow in test environment:** Configure Airflow in the test environment to match the production environment.
*   **Use a dedicated test database:** Use a dedicated test database to prevent data corruption.

### TaskFlow API Testing

*   **Test TaskFlow functions:** Test TaskFlow functions in isolation to ensure they function correctly.
*   **Test DAG execution with TaskFlow:** Test the execution of DAGs that use TaskFlow API to ensure that tasks run successfully and produce the expected results.
*   **Test XCom usage with TaskFlow:** Test the flow of data between tasks using XComs to ensure that they are correctly configured.

Example:

````python
import pytest
from airflow.models import DagBag

# Test DAG integrity
def test_dag_loads():
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors
    assert len(dag_bag.dags) > 0

# Test specific DAG structure
def test_specific_dag_structure():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag('example_dag')
    assert dag is not None
    
    # Test task count and names
    task_ids = [t.task_id for t in dag.tasks]
    assert len(task_ids) == 3  # Expected number of tasks
    assert 'start' in task_ids
    assert 'process' in task_ids
    assert 'end' in task_ids
    
    # Test dependencies
    start_task = dag.get_task('start')
    process_task = dag.get_task('process')
    end_task = dag.get_task('end')
    
    assert start_task.downstream_list == [process_task]
    assert process_task.downstream_list == [end_task]
    assert end_task.upstream_list == [process_task]
````

## Security Best Practices

### Credential Management

*   **Store sensitive credentials in Secret Manager:** Use Secret Manager to store sensitive credentials, such as passwords, API keys, and connection strings.
*   **Use connection URI references:** Use connection URI references in Airflow to access credentials stored in Secret Manager.
*   **Avoid hardcoding credentials:** Never hardcode credentials directly in DAG code or configuration files.

### Connection Security

*   **Use encrypted connections:** Use encrypted connections (e.g., TLS/SSL) to protect data in transit.
*   **Configure network security:** Configure network security for Cloud Composer 2 to restrict access to the environment.
*   **Use private IP:** Use private IP networking to prevent exposure of the environment to the public internet.

### Variable Security

*   **Store sensitive variables in Secret Manager:** Use Secret Manager to store sensitive variables, such as API keys and configuration settings.
*   **Use environment variables:** Use environment variables for configuration settings that vary across environments.
*   **Restrict access to variables:** Implement appropriate RBAC controls to restrict access to variables.

### Authentication Configuration

*   **Use Google SSO:** Use Google SSO for user authentication.
*   **Enforce multi-factor authentication:** Enforce multi-factor authentication (MFA) for all users.
*   **Use strong passwords:** Enforce strong password policies for all users.

### Authorization Controls

*   **Implement RBAC:** Implement RBAC to control access to Airflow resources based on user roles.
*   **Apply least privilege principle:** Grant users only the minimum permissions required to perform their tasks.
*   **Regularly review permissions:** Regularly review user permissions to ensure they are still appropriate.

### Sensitive Data Handling

*   **Encrypt sensitive data in XComs:** Encrypt sensitive data in XComs to protect it from unauthorized access.
*   **Mask sensitive data in logs:** Mask sensitive data in logs to prevent it from being exposed.
*   **Avoid storing sensitive data in DAG code:** Avoid storing sensitive data directly in DAG code.

### Network Security

*   **Use private IP networking:** Use private IP networking to prevent exposure of the environment to the public internet.
*   **Configure firewall rules:** Configure firewall rules to restrict access to the environment.
*   **Implement network policies:** Implement Kubernetes network policies to control pod-to-pod communication.

### Secure Coding Practices

*   **Validate inputs:** Validate all inputs to prevent injection attacks.
*   **Sanitize outputs:** Sanitize all outputs to prevent cross-site scripting (XSS) attacks.
*   **Use secure coding libraries:** Use secure coding libraries to prevent common security vulnerabilities.

## Deployment Best Practices

### CI/CD Integration

*   **Use a CI/CD pipeline:** Use a CI/CD pipeline to automate the deployment process.
*   **Integrate with version control:** Integrate the CI/CD pipeline with version control to track changes and enable rollbacks.
*   **Automate testing:** Automate testing as part of the CI/CD pipeline to ensure code quality.

### Environment Promotion

*   **Use a multi-environment deployment process:** Use a multi-environment deployment process to promote DAGs through development, QA, and production environments.
*   **Implement approval workflows:** Implement approval workflows to control the deployment process.
*   **Automate environment promotion:** Automate the environment promotion process to reduce manual effort and improve consistency.

### Version Control

*   **Use version control for DAGs:** Use version control to track changes to DAGs and enable rollbacks.
*   **Use branching strategies:** Use branching strategies to manage different versions of DAGs.
*   **Tag releases:** Tag releases to identify specific versions of DAGs.

### Validation Procedures

*   **Implement pre-deployment validation:** Implement pre-deployment validation checks to ensure that DAGs are valid and compatible with the target environment.
*   **Implement post-deployment validation:** Implement post-deployment validation checks to verify that DAGs are running correctly and producing the expected results.
*   **Automate validation procedures:** Automate validation procedures to reduce manual effort and improve consistency.

### Rollback Strategies

*   **Implement rollback procedures:** Implement rollback procedures to quickly revert to a previous version of a DAG in case of issues.
*   **Automate rollback procedures:** Automate rollback procedures to reduce manual effort and improve speed.
*   **Test rollback procedures:** Test rollback procedures regularly to ensure they function correctly.

### Configuration Management

*   **Use configuration management tools:** Use configuration management tools to manage environment-specific configurations.
*   **Store configuration in version control:** Store configuration in version control to track changes and enable rollbacks.
*   **Automate configuration deployment:** Automate configuration deployment as part of the CI/CD pipeline.

### Approval Workflows

*   **Implement approval workflows:** Implement approval workflows to control the deployment process.
*   **Define clear roles and responsibilities:** Define clear roles and responsibilities for approvers.
*   **Automate approval workflows:** Automate approval workflows to reduce manual effort and improve efficiency.

### Deployment Monitoring

*   **Monitor deployments for issues:** Monitor deployments for issues and errors.
*   **Set up alerts for deployment failures:** Set up alerts for deployment failures to ensure that they are quickly addressed.
*   **Track deployment performance:** Track deployment performance to identify areas for improvement.

## Performance Best Practices

### DAG Design for Performance

*   **Minimize DAG complexity:** Keep DAGs as simple as possible to reduce parsing time and improve performance.
*   **Use appropriate operators:** Select the most efficient operator for each task based on its function.
*   **Avoid unnecessary dependencies:** Avoid creating unnecessary dependencies between tasks.

### Resource Allocation

*   **Allocate appropriate resources to workers:** Allocate appropriate resources (CPU, memory) to Airflow workers based on the workload.
*   **Use auto-scaling:** Use auto-scaling to automatically adjust the number of workers based on the workload.
*   **Configure resource requests and limits:** Configure resource requests and limits for tasks to ensure that they have enough resources to run successfully.

### Concurrency Settings

*   **Configure appropriate concurrency settings:** Configure appropriate concurrency settings for the Airflow scheduler and workers to optimize performance.
*   **Adjust `dag_concurrency`:** Adjust the `dag_concurrency` setting to control the number of DAG runs that can be executed concurrently.
*   **Adjust `parallelism`:** Adjust the `parallelism` setting to control the number of tasks that can be executed concurrently.

### Parallel Execution

*   **Use parallel task execution:** Use parallel task execution to improve DAG performance.
*   **Use the `parallelism` parameter:** Use the `parallelism` parameter to control the number of tasks that can be executed concurrently.
*   **Use the `max_active_runs` parameter:** Use the `max_active_runs` parameter to control the number of DAG runs that can be executed concurrently.

### Database Optimization

*   **Optimize database queries:** Optimize database queries to improve performance.
*   **Use indexes:** Use indexes to improve query performance.
*   **Tune database settings:** Tune database settings to optimize performance.

### DAG Scheduling

*   **Optimize DAG scheduling patterns:** Optimize DAG scheduling patterns to reduce DAG parsing time and improve performance.
*   **Avoid overlapping DAG runs:** Avoid scheduling DAGs to run at the same time to prevent resource contention.
*   **Use appropriate schedule intervals:** Use appropriate schedule intervals to ensure that DAGs run frequently enough to meet business requirements.

### XCom Optimization

*   **Minimize XCom data size:** Minimize the size of data transferred between tasks using XComs.
*   **Use appropriate XCom backend:** Use an appropriate XCom backend based on the size and type of data being transferred.
*   **Avoid unnecessary XComs:** Avoid using XComs when data can be passed directly between tasks.

### Worker Scaling

*   **Configure worker scaling:** Configure worker scaling to automatically adjust the number of workers based on the workload.
*   **Use horizontal pod autoscaling:** Use horizontal pod autoscaling (HPA) to automatically scale the number of worker pods based on CPU utilization.
*   **Use vertical pod autoscaling:** Use vertical pod autoscaling (VPA) to automatically adjust the resources (CPU, memory) allocated to worker pods.

## Monitoring Best Practices

### Logging Strategy

*   **Implement effective logging:** Implement effective logging in DAGs to track task execution and identify issues.
*   **Use structured logging:** Use structured logging to make logs easier to analyze.
*   **Log relevant information:** Log relevant information, such as task IDs, execution dates, and XCom values.

### Alert Configuration

*   **Set up meaningful alerts:** Set up meaningful alerts for DAG issues, such as task failures, SLA misses, and data quality issues.
*   **Use appropriate alert levels:** Use appropriate alert levels (e.g., INFO, WARNING, ERROR) to prioritize alerts.
*   **Route alerts to the appropriate teams:** Route alerts to the appropriate teams based on the type of issue.

### Metrics Collection

*   **Collect performance metrics:** Collect performance metrics, such as DAG parsing time, task execution time, and resource utilization.
*   **Use Prometheus:** Use Prometheus to collect and store metrics.
*   **Use Grafana:** Use Grafana to visualize metrics and create dashboards.

### Dashboard Setup

*   **Create effective monitoring dashboards:** Create effective monitoring dashboards to visualize key metrics and identify issues.
*   **Use clear and concise labels:** Use clear and concise labels for metrics and dashboards.
*   **Organize dashboards logically:** Organize dashboards logically based on business function or team ownership.

### Health Checks

*   **Implement health checks for DAGs:** Implement health checks for DAGs to ensure that they are running correctly.
*   **Implement health checks for environment:** Implement health checks for the Cloud Composer 2 environment to ensure that it is healthy.
*   **Automate health checks:** Automate health checks to reduce manual effort and improve consistency.

### SLA Monitoring

*   **Monitor SLAs for critical workflows:** Monitor SLAs for critical workflows to ensure that they are being met.
*   **Set up alerts for SLA misses:** Set up alerts for SLA misses to ensure that they are quickly addressed.
*   **Track SLA performance over time:** Track SLA performance over time to identify areas for improvement.

### Error Detection

*   **Implement early error detection:** Implement early error detection techniques to identify issues before they impact production systems.
*   **Use data quality checks:** Use data quality checks to ensure that data is accurate and consistent.
*   **Use anomaly detection:** Use anomaly detection to identify unusual patterns in data or system behavior.

### Operational Reporting

*   **Generate operational reports for stakeholders:** Generate operational reports for stakeholders to provide visibility into the performance and health of Airflow workflows.
*   **Automate report generation:** Automate report generation to reduce manual effort and improve consistency.
*   **Distribute reports to stakeholders:** Distribute reports to stakeholders on a regular basis.

## Migration Best Practices

### Pre-Migration Assessment

*   **Evaluate existing DAGs:** Evaluate existing DAGs for migration readiness, including code complexity, dependencies, and security requirements.
*   **Identify deprecated operators:** Identify deprecated operators and plan for their replacement.
*   **Assess XCom usage:** Assess XCom usage patterns and plan for any necessary adjustments.

### Operator Migration

*   **Replace deprecated operators:** Replace deprecated operators with their Airflow 2.X equivalents.
*   **Update import statements:** Update import statements to reflect the new Airflow 2.X package structure.
*   **Test migrated operators:** Test migrated operators to ensure they function correctly.

### Import Statement Updates

*   **Update import statements:** Update import statements to reflect the new Airflow 2.X package structure.
*   **Use provider packages:** Use provider packages for connections to external systems, such as GCP services, databases, and APIs.
*   **Avoid wildcard imports:** Use explicit imports to improve code clarity and avoid naming conflicts.

### XCom Migration

*   **Review XCom usage patterns:** Review XCom usage patterns to ensure compatibility with Airflow 2.X.
*   **Use TaskFlow API:** Consider using the TaskFlow API to simplify XCom management.
*   **Avoid large data transfers:** Avoid transferring large amounts of data between tasks using XComs.

### TaskFlow API Adoption

*   **Convert suitable PythonOperators to TaskFlow API:** Convert suitable PythonOperators to the TaskFlow API for simplified DAG creation.
*   **Use the `@task` decorator:** Decorate Python functions with the `@task` decorator to define tasks.
*   **Define clear inputs and outputs:** Use function parameters and return values to define task inputs and outputs.

### Testing Migration Changes

*   **Write unit tests:** Write unit tests for all migrated operators to ensure they function correctly.
*   **Write integration tests:** Write integration tests for all migrated DAGs to ensure they can run successfully and produce the expected results.
*   **Validate data integrity:** Validate that all data has been migrated correctly and that there is no data loss.

### Incremental Migration

*   **Implement an incremental migration strategy:** Migrate DAGs in small batches to reduce risk and improve manageability.
*   **Monitor migrated DAGs:** Monitor migrated DAGs closely to identify any issues.
*   **Roll back changes if necessary:** Roll back changes if any issues are identified.

### Post-Migration Validation

*   **Validate DAG parsing:** Validate that all DAG files parse correctly in the Cloud Composer 2 environment.
*   **Verify task execution:** Verify that all tasks execute successfully and produce the expected results.
*   **Validate data integrity:** Validate that all data has been migrated correctly and that there is no data loss.

## Example Patterns

### Basic DAG Pattern

Example of a well-structured basic DAG

### TaskFlow DAG Pattern

Example of a DAG using TaskFlow API

### Error Handling Pattern

Example of robust error handling

### Dynamic DAG Pattern

Example of dynamically generated DAGs

### Testing Pattern

Example of comprehensive DAG testing

### Secure DAG Pattern

Example of a DAG with security best practices

### Performance-Optimized Pattern

Example of a performance-optimized DAG

### Migration Pattern

Example of a migrated DAG from Airflow 1.X to 2.X

## References

### Internal Documentation

Links to related internal documentation

### External Resources

Links to official Airflow documentation and resources

### GCP Documentation

Links to relevant Google Cloud documentation

### Example Code Repository

Links to example code and templates