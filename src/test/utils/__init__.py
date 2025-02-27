"""
Initialization module for the test utilities package.
Exposes key functionality for testing Airflow DAGs, operators, and task compatibility
during migration from Airflow 1.10.15 to Airflow 2.X.
Acts as a centralized access point for all testing utilities used in the migration project.
"""

# Internal imports
from .airflow2_compatibility_utils import (  # src/test/utils/airflow2_compatibility_utils.py
    is_airflow2,
    is_taskflow_available,
    get_airflow_version_info,
    transform_import,
    transform_code_imports,
    transform_operator_references,
    convert_to_taskflow,
    mock_airflow2_imports,
    mock_airflow1_imports,
    Airflow2CompatibilityTestMixin,
    Airflow2CodeComparator,
    CompatibleDAGTestCase,
    AIRFLOW_VERSION,
    AIRFLOW_1_TO_2_IMPORT_MAPPING,
    AIRFLOW_1_TO_2_OPERATOR_MAPPING,
)
from .assertion_utils import (  # src/test/utils/assertion_utils.py
    assert_dag_structure_unchanged,
    assert_operator_compatibility,
    assert_task_execution_unchanged,
    assert_xcom_compatibility,
    assert_dag_run_state,
    assert_dag_params_match,
    assert_dag_airflow2_compatible,
    assert_operator_airflow2_compatible,
    assert_scheduler_metrics,
    assert_dag_execution_time,
    assert_dag_structure,
    assert_migrations_successful,
    CompatibilityAsserter,
)
from .dag_validation_utils import (  # src/test/utils/dag_validation_utils.py
    validate_dag_integrity,
    validate_task_relationships,
    check_task_ids,
    measure_dag_parse_time,
    validate_dag_loading,
    validate_airflow2_compatibility,
    validate_dag_parameters,
    check_parsing_performance,
    validate_tasks_compatibility,
    validate_dag_scheduling,
    check_for_cycles,
    verify_dag_execution,
    DAGValidator,
    TaskValidator,
    DAGTestCase,
)
from .test_helpers import (  # src/test/utils/test_helpers.py
    run_dag,
    run_dag_task,
    compare_dags,
    compare_operators,
    measure_performance,
    create_test_execution_context,
    version_compatible_test,
    run_with_timeout,
    create_airflow_version_dag,
    setup_test_dag,
    capture_logs,
    DAGTestRunner,
    TestAirflowContext,
    OperatorTester,
    DEFAULT_EXECUTION_DATE,
    DEFAULT_TEST_TIMEOUT,
)
from .operator_validation_utils import (  # src/test/utils/operator_validation_utils.py
    validate_operator_signature,
    validate_operator_parameters,
    compare_operator_attributes,
    identify_deprecated_operators,
    convert_operator_to_airflow2,
    test_operator_migration,
    get_operator_migration_plan,
    validate_taskflow_conversion,
    ParameterValidationResult,
    MigrationTestResult,
    MigrationPlan,
    TaskFlowValidationResult,
    OperatorTestCase,
    OperatorMigrationValidator,
    OPERATOR_VALIDATION_LEVEL,
)

VERSION = "0.1.0"