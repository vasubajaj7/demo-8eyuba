#!/bin/bash
# Unit Test Execution Script for Airflow 1.x to 2.x Migration Project
# This script runs all unit tests and generates reports for CI/CD pipelines

set -e

# =========================================================
# Configuration Variables
# =========================================================
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)
TEST_MARKERS="unit"
TEST_DIRS="./src/test"
PYTEST_CONFIG="./src/test/config/pytest.ini"
COVERAGE_CONFIG="./src/test/config/.coveragerc"
OUTPUT_DIR="./test-reports"
PYTHON_VERSION="3.8"
AIRFLOW_VERSION="2.0.0"
PARALLEL_TESTS="auto"

# =========================================================
# Banner
# =========================================================
echo "============================================================"
echo "      Airflow Migration Project - Unit Test Runner"
echo "============================================================"
echo "Python Version: $PYTHON_VERSION"
echo "Airflow Version: $AIRFLOW_VERSION"
echo "Running tests from: $TEST_DIRS"
echo "Environment: ${CI:+CI}${CI:-Local}"
echo "============================================================"

# =========================================================
# Setup Environment Function
# =========================================================
setup_environment() {
    echo "📦 Setting up test environment..."
    
    # Create virtual environment if it doesn't exist
    if [ ! -d ".venv" ]; then
        echo "Creating Python virtual environment..."
        python -m venv .venv
    fi
    
    # Activate virtual environment
    echo "Activating virtual environment..."
    source .venv/bin/activate
    
    # Install test dependencies
    echo "Installing test dependencies..."
    pip install -r src/test/config/requirements-test.txt
    
    # Configure PYTHONPATH for module resolution
    export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"
    
    # Setup Airflow environment for testing
    export AIRFLOW_HOME="${PROJECT_ROOT}/airflow-home"
    export AIRFLOW__CORE__DAGS_FOLDER="${PROJECT_ROOT}/src/dags"
    export AIRFLOW__CORE__UNIT_TEST_MODE=True
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW_TEST_MODE=True
    
    # Create output directory for test reports
    mkdir -p "${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}/coverage"
    mkdir -p "${OUTPUT_DIR}/junit"
    
    echo "Environment setup complete."
    return 0
}

# =========================================================
# Run Tests Function
# =========================================================
run_tests() {
    echo "🧪 Running unit tests..."
    
    # Run pytest with appropriate configuration
    pytest_cmd="python -m pytest"
    
    # Add test markers
    pytest_cmd+=" -m ${TEST_MARKERS}"
    
    # Add configuration
    pytest_cmd+=" -c ${PYTEST_CONFIG}"
    
    # Add coverage configuration
    pytest_cmd+=" --cov --cov-config=${COVERAGE_CONFIG}"
    
    # Add report formats
    pytest_cmd+=" --cov-report=xml:${OUTPUT_DIR}/coverage/coverage.xml"
    pytest_cmd+=" --cov-report=html:${OUTPUT_DIR}/coverage/html"
    pytest_cmd+=" --junitxml=${OUTPUT_DIR}/junit/test-results.xml"
    
    # Add test directories
    pytest_cmd+=" ${TEST_DIRS}"
    
    # Add parallelism if enabled
    if [ "${PARALLEL_TESTS}" != "disabled" ]; then
        pytest_cmd+=" -xvs"
        if [ "${PARALLEL_TESTS}" != "auto" ]; then
            pytest_cmd+=" -n ${PARALLEL_TESTS}"
        else
            pytest_cmd+=" -n auto"
        fi
    else
        pytest_cmd+=" -xvs"
    fi
    
    # Run the tests and capture exit code
    echo "Executing: $pytest_cmd"
    set +e
    eval "$pytest_cmd"
    PYTEST_EXIT_CODE=$?
    set -e
    
    echo "Test execution finished with exit code: ${PYTEST_EXIT_CODE}"
    return $PYTEST_EXIT_CODE
}

# =========================================================
# Generate Reports Function
# =========================================================
generate_reports() {
    echo "📊 Generating test reports..."
    
    # Check if reports exist
    if [ ! -f "${OUTPUT_DIR}/junit/test-results.xml" ]; then
        echo "Error: Test result file not found. Tests may have failed to run properly."
        return 1
    fi
    
    # Generate summary report
    echo "Test Summary:"
    
    # Parse JUnit XML to get test summary
    if command -v xmllint >/dev/null 2>&1; then
        echo "-------------------------------------------------------------------------"
        TOTAL_TESTS=$(xmllint --xpath "string(/testsuite/@tests)" "${OUTPUT_DIR}/junit/test-results.xml")
        TOTAL_FAILURES=$(xmllint --xpath "string(/testsuite/@failures)" "${OUTPUT_DIR}/junit/test-results.xml")
        TOTAL_ERRORS=$(xmllint --xpath "string(/testsuite/@errors)" "${OUTPUT_DIR}/junit/test-results.xml")
        echo "Tests: $TOTAL_TESTS, Failures: $TOTAL_FAILURES, Errors: $TOTAL_ERRORS"
        echo "-------------------------------------------------------------------------"
    fi
    
    # Process coverage report
    if [ -f "${OUTPUT_DIR}/coverage/coverage.xml" ]; then
        echo "Coverage Report:"
        echo "-------------------------------------------------------------------------"
        if command -v coverage >/dev/null 2>&1; then
            coverage report --skip-covered
        else
            echo "Coverage tool not found. Install it with: pip install coverage"
        fi
        echo "-------------------------------------------------------------------------"
        echo "Detailed coverage report available at: ${OUTPUT_DIR}/coverage/html/index.html"
    fi
    
    # Highlight failures if any
    if [ "$TOTAL_FAILURES" -gt 0 ] || [ "$TOTAL_ERRORS" -gt 0 ]; then
        echo "🚨 Test Failures Detected: Please review the detailed test logs."
    else 
        echo "✅ All tests passed successfully."
    fi
    
    echo "Reports generated and stored in: ${OUTPUT_DIR}"
    return 0
}

# =========================================================
# Cleanup Function
# =========================================================
cleanup() {
    echo "🧹 Cleaning up resources..."
    
    # Remove Python cache files
    find "${PROJECT_ROOT}" -type d -name "__pycache__" -exec rm -rf {} +
    find "${PROJECT_ROOT}" -name "*.pyc" -delete
    
    # Remove temporary test files but preserve reports
    find "${PROJECT_ROOT}" -name ".pytest_cache" -exec rm -rf {} +
    find "${PROJECT_ROOT}" -name ".coverage.*" -delete
    
    # Reset environment variables
    if [ -z "$CI" ]; then
        # Only deactivate if not running in CI
        type deactivate &>/dev/null && deactivate
    fi
    
    echo "Cleanup complete."
    return 0
}

# =========================================================
# Main Function
# =========================================================
main() {
    # Record start time
    START_TIME=$(date +%s)
    
    # Initialize exit code
    EXIT_CODE=0
    
    # Trap for cleanup on script exit
    trap cleanup EXIT
    
    # Run the test process
    setup_environment || { EXIT_CODE=1; echo "⛔ Environment setup failed!"; return $EXIT_CODE; }
    run_tests
    EXIT_CODE=$?
    
    # Generate reports regardless of test exit code
    generate_reports || { echo "⚠️ Report generation failed, but continuing..."; }
    
    # Calculate execution time
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo "Total execution time: ${DURATION} seconds"
    
    # Return with the test execution exit code
    return $EXIT_CODE
}

# Execute main function
main
exit $?