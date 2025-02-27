#!/bin/bash
# Integration Test Execution Script for Airflow 1.x to 2.x Migration Project
# This script runs all integration tests and generates reports for CI/CD pipelines

# Script and directory configuration
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

# Test configuration
TEST_MARKERS="integration"
TEST_DIRS="./src/test/integration_tests"
PYTEST_CONFIG="./src/test/config/pytest.ini"
COVERAGE_CONFIG="./src/test/config/.coveragerc"
OUTPUT_DIR="./integration-test-reports"

# Environment configuration
PYTHON_VERSION="3.8"
AIRFLOW_VERSION="2.0.0"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-composer-migration-test}"
GCP_REGION="${GCP_REGION:-us-central1}"
CI_BUILD_ID="${CI_BUILD_ID:-local-$(date +%s)}"
MOCK_GCP_SERVICES="true"
PARALLEL_TESTS="auto"

# Error handling
set -e  # Exit immediately if a command exits with a non-zero status

# Utility for colorful output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function for section printing
print_section() {
  echo -e "\n${BLUE}===${NC} $1 ${BLUE}===${NC}\n"
}

# Function for success/error messages
print_success() {
  echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
  echo -e "${RED}✗ $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}⚠ $1${NC}"
}

setup_environment() {
  print_section "Setting up test environment"
  
  # Navigate to project root
  cd "$PROJECT_ROOT"
  
  # Create virtual environment if it doesn't exist
  if [ ! -d "venv" ]; then
    print_warning "Virtual environment not found, creating..."
    python${PYTHON_VERSION} -m venv venv
  fi
  
  # Activate virtual environment
  source venv/bin/activate
  
  # Install test dependencies
  print_success "Installing test dependencies..."
  pip install -r src/test/config/requirements-test.txt
  
  # Install GCP dependencies and mock libraries
  pip install google-cloud-storage google-cloud-bigquery google-cloud-secretmanager
  pip install pytest-mock moto
  
  # Configure PYTHONPATH to include project directories
  export PYTHONPATH="$PROJECT_ROOT:$PROJECT_ROOT/src:$PYTHONPATH"
  
  # Set up Airflow environment variables
  export AIRFLOW_HOME="$PROJECT_ROOT/airflow"
  export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/src/dags"
  export AIRFLOW__CORE__LOAD_EXAMPLES=False
  export AIRFLOW__CORE__UNIT_TEST_MODE=True
  
  # Set up GCP environment variables
  export GOOGLE_CLOUD_PROJECT="$GCP_PROJECT_ID"
  export GCP_TEST_PROJECT_ID="$GCP_PROJECT_ID"
  export GCP_TEST_REGION="$GCP_REGION"
  export GCP_COMPOSER_LOCATION="$GCP_REGION"
  
  # Create output directory for test reports
  mkdir -p "$OUTPUT_DIR"
  
  # Set up Docker containers for dependent services if needed
  if command -v docker &>/dev/null; then
    # Check if we need to run dependent services in Docker
    if [ "${RUN_DOCKER_SERVICES:-false}" = "true" ]; then
      print_warning "Starting Docker containers for dependent services..."
      docker-compose -f "$PROJECT_ROOT/src/test/config/docker-compose-test.yml" up -d
    fi
  fi
  
  print_success "Environment setup complete"
  return 0
}

setup_mock_services() {
  if [ "$MOCK_GCP_SERVICES" != "true" ]; then
    print_warning "Mock GCP services disabled, skipping mock setup"
    return 0
  fi
  
  print_section "Setting up mock GCP services"
  
  # Export environment variables to indicate mock usage
  export GCP_MOCK_SERVICES="true"
  export GCP_MOCK_PROJECT_ID="$GCP_PROJECT_ID-mock"
  
  # Start mock services
  python -m src.test.fixtures.mock_gcp_services start \
    --project-id "$GCP_MOCK_PROJECT_ID" \
    --gcs-port 8081 \
    --bigquery-port 8082 \
    --secretmanager-port 8083 \
    --cloudsql-port 8084
  
  # Set environment variables for mock endpoints
  export GCP_MOCK_GCS_ENDPOINT="http://localhost:8081"
  export GCP_MOCK_BIGQUERY_ENDPOINT="http://localhost:8082"
  export GCP_MOCK_SECRETMANAGER_ENDPOINT="http://localhost:8083"
  export GCP_MOCK_CLOUDSQL_ENDPOINT="http://localhost:8084"
  
  # Create test buckets and datasets in mock services
  python -m src.test.fixtures.setup_mock_resources \
    --project-id "$GCP_MOCK_PROJECT_ID" \
    --create-buckets "airflow-dag-bucket,test-data-bucket" \
    --create-datasets "test_dataset" \
    --create-secret "airflow-connections"
  
  print_success "Mock GCP services setup complete"
  return 0
}

run_tests() {
  print_section "Running integration tests"
  
  # Navigate to project root
  cd "$PROJECT_ROOT"
  
  # Prepare pytest parameters
  PYTEST_ARGS=(
    # Use specified config
    "-c" "$PYTEST_CONFIG"
    
    # Mark only integration tests
    "-m" "$TEST_MARKERS"
    
    # Configure code coverage
    "--cov=src"
    "--cov-config=$COVERAGE_CONFIG"
    "--no-cov-on-fail"
    
    # Generate reports
    "--junitxml=$OUTPUT_DIR/junit-integration.xml"
    "--html=$OUTPUT_DIR/report.html"
    "--self-contained-html"
    
    # Test directories
    "$TEST_DIRS"
    
    # Verbose output
    "-v"
  )
  
  # Add parallel execution if enabled
  if [ "$PARALLEL_TESTS" != "false" ]; then
    PYTEST_ARGS+=("-n" "$PARALLEL_TESTS")
  fi
  
  # Run the tests
  print_warning "Executing pytest with arguments: ${PYTEST_ARGS[*]}"
  python -m pytest "${PYTEST_ARGS[@]}"
  
  # Capture return code
  TEST_RESULT=$?
  
  if [ $TEST_RESULT -eq 0 ]; then
    print_success "All integration tests passed successfully"
  else
    print_error "Integration tests failed with exit code $TEST_RESULT"
  fi
  
  return $TEST_RESULT
}

run_gcp_integration_tests() {
  # Check if GCP integration tests should be run
  if [ "${RUN_GCP_TESTS:-false}" != "true" ]; then
    print_warning "GCP integration tests disabled, skipping"
    return 0
  fi
  
  print_section "Running GCP-specific integration tests"
  
  # Navigate to project root
  cd "$PROJECT_ROOT"
  
  # Prepare pytest parameters for GCP tests
  GCP_PYTEST_ARGS=(
    # Use specified config
    "-c" "$PYTEST_CONFIG"
    
    # Mark only GCP integration tests
    "-m" "gcp_integration"
    
    # Configure code coverage
    "--cov=src"
    "--cov-config=$COVERAGE_CONFIG"
    "--no-cov-on-fail"
    
    # Generate reports
    "--junitxml=$OUTPUT_DIR/junit-gcp-integration.xml"
    "--html=$OUTPUT_DIR/gcp-report.html"
    "--self-contained-html"
    
    # Test directories
    "$TEST_DIRS"
    
    # Verbose output
    "-v"
  )
  
  # Run the GCP integration tests
  print_warning "Executing GCP integration tests"
  python -m pytest "${GCP_PYTEST_ARGS[@]}"
  
  # Capture return code
  GCP_TEST_RESULT=$?
  
  if [ $GCP_TEST_RESULT -eq 0 ]; then
    print_success "All GCP integration tests passed successfully"
  else
    print_error "GCP integration tests failed with exit code $GCP_TEST_RESULT"
  fi
  
  return $GCP_TEST_RESULT
}

generate_reports() {
  print_section "Generating test reports"
  
  # Check if test reports exist
  if [ ! -f "$OUTPUT_DIR/junit-integration.xml" ]; then
    print_error "Test report file not found. Tests may have failed to run."
    return 1
  fi
  
  # Generate a summary of test results
  echo "Generating test summary report..."
  
  # Parse JUnit XML to extract test counts
  TOTAL_TESTS=$(grep -c "<testcase " "$OUTPUT_DIR/junit-integration.xml" || echo "0")
  FAILED_TESTS=$(grep -c "<failure " "$OUTPUT_DIR/junit-integration.xml" || echo "0")
  SKIPPED_TESTS=$(grep -c "<skipped " "$OUTPUT_DIR/junit-integration.xml" || echo "0")
  PASSED_TESTS=$((TOTAL_TESTS - FAILED_TESTS - SKIPPED_TESTS))
  
  # Generate summary report
  cat > "$OUTPUT_DIR/summary.txt" << EOF
Integration Test Summary
-----------------------
Date: $(date)
Build ID: $CI_BUILD_ID
Airflow Version: $AIRFLOW_VERSION

Tests Summary:
  Total Tests: $TOTAL_TESTS
  Passed: $PASSED_TESTS
  Failed: $FAILED_TESTS
  Skipped: $SKIPPED_TESTS
  
Coverage Summary:
$(python -c "import coverage; cov = coverage.Coverage(data_file='.coverage'); cov.load(); print(cov.report(file=None))" 2>/dev/null || echo "  Coverage data not available")

EOF
  
  # Generate detailed reports for failed tests
  if [ "$FAILED_TESTS" -gt 0 ]; then
    echo "Generating detailed failure report..."
    grep -A 10 "<failure " "$OUTPUT_DIR/junit-integration.xml" > "$OUTPUT_DIR/failures.txt"
  fi
  
  # Generate Airflow 1.X vs 2.X compatibility report if available
  if [ -f "$PROJECT_ROOT/src/test/reports/compatibility_results.json" ]; then
    echo "Generating Airflow 1.X vs 2.X compatibility report..."
    python -m src.test.tools.generate_compatibility_report \
      --input "$PROJECT_ROOT/src/test/reports/compatibility_results.json" \
      --output "$OUTPUT_DIR/compatibility_report.html"
  fi
  
  # Generate coverage reports in multiple formats
  python -m coverage html -d "$OUTPUT_DIR/coverage"
  python -m coverage xml -o "$OUTPUT_DIR/coverage.xml"
  
  print_success "Reports generated successfully at $OUTPUT_DIR"
  print_warning "See full HTML report at file://$PROJECT_ROOT/$OUTPUT_DIR/report.html"
  
  # If this is running in CI, output summary to console
  if [ -n "$CI" ]; then
    echo "##[group]Test Summary"
    cat "$OUTPUT_DIR/summary.txt"
    echo "##[endgroup]"
    
    if [ "$FAILED_TESTS" -gt 0 ]; then
      echo "##[group]Test Failures"
      cat "$OUTPUT_DIR/failures.txt"
      echo "##[endgroup]"
    fi
  else
    # Just output basic summary to console for local runs
    cat "$OUTPUT_DIR/summary.txt"
  fi
  
  return 0
}

cleanup() {
  print_section "Cleaning up test environment"
  
  # Stop mock GCP services if they were started
  if [ "$MOCK_GCP_SERVICES" = "true" ]; then
    python -m src.test.fixtures.mock_gcp_services stop || true
  fi
  
  # Stop Docker containers if they were started
  if [ "${RUN_DOCKER_SERVICES:-false}" = "true" ] && command -v docker &>/dev/null; then
    docker-compose -f "$PROJECT_ROOT/src/test/config/docker-compose-test.yml" down || true
  fi
  
  # Remove temporary files but preserve test results
  find "$PROJECT_ROOT" -name "*.pyc" -delete
  find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
  
  # Deactivate virtual environment if it was activated
  if [ -n "$VIRTUAL_ENV" ]; then
    deactivate || true
  fi
  
  print_success "Cleanup completed"
  return 0
}

main() {
  # Print banner
  echo "==============================================================="
  echo "  AIRFLOW MIGRATION PROJECT - INTEGRATION TEST EXECUTION"
  echo "  Build ID: $CI_BUILD_ID"
  echo "  Airflow Version: $AIRFLOW_VERSION"
  echo "  Date: $(date)"
  echo "==============================================================="
  
  # Initialize return code
  RETURN_CODE=0
  
  # Set up trap to ensure cleanup happens even on failure
  trap cleanup EXIT
  
  # Set up environment
  setup_environment
  if [ $? -ne 0 ]; then
    print_error "Environment setup failed"
    return 1
  fi
  
  # Set up mock services if enabled
  if [ "$MOCK_GCP_SERVICES" = "true" ]; then
    setup_mock_services
    if [ $? -ne 0 ]; then
      print_error "Mock service setup failed"
      return 1
    fi
  fi
  
  # Run integration tests
  run_tests
  RETURN_CODE=$?
  
  # Run GCP integration tests if enabled
  if [ "${RUN_GCP_TESTS:-false}" = "true" ]; then
    run_gcp_integration_tests
    # Only update return code if it was previously successful
    if [ $RETURN_CODE -eq 0 ]; then
      RETURN_CODE=$?
    fi
  fi
  
  # Generate reports
  generate_reports
  if [ $? -ne 0 ] && [ $RETURN_CODE -eq 0 ]; then
    RETURN_CODE=3
  fi
  
  # Final result
  if [ $RETURN_CODE -eq 0 ]; then
    print_success "Integration tests completed successfully"
  else
    print_error "Integration tests failed with exit code $RETURN_CODE"
  fi
  
  return $RETURN_CODE
}

# Execute the main function
main
exit $?