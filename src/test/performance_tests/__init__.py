"""
Package initialization file for the performance test suite of the Cloud Composer migration project.
Exposes key performance testing utilities, classes, and functions for comparing Airflow 1.10.15 and Airflow 2.X
performance during the migration process from Cloud Composer 1 to Cloud Composer 2.
"""

# Standard library imports
import os  # v Python standard library: Operating system interfaces for file paths and environment variables
import logging  # v Python standard library: Configure logging for the performance test package

# Internal module imports
from .test_dag_parsing_performance import measure_parsing_time  # src/test/performance_tests/test_dag_parsing_performance.py: Import function to measure DAG parsing time
from .test_dag_parsing_performance import DagParsingBenchmark  # src/test/performance_tests/test_dag_parsing_performance.py: Import class for DAG parsing benchmarks
from .test_task_execution_performance import measure_task_execution_time  # src/test/performance_tests/test_task_execution_performance.py: Import function to measure task execution time
from .test_task_execution_performance import PerformanceMetrics  # src/test/performance_tests/test_task_execution_performance.py: Import class for collecting performance metrics
from .test_task_execution_performance import TaskExecutionBenchmark  # src/test/performance_tests/test_task_execution_performance.py: Import class for task execution benchmarks
from .performance_comparison import PerformanceComparison  # src/test/performance_tests/performance_comparison.py: Import class for performance comparison between Airflow versions
from .performance_comparison import ComparisonMetrics  # src/test/performance_tests/performance_comparison.py: Import class for analyzing comparison metrics
from .performance_comparison import ComplexityScaling  # src/test/performance_tests/performance_comparison.py: Import class for analyzing performance scaling with complexity
from .performance_comparison import run_airflow1_benchmarks  # src/test/performance_tests/performance_comparison.py: Import function to run Airflow 1.X benchmarks
from .performance_comparison import run_airflow2_benchmarks  # src/test/performance_tests/performance_comparison.py: Import function to run Airflow 2.X benchmarks

# Initialize logger
logger = logging.getLogger('airflow.test.performance')

# Define global variables
PERFORMANCE_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REPORTS_DIR = os.path.join(PERFORMANCE_TEST_DIR, 'reports')
MAX_PARSING_TIME_SECONDS = 30.0
DEFAULT_ITERATIONS = 5
__version__ = "1.0.0"


def setup_package():
    """
    Initialize performance testing package and configure environment
    """
    # Set up logging configuration for performance tests
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create reports directory if it doesn't exist
    if not os.path.exists(DEFAULT_REPORTS_DIR):
        os.makedirs(DEFAULT_REPORTS_DIR)
        logger.info(f"Created reports directory: {DEFAULT_REPORTS_DIR}")

    # Initialize environment variables for performance testing
    logger.info("Setting up performance testing environment")


def run_performance_comparison(dag_files: list, iterations: int, output_dir: str) -> dict:
    """
    Runs a comprehensive performance comparison between Airflow 1.X and 2.X

    Args:
        dag_files: List of DAG file paths
        iterations: Number of iterations to run
        output_dir: Output directory for reports

    Returns:
        Comprehensive comparison results dictionary
    """
    # Initialize PerformanceComparison with provided DAGs and iterations
    performance_comparison = PerformanceComparison(dags=dag_files, iterations=iterations)

    # Run comparison between Airflow versions
    comparison_results = performance_comparison.run_comparison()

    # Validate results against performance requirements
    performance_comparison.validate_results()

    # Export results to specified output directory
    performance_comparison.export_results(output_format='json', output_dir=output_dir)

    # Return comparison results dictionary
    return comparison_results


# Expose function to measure DAG parsing time
__all__ = ['measure_parsing_time']

# Expose function to measure task execution time
__all__.append('measure_task_execution_time')

# Expose class for DAG parsing benchmarks
__all__.append('DagParsingBenchmark')

# Expose class for task execution benchmarks
__all__.append('TaskExecutionBenchmark')

# Expose class for collecting performance metrics
__all__.append('PerformanceMetrics')

# Expose class for performance comparison between Airflow versions
__all__.append('PerformanceComparison')

# Expose class for analyzing comparison metrics
__all__.append('ComparisonMetrics')

# Expose class for analyzing performance scaling with complexity
__all__.append('ComplexityScaling')

# Expose function to run Airflow 1.X benchmarks
__all__.append('run_airflow1_benchmarks')

# Expose function to run Airflow 2.X benchmarks
__all__.append('run_airflow2_benchmarks')

# Expose function to run comprehensive performance comparison
__all__.append('run_performance_comparison')

# Expose the version string for the performance test package
__all__.append('__version__')