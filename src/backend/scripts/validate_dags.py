#!/usr/bin/env python
"""
Script for validating Apache Airflow DAG files for compatibility with Airflow 2.X during migration 
from Airflow 1.10.15 to Cloud Composer 2. It analyzes DAGs for deprecated features, import statements, 
and structural issues, generating detailed reports with migration guidance.
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime
import re
import glob
from contextlib import contextmanager

# Airflow import
import airflow

# Internal imports
from ..dags.utils.validation_utils import (
    validate_dag_file,
    check_deprecated_features,
    VALIDATION_LEVELS,
    DEFAULT_VALIDATION_LEVEL,
    DAGValidator
)
from ..dags.utils.gcp_utils import gcs_upload_file, gcs_file_exists

# Set up logger
logger = logging.getLogger('airflow.scripts.validate_dags')

# Constants
DEFAULT_OUTPUT_FORMAT = 'json'
VALID_OUTPUT_FORMATS = ['json', 'text', 'html', 'csv']
DEFAULT_REPORT_NAME = 'airflow2_compatibility_report'


class ValidationContext:
    """Context manager for setting up validation environment"""
    
    def __init__(self):
        """Initialize validation context"""
        self.original_path = os.getcwd()
        self.temp_paths = []
        
    def __enter__(self):
        """Set up validation environment"""
        # Configure Python path for Airflow
        if 'PYTHONPATH' in os.environ:
            os.environ['PYTHONPATH'] = os.path.dirname(os.path.abspath(__file__)) + ':' + os.environ['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = os.path.dirname(os.path.abspath(__file__))
            
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up validation environment"""
        # Restore original directory
        os.chdir(self.original_path)
        
        # Clean up any temporary files or environments
        for path in self.temp_paths:
            if os.path.exists(path):
                try:
                    if os.path.isdir(path):
                        os.rmdir(path)
                    else:
                        os.remove(path)
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary path {path}: {str(e)}")
                    
        return None


def find_dag_files(directory_path, recursive=True):
    """
    Recursively finds all Python files in a directory that might contain DAGs
    
    Args:
        directory_path: Path to the directory to search
        recursive: Whether to search subdirectories
        
    Returns:
        List of Python file paths that might contain DAGs
    """
    if not os.path.isdir(directory_path):
        logger.error(f"Directory not found: {directory_path}")
        return []
    
    python_files = []
    
    # Check for .airflowignore file
    ignore_patterns = []
    airflow_ignore_path = os.path.join(directory_path, '.airflowignore')
    if os.path.exists(airflow_ignore_path):
        try:
            with open(airflow_ignore_path, 'r') as f:
                ignore_patterns = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            logger.info(f"Found .airflowignore with {len(ignore_patterns)} patterns")
        except Exception as e:
            logger.warning(f"Error reading .airflowignore: {str(e)}")
    
    # Find Python files
    if recursive:
        # Walk through directory and subdirectories
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, directory_path)
                    
                    # Check if file should be ignored
                    if any(re.match(pattern, rel_path) for pattern in ignore_patterns):
                        logger.debug(f"Ignoring file based on .airflowignore: {rel_path}")
                        continue
                    
                    python_files.append(file_path)
    else:
        # Only check files in the specified directory
        pattern = os.path.join(directory_path, '*.py')
        for file_path in glob.glob(pattern):
            rel_path = os.path.relpath(file_path, directory_path)
            
            # Check if file should be ignored
            if any(re.match(pattern, rel_path) for pattern in ignore_patterns):
                logger.debug(f"Ignoring file based on .airflowignore: {rel_path}")
                continue
            
            python_files.append(file_path)
    
    logger.info(f"Found {len(python_files)} Python files in {directory_path}")
    return python_files


def validate_dag_files(file_paths, validation_level=DEFAULT_VALIDATION_LEVEL):
    """
    Validates multiple DAG files and aggregates results
    
    Args:
        file_paths: List of file paths to validate
        validation_level: Validation level to use
        
    Returns:
        Validation results with errors, warnings, and statistics
    """
    results = {
        'files_validated': len(file_paths),
        'success': True,
        'files': [],
        'summary': {
            'total_files': len(file_paths),
            'passed_files': 0,
            'failed_files': 0,
            'total_dags': 0,
            'compatible_dags': 0,
            'incompatible_dags': 0,
            'error_count': 0,
            'warning_count': 0,
            'info_count': 0,
            'categories': {}
        }
    }
    
    # Create DAGValidator with specified validation level
    validator = DAGValidator(validation_level=validation_level)
    
    # Track issues by category for summary
    category_counts = {}
    
    # Process each file
    for file_path in file_paths:
        try:
            logger.info(f"Validating file: {file_path}")
            file_result = validate_dag_file(file_path, validation_level)
            
            # Update overall success flag
            if not file_result['success']:
                results['success'] = False
                results['summary']['failed_files'] += 1
            else:
                results['summary']['passed_files'] += 1
            
            # Count DAGs
            dag_count = len(file_result.get('dags', []))
            results['summary']['total_dags'] += dag_count
            
            # Count compatible/incompatible DAGs
            if file_result['success']:
                results['summary']['compatible_dags'] += dag_count
            else:
                results['summary']['incompatible_dags'] += dag_count
            
            # Count issues
            results['summary']['error_count'] += len(file_result['issues']['errors'])
            results['summary']['warning_count'] += len(file_result['issues']['warnings'])
            results['summary']['info_count'] += len(file_result['issues']['info'])
            
            # Categorize issues
            for severity in ['errors', 'warnings', 'info']:
                for issue in file_result['issues'][severity]:
                    component = issue.get('component', 'unknown')
                    category = component.split('.')[0] if '.' in component else component
                    
                    if category not in category_counts:
                        category_counts[category] = {'errors': 0, 'warnings': 0, 'info': 0}
                    
                    category_counts[category][severity] += 1
            
            # Add file result to overall results
            results['files'].append({
                'file_path': file_path,
                'success': file_result['success'],
                'validation': file_result
            })
            
        except Exception as e:
            logger.error(f"Error validating file {file_path}: {str(e)}")
            results['success'] = False
            results['summary']['failed_files'] += 1
            results['files'].append({
                'file_path': file_path,
                'success': False,
                'error': str(e),
                'validation': {
                    'issues': {
                        'errors': [{'component': 'validation', 'message': str(e)}],
                        'warnings': [],
                        'info': []
                    }
                }
            })
    
    # Add categories to summary
    results['summary']['categories'] = category_counts
    
    return results


def generate_report(validation_results, output_format=DEFAULT_OUTPUT_FORMAT, output_file=None):
    """
    Generates a formatted validation report based on validation results
    
    Args:
        validation_results: Results from validating DAG files
        output_format: Format for the report (json, text, html, csv)
        output_file: Path where the report should be saved (local or GCS)
        
    Returns:
        True if report was successfully generated
    """
    if output_format not in VALID_OUTPUT_FORMATS:
        logger.error(f"Invalid output format: {output_format}. Must be one of {VALID_OUTPUT_FORMATS}")
        return False
    
    try:
        # Format the results
        formatter = ReportFormatter(validation_results, output_format)
        formatted_report = formatter.format()
        
        # Output the report
        if output_file:
            # Check if GCS path
            if output_file.startswith('gs://'):
                # Extract bucket and object name
                bucket_name, object_name = output_file[5:].split('/', 1)
                
                # Create a temporary file
                temp_file = f"/tmp/airflow2_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{output_format}"
                with open(temp_file, 'w') as f:
                    f.write(formatted_report)
                
                # Upload to GCS
                try:
                    gcs_upload_file(temp_file, bucket_name, object_name)
                    logger.info(f"Report uploaded to {output_file}")
                    
                    # Clean up temp file
                    os.remove(temp_file)
                except Exception as e:
                    logger.error(f"Failed to upload report to GCS: {str(e)}")
                    return False
            else:
                # Ensure directory exists
                os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
                
                # Local file
                with open(output_file, 'w') as f:
                    f.write(formatted_report)
                logger.info(f"Report saved to {output_file}")
        else:
            # Print to stdout
            print(formatted_report)
        
        return True
    
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        return False


def create_airflow2_compatibility_report(dag_directory, output_file=None,
                                       output_format=DEFAULT_OUTPUT_FORMAT,
                                       validation_level=DEFAULT_VALIDATION_LEVEL):
    """
    Creates a comprehensive report about Airflow 2.X compatibility issues
    
    Args:
        dag_directory: Directory containing DAG files
        output_file: File to write the report to
        output_format: Format for the report (json, text, html, csv)
        validation_level: Validation level to use
        
    Returns:
        True if report was successfully generated
    """
    # Validate inputs
    if not os.path.isdir(dag_directory):
        logger.error(f"Directory not found: {dag_directory}")
        return False
    
    if output_format not in VALID_OUTPUT_FORMATS:
        logger.error(f"Invalid output format: {output_format}. Must be one of {VALID_OUTPUT_FORMATS}")
        return False
    
    try:
        # Find all DAG files
        dag_files = find_dag_files(dag_directory, recursive=True)
        
        if not dag_files:
            logger.error(f"No Python files found in {dag_directory}")
            return False
        
        # Validate all DAG files
        validation_results = validate_dag_files(dag_files, validation_level)
        
        # Enhance results with migration guidance and version compatibility info
        validation_results['airflow_versions'] = {
            'source_version': '1.10.15',
            'target_version': '2.X',
            'composer_target': 'Cloud Composer 2'
        }
        
        validation_results['migration_guidance'] = {
            'general_guidance': [
                "Update import paths according to Airflow 2.X conventions",
                "Remove provide_context=True from PythonOperators",
                "Ensure callback functions accept a 'context' parameter",
                "Consider adopting TaskFlow API for Python functions",
                "Check connection types and install required provider packages"
            ],
            'provider_packages': [
                "apache-airflow-providers-google for GCP integrations",
                "apache-airflow-providers-postgres for PostgreSQL connections",
                "apache-airflow-providers-http for HTTP connections"
            ],
            'documentation_links': [
                "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-from-1-10/index.html",
                "https://cloud.google.com/composer/docs/composer-2/composer-1-to-2-overview"
            ]
        }
        
        # Add timestamp
        validation_results['generated_at'] = datetime.now().isoformat()
        validation_results['validation_level'] = validation_level
        
        # Add summary statistics
        validation_results['summary'] = summarize_results(validation_results)
        
        # Generate and save the report
        return generate_report(validation_results, output_format, output_file)
    
    except Exception as e:
        logger.error(f"Error creating Airflow 2.X compatibility report: {str(e)}")
        return False


def summarize_results(validation_results):
    """
    Creates a concise summary of validation results
    
    Args:
        validation_results: Results from validating DAG files
        
    Returns:
        Summary statistics and categorized issues
    """
    summary = validation_results.get('summary', {})
    
    # If summary is already calculated, return it
    if summary and 'categories' in summary:
        return summary
    
    # Initialize summary
    summary = {
        'total_files': len(validation_results.get('files', [])),
        'passed_files': 0,
        'failed_files': 0,
        'total_dags': 0,
        'compatible_dags': 0,
        'incompatible_dags': 0,
        'error_count': 0,
        'warning_count': 0,
        'info_count': 0,
        'categories': {},
        'most_common_issues': []
    }
    
    # Count files and issues
    issue_counts = {}
    for file_result in validation_results.get('files', []):
        if file_result.get('success', False):
            summary['passed_files'] += 1
        else:
            summary['failed_files'] += 1
        
        # Count DAGs
        validation = file_result.get('validation', {})
        dag_count = len(validation.get('dags', []))
        summary['total_dags'] += dag_count
        
        if file_result.get('success', False):
            summary['compatible_dags'] += dag_count
        else:
            summary['incompatible_dags'] += dag_count
        
        # Count and categorize issues
        for severity in ['errors', 'warnings', 'info']:
            issues = validation.get('issues', {}).get(severity, [])
            summary[f"{severity[:-1]}_count"] += len(issues)
            
            # Group by component
            for issue in issues:
                component = issue.get('component', 'unknown')
                category = component.split('.')[0] if '.' in component else component
                
                if category not in summary['categories']:
                    summary['categories'][category] = {'errors': 0, 'warnings': 0, 'info': 0}
                
                summary['categories'][category][severity] += 1
                
                # Track occurrences of each issue
                issue_text = issue.get('message', '')
                if issue_text:
                    key = f"{severity}: {issue_text}"
                    issue_counts[key] = issue_counts.get(key, 0) + 1
    
    # Get most common issues
    most_common = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    summary['most_common_issues'] = [{'issue': k, 'count': v} for k, v in most_common]
    
    return summary


def format_as_text(validation_results):
    """
    Formats validation results as plain text report
    
    Args:
        validation_results: Results from validating DAG files
        
    Returns:
        Formatted text report
    """
    summary = validation_results.get('summary', {})
    
    # Create header
    text = "Airflow 2.X Compatibility Report\n"
    text += "==============================\n\n"
    text += f"Generated at: {validation_results.get('generated_at', datetime.now().isoformat())}\n"
    text += f"Validation level: {validation_results.get('validation_level', DEFAULT_VALIDATION_LEVEL)}\n\n"
    
    # Add summary
    text += "Summary\n"
    text += "-------\n"
    text += f"Files analyzed: {summary.get('total_files', 0)}\n"
    text += f"Compatible files: {summary.get('passed_files', 0)}\n"
    text += f"Incompatible files: {summary.get('failed_files', 0)}\n"
    text += f"Total DAGs: {summary.get('total_dags', 0)}\n"
    text += f"Compatible DAGs: {summary.get('compatible_dags', 0)}\n"
    text += f"Incompatible DAGs: {summary.get('incompatible_dags', 0)}\n\n"
    
    text += "Issues\n"
    text += "------\n"
    text += f"Errors: {summary.get('error_count', 0)}\n"
    text += f"Warnings: {summary.get('warning_count', 0)}\n"
    text += f"Info: {summary.get('info_count', 0)}\n\n"
    
    # Add most common issues
    text += "Most Common Issues\n"
    text += "-----------------\n"
    for issue in summary.get('most_common_issues', []):
        text += f"- {issue['issue']} (Count: {issue['count']})\n"
    text += "\n"
    
    # Add migration guidance
    text += "Migration Guidance\n"
    text += "-----------------\n"
    for item in validation_results.get('migration_guidance', {}).get('general_guidance', []):
        text += f"- {item}\n"
    text += "\n"
    
    text += "Required Provider Packages\n"
    text += "-------------------------\n"
    for package in validation_results.get('migration_guidance', {}).get('provider_packages', []):
        text += f"- {package}\n"
    text += "\n"
    
    # Add file details
    text += "File Details\n"
    text += "------------\n"
    for file_result in validation_results.get('files', []):
        file_path = file_result.get('file_path', 'Unknown')
        status = "PASS" if file_result.get('success', False) else "FAIL"
        
        text += f"\n{file_path}: {status}\n"
        
        validation = file_result.get('validation', {})
        
        # Add DAG info
        dags = validation.get('dags', [])
        if dags:
            text += f"  DAGs: {len(dags)}\n"
            for dag in dags:
                dag_id = dag.get('dag_id', 'Unknown')
                dag_status = "Compatible" if dag.get('validation', {}).get('success', False) else "Incompatible"
                text += f"  - {dag_id}: {dag_status}\n"
        
        # Add issues
        issues = validation.get('issues', {})
        
        for severity in ['errors', 'warnings', 'info']:
            severity_issues = issues.get(severity, [])
            if severity_issues:
                text += f"\n  {severity.capitalize()}:\n"
                for issue in severity_issues:
                    component = issue.get('component', '')
                    message = issue.get('message', '')
                    text += f"  - {component}: {message}\n"
    
    return text


def format_as_html(validation_results):
    """
    Formats validation results as HTML report
    
    Args:
        validation_results: Results from validating DAG files
        
    Returns:
        HTML formatted report
    """
    summary = validation_results.get('summary', {})
    
    # Create basic HTML structure
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Airflow 2.X Compatibility Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; line-height: 1.4; }
        h1, h2, h3, h4 { color: #333; }
        .summary { background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .file-details { margin-top: 20px; border: 1px solid #ddd; padding: 10px; border-radius: 5px; }
        .success { color: green; }
        .failure { color: red; }
        .error { color: red; }
        .warning { color: orange; }
        .info { color: blue; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        .toggle-button { cursor: pointer; background: #e2e2e2; padding: 5px 10px; border-radius: 3px; display: inline-block; }
        .hidden { display: none; }
    </style>
    <script>
        function toggleSection(id) {
            var section = document.getElementById(id);
            if (section.classList.contains('hidden')) {
                section.classList.remove('hidden');
            } else {
                section.classList.add('hidden');
            }
        }
    </script>
</head>
<body>
    <h1>Airflow 2.X Compatibility Report</h1>
"""

    # Add summary section
    html += f"""
    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Generated at:</strong> {validation_results.get('generated_at', datetime.now().isoformat())}</p>
        <p><strong>Validation level:</strong> {validation_results.get('validation_level', DEFAULT_VALIDATION_LEVEL)}</p>
        
        <h3>Statistics</h3>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
            </tr>
            <tr>
                <td>Files analyzed</td>
                <td>{summary.get('total_files', 0)}</td>
            </tr>
            <tr>
                <td>Compatible files</td>
                <td><span class="success">{summary.get('passed_files', 0)}</span></td>
            </tr>
            <tr>
                <td>Incompatible files</td>
                <td><span class="failure">{summary.get('failed_files', 0)}</span></td>
            </tr>
            <tr>
                <td>Total DAGs</td>
                <td>{summary.get('total_dags', 0)}</td>
            </tr>
            <tr>
                <td>Compatible DAGs</td>
                <td><span class="success">{summary.get('compatible_dags', 0)}</span></td>
            </tr>
            <tr>
                <td>Incompatible DAGs</td>
                <td><span class="failure">{summary.get('incompatible_dags', 0)}</span></td>
            </tr>
            <tr>
                <td>Total issues</td>
                <td>{summary.get('error_count', 0) + summary.get('warning_count', 0) + summary.get('info_count', 0)}</td>
            </tr>
            <tr>
                <td>Errors</td>
                <td><span class="error">{summary.get('error_count', 0)}</span></td>
            </tr>
            <tr>
                <td>Warnings</td>
                <td><span class="warning">{summary.get('warning_count', 0)}</span></td>
            </tr>
            <tr>
                <td>Info</td>
                <td><span class="info">{summary.get('info_count', 0)}</span></td>
            </tr>
        </table>
        
        <h3>Most Common Issues</h3>
        <ul>
"""

    # Add most common issues
    for issue in summary.get('most_common_issues', []):
        html += f"            <li>{issue['issue']} (Count: {issue['count']})</li>\n"
    
    html += """
        </ul>
        
        <h3>Migration Guidance</h3>
        <ul>
"""

    # Add migration guidance
    for item in validation_results.get('migration_guidance', {}).get('general_guidance', []):
        html += f"            <li>{item}</li>\n"
    
    html += """
        </ul>
        
        <h3>Required Provider Packages</h3>
        <ul>
"""

    # Add provider packages
    for package in validation_results.get('migration_guidance', {}).get('provider_packages', []):
        html += f"            <li>{package}</li>\n"
    
    html += """
        </ul>
        
        <h3>Documentation Links</h3>
        <ul>
"""

    # Add documentation links
    for link in validation_results.get('migration_guidance', {}).get('documentation_links', []):
        html += f"            <li><a href='{link}' target='_blank'>{link}</a></li>\n"
    
    html += """
        </ul>
    </div>
    
    <h2>File Details</h2>
"""

    # Add file details
    for i, file_result in enumerate(validation_results.get('files', [])):
        file_path = file_result.get('file_path', 'Unknown')
        status = "PASS" if file_result.get('success', False) else "FAIL"
        status_class = "success" if file_result.get('success', False) else "failure"
        section_id = f"file-section-{i}"
        
        html += f"""
    <div class="file-details">
        <h3>
            <span class="{status_class}">[{status}]</span> {file_path}
            <span class="toggle-button" onclick="toggleSection('{section_id}')">Toggle Details</span>
        </h3>
        
        <div id="{section_id}" class="hidden">
"""
        
        validation = file_result.get('validation', {})
        
        # Add DAG info
        dags = validation.get('dags', [])
        if dags:
            html += f"""
            <h4>DAGs ({len(dags)})</h4>
            <table>
                <tr>
                    <th>DAG ID</th>
                    <th>Status</th>
                </tr>
"""
            
            for dag in dags:
                dag_id = dag.get('dag_id', 'Unknown')
                dag_success = dag.get('validation', {}).get('success', False)
                dag_status = "Compatible" if dag_success else "Incompatible"
                dag_class = "success" if dag_success else "failure"
                
                html += f"""
                <tr>
                    <td>{dag_id}</td>
                    <td><span class="{dag_class}">{dag_status}</span></td>
                </tr>
"""
            
            html += """
            </table>
"""
        
        # Add issues
        issues = validation.get('issues', {})
        
        for severity in ['errors', 'warnings', 'info']:
            severity_issues = issues.get(severity, [])
            if severity_issues:
                html += f"""
            <h4>{severity.capitalize()} ({len(severity_issues)})</h4>
            <table>
                <tr>
                    <th>Component</th>
                    <th>Message</th>
                </tr>
"""
                
                for issue in severity_issues:
                    component = issue.get('component', '')
                    message = issue.get('message', '')
                    
                    html += f"""
                <tr>
                    <td>{component}</td>
                    <td class="{severity[:-1]}">{message}</td>
                </tr>
"""
                
                html += """
            </table>
"""
        
        html += """
        </div>
    </div>
"""
    
    # Close HTML
    html += """
</body>
</html>
"""
    
    return html


def format_as_csv(validation_results):
    """
    Formats validation results as CSV data
    
    Args:
        validation_results: Results from validating DAG files
        
    Returns:
        CSV formatted report
    """
    csv_rows = []
    
    # Add header row
    csv_rows.append("file_path,dag_id,component,severity,message")
    
    # Add data rows
    for file_result in validation_results.get('files', []):
        file_path = file_result.get('file_path', 'Unknown').replace(',', '_')
        validation = file_result.get('validation', {})
        
        # Add issues
        issues = validation.get('issues', {})
        
        for severity in ['errors', 'warnings', 'info']:
            for issue in issues.get(severity, []):
                component = issue.get('component', '').replace(',', '_')
                message = issue.get('message', '').replace(',', '_').replace('\n', ' ')
                
                csv_rows.append(f"{file_path},,{component},{severity[:-1]},{message}")
        
        # Add DAG-specific issues
        for dag in validation.get('dags', []):
            dag_id = dag.get('dag_id', 'Unknown').replace(',', '_')
            dag_validation = dag.get('validation', {})
            
            for severity in ['errors', 'warnings', 'info']:
                for issue in dag_validation.get('issues', {}).get(severity, []):
                    component = issue.get('component', '').replace(',', '_')
                    message = issue.get('message', '').replace(',', '_').replace('\n', ' ')
                    
                    csv_rows.append(f"{file_path},{dag_id},{component},{severity[:-1]},{message}")
    
    return '\n'.join(csv_rows)


class ReportFormatter:
    """Formats validation results into various output formats"""
    
    def __init__(self, validation_results, output_format=DEFAULT_OUTPUT_FORMAT):
        """
        Initialize with validation results and output format
        
        Args:
            validation_results: Results from validating DAG files
            output_format: Format for the report (json, text, html, csv)
        """
        self.validation_results = validation_results
        
        if output_format not in VALID_OUTPUT_FORMATS:
            logger.warning(f"Invalid output format: {output_format}. Using {DEFAULT_OUTPUT_FORMAT}")
            self.output_format = DEFAULT_OUTPUT_FORMAT
        else:
            self.output_format = output_format
    
    def format(self):
        """
        Format results according to specified output format
        
        Returns:
            Formatted report content
        """
        if self.output_format == 'json':
            return self.format_as_json()
        elif self.output_format == 'text':
            return self.format_as_text()
        elif self.output_format == 'html':
            return self.format_as_html()
        elif self.output_format == 'csv':
            return self.format_as_csv()
        else:
            # Default to JSON if format is invalid
            return self.format_as_json()
    
    def format_as_json(self):
        """
        Format results as JSON
        
        Returns:
            JSON formatted string
        """
        return json.dumps(self.validation_results, indent=2)
    
    def format_as_text(self):
        """
        Format results as plain text
        
        Returns:
            Text formatted report
        """
        return format_as_text(self.validation_results)
    
    def format_as_html(self):
        """
        Format results as HTML
        
        Returns:
            HTML formatted report
        """
        return format_as_html(self.validation_results)
    
    def format_as_csv(self):
        """
        Format results as CSV
        
        Returns:
            CSV formatted report
        """
        return format_as_csv(self.validation_results)


def setup_logging(verbose=False):
    """
    Configures logging for the script
    
    Args:
        verbose: Whether to enable verbose logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    root_logger.addHandler(console_handler)
    
    # Configure our module's logger
    logger.setLevel(log_level)


def main():
    """
    Main entry point for the script
    
    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    parser = argparse.ArgumentParser(
        description="Validate Apache Airflow DAG files for compatibility with Airflow 2.X"
    )
    
    parser.add_argument(
        'directory',
        help='Directory containing DAG files to validate'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output file path for the report (local or GCS path starting with gs://)'
    )
    
    parser.add_argument(
        '-f', '--format',
        choices=VALID_OUTPUT_FORMATS,
        default=DEFAULT_OUTPUT_FORMAT,
        help=f'Output format for the report (default: {DEFAULT_OUTPUT_FORMAT})'
    )
    
    parser.add_argument(
        '-l', '--level',
        choices=list(VALIDATION_LEVELS.keys()),
        default=DEFAULT_VALIDATION_LEVEL,
        help=f'Validation level (default: {DEFAULT_VALIDATION_LEVEL})'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.verbose)
    
    # Validate input directory
    if not os.path.isdir(args.directory):
        logger.error(f"Directory not found: {args.directory}")
        return 1
    
    # If output file is specified, ensure parent directory exists
    if args.output and not args.output.startswith('gs://'):
        output_dir = os.path.dirname(os.path.abspath(args.output))
        os.makedirs(output_dir, exist_ok=True)
    
    # Create compatibility report
    logger.info(f"Creating Airflow 2.X compatibility report for DAGs in {args.directory}")
    success = create_airflow2_compatibility_report(
        dag_directory=args.directory,
        output_file=args.output,
        output_format=args.format,
        validation_level=args.level
    )
    
    if success:
        logger.info("Compatibility report created successfully")
        return 0
    else:
        logger.error("Failed to create compatibility report")
        return 1


if __name__ == "__main__":
    sys.exit(main())