# Contributing to the Cloud Composer Migration Project

## Introduction

Welcome to the Cloud Composer 1 to Cloud Composer 2 migration project! This document provides guidelines for contributing to the project to ensure a smooth and consistent migration process. We appreciate your interest in contributing and look forward to your involvement in modernizing our Airflow infrastructure.

## Contributor Code of Conduct

We are committed to providing a welcoming and inclusive environment for everyone involved in this project. As a contributor, you are expected to:  

- Use welcoming and inclusive language
- Be respectful of differing viewpoints and experiences
- Gracefully accept constructive criticism
- Focus on what is best for the community and project
- Show empathy towards other community members
- Provide detailed, constructive feedback on migration approaches

Unacceptable behaviors include harassment, trolling, derogatory comments, or any conduct that could reasonably be considered inappropriate in a professional setting. Instances of unacceptable behavior may be reported to project maintainers, who will review and investigate all complaints promptly and fairly.

## Getting Started

Before you begin contributing, please ensure you have a basic understanding of Apache Airflow, especially the differences between version 1.10.15 and 2.X. Familiarize yourself with Cloud Composer 1 and 2 as well.

### Project Overview

This project aims to migrate an existing Apache Airflow 1.10.15 codebase from Cloud Composer 1 to Cloud Composer 2 with Airflow 2.X. For a comprehensive overview, please refer to the [README.md](README.md).

### Repository Structure

The repository is organized as follows:
- `src/backend/`: Backend code including DAGs, plugins, and configurations
- `src/backend/dags/`: Airflow DAG definitions
- `src/backend/plugins/`: Custom Airflow plugins
- `src/backend/config/`: Configuration files for different environments
- `src/backend/migrations/`: Migration scripts and utilities
- `src/backend/scripts/`: Utility scripts for managing the migration
- `src/backend/docs/`: Documentation including migration guides
- `src/test/`: Comprehensive test suite

## Development Environment Setup

Setting up your development environment correctly is crucial for effective contribution.

### Prerequisites

Before setting up your development environment, ensure you have the following installed:
- Git
- Python 3.8 or higher
- Docker and Docker Compose
- Google Cloud SDK (optional, but recommended)
- Terraform (optional, for infrastructure changes)

### Local Development Environment

Follow these steps to set up your local development environment:

1. Clone the repository:
   ```bash
   git clone https://github.com/username/repo.git
   cd repo
   ```

2. Navigate to the backend directory:
   ```bash
   cd src/backend
   ```

3. Set up your environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with appropriate values
   ```

4. Start the local Airflow environment:
   ```bash
   docker-compose up -d
   ```

5. Access the Airflow UI at http://localhost:8080

### Installing Dependencies

Install development dependencies using pip:

```bash
pip install -r requirements-dev.txt
```

This will install all necessary development tools, including:
- pytest for testing
- black for code formatting
- pylint for linting
- pre-commit for Git hooks

### Setting Up Pre-commit Hooks

We use pre-commit hooks to ensure code quality before commits. Install them with:

```bash
pre-commit install
```

This will automatically run code formatting, linting, and other checks before each commit.

## Coding Standards

Adhering to consistent coding standards is essential for maintainability and readability of the codebase.

### Python Style Guide

We follow the PEP 8 style guide with some project-specific modifications:

- Line length: 100 characters maximum
- Use 4 spaces for indentation (no tabs)
- Use double quotes for strings unless single quotes are needed to avoid escaping
- Use f-strings for string formatting wherever possible
- Use meaningful variable and function names

We use Black // @version 23.1.0 for code formatting and pylint // @version 2.16.2 for linting.

### DAG Style Guidelines

For DAG files:

- Use the context manager pattern for DAG definitions in Airflow 2.X
- Follow Airflow 2.X import patterns (provider packages)
- Implement proper task dependencies using the `>>` and `<<` operators
- Include appropriate documentation in docstrings
- Define meaningful task IDs that describe the purpose of the task
- Use default_args dictionary for common parameters
- Explicitly set `catchup=False` unless backfilling is needed

Refer to [Best Practices](src/backend/docs/best_practices.md) for more detailed guidelines.

### Code Documentation

All code should be properly documented:

- Use docstrings for modules, classes, and functions
- Follow Google-style docstring format
- Include parameter descriptions, return types, and examples where appropriate
- Add comments for complex logic or non-obvious behavior
- Maintain up-to-date documentation in the relevant docs directory

### Commit Message Format

Use the following format for commit messages:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Where:
- `<type>` is one of: feat, fix, docs, style, refactor, test, chore
- `<scope>` is optional and represents the module affected
- `<subject>` is a short description of the change
- `<body>` provides detailed description (optional)
- `<footer>` references issues, PRs, etc. (optional)

Example:
```
feat(operators): migrate BigQueryOperator to new version

Update BigQueryOperator to use the new provider package format from Airflow 2.X.
Replace deprecated parameters and update import statements.

Resolves #123
```

## Branching Strategy

We use a structured branching strategy to manage contributions and releases.

### Branch Naming

Follow these naming conventions for branches:

- Feature branches: `feature/descriptive-name`
- Bug fix branches: `bugfix/issue-number-description`
- Migration branches: `migration/component-name`
- Documentation branches: `docs/topic-description`
- Release branches: `release/version-number`

### Main Branches

The repository maintains several important branches:

- `main`: The main development branch
- `release/*`: Release branches for specific versions
- `dev`: Integration branch for development features

Never commit directly to these branches. Always use feature branches and pull requests.

### Working with Branches

1. Create a new branch from `main` or `dev` for your changes
2. Make your changes in small, focused commits
3. Push your branch to the remote repository
4. Create a pull request for review
5. Address any feedback from reviewers
6. Once approved, your changes will be merged

## Testing Requirements

Comprehensive testing is crucial for ensuring the quality and reliability of the migration.

### Test Categories

We use the following test categories:

- Unit tests: Test individual components in isolation
- Integration tests: Test interactions between components
- Migration tests: Specifically designed to validate migration compatibility
- Performance tests: Measure and compare performance metrics
- Security tests: Validate security aspects
- End-to-end tests: Validate complete workflows

For a detailed explanation of each category, refer to [Testing Guidelines](src/test/docs/TESTING.md).

### Writing Tests

When contributing, you must include appropriate tests for your changes:

- Unit tests for new functions, operators, or hooks
- Integration tests for interactions with external systems
- Migration tests for any migration-specific changes
- Test both success and failure scenarios
- Use pytest fixtures for consistent testing
- Mock external dependencies appropriately

Tests should be placed in the corresponding directories under `src/test/`.

### Running Tests

To run tests locally:

```bash
# Run all tests
pytest src/test/

# Run specific test categories
pytest -m unit src/test/
pytest -m integration src/test/
pytest -m migration src/test/

# Run tests for a specific component
pytest src/test/dag_tests/
pytest src/test/operator_tests/
```

Tests must pass locally before pushing changes.

### CI/CD Integration

All tests will automatically run in the CI/CD pipeline when you create a pull request. The pipeline includes:

- Linting and code style checks
- Unit and integration tests
- Migration-specific tests
- Security scanning

All tests must pass in the CI/CD pipeline before your PR can be merged.

## Migration Guidelines

When contributing to the migration effort, follow these specific guidelines.

### Import Statement Updates

Airflow 2.X uses a different import structure with provider packages. Update import statements following these patterns:

```python
# Airflow 1.10.15
from airflow.operators.bash_operator import BashOperator

# Airflow 2.X
from airflow.operators.bash import BashOperator
```

For provider packages:

```python
# Airflow 1.10.15
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

# Airflow 2.X
from airflow.providers.google.cloud.hooks.gcs import GCSHook
```

### Operator Migration

When migrating operators:

1. Identify the correct provider package for the operator
2. Update the import statement
3. Update any renamed parameters
4. Check for behavior changes between versions
5. Update any deprecated features

Consult [Best Practices](src/backend/docs/best_practices.md) for specific operator migration guidance.

### TaskFlow API Adoption

Airflow 2.X introduces the TaskFlow API. Consider using it for Python functions:

```python
# Traditional approach in Airflow 1.10.15
def process_data(**kwargs):
    return {"count": 42}

process_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag
)

# TaskFlow API in Airflow 2.X
from airflow.decorators import task

@task
def process_data():
    return {"count": 42}

with DAG("example_dag") as dag:
    process_task = process_data()