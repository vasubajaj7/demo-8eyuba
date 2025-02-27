import setuptools
from setuptools import find_packages
from pathlib import Path
import io
import os

# Define paths and constants
here = Path(__file__).parent.resolve()
VERSION = "1.0.0"  # Main package version

def get_requirements():
    """Parse requirements from requirements.txt file for installation"""
    requirements_file = here / 'requirements.txt'
    try:
        with open(requirements_file) as f:
            requirements = [
                line.strip() for line in f
                if line.strip() and not line.startswith('#')
            ]
        return requirements
    except FileNotFoundError:
        # Return minimum requirements if file doesn't exist
        return [
            'apache-airflow>=2.5.1,<2.6.0',
            'apache-airflow-providers-google>=8.10.0',
            'apache-airflow-providers-http>=4.3.0',
            'apache-airflow-providers-postgres>=5.4.0',
            'apache-airflow-providers-celery>=3.1.0',
            'apache-airflow-providers-redis>=3.0.0',
            'google-cloud-storage>=2.8.0',
            'google-cloud-secret-manager>=2.15.0',
            'google-cloud-monitoring>=2.12.0',
            'psycopg2-binary>=2.9.5',
            'redis>=4.3.6',
            'jsonschema>=4.17.3',
            'pyyaml>=6.0',
            'pendulum>=2.1.2',
        ]

def get_long_description():
    """Get long description from README.md for package documentation"""
    readme_file = here / 'README.md'
    try:
        with io.open(readme_file, encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return "Apache Airflow 2.X migration project for Cloud Composer 2"

setuptools.setup(
    name="airflow-composer2-migration",
    version=VERSION,
    description="Migration project from Apache Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="DevOps Team",
    author_email="devops@example.com",
    url="https://github.com/example/airflow-composer2-migration",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    keywords="airflow,composer,migration,gcp,workflow",
    packages=find_packages(where="src", include=["backend*"], exclude=["test*"]),
    package_dir={"": "src"},
    python_requires=">=3.8,<3.9",
    install_requires=[
        'apache-airflow>=2.5.1,<2.6.0',
        'apache-airflow-providers-google>=8.10.0',
        'apache-airflow-providers-http>=4.3.0',
        'apache-airflow-providers-postgres>=5.4.0',
        'apache-airflow-providers-celery>=3.1.0',
        'apache-airflow-providers-redis>=3.0.0',
        'google-cloud-storage>=2.8.0',
        'google-cloud-secret-manager>=2.15.0',
        'google-cloud-monitoring>=2.12.0',
        'psycopg2-binary>=2.9.5',
        'redis>=4.3.6',
        'jsonschema>=4.17.3',
        'pyyaml>=6.0',
        'pendulum>=2.1.2',
    ],
    entry_points={
        "apache_airflow_provider": [
            "provider_info=src.backend.providers:discover_providers",
        ],
        "console_scripts": [
            "validate-dags=src.backend.scripts.validate_dags:main",
            "import-variables=src.backend.scripts.import_variables:main",
            "import-connections=src.backend.scripts.import_connections:main",
            "setup-composer=src.backend.scripts.setup_composer:main",
            "deploy-dags=src.backend.scripts.deploy_dags:main",
            "backup-metadata=src.backend.scripts.backup_metadata:main",
            "restore-metadata=src.backend.scripts.restore_metadata:main",
            "migrate-dags=src.backend.migrations.migration_airflow1_to_airflow2:main",
        ],
    },
    extras_require={
        "dev": [
            "pytest>=7.3.1",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.10.0",
            "pytest-airflow>=0.3.0",
            "black>=23.3.0",
            "pylint>=2.17.4",
            "flake8>=6.0.0",
            "mypy>=1.3.0",
            "isort>=5.12.0",
            "pre-commit>=3.3.2",
        ],
        "test": [
            "pytest>=7.3.1",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.10.0",
            "pytest-airflow>=0.3.0",
            "pytest-timeout>=2.1.0",
        ],
        "docs": [
            "sphinx>=7.0.1",
            "sphinx-rtd-theme>=1.2.0",
            "sphinx-autoapi>=2.1.0",
            "myst-parser>=2.0.0",
        ],
        "security": [
            "bandit>=1.7.5",
            "safety>=2.3.5",
        ],
        "migration": [
            "ast2json>=0.3.0",
            "astunparse>=1.6.3",
            "astor>=0.8.1",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["README.md", "LICENSE"],
        "src.backend.config": ["*.json"],
    },
    zip_safe=False,
    scripts=[
        "src/backend/scripts/validate_dags.py",
        "src/backend/scripts/import_variables.py",
        "src/backend/scripts/import_connections.py",
        "src/backend/scripts/backup_metadata.py",
        "src/backend/scripts/restore_metadata.py",
        "src/backend/scripts/rotate_secrets.py",
        "src/backend/scripts/setup_composer.py",
        "src/backend/scripts/deploy_dags.py",
    ],
)