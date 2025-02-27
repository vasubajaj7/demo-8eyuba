# setuptools >=42.0.0
from setuptools import setup, find_packages  # package: setuptools
# Python standard library
from pathlib import Path  # package: pathlib
import io  # package: io
import os  # package: os

# Internal imports
from src.test.utils import VERSION  # src/test/utils/__init__.py


here = Path(__file__).parent.resolve()

# Get the long description from the README file
README = open(here / 'README.md', encoding='utf-8').read()


def get_requirements():
    """Parse test requirements from requirements-test.txt file for installation"""
    # Open requirements-test.txt file from the config directory
    requirements_path = here / 'src' / 'test' / 'config' / 'requirements-test.txt'
    with open(requirements_path, 'r') as f:
        # Read all lines from the file
        requirements = f.readlines()
    # Filter out empty lines and comments, and strip whitespace from each line
    requirements = [req.strip() for req in requirements if req.strip() and not req.startswith('#')]
    # Return the list of package requirements
    return requirements


def get_long_description():
    """Get long description from README.md for package documentation"""
    # Open README.md file
    readme_path = here / 'README.md'
    with open(readme_path, 'r', encoding='utf-8') as f:
        # Read content as string
        long_description = f.read()
    # Return content for use in setup function
    return long_description


package_metadata = {
    'name': 'airflow-composer2-migration-tests',
    'version': VERSION,
    'description': 'Test framework for validating migration from Apache Airflow 1.10.15 to Airflow 2.X on Cloud Composer 2',
    'author': 'DevOps Team',
    'author_email': 'devops@example.com',
    'url': 'https://github.com/example/airflow-composer2-migration',
    'classifiers': [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Framework :: Pytest',
        'Framework :: Apache Airflow',
        'Topic :: Software Development :: Testing'
    ],
    'python_requires': '>=3.8,<3.9',
    'package_data': {
        '': ['README.md', 'LICENSE'],
        'src.test.config': ['pytest.ini', '.coveragerc']
    },
    'entry_points': {
        'console_scripts': [
            'run-airflow-tests=src.test.ci.run_unit_tests:main',
            'run-airflow-integration-tests=src.test.ci.run_integration_tests:main',
            'test-environment-setup=src.test.ci.test_environment_setup:main',
            'test-report=src.test.ci.test_report:main'
        ],
        'pytest11': [
            'airflow_migration=src.test.fixtures.dag_fixtures',
            'airflow_compatibility=src.test.utils.airflow2_compatibility_utils'
        ]
    },
    'install_requires': get_requirements(),
    'extras_require': {
        'airflow1': ['apache-airflow==1.10.15', 'apache-airflow-backport-providers-google>=2020.10.5'],
        'airflow2': ['apache-airflow>=2.0.0,<3.0.0', 'apache-airflow-providers-google>=2.0.0',
                     'apache-airflow-providers-http>=2.0.0', 'apache-airflow-providers-postgres>=2.0.0'],
        'dev': ['black>=21.5b0', 'pylint>=2.8.0', 'mypy>=0.812', 'flake8>=3.9.0'],
        'migration': ['ast2json>=0.3.0', 'astunparse>=1.6.3', 'astor>=0.8.1']
    }
}

setup(
    name=package_metadata['name'],
    version=package_metadata['version'],
    description=package_metadata['description'],
    long_description=README,
    long_description_content_type='text/markdown',
    url=package_metadata['url'],
    author=package_metadata['author'],
    author_email=package_metadata['author_email'],
    classifiers=package_metadata['classifiers'],
    keywords='airflow,composer,migration,testing,gcp,workflow',
    packages=find_packages(where='src', include=['test*'], exclude=['test*.egg-info']),
    package_dir={'': 'src'},
    python_requires=package_metadata['python_requires'],
    install_requires=package_metadata['install_requires'],
    extras_require=package_metadata['extras_require'],
    include_package_data=True,
    zip_safe=False,
    entry_points=package_metadata['entry_points'],
    scripts=['src/test/ci/run_unit_tests.sh', 'src/test/ci/run_integration_tests.sh'],
)