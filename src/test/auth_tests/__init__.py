"""Authentication testing package for Cloud Composer 2 migration project.

This package contains test modules for validating authentication mechanisms
in the migrated Airflow 2.X environment. It covers API keys, Google SSO,
OAuth, and Service Account authentication methods as specified in the
technical requirements.
"""

__all__ = ['test_api_keys', 'test_google_sso', 'test_oauth', 'test_service_accounts']