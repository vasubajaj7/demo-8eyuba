"""
Initialization module for the security tests package that exports test classes and utilities
for testing authentication, authorization, data security, and secret handling in the
Airflow 2.X migration project.
"""

# Import authentication test classes and utilities
from .test_authentication import (
    TestGoogleSSO,
    TestServiceAccount,
    TestAPIKey,
    TestOAuth2,
    TestAirflow2AuthMigration,
    TestComposer2Auth,
    create_mock_jwt_token,
    mock_oauth_response,
    create_mock_service_account_token,
)

# Import authorization test classes and utilities
from .test_authorization import (
    TestRBACRoles,
    TestCustomRoles,
    TestAccessControl,
    TestComposer2Authorization,
    TestAirflow2AuthMigration as TestAirflow2AuthMigrationAuth,
    create_test_user,
    create_mock_security_manager,
)

# Import data security test classes and utilities
from .test_data_security import (
    TestDataEncryption,
    TestSecureStorage,
    TestDataAccessControls,
    TestSecureCommunication,
    TestAirflow2DataSecurityMigration,
    encrypt_test_data,
    decrypt_test_data,
)

# Import secret handling test classes and utilities
from .test_secret_handling import (
    TestSecretHandling,
    TestSecretHandlingIntegration,
    create_mock_secret,
)

# Export authentication test classes and utilities
__all__ = [
    "TestGoogleSSO",
    "TestServiceAccount",
    "TestAPIKey",
    "TestOAuth2",
    "TestAirflow2AuthMigration",
    "TestComposer2Auth",
    "TestRBACRoles",
    "TestCustomRoles",
    "TestAccessControl",
    "TestComposer2Authorization",
    "TestDataEncryption",
    "TestSecureStorage",
    "TestDataAccessControls",
    "TestSecureCommunication",
    "TestAirflow2DataSecurityMigration",
    "TestSecretHandling",
    "TestSecretHandlingIntegration",
    "create_mock_jwt_token",
    "mock_oauth_response",
    "create_mock_service_account_token",
    "create_test_user",
    "create_mock_security_manager",
    "encrypt_test_data",
    "decrypt_test_data",
    "create_mock_secret",
]