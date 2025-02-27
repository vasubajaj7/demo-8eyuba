#!/usr/bin/env python3
"""
Test module that validates the Docker configuration for Apache Airflow 2.X migration by testing
syntax, security, and compatibility aspects of the Dockerfile in the project. Ensures the
containerized environment meets requirements for Cloud Composer 2 migration.
"""

import os  # Python standard library
import re  # Python standard library
import json  # Python standard library
import logging  # Python standard library
import time  # Python standard library
import subprocess  # Python standard library

import pytest  # pytest-6.0+
import docker  # docker-5.0.0+

from ..utils.assertion_utils import assert_dag_airflow2_compatible  # src/test/utils/assertion_utils.py
from ..utils.test_helpers import capture_logs  # src/test/utils/test_helpers.py
from ..fixtures.mock_data import MOCK_PROJECT_ID  # src/test/fixtures/mock_data.py

# Define the path to the Dockerfile
DOCKERFILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'backend', 'Dockerfile')

# Get a logger instance
logger = logging.getLogger('airflow.test.container.dockerfile')

# Define expected values and security patterns
EXPECTED_BASE_IMAGE = 'python:3.8-slim'
EXPECTED_AIRFLOW_VERSION = '2.5.1'
SECURITY_PATTERNS = [r'USER\s+airflow', r'RUN\s+pip\s+.*--no-cache', r'COPY\s+--chown=airflow']
INSECURE_PATTERNS = [r'USER\s+root', r'.*curl.*\s*\|\s*sh', r'.*wget.*\s*\|\s*sh']


@pytest.fixture
def dockerfile_content() -> str:
    """
    Pytest fixture that reads and returns the content of the Dockerfile

    Returns:
        str: Content of the Dockerfile as a string
    """
    # Check if DOCKERFILE_PATH exists
    if not os.path.exists(DOCKERFILE_PATH):
        # If file doesn't exist, raise FileNotFoundError with descriptive message
        raise FileNotFoundError(f"Dockerfile not found at: {DOCKERFILE_PATH}")

    # Open and read file content
    with open(DOCKERFILE_PATH, 'r') as f:
        content = f.read()

    # Return file content as string
    return content


@pytest.fixture
def docker_client():
    """
    Pytest fixture that provides a Docker client for interacting with Docker daemon

    Returns:
        docker.client.DockerClient: Docker client instance
    """
    # Import docker module
    # Try to create and return a Docker client
    try:
        client = docker.from_env()
        client.ping()
        return client
    # If connection fails, skip the test with descriptive message
    except Exception as e:
        pytest.skip(f"Docker daemon not available: {e}")


def parse_dockerfile_instructions(content: str) -> list:
    """
    Parses Dockerfile content into a list of instructions

    Args:
        content (str): Dockerfile content

    Returns:
        list: List of parsed Dockerfile instructions
    """
    # Split content into lines
    lines = content.splitlines()
    instructions = []
    i = 0
    # Group continuation lines (ending with \)
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith('#'):
            i += 1
            continue
        # Parse each instruction into type and arguments
        if line.endswith('\\'):
            instruction = [line[:-1].strip()]
            i += 1
            while i < len(lines) and lines[i].strip().endswith('\\'):
                instruction.append(lines[i].strip()[:-1].strip())
                i += 1
            if i < len(lines):
                instruction.append(lines[i].strip())
            i += 1
            line = ' '.join(instruction)
        else:
            i += 1
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            instruction_type, arguments = parts
        else:
            instruction_type = parts[0]
            arguments = ''
        instructions.append((instruction_type.upper(), arguments.strip()))
    # Return list of (instruction_type, arguments) tuples
    return instructions


def find_instruction(instructions: list, instruction_type: str) -> list:
    """
    Finds all instances of a specific instruction in the Dockerfile

    Args:
        instructions (list): List of instructions
        instruction_type (str): Instruction type

    Returns:
        list: List of matching instruction arguments
    """
    # Filter instructions list by instruction_type
    return [args for instr_type, args in instructions if instr_type == instruction_type.upper()]


class TestDockerfileSyntax:
    """Test class for validating the syntax and structure of the Dockerfile"""

    def __init__(self):
        """Default constructor"""
        pass

    def test_dockerfile_exists(self):
        """Tests that the Dockerfile exists at the expected location"""
        # Assert that os.path.exists(DOCKERFILE_PATH) is True
        assert os.path.exists(DOCKERFILE_PATH)
        # Log success message if file exists
        logger.info(f"Dockerfile found at: {DOCKERFILE_PATH}")

    def test_base_image(self, dockerfile_content: str):
        """Verifies the Dockerfile uses the correct base image"""
        # Parse Dockerfile content using parse_dockerfile_instructions
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Find FROM instructions using find_instruction
        from_instructions = find_instruction(instructions, 'FROM')
        # Assert that the first FROM instruction uses python:3.8-slim
        assert from_instructions, "No FROM instruction found in Dockerfile"
        assert from_instructions[0] == EXPECTED_BASE_IMAGE, f"Base image should be {EXPECTED_BASE_IMAGE}"
        # Log base image verification result
        logger.info(f"Base image is correct: {EXPECTED_BASE_IMAGE}")

    def test_required_instructions(self, dockerfile_content: str):
        """Verifies the Dockerfile has all required instructions"""
        # Parse Dockerfile content
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Create set of instruction types in the Dockerfile
        instruction_types = {instr_type for instr_type, _ in instructions}
        # Define set of required instructions (FROM, RUN, COPY, ENV, USER, WORKDIR, EXPOSE, ENTRYPOINT)
        required_instructions = {'FROM', 'RUN', 'COPY', 'ENV', 'USER', 'WORKDIR', 'EXPOSE', 'ENTRYPOINT'}
        # Assert that all required instructions are present
        missing_instructions = required_instructions - instruction_types
        assert not missing_instructions, f"Missing required instructions: {missing_instructions}"
        # Log missing instructions if any
        logger.info("All required instructions are present")

    def test_multi_stage_build(self, dockerfile_content: str):
        """Verifies the Dockerfile uses multi-stage builds"""
        # Parse Dockerfile content
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Count FROM instructions using find_instruction
        from_instructions = find_instruction(instructions, 'FROM')
        # Assert that there are at least 2 FROM instructions
        assert len(from_instructions) >= 2, "Dockerfile should use multi-stage builds (at least 2 FROM instructions)"
        # Log multi-stage build verification result
        logger.info("Dockerfile uses multi-stage builds")

    def test_airflow_version(self, dockerfile_content: str):
        """Verifies the Dockerfile uses the correct Airflow version"""
        # Parse Dockerfile content
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Find ARG instructions defining AIRFLOW_VERSION
        arg_instructions = find_instruction(instructions, 'ARG')
        airflow_version_arg = next((arg for arg in arg_instructions if 'AIRFLOW_VERSION' in arg), None)
        # Assert AIRFLOW_VERSION is set to expected version (2.5.1)
        assert airflow_version_arg, "AIRFLOW_VERSION ARG not found"
        assert EXPECTED_AIRFLOW_VERSION in airflow_version_arg, f"AIRFLOW_VERSION should be {EXPECTED_AIRFLOW_VERSION}"

        # Find RUN instructions installing airflow
        run_instructions = find_instruction(instructions, 'RUN')
        airflow_install_command = next((run for run in run_instructions if 'pip install apache-airflow' in run), None)
        # Assert installation command includes correct version
        assert airflow_install_command, "No pip install apache-airflow command found"
        assert EXPECTED_AIRFLOW_VERSION in airflow_install_command, f"Airflow version should be {EXPECTED_AIRFLOW_VERSION}"
        # Log Airflow version verification result
        logger.info(f"Airflow version is correct: {EXPECTED_AIRFLOW_VERSION}")


class TestDockerfileSecurity:
    """Test class for validating security practices in the Dockerfile"""

    def __init__(self):
        """Default constructor"""
        pass

    def test_secure_practices(self, dockerfile_content: str):
        """Tests that the Dockerfile follows security best practices"""
        # For each pattern in SECURITY_PATTERNS:
        for pattern in SECURITY_PATTERNS:
            # Assert that the pattern exists in dockerfile_content
            assert re.search(pattern, dockerfile_content, re.MULTILINE), f"Dockerfile should contain: {pattern}"
            # Log security pattern verification results
            logger.info(f"Dockerfile contains secure practice: {pattern}")

    def test_avoid_insecure_practices(self, dockerfile_content: str):
        """Tests that the Dockerfile avoids insecure practices"""
        # For each pattern in INSECURE_PATTERNS:
        for pattern in INSECURE_PATTERNS:
            # If pattern is for USER root, allow it only if followed by USER airflow
            if pattern == r'USER\s+root':
                if re.search(r'USER\s+root', dockerfile_content) and not re.search(r'USER\s+airflow', dockerfile_content):
                    assert False, "Dockerfile should not contain USER root without subsequent USER airflow"
            # For other patterns, assert they don't exist in dockerfile_content
            else:
                assert not re.search(pattern, dockerfile_content, re.MULTILINE), f"Dockerfile should not contain: {pattern}"
            # Log insecure pattern verification results
            logger.info(f"Dockerfile avoids insecure practice: {pattern}")

    def test_no_plaintext_credentials(self, dockerfile_content: str):
        """Tests that the Dockerfile doesn't contain plaintext credentials"""
        # Define patterns for common credential keywords
        credential_patterns = [r'password\s*=\s*[\'"].*?[\'"]', r'secret\s*=\s*[\'"].*?[\'"]', r'api_key\s*=\s*[\'"].*?[\'"]']
        # For each credential pattern:
        for pattern in credential_patterns:
            # Check dockerfile_content doesn't contain plaintext credentials
            matches = re.findall(pattern, dockerfile_content, re.MULTILINE | re.IGNORECASE)
            # Allow ARG and ENV variables for credentials
            allowed_matches = []
            for match in matches:
                if 'ARG' in match or 'ENV' in match:
                    allowed_matches.append(match)
            # Assert that the pattern doesn't exist in dockerfile_content
            assert len(matches) == len(allowed_matches), f"Dockerfile should not contain plaintext credentials: {pattern}"
            # Log credential scanning results
            logger.info(f"Dockerfile doesn't contain plaintext credentials: {pattern}")

    def test_minimal_attack_surface(self, dockerfile_content: str):
        """Tests that the Dockerfile creates minimal attack surface"""
        # Check that unnecessary packages are removed
        assert re.search(r'apt-get\s+purge', dockerfile_content, re.MULTILINE), "Dockerfile should remove unnecessary packages"
        # Verify apt/pip cache is cleaned up
        assert re.search(r'apt-get\s+autoremove', dockerfile_content, re.MULTILINE), "Dockerfile should clean up apt cache"
        assert re.search(r'pip\s+cache\s+purge', dockerfile_content, re.MULTILINE), "Dockerfile should clean up pip cache"
        # Check that build tools are not present in final stage
        assert 'gcc' not in dockerfile_content, "Dockerfile should not contain gcc in final stage"
        assert 'make' not in dockerfile_content, "Dockerfile should not contain make in final stage"
        # Log attack surface minimization results
        logger.info("Dockerfile minimizes attack surface")


class TestDockerfileBuild:
    """Test class for validating the build process of the Dockerfile"""

    def __init__(self):
        """Default constructor"""
        pass

    def test_dockerfile_linting(self):
        """Tests Dockerfile through a linter for best practices"""
        # Use subprocess to run hadolint on DOCKERFILE_PATH
        try:
            result = subprocess.run(['hadolint', DOCKERFILE_PATH], capture_output=True, text=True, check=True)
            # Check return code for linting results
            # Assert linting passes with no errors
            assert result.returncode == 0, f"Dockerfile linting failed: {result.stderr}"
            # Log linting results
            logger.info("Dockerfile linting passed")
        # Skip test if hadolint is not installed
        except FileNotFoundError:
            pytest.skip("hadolint not installed")

    def test_docker_build(self, docker_client):
        """Tests that the Dockerfile can be built successfully"""
        # Skip test if docker_client is not available
        if not docker_client:
            pytest.skip("Docker daemon not available")
        # Set build context to the backend directory
        build_context = os.path.dirname(DOCKERFILE_PATH)
        # Use docker_client to build image with tag airflow-test:latest
        try:
            image, build_logs = docker_client.images.build(path=build_context, tag='airflow-test:latest', dockerfile='Dockerfile')
            # Assert build completes without errors
            assert image is not None, "Dockerfile build failed"
            # Log build test results
            logger.info("Dockerfile build succeeded")
        except Exception as e:
            # Assert build completes without errors
            assert False, f"Dockerfile build failed: {e}"
        finally:
            # Clean up by removing the test image
            try:
                docker_client.images.remove('airflow-test:latest', force=True)
            except Exception as e:
                logger.warning(f"Failed to remove test image: {e}")

    def test_stage_selection(self, docker_client, dockerfile_content: str):
        """Tests building specific stages of the multi-stage Dockerfile"""
        # Skip test if docker_client is not available
        if not docker_client:
            pytest.skip("Docker daemon not available")
        # Parse Dockerfile content to extract stage names
        instructions = parse_dockerfile_instructions(dockerfile_content)
        from_instructions = find_instruction(instructions, 'FROM')
        stage_names = [args.split(' ')[-1] for args in from_instructions if ' AS ' in args]
        # For each stage (airflow-base, airflow-builder, airflow-final, airflow-dev):
        for stage in stage_names:
            # Use docker_client to build image with --target option for the stage
            try:
                build_context = os.path.dirname(DOCKERFILE_PATH)
                image, build_logs = docker_client.images.build(path=build_context, tag=f'airflow-test:{stage}', dockerfile='Dockerfile', target=stage)
                # Assert stage builds complete without errors
                assert image is not None, f"Stage {stage} build failed"
                # Log stage building results
                logger.info(f"Stage {stage} build succeeded")
            except Exception as e:
                # Assert stage builds complete without errors
                assert False, f"Stage {stage} build failed: {e}"
            finally:
                # Clean up by removing test images
                try:
                    docker_client.images.remove(f'airflow-test:{stage}', force=True)
                except Exception as e:
                    logger.warning(f"Failed to remove test image for stage {stage}: {e}")


class TestDockerfileAirflow2Compatibility:
    """Test class for validating Airflow 2.X compatibility in the Dockerfile"""

    def __init__(self):
        """Default constructor"""
        pass

    def test_airflow2_packages(self, dockerfile_content: str):
        """Tests that the Dockerfile installs Airflow 2.X compatible packages"""
        # Parse Dockerfile content
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Find RUN instructions that install Python packages
        run_instructions = find_instruction(instructions, 'RUN')
        pip_install_commands = [run for run in run_instructions if 'pip install' in run]
        # Verify apache-airflow==2.X is installed
        assert any('apache-airflow==' in cmd for cmd in pip_install_commands), "apache-airflow==2.X not found"
        # Check for Airflow 2.X providers (apache-airflow-providers-*)
        assert any('apache-airflow-providers-' in cmd for cmd in pip_install_commands), "No Airflow 2.X providers found"
        # Verify deprecated packages are not installed
        deprecated_packages = ['airflow[all]', 'apache-airflow-backport-providers-google']
        assert not any(pkg in cmd for cmd in pip_install_commands for pkg in deprecated_packages), f"Deprecated packages found: {deprecated_packages}"
        # Log Airflow 2.X package verification results
        logger.info("Airflow 2.X packages are correctly installed")

    def test_airflow_constraints(self, dockerfile_content: str):
        """Tests that the Dockerfile uses the correct constraints for Airflow 2.X"""
        # Parse Dockerfile content
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Find ARG instructions defining CONSTRAINT_URL
        arg_instructions = find_instruction(instructions, 'ARG')
        constraint_url_arg = next((arg for arg in arg_instructions if 'CONSTRAINT_URL' in arg), None)
        # Verify it references Airflow 2.X constraints URL
        assert constraint_url_arg, "CONSTRAINT_URL ARG not found"
        assert 'constraints-' in constraint_url_arg, "CONSTRAINT_URL should reference Airflow 2.X constraints"
        # Find RUN instructions using pip install with constraints
        run_instructions = find_instruction(instructions, 'RUN')
        pip_install_commands = [run for run in run_instructions if 'pip install' in run]
        # Assert pip commands use constraint file
        assert any('--constraint' in cmd for cmd in pip_install_commands), "pip commands should use constraint file"
        # Log constraint verification results
        logger.info("Airflow constraints are correctly used")

    def test_airflow_config(self, dockerfile_content: str):
        """Tests that the Dockerfile sets up correct Airflow 2.X configuration"""
        # Parse Dockerfile content
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Find ENV instructions setting Airflow configuration
        env_instructions = find_instruction(instructions, 'ENV')
        airflow_config_envs = [env for env in env_instructions if 'AIRFLOW__' in env]
        # Verify Airflow config uses Airflow 2.X syntax (double underscore format)
        assert all('__' in env for env in airflow_config_envs), "Airflow config should use double underscore format"
        # Check that deprecated 1.X config options are not used
        deprecated_config_options = ['airflow_home', 'dags_folder']
        assert not any(opt in env for env in airflow_config_envs for opt in deprecated_config_options), f"Deprecated config options found: {deprecated_config_options}"
        # Log Airflow configuration verification results
        logger.info("Airflow configuration is correct")

    def test_entrypoint_compatibility(self, dockerfile_content: str):
        """Tests that the Dockerfile entrypoint is compatible with Airflow 2.X"""
        # Parse Dockerfile content
        instructions = parse_dockerfile_instructions(dockerfile_content)
        # Find ENTRYPOINT instruction
        entrypoint_instructions = find_instruction(instructions, 'ENTRYPOINT')
        # Verify entrypoint script exists and handles Airflow 2.X initialization
        assert entrypoint_instructions, "No ENTRYPOINT instruction found"
        entrypoint_script = entrypoint_instructions[0]
        # Check for compatibility with Airflow 2.X CLI commands
        assert 'airflow db init' in entrypoint_script, "Entrypoint should initialize Airflow DB"
        # Log entrypoint compatibility verification results
        logger.info("Entrypoint is compatible with Airflow 2.X")