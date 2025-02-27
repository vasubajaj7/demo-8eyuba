# ******************************************************************************
# Dockerfile for Apache Airflow 2.X
# 
# This Dockerfile creates a containerized environment for Apache Airflow 2.X as
# part of the migration from Cloud Composer 1 to Cloud Composer 2. It includes:
#
# - Multi-stage build for development, testing, and production
# - Security hardening
# - Google Cloud SDK integration
# - Comprehensive validation and initialization
# ******************************************************************************

# Base image for all builds
FROM python:3.8-slim AS airflow-base

# Arguments that can be passed at build time
ARG AIRFLOW_VERSION=2.5.1
ARG PYTHON_VERSION=3.8
ARG ADDITIONAL_PYTHON_DEPS=""
ARG ADDITIONAL_SYSTEM_DEPS=""
ARG BUILD_ENV=dev

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}
ENV PYTHON_VERSION=${PYTHON_VERSION}
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ENV PYTHONPATH=${AIRFLOW_HOME}:${PYTHONPATH}
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Install system dependencies
# These packages are required for:
# - build-essential: Compilation of Python packages
# - libpq-dev: PostgreSQL client connection
# - curl, wget: Downloading files
# - ca-certificates: SSL certificates
# - netcat: Health checks and connection testing
# - gosu, dumb-init: Process management
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    curl \
    wget \
    ca-certificates \
    gnupg \
    apt-transport-https \
    lsb-release \
    libssl-dev \
    libffi-dev \
    libsasl2-dev \
    libldap2-dev \
    default-libmysqlclient-dev \
    freetds-dev \
    freetds-bin \
    krb5-user \
    ldap-utils \
    libaio1 \
    unixodbc \
    unixodbc-dev \
    libevent-dev \
    pkg-config \
    gcc \
    g++ \
    python3-dev \
    netcat \
    gosu \
    procps \
    dumb-init \
    ${ADDITIONAL_SYSTEM_DEPS} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK for GCP service interactions
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y --no-install-recommends \
    google-cloud-sdk \
    google-cloud-sdk-gke-gcloud-auth-plugin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup airflow user and directories with proper UID/GID for better security
# Using fixed UID/GID for consistency across environments
RUN groupadd -g 50000 airflow && \
    useradd -u 50000 -g airflow -d ${AIRFLOW_HOME} -s /bin/bash -m airflow && \
    mkdir -p ${AIRFLOW_HOME}/dags && \
    mkdir -p ${AIRFLOW_HOME}/logs && \
    mkdir -p ${AIRFLOW_HOME}/plugins && \
    mkdir -p ${AIRFLOW_HOME}/config && \
    mkdir -p ${AIRFLOW_HOME}/scripts && \
    chown -R airflow:airflow ${AIRFLOW_HOME}

# ******************************************************************************
# Builder stage for dependencies and compilation
# This stage installs all Python dependencies and runs security checks
# ******************************************************************************
FROM airflow-base AS airflow-builder

# Switch to root for installation
USER root

# Copy requirements files
COPY --chown=airflow:airflow requirements.txt constraints.txt ${AIRFLOW_HOME}/

# Install Python dependencies
# 1. Upgrade pip, setuptools, and wheel
# 2. Install Apache Airflow with the specified version
# 3. Install project-specific requirements
# 4. Install provider packages for integration with external services
# 5. Run security check on dependencies
# 6. Clean up to reduce image size
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -c ${AIRFLOW_HOME}/constraints.txt && \
    pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt -c ${AIRFLOW_HOME}/constraints.txt && \
    if [ -n "${ADDITIONAL_PYTHON_DEPS}" ]; then pip install --no-cache-dir ${ADDITIONAL_PYTHON_DEPS}; fi && \
    # Install Google Cloud provider packages specifically for Cloud Composer 2 
    pip install --no-cache-dir \
    "apache-airflow-providers-google>=8.10.0" \
    "apache-airflow-providers-postgres>=5.4.0" \
    "apache-airflow-providers-http>=4.3.0" \
    "apache-airflow-providers-redis>=3.0.0" \
    "apache-airflow-providers-celery>=3.1.0" && \
    # Security scan on dependencies
    pip install --no-cache-dir "safety>=2.3.5" && \
    safety check && \
    # Remove build dependencies to reduce image size
    pip uninstall -y safety && \
    find /usr/local -type d -name '__pycache__' -exec rm -rf {} +

# ******************************************************************************
# Production-ready image
# This stage creates a minimal production-ready Airflow image
# ******************************************************************************
FROM airflow-base AS airflow-final

# Copy installed packages from builder stage
COPY --from=airflow-builder /usr/local/lib/python${PYTHON_VERSION}/site-packages /usr/local/lib/python${PYTHON_VERSION}/site-packages
COPY --from=airflow-builder /usr/local/bin /usr/local/bin

# Copy configuration files
COPY --chown=airflow:airflow airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY --chown=airflow:airflow config/ ${AIRFLOW_HOME}/config/
COPY --chown=airflow:airflow scripts/ ${AIRFLOW_HOME}/scripts/

# Create the entrypoint script
# This script handles initialization, database setup, and command execution
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Setup environment variables for Airflow if not set\n\
export AIRFLOW__CORE__EXECUTOR=${AIRFLOW__CORE__EXECUTOR:-CeleryExecutor}\n\
export AIRFLOW__CORE__LOAD_EXAMPLES=${AIRFLOW__CORE__LOAD_EXAMPLES:-false}\n\
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-postgresql+psycopg2://airflow:airflow@postgres/airflow}\n\
export AIRFLOW__CELERY__RESULT_BACKEND=${AIRFLOW__CELERY__RESULT_BACKEND:-db+postgresql://airflow:airflow@postgres/airflow}\n\
export AIRFLOW__CELERY__BROKER_URL=${AIRFLOW__CELERY__BROKER_URL:-redis://:@redis:6379/0}\n\
\n\
# Wait for database to be ready\n\
if [[ -z "${AIRFLOW_SKIP_DB_CHECK}" ]]; then\n\
    echo "Waiting for database..."\n\
    DB_HOST=$(echo ${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN} | awk -F@ \047{print $2}\047 | awk -F/ \047{print $1}\047)\n\
    while ! nc -z ${DB_HOST%:*} ${DB_HOST#*:}; do\n\
        sleep 1\n\
    done\n\
    echo "Database is ready!"\n\
fi\n\
\n\
# Initialize Airflow database if needed\n\
if [[ "$1" == "webserver" || "$1" == "scheduler" ]]; then\n\
    echo "Initializing Airflow database..."\n\
    airflow db init\n\
\n\
    # Create default user if not present\n\
    if [[ -n "${AIRFLOW_USERNAME}" && -n "${AIRFLOW_PASSWORD}" && -n "${AIRFLOW_EMAIL}" ]]; then\n\
        echo "Creating admin user..."\n\
        airflow users create \\\n\
            --username "${AIRFLOW_USERNAME}" \\\n\
            --password "${AIRFLOW_PASSWORD}" \\\n\
            --firstname "Admin" \\\n\
            --lastname "User" \\\n\
            --role "Admin" \\\n\
            --email "${AIRFLOW_EMAIL}"\n\
    fi\n\
\n\
    # Run database migrations for Airflow 2.X compatibility\n\
    echo "Running database migrations..."\n\
    airflow db upgrade\n\
fi\n\
\n\
# Validate DAGs if requested\n\
if [[ -n "${AIRFLOW_VALIDATE_DAGS}" && "${AIRFLOW_VALIDATE_DAGS}" == "true" ]]; then\n\
    echo "Validating DAGs..."\n\
    python ${AIRFLOW_HOME}/scripts/validate_dags.py ${AIRFLOW_HOME}/dags\n\
fi\n\
\n\
# Run the provided command\n\
exec airflow "$@"\n' > /entrypoint.sh && \
    chmod +x /entrypoint.sh && \
    chown airflow:airflow /entrypoint.sh

# Setup permissions for better security and group access
USER root
RUN chmod -R g+w ${AIRFLOW_HOME} && \
    chmod -R 775 ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins && \
    find ${AIRFLOW_HOME} -type d -exec chmod 775 {} \; && \
    find ${AIRFLOW_HOME} -type f -exec chmod 664 {} \;

# Expose ports
# - 8080: Airflow webserver
# - 5555: Flower monitoring interface
# - 8793: Worker log server
EXPOSE 8080 5555 8793

# Define volumes for:
# - DAGs: User's workflow definitions
# - Logs: Execution logs for better persistence
# - Plugins: Custom plugins and hooks
# - Config: Environment-specific configuration
VOLUME ["${AIRFLOW_HOME}/dags", "${AIRFLOW_HOME}/logs", "${AIRFLOW_HOME}/plugins", "${AIRFLOW_HOME}/config"]

# Health check to ensure webserver is running
HEALTHCHECK --interval=30s --timeout=30s --retries=3 --start-period=40s \
    CMD curl --fail http://localhost:8080/health || exit 1

# Set back to airflow user for security
USER airflow

# Default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]

# ******************************************************************************
# Development image with additional tools
# This stage adds development tools and debugging support
# ******************************************************************************
FROM airflow-final AS airflow-dev

USER root

# Copy development requirements
COPY --chown=airflow:airflow requirements-dev.txt ${AIRFLOW_HOME}/

# Install development dependencies
# These tools help with:
# - Code formatting and linting
# - Testing and debugging
# - Development workflow
RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements-dev.txt -c ${AIRFLOW_HOME}/constraints.txt && \
    pip install --no-cache-dir ipython pytest pytest-cov black flake8 mypy && \
    mkdir -p ${AIRFLOW_HOME}/data && \
    chown -R airflow: ${AIRFLOW_HOME}/data

# Set environment variables for development
ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
ENV PYTHONBREAKPOINT=ipdb.set_trace
ENV AIRFLOW_ENV=dev

USER airflow