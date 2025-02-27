"""
Provides mock implementations of Google Cloud Platform services for testing Airflow DAGs
and operators during migration from Airflow 1.10.15 to Airflow 2.X.

This module contains mock classes and factory functions for GCS, BigQuery, Secret Manager,
Cloud Monitoring, and Cloud Build to enable isolated testing without actual GCP dependencies.
"""

import io
import datetime
from typing import Dict, List, Optional, Union, Any, Tuple, Set

# Third-party imports
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

# Google Cloud imports for type hints
from google.cloud.storage import Client as StorageClient
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.secretmanager import SecretManagerServiceClient
from google.cloud.monitoring import MetricServiceClient
from google.cloud.devtools.cloudbuild import CloudBuildClient

# Internal imports
from ..utils.airflow2_compatibility_utils import is_airflow2, AIRFLOW_VERSION

# Default constants for mock configuration
DEFAULT_PROJECT_ID = "mock-gcp-project"
DEFAULT_BUCKET_NAME = "mock-gcs-bucket"
DEFAULT_DATASET_ID = "mock_dataset"
DEFAULT_TABLE_ID = "mock_table"
DEFAULT_INSTANCE_NAME = "mock-cloudsql-instance"
DEFAULT_DATABASE_NAME = "mock_database"
DEFAULT_SECRET_ID = "mock-secret"
DEFAULT_LOCATION = "us-central1"


class MockBlob:
    """Mock implementation of GCS Blob (Storage Object)"""
    
    def __init__(self, name: str, bucket, content: bytes = None):
        """
        Initialize the mock GCS blob
        
        Args:
            name: Name of the blob
            bucket: Parent bucket object
            content: Optional initial content bytes
        """
        self.name = name
        self.bucket = bucket
        self._content = content or b''
        self.metadata = {}
        self.size = len(self._content)
        self.updated = datetime.datetime.now()
    
    def exists(self) -> bool:
        """
        Check if this blob exists in the bucket
        
        Returns:
            True if the blob exists in the bucket, False otherwise
        """
        return self.name in self.bucket._blobs
    
    def download_as_string(self) -> bytes:
        """
        Download blob content as string (Airflow 1.X compatibility)
        
        Returns:
            Blob content as bytes
        """
        return self._content
    
    def download_as_bytes(self) -> bytes:
        """
        Download blob content as bytes (Airflow 2.X compatibility)
        
        Returns:
            Blob content as bytes
        """
        return self._content
    
    def download_as_text(self) -> str:
        """
        Download blob content as text (Airflow 2.X compatibility)
        
        Returns:
            Blob content as string
        """
        return self._content.decode('utf-8')
    
    def upload_from_string(self, data: Union[str, bytes], content_type: str = None) -> None:
        """
        Upload string content to blob (Airflow 1.X compatibility)
        
        Args:
            data: Content to upload
            content_type: Optional content type
        """
        if isinstance(data, str):
            self._content = data.encode('utf-8')
        else:
            self._content = data
        
        self.size = len(self._content)
        if content_type:
            self.metadata['contentType'] = content_type
        self.updated = datetime.datetime.now()
    
    def upload_from_file(self, file_obj, content_type: str = None, rewind: bool = False) -> None:
        """
        Upload file content to blob
        
        Args:
            file_obj: File-like object to read content from
            content_type: Optional content type
            rewind: Whether to seek to the beginning of the file before reading
        """
        if rewind:
            file_obj.seek(0)
        
        self._content = file_obj.read()
        self.size = len(self._content)
        if content_type:
            self.metadata['contentType'] = content_type
        self.updated = datetime.datetime.now()
    
    def delete(self) -> None:
        """Delete this blob from its bucket"""
        self.bucket.delete_blob(self.name)


class MockBucket:
    """Mock implementation of GCS Bucket"""
    
    def __init__(self, name: str, location: str = None):
        """
        Initialize the mock GCS bucket
        
        Args:
            name: Bucket name
            location: Optional bucket location
        """
        self.name = name
        self.location = location or DEFAULT_LOCATION
        self._blobs = {}
        self.metadata = {
            'name': name,
            'location': self.location,
            'timeCreated': datetime.datetime.now().isoformat()
        }
    
    def blob(self, blob_name: str) -> MockBlob:
        """
        Get or create a blob object in this bucket
        
        Args:
            blob_name: Name of the blob
            
        Returns:
            Mock blob object
        """
        if blob_name not in self._blobs:
            self._blobs[blob_name] = MockBlob(blob_name, self)
        return self._blobs[blob_name]
    
    def get_blob(self, blob_name: str) -> Optional[MockBlob]:
        """
        Get a blob if it exists in this bucket
        
        Args:
            blob_name: Name of the blob to get
            
        Returns:
            Mock blob if found, None otherwise
        """
        return self._blobs.get(blob_name)
    
    def list_blobs(self, prefix: str = None, delimiter: str = None) -> List[MockBlob]:
        """
        List blobs in the bucket with optional prefix filtering
        
        Args:
            prefix: Optional prefix to filter by
            delimiter: Optional delimiter for hierarchical listing
            
        Returns:
            List of matching blob objects
        """
        blobs = list(self._blobs.values())
        
        if prefix:
            blobs = [b for b in blobs if b.name.startswith(prefix)]
        
        if delimiter:
            # Simulating folder-like behavior with delimiters
            result = []
            prefixes = set()
            
            for blob in blobs:
                if delimiter in blob.name[len(prefix or ''):]:
                    # If there's a delimiter in the name after the prefix, 
                    # add the prefix up to the delimiter
                    prefix_part = blob.name[:blob.name.index(delimiter, len(prefix or '')) + 1]
                    prefixes.add(prefix_part)
                else:
                    result.append(blob)
            
            # Add common prefixes as blobs
            for prefix_item in prefixes:
                result.append(MockBlob(prefix_item, self))
            
            return result
        
        return blobs
    
    def delete_blob(self, blob_name: str) -> bool:
        """
        Delete a blob from the bucket
        
        Args:
            blob_name: Name of the blob to delete
            
        Returns:
            True if deleted, False if not found
        """
        if blob_name in self._blobs:
            del self._blobs[blob_name]
            return True
        return False


class MockStorageClient:
    """Mock implementation of Google Cloud Storage Client"""
    
    def __init__(self, project: str = None, fail_on_missing_bucket: bool = False):
        """
        Initialize the mock GCS client
        
        Args:
            project: GCP project ID
            fail_on_missing_bucket: Whether to raise exception when a bucket is not found
        """
        self.project = project or DEFAULT_PROJECT_ID
        self._buckets = {}
        self._fail_on_missing_bucket = fail_on_missing_bucket
    
    def bucket(self, bucket_name: str) -> MockBucket:
        """
        Get or create a bucket object
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Mock bucket object
        """
        if bucket_name not in self._buckets:
            self._buckets[bucket_name] = MockBucket(bucket_name)
        return self._buckets[bucket_name]
    
    def get_bucket(self, bucket_name: str) -> MockBucket:
        """
        Get a bucket if it exists
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Mock bucket object if found
            
        Raises:
            NotFound: If bucket doesn't exist and fail_on_missing_bucket is True
        """
        if bucket_name in self._buckets:
            return self._buckets[bucket_name]
        
        if self._fail_on_missing_bucket:
            from google.cloud.exceptions import NotFound
            raise NotFound(f"Bucket {bucket_name} not found")
        
        # Auto-create the bucket if not failing
        return self.bucket(bucket_name)
    
    def list_buckets(self) -> List[MockBucket]:
        """
        List all buckets in the project
        
        Returns:
            List of mock bucket objects
        """
        return list(self._buckets.values())
    
    def create_bucket(self, bucket_name: str, location: str = None) -> MockBucket:
        """
        Create a new bucket
        
        Args:
            bucket_name: Name of the bucket to create
            location: Optional bucket location
            
        Returns:
            Newly created mock bucket
        """
        bucket = MockBucket(bucket_name, location)
        self._buckets[bucket_name] = bucket
        return bucket


class MockQueryJob:
    """Mock implementation of BigQuery QueryJob"""
    
    def __init__(self, results: List[Dict] = None, error: bool = False):
        """
        Initialize the mock query job
        
        Args:
            results: Query results rows
            error: Whether to simulate an error
        """
        self._results = results or []
        self._done = True
        self.errors = ["Query execution failed"] if error else []
    
    def result(self):
        """
        Get query results
        
        Returns:
            Mock row iterator for results
            
        Raises:
            Exception: If error is simulated
        """
        if self.errors:
            raise Exception(self.errors[0])
        
        # In real BigQuery, this would return a RowIterator, but for simplicity
        # we'll just use a list of dictionaries that can be converted to a DataFrame
        return self._results
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert query results to a pandas DataFrame
        
        Returns:
            Results as a pandas DataFrame
            
        Raises:
            Exception: If error is simulated
        """
        if self.errors:
            raise Exception(self.errors[0])
        
        return pd.DataFrame(self._results)


class MockTable:
    """Mock implementation of BigQuery Table"""
    
    def __init__(self, table_id: str, dataset, schema: List = None):
        """
        Initialize the mock BigQuery table
        
        Args:
            table_id: Table ID
            dataset: Parent dataset object
            schema: Optional table schema
        """
        self.table_id = table_id
        self.dataset = dataset
        self.schema = schema or []
        self._rows = []
        self.metadata = {
            'tableId': table_id,
            'datasetId': dataset.dataset_id,
            'projectId': dataset.project
        }
    
    def exists(self) -> bool:
        """
        Check if this table exists in the dataset
        
        Returns:
            True if exists, False otherwise
        """
        return self.table_id in self.dataset._tables
    
    def reload(self) -> None:
        """Reload this table's metadata (mock method for API compatibility)"""
        pass


class MockDataset:
    """Mock implementation of BigQuery Dataset"""
    
    def __init__(self, dataset_id: str, project: str = None):
        """
        Initialize the mock BigQuery dataset
        
        Args:
            dataset_id: Dataset ID
            project: GCP project ID
        """
        self.dataset_id = dataset_id
        self.project = project or DEFAULT_PROJECT_ID
        self._tables = {}
        self.metadata = {
            'datasetId': dataset_id,
            'projectId': self.project
        }
    
    def table(self, table_id: str) -> MockTable:
        """
        Get a reference to a table in this dataset
        
        Args:
            table_id: Table ID
            
        Returns:
            Mock table reference
        """
        if table_id not in self._tables:
            table = MockTable(table_id, self)
            self._tables[table_id] = table
        return self._tables[table_id]
    
    def list_tables(self) -> List[MockTable]:
        """
        List all tables in this dataset
        
        Returns:
            List of mock table objects
        """
        return list(self._tables.values())


class MockBigQueryClient:
    """Mock implementation of BigQuery Client"""
    
    def __init__(self, project: str = None, fail_on_missing_dataset: bool = False):
        """
        Initialize the mock BigQuery client
        
        Args:
            project: GCP project ID
            fail_on_missing_dataset: Whether to raise exception when a dataset is not found
        """
        self.project = project or DEFAULT_PROJECT_ID
        self._datasets = {}
        self._query_results = {}
        self._fail_on_missing_dataset = fail_on_missing_dataset
    
    def dataset(self, dataset_id: str) -> MockDataset:
        """
        Get a reference to a dataset
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            Mock dataset reference
        """
        if dataset_id not in self._datasets:
            self._datasets[dataset_id] = MockDataset(dataset_id, self.project)
        return self._datasets[dataset_id]
    
    def get_dataset(self, dataset_id: str) -> MockDataset:
        """
        Get a dataset if it exists
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            Mock dataset if found
            
        Raises:
            NotFound: If dataset doesn't exist and fail_on_missing_dataset is True
        """
        if dataset_id in self._datasets:
            return self._datasets[dataset_id]
        
        if self._fail_on_missing_dataset:
            from google.cloud.exceptions import NotFound
            raise NotFound(f"Dataset {dataset_id} not found")
        
        # Auto-create the dataset if not failing
        return self.dataset(dataset_id)
    
    def create_dataset(self, dataset_id: str, dataset_info: Dict = None) -> MockDataset:
        """
        Create a new dataset
        
        Args:
            dataset_id: Dataset ID
            dataset_info: Optional dataset configuration
            
        Returns:
            Newly created mock dataset
        """
        dataset = MockDataset(dataset_id, self.project)
        
        # Apply dataset_info if provided
        if dataset_info and isinstance(dataset_info, dict):
            for key, value in dataset_info.items():
                if key != 'dataset_id':
                    dataset.metadata[key] = value
        
        self._datasets[dataset_id] = dataset
        return dataset
    
    def query(self, sql: str, job_config: Dict = None) -> MockQueryJob:
        """
        Execute a SQL query
        
        Args:
            sql: SQL query string
            job_config: Optional job configuration
            
        Returns:
            Mock query job object
        """
        # Check if we have predefined results for this query
        if sql in self._query_results:
            return MockQueryJob(self._query_results[sql])
        
        # Return empty results by default
        return MockQueryJob([])


class MockSecretPayload:
    """Mock implementation of Secret Manager Payload"""
    
    def __init__(self, data: bytes):
        """
        Initialize the mock secret payload
        
        Args:
            data: Secret data bytes
        """
        self.data = data


class MockSecretVersion:
    """Mock implementation of Secret Manager Secret Version"""
    
    def __init__(self, name: str, secret, payload: bytes):
        """
        Initialize the mock secret version
        
        Args:
            name: Version name
            secret: Parent secret object
            payload: Secret payload bytes
        """
        self.name = name
        self.secret = secret
        self._payload = payload
        self.create_time = datetime.datetime.now()
    
    def payload(self) -> MockSecretPayload:
        """
        Get the payload of this secret version
        
        Returns:
            Mock secret payload object
        """
        return MockSecretPayload(self._payload)


class MockSecret:
    """Mock implementation of Secret Manager Secret"""
    
    def __init__(self, name: str):
        """
        Initialize the mock secret
        
        Args:
            name: Secret name
        """
        self.name = name
        self._versions = {}
    
    def add_version(self, payload: bytes, version_id: str = None) -> MockSecretVersion:
        """
        Add a new version to this secret
        
        Args:
            payload: Secret payload bytes
            version_id: Optional version ID (auto-generated if not provided)
            
        Returns:
            Newly created secret version
        """
        if not version_id:
            version_id = str(len(self._versions) + 1)
        
        version_name = f"{self.name}/versions/{version_id}"
        version = MockSecretVersion(version_name, self, payload)
        self._versions[version_id] = version
        return version
    
    def get_version(self, version_id: str) -> Optional[MockSecretVersion]:
        """
        Get a version of this secret
        
        Args:
            version_id: Version ID to get
            
        Returns:
            Secret version if found, None otherwise
        """
        if version_id == 'latest':
            if not self._versions:
                return None
            latest_id = max(self._versions.keys(), key=lambda x: int(x) if x.isdigit() else 0)
            return self._versions.get(latest_id)
        
        return self._versions.get(version_id)


class MockSecretManagerClient:
    """Mock implementation of Secret Manager Client"""
    
    def __init__(self, project: str = None, fail_on_missing_secret: bool = False):
        """
        Initialize the mock Secret Manager client
        
        Args:
            project: GCP project ID
            fail_on_missing_secret: Whether to raise exception when a secret is not found
        """
        self.project = project or DEFAULT_PROJECT_ID
        self._secrets = {}
        self._fail_on_missing_secret = fail_on_missing_secret
    
    def access_secret_version(self, name: str) -> MockSecretVersion:
        """
        Access a secret version
        
        Args:
            name: Full resource name of the secret version
            
        Returns:
            Mock secret version
            
        Raises:
            NotFound: If secret doesn't exist and fail_on_missing_secret is True
        """
        # Parse secret_id and version_id from name
        # Format: projects/{project}/secrets/{secret_id}/versions/{version_id}
        parts = name.split('/')
        if len(parts) < 6:
            raise ValueError(f"Invalid secret version name: {name}")
        
        secret_id = parts[3]
        version_id = parts[5]
        
        if secret_id in self._secrets:
            version = self._secrets[secret_id].get_version(version_id)
            if version:
                return version
        
        if self._fail_on_missing_secret:
            from google.cloud.exceptions import NotFound
            raise NotFound(f"Secret {secret_id} version {version_id} not found")
        
        # Create a default secret and version if not failing
        if secret_id not in self._secrets:
            self._secrets[secret_id] = MockSecret(f"projects/{self.project}/secrets/{secret_id}")
        
        # Create a default version with empty payload
        return self._secrets[secret_id].add_version(b"mock-secret-value", version_id)
    
    def add_secret_version(self, parent: str, payload: Dict) -> MockSecretVersion:
        """
        Add a new version to a secret
        
        Args:
            parent: Parent secret resource name
            payload: Secret payload
            
        Returns:
            Newly created secret version
        """
        # Parse secret_id from parent
        # Format: projects/{project}/secrets/{secret_id}
        parts = parent.split('/')
        if len(parts) < 4:
            raise ValueError(f"Invalid secret parent: {parent}")
        
        secret_id = parts[3]
        
        # Create secret if it doesn't exist
        if secret_id not in self._secrets:
            self._secrets[secret_id] = MockSecret(parent)
        
        # Extract payload data
        data = payload.get('data', b'')
        
        # Add new version
        return self._secrets[secret_id].add_version(data)
    
    def create_secret(self, parent: str, secret_id: str, secret: Dict) -> MockSecret:
        """
        Create a new secret
        
        Args:
            parent: Parent resource name
            secret_id: Secret ID
            secret: Secret configuration
            
        Returns:
            Newly created secret
        """
        secret_name = f"{parent}/secrets/{secret_id}"
        mock_secret = MockSecret(secret_name)
        self._secrets[secret_id] = mock_secret
        return mock_secret


class MockMonitoringClient:
    """Mock implementation of Cloud Monitoring Client"""
    
    def __init__(self, project: str = None, fail_on_missing_metric: bool = False):
        """
        Initialize the mock monitoring client
        
        Args:
            project: GCP project ID
            fail_on_missing_metric: Whether to raise exception when a metric is not found
        """
        self.project = project or DEFAULT_PROJECT_ID
        self._time_series = {}
        self._alerts = {}
        self._fail_on_missing_metric = fail_on_missing_metric
    
    def list_time_series(self, name: str, filter: str, interval: Dict) -> List[Dict]:
        """
        List time series data for metrics
        
        Args:
            name: Resource name
            filter: Metric filter expression
            interval: Time interval
            
        Returns:
            List of time series data
            
        Raises:
            NotFound: If metric doesn't exist and fail_on_missing_metric is True
        """
        # Extract metric type from filter
        metric_type = None
        if 'metric.type=' in filter:
            metric_type = filter.split('metric.type=')[1].split('"')[1]
        
        if metric_type and metric_type in self._time_series:
            return self._time_series[metric_type]
        
        if self._fail_on_missing_metric:
            from google.cloud.exceptions import NotFound
            raise NotFound(f"Metric {metric_type} not found")
        
        return []
    
    def get_metric(self, name: str) -> Dict:
        """
        Get a specific metric descriptor
        
        Args:
            name: Metric name
            
        Returns:
            Mock metric descriptor
            
        Raises:
            NotFound: If metric doesn't exist and fail_on_missing_metric is True
        """
        # Parse metric type from name
        # Format: projects/{project}/metricDescriptors/{metric_type}
        parts = name.split('/')
        if len(parts) < 4:
            raise ValueError(f"Invalid metric name: {name}")
        
        metric_type = parts[3]
        
        if metric_type in self._time_series:
            return {'name': name, 'type': metric_type, 'description': f"Mock metric {metric_type}"}
        
        if self._fail_on_missing_metric:
            from google.cloud.exceptions import NotFound
            raise NotFound(f"Metric {metric_type} not found")
        
        # Return a default descriptor
        return {'name': name, 'type': metric_type, 'description': f"Mock metric {metric_type}"}


class MockCloudBuildClient:
    """Mock implementation of Cloud Build Client"""
    
    def __init__(self, project: str = None, fail_on_missing_build: bool = False):
        """
        Initialize the mock Cloud Build client
        
        Args:
            project: GCP project ID
            fail_on_missing_build: Whether to raise exception when a build is not found
        """
        self.project = project or DEFAULT_PROJECT_ID
        self._builds = {}
        self._triggers = {}
        self._fail_on_missing_build = fail_on_missing_build
    
    def create_build(self, project_id: str, build: Dict) -> Dict:
        """
        Create a new build
        
        Args:
            project_id: GCP project ID
            build: Build configuration
            
        Returns:
            Mock build object
        """
        build_id = f"build-{len(self._builds) + 1}"
        build_obj = {
            'id': build_id,
            'projectId': project_id,
            'status': 'QUEUED',
            'createTime': datetime.datetime.now().isoformat(),
            'startTime': None,
            'finishTime': None,
            'logsBucket': f"gs://mock-cloudbuild-logs/{build_id}",
            'steps': build.get('steps', []),
            'options': build.get('options', {}),
            'timeout': build.get('timeout', '600s')
        }
        self._builds[build_id] = build_obj
        return build_obj
    
    def get_build(self, name: str) -> Optional[Dict]:
        """
        Get a build by ID
        
        Args:
            name: Build resource name
            
        Returns:
            Mock build if found
            
        Raises:
            NotFound: If build doesn't exist and fail_on_missing_build is True
        """
        # Parse build ID from name
        # Format: projects/{project}/builds/{build_id}
        parts = name.split('/')
        if len(parts) < 4:
            raise ValueError(f"Invalid build name: {name}")
        
        build_id = parts[3]
        
        if build_id in self._builds:
            return self._builds[build_id]
        
        if self._fail_on_missing_build:
            from google.cloud.exceptions import NotFound
            raise NotFound(f"Build {build_id} not found")
        
        return None
    
    def list_builds(self, project_id: str, filter: str = None) -> List[Dict]:
        """
        List all builds in the project
        
        Args:
            project_id: GCP project ID
            filter: Optional filter expression
            
        Returns:
            List of mock build objects
        """
        builds = list(self._builds.values())
        if filter:
            # Simple filter implementation (in reality, this would be more sophisticated)
            if 'status=' in filter:
                status = filter.split('status=')[1].split('"')[1]
                builds = [b for b in builds if b.get('status') == status]
        
        return builds


def create_mock_storage_client(mock_responses: Dict = None, fail_on_missing_bucket: bool = False) -> MockStorageClient:
    """
    Creates a mock Google Cloud Storage client with configurable behavior
    
    Args:
        mock_responses: Dict mapping bucket names to dictionaries of blobs and their content
        fail_on_missing_bucket: Whether to raise NotFound for missing buckets
        
    Returns:
        Configured mock GCS client
    """
    client = MockStorageClient(fail_on_missing_bucket=fail_on_missing_bucket)
    
    # Configure mock responses
    if mock_responses:
        for bucket_name, bucket_data in mock_responses.items():
            bucket = client.bucket(bucket_name)
            
            # Configure bucket properties if provided
            if isinstance(bucket_data, dict):
                # Set bucket metadata if provided
                if 'metadata' in bucket_data:
                    bucket.metadata.update(bucket_data['metadata'])
                
                # Configure blob contents
                if 'blobs' in bucket_data and isinstance(bucket_data['blobs'], dict):
                    for blob_name, blob_content in bucket_data['blobs'].items():
                        blob = bucket.blob(blob_name)
                        
                        if isinstance(blob_content, dict):
                            # Dict with content and metadata
                            if 'content' in blob_content:
                                content = blob_content['content']
                                if isinstance(content, str):
                                    blob.upload_from_string(content)
                                else:
                                    blob.upload_from_string(content)
                            
                            # Set blob metadata if provided
                            if 'metadata' in blob_content:
                                blob.metadata.update(blob_content['metadata'])
                        else:
                            # String or bytes content
                            if isinstance(blob_content, str):
                                blob.upload_from_string(blob_content)
                            else:
                                blob.upload_from_string(blob_content)
    
    return client


def create_mock_bigquery_client(mock_responses: Dict = None, fail_on_missing_dataset: bool = False) -> MockBigQueryClient:
    """
    Creates a mock BigQuery client with configurable behavior
    
    Args:
        mock_responses: Dict mapping datasets to tables and query results
        fail_on_missing_dataset: Whether to raise NotFound for missing datasets
        
    Returns:
        Configured mock BigQuery client
    """
    client = MockBigQueryClient(fail_on_missing_dataset=fail_on_missing_dataset)
    
    # Configure mock responses
    if mock_responses:
        # Configure datasets and tables
        if 'datasets' in mock_responses:
            for dataset_id, dataset_data in mock_responses['datasets'].items():
                dataset = client.dataset(dataset_id)
                
                # Configure tables if provided
                if 'tables' in dataset_data and isinstance(dataset_data['tables'], dict):
                    for table_id, table_data in dataset_data['tables'].items():
                        table = dataset.table(table_id)
                        
                        # Configure table schema if provided
                        if 'schema' in table_data:
                            table.schema = table_data['schema']
                        
                        # Configure table rows if provided
                        if 'rows' in table_data:
                            table._rows = table_data['rows']
        
        # Configure query results
        if 'queries' in mock_responses:
            client._query_results = mock_responses['queries']
    
    return client


def create_mock_secret_manager_client(mock_responses: Dict = None, fail_on_missing_secret: bool = False) -> MockSecretManagerClient:
    """
    Creates a mock Secret Manager client with configurable behavior
    
    Args:
        mock_responses: Dict mapping secret IDs to secret data
        fail_on_missing_secret: Whether to raise NotFound for missing secrets
        
    Returns:
        Configured mock Secret Manager client
    """
    client = MockSecretManagerClient(fail_on_missing_secret=fail_on_missing_secret)
    
    # Configure mock responses
    if mock_responses and 'secrets' in mock_responses:
        for secret_id, secret_data in mock_responses['secrets'].items():
            # Create the secret
            secret_name = f"projects/{client.project}/secrets/{secret_id}"
            secret = MockSecret(secret_name)
            client._secrets[secret_id] = secret
            
            # Add versions
            if 'versions' in secret_data and isinstance(secret_data['versions'], dict):
                for version_id, version_data in secret_data['versions'].items():
                    # Convert payload to bytes if needed
                    payload = version_data.get('payload', b'')
                    if isinstance(payload, str):
                        payload = payload.encode('utf-8')
                    
                    # Add the version
                    secret.add_version(payload, version_id)
            
            # Add a default version if none provided
            if not secret._versions:
                default_payload = secret_data.get('defaultPayload', b'default-secret-value')
                if isinstance(default_payload, str):
                    default_payload = default_payload.encode('utf-8')
                secret.add_version(default_payload, '1')
    
    return client


def create_mock_monitoring_client(mock_responses: Dict = None, fail_on_missing_metric: bool = False) -> MockMonitoringClient:
    """
    Creates a mock Cloud Monitoring client with configurable behavior
    
    Args:
        mock_responses: Dict mapping metric types to time series data
        fail_on_missing_metric: Whether to raise NotFound for missing metrics
        
    Returns:
        Configured mock Cloud Monitoring client
    """
    client = MockMonitoringClient(fail_on_missing_metric=fail_on_missing_metric)
    
    # Configure mock responses
    if mock_responses and 'metrics' in mock_responses:
        for metric_type, time_series in mock_responses['metrics'].items():
            client._time_series[metric_type] = time_series
    
    return client


def create_mock_cloudbuild_client(mock_responses: Dict = None, fail_on_missing_build: bool = False) -> MockCloudBuildClient:
    """
    Creates a mock Cloud Build client with configurable behavior
    
    Args:
        mock_responses: Dict with build configs
        fail_on_missing_build: Whether to raise NotFound for missing builds
        
    Returns:
        Configured mock Cloud Build client
    """
    client = MockCloudBuildClient(fail_on_missing_build=fail_on_missing_build)
    
    # Configure mock responses
    if mock_responses and 'builds' in mock_responses:
        for build_id, build_data in mock_responses['builds'].items():
            client._builds[build_id] = build_data
    
    return client


def patch_gcp_services(mock_responses: Dict = None) -> Dict:
    """
    Creates patchers for GCP service clients to use in tests
    
    Args:
        mock_responses: Dictionary of mock responses for different services
        
    Returns:
        Dictionary of patchers for GCP services
    """
    gcs_responses = mock_responses.get('gcs', {}) if mock_responses else {}
    bq_responses = mock_responses.get('bigquery', {}) if mock_responses else {}
    sm_responses = mock_responses.get('secretmanager', {}) if mock_responses else {}
    monitoring_responses = mock_responses.get('monitoring', {}) if mock_responses else {}
    cloudbuild_responses = mock_responses.get('cloudbuild', {}) if mock_responses else {}
    
    # Create mock clients
    mock_storage_client = create_mock_storage_client(gcs_responses)
    mock_bigquery_client = create_mock_bigquery_client(bq_responses)
    mock_secretmanager_client = create_mock_secret_manager_client(sm_responses)
    mock_monitoring_client = create_mock_monitoring_client(monitoring_responses)
    mock_cloudbuild_client = create_mock_cloudbuild_client(cloudbuild_responses)
    
    # Create patchers
    patchers = {
        'storage': patch('google.cloud.storage.Client', return_value=mock_storage_client),
        'bigquery': patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client),
        'secretmanager': patch('google.cloud.secretmanager.SecretManagerServiceClient', return_value=mock_secretmanager_client),
        'monitoring': patch('google.cloud.monitoring.MetricServiceClient', return_value=mock_monitoring_client),
        'cloudbuild': patch('google.cloud.devtools.cloudbuild.CloudBuildClient', return_value=mock_cloudbuild_client)
    }
    
    return patchers