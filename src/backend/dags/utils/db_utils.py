"""
Database utility functions for Apache Airflow 2.X.

This module provides a comprehensive set of database operations for DAGs, including
connection management, query execution, transaction handling, and data loading capabilities,
ensuring compatibility with Cloud Composer 2 environments and PostgreSQL/Cloud SQL databases.
"""

import os
import logging
import json
import csv
from typing import List, Dict, Union, Optional, Any, Tuple
from pathlib import Path

# Pandas v1.3.5
import pandas as pd
from pandas import DataFrame

# PostgreSQL adapter v2.9.3
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_batch

# SQLAlchemy v1.4.0+
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column

# Retry functionality v6.2.0+
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Airflow v2.0.0+
from airflow.exceptions import AirflowException
from airflow.models import Connection

# Airflow PostgreSQL provider v2.0.0+
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow Google Cloud provider v2.0.0+
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook

# Internal imports
from .gcp_utils import get_secret

# Configure logging
logger = logging.getLogger('airflow.utils.db')

# Global constants
POSTGRES_CONN_ID = 'postgres_default'
CLOUD_SQL_CONN_ID = 'google_cloud_default'
DEFAULT_SCHEMA = 'public'
DEFAULT_CHUNK_SIZE = 10000
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0


def validate_connection_internal(conn_id: str) -> Dict[str, Any]:
    """
    Internal function to validate if a connection exists and has required attributes.
    
    Args:
        conn_id: Connection ID to validate
        
    Returns:
        Dict containing validation results with status, issues, and recommendations
        
    Raises:
        AirflowException: If validation fails critically
    """
    validation_result = {
        'status': 'valid',
        'issues': [],
        'recommendations': []
    }
    
    if not conn_id:
        validation_result['status'] = 'invalid'
        validation_result['issues'].append('Connection ID is not provided')
        validation_result['recommendations'].append('Provide a valid connection ID')
        return validation_result
    
    try:
        conn = Connection.get_connection_from_secrets(conn_id)
        
        # Check if connection is Postgres type
        if conn.conn_type not in ('postgres', 'google_cloud_platform'):
            validation_result['status'] = 'warning'
            validation_result['issues'].append(
                f"Connection type '{conn.conn_type}' may not be compatible with PostgreSQL operations"
            )
            validation_result['recommendations'].append(
                'Use connection type "postgres" or "google_cloud_platform" for PostgreSQL operations'
            )
        
        # Check for required attributes
        if not conn.host:
            validation_result['status'] = 'warning'
            validation_result['issues'].append('Connection has no host specified')
            validation_result['recommendations'].append('Specify host in connection')
        
        if not conn.schema and conn.conn_type == 'postgres':
            validation_result['status'] = 'warning'
            validation_result['issues'].append('Connection has no schema/database specified')
            validation_result['recommendations'].append('Specify schema/database in connection')
        
        # For GCP connections, check for project_id in extras
        if conn.conn_type == 'google_cloud_platform':
            extras = conn.extra_dejson
            if not extras.get('project_id'):
                validation_result['status'] = 'warning'
                validation_result['issues'].append('GCP connection has no project_id specified')
                validation_result['recommendations'].append('Specify project_id in connection extras')
        
        logger.debug(f"Connection validation for '{conn_id}': {validation_result['status']}")
        
        return validation_result
    
    except AirflowException as e:
        validation_result['status'] = 'invalid'
        validation_result['issues'].append(f"Connection '{conn_id}' not found: {str(e)}")
        validation_result['recommendations'].append(f"Create connection '{conn_id}' in Airflow")
        logger.error(f"Connection validation failed for '{conn_id}': {str(e)}")
        
        return validation_result


def get_postgres_hook(conn_id: str = None, schema: str = None) -> PostgresHook:
    """
    Get a PostgresHook instance with the provided connection ID and schema.
    
    Args:
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema to use (defaults to DEFAULT_SCHEMA)
        
    Returns:
        Configured PostgresHook instance
        
    Raises:
        AirflowException: If connection cannot be established
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    # Validate connection
    validation = validate_connection_internal(conn_id)
    if validation['status'] == 'invalid':
        issues = '; '.join(validation['issues'])
        raise AirflowException(f"Invalid connection '{conn_id}': {issues}")
    
    try:
        hook = PostgresHook(postgres_conn_id=conn_id, schema=schema)
        
        # Test connection
        hook.get_conn().close()
        
        logger.info(f"Successfully initialized PostgresHook with connection '{conn_id}'")
        return hook
    
    except Exception as e:
        logger.error(f"Failed to initialize PostgresHook with connection '{conn_id}': {str(e)}")
        raise AirflowException(f"Failed to initialize PostgresHook: {str(e)}")


def get_cloud_sql_hook(conn_id: str = None) -> CloudSQLHook:
    """
    Get a CloudSQLHook instance with the provided connection ID.
    
    Args:
        conn_id: Connection ID to use (defaults to CLOUD_SQL_CONN_ID)
        
    Returns:
        Configured CloudSQLHook instance
        
    Raises:
        AirflowException: If connection cannot be established
    """
    conn_id = conn_id or CLOUD_SQL_CONN_ID
    
    # Validate connection
    validation = validate_connection_internal(conn_id)
    if validation['status'] == 'invalid':
        issues = '; '.join(validation['issues'])
        raise AirflowException(f"Invalid connection '{conn_id}': {issues}")
    
    try:
        hook = CloudSQLHook(gcp_conn_id=conn_id)
        
        # Test connection by initializing client
        hook.get_conn()
        
        logger.info(f"Successfully initialized CloudSQLHook with connection '{conn_id}'")
        return hook
    
    except Exception as e:
        logger.error(f"Failed to initialize CloudSQLHook with connection '{conn_id}': {str(e)}")
        raise AirflowException(f"Failed to initialize CloudSQLHook: {str(e)}")


def execute_query(
    sql: str,
    parameters: Dict = None,
    conn_id: str = None,
    autocommit: bool = False,
    return_dict: bool = False
) -> List:
    """
    Execute a SQL query and return the results.
    
    Args:
        sql: SQL query to execute
        parameters: Query parameters (optional)
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        autocommit: Whether to autocommit the query
        return_dict: If True, return results as list of dictionaries
        
    Returns:
        Query results as list of tuples or dictionaries
        
    Raises:
        AirflowException: If query execution fails
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    parameters = parameters or {}
    
    try:
        hook = get_postgres_hook(conn_id=conn_id)
        
        logger.info(f"Executing query using connection '{conn_id}'")
        
        if return_dict:
            # For dictionary results, we need a custom PostgresHook method
            conn = hook.get_conn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql, parameters)
            result = cursor.fetchall()
            
            # Convert from RealDictRow to regular dict
            result = [dict(row) for row in result]
            
            cursor.close()
            conn.close()
        else:
            result = hook.run(sql, parameters=parameters, autocommit=autocommit)
        
        row_count = len(result) if result else 0
        logger.info(f"Query executed successfully, returned {row_count} rows")
        
        return result
    
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise AirflowException(f"Failed to execute query: {str(e)}")


def execute_query_as_df(
    sql: str,
    parameters: Dict = None,
    conn_id: str = None
) -> DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame.
    
    Args:
        sql: SQL query to execute
        parameters: Query parameters (optional)
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        
    Returns:
        Query results as pandas DataFrame
        
    Raises:
        AirflowException: If query execution fails
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    parameters = parameters or {}
    
    try:
        hook = get_postgres_hook(conn_id=conn_id)
        
        logger.info(f"Executing query as DataFrame using connection '{conn_id}'")
        df = hook.get_pandas_df(sql, parameters=parameters)
        
        logger.info(f"Query executed successfully, returned DataFrame with shape {df.shape}")
        return df
    
    except Exception as e:
        logger.error(f"Query execution as DataFrame failed: {str(e)}")
        raise AirflowException(f"Failed to execute query as DataFrame: {str(e)}")


def execute_batch(
    sql: str,
    params_list: List[Dict],
    conn_id: str = None,
    autocommit: bool = False
) -> bool:
    """
    Execute a batch of SQL statements with parameters.
    
    Args:
        sql: SQL statement template to execute
        params_list: List of parameter dictionaries for batch execution
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        autocommit: Whether to autocommit after each batch
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    
    if not params_list:
        logger.warning("No parameters provided for batch execution")
        return False
    
    try:
        hook = get_postgres_hook(conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        logger.info(f"Executing batch of {len(params_list)} operations")
        
        # Use psycopg2's execute_batch for efficient batch processing
        execute_batch(cursor, sql, params_list)
        
        if autocommit:
            conn.commit()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully executed batch of {len(params_list)} operations")
        return True
    
    except Exception as e:
        logger.error(f"Batch execution failed: {str(e)}")
        return False


def bulk_load_from_csv(
    csv_path: str,
    table_name: str,
    conn_id: str = None,
    schema: str = None,
    delimiter: str = ',',
    header: bool = True
) -> bool:
    """
    Load data from a CSV file into a database table.
    
    Args:
        csv_path: Path to the CSV file
        table_name: Destination table name
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        delimiter: CSV delimiter character (defaults to comma)
        header: Whether CSV has a header row (defaults to True)
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    # Check if file exists
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return False
    
    try:
        # Count records for logging
        with open(csv_path, 'r', encoding='utf-8') as f:
            if header:
                next(f)  # Skip header
            record_count = sum(1 for _ in f)
        
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        qualified_table = f"{schema}.{table_name}" if schema else table_name
        
        logger.info(f"Loading {record_count} records from {csv_path} to {qualified_table}")
        
        # Construct the COPY command
        copy_sql = f"""
            COPY {qualified_table} FROM STDIN 
            WITH (FORMAT CSV, DELIMITER '{delimiter}'{', HEADER' if header else ''})
        """
        
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(copy_sql, f)
                conn.commit()
        
        logger.info(f"Successfully loaded {record_count} records into {qualified_table}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to load data from CSV: {str(e)}")
        return False


def bulk_load_from_df(
    df: DataFrame,
    table_name: str,
    conn_id: str = None,
    schema: str = None,
    if_exists: str = 'replace',
    index: bool = False,
    dtype: Dict = None
) -> bool:
    """
    Load data from a pandas DataFrame into a database table.
    
    Args:
        df: Pandas DataFrame to load
        table_name: Destination table name
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        if_exists: Action if table exists ('fail', 'replace', or 'append')
        index: Whether to include DataFrame index (defaults to False)
        dtype: Column data types to force
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    if df.empty:
        logger.warning("DataFrame is empty, no data to load")
        return False
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        conn = hook.get_conn()
        
        # Create SQLAlchemy engine from connection
        engine = sqlalchemy.create_engine('postgresql://', creator=lambda: conn)
        
        logger.info(f"Loading DataFrame with {len(df)} rows to {schema}.{table_name}")
        
        # Use pandas to_sql for efficient DataFrame loading
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=index,
            dtype=dtype,
            chunksize=DEFAULT_CHUNK_SIZE
        )
        
        logger.info(f"Successfully loaded {len(df)} rows into {schema}.{table_name}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to load DataFrame to database: {str(e)}")
        return False


def table_exists(
    table_name: str,
    conn_id: str = None,
    schema: str = None
) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        table_name: Table name to check
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        
    Returns:
        True if table exists, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        
        # Query information_schema to check table existence
        check_sql = """
            SELECT COUNT(1) FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        """
        
        result = hook.run(check_sql, parameters=(schema, table_name))
        exists = result[0][0] > 0
        
        if exists:
            logger.info(f"Table {schema}.{table_name} exists")
        else:
            logger.info(f"Table {schema}.{table_name} does not exist")
        
        return exists
    
    except Exception as e:
        logger.error(f"Failed to check if table exists: {str(e)}")
        return False


def create_table(
    table_name: str,
    table_definition: str,
    conn_id: str = None,
    schema: str = None,
    if_not_exists: bool = True
) -> bool:
    """
    Create a new table in the database.
    
    Args:
        table_name: Name of the table to create
        table_definition: SQL table definition (column and constraint specifications)
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        if_not_exists: Add IF NOT EXISTS clause to prevent errors if table exists
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    # Check if table exists when if_not_exists is True
    if if_not_exists and table_exists(table_name, conn_id, schema):
        logger.info(f"Table {schema}.{table_name} already exists, skipping creation")
        return True
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        
        # Construct CREATE TABLE statement
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        create_sql = f"CREATE TABLE {exists_clause}{schema}.{table_name} ({table_definition})"
        
        logger.info(f"Creating table {schema}.{table_name}")
        hook.run(create_sql, autocommit=True)
        
        logger.info(f"Successfully created table {schema}.{table_name}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        return False


def drop_table(
    table_name: str,
    conn_id: str = None,
    schema: str = None,
    if_exists: bool = True,
    cascade: bool = False
) -> bool:
    """
    Drop a table from the database.
    
    Args:
        table_name: Name of the table to drop
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        if_exists: Add IF EXISTS clause to prevent errors if table doesn't exist
        cascade: Add CASCADE clause to drop dependent objects
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    # Check if table exists when if_exists is True
    if if_exists and not table_exists(table_name, conn_id, schema):
        logger.info(f"Table {schema}.{table_name} does not exist, skipping drop")
        return True
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        
        # Construct DROP TABLE statement
        exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        drop_sql = f"DROP TABLE {exists_clause}{schema}.{table_name}{cascade_clause}"
        
        logger.info(f"Dropping table {schema}.{table_name}")
        hook.run(drop_sql, autocommit=True)
        
        logger.info(f"Successfully dropped table {schema}.{table_name}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to drop table: {str(e)}")
        return False


def truncate_table(
    table_name: str,
    conn_id: str = None,
    schema: str = None,
    cascade: bool = False
) -> bool:
    """
    Truncate a table in the database.
    
    Args:
        table_name: Name of the table to truncate
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        cascade: Add CASCADE clause to truncate dependent tables
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    # Check if table exists
    if not table_exists(table_name, conn_id, schema):
        logger.error(f"Table {schema}.{table_name} does not exist, can't truncate")
        return False
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        
        # Construct TRUNCATE TABLE statement
        cascade_clause = " CASCADE" if cascade else ""
        truncate_sql = f"TRUNCATE TABLE {schema}.{table_name}{cascade_clause}"
        
        logger.info(f"Truncating table {schema}.{table_name}")
        hook.run(truncate_sql, autocommit=True)
        
        logger.info(f"Successfully truncated table {schema}.{table_name}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to truncate table: {str(e)}")
        return False


def copy_table(
    source_table: str,
    target_table: str,
    conn_id: str = None,
    schema: str = None,
    where_clause: str = None,
    columns: List[str] = None,
    truncate_target: bool = False,
    create_target: bool = False
) -> bool:
    """
    Copy data from one table to another.
    
    Args:
        source_table: Source table name
        target_table: Target table name
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        where_clause: Optional WHERE clause to filter source data
        columns: List of columns to copy (defaults to all columns)
        truncate_target: Whether to truncate target table before copying
        create_target: Whether to create target table if it doesn't exist
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    # Check if source table exists
    if not table_exists(source_table, conn_id, schema):
        logger.error(f"Source table {schema}.{source_table} does not exist")
        return False
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        
        # Create target table if requested
        if create_target and not table_exists(target_table, conn_id, schema):
            logger.info(f"Creating target table {schema}.{target_table} based on source schema")
            
            # Get source table schema
            create_sql = f"""
                CREATE TABLE {schema}.{target_table} AS 
                SELECT * FROM {schema}.{source_table} WHERE 1=0
            """
            hook.run(create_sql, autocommit=True)
        
        # Truncate target table if requested
        if truncate_target and table_exists(target_table, conn_id, schema):
            truncate_table(target_table, conn_id, schema)
        
        # Build column list for INSERT
        column_str = ", ".join(columns) if columns else "*"
        
        # Build WHERE clause
        where_str = f" WHERE {where_clause}" if where_clause else ""
        
        # Construct INSERT statement
        insert_sql = f"""
            INSERT INTO {schema}.{target_table}
            SELECT {column_str} FROM {schema}.{source_table}{where_str}
        """
        
        logger.info(f"Copying data from {schema}.{source_table} to {schema}.{target_table}")
        hook.run(insert_sql, autocommit=True)
        
        # Get row count for logging
        count_sql = f"SELECT COUNT(1) FROM {schema}.{target_table}"
        result = hook.run(count_sql)
        row_count = result[0][0] if result else 0
        
        logger.info(f"Successfully copied {row_count} rows to {schema}.{target_table}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to copy table: {str(e)}")
        return False


def execute_transaction(
    statements: List[str],
    parameters: List[Dict] = None,
    conn_id: str = None
) -> bool:
    """
    Execute multiple SQL statements in a transaction.
    
    Args:
        statements: List of SQL statements to execute
        parameters: List of parameter dictionaries for each statement (optional)
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    
    if not statements:
        logger.warning("No statements provided for transaction")
        return False
    
    # Ensure parameters length matches statements
    if parameters is None:
        parameters = [{}] * len(statements)
    elif len(parameters) != len(statements):
        logger.error("Parameters list length doesn't match statements list length")
        return False
    
    try:
        hook = get_postgres_hook(conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        logger.info(f"Executing transaction with {len(statements)} statements")
        
        try:
            # Execute each statement with its parameters
            for i, (stmt, params) in enumerate(zip(statements, parameters)):
                cursor.execute(stmt, params)
                logger.debug(f"Executed statement {i+1} in transaction")
            
            # Commit transaction
            conn.commit()
            logger.info("Transaction committed successfully")
            
        except Exception as e:
            # Roll back transaction on error
            conn.rollback()
            logger.error(f"Transaction failed, rolled back: {str(e)}")
            raise
        
        finally:
            cursor.close()
            conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"Failed to execute transaction: {str(e)}")
        return False


def get_table_row_count(
    table_name: str,
    conn_id: str = None,
    schema: str = None,
    where_clause: str = None
) -> int:
    """
    Get the number of rows in a table.
    
    Args:
        table_name: Name of the table
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        where_clause: Optional WHERE clause to filter count
        
    Returns:
        Number of rows in the table
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        
        # Build WHERE clause
        where_str = f" WHERE {where_clause}" if where_clause else ""
        
        # Construct COUNT query
        count_sql = f"SELECT COUNT(1) FROM {schema}.{table_name}{where_str}"
        
        logger.info(f"Getting row count for {schema}.{table_name}")
        result = hook.run(count_sql)
        
        row_count = result[0][0] if result else 0
        logger.info(f"Table {schema}.{table_name} has {row_count} rows")
        
        return row_count
    
    except Exception as e:
        logger.error(f"Failed to get row count: {str(e)}")
        return 0


def get_table_schema(
    table_name: str,
    conn_id: str = None,
    schema: str = None
) -> List[Dict]:
    """
    Get table schema information.
    
    Args:
        table_name: Name of the table
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        schema: Database schema (defaults to DEFAULT_SCHEMA)
        
    Returns:
        List of column definitions with name, type, and constraints
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    schema = schema or DEFAULT_SCHEMA
    
    try:
        hook = get_postgres_hook(conn_id=conn_id, schema=schema)
        
        # Query information_schema to get column details
        column_sql = """
            SELECT 
                column_name, 
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM 
                information_schema.columns
            WHERE 
                table_schema = %s AND table_name = %s
            ORDER BY 
                ordinal_position
        """
        
        logger.info(f"Getting schema for {schema}.{table_name}")
        columns = hook.run(column_sql, parameters=(schema, table_name))
        
        # Format column information
        column_defs = []
        for col in columns:
            col_name, data_type, max_length, nullable, default = col
            
            # Format type with length if applicable
            type_def = data_type
            if max_length:
                type_def = f"{data_type}({max_length})"
            
            # Format nullable constraint
            null_def = "NULL" if nullable == "YES" else "NOT NULL"
            
            # Format default
            default_def = f"DEFAULT {default}" if default else ""
            
            column_defs.append({
                "name": col_name,
                "type": type_def,
                "nullable": nullable == "YES",
                "default": default,
                "definition": f"{col_name} {type_def} {null_def} {default_def}".strip()
            })
        
        logger.info(f"Retrieved schema for {schema}.{table_name} with {len(column_defs)} columns")
        return column_defs
    
    except Exception as e:
        logger.error(f"Failed to get table schema: {str(e)}")
        return []


def run_migration_script(
    script_path: str,
    conn_id: str = None,
    transaction: bool = True
) -> bool:
    """
    Execute SQL statements from a migration script file.
    
    Args:
        script_path: Path to SQL script file
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        transaction: Whether to execute script in a transaction
        
    Returns:
        True if successful, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    
    # Check if script file exists
    if not os.path.exists(script_path):
        logger.error(f"Migration script not found: {script_path}")
        return False
    
    try:
        # Read script file
        with open(script_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        # Split script into individual statements
        # This is a simple implementation and may not handle all SQL edge cases
        statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
        
        logger.info(f"Executing migration script {script_path} with {len(statements)} statements")
        
        if transaction:
            # Execute all statements in a transaction
            result = execute_transaction(statements=statements, conn_id=conn_id)
        else:
            # Execute statements individually
            hook = get_postgres_hook(conn_id=conn_id)
            for i, stmt in enumerate(statements):
                hook.run(stmt, autocommit=True)
                logger.debug(f"Executed statement {i+1}/{len(statements)}")
            result = True
        
        if result:
            logger.info(f"Successfully executed migration script {script_path}")
        
        return result
    
    except Exception as e:
        logger.error(f"Failed to execute migration script: {str(e)}")
        return False


def get_db_info(conn_id: str = None) -> Dict:
    """
    Get database information including version and settings.
    
    Args:
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        
    Returns:
        Dictionary with database information
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    
    try:
        hook = get_postgres_hook(conn_id=conn_id)
        
        # Get PostgreSQL version
        version_sql = "SELECT version()"
        version_result = hook.run(version_sql)
        version = version_result[0][0] if version_result else "Unknown"
        
        # Get database settings
        settings_sql = """
            SELECT name, setting, category, short_desc
            FROM pg_settings
            WHERE name IN (
                'max_connections', 'shared_buffers', 'work_mem', 
                'maintenance_work_mem', 'effective_cache_size'
            )
        """
        settings_result = hook.run(settings_sql)
        settings = {row[0]: {
            'value': row[1], 
            'category': row[2], 
            'description': row[3]
        } for row in settings_result}
        
        # Get database statistics
        stats_sql = """
            SELECT 
                pg_database.datname as database_name,
                pg_size_pretty(pg_database_size(pg_database.datname)) as size
            FROM pg_database
            WHERE pg_database.datname = current_database()
        """
        stats_result = hook.run(stats_sql)
        stats = {
            'database_name': stats_result[0][0] if stats_result else "Unknown",
            'size': stats_result[0][1] if stats_result else "Unknown"
        }
        
        # Get connection information
        connection = hook.get_connection(conn_id)
        conn_info = {
            'host': connection.host,
            'port': connection.port,
            'schema': connection.schema,
            'login': connection.login
        }
        
        db_info = {
            'version': version,
            'settings': settings,
            'statistics': stats,
            'connection': conn_info
        }
        
        logger.info(f"Retrieved database information for connection '{conn_id}'")
        return db_info
    
    except Exception as e:
        logger.error(f"Failed to get database information: {str(e)}")
        return {}


def verify_connection(conn_id: str = None) -> bool:
    """
    Verify database connection is working.
    
    Args:
        conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
        
    Returns:
        True if connection is valid, False otherwise
    """
    conn_id = conn_id or POSTGRES_CONN_ID
    
    try:
        hook = get_postgres_hook(conn_id=conn_id)
        
        # Execute simple test query
        test_sql = "SELECT 1 AS connection_test"
        result = hook.run(test_sql)
        
        is_valid = result and result[0][0] == 1
        
        if is_valid:
            logger.info(f"Successfully verified connection '{conn_id}'")
        else:
            logger.warning(f"Connection '{conn_id}' test query returned unexpected result")
        
        return is_valid
    
    except Exception as e:
        logger.error(f"Connection '{conn_id}' verification failed: {str(e)}")
        return False


class DBConnectionManager:
    """
    Context manager for database connections.
    
    This class provides a simplified way to manage database connections
    using the 'with' statement, ensuring proper resource cleanup.
    
    Example:
        with DBConnectionManager(conn_id='my_postgres') as manager:
            result = manager.cursor.execute("SELECT * FROM my_table").fetchall()
    """
    
    def __init__(self, conn_id: str = None, schema: str = None):
        """
        Initialize the connection manager.
        
        Args:
            conn_id: Connection ID to use (defaults to POSTGRES_CONN_ID)
            schema: Database schema (defaults to DEFAULT_SCHEMA)
        """
        self.conn_id = conn_id or POSTGRES_CONN_ID
        self.schema = schema or DEFAULT_SCHEMA
        self.hook = None
        self.connection = None
        self.cursor = None
    
    def __enter__(self):
        """
        Establish database connection when entering context.
        
        Returns:
            Self reference for context manager
            
        Raises:
            AirflowException: If connection cannot be established
        """
        try:
            self.hook = get_postgres_hook(conn_id=self.conn_id, schema=self.schema)
            self.connection = self.hook.get_conn()
            self.cursor = self.connection.cursor()
            
            logger.debug(f"Established database connection using '{self.conn_id}'")
            return self
        
        except Exception as e:
            logger.error(f"Failed to establish database connection: {str(e)}")
            self.__exit__(None, None, None)
            raise AirflowException(f"Failed to establish database connection: {str(e)}")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close database connection when exiting context.
        
        Args:
            exc_type: Exception type if an exception was raised in the context
            exc_val: Exception value if an exception was raised in the context
            exc_tb: Exception traceback if an exception was raised in the context
        """
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            
            if self.connection:
                self.connection.close()
                self.connection = None
            
            self.hook = None
            
            logger.debug("Closed database connection")
        
        except Exception as e:
            logger.warning(f"Error while closing database connection: {str(e)}")