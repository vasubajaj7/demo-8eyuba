"""
Custom PostgreSQL hook for Apache Airflow 2.X that extends the standard PostgresHook 
with enhanced functionality for data processing, error handling, and transaction 
management to support the migration from Airflow 1.10.15 to Cloud Composer 2.
"""

from typing import List, Dict, Union, Optional, Any, Tuple
import logging
import pandas as pd  # pandas v1.3.5
import psycopg2  # psycopg2-binary v2.9.3
import psycopg2.extras  # psycopg2-binary v2.9.3
import sqlalchemy  # sqlalchemy v1.4.0+
import tenacity  # tenacity v6.2.0+

from airflow.providers.postgres.hooks.postgres import PostgresHook  # airflow.providers.postgres v2.0.0+
from airflow.exceptions import AirflowException  # airflow v2.0.0+

# Internal imports
from ...dags.utils.db_utils import validate_connection_internal, DEFAULT_SCHEMA

# Configure logging
logger = logging.getLogger('airflow.hooks.custom_postgres')

# Global constants
DEFAULT_POSTGRES_CONN_ID = 'postgres_default'
DEFAULT_SCHEMA = DEFAULT_SCHEMA  # Imported from db_utils
MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0


class CustomPostgresHook(PostgresHook):
    """
    Enhanced PostgreSQL hook that extends Airflow's PostgresHook with additional 
    functionality for data processing, error handling, and transaction management.
    
    This hook is designed to provide a seamless migration path from Airflow 1.10.15 
    to Airflow 2.X while adding robust error handling, connection pooling, and 
    data processing capabilities.
    """
    
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = DEFAULT_POSTGRES_CONN_ID
    
    def __init__(
        self,
        postgres_conn_id: str = DEFAULT_POSTGRES_CONN_ID,
        schema: str = DEFAULT_SCHEMA,
        use_persistent_connection: bool = False,
        retry_count: int = MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY
    ):
        """
        Initialize the CustomPostgresHook with enhanced configurations.
        
        Args:
            postgres_conn_id: The Airflow connection ID for PostgreSQL
            schema: The database schema to use
            use_persistent_connection: Whether to maintain a persistent database connection
            retry_count: Number of times to retry operations on failure
            retry_delay: Delay between retry attempts in seconds
        """
        super().__init__(postgres_conn_id=postgres_conn_id, schema=schema)
        self._conn = None
        self._engine = None
        self._use_persistent_connection = use_persistent_connection
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        
        logger.info(f"Initialized CustomPostgresHook with connection ID '{postgres_conn_id}' "
                   f"and schema '{schema}', persistent connection: {use_persistent_connection}")
    
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(MAX_RETRIES),
        wait=tenacity.wait_fixed(DEFAULT_RETRY_DELAY),
        retry=tenacity.retry_if_exception_type(Exception),
        before=tenacity.before_log(logger, logging.INFO),
        after=tenacity.after_log(logger, logging.INFO)
    )
    def get_conn(self):
        """
        Get a PostgreSQL connection with persistent connection support.
        
        Returns:
            PostgreSQL database connection
            
        Raises:
            AirflowException: If connection cannot be established
        """
        try:
            # Return cached connection if using persistent connections
            if self._use_persistent_connection and self._conn is not None:
                if not self._conn.closed:
                    return self._conn
                else:
                    logger.info("Persistent connection is closed, reconnecting...")
                    self._conn = None
            
            # Get a new connection using parent class method
            conn = super().get_conn()
            
            # Cache the connection if using persistent connections
            if self._use_persistent_connection:
                self._conn = conn
                
            logger.debug(f"Established PostgreSQL connection using '{self.postgres_conn_id}'")
            return conn
            
        except Exception as e:
            error_msg = f"Failed to establish PostgreSQL connection: {str(e)}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
    
    def get_sqlalchemy_engine(self):
        """
        Get a SQLAlchemy engine with connection pooling.
        
        Returns:
            SQLAlchemy engine for database operations
            
        Raises:
            AirflowException: If engine creation fails
        """
        try:
            # Return cached engine if using persistent connections
            if self._use_persistent_connection and self._engine is not None:
                return self._engine
                
            # Get connection URI from Airflow connection
            conn = self.get_connection(self.postgres_conn_id)
            uri = conn.get_uri()
            
            # Create SQLAlchemy engine with connection pooling options
            engine = sqlalchemy.create_engine(
                uri,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=5,
                max_overflow=10
            )
            
            # Cache the engine if using persistent connections
            if self._use_persistent_connection:
                self._engine = engine
                
            logger.debug("Created SQLAlchemy engine with connection pooling")
            return engine
            
        except Exception as e:
            error_msg = f"Failed to create SQLAlchemy engine: {str(e)}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
    
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(MAX_RETRIES),
        wait=tenacity.wait_fixed(DEFAULT_RETRY_DELAY),
        retry=tenacity.retry_if_exception_type(Exception),
        before=tenacity.before_log(logger, logging.INFO),
        after=tenacity.after_log(logger, logging.WARNING)
    )
    def execute_query(
        self,
        sql: str,
        parameters: Dict = None,
        autocommit: bool = False,
        return_dict: bool = False
    ) -> List:
        """
        Execute a SQL query with enhanced error handling and retry logic.
        
        Args:
            sql: SQL query to execute
            parameters: Query parameters (optional)
            autocommit: Whether to autocommit the query
            return_dict: If True, return results as list of dictionaries
            
        Returns:
            Query results as list of tuples or dictionaries
            
        Raises:
            AirflowException: If query execution fails
        """
        parameters = parameters or {}
        conn = None
        cursor = None
        
        try:
            conn = self.get_conn()
            
            if return_dict:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                cursor = conn.cursor()
                
            logger.info(f"Executing query: {sql}")
            cursor.execute(sql, parameters)
            
            # Fetch results if any
            if cursor.description:
                results = cursor.fetchall()
                
                # Convert from RealDictRow to regular dict if using dict cursor
                if return_dict:
                    results = [dict(row) for row in results]
            else:
                results = []
                
            # Commit if requested
            if autocommit:
                conn.commit()
                
            row_count = len(results) if results else 0
            logger.info(f"Query executed successfully, returned {row_count} rows")
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to execute query: {str(e)}"
            logger.error(error_msg)
            if conn and not autocommit:
                conn.rollback()
                logger.info("Transaction rolled back")
            raise AirflowException(error_msg)
            
        finally:
            if cursor:
                cursor.close()
                
            # Close connection if not using persistent connections
            if conn and not self._use_persistent_connection:
                conn.close()
                logger.debug("Database connection closed")
    
    def execute_values(
        self,
        sql: str,
        values: List,
        template: str = None,
        page_size: int = 1000,
        fetch: bool = False,
        autocommit: bool = False
    ) -> List:
        """
        Execute a batch insert using psycopg2.extras.execute_values.
        
        Args:
            sql: SQL statement for batch insert
            values: List of parameter tuples or dictionaries
            template: Optional template string for execute_values
            page_size: Number of rows per batch
            fetch: Whether to fetch and return results
            autocommit: Whether to autocommit the operation
            
        Returns:
            Query results if fetch is True, otherwise None
            
        Raises:
            AirflowException: If batch insert fails
        """
        conn = None
        cursor = None
        
        try:
            conn = self.get_conn()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor if fetch else None)
            
            logger.info(f"Executing batch insert with {len(values)} values")
            psycopg2.extras.execute_values(
                cursor,
                sql,
                values,
                template=template,
                page_size=page_size
            )
            
            # Fetch results if requested
            results = None
            if fetch and cursor.description:
                results = cursor.fetchall()
                if isinstance(results[0], psycopg2.extras.RealDictRow):
                    results = [dict(row) for row in results]
                    
            # Commit if requested
            if autocommit:
                conn.commit()
                
            affected_rows = cursor.rowcount
            logger.info(f"Batch insert completed, affected {affected_rows} rows")
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to execute batch insert: {str(e)}"
            logger.error(error_msg)
            if conn and not autocommit:
                conn.rollback()
                logger.info("Transaction rolled back")
            raise AirflowException(error_msg)
            
        finally:
            if cursor:
                cursor.close()
                
            # Close connection if not using persistent connections
            if conn and not self._use_persistent_connection:
                conn.close()
                logger.debug("Database connection closed")
    
    def execute_batch(
        self,
        sql: str,
        params_list: List,
        batch_size: int = 1000,
        autocommit: bool = False
    ) -> bool:
        """
        Execute a batch of statements using psycopg2.extras.execute_batch.
        
        Args:
            sql: SQL statement to execute in batch
            params_list: List of parameter tuples or dictionaries
            batch_size: Number of operations per batch
            autocommit: Whether to autocommit the operation
            
        Returns:
            True if successful, False otherwise
        """
        conn = None
        cursor = None
        
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            
            logger.info(f"Executing batch of {len(params_list)} operations")
            psycopg2.extras.execute_batch(
                cursor,
                sql,
                params_list,
                page_size=batch_size
            )
            
            # Commit if requested
            if autocommit:
                conn.commit()
                
            affected_rows = cursor.rowcount
            logger.info(f"Batch execution completed, affected {affected_rows} rows")
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to execute batch: {str(e)}"
            logger.error(error_msg)
            if conn and not autocommit:
                conn.rollback()
                logger.info("Transaction rolled back")
            return False
            
        finally:
            if cursor:
                cursor.close()
                
            # Close connection if not using persistent connections
            if conn and not self._use_persistent_connection:
                conn.close()
                logger.debug("Database connection closed")
    
    def query_to_df(
        self,
        sql: str,
        parameters: Dict = None,
        columns: List = None
    ) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.
        
        Args:
            sql: SQL query to execute
            parameters: Query parameters (optional)
            columns: Column names for the DataFrame (optional)
            
        Returns:
            Query results as pandas DataFrame
            
        Raises:
            AirflowException: If query execution fails
        """
        parameters = parameters or {}
        
        try:
            engine = self.get_sqlalchemy_engine()
            
            logger.info(f"Executing query as DataFrame: {sql}")
            df = pd.read_sql(
                sql,
                engine,
                params=parameters
            )
            
            # Apply column names if provided
            if columns and len(columns) == len(df.columns):
                df.columns = columns
                
            logger.info(f"Query executed successfully, returned DataFrame with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            error_msg = f"Failed to execute query as DataFrame: {str(e)}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
            
        finally:
            # Close engine connection if not using persistent connections
            if not self._use_persistent_connection and 'engine' in locals():
                engine.dispose()
                logger.debug("SQLAlchemy engine disposed")
    
    def df_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = None,
        if_exists: str = 'replace',
        index: bool = False,
        dtype: Dict = None,
        chunksize: int = 1000
    ) -> bool:
        """
        Load a pandas DataFrame into a database table.
        
        Args:
            df: Pandas DataFrame to load
            table_name: Destination table name
            schema: Database schema (defaults to hook's schema)
            if_exists: Action if table exists ('fail', 'replace', or 'append')
            index: Whether to include DataFrame index
            dtype: Column data types to force
            chunksize: Number of rows per batch
            
        Returns:
            True if successful, False otherwise
        """
        schema = schema or self.schema
        
        try:
            engine = self.get_sqlalchemy_engine()
            
            if df.empty:
                logger.warning("DataFrame is empty, no data to load to database")
                return False
                
            logger.info(f"Loading DataFrame with {len(df)} rows to {schema}.{table_name}")
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=index,
                dtype=dtype,
                chunksize=chunksize
            )
            
            logger.info(f"Successfully loaded {len(df)} rows into {schema}.{table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to load DataFrame to table: {str(e)}"
            logger.error(error_msg)
            return False
            
        finally:
            # Close engine connection if not using persistent connections
            if not self._use_persistent_connection and 'engine' in locals():
                engine.dispose()
                logger.debug("SQLAlchemy engine disposed")
    
    def test_connection(self) -> bool:
        """
        Test if the database connection is working.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Validate connection configuration
            validation_result = validate_connection_internal(self.postgres_conn_id)
            if validation_result['status'] == 'invalid':
                issues = '; '.join(validation_result['issues'])
                logger.error(f"Invalid connection configuration: {issues}")
                return False
            
            # Test connection with a simple query
            conn = None
            cursor = None
            
            try:
                conn = self.get_conn()
                cursor = conn.cursor()
                
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                
                is_successful = result and result[0] == 1
                
                if is_successful:
                    logger.info(f"Connection test successful for {self.postgres_conn_id}")
                else:
                    logger.warning(f"Connection test failed for {self.postgres_conn_id}")
                    
                return is_successful
                
            finally:
                if cursor:
                    cursor.close()
                    
                # Close connection if not using persistent connections
                if conn and not self._use_persistent_connection:
                    conn.close()
                    logger.debug("Database connection closed")
        
        except Exception as e:
            logger.error(f"Connection test failed for {self.postgres_conn_id}: {str(e)}")
            return False
    
    def create_schema_if_not_exists(
        self,
        schema_name: str
    ) -> bool:
        """
        Create a database schema if it doesn't exist.
        
        Args:
            schema_name: Name of the schema to create
            
        Returns:
            True if successful, False otherwise
        """
        conn = None
        cursor = None
        
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            
            # Check if schema exists
            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                (schema_name,)
            )
            exists = cursor.fetchone() is not None
            
            if exists:
                logger.info(f"Schema {schema_name} already exists")
                return True
                
            # Create schema
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            conn.commit()
            
            logger.info(f"Successfully created schema {schema_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to create schema {schema_name}: {str(e)}"
            logger.error(error_msg)
            if conn:
                conn.rollback()
                logger.info("Transaction rolled back")
            return False
            
        finally:
            if cursor:
                cursor.close()
                
            # Close connection if not using persistent connections
            if conn and not self._use_persistent_connection:
                conn.close()
                logger.debug("Database connection closed")
    
    def create_table_if_not_exists(
        self,
        table_name: str,
        table_definition: str,
        schema: str = None
    ) -> bool:
        """
        Create a database table if it doesn't exist.
        
        Args:
            table_name: Name of the table to create
            table_definition: SQL table definition (column and constraint specifications)
            schema: Database schema (defaults to hook's schema)
            
        Returns:
            True if successful, False otherwise
        """
        schema = schema or self.schema
        conn = None
        cursor = None
        
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table_name)
            )
            exists = cursor.fetchone() is not None
            
            if exists:
                logger.info(f"Table {schema}.{table_name} already exists")
                return True
                
            # Create table
            create_sql = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({table_definition})"
            cursor.execute(create_sql)
            conn.commit()
            
            logger.info(f"Successfully created table {schema}.{table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to create table {schema}.{table_name}: {str(e)}"
            logger.error(error_msg)
            if conn:
                conn.rollback()
                logger.info("Transaction rolled back")
            return False
            
        finally:
            if cursor:
                cursor.close()
                
            # Close connection if not using persistent connections
            if conn and not self._use_persistent_connection:
                conn.close()
                logger.debug("Database connection closed")
    
    def copy_expert(
        self,
        sql: str,
        file_obj,
        size: int = 8192,
        autocommit: bool = False
    ) -> bool:
        """
        Execute a COPY command using psycopg2's copy_expert method.
        
        Args:
            sql: COPY SQL statement
            file_obj: File-like object to read from or write to
            size: Size of the read buffer
            autocommit: Whether to autocommit the operation
            
        Returns:
            True if successful, False otherwise
        """
        conn = None
        cursor = None
        
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            
            logger.info(f"Executing COPY command: {sql}")
            cursor.copy_expert(sql, file_obj, size=size)
            
            # Commit if requested
            if autocommit:
                conn.commit()
                
            affected_rows = cursor.rowcount
            logger.info(f"COPY command completed, affected {affected_rows} rows")
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to execute COPY command: {str(e)}"
            logger.error(error_msg)
            if conn and not autocommit:
                conn.rollback()
                logger.info("Transaction rolled back")
            return False
            
        finally:
            if cursor:
                cursor.close()
                
            # Close connection if not using persistent connections
            if conn and not self._use_persistent_connection:
                conn.close()
                logger.debug("Database connection closed")
    
    def get_table_info(
        self,
        table_name: str,
        schema: str = None
    ) -> Dict:
        """
        Get detailed information about a database table.
        
        Args:
            table_name: Name of the table
            schema: Database schema (defaults to hook's schema)
            
        Returns:
            Dictionary with table structure information
        """
        schema = schema or self.schema
        result = {}
        
        try:
            # Get column information
            columns_sql = """
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
            columns = self.execute_query(columns_sql, parameters={"table_schema": schema, "table_name": table_name})
            
            # Format column information
            column_info = []
            for col in columns:
                col_name, data_type, max_length, nullable, default = col
                
                # Format type with length if applicable
                type_def = data_type
                if max_length:
                    type_def = f"{data_type}({max_length})"
                
                column_info.append({
                    "name": col_name,
                    "type": type_def,
                    "nullable": nullable == "YES",
                    "default": default
                })
                
            # Get primary key information
            pk_sql = """
                SELECT 
                    kcu.column_name
                FROM 
                    information_schema.table_constraints tc
                JOIN 
                    information_schema.key_column_usage kcu
                ON 
                    tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE 
                    tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
                ORDER BY 
                    kcu.ordinal_position
            """
            pk_columns = self.execute_query(pk_sql, parameters={"table_schema": schema, "table_name": table_name})
            pk_columns = [row[0] for row in pk_columns]
            
            # Get index information
            index_sql = """
                SELECT 
                    i.relname as index_name,
                    a.attname as column_name,
                    ix.indisunique as is_unique
                FROM 
                    pg_index ix
                JOIN 
                    pg_class i ON i.oid = ix.indexrelid
                JOIN 
                    pg_class t ON t.oid = ix.indrelid
                JOIN 
                    pg_namespace n ON n.oid = t.relnamespace
                JOIN 
                    pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE 
                    t.relname = %s
                    AND n.nspname = %s
                ORDER BY 
                    i.relname, a.attnum
            """
            index_data = self.execute_query(index_sql, parameters={"table_name": table_name, "nspname": schema})
            
            # Format index information
            indexes = {}
            for row in index_data:
                index_name, column_name, is_unique = row
                if index_name not in indexes:
                    indexes[index_name] = {"columns": [], "unique": is_unique}
                indexes[index_name]["columns"].append(column_name)
            
            result = {
                "table_name": table_name,
                "schema": schema,
                "columns": column_info,
                "primary_key": pk_columns,
                "indexes": indexes
            }
            
            logger.info(f"Retrieved information for table {schema}.{table_name}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to get table information for {schema}.{table_name}: {str(e)}"
            logger.error(error_msg)
            return {}
    
    def run_transaction(
        self,
        statements: List[str],
        parameters: List[Dict] = None,
        isolation_level: str = None
    ) -> List:
        """
        Execute multiple SQL statements in a transaction.
        
        Args:
            statements: List of SQL statements to execute
            parameters: List of parameter dictionaries for each statement (optional)
            isolation_level: Transaction isolation level (None, 'READ UNCOMMITTED', 
                            'READ COMMITTED', 'REPEATABLE READ', or 'SERIALIZABLE')
            
        Returns:
            Results from executed statements
            
        Raises:
            AirflowException: If transaction execution fails
        """
        if not statements:
            logger.warning("No statements provided for transaction")
            return []
            
        # Ensure parameters length matches statements
        if parameters is None:
            parameters = [{}] * len(statements)
        elif len(parameters) != len(statements):
            error_msg = "Parameters list length doesn't match statements list length"
            logger.error(error_msg)
            raise AirflowException(error_msg)
            
        conn = None
        cursor = None
        results = []
        
        try:
            conn = self.get_conn()
            
            # Set isolation level if provided
            if isolation_level:
                if isolation_level.upper() == 'READ UNCOMMITTED':
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)
                elif isolation_level.upper() == 'READ COMMITTED':
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
                elif isolation_level.upper() == 'REPEATABLE READ':
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
                elif isolation_level.upper() == 'SERIALIZABLE':
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
            
            # Start transaction
            conn.autocommit = False
            cursor = conn.cursor()
            
            # Create a savepoint for partial rollback support
            cursor.execute("SAVEPOINT transaction_begin")
            
            logger.info(f"Executing transaction with {len(statements)} statements")
            
            # Execute each statement with its parameters
            for i, (stmt, params) in enumerate(zip(statements, parameters)):
                cursor.execute(stmt, params)
                
                # Collect results if any
                if cursor.description:
                    stmt_results = cursor.fetchall()
                    results.append(stmt_results)
                else:
                    results.append([])
                    
                logger.debug(f"Executed statement {i+1}/{len(statements)} in transaction")
            
            # Commit transaction
            conn.commit()
            logger.info("Transaction committed successfully")
            
            return results
            
        except Exception as e:
            error_msg = f"Transaction failed: {str(e)}"
            logger.error(error_msg)
            
            # Roll back transaction
            if conn:
                conn.rollback()
                logger.info("Transaction rolled back")
                
            raise AirflowException(error_msg)
            
        finally:
            if cursor:
                cursor.close()
                
            # Close connection if not using persistent connections
            if conn and not self._use_persistent_connection:
                conn.close()
                logger.debug("Database connection closed")
    
    def close_conn(self) -> None:
        """
        Close database connection and engine.
        """
        try:
            # Close connection if it exists
            if self._conn is not None:
                if not self._conn.closed:
                    self._conn.close()
                self._conn = None
                logger.debug("Database connection closed")
                
            # Dispose engine if it exists
            if self._engine is not None:
                self._engine.dispose()
                self._engine = None
                logger.debug("SQLAlchemy engine disposed")
                
        except Exception as e:
            logger.warning(f"Error while closing database resources: {str(e)}")
    
    def get_pool_status(self) -> Dict:
        """
        Get status information about the connection pool.
        
        Returns:
            Dictionary with pool status information
        """
        try:
            if not hasattr(self, '_engine') or self._engine is None:
                logger.warning("No SQLAlchemy engine available")
                return {}
                
            if not hasattr(self._engine, 'pool'):
                logger.warning("Engine does not have a connection pool")
                return {}
                
            pool = self._engine.pool
            status = {
                "pool_size": pool.size(),
                "checkedin": pool.checkedin(),
                "checkedout": pool.checkedout(),
                "overflow": pool.overflow(),
                "use_persistent_connection": self._use_persistent_connection
            }
            
            logger.debug(f"Pool status: {status}")
            return status
            
        except Exception as e:
            logger.error(f"Failed to get pool status: {str(e)}")
            return {}