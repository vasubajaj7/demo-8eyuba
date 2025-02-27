"""Initial database schema migration for Apache Airflow 2.X

This migration creates the foundational database schema required for 
Apache Airflow 2.X in Cloud Composer 2. It establishes all the necessary
tables, columns, constraints, and indexes to support migration from 
Airflow 1.10.15 to Airflow 2.X.

Revision ID: d0e3cef313cc
Revises: None
Create Date: 2023-01-15 00:00:00.000000
"""

import uuid
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic
revision = 'd0e3cef313cc'
down_revision = None
branch_labels = None
depends_on = None
create_date = datetime(2023, 1, 15)


def upgrade():
    """Creates the initial database schema for Airflow 2.X."""
    conn = op.get_bind()
    
    # ========== Core Tables ==========
    
    # Create DAG table
    op.create_table(
        'dag',
        sa.Column('dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('root_dag_id', sa.String(250), nullable=True),
        sa.Column('is_paused', sa.Boolean, default=False),
        sa.Column('is_subdag', sa.Boolean, default=False),
        sa.Column('is_active', sa.Boolean, default=True),
        sa.Column('last_parsed_time', sa.DateTime, nullable=True),
        sa.Column('last_pickled', sa.DateTime, nullable=True),
        sa.Column('last_expired', sa.DateTime, nullable=True),
        sa.Column('scheduler_lock', sa.Boolean, default=False),
        sa.Column('pickle_id', sa.Integer, nullable=True),
        sa.Column('fileloc', sa.String(2000), nullable=True),
        sa.Column('owners', sa.String(2000), nullable=True),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('default_view', sa.String(25), default='grid'),
        sa.Column('schedule_interval', sa.Text, nullable=True),
        sa.Column('timetable_description', sa.String(1000), nullable=True),
        sa.Column('tags', postgresql.JSONB, nullable=True),
        sa.Column('has_task_concurrency_limits', sa.Boolean, default=True),
        sa.Column('max_active_tasks', sa.Integer, default=16),
        sa.Column('max_active_runs', sa.Integer, default=16),
    )
    
    # Create DAG Run table
    op.create_table(
        'dag_run',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('dag_id', sa.String(250), nullable=False),
        sa.Column('execution_date', sa.DateTime, nullable=False),
        sa.Column('state', sa.String(50), nullable=True),
        sa.Column('run_id', sa.String(250), nullable=False),
        sa.Column('external_trigger', sa.Boolean, default=False),
        sa.Column('conf', postgresql.JSONB, nullable=True),
        sa.Column('end_date', sa.DateTime, nullable=True),
        sa.Column('start_date', sa.DateTime, nullable=True),
        sa.Column('run_type', sa.String(50), nullable=False, default='manual'),
        sa.Column('last_scheduling_decision', sa.DateTime, nullable=True),
        sa.Column('creating_job_id', sa.Integer, nullable=True),
        sa.Column('data_interval_start', sa.DateTime, nullable=True),
        sa.Column('data_interval_end', sa.DateTime, nullable=True),
        sa.Column('queued_at', sa.DateTime, nullable=True),
        sa.Column('updated_at', sa.DateTime, nullable=True),
        sa.UniqueConstraint('dag_id', 'run_id', name='dag_run_dag_id_run_id_key'),
        sa.UniqueConstraint('dag_id', 'execution_date', name='dag_run_dag_id_execution_date_key'),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
    )
    
    # Create Task Instance table
    op.create_table(
        'task_instance',
        sa.Column('task_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('run_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('map_index', sa.Integer, primary_key=True, nullable=False, server_default='-1'),
        sa.Column('start_date', sa.DateTime, nullable=True),
        sa.Column('end_date', sa.DateTime, nullable=True),
        sa.Column('duration', sa.Float, nullable=True),
        sa.Column('state', sa.String(20), nullable=True),
        sa.Column('try_number', sa.Integer, default=0),
        sa.Column('max_tries', sa.Integer, default=-1),
        sa.Column('hostname', sa.String(1000), nullable=True),
        sa.Column('unixname', sa.String(1000), nullable=True),
        sa.Column('job_id', sa.Integer, nullable=True),
        sa.Column('pool', sa.String(256), nullable=True),
        sa.Column('pool_slots', sa.Integer, default=1),
        sa.Column('queue', sa.String(256), nullable=True),
        sa.Column('priority_weight', sa.Integer, nullable=True),
        sa.Column('operator', sa.String(1000), nullable=True),
        sa.Column('queued_dttm', sa.DateTime, nullable=True),
        sa.Column('queued_by_job_id', sa.Integer, nullable=True),
        sa.Column('pid', sa.Integer, nullable=True),
        sa.Column('executor_config', postgresql.JSONB, nullable=True),
        sa.Column('updated_at', sa.DateTime, nullable=True),
        sa.Column('external_executor_id', sa.String(250), nullable=True),
        sa.Column('trigger_id', sa.Integer, nullable=True),
        sa.Column('trigger_timeout', sa.DateTime, nullable=True),
        sa.Column('next_method', sa.String(1000), nullable=True),
        sa.Column('next_kwargs', postgresql.JSONB, nullable=True),
        sa.ForeignKeyConstraint(['dag_id', 'run_id'], ['dag_run.dag_id', 'dag_run.run_id'],
                               ondelete='CASCADE', name='task_instance_dag_run_fkey'),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE', name='task_instance_dag_id_fkey'),
    )
    
    # Create XCom table
    op.create_table(
        'xcom',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('key', sa.String(512), nullable=False),
        sa.Column('value', sa.LargeBinary, nullable=True),
        sa.Column('timestamp', sa.DateTime, default=datetime.utcnow, nullable=False),
        sa.Column('dag_id', sa.String(250), nullable=False),
        sa.Column('task_id', sa.String(250), nullable=False),
        sa.Column('run_id', sa.String(250), nullable=False),
        sa.Column('map_index', sa.Integer, nullable=False, server_default='-1'),
        sa.ForeignKeyConstraint(['dag_id', 'task_id', 'run_id', 'map_index'],
                              ['task_instance.dag_id', 'task_instance.task_id',
                               'task_instance.run_id', 'task_instance.map_index'],
                              name='xcom_task_instance_fkey',
                              ondelete='CASCADE'),
    )
    
    # Create Variable table
    op.create_table(
        'variable',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('key', sa.String(250), unique=True, nullable=False),
        sa.Column('val', sa.Text, nullable=True),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('is_encrypted', sa.Boolean, default=False),
    )
    
    # Create Connection table
    op.create_table(
        'connection',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('conn_id', sa.String(250), unique=True, nullable=False),
        sa.Column('conn_type', sa.String(500), nullable=False),
        sa.Column('host', sa.String(500), nullable=True),
        sa.Column('schema', sa.String(500), nullable=True),
        sa.Column('login', sa.String(500), nullable=True),
        sa.Column('password', sa.String(5000), nullable=True),
        sa.Column('port', sa.Integer, nullable=True),
        sa.Column('extra', sa.Text, nullable=True),
        sa.Column('is_encrypted', sa.Boolean, default=False),
        sa.Column('is_extra_encrypted', sa.Boolean, default=False),
        sa.Column('description', sa.Text, nullable=True),
    )
    
    # Create Slot Pool table
    op.create_table(
        'slot_pool',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('pool', sa.String(256), unique=True, nullable=False),
        sa.Column('slots', sa.Integer, default=0),
        sa.Column('description', sa.Text, nullable=True),
    )
    
    # Create SLA Miss table
    op.create_table(
        'sla_miss',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('task_id', sa.String(250), nullable=False),
        sa.Column('dag_id', sa.String(250), nullable=False),
        sa.Column('execution_date', sa.DateTime, nullable=False),
        sa.Column('email_sent', sa.Boolean, default=False),
        sa.Column('timestamp', sa.DateTime, nullable=False),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('notification_sent', sa.Boolean, default=False),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
        sa.UniqueConstraint('task_id', 'dag_id', 'execution_date', name='sla_miss_unique_constraint'),
    )
    
    # Create Task Reschedule table
    op.create_table(
        'task_reschedule',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('task_id', sa.String(250), nullable=False),
        sa.Column('dag_id', sa.String(250), nullable=False),
        sa.Column('run_id', sa.String(250), nullable=False),
        sa.Column('map_index', sa.Integer, nullable=False, server_default='-1'),
        sa.Column('try_number', sa.Integer, nullable=False),
        sa.Column('start_date', sa.DateTime, nullable=False),
        sa.Column('end_date', sa.DateTime, nullable=False),
        sa.Column('reschedule_date', sa.DateTime, nullable=False),
        sa.ForeignKeyConstraint(['dag_id', 'task_id', 'run_id', 'map_index'],
                              ['task_instance.dag_id', 'task_instance.task_id',
                               'task_instance.run_id', 'task_instance.map_index'],
                              name='task_reschedule_task_instance_fkey',
                              ondelete='CASCADE'),
    )
    
    # Create Import Error table
    op.create_table(
        'import_error',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('timestamp', sa.DateTime, nullable=False),
        sa.Column('filename', sa.String(1024), nullable=False),
        sa.Column('stacktrace', sa.Text, nullable=False),
    )
    
    # Create Job table
    op.create_table(
        'job',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('dag_id', sa.String(250), nullable=True),
        sa.Column('state', sa.String(20), nullable=True),
        sa.Column('job_type', sa.String(30), nullable=False),
        sa.Column('start_date', sa.DateTime, nullable=False),
        sa.Column('end_date', sa.DateTime, nullable=True),
        sa.Column('latest_heartbeat', sa.DateTime, nullable=False),
        sa.Column('executor_class', sa.String(500), nullable=True),
        sa.Column('hostname', sa.String(500), nullable=False),
        sa.Column('unixname', sa.String(1000), nullable=False),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
    )
    
    # ========== Airflow 2.X Specific Tables ==========
    
    # Create Serialized DAG table
    op.create_table(
        'serialized_dag',
        sa.Column('dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('fileloc', sa.String(2000), nullable=False),
        sa.Column('fileloc_hash', sa.Integer, nullable=False),
        sa.Column('data', sa.LargeBinary, nullable=False),
        sa.Column('last_updated', sa.DateTime, nullable=False),
        sa.Column('dag_hash', sa.String(32), nullable=False),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
    )
    
    # Create DAG Tag table
    op.create_table(
        'dag_tag',
        sa.Column('dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('tag', sa.String(100), primary_key=True, nullable=False),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
    )
    
    # Create DB Upgrade Meta table
    op.create_table(
        'db_upgrade_meta',
        sa.Column('id', sa.Integer, primary_key=True, nullable=False, autoincrement=True),
        sa.Column('from_version', sa.String(100), nullable=True),
        sa.Column('to_version', sa.String(100), nullable=True),
        sa.Column('start_time', sa.DateTime, nullable=True),
        sa.Column('end_time', sa.DateTime, nullable=True),
        sa.Column('result', sa.Text, nullable=True),
        sa.Column('success', sa.Boolean, nullable=True),
    )
    
    # Create Task Map table
    op.create_table(
        'task_map',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('dag_id', sa.String(250), nullable=False),
        sa.Column('task_id', sa.String(250), nullable=False),
        sa.Column('run_id', sa.String(250), nullable=False),
        sa.Column('map_index', sa.Integer, nullable=False),
        sa.Column('length', sa.Integer, nullable=False),
        sa.Column('keys', sa.Text, nullable=True),
        sa.ForeignKeyConstraint(['dag_id', 'task_id', 'run_id'],
                              ['task_instance.dag_id', 'task_instance.task_id', 'task_instance.run_id'],
                              name='task_map_task_instance_fkey',
                              ondelete='CASCADE'),
        sa.UniqueConstraint('dag_id', 'task_id', 'run_id', 'map_index',
                          name='task_map_unique_constraint'),
    )
    
    # Create Rendered Task Instance Fields table
    op.create_table(
        'rendered_task_instance_fields',
        sa.Column('dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('task_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('run_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('map_index', sa.Integer, primary_key=True, nullable=False, server_default='-1'),
        sa.Column('rendered_fields', sa.LargeBinary, nullable=False),
        sa.Column('k8s_pod_yaml', sa.LargeBinary, nullable=True),
        sa.ForeignKeyConstraint(['dag_id', 'task_id', 'run_id', 'map_index'],
                              ['task_instance.dag_id', 'task_instance.task_id',
                               'task_instance.run_id', 'task_instance.map_index'],
                              name='rtif_task_instance_fkey',
                              ondelete='CASCADE'),
    )
    
    # Create Dataset tables (Airflow 2.3+)
    op.create_table(
        'dataset',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('uri', sa.String(3000), nullable=False, unique=True),
        sa.Column('extra', postgresql.JSONB, nullable=True),
        sa.Column('created_at', sa.DateTime, default=datetime.utcnow, nullable=False),
        sa.Column('updated_at', sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False),
    )
    
    op.create_table(
        'dataset_event',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('dataset_id', sa.Integer, nullable=False),
        sa.Column('extra', postgresql.JSONB, nullable=True),
        sa.Column('source_task_id', sa.String(250), nullable=True),
        sa.Column('source_dag_id', sa.String(250), nullable=True),
        sa.Column('source_run_id', sa.String(250), nullable=True),
        sa.Column('source_map_index', sa.Integer, nullable=True),
        sa.Column('timestamp', sa.DateTime, default=datetime.utcnow, nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(
            ['source_dag_id', 'source_task_id', 'source_run_id', 'source_map_index'],
            ['task_instance.dag_id', 'task_instance.task_id', 'task_instance.run_id', 'task_instance.map_index'],
            name='dataset_event_task_instance_fkey',
            ondelete='SET NULL',
        ),
    )
    
    op.create_table(
        'task_outlet',
        sa.Column('dataset_id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('task_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('created_at', sa.DateTime, default=datetime.utcnow, nullable=False),
        sa.Column('updated_at', sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['dag_id', 'task_id'], ['task_instance.dag_id', 'task_instance.task_id'],
                              name='task_outlet_task_instance_fkey',
                              ondelete='CASCADE'),
    )
    
    op.create_table(
        'dag_schedule_dataset_reference',
        sa.Column('dataset_id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('created_at', sa.DateTime, default=datetime.utcnow, nullable=False),
        sa.Column('updated_at', sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
    )
    
    op.create_table(
        'dataset_dag_run_queue',
        sa.Column('dataset_id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('target_dag_id', sa.String(250), primary_key=True, nullable=False),
        sa.Column('created_at', sa.DateTime, default=datetime.utcnow, nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['target_dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
    )
    
    # Create Callback Request (for triggering deferred tasks)
    op.create_table(
        'callback_request',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('created_at', sa.DateTime, default=datetime.utcnow, nullable=False),
        sa.Column('priority_weight', sa.Integer, nullable=False),
        sa.Column('callback_data', sa.LargeBinary, nullable=False),
        sa.Column('processor_subdir', sa.String(250), nullable=False),
    )
    
    # ========== Create Indexes ==========
    
    # DAG indexes
    op.create_index('idx_dag_dag_id', 'dag', ['dag_id'])
    op.create_index('idx_dag_root_dag_id', 'dag', ['root_dag_id'])
    
    # DAG Run indexes
    op.create_index('idx_dag_run_dag_id', 'dag_run', ['dag_id'])
    op.create_index('idx_dag_run_state', 'dag_run', ['state'])
    op.create_index('idx_dag_run_execution_date', 'dag_run', ['execution_date'])
    op.create_index('idx_dag_run_run_id', 'dag_run', ['run_id'])
    op.create_index('idx_dag_run_creating_job_id', 'dag_run', ['creating_job_id'])
    op.create_index('idx_dag_run_dag_id_execution_date', 'dag_run', ['dag_id', 'execution_date'])
    op.create_index('idx_dag_run_dag_id_run_id', 'dag_run', ['dag_id', 'run_id'])
    
    # Task Instance indexes
    op.create_index('idx_task_instance_dag_id', 'task_instance', ['dag_id'])
    op.create_index('idx_task_instance_state', 'task_instance', ['state'])
    op.create_index('idx_task_instance_pool', 'task_instance', ['pool'])
    op.create_index('idx_task_instance_job_id', 'task_instance', ['job_id'])
    op.create_index('idx_task_instance_dag_id_task_id', 'task_instance', ['dag_id', 'task_id'])
    op.create_index('idx_task_instance_dag_id_state', 'task_instance', ['dag_id', 'state'])
    op.create_index('idx_task_instance_dag_run', 'task_instance', ['dag_id', 'run_id'])
    op.create_index('idx_task_instance_dag_id_task_id_state', 'task_instance', ['dag_id', 'task_id', 'state'])
    
    # XCom indexes
    op.create_index('idx_xcom_dag_task_date', 'xcom', ['dag_id', 'task_id', 'run_id', 'map_index'])
    op.create_index('idx_xcom_key', 'xcom', ['key'])
    
    # Connection indexes
    op.create_index('idx_connection_conn_id', 'connection', ['conn_id'])
    
    # Variable indexes
    op.create_index('idx_variable_key', 'variable', ['key'])
    
    # Job indexes
    op.create_index('idx_job_dag_id', 'job', ['dag_id'])
    op.create_index('idx_job_state_heartbeat', 'job', ['state', 'latest_heartbeat'])
    op.create_index('idx_job_type_state', 'job', ['job_type', 'state'])
    
    # Serialized DAG indexes
    op.create_index('idx_serialized_dag_fileloc_hash', 'serialized_dag', ['fileloc_hash'])
    
    # DAG Tag index
    op.create_index('idx_dag_tag_dag_id', 'dag_tag', ['dag_id'])
    
    # Dataset indexes
    op.create_index('idx_dataset_uri', 'dataset', ['uri'])
    op.create_index('idx_dataset_event_dataset_id', 'dataset_event', ['dataset_id'])
    op.create_index('idx_dataset_event_source_task', 'dataset_event', 
                  ['source_dag_id', 'source_task_id', 'source_run_id', 'source_map_index'])
    
    # Callback Request indexes
    op.create_index('idx_callback_request_processor_subdir', 'callback_request', ['processor_subdir'])


def downgrade():
    """Removes the entire database schema created in the upgrade function."""
    
    # Drop tables in reverse order to respect foreign key constraints
    
    # First drop indexes (not strictly necessary as dropping tables drops indexes)
    
    # Drop Airflow 2.X specific tables
    op.drop_table('callback_request')
    op.drop_table('dataset_dag_run_queue')
    op.drop_table('dag_schedule_dataset_reference')
    op.drop_table('task_outlet')
    op.drop_table('dataset_event')
    op.drop_table('dataset')
    op.drop_table('rendered_task_instance_fields')
    op.drop_table('task_map')
    op.drop_table('dag_tag')
    op.drop_table('serialized_dag')
    
    # Drop task-related tables
    op.drop_table('task_reschedule')
    op.drop_table('xcom')
    op.drop_table('task_instance')
    
    # Drop DAG-related tables
    op.drop_table('sla_miss')
    op.drop_table('dag_run')
    
    # Drop job-related tables
    op.drop_table('job')
    
    # Drop configuration tables
    op.drop_table('slot_pool')
    op.drop_table('connection')
    op.drop_table('variable')
    op.drop_table('import_error')
    
    # Drop core tables last
    op.drop_table('dag')
    op.drop_table('db_upgrade_meta')