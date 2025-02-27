"""Migration from Airflow 1.10.15 to Airflow 2.X compatible schema

Revision ID: a9190532efb2
Revises: previous_migration_id
Create Date: 2023-05-01

This migration script implements the necessary schema changes to upgrade
from Airflow 1.10.15 to Airflow 2.X compatibility, supporting Cloud Composer 2.
"""

# revision identifiers, used by Alembic
revision = 'a9190532efb2'  # Generated using uuid.uuid4().hex
down_revision = 'previous_migration_id'  # Replace with actual previous migration ID
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func
import uuid
from datetime import datetime, timedelta


def upgrade():
    """
    Upgrades the database schema from Airflow 1.10.15 to Airflow 2.X compatibility.
    
    This function:
    1. Creates new Airflow 2.X specific tables
    2. Adds new columns to existing tables
    3. Creates optimal indexes for performance
    4. Sets up proper foreign key constraints
    5. Performs data migrations where necessary
    """
    
    # Add new columns to dag_run table
    op.add_column('dag_run', sa.Column('data_interval_start', sa.DateTime(), nullable=True))
    op.add_column('dag_run', sa.Column('data_interval_end', sa.DateTime(), nullable=True))
    op.add_column('dag_run', sa.Column('logical_date', sa.DateTime(), nullable=True))
    
    # Backfill logical_date with execution_date
    op.execute("UPDATE dag_run SET logical_date = execution_date")
    
    # Add not-null constraint to logical_date after backfill
    op.alter_column('dag_run', 'logical_date', nullable=False)
    
    # Add new columns to task_instance table
    op.add_column('task_instance', sa.Column('map_index', sa.Integer(), server_default='-1', nullable=False))
    op.add_column('task_instance', sa.Column('operator', sa.String(length=1000), nullable=True))
    op.add_column('task_instance', sa.Column('pool_slots', sa.Integer(), server_default='1', nullable=False))
    
    # Add new columns to xcom table
    op.add_column('xcom', sa.Column('value_source', sa.String(length=100), nullable=True))
    op.add_column('xcom', sa.Column('serialized_value', sa.LargeBinary(), nullable=True))
    
    # Create new table: dag_tag
    op.create_table(
        'dag_tag',
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('tag_name', sa.String(length=100), nullable=False),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('dag_id', 'tag_name')
    )
    
    # Create new table: serialized_dag
    op.create_table(
        'serialized_dag',
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('fileloc', sa.String(length=2000), nullable=False),
        sa.Column('data', sa.Text(), nullable=False),
        sa.Column('last_updated', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.PrimaryKeyConstraint('dag_id')
    )
    
    # Create new table: rendered_task_instance_fields
    op.create_table(
        'rendered_task_instance_fields',
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('task_id', sa.String(length=250), nullable=False),
        sa.Column('execution_date', sa.DateTime(), nullable=False),
        sa.Column('rendered_fields', sa.Text(), nullable=False),
        sa.Column('map_index', sa.Integer(), nullable=False, server_default='-1'),
        sa.ForeignKeyConstraint(
            ['dag_id', 'task_id', 'execution_date', 'map_index'],
            ['task_instance.dag_id', 'task_instance.task_id', 'task_instance.execution_date', 'task_instance.map_index'],
            name='rtif_ti_fkey',
            ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('dag_id', 'task_id', 'execution_date', 'map_index')
    )
    
    # Create new table: task_map
    op.create_table(
        'task_map',
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('task_id', sa.String(length=250), nullable=False),
        sa.Column('execution_date', sa.DateTime(), nullable=False),
        sa.Column('map_index', sa.Integer(), nullable=False),
        sa.Column('length', sa.Integer(), nullable=True),
        sa.Column('keys', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ['dag_id', 'task_id', 'execution_date'],
            ['task_instance.dag_id', 'task_instance.task_id', 'task_instance.execution_date'],
            name='task_map_ti_fkey',
            ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('dag_id', 'task_id', 'execution_date', 'map_index')
    )
    
    # Create dataset tables
    op.create_table(
        'dataset',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('uri', sa.String(length=3000), nullable=False),
        sa.Column('extra', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_index('idx_dataset_uri', 'dataset', ['uri'], unique=True)
    
    op.create_table(
        'dataset_event',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dataset_id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.Column('source_task_id', sa.String(length=250), nullable=True),
        sa.Column('source_dag_id', sa.String(length=250), nullable=True),
        sa.Column('source_run_id', sa.String(length=250), nullable=True),
        sa.Column('source_map_index', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_index('idx_dataset_event_timestamp', 'dataset_event', ['timestamp'], unique=False)
    
    op.create_table(
        'dataset_dag_run_queue',
        sa.Column('dataset_id', sa.Integer(), nullable=False),
        sa.Column('target_dag_id', sa.String(length=250), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('dataset_id', 'target_dag_id')
    )
    
    op.create_table(
        'dag_schedule_dataset_reference',
        sa.Column('dataset_id', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('dataset_id', 'dag_id')
    )
    
    op.create_table(
        'task_outlet_dataset_reference',
        sa.Column('dataset_id', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('task_id', sa.String(length=250), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.ForeignKeyConstraint(['dataset_id'], ['dataset.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['dag_id', 'task_id'], ['task.dag_id', 'task.task_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('dataset_id', 'dag_id', 'task_id')
    )
    
    # Create callback_request table
    op.create_table(
        'callback_request',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(length=250), nullable=True),
        sa.Column('task_id', sa.String(length=250), nullable=True),
        sa.Column('execution_date', sa.DateTime(), nullable=True),
        sa.Column('callback_data', sa.LargeBinary(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.Column('priority_weight', sa.Integer(), server_default='0', nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_index('idx_callback_request_created_at', 'callback_request', ['created_at'], unique=False)
    
    # Create additional indexes
    op.create_index('idx_task_instance_map_index', 'task_instance', ['map_index'], unique=False)
    op.create_index('idx_dag_run_data_interval', 'dag_run', ['data_interval_start', 'data_interval_end'], unique=False)


def downgrade():
    """
    Downgrades the database schema back to Airflow 1.10.15 compatibility.
    
    This function:
    1. Drops all Airflow 2.X specific tables
    2. Removes added columns from existing tables
    3. Drops indexes and constraints specific to Airflow 2.X
    """
    
    # Drop indexes
    op.drop_index('idx_dag_run_data_interval', table_name='dag_run')
    op.drop_index('idx_task_instance_map_index', table_name='task_instance')
    op.drop_index('idx_callback_request_created_at', table_name='callback_request')
    op.drop_index('idx_dataset_event_timestamp', table_name='dataset_event')
    op.drop_index('idx_dataset_uri', table_name='dataset')
    
    # Drop tables in reverse order to avoid constraint violations
    op.drop_table('callback_request')
    
    # Drop dataset tables
    op.drop_table('task_outlet_dataset_reference')
    op.drop_table('dag_schedule_dataset_reference')
    op.drop_table('dataset_dag_run_queue')
    op.drop_table('dataset_event')
    op.drop_table('dataset')
    
    # Drop other Airflow 2.X specific tables
    op.drop_table('task_map')
    op.drop_table('rendered_task_instance_fields')
    op.drop_table('serialized_dag')
    op.drop_table('dag_tag')
    
    # Drop added columns from xcom table
    op.drop_column('xcom', 'serialized_value')
    op.drop_column('xcom', 'value_source')
    
    # Drop added columns from task_instance table
    op.drop_column('task_instance', 'pool_slots')
    op.drop_column('task_instance', 'operator')
    op.drop_column('task_instance', 'map_index')
    
    # Drop added columns from dag_run table
    op.drop_column('dag_run', 'logical_date')
    op.drop_column('dag_run', 'data_interval_end')
    op.drop_column('dag_run', 'data_interval_start')