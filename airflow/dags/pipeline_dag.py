from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lakehouse_coordination_batch',
    default_args=default_args,
    description='Orchestrates Batch transformations and Feature Materialization',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['coordination', 'batch'],
) as dag:

    # 1. Data Quality Validation (Blocking Step)
    # Validates the Silver layer before analytics/features are built
    validate_silver = BashOperator(
        task_id='validate_silver_layer',
        bash_command='python /app/data_quality/validate.py',
    )

    # 2. Run dbt Transformations (Aggregating Gold Layer)
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /app/dbt_project && dbt run',
    )

    # 3. Materialize Features for ML Serving
    # Syncs offline Delta data to online Redis store
    materialize_feast = BashOperator(
        task_id='materialize_feast',
        bash_command='cd /app/feature_repo && feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")',
    )

    # Dependency Chain
    validate_silver >> run_dbt >> materialize_feast
