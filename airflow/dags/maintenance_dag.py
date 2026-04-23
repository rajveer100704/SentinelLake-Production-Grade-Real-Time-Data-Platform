from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    'delta_lake_maintenance',
    default_args=default_args,
    description='Handles Compaction (Small File Problem) and Z-Ordering',
    schedule_interval='@daily',
    catchup=False,
    tags=['maintenance', 'performance'],
) as dag:

    # 1. Optimize Silver Layer (Compaction + Z-Ordering)
    # Z-Ordering on user_id improves MERGE performance significantly
    optimize_silver = BashOperator(
        task_id='optimize_silver',
        bash_command='spark-submit --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog -e "OPTIMIZE delta.\'/tmp/delta/silver\' ZORDER BY (user_id)"',
    )

    # 2. Optimize Gold Layer
    optimize_gold = BashOperator(
        task_id='optimize_gold',
        bash_command='spark-submit --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog -e "OPTIMIZE delta.\'/tmp/delta/gold\'"',
    )

    # 3. Vacuum (Cleanup old versions to save costs)
    vacuum_tables = BashOperator(
        task_id='vacuum_tables',
        bash_command='spark-submit --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog -e "VACUUM delta.\'/tmp/delta/silver\' RETAIN 168 HOURS"',
    )

    optimize_silver >> optimize_gold >> vacuum_tables
