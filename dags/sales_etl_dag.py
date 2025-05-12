from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    transform_sales_data = GlueJobOperator(
        task_id='transform_sales_data',
        job_name='sales_data_transformer',
        script_location='s3://etl-scripts-bucket/glue_scripts/sales_transformer.py',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        script_args={
            '--INPUT_PATH': 's3://raw-sales-data/{{ ds }}/',
            '--OUTPUT_PATH': 's3://etl-temp-bucket/{{ ds }}/'
        }
    )
    
    load_to_redshift = RedshiftDataOperator(
        task_id='load_to_redshift',
        sql='sql/refresh_materialized_views.sql',
        redshift_conn_id='redshift_default',
        database='sales_dw'
    )
    
    transform_sales_data >> load_to_redshift