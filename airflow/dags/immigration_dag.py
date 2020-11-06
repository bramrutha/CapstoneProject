from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from operators.create_tables import CreateTableOperator
from operators.load_tables import StageToRedshiftOperator
from operators.data_quality import DataQualityOperator
from datetime import datetime
from helpers.sql_queries import SqlQueries


default_args = {
    'owner' : 'amrutha',
    'depends_on_past' : True,
    'retries' : 2,
    'start_date' : datetime(2020,10,17,0,0,0,0),
    'end_date' : datetime(2020,10,18,0,0,0,0),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'catchup':False
}

#Defining the Dag
dag = DAG('immigration_pipeline',
          default_args = default_args,
          description = 'Extract data from S3 and load it in the Immigration warehouse',
          schedule_interval = '0 0 * * *',
          max_active_runs = 1)

#defining the first task to begin execution
start_task = DummyOperator(task_id = 'Begin Execution',dag=dag)

#Creating a Hook to coonect to EMR
####CreateSSH Coonection in Airflow
emr_ssh_hook = SSHHook(conn_id='emr_conn_id')

#Task to run ETL from Raw S3 to Transformed S3
run_emr_task = SSHOperator(
            task_id = 'immigration_etl_to_s3',
            command = 'cd /home/hadoop;spark-submit --master yarn immigration_driver.py',
            ssh_hook = emr_ssh_hook,
            dag=dag
)

#Task to create tables in Redshift
create_table_operator = CreateTableOperator(
            task_id = 'create_tables_in_redshift',
            redshift_conn_id = 'redshift',
            sql_query = [SqlQueries.dates_create_table,SqlQueries.immigration_create_table,
                         SqlQueries.weather_create_table,SqlQueries.state_create_table,SqlQueries.airport_create_table],
            provide_context=True,
            dag=dag
)

#Task to load the tables in Redshift
load_dates_table = StageToRedshiftOperator(
    task_id = 'load_dates_table',
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    table = 'dates',
    s3_bucket = 'amr-transformed',
    s3_key = 'dates.parquet',
    copy_options = 'FORMAT AS PARQUET',
    provide_context=True,
    dag=dag
)

load_immigration_table = StageToRedshiftOperator(
    task_id='load_immigration_table',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    table='immigration',
    s3_bucket='amr-transformed',
    s3_key='immigration.parquet',
    copy_options='FORMAT AS PARQUET',
    provide_context=True,
    dag=dag
)

load_weather_table = StageToRedshiftOperator(
    task_id='load_weather_table',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    s3_bucket='amr-transformed',
    s3_key='weather.parquet',
    table='weather',
    copy_options='FORMAT AS PARQUET',
    provide_context=True,
    dag=dag
)

load_state_table = StageToRedshiftOperator(
    task_id='load_state_table',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    s3_bucket='amr-transformed',
    s3_key='state.parquet',
    table='states',
    copy_options='FORMAT AS PARQUET',
    provide_context=True,
    dag=dag
)

load_airport_table = StageToRedshiftOperator(
    task_id='load_airport_table',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    s3_bucket='amr-transformed',
    s3_key='airport.parquet',
    table='airports',
    copy_options='FORMAT AS PARQUET',
    provide_context=True,
    dag=dag
)

data_quality_task = DataQualityOperator(
    task_id='data_quality_task',
    redshift_conn_id='redshift',
    tables=['immigration','weather','states','airports','dates'],
    provide_context=True,
    dag=dag
)

start_task >> run_emr_task
run_emr_task >> create_table_operator
create_table_operator >> load_dates_table
create_table_operator >> load_immigration_table
create_table_operator >> load_weather_table
create_table_operator >> load_state_table
create_table_operator >> load_airport_table
load_dates_table >> data_quality_task
load_immigration_table >> data_quality_task
load_weather_table >> data_quality_task
load_state_table >> data_quality_task
load_airport_table >> data_quality_task


