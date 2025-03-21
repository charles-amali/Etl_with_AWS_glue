from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from plugins.helpers.validation import validate_data
from plugins.helpers.archiving import archive_processed_files

default_args = {
    'owner': 'charles',
    'start_date': days_ago(1),
    'retries': 3,

}

dag = DAG(
    'music_streaming_pipeline',
    default_args=default_args,
    description='ETL pipeline for streaming data',
    schedule_interval=None,
    catchup=False
)

# Monitor S3 for new files
check_users = S3KeySensor(
    task_id='check_users_file',
    bucket_name='music-stream-bkt',
    bucket_key='data/users/users.csv',
    aws_conn_id='aws_default',
    timeout=10,
    poke_interval=60,
    mode='poke',
    dag=dag
)

check_songs = S3KeySensor(
    task_id='check_songs_file',
    bucket_name='music-stream-bkt',
    bucket_key='data/songs/songs.csv',
    aws_conn_id='aws_default',
    timeout=10,
    poke_interval=10,
    mode='poke',
    dag=dag
)

check_streams = S3KeySensor(
    task_id='check_streams_file',
    bucket_name='music-stream-bkt',
    bucket_key='data/streams/streams1.csv',
    aws_conn_id='aws_default',
    timeout=10,
    poke_interval=60,
    mode='poke',
    dag=dag
)

# Validate Data
validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    op_kwargs={
        'bucket': 'music-stream-bkt',
        'keys': {
            'users': 'data/users/users.csv',
            'songs': 'data/songs/songs.csv',
            'streams': 'data/streams/streams1.csv'
        }
    },
    do_xcom_push=True, 
    dag=dag
)

job_name = 'Stream-KPI-Job'
script_location ='s3://aws-glue-assets-842676015206-eu-west-1/scripts/Stream-KPI-Job.py' 
s3_songs_path = 's3://music-stream-bkt/processed/songs/20250320_160751_songs.csv'
s3_users_path = 's3://music-stream-bkt/processed/users/20250320_160749_users.csv'
s3_streams_path = 's3://music-stream-bkt/processed/streams/20250320_160753_streams1.csv'
target_dynamodb_table = 'Dynamo-Kpis'

run_job = GlueJobOperator(
    task_id='run_glue_job_task',
    job_name=job_name,
    script_location=script_location,
    script_args={
        '--JOB_NAME': job_name,
        '--s3_songs_path': s3_songs_path,
        '--s3_users_path': s3_users_path,
        '--s3_streams_path': s3_streams_path,
        '--target_dynamodb_table': target_dynamodb_table
    },
    aws_conn_id='aws_default',
    region_name='eu-west-1',
    wait_for_completion=True,
    verbose=True,
    execution_timeout=timedelta(hours=2),
    dag=dag
)

processed_bucket = 'music-stream-bkt'
processed_prefix = 'processed/*'
archive_bucket = 'music-stream-bkt'
archive_prefix = 'archive/'
current_date = datetime.now().strftime('%Y%m%d')

archive_processed = PythonOperator(
    task_id='archive_processed_files',
    python_callable=archive_processed_files,
    provide_context=True,
    dag=dag
)

def log_archive_success(context):
    task = context['task']
    logging.info(f"Successfully archived processed files to {archive_bucket}/{archive_prefix}{current_date}/ by task {task.task_id}")

archive_processed.on_success_callback = log_archive_success

cleanup_processed = S3DeleteObjectsOperator(
    task_id='cleanup_processed_files',
    bucket=processed_bucket,
    prefix=processed_prefix,
    aws_conn_id='aws_default',
    dag=dag
)
 
# DAG Task Dependencies
[check_users, check_songs, check_streams] >> validate_data_task >> run_job >> \
archive_processed >> cleanup_processed

