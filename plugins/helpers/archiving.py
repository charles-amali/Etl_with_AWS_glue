from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import logging

processed_bucket = 'music-stream-bkt'
processed_prefix = 'processed/'  # Changed from 'processed/*'
archive_bucket = 'music-stream-bkt'
archive_prefix = 'archive/'
current_date = datetime.now().strftime('%Y%m%d')

def archive_processed_files(**context):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    keys = s3_hook.list_keys(bucket_name=processed_bucket, prefix=processed_prefix)
    
    for key in keys:
        if key.endswith('.csv'):  # Only process CSV files
            # Preserve the folder structure after 'processed/'
            relative_path = key.replace(processed_prefix, '', 1)
            dest_key = f'{archive_prefix}{current_date}/{relative_path}'
            
            s3_hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=dest_key,
                source_bucket_name=processed_bucket,
                dest_bucket_name=archive_bucket
            )
            logging.info(f"Archived {key} to {dest_key}")
