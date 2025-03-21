import boto3
import pandas as pd
import logging
import os
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure AWS client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'eu-west-1')
)

logger = logging.getLogger(__name__)

def merge_and_process_data(source_bucket: str, dest_bucket: str, keys: dict, dest_prefix: str) -> bool:
    """
    Merges streams, songs, and users data and saves to S3
    
    Args:
        source_bucket: Source S3 bucket name
        dest_bucket: Destination S3 bucket name
        keys: Dictionary containing file paths for each data type
        dest_prefix: Prefix for the output file in destination bucket
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Read streams data
        logger.info(f"Reading streams data from s3://{source_bucket}/{keys['streams']}")
        streams_df = pd.read_csv(
            f"s3://{source_bucket}/{keys['streams']}", 
            usecols=['user_id', 'track_id', 'listen_time']
        )
        
        # Read users data
        logger.info(f"Reading users data from s3://{source_bucket}/{keys['users']}")
        users_df = pd.read_csv(
            f"s3://{source_bucket}/{keys['users']}", 
            usecols=['user_id', 'user_name', 'user_age', 'user_country']
        )
        
        # Read songs data
        logger.info(f"Reading songs data from s3://{source_bucket}/{keys['songs']}")
        songs_df = pd.read_csv(
            f"s3://{source_bucket}/{keys['songs']}", 
            usecols=['track_id', 'track_name', 'artists', 'album_name', 
                    'track_genre', 'duration_ms', 'popularity', 'explicit']
        )

        # Merge dataframes
        logger.info("Merging dataframes...")
        merged_df = (streams_df
                    .merge(users_df, on='user_id', how='left')
                    .merge(songs_df, on='track_id', how='left'))

        # Add timestamp column
        merged_df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Generate output path with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"{dest_prefix}/processed_data_{timestamp}.csv"

        logger.info(f"Saving processed data to s3://{dest_bucket}/{output_key}")
        
        # Save to S3
        csv_buffer = StringIO()
        merged_df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=dest_bucket,
            Key=output_key,
            Body=csv_buffer.getvalue()
        )

        logger.info(f"Successfully merged and saved data to s3://{dest_bucket}/{output_key}")
        return True

    except Exception as e:
        logger.error(f"Error processing and merging data: {str(e)}")
        logger.exception("Detailed error traceback:")  # This will log the full stack trace
        return False
