import boto3
import pandas as pd
import logging
import os
from typing import Dict
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Get AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'eu-west-1')

# Initialize S3 client with credentials from environment variables
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# Required columns for each data type
REQUIRED_COLUMNS = {
    'users': [
        "user_id",
        "user_name",
        "user_age",
        "user_country",
        "created_at",
    ],
    'songs': [
        "track_id",
        "track_name",
        "artists",
        "album_name",
        "track_genre",
        "duration_ms",
        "popularity",
        "explicit"
    ],
    'streams': [
        "user_id",
        "track_id",
        "listen_time",
    ]
}

def save_to_processed_folder(bucket: str, source_key: str, data_type: str) -> str:
    """
    Save validated file to processed folder with timestamp
    
    Args:
        bucket: S3 bucket name
        source_key: Original file key
        data_type: Type of data (users, songs, or streams)
    
    Returns:
        str: New file key in processed folder
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.basename(source_key)
    processed_key = f"processed/{data_type}/{timestamp}_{filename}"
    
    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=processed_key
        )
        logger.info(f"Saved validated file to: s3://{bucket}/{processed_key}")
        return processed_key
    except Exception as e:
        logger.error(f"Error saving to processed folder: {str(e)}")
        return None

def validate_data(bucket: str, keys: Dict[str, str]) -> Dict[str, any]:
    """
    Validates that each dataset contains its required columns and saves valid files to processed folder
    
    Args:
        bucket: S3 bucket name
        keys: Dictionary mapping data type to S3 key (e.g., {'users': 'data/users.csv'})
    
    Returns:
        Dict with validation results and processed file locations
    """
    if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY]):
        logger.error("AWS credentials not found in environment variables")
        return {'valid': False, 'processed_files': {}}

    all_valid = True
    processed_files = {}

    for data_type, key in keys.items():
        if data_type not in REQUIRED_COLUMNS:
            logger.error(f"Unknown data type: {data_type}")
            return {'valid': False, 'processed_files': {}}

        try:
            # Only read the header
            response = s3_client.get_object(Bucket=bucket, Key=key)
            df_columns = pd.read_csv(response['Body'], nrows=0).columns.tolist()
            
            # Check for required columns
            missing_columns = set(REQUIRED_COLUMNS[data_type]) - set(df_columns)
            
            if missing_columns:
                logger.error(f"Missing columns in {data_type} data: {missing_columns}")
                error_path = f"error/{data_type}/{os.path.basename(key)}"
                s3_client.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': key},
                    Key=error_path
                )
                all_valid = False
                continue

            # If validation passes, save to processed folder
            processed_key = save_to_processed_folder(bucket, key, data_type)
            if processed_key:
                processed_files[data_type] = processed_key
            
            logger.info(f"Successfully validated columns for {data_type} data")

        except Exception as e:
            logger.error(f"Error processing {data_type} data: {str(e)}")
            all_valid = False

    return {
        'valid': all_valid,
        'processed_files': processed_files
    }
