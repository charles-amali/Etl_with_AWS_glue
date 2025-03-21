import pytest
import pandas as pd
import boto3
from unittest.mock import patch, MagicMock
from io import StringIO
from plugins.helpers.validation import validate_data
from plugins.helpers.transform import merge_and_process_data

@pytest.fixture
def sample_data():
    """Fixture to create sample DataFrames for testing"""
    users_data = {
        'user_id': ['1', '2', '3'],
        'user_name': ['John', 'Jane', 'Bob'],
        'user_age': [25, 30, 35],
        'user_country': ['US', 'UK', 'CA']
    }
    
    songs_data = {
        'track_id': ['t1', 't2', 't3'],
        'track_name': ['Song1', 'Song2', 'Song3'],
        'artists': ['Artist1', 'Artist2', 'Artist3'],
        'album_name': ['Album1', 'Album2', 'Album3'],
        'track_genre': ['Rock', 'Pop', 'Jazz'],
        'duration_ms': [180000, 210000, 240000],
        'popularity': [80, 75, 90],
        'explicit': [False, True, False]
    }
    
    streams_data = {
        'user_id': ['1', '2', '1'],
        'track_id': ['t1', 't2', 't3'],
        'listen_time': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'created_at': ['2023-01-01', '2023-01-02', '2023-01-03']
    }
    
    return {
        'users': pd.DataFrame(users_data),
        'songs': pd.DataFrame(songs_data),
        'streams': pd.DataFrame(streams_data)
    }

@pytest.fixture
def mock_s3_client():
    """Fixture to mock S3 client"""
    with patch('boto3.client') as mock_client:
        yield mock_client

class TestDataValidation:
    def test_validate_data_success(self, sample_data, mock_s3_client):
        """Test successful validation of data"""
        # Mock S3 get_object response
        mock_s3_response = {
            'Body': StringIO(sample_data['users'].to_csv(index=False))
        }
        mock_s3_client.return_value.get_object.return_value = mock_s3_response

        result = validate_data(
            bucket='test-bucket',
            keys={
                'users': 'data/users.csv',
                'songs': 'data/songs.csv',
                'streams': 'data/streams.csv'
            }
        )
        
        assert result is True

    def test_validate_data_missing_columns(self, mock_s3_client):
        """Test validation with missing columns"""
        # Create DataFrame with missing required columns
        invalid_data = pd.DataFrame({
            'wrong_column': ['1', '2', '3']
        })
        
        mock_s3_response = {
            'Body': StringIO(invalid_data.to_csv(index=False))
        }
        mock_s3_client.return_value.get_object.return_value = mock_s3_response

        result = validate_data(
            bucket='test-bucket',
            keys={
                'users': 'data/users.csv'
            }
        )
        
        assert result is False

class TestDataMerging:
    @patch('pandas.read_csv')
    def test_merge_and_process_data_success(self, mock_read_csv, sample_data, mock_s3_client):
        """Test successful merging of data"""
        # Mock the read_csv calls to return our sample data
        mock_read_csv.side_effect = [
            sample_data['streams'],
            sample_data['users'],
            sample_data['songs']
        ]

        result = merge_and_process_data(
            source_bucket='source-bucket',
            dest_bucket='dest-bucket',
            keys={
                'users': 'data/users.csv',
                'songs': 'data/songs.csv',
                'streams': 'data/streams.csv'
            },
            dest_prefix='processed_data'
        )
        
        assert result is True
        # Verify S3 put_object was called
        mock_s3_client.return_value.put_object.assert_called_once()

    @patch('pandas.read_csv')
    def test_merge_and_process_data_failure(self, mock_read_csv, mock_s3_client):
        """Test merging with invalid data"""
        # Mock read_csv to raise an exception
        mock_read_csv.side_effect = Exception("Failed to read CSV")

        result = merge_and_process_data(
            source_bucket='source-bucket',
            dest_bucket='dest-bucket',
            keys={
                'users': 'data/users.csv',
                'songs': 'data/songs.csv',
                'streams': 'data/streams.csv'
            },
            dest_prefix='processed_data'
        )
        
        assert result is False
        # Verify S3 put_object was not called
        mock_s3_client.return_value.put_object.assert_not_called()

def test_merged_data_structure(sample_data, mock_s3_client):
    """Test the structure of merged data"""
    with patch('pandas.read_csv') as mock_read_csv:
        mock_read_csv.side_effect = [
            sample_data['streams'],
            sample_data['users'],
            sample_data['songs']
        ]

        merge_and_process_data(
            source_bucket='source-bucket',
            dest_bucket='dest-bucket',
            keys={
                'users': 'data/users.csv',
                'songs': 'data/songs.csv',
                'streams': 'data/streams.csv'
            },
            dest_prefix='processed_data'
        )

        # Get the DataFrame that was sent to S3
        called_args = mock_s3_client.return_value.put_object.call_args
        sent_data = pd.read_csv(StringIO(called_args[1]['Body']))

        # Verify the merged DataFrame has all required columns
        expected_columns = {
            'user_id', 'track_id', 'listen_time', 'created_at',
            'user_name', 'user_age', 'user_country',
            'track_name', 'artists', 'album_name', 'track_genre',
            'duration_ms', 'popularity', 'explicit'
        }
        assert set(sent_data.columns) == expected_columns