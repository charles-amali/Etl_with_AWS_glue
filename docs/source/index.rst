Music Streaming ETL Pipeline
===========================

A data pipeline that processes music streaming data using Apache Airflow and AWS services.

Components
----------

1. Data Validation
   - Validates incoming CSV files for users, songs, and streams
   - Checks for required columns and data integrity
   - Moves valid files to processed folder

2. Data Processing
   - AWS Glue job processes the data
   - Generates KPIs and analytics
   - Stores results in DynamoDB

3. Data Archiving
   - Archives processed files with timestamps
   - Maintains data history
   - Cleans up processed files after archival

DAG Structure
------------

.. code-block:: text

   [S3 Sensors] >> [Data Validation] >> [Glue Processing] >> [Archiving] >> [Cleanup]

Configuration
------------

AWS Configuration:
- Bucket: music-stream-bkt
- Region: eu-west-1
- Required IAM permissions for S3 and Glue

Required Data Schema:

Users:
- user_id
- user_name
- user_age
- user_country
- created_at

Songs:
- track_id
- track_name
- artists
- album_name
- track_genre
- duration_ms
- popularity
- explicit

Streams:
- user_id
- track_id
- listen_time

Error Handling
-------------

- Failed validations move files to error/ folder
- Detailed logging for debugging
- Retries configured for AWS service calls

Maintenance
----------

- Monitor Airflow logs for task failures
- Check error/ folder for invalid files
- Review archived data periodically
- Update AWS credentials as needed