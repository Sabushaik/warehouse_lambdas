# AWS Lambda Function - Blob Trigger

## Overview
This Lambda function processes S3 bucket events for video files uploaded to the `pipeline` folder. It integrates with a PostgreSQL database and a FastAPI service for video processing.

## Conversion from Azure Function
This code was converted from an Azure Function to an AWS Lambda function. The conversion maintains all business logic while adapting to AWS S3 events.

## Lambda Handler
- **Function**: `lambda_handler(event, context)`
- **Trigger**: S3 bucket object creation events
- **Event Source**: `aws:s3`
- **Event Type**: `ObjectCreated:*`

## Required Dependencies
The following Python packages must be included in your Lambda deployment package or Lambda layer:

### Standard Library (included with Python runtime)
- `json`
- `logging`
- `os`
- `re`
- `urllib.parse`
- `typing`
- `datetime`

### Third-Party Packages (must be included)
- `requests` - HTTP client for FastAPI calls
- `psycopg2` or `psycopg2-binary` - PostgreSQL database adapter

## Environment Variables
- `FASTAPI_SERVICE_URL` (optional): URL of the FastAPI service (defaults to `https://warehousebackend.p9sphere.com`)

## Database Configuration
The function connects to a PostgreSQL database with hardcoded credentials (consider moving to AWS Secrets Manager):
- **Host**: 145.190.8.4
- **Port**: 5432
- **Database**: ap_warehouse
- **User**: spectra
- **Password**: (hardcoded in the file)

## S3 Event Structure
The Lambda function expects S3 events in the following structure:
```json
{
  "Records": [
    {
      "eventSource": "aws:s3",
      "eventName": "ObjectCreated:Put",
      "eventTime": "2024-01-01T12:00:00.000Z",
      "awsRegion": "us-east-1",
      "s3": {
        "bucket": {
          "name": "your-bucket-name"
        },
        "object": {
          "key": "pipeline/2024-01-01/WH001/CAM001/CHUNK_abc123/CHUNK_abc123.mp4"
        }
      }
    }
  ]
}
```

## File Path Pattern
The function processes files matching this pattern:
```
pipeline/YYYY-MM-DD/WH###/CAM###/CHUNK_uuid/CHUNK_uuid.mp4
```

Where:
- `YYYY-MM-DD`: Date of the video
- `WH###`: Warehouse ID (e.g., WH001)
- `CAM###`: Camera ID (e.g., CAM001)
- `uuid`: Unique chunk identifier

## Processing Flow
1. **Parse S3 Event**: Extract bucket name, object key, and event metadata
2. **Validate Event**: Check event age (max 3 minutes) and file type (.mp4)
3. **Parse Blob URL**: Extract warehouse ID, camera ID, chunk ID, and date from URL
4. **Check Duplicate**: Verify chunk doesn't already exist in database
5. **Insert Metadata**: Store chunk information in `wh_chunks` table
6. **Fetch Camera Config**: Retrieve camera settings from `cameras` table
7. **Construct Payload**: Build request payload for FastAPI service
8. **Call FastAPI**: Send video for processing
9. **Log Results**: Record processing status and results

## Key Features
- **Duplicate Prevention**: Checks database before processing to avoid reprocessing
- **Event Age Validation**: Only processes events less than 3 minutes old
- **Structured Logging**: JSON-formatted logs for monitoring and debugging
- **Error Handling**: Comprehensive exception handling with detailed logging
- **URL Decoding**: Handles URL-encoded S3 object keys

## Lambda Configuration Recommendations
- **Timeout**: 900 seconds (15 minutes) - allows for 800-second FastAPI timeout plus overhead
- **Memory**: 512 MB or higher depending on workload
- **VPC**: Must be configured if database is in a private subnet
- **IAM Permissions**: 
  - `s3:GetObject` for reading S3 objects
  - VPC network interface permissions if in VPC
  - CloudWatch Logs permissions for logging

## Deployment Package Structure
```
lambda_deployment_package/
├── Blob_trigger.py          # Main Lambda function
├── psycopg2/                # PostgreSQL adapter
├── requests/                # HTTP client library
└── (other dependencies)
```

## Testing
You can test the Lambda function locally using mock S3 events or by deploying to AWS and triggering with actual S3 uploads.

### Sample Test Event
```python
test_event = {
    "Records": [{
        "eventSource": "aws:s3",
        "eventName": "ObjectCreated:Put",
        "eventTime": "2024-01-01T12:00:00.000Z",
        "awsRegion": "us-east-1",
        "s3": {
            "bucket": {"name": "test-bucket"},
            "object": {"key": "pipeline/2024-01-01/WH001/CAM001/CHUNK_test123/CHUNK_test123.mp4"}
        }
    }]
}
```

## Monitoring
The function logs structured JSON messages for:
- Event processing start/end
- Database operations
- FastAPI service calls
- Errors and exceptions

Monitor these logs in CloudWatch Logs for operational insights.
