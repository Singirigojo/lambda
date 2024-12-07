import json
import boto3
import logging
from typing import Dict, Any
from botocore.exceptions import ClientError
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
sleep_records_table = dynamodb.Table('sleep_records')

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'statusCode': status_code,
        'body': json.dumps(body, default=decimal_default)
    }

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def fetch_sleep_records(session_uuid: str) -> list:
    try:
        response = sleep_records_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('session_uuid').eq(session_uuid)
        )
        return response.get('Items', [])
    except ClientError as e:
        logger.error(f"Error fetching sleep records: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        # Log full event for debugging
        logger.info(f"Raw event: {json.dumps(event)}")
        
        # Get query parameters from event
        query_params = event.get('queryStringParameters')
        logger.info(f"Query parameters: {query_params}")
        
        # Get session_uuid
        session_uuid = None
        if query_params:
            session_uuid = query_params.get('session_uuid')
        
        if not session_uuid:
            return create_response(400, {
                'error': 'session_uuid is required as query parameter',
                'success': False
            })
        
        # Fetch sleep records
        sleep_records = fetch_sleep_records(session_uuid)
        
        if not sleep_records:
            return create_response(404, {
                'error': 'No sleep records found for the given session_uuid',
                'success': False
            })
        
        # Extract relevant data
        records = [
            {
                'start_time': record['start_time'],
                'end_time': record['end_time'],
                'stage': record['stage']
            }
            for record in sleep_records
        ]
        
        return {
            'records': records
        }

    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return create_response(500, {
            'error': f"DynamoDB error: {str(e)}",
            'success': False
        })
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return create_response(500, {
            'error': str(e),
            'success': False
        })