import json
import boto3
import logging
from typing import Dict, Any
from botocore.exceptions import ClientError
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
analysis_table = dynamodb.Table('sleep_analysis')

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'statusCode': status_code,
        'body': json.dumps(body, default=decimal_default)
    }

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def fetch_analysis_data(session_uuid: str) -> Dict[str, Any]:
    try:
        response = analysis_table.get_item(
            Key={'session_uuid': session_uuid}
        )
        return response.get('Item', {})
    except ClientError as e:
        logger.error(f"Error fetching analysis data: {str(e)}")
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
        
        # Fetch analysis data
        analysis_data = fetch_analysis_data(session_uuid)
        
        if not analysis_data:
            return create_response(404, {
                'error': 'Analysis data not found',
                'success': False
            })
        
        # Return the analysis
        analysis = analysis_data.get('analysis')
        
        return {
            'analysis': analysis
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