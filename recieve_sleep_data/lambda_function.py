import json
import boto3
import logging
from typing import Dict, Any
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('sleep_records')
lambda_client = boto3.client('lambda')

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'statusCode': status_code,
        'body': json.dumps(body)
    }

def invoke_analysis_lambda(session_uuid: str) -> Dict[str, Any]:
    try:
        response = lambda_client.invoke(
            FunctionName='sleep_data_analysis',
            InvocationType='Event',  # Use 'RequestResponse' for synchronous invocation
            Payload=json.dumps({
                'session_uuid': session_uuid
            })
        )
        logger.info(f"Invoked analysis Lambda function with response: {response}")
        return response
    except ClientError as e:
        logger.error(f"Error invoking analysis Lambda function: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        # Log full event for debugging
        logger.info(f"Raw event: {json.dumps(event)}")
        
        # Handle CORS preflight
        if event.get('httpMethod') == 'OPTIONS':
            return create_response(200, {'message': 'OK'})

        # Get query parameters from event
        query_params = event.get('queryStringParameters')
        logger.info(f"Query parameters: {query_params}")
        
        # Get client_uuid - handle both cases
        client_uuid = None
        if query_params:
            client_uuid = query_params.get('client_uuid')
        
        # Also check multiValueQueryStringParameters as backup
        if not client_uuid and event.get('multiValueQueryStringParameters'):
            client_uuid = event.get('multiValueQueryStringParameters').get('client_uuid', [None])[0]
            
        if not client_uuid:
            return create_response(400, {
                'error': 'client_uuid is required as query parameter',
                'success': False
            })
        
        # Parse request body - handle different API Gateway payload formats
        body = None
        if isinstance(event.get('body'), str):
            try:
                body = json.loads(event.get('body'))
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error: {str(e)}")
                return create_response(400, {
                    'error': 'Invalid JSON format',
                    'success': False
                })
        elif isinstance(event.get('body'), dict):
            body = event.get('body')
        else:
            logger.error(f"Unexpected body format: {type(event.get('body'))}")
            return create_response(400, {
                'error': 'Invalid request body format',
                'success': False
            })

        logger.info(f"Parsed body: {json.dumps(body)}")

        # Validate body
        if not body or not isinstance(body, dict):
            return create_response(400, {
                'error': 'Invalid request body',
                'success': False
            })

        # Validate required fields
        if 'sleep_data' not in body or not isinstance(body['sleep_data'], list):
            return create_response(400, {
                'error': 'Invalid request body: missing sleep_data list',
                'success': False
            })

        # Process each sleep record
        for record in body['sleep_data']:
            required_fields = ['sessionId', 'startTime', 'endTime', 'stage']
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                return create_response(400, {
                    'error': f'Missing required fields in one of the records: {", ".join(missing_fields)}',
                    'success': False
                })

            # Create DynamoDB item
            item = {
                'client_uuid': str(client_uuid),  # Ensure client_uuid is a string
                'session_uuid': str(record['sessionId']),  # Ensure session_uuid is a string
                'start_time': int(record['startTime']),
                'end_time': int(record['endTime']),
                'stage': int(record['stage'])
            }

            # Store in DynamoDB
            table.put_item(Item=item)
            
            # Check if 'end' field is true
            if record.get('end', False):
                # Invoke the analysis Lambda function with sessionId
                invoke_analysis_lambda(record['sessionId'])
        
        return create_response(200, {
            'message': 'Sleep data stored successfully',
            'success': True
        })

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