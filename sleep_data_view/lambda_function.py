import json
import boto3
import logging
from typing import Dict, Any
from botocore.exceptions import ClientError
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
sleep_records_table = dynamodb.Table('sleep_records')

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def fetch_all_sleep_records() -> list:
    try:
        response = sleep_records_table.scan()
        return response.get('Items', [])
    except ClientError as e:
        logger.error(f"Error fetching sleep records: {str(e)}")
        raise

def epoch_to_java_instant(epoch_time: int) -> str:
    return datetime.utcfromtimestamp(epoch_time).isoformat() + 'Z'

def generate_html_table(records: list) -> str:
    html = """
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Sleep Records</title>
    </head>
    <body>
        <h1>Sleep Records</h1>
        <table border="1">
            <tr>
                <th>Session UUID</th>
                <th>Start Time</th>
                <th>End Time</th>
                <th>Stage</th>
            </tr>
    """
    for record in records:
        start_time = epoch_to_java_instant(int(record['start_time']))
        end_time = epoch_to_java_instant(int(record['end_time']))
        html += f"""
            <tr>
                <td>{record['session_uuid']}</td>
                <td>{start_time}</td>
                <td>{end_time}</td>
                <td>{record['stage']}</td>
            </tr>
        """
    html += """
        </table>
    </body>
    </html>
    """
    return html

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        # Log full event for debugging
        logger.info(f"Raw event: {json.dumps(event)}")
        
        # Fetch all sleep records
        sleep_records = fetch_all_sleep_records()
        
        if not sleep_records:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'text/html; charset=UTF-8'
                },
                'body': '<html><body><h1>No sleep records found</h1></body></html>'
            }
        
        # Extract relevant data
        records = [
            {
                'session_uuid': record['session_uuid'],
                'start_time': record['start_time'],
                'end_time': record['end_time'],
                'stage': record['stage']
            }
            for record in sleep_records
        ]
        
        # Generate HTML table
        html_content = generate_html_table(records)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/html; charset=UTF-8'
            },
            'body': html_content
        }

    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'text/html; charset=UTF-8'
            },
            'body': f'<html><body><h1>DynamoDB error: {str(e)}</h1></body></html>'
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'text/html; charset=UTF-8'
            },
            'body': f'<html><body><h1>Unexpected error: {str(e)}</h1></body></html>'
        }