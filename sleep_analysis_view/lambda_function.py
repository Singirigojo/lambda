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

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def fetch_all_analysis_data() -> list:
    try:
        response = analysis_table.scan()
        return response.get('Items', [])
    except ClientError as e:
        logger.error(f"Error fetching analysis data: {str(e)}")
        raise

def generate_html_table(records: list) -> str:
    html = """
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Analysis Data</title>
    </head>
    <body>
        <h1>Analysis Data</h1>
        <table border="1">
            <tr>
                <th>Session UUID</th>
                <th>Score</th>
                <th>Analysis</th>
            </tr>
    """
    for record in records:
        html += f"""
            <tr>
                <td>{record['session_uuid']}</td>
                <td>{record['score']}</td>
                <td>{record['analysis']}</td>
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
        
        # Fetch all analysis data
        analysis_data = fetch_all_analysis_data()
        
        if not analysis_data:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'text/html'
                },
                'body': '<html><body><h1>No analysis data found</h1></body></html>'
            }
        
        # Extract relevant data
        records = [
            {
                'session_uuid': record['session_uuid'],
                'score': record['score'],
                'analysis': record['analysis']
            }
            for record in analysis_data
        ]
        
        # Generate HTML table
        html_content = generate_html_table(records)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/html'
            },
            'body': html_content
        }

    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'text/html'
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
                'Content-Type': 'text/html'
            },
            'body': f'<html><body><h1>Unexpected error: {str(e)}</h1></body></html>'
        }