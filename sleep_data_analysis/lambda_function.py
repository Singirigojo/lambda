import json
import logging
import boto3
import os
import re
from decimal import Decimal
from openai import OpenAI
from typing import Dict, Any
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
analysis_table = dynamodb.Table('sleep_analysis')
sleep_records_table = dynamodb.Table('sleep_records')

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'statusCode': status_code,
        'body': json.dumps(body)
    }

def fetch_session_data(session_uuid: str) -> list:
    try:
        response = sleep_records_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('session_uuid').eq(session_uuid)
        )
        return response.get('Items', [])
    except ClientError as e:
        logger.error(f"Error fetching session data: {str(e)}")
        raise

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def fetch_sensor_data(client_uuid: str, start_time: int, end_time: int) -> list:
    sensor_table = dynamodb.Table('sensor_data')
    try:
        if not all([client_uuid, start_time, end_time]):
            logger.warning("Missing required parameters for sensor data fetch")
            return None
            
        response = sensor_table.query(
            KeyConditionExpression='client_uuid = :client_uuid AND #time BETWEEN :start_time AND :end_time',
            ExpressionAttributeNames={
                '#time': 'time'
            },
            ExpressionAttributeValues={
                ':client_uuid': client_uuid,
                ':start_time': start_time,
                ':end_time': end_time
            }
        )
        items = response.get('Items', [])

        return items
    except ClientError as e:
        logger.error(f"Error fetching sensor data: {str(e)}")
        return None


def GPT(*data):
    try:
        openai_api_key=os.environ['OPENAI_API_KEY']
        client = OpenAI(api_key=openai_api_key)

        thread = client.beta.threads.create()

        message = client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=f"데이터: {data}"
        )

        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id="asst_OiGYNlV63y7lopRWaauXezf6"
        )

        # 실행 완료까지 대기
        while run.status != "completed":
            run = client.beta.threads.runs.retrieve(
                thread_id=thread.id,
                run_id=run.id
            )

        # 메시지 출력
        messages = client.beta.threads.messages.list(thread_id=thread.id)
        messages = messages.data[0].content[0].text.value
        logger.info(f"Assistant Message before parsing: {messages}")

        # Update regex pattern to find JSON block within ```json ... ``` markers
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', messages, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
            logger.info(f"Extracted JSON: {json_str}")
            result = json.loads(json_str)
            return result
        else:
            logger.error("JSON 부분을 찾을 수 없습니다.")
            return None

    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 에러: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"GPT 처리 중 에러 발생: {str(e)}")
        return None


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    session_uuid = str(event.get('session_uuid'))
    
    # Fetch session data from DynamoDB
    session_data = fetch_session_data(session_uuid)
    
    if not session_data:
        return create_response(404, {'error': 'Session data not found'})

    try:
        client_uuid = session_data[0].get('client_uuid')
        start_times = [item.get('start_time') for item in session_data if item.get('start_time')]
        end_times = [item.get('end_time') for item in session_data if item.get('end_time')]
        
        if not (start_times and end_times):
            logger.error("Missing time values in session data")
            return create_response(400, {'error': 'Invalid session data'})
            
        start_time = min(start_times)
        end_time = max(end_times)

        logger.info(f"Extracted values - client_uuid: {client_uuid}, start_time: {start_time}, end_time: {end_time}")
        
        # Fetch sensor data from DynamoDB
        sensor_data = fetch_sensor_data(client_uuid, start_time, end_time)
    
    except Exception as e:
        logger.error(f"Error extracting values from session data: {str(e)}")
        return create_response(500, {'error': 'Error processing session data'})
        
    # GPT result
    gpt_result = GPT(session_data, sensor_data)
    if gpt_result is None:
        return create_response(500, {'error': 'Error processing GPT request'})

    # Generate random score and analysis
    score = gpt_result.get('score')
    analysis = gpt_result.get('analysis')
    
    # Store analysis result in DynamoDB
    analysis_item = {
        'session_uuid': session_uuid,
        'score': score,
        'analysis': analysis
    }
    analysis_table.put_item(Item=analysis_item)
    logger.info(f"Stored analysis item: {json.dumps(analysis_item)}")
    
    return {
        'message': 'Analysis completed successfully',
        'score': score,
        'analysis': analysis
    }