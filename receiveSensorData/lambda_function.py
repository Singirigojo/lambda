import json
import boto3
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('sensor_data')

def lambda_handler(event, context):
    # 로그 출력
    print("Received event: " + json.dumps(event))
    
    # 데이터 추출
    client_uuid = event['client']
    data = event['data']
    
    # 현재 시간 (ISO 8601 형식)
    received_time = int(datetime.utcnow().timestamp())
    
    # 아이템 생성
    item={
            'client_uuid': client_uuid,
            'time': received_time
        }
    
    # 데이터의 모든 키-값 쌍을 처리하여 아이템에 추가
    for key, value in data.items():
        if isinstance(value, (int, float, Decimal)):
            item[key] = Decimal(str(value))
        else:
            item[key] = value
    
    # DynamoDB에 데이터 저장
    table.put_item(Item=item)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data stored successfully!')
    }