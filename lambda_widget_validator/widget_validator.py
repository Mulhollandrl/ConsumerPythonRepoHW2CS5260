import json
import boto3
from botocore.exceptions import BotoCoreError

client = boto3.client('lambda', region_name='us-east-1a')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    body = json.loads(event['body'])

    if not validate_request(body):
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid request')
        }

    try:
        response = sqs.send_message(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/735066251823/cs5260-requests',
            MessageBody=json.dumps(body)
        )
    except BotoCoreError as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error placing request in queue')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Request placed in queue successfully')
    }

def validate_request(body):
    required_fields = ["type", "requestId", "widgetId", "owner"]

    # Check if all required fields are in the properties of the body
    if not all(field in body['properties'] for field in required_fields):
        return False

    return True