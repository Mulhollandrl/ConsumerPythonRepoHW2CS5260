import json
import boto3
from botocore.exceptions import BotoCoreError

client = boto3.client('lambda', region_name='us-east-1a')

def lambda_handler(event, context):
    body = json.loads(event['body'])

    if not validate_request(body):
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid request')
        }

    try:
        response = client.invoke(
            FunctionName='my_lambda_function',
            InvocationType='Event',
            Payload=json.dumps(body)
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
    # Add your validation logic here
    return True
