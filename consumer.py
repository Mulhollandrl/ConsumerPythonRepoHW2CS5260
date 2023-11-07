import boto3
import time
import argparse
import json
import logging

logging.basicConfig(filename='consumer.log', level=logging.INFO)

# This is work being done to parse the arguments because that is what we need for docker later on
parser = argparse.ArgumentParser(description='consumer for processing requests for widgets')
parser.add_argument('--storage_strategy', type=str, help='Storage strategy to use.')
parser.add_argument('--queue_name', type=str, help='Name of the SQS queue.')
parser.add_argument('--access_key', type=str, help='Your access key.')
parser.add_argument('--secret_key', type=str, help='Your secret key.')
parser.add_argument('--session_token', type=str, help='Your session token.')
parser.add_argument('--bucket2_name', type=str, help='Your bucket2 name.')
parser.add_argument('--bucket3_name', type=str, help='Your bucket3 name.')
parser.add_argument('--table_name', type=str, help='Your DynamoDB table name.')
args = parser.parse_args()

# Create an AWS session using boto3
session = boto3.Session(
    aws_access_key_id=args.access_key,
    aws_secret_access_key=args.secret_key,
    aws_session_token=args.session_token,
    region_name='us-east-1'
)

# Create the objects we need for s3, dynamodb and sqs using that boto3 session
s3 = session.resource('s3')
dynamodb = session.resource('dynamodb')
sqs = session.client('sqs')

# Get all of the buckets we need, the dynamodb table and the sqs queue using the aforedefined objects
bucket2 = s3.Bucket(args.bucket2_name)
bucket3 = s3.Bucket(args.bucket3_name)
table = dynamodb.Table(args.table_name)
queue_url = sqs.get_queue_url(QueueName=args.queue_name)['QueueUrl']

# This processes the requests...
def process_request(request, args):
    # Now, we want to process create, delete and update requests.
    if request['type']['pattern'] in ['create', 'delete', 'update']:
        # If the storage method is s3, then we need to make sure we parse it the correct way and then feed it in
        if args.storage_strategy == 's3':
            object_key = f"widgets/{request['owner']['pattern'].replace(" ", "-").lower()}/{request['widgetId']}"
            bucket3.put_object(Key=object_key, Body=json.dumps(request))
            # Log
            logging.info(f'Widget {request["type"]["pattern"]}d with id {request["widgetId"]}')
        # Meanwhile, DynamoDB will just accept anything haha
        elif args.storage_strategy == 'dynamodb':
            if request['type']['pattern'] == 'create':
                table.put_item(Item=request)
            elif request['type']['pattern'] == 'delete':
                table.delete_item(Key={'widgetId': request['widgetId']})
            elif request['type']['pattern'] == 'update':
                table.update_item(Key={'widgetId': request['widgetId']}, AttributeUpdates=request['updates'])
            # Log
            logging.info(f'Widget {request["type"]["pattern"]}d with id {request["widgetId"]}')

# This is the bulk of the program, and the loop that you specified in the instructions
def consumer_program():
    # Cache for storing messages
    message_cache = []

    while True:
        # If cache is empty, retrieve up to 10 messages from SQS queue
        if not message_cache:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=10
            )

            # If messages are found, add them to the cache
            if 'Messages' in response:
                message_cache.extend(response['Messages'])

        # If there are messages in the cache, process them
        if message_cache:
            message = message_cache.pop(0)
            request = json.loads(message['Body'])

            # Process the request
            process_request(request)

            # Delete the message from the queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        # Sleep if you have nothing else to do. I feel like these are wise words.
        else:
            time.sleep(0.1)

if __name__ == "__main__":
    consumer_program()