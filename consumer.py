import boto3
import time
import argparse
import json
import logging

logging.basicConfig(filename='consumer.log', level=logging.INFO)

# This is work being done to parse the arguments for the storage strategy and resources to use as specified in Step 1
parser = argparse.ArgumentParser(description='consumer for processing requests for widgets')
parser.add_argument('--storage_strategy', type=str, help='Storage strategy to use.')
parser.add_argument('--queue_name', type=str, help='Name of the SQS queue.')
args = parser.parse_args()

access_key = input("Please provide your access key: ")
secret_key = input("Please provide your secret key: ")
session_token = input("Please provide your session token: ")
bucket2_name = input("Please provide your bucket2 name: ")
bucket3_name = input("Please provide your bucket3 name: ")
table_name = input("Please provide your DynamoDB table name: ")

# Create an AWS session using boto3
session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    aws_session_token=session_token,
    region_name='us-east-1'
)

# Create the objects we need for s3, dynamodb and sqs
s3 = session.resource('s3')
dynamodb = session.resource('dynamodb')
sqs = session.client('sqs')

# Get all of the buckets we need, the dynamodb table and the sqs queue
bucket2 = s3.Bucket(bucket2_name)
bucket3 = s3.Bucket(bucket3_name)
table = dynamodb.Table(table_name)
queue_url = sqs.get_queue_url(QueueName=args.queue_name)['QueueUrl']
print(f"\n\n\n\n\n\n{queue_url}\n\n\n\n\n\n")

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
        else:
            time.sleep(0.1)

if __name__ == "__main__":
    consumer_program()