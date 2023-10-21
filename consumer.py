import boto3
import time
import argparse
import json
import logging

logging.basicConfig(filename='consumer.log', level=logging.INFO)

# This is work being done to parse the arguments for the storage strategy and resources to use as specified in Step 1
parser = argparse.ArgumentParser(description='consumer for processing requests for widgets')
parser.add_argument('--storage_strategy', type=str, help='Storage strategy to use.')
args = parser.parse_args()

access_key = input("Please provide your access key: ")
secret_key = input("Please provide your secret key: ")
bucket2_name = input("Please provide your bucket2 name: ")
bucket3_name = input("Please provide your bucket3 name: ")
table_name = input("Please provide your DynamoDB table name: ")

# Create an AWS session using boto3
session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name='us-east-1a'
)

# Create the objects we need for s3 and dynamodb
s3 = session.resource('s3')
dynamodb = session.resource('dynamodb')

# Get all of the buckets we need and the dynamodb table
bucket2 = s3.Bucket(bucket2_name)
bucket3 = s3.Bucket(bucket3_name)
table = dynamodb.Table(table_name)

# Believe it or not, this processes the requests...
def process_request(request):
    # Right now, we only want to process create requests. We may change this later in other homework assignments
    if request['type']['pattern'] == 'create':
        # If the storage method is s3, then we need to make sure we parse it the correct way and then feed it in
        if args.storage_strategy == 's3':
            object_key = f"widgets/{request['owner']['pattern'].replace(" ", "-").lower()}/{request['widgetId']}"
            bucket3.put_object(Key=object_key, Body=json.dumps(request))
            # Log
            logging.info(f'Widget created with id {request["widgetId"]}')
        # Meanwhile, DynamoDB will just accept anything haha
        elif args.storage_strategy == 'dynamodb':
            table.put_item(Item=request)
            # Log
            logging.info(f'Widget created with id {request["widgetId"]}')

# This is the bulk of the program, and the loop that you specified in the instructions
def consumer_program():
    while True:
        # We want a list of all of the objects in the requests bucket
        objects = list(bucket2.objects.all())

        # If there are actually objects in there, then we are good to continue
        if objects:
            # As per the instructions, we are sorting by the object keys, and then we are getting the key of the first object post-sort
            objects.sort(key=lambda obj: obj.key)
            object_key = objects[0].key

            # Get the object properties and then parse them with JSON per the requirements
            body = bucket2.Object(object_key).get()['properties'].read().decode('utf-8')
            request = json.loads(body)

            # Delete the request
            bucket2.Object(object_key).delete()

            # And then process it
            process_request(request)
        # If there are not, then we want to wait
        else:
            time.sleep(0.1)

if __name__ == "__main__":
    consumer_program()
