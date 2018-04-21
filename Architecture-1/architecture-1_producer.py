import boto3
import time


sqs = boto3.client('sqs', 'us-east-1')
dynamodb = boto3.client('dynamodb', 'us-east-1')
sns = boto3.client('sns', 'us-east-1')

# Send Message to SQS Queue
def send_msg_to_queue():
    try:
        url = sqs.get_queue_url(QueueName='arch1-sqs-queue')['QueueUrl']
    except Exception as e:
        print("Error getting queueurl")
        print(e)

    file_count = 0
    queue_count = 0
    with open('restaurants.json', 'r') as file:
        for record in file:
            file_count = file_count + 1
            try:
                response = sqs.send_message(QueueUrl=url,
                                            MessageBody=record)
            except Exception as e:
                print(e)
                quit()
            else:
                queue_count = queue_count + 1
                print("No. of records put in Queue - {}".format(queue_count), end="\r")

    print("Total records read from file - ", file_count)
    print("Total records sent to queue  - ", queue_count)


# Create SQS queue
print("CREATING SQS QUEUE...\n")
try:
    response = sqs.create_queue(QueueName='arch1-sqs-queue',
                                Attributes={
                                    'VisibilityTimeout' : '10',
                                    'MessageRetentionPeriod' : '180'
                                })
except sqs.exceptions.QueueNameExists:
    print("Queue Already exists... Moving on...")
except Exception as e:
    print("Error while creating queue")
    print(e)
    quit()


# Creare DynamoDB Table
print("CREATING DYNAMODB TABLE...\n")

try:
    response = dynamodb.create_table(TableName='arch1-dynamodb-table',
                                     AttributeDefinitions=[
                                         { 'AttributeName' : 'restaurant_id' , 'AttributeType': 'N' }
                                     ],
                                     KeySchema=[{ 'AttributeName': 'restaurant_id', 'KeyType': 'HASH' }],
                                     ProvisionedThroughput = {'ReadCapacityUnits': 5,
                                                              'WriteCapacityUnits': 5})
except dynamodb.exceptions.ResourceInUseException:
    print("Table already exists... moving on...")
except Exception as e:
    print("Error creating DynamoDB table")
    print(e)
    quit()
else:
    message = 'creating table'
    while True:
        table_status = dynamodb.describe_table(TableName='arch1-dynamodb-table')['Table']['TableStatus']
        if table_status != 'ACTIVE':
            message = message + "."
            print(message, end="\r")
            time.sleep(1)
            # print(response['TableDescription']['TableStatus'])
        else:
            print("\nTable Created.")
            break


send_msg_to_queue()