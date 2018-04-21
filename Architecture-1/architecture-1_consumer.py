import boto3
import json

sqs = boto3.client('sqs', 'us-east-1')
dynamodb = boto3.client('dynamodb', 'us-east-1')
sns = boto3.client('sns', 'us-east-1')

file = open('architecture-1-outfile.json', 'w')

queue_count = 0
dynamo_count = 0

while True:
    url = sqs.get_queue_url(QueueName='arch1-sqs-queue')['QueueUrl']

    # RECEIVE Message from SQS QUEUE
    try:
        response = sqs.receive_message(QueueUrl=url,
                                       MaxNumberOfMessages=1)
    except Exception as e:
        print(e)
        quit()
    else:
        if 'Messages' not in response:
            break
        else:
            queue_count = queue_count + 1
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            # print("No. of records read from queue - {}", queue_count, end="\r")

            # Write to FILE
            data = response['Messages'][0]['Body']
            file.write(data)

            # Format and write to DYNAMODB table
            json_data = json.loads(data)
            address = json_data['address']
            borough = json_data['borough']
            cuisine = json_data['cuisine']
            name = json_data['name']
            restaurant_id = json_data['restaurant_id']

            try:
                response = dynamodb.put_item(TableName='arch1-dynamodb-table',
                                             Item={
                                                 # 'address': { 'M': {'address' : address }},
                                                 'address': { 'M': address },
                                                 'borough': { 'S': borough },
                                                 'cuisine': { 'S': cuisine },
                                                 'name': { 'S': name },
                                                 'restaurant_id': { 'N': restaurant_id}
                                             },
                                             ReturnConsumedCapacity='TOTAL')
            except Exception as e:
                print("ERROR!!! Could not write data to table")
                print(e)
                quit()
            else:
                dynamo_count = dynamo_count + 1
                print("No. of records put in DynamoDB Table - {}".format(dynamo_count), end="\r")
                # print(response['ConsumedCapacity'])
                sqs.delete_message(QueueUrl=url, ReceiptHandle=receipt_handle)

print("Total no. of records read from Queue: ", queue_count)
print("Total no. of records PUT to DynamoDB table: ", dynamo_count)

try:
    response = sns.publish(TopicArn='arn:aws:sns:us-east-1:968088411430:arch1-sns-topic',
                           Message='Uploading DynamoDB table complete',
                           Subject='architecture-1 output message')
except Exception as e:
    print("Error publishing message to SNS Topic")
    print(e)


