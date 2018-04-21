import boto3
import json

'''

Reads records from a file and puts all of them to a Kinesis Stream one by one

'''

stream_name = 'kinesis-boto3'
kinesis_producer_client = boto3.client('kinesis', 'us-east-1')
# sqs_queue = boto3.client('sqs', 'us-east-1')
# sqs_queue_url = sqs_queue.get_queue_url(QueueName='sqs-queue-for-kinesis')['QueueUrl']


# Create steam

try:
    create_stream = kinesis_producer_client.create_stream(StreamName=stream_name, ShardCount=1)
except kinesis_producer_client.exceptions.ResourceInUseException as e:
    print("Stream already exists!!!")
    response = kinesis_producer_client.describe_stream(StreamName=stream_name)
    print(response)
else:
    print("Stream Created Successfully: ", stream_name )
    response = kinesis_producer_client.describe_stream(StreamName=stream_name)
    print("\nFollowing is the Stream detail....\n")
    print(response)
    print("\n")

file_record = 0
queue_record = 0
kinesis_record = 0

# Read each record from file and put it in Kinesis Stream
# If put_record is successful then send the output file containing "SequenceNumber" to consumer application through SQS

with open('sample-data.json','r') as input_file:
    for record in input_file:
        file_record = file_record + 1
        partition_key=json.loads(record)['id']
        partition_key=str(partition_key)

        try:
            put_data = kinesis_producer_client.put_record(StreamName=stream_name,
                                                          Data=record,
                                                          PartitionKey=partition_key)

        except Exception as e:
            print("ERROR!! Unable to put record in stream %s" % stream_name)
            print(e)
        else:
            kinesis_record = kinesis_record + 1
            print("{} records put in Kinesis Stream in Shard {}".format(kinesis_record, put_data['ShardId']))
            # shard_id = put_data['ShardId']
            # seq_no = put_data['SequenceNumber']
            # sqs_record=shard_id + "," + seq_no
            # try:
            #     sqs_queue.send_message(QueueUrl=sqs_queue_url,MessageBody=sqs_record)
            #     queue_record = queue_record + 1
            # except Exception as e:
            #     print("ERROR!!! unable to send message to sqs queue")

print("Total Records read from file: %s" % file_record)
print("Total Records sent to Kinesis Stream: %s" % kinesis_record)
print("Total Records sent to SQS Queue: %s" % queue_record)

