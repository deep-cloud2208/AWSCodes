import boto3
import os

def add_shard(event, context):
    kinesis = boto3.client('kinesis', 'us-east-1')
    # sqs = boto3.client('sqs', 'us-east-1')

    # url=sqs.get_queue_url(QueueName='kinesis-stream-with-dynamic-shards-SQS')['QueueUrl']
    # response = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)

    # queue_message = response['Messages'][0]['Body']
    # current_shard_count = queue_message.split(',')[1]

    current_shard_count = os.environ['current_shard_count'] # Get it from Lambda Environment Variable
    current_shard_count = int(current_shard_count)
    target_shard_count = current_shard_count + 1
    shard_id = os.environ['shard_id'] # Get it from Lambda Environment Variable

    try:
        response = kinesis.update_shard_count(StreamName='kinesis-stream-with-dynamic-shards',
                                              TargetShardCount=target_shard_count,
                                              ScalingType='UNIFORM_SCALING')
    except Exception as e:
        print("Error updating shard")
        print(e)

    try:
        updated_shard_count = kinesis.describe_stream(StreamName='kinesis-stream-with-dynamic-shards')
    except Exception as e:
        print("error getting Stream Information")
        print(e)
    else:
        NewShardCount = updated_shard_count['StreamDescription']['Shards']
        NewShardCount = len(NewShardCount)

    print("Time Remaining: ", context.get_remaining_time_in_millis())
    print("Shard Id: ", shard_id)
    print("Updated Shard Count: ", NewShardCount)


