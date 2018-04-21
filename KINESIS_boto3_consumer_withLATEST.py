import boto3
import time
import threading

'''
- This consumer code will get the latest record from Kinesis Stream.
- It has to be run as a DAEMON.
- This program SHOULD run for each Shard in the stream. e.g. if there are 10 Shards, 10 instances of this program 
  should run, each instance processing one shard. 

... DO NOT USE THREADING ... 

'''

def get_data_from_shard(shard_id):

    print("Shard Id: \n", shard_id)
    count = 0

    while True:
        response = kinesis.get_shard_iterator(StreamName='boto3-kinesis-stream',
                                              ShardId=shard_id,
                                              ShardIteratorType='LATEST')
        shard_iterator = response['ShardIterator']
        record = kinesis.get_records(ShardIterator=shard_iterator, Limit=1)

        is_record = len(record['Records'])
        if is_record > 0:
            data = record['Records'][0]['Data']
            data = data.decode()
            out_file.write(data)
            count = count + 1
            print("{} records written".format(count))
            next_shard_iterator = record['NextShardIterator']
            break

    while True:
        next_record = kinesis.get_records(ShardIterator=next_shard_iterator, Limit=1)
        next_shard_iterator = next_record['NextShardIterator']
        record = next_record

        if len(record['Records']) > 0:
            data = record['Records'][0]['Data']
            data = data.decode()
            out_file.write(data)
            count = count + 1
            # print("Thread: {} - {} records written".format(threading.current_thread().getName(),count))
            print("{} records written".format(count))
        # else:
        #     print("Inside 2nd infinite loop - No record written...")

kinesis = boto3.client('kinesis', 'us-east-1')

out_file = open('kinesis-trial-run2.json', 'a+')

response = kinesis.describe_stream(StreamName='boto3-kinesis-stream')
shard_list = response['StreamDescription']['Shards']

shard1 = shard_list[0]['ShardId']
get_data_from_shard(shard1)

# shard2 = shard_list[1]['ShardId']
#
# Thread1 = threading.Thread(target=get_data_from_shard,args=(shard1,))
# Thread2 = threading.Thread(target=get_data_from_shard,args=(shard2,))
#
# Thread1.start()
# Thread2.start()
#
# Thread1.join()
# Thread2.join()

out_file.close()