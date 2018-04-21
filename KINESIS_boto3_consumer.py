import boto3

'''
Reads records from Kinesis Stream and writes all data to a file one by one.
Records are not deleted automatically once read. They need to be deleted explicitly.

'''

kinesis_consumer_client = boto3.client('kinesis', 'us-east-1')
# sqs_queue = boto3.client('sqs', 'us-east-1')
# sqs_queue_url = sqs_queue.get_queue_url(QueueName='sqs-queue-for-kinesis')['QueueUrl']

output_file = open('kinesis-output-file','w')

'''
To read records from a stream, it has to be read from each shard one by one. Following are the steps
step 1. Get Shard Id: Use "describe_stream"
step 2. Get Shard Iterator for the first time, 2nd time onwards get Next Shard Iterator: Use "get_shard_iterator"  
step 3. Get records using shard iterator: Use "get_records"
'''

stream_name = 'kinesis-boto3'
try:
    total_shards = kinesis_consumer_client.describe_stream(StreamName=stream_name)
except Exception as e:
    print(e)
    print("\nSome error while reading stream... Quitting...")
    quit()
else:
    total_shards_len = len(total_shards['StreamDescription']['Shards'])
    total_shards_list = total_shards['StreamDescription']['Shards']

record_count = 0
file_count = 0

for shard_no in range(0, total_shards_len):
    try:
        shard_id = total_shards_list[shard_no]['ShardId']
        print("\nCurrent Shard: ", shard_id)

        shard_iterator = kinesis_consumer_client.get_shard_iterator(StreamName=stream_name,
                                                                    ShardId=shard_id,
                                                                    ShardIteratorType='TRIM_HORIZON')
        shard_iterator = shard_iterator['ShardIterator']

        record = kinesis_consumer_client.get_records(ShardIterator=shard_iterator,
                                                     Limit=1)
        record_exist = len(record['Records'])
        # Reason for the above code - Even if there's no more record in the Shard, get_records will get
        # NextShardIterator with an empty 'Records' list


        data = record['Records'][0]['Data'] # Get Binary data from "record"
        data = data.decode()  # This is because data is kept in Binary in the stream: b'data'
        next_shard_itr = record['NextShardIterator'] # Get NextShardIterator from "record"

        output_file.write(data)

        file_count = file_count + 1
        record_count = record_count + 1
        print("Record no. {} written".format(record_count))

    except Exception as e:
        print(e)
    else:
        while record_exist > 0:
            try:
                record_2_onwards = kinesis_consumer_client.get_records(ShardIterator=next_shard_itr,
                                                                   Limit=1)
                record_exist = len(record_2_onwards['Records'])
            except Exception as e:
                print("\n")
                print(e)
            else:
                if record_exist > 0:
                    next_shard_itr = record_2_onwards['NextShardIterator']
                    data = record_2_onwards['Records'][0]['Data'].decode()
                    record_count = record_count + 1
                    output_file.write(data)
                    file_count = file_count + 1
                    print("Record no. {} written".format(record_count))


output_file.close()
print("\n")
print("Total Records read from Steam: ", record_count)
print("Total Records written to file: ", file_count)

'''
TRIM_HORIZON:   for reading PAST records delayed between many minutes (even hours) and 24 hours old. It doesn't return 
                recently put records. Using AFTER_SEQUENCE_NUMBER on the last record seen by this iterator returns an 
                empty array even when records have been recently PUT.

LATEST:     for reading FUTURE records in real time (immediately after they are PUT). I was tricked by the only sentence 
            of documentation I could find on this "Start reading just after the most recent record in the shard, so 
            that you always read the most recent data in the shard." You were getting an empty array because no records 
            had been PUT since getting the iterator. If you get this type of iterator, and then PUT a record, that 
            record will be immediately available.

Lastly, if you know the sequence id of a recently put record, you can get it immediately using AT_SEQUENCE_NUMBER, 
and you can get later records using AFTER_SEQUENCE_NUMBER even though they wont appear to a TRIM_HORIZON iterator.
'''