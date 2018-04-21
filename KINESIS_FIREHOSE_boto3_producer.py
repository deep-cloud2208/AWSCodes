import boto3

'''
Reads records from a file and puts those in Firehose Delivery Stream before sending to S3

'''

# s3_client = boto3.client('s3', 'us-east-1')
firehose_client = boto3.client('firehose', 'us-east-1')

try:
    fh_stream = firehose_client.create_delivery_stream(DeliveryStreamName='kinesis-firehose-boto3-stream',
                                                       # DelivereyStreamType='DirectPut',
                                                       S3DestinationConfiguration={
                                                           'BucketARN' : 'arn:aws:s3:::boto3-kinesis-firehose-bucket',
                                                           'RoleARN' : 'arn:aws:iam::968088411430:role/Boto3KinesisFirehoseRole',
                                                           'BufferingHints': {
                                                               'SizeInMBs': 10, 'IntervalInSeconds': 60
                                                           }
                                                       }
                                                       )
except firehose_client.exceptions.ResourceInUseException as e:
    print("Delivery Stream already exists... Moving on...")
    # print(e)
except Exception as e:
    print("Error!!! Unable to create stream....")
    print(e)
    quit()

file_count = 0
record_count = 0
with open('restaurants.json','r') as file:
    for record in file:
        file_count = file_count + 1
        try:
            data = record.encode()
            put_response = firehose_client.put_record(DeliveryStreamName='kinesis-firehose-boto3-stream',
                                                      Record={'Data': data })
        except Exception as e:
            print("Error writing data to Firehose Stream")
            print(e)
        else:
            record_count = record_count + 1
            print("{} records written".format(record_count))

print("Total Records read from file: ", file_count)
print("Total Records written to Firehose Stream: ", record_count)









