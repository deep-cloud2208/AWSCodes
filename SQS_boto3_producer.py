import boto3
import datetime
import json

sqs_client = boto3.client('sqs','us-east-1')

try:
    response = sqs_client.create_queue(QueueName='intellipaat-sqs-queue',
                                       Attributes={
                                           'DelaySeconds' : '5',
                                           'VisibilityTimeout' : '300'
                                       })
except sqs_client.exceptions.QueueNameExists as e1:
    print("Queue exists!!!")
    print(e1)
except Exception as e:
    print(e)
    print("Something went wrong... quitting")
    quit()

QueueUrl = sqs_client.get_queue_url(QueueName='intellipaat-sqs-queue')['QueueUrl']

# redrive_policy = {
#                     'deadLetterTargetArn': 'arn:aws:sqs:us-east-1:968088411430:sqs-boto3-queue-redrive',
#                     'maxReceiveCount': '5'
# }
#
# response = sqs_client.set_queue_attributes(QueueUrl=QueueName,
#                                            Attributes={'RedrivePolicy': json.dumps(redrive_policy)}
#                                            )

record_count = 0
message_count = 0

start_time = datetime.datetime.now()

with open('restaurants.json','r') as input_file:
    for line in input_file:
        record_count = record_count + 1
        queue_message = line

        try:
            response = sqs_client.send_message(QueueUrl=QueueUrl,
                                               MessageBody=queue_message)
        except Exception as e:
            print(e)
        else:
            message_count = message_count + 1
            print("{} Messages Sent Successfully".format(message_count))

finish_time = datetime.datetime.now()
diff = finish_time - start_time

print("Total Records read from file: %d" % (record_count))
print("Total Message sent to Queue : %d" % (message_count))
print("Total time taken in seconds : %s" % (diff.total_seconds()))