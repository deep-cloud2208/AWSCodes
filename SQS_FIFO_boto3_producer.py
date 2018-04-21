import boto3
import json

sqs_fifo_client = boto3.client('sqs', 'us-east-1')


try:
    response = sqs_fifo_client.create_queue(QueueName='sqs-boto3-queue-fifo.fifo',
                                            Attributes={'VisibilityTimeout': '5',
                                                        'FifoQueue': 'True',
                                                        'ContentBasedDeduplication': 'True'})
except sqs_fifo_client.exceptions.QueueNameExists as e2:
    print("Queue already exists... continuing")
except Exception as e1:
    print("Error while creating FIFO queue")
    print(e1)
    quit()

QueueName = sqs_fifo_client.get_queue_url(QueueName='sqs-boto3-queue-fifo.fifo')['QueueUrl']

file_count = 0
message_count = 0

with open('restaurants.json', 'r') as file:
    for line in file:
        queue_message = line
        file_count = file_count + 1
        try:
            response = sqs_fifo_client.send_message(QueueUrl=QueueName,MessageBody=queue_message,
                                                    MessageGroupId='deep2208mech')
        except Exception as e:
            print("Error while sending message!!!")
            print(e)
            break
        else:
            message_count = message_count + 1
            print(message_count)

print("Total Records Read from File: %d" % file_count)
print("Total msgs send to FIFO queue: %d" % message_count)


