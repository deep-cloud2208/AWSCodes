import boto3
import json

sqs_client = boto3.client('sqs','us-east-1')
QueueName = sqs_client.get_queue_url(QueueName='intellipaat-sqs-queue')['QueueUrl']

queue_message_count = 0
file_record_count = 0

output_file = open('restaurants-out.json', 'w')

while True:
    try:
        queue_message = sqs_client.receive_message(QueueUrl=QueueName,
                                                   MaxNumberOfMessages=1)
    except Exception as e:
        print(e)
        print("something went wrong while receiving messages from SQS Queue!!!")
        quit()

    if 'Messages' not in queue_message:
        break
    else:
        ReceiptHandle = queue_message['Messages'][0]['ReceiptHandle']
        MessageBody = queue_message['Messages'][0]['Body']
        queue_message_count = queue_message_count + 1
        try:
            # print(MessageBody)
            output_file.write(MessageBody)
            file_record_count = file_record_count + 1
        except Exception as e:
            print(e)
            print("Error while writing to file")
        else:
            try:
                response = sqs_client.delete_message(QueueUrl=QueueName,
                                                     ReceiptHandle=ReceiptHandle)
                print(file_record_count)
            except Exception as e:
                print(e)
                print("Error while deleting message from queue!!")

output_file.close()
print("Total messages read from Queue: %s" % (queue_message_count) )
print("Total messages written to file: %s" % (file_record_count) )