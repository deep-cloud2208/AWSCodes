import boto3

'''
Once a message is read from FIFO queue it is AUTOMATICALLY DELETED
'''

sqs_fifo_client = boto3.client('sqs','us-east-1')
QueueUrl = sqs_fifo_client.get_queue_url(QueueName='sqs-boto3-queue-fifo.fifo')['QueueUrl']

output_file = open('res-out.json','w')

message_count = 0
file_count = 0

while True:
    response = sqs_fifo_client.receive_message(QueueUrl=QueueUrl,MaxNumberOfMessages=1,
                                               AttributeNames=['MessageGroupId'])
    try:
        pass
    except Exception as e:
        print(e)

    if 'Messages' not in response:
        break
    else:
        MessageBody = response['Messages'][0]['Body']
        ReceiptHandle = response['Messages'][0]['ReceiptHandle']
        message_count = message_count + 1
        print(response['Messages'][0]['Attributes']['MessageGroupId'])

    try:
        output_file.write(MessageBody)
    except Exception as e:
        print("Error writing in file")
    else:
        file_count = file_count + 1
        try:
            delete_msg = sqs_fifo_client.delete_message(QueueUrl=QueueUrl,ReceiptHandle=ReceiptHandle)
        except Exception as e:
            print("Error Deleting Message from FIFO queue")
            print(e)

print("Total Records read from FIFO Queue: %d" % message_count)
print("Total Records written to file: %d" % file_count)
output_file.close()
