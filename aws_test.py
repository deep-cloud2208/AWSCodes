import boto3
import pprint
import json

region = 'us-east-1'
table_name = 'intellipaat-table'

dynamodb = boto3.resource('dynamodb', region)

table = dynamodb.Table(table_name)

for data in table.scan()['Items']:
    pprint.pprint(data)
