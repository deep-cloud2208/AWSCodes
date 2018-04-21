import json
import boto3

#----------------------------------------------------------------------------------------------------------------------
# Using CLIENT

# s3_client = boto3.client('s3')

# CREATE AND DELETE buckets
# response_crt = s3_client.create_bucket(Bucket='s3-client-bucket3',
#                                    CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})
# response_del = s3_client.delete_bucket(Bucket='s3-client-bucket')
# response_lifcycl = s3_client.get_bucket_lifecyle(Bucket='s3-client-bucket')

# response_list = s3_client.list_buckets()

# PUT/LIST/DELETE OBJECTs
# with open('/HDD_Soumyadeep/Technical/Programming/Python/djangobook.pdf', 'rb') as file:
#     response = s3_client.put_object(Body=file, CacheControl='no-cache', Bucket='s3-client-bucket',
#                                     Metadata={'x-amz-uploaded': 'uploaded-using-python'},
#                                     Key='djangobook2.pdf')
# response = s3_client.list_objects(Bucket='s3-client-bucket')
# response = s3_client.delete_object(Bucket='s3-client-bucket',Key='djangobook2.pdf')
# response = s3_client.get_object_acl(Bucket='s3-client-bucket',Key='djangobook.pdf')
# response = s3_client.put_object_acl(Bucket='s3-client-bucket',ACL='public-read',Key='djangobook.pdf')
# response = s3_client.upload_file('mytemplate.html','s3-client-bucket','mytemplate.html')
# response = s3_client.delete_objects(Bucket='s3-client-bucket', Delete={ 'Objects' :
#                                                                             [{'Key': 'djangobook.pdf'},
#                                                                              {'Key': 'mytemplate.html'}]
#                                                                         })


# print(response)
# for i in response['Contents']:
#     print(i['Key'])
# for i in response['Buckets']:
#     print(i['Name'])
# print(response['Buckets'])
# print(response['ResponseMetadata']['HTTPHeaders']['location'])
# print(response['Location'])
#----------------------------------------------------------------------------------------------------------------------


# Using RESOURCE
s3_resource = boto3.resource('s3')
s3_bucket = s3_resource.Bucket('s3-resource-bucket')

response = s3_bucket.create(ACL='public-read', CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})
# response = s3_bucket.delete()
print(response)





