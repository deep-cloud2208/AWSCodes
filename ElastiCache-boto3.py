import boto3
import redis
import sys

dynamodb = boto3.resource('dynamodb', 'us-east-1')
table = dynamodb.Table('elasticache-table')
cache = redis.StrictRedis(host='redis-boto-cls.sizfbd.ng.0001.use1.cache.amazonaws.com',port=6379)

if len(sys.argv) > 1:
    key = int(sys.argv[1])
    cache_key = "elasticache-table:id:" + str(key)

    cache_data = cache.hmget(cache_key, "name", "technology", "email")
    # cache_data = None

    if None in cache_data:
        print("\nCACHE MISS... getting data from DynamoDB")

        try:
            response = table.put_item(Key={'id':key})

        except Exception as e:
            print("Could not retrieve data from DynamoDB because of the following reason")
            print(e)

        else:
            map = {}
            retrieved_data = response['Item']

            id = retrieved_data['id']
            name = retrieved_data['name']['first_name'] + " " + retrieved_data['name']['last_name']
            technology = retrieved_data['technology']
            email = retrieved_data['email']

            map['id'] = id
            map['name'] = name
            map['technology'] = technology
            map['email'] = email

            print("\nFollowing is the information you are looking for...\n")
            print("Name:       ", name)
            print("Email:      ", email)
            print("Technology: ", technology)
            print("\n")

            try:
                print("\nPutting Above data in Redis Cache")
                cache_set = cache.hmset(cache_key,map)

            except Exception as e:
                print("Error while putting data in Redis Cache...")
                print(e)

    else:
        print("\nCACHE HIT... following are the details")
        name,technology,email = cache.hmget(cache_key, "name", "technology", "email")

        print("\nFollowing is the information you are looking for...\n")
        print("Name:       ", name.decode())
        print("Email:      ", email.decode())
        print("Technology: ", technology.decode(else:
        print("\nCACHE HIT... following are the details")))
        print("\n")
