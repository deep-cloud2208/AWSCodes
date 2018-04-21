import redis
import sys
import datetime

cache = redis.Redis(host="localhost", port=6379, db=0)
start_time = datetime.datetime.now()

if len(sys.argv) > 1:
    key = "id:" + str(sys.argv[1])

    try:
        redis_record = cache.get(key)
    except Exception as e:
        print("error getting record from redis")
    else:
        if redis_record is None:
            print("Cache MISS... getting record from FILE")

            with open('sample-data.csv','r') as input_file:
                for i in input_file:
                    id, fname, lname, email, gender, ipaddr = i.split(",")
                    cache.set(key,i)
        else:
            print("Cache HIT... getting record from REDIS")
            redis_record = redis_record.strip().decode()
            redis_record=str(redis_record)

            id, fname, lname, email, gender, ipaddr = redis_record.split(",")

    print("\nFollowing is the Employee information: ")
    print("ID: ", id)
    print("Name: {} {}".format(fname,lname))

else:
    count = 0

    with open('sample-data.csv', 'r') as file:
        for record in file:
            count = count + 1
            id, fname, lname, email, gender, ipaddr = record.split(",")
            key = "id:" + str(count)

            try:
                cache.set(key, record.strip())
            except Exception as e:
                print(e)


end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print("Total Time taken: ", elapsed_time.total_seconds())