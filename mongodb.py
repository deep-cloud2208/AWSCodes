# coding: utf-8
from pymongo import MongoClient
import pprint

conn = MongoClient('mongodb://18.219.199.41:27018')
db = conn['edureka']
coll = db['rest']

result = coll.find_one()

pprint(result)


