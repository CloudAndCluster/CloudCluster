#!/usr/bin/python3

from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

with client:

    db = client.Freeway

    number_speed = db.loopdata.count_documents({'speed': {'$gt':100}})

    print("Count of high speeds are: {}".format(number_speed))
