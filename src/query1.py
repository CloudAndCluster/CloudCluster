#!/usr/bin/python3

from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

with client:

    db = client.projectpart2

    number_speed = db.loopdataAll.count({'speed': {'$gt':100}})

    print("There are {} number of speeds".format(number_speed))
