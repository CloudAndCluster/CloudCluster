from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

with client:
    
    db = client.Freeway
    station = db.stationdata
    stations = station.find({'locationtext': 'Foster NB'},{"length" : 1})
    
    global len
    for st in stations:
        len = st['length']
    
    pipeline = [{'$match': {
                'locationtext': "Foster NB"
                }
            },
            {
             '$unwind': "$detectors",
             
            },
            {
            '$lookup': {
               'from': "loopdata",
               'localField': "detectors.detectorid",
               'foreignField': "detectorid",
               'as': "sameID"
                }
            },
            {'$unwind': "$sameID"},
            {'$match':
                {'$and' :[
                        {"sameID.starttime": {'$gte' : "2011-09-22T07:00:00"}},
                        {"sameID.starttime": {'$lt':"2011-09-22T09:00:00"}}
                    ] 
                }
            },
            {'$group': {'_id': 'null',  'total': {'$avg':"$sameID.speed"}}}
   ]
   
    pipeline1 = [{'$match': {
                'locationtext': "Foster NB"
                }
            },
            {
             '$unwind': "$detectors",
             
            },
            {
            '$lookup': {
               'from': "loopdata",
               'localField': "detectors.detectorid",
               'foreignField': "detectorid",
               'as': "sameID"
                }
            },
            {'$unwind': "$sameID"},
            {'$match':
                {'$and' :[
                        {"sameID.starttime": {'$gte' : "2011-09-22T16:00:00"}},
                        {"sameID.starttime": {'$lt':"2011-09-22T18:00:00"}}
                    ] 
                }
            },
            {'$group': {'_id': 'null',  'total': {'$avg':"$sameID.speed"}}}
   ]
   

    for doc in (station.aggregate(pipeline)):
        avgspeed = doc['total']
        traveltime = (len/avgspeed)*3600
        print("Travel time between 7-9AM: {0:.2f} sec".format(traveltime))
        
    for doc in (station.aggregate(pipeline1)):
        avgspeed = doc['total']
        traveltime = (len/avgspeed)*3600
        print("Travel time between 4-6PM: {0:.2f} sec".format(traveltime))
            