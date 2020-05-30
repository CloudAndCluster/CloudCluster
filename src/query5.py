from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')


station_length = {}
traveltime7_9 = 0
traveltime4_6 = 0
with client:
    
    db = client.Freeway
    station = db.stationdata
    stations = station.find({'highway.highwayid': 3},{"stationid" : 1, "length" : 1})

    for st in stations:
        stat = st['stationid']
        len = st['length']
        station_length[stat] = len
    
    print(station_length)
    
    for stat, len in station_length.items():
        print(stat," ", len)
        pipeline = [{'$match': {
                    'stationid': stat
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
                        # {'$and' :[
                            # {"sameID.starttime": {'$gte' : "2011-09-22T16:00:00"}},
                            # {"sameID.starttime": {'$lt':"2011-09-22T18:00:00"}}
                        # ]}  
                    }
                },
                {'$group': {'_id': 'null',  'total': {'$avg':"$sameID.speed"}}}
        ]
   
        pipeline1 = [{'$match': {
                    'stationid': stat
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
            traveltime = (len/avgspeed)
            traveltime7_9 += traveltime
            print("Travel time between 7-9AM for stationid {} is {} hr".format(stat, traveltime))
            
        for doc in (station.aggregate(pipeline1)):
            avgspeed = doc['total']
            traveltime = (len/avgspeed)
            traveltime4_6 += traveltime
            print("Travel time between 4-6PM for stationid {} is {} hr".format(stat, traveltime))

print("Travel time between 7-9AM: {0:.2f} mins".format(traveltime7_9 * 60))
print("Travel time between 4-6PM: {0:.2f} mins".format(traveltime4_6 * 60))                        