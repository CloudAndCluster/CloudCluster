from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

with client:
    
    db = client.Freeway
    station = db.stationdata
    
    pipeline = [{'$match': {
                    'locationtext': "Foster NB"
                    }
                },
                {
                 '$unwind': "$detectors"
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
                    {'$and' :
                        [{"sameID.starttime": {'$gte' : "2011-09-21T00:00:00"}},
                        {"sameID.starttime": {'$lt':"2011-09-22T00:00:00"}}
                        ]
                    }
                },
                {'$group': {'_id': 'null', 'total': {'$sum':"$sameID.volume"}}}
                ]

    for doc in (station.aggregate(pipeline)):
        TotalVolume = doc['total']
        print("TotalVolume is: {}".format(TotalVolume))

      

