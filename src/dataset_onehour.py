import csv
import json
from datetime import datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

## Read the CSV files
highway_file1 = csv.DictReader(open("highways.csv", 'r'))
station_file1 = csv.DictReader(open("freeway_stations.csv", 'r'))
detector_file1 = csv.DictReader(open("freeway_detectors.csv", 'r'))
loopdata_file1 = csv.DictReader(open("freeway_loopdata_OneHour.csv", 'r'))


## Store it in the memory
highway_file = list(highway_file1)
station_file = list(station_file1)
detector_file = list(detector_file1)
loopdata_file = list(loopdata_file1)


stations = []
loopdataAll = []
for h in highway_file:
    for s in station_file:
        if s["highwayid"] == h["highwayid"]:
            station = {
                "stationid": int(s["stationid"]),
                "milepost": float(s["milepost"]),
                "locationtext": s["locationtext"],
                "upstream": int(s["upstream"]),
                "downstream": int(s["downstream"]),
                "stationclass": int(s["stationclass"]),
                "numberlanes": int(s["numberlanes"]),
                "latlon": s["latlon"],
                "length": float(s["length"]),
                "detectors" : [],
                "highway" : {
                    "highwayid": int(h["highwayid"]),
                    "shortdirection": h["shortdirection"],
                    "direction": h["direction"],
                    "highwayname": h["highwayname"]
                }
            }
            for d in detector_file:
                if d["stationid"] == s["stationid"]:
                    detector = {"detectorid": int(d["detectorid"]),
                    "lanenumber": int(d["lanenumber"]),
                    "loopdata": {}
                    }
                    station["detectors"].append(detector)
            stations.append(station)
           

for ll in loopdata_file:

    speed = ll["speed"]
    try:
        speed = int(speed)
    except:
        pass
    
    occupancy = ll["occupancy"]
    try:
        occupancy = int(occupancy)
    except:
        pass
    
    volume = ll["volume"]
    
    try:
        volume = int(volume)
    except:
        pass
    
    loopone = {
        "detectorid" : int(ll["detectorid"]),
        "starttime": datetime.strptime(ll["starttime"], '%m/%d/%Y %H:%M:%S'),
        "volume": volume,
        "speed": speed,
        "occupancy": occupancy,
        "status": int(ll["status"]),
        "dqflags": int(ll["dqflags"])
    }
    if(speed == ""):
        continue
    loopdataAll.append(loopone)

f = open( 'stationdata1.json', 'w')
out = json.dumps(stations, indent=4)
f.write(out)

g = open( 'loopdata1.json', 'w')
out = json.dumps(loopdataAll, indent=4, default=json_serial)
g.write(out)