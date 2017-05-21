import pandas as pd
import math
import googlemaps
import csv
import json as simplejson
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
import sys

test = []
additionalTable = {}
currentAddress = []

centerPoint = {}
checkPoint = {}

#mrc klucz
gmaps = googlemaps.Client(key='**********************')
#ania klucz
#gmaps = googlemaps.Client(key='****************************************)

tmpstops = []

def areStopNear(checkPoint, centerPoint, km):
    l = []
    centerPoint['lat'] = float(centerPoint['lat'])
    centerPoint['lon'] = float(centerPoint['lon'])
    checkPoint['lat'] = float(checkPoint['lat'])
    checkPoint['lon'] = float(checkPoint['lon'])
    ky = 40000 / 360
    kx = math.cos(math.pi * centerPoint['lat'] / 180.0) * ky
    dx = math.fabs(centerPoint['lon'] - checkPoint['lon']) * kx
    dy = math.fabs(centerPoint['lat'] - checkPoint['lat']) * ky
    if math.sqrt(dx * dx + dy * dy) <= km:
        l = [checkPoint['lat'], checkPoint['lon']]
        return l



data_file = "/home/mapastec/Documents/studia/KoloNaukowe/dane/szkolykur.csv"
with open(data_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    szkoly = pd.DataFrame(list(reader))

data_file = "/home/mapastec/Documents/studia/KoloNaukowe/dane/lokale.csv"
with open(data_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    lokale = pd.DataFrame(list(reader))
lokale['closestStop'] = 0
lokale['closestStop'] = lokale['closestStop'].asobject

def getcloseobject(centerobjects, poiobjects):
    result = []
    tmpcount = 0
    for ind1, row1 in centerobjects.iterrows():
        nearstops = []
        if tmpcount % 100 == 0:
            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            print("time: %s, tmpcount: %s" % (st,tmpcount))

        for ind2, row2 in poiobjects.iterrows():
            centerPoint = {
                'lat': row1['lat'],
                'lon': row1['lon'],
            }
            checkPoint = {
                'lat': row2['lat'],
                'lon': row2['lon'],
            }
            stops = areStopNear(checkPoint, centerPoint, 1)
            if stops is not None:
                nearstops.append(stops)

        tmpcount += 1
        currentAddress = [row1['Ulica'], row1['NrBudynku'], float(row1['lat']), float(row1['lon'])]
        result.append((ind1, {'index': ind1, 'address': currentAddress, 'stops': nearstops}))
    return result

closeobject = getcloseobject(lokale, szkoly)

for i in range(len(closeobject)):
    additionalTable[closeobject[i][0]] = closeobject[i][1]

count = 0
objectscount = 0

with ThreadPoolExecutor(max_workers=4) as executor:
    for item in additionalTable.items():
        print(count)
        closestStop = []
        address = {
            'lat': item[1]['address'][2],
            'lon': item[1]['address'][3],
        }

        toBeSorted = []

        directions_result = None
        for i in item[1]['stops']:
            if i:
                count += 1
                try:
                    directions_result = gmaps.directions([address['lat'], address['lon']],
                                                     [i[0], i[1]],
                                                     mode="walking")
                except:
                    print("Googe directions unknown error: ", sys.exc_info())

                if directions_result:
                    try:
                        directions_result = simplejson.dumps([j['distance'] for j in directions_result[0]['legs']], indent=2)
                        mydistance = [directions_result.split('value": ')[1].split()][0][0]  # to jest okrutnie brzydkie, pls halp
                        distance = mydistance.replace(",", "")
                    except:
                        TypeError
                        print("Google returned something weird, sleeping for 30 seconds.")
                        time.sleep(30)
                    try:
                        if int(distance):
                            toBeSorted.append([i[0], i[1], int(distance)])
                    except ValueError:
                        print("Could not convert distance to int")
                    except:
                        print("Unkown error: ", sys.exc_info()[0])

        index = item[1]['index']
        countofobjects = 0
        # if result --> add values to coresponding columns, otherwise set it to None
        if toBeSorted:
            toBeSorted.sort(key=lambda x: x[2])
            countofobjects = int(len(toBeSorted))
            closestStop = toBeSorted[0]
            #print(item[1])
            lokale.set_value(index, 'closestStop', closestStop)
            lokale.set_value(index, 'countOfObjects', countofobjects)
        else:
            lokale.set_value(index, 'closestStop', None)
            lokale.set_value(index, 'countOfObjects', None)
        # Save result to file every 100 objects, this is in case something goes wrong and program
        # terminates. Thanks to that 100 rows of data will be lost at most.
        if objectscount % 100 == 0:
            lokale.to_csv("eduresult.csv", sep=',')
            print("Saving to file! Objects count: %s" %objectscount)
        objectscount += 1

print(count)
print("mission accomplished")
