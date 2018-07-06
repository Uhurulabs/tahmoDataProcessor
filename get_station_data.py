#!/usr/bin/env python2.7
import urllib
import urllib2
import base64
import json
import os
import datetime
import time
import dateutil.parser as dp
import ConfigParser
import sys
import MySQLdb as my
import Geohash


config = ConfigParser.ConfigParser()
config.readfp(open(r'config.cfg'))
API_ID = config.get('API-CREDS', 'API_ID')
API_SECRET = config.get('API-CREDS', 'API_SECRET')
MYSQL_SERVER = config.get('MYSQL-CREDS', 'MYSQL_SERVER')
MYSQL_USER = config.get('MYSQL-CREDS', 'MYSQL_USER')
MYSQL_PASS = config.get('MYSQL-CREDS', 'MYSQL_PASS')

authKey = base64.encodestring('%s:%s' % (API_ID, API_SECRET)).replace('\n', '')
url = "https://tahmoapi.mybluemix.net/v1/stations"
stationFile = "stations.json"

stationNames = []
firstReading = []
lastReading = []
stationId = []


def apiRequest(url, params={}):
    encodedParams = urllib.urlencode(params)
    try:
        request = urllib2.Request(url + '?' + encodedParams)
        request.add_header("Authorization", "Basic %s" % authKey)
        tries = 5
        while tries >= 0:
            try:
                urlobject = urllib2.urlopen(request)
                jsondata = json.loads(urlobject.read())
                return jsondata
            except:
                if tries == 0:
                    # If we keep failing, raise the exception for the outer exception
                    # handling to deal with
                    raise
                else:
                    # Wait a few seconds before retrying and hope the problem goes away
                    time.sleep(3)
                    tries -= 1
                    continue
    except urllib2.HTTPError, err:
        if err.code == 401:
            print "Error: Invalid API credentials"
            quit()
        elif err.code == 404:
            print "Error: The API endpoint is currently unavailable"
            quit()
        else:
            print err
            quit()

def convertToUnixTimeStamp(date):
    t = date
    parsed_t = dp.parse(t)
    unixTimeStamp = parsed_t.strftime('%s')
    return unixTimeStamp

#response = apiRequest(url)
#with open(stationFile, 'w') as outfile:
#    json.dump(response, outfile, sort_keys=True, indent=4,ensure_ascii=False)



with open(stationFile) as f:
    data = json.load(f)
del data["status"]
print "Length :",len(data)
print "Keys: ", data.keys()
#print "Keys: ", data["stations"].keys()

def putDataInfoMysql():

    sqlConnection = my.connect(host= "amina.uhurulabs.org",
                      user="root",
                      passwd="",
                      db="sensors")
    cursor = sqlConnection.cursor()

    for key in data["stations"]:
        elevation = str(key.get("elevation"))
        name = str(key.get("name"))
        lng = str(key["location"].get("lng"))
        lat = str(key["location"].get("lat"))
        lastMeasurement = str(key.get("lastMeasurement"))
        firstMeasurement = str(key.get("firstMeasurement"))
        timezoneOffset = str(key.get("timezoneOffset"))
        battery = str(key.get("battery"))
        tahmoId = str(key.get("id"))
        deviceId = str(key.get("deviceId"))
        first = convertToUnixTimeStamp(firstMeasurement)
        gHash = Geohash.encode((key["location"].get("lat")),(key["location"].get("lng")), precision=10)
        #print tahmoId, deviceId, name, lng, lat, elevation, battery, firstMeasurement, first, lastMeasurement, gHash
        sqlCommand = "INSERT INTO weatherstations (tahmoId, name, longitude, latitude, elevation, battery, deviceId, firstMeasurement, lastMeasurement, geohash) \
                VALUES ('" + tahmoId + "','" + name + "','" + lng + "','" + lat + "','" + elevation + "','" + battery + "','" + deviceId + "','" + firstMeasurement + "','" + lastMeasurement + "','" + gHash + "')"
        print sqlCommand

        try:
           cursor.execute(sqlCommand)
           sqlConnection.commit()
        except my.Error as e:
            print e
            sqlConnection.rollback()
    sqlConnection.close ()
