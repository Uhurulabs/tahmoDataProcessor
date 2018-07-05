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


config = ConfigParser.ConfigParser()
config.readfp(open(r'config.cfg'))
API_ID = config.get('API-CREDS', 'API_ID')
API_SECRET = config.get('API-CREDS', 'API_SECRET')
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


#response = apiRequest(url)
#with open(stationFile, 'w') as outfile:
#    json.dump(response, outfile, sort_keys=True, indent=4,ensure_ascii=False)



with open(stationFile) as f:
    data = json.load(f)
del data["status"]
print "Length :",len(data)
print "Keys: ", data.keys()
