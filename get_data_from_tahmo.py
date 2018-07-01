#!/usr/bin/env python2.7
import urllib
import urllib2
import base64
import json
import csv
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
base_url = "https://tahmoapi.mybluemix.net/v1/timeseries/"

stationNames = []
firstReading = []
lastReading = []
stationId = []
combined = {}

# Generate base64 encoded authorization string
basicAuthString = base64.encodestring('%s:%s' % (API_ID, API_SECRET)).replace('\n', '')


def convertToUnixTimeStamp(date):
    t = date
    parsed_t = dp.parse(t)
    unixTimeStamp = parsed_t.strftime('%s')
    return unixTimeStamp


def readStationFile(stationFile):
    count = 0
    file = open(stationFile, "r")
    for line in file:
        count = count + 1
        if count == 1:
            stationId.append(str.strip(line))
        elif count == 2:
            stationNames.append(str.strip(line))
        elif count == 3:
            firstReading.append(str.strip(line))
        else:
            lastReading.append(str.strip(line))
            count = 0
    return
# Function to request data from API


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


def requestData(stationId, startDate, endDate, count):
    # Request timeseries for specific station
    tsResponse = apiRequest("https://tahmoapi.mybluemix.net/v1/timeseries/" + stationId[count] + "/", {
                            'startDate': startDate.strftime("%Y-%m-%d"), 'endDate': endDate.strftime("%Y-%m-%d")})
    decodedTsResponse = json.loads(tsResponse)
    if(decodedTsResponse['status'] == 'error'):
        print "Error:", decodedTsResponse['error']
    elif(decodedTsResponse['status'] == 'success'):
        print "API call success"
    for variable in decodedTsResponse['station']['variables']:
        try:
            # Loop through variable specific timeseries
            for timestamp, value in sorted(decodedTsResponse['timeseries'][variable].items()):
                print(variable)
                print sorted(decodedTsResponse['timeseries'][variable].items())
                if timestamp not in combined:
                    combined[timestamp] = {}
                combined[timestamp][variable] = value
        except:
            print "Could not process variable ", variable


readStationFile('stations_test.txt')
count = 0
attempt = 0
for station in stationNames:
    log_file = "retreval.log"
    log = open(log_file, "w+")

    startDate = datetime.datetime.strptime(firstReading[count], '%Y-%m-%dT%H:%M:%S.000Z').date()
    # SET LAST DATE TO GET TO CURRENT DATE
    # ctime = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    # HACK TO ONLY GET THREE DAYS DATA
    lastReading = startDate + datetime.timedelta(days=3)
    ctime = startDate + datetime.timedelta(days=1)
    #lastReading = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S.000Z').date()
    print "Getting data for ", stationNames[count]
    print "Start date :", startDate
    print "Last date :", lastReading
    sys.stdout.flush()
    while startDate < lastReading:
        delta = datetime.timedelta(days=1)
        endDate = startDate + delta
        if endDate > lastReading:
            endDate = lastReading
        url = base_url + stationId[count] + "/"
        params = {'startDate': startDate.strftime(
            "%Y-%m-%d"), 'endDate': endDate.strftime("%Y-%m-%d")}
        filename = stationId[count] + "_" + \
            startDate.strftime("%Y-%m-%d") + "-" + endDate.strftime("%Y-%m-%d") + ".json"
        #print "URL: ", url
        #print "PARAMS: ", params
        #print "FILENAME: ", filename
        print "Getting :", startDate, params,
        print'.'
        sys.stdout.flush()
        data = apiRequest(url, params)
        #print json.dumps(data, sort_keys=True, indent=4)
        with open(filename, 'w') as outfile:
            json.dump(data, outfile, sort_keys=True, indent=4,
                      ensure_ascii=False)
        logLine = "Written :" + filename + "\n"
        log.write(logLine)
        startDate = endDate
    count = count + 1
    log.close()
