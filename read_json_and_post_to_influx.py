#!/usr/bin/env python2.7
import json
import requests
import os
import dateutil.parser as dp

count = 0
variableName = {}
dataPath = "data/"
influxDbHost = "localhost"


def convertToUnixTimeStamp(date):
    t = date
    parsed_t = dp.parse(t)
    unixTimeStamp = parsed_t.strftime('%s')
    return unixTimeStamp


def getDirectoryFileList(dataPath):
    file_list = os.listdir(dataPath)
    return file_list


json_files = getDirectoryFileList(dataPath)

for json_file in json_files:
    json_file = dataPath + json_file
    with open(json_file) as f:
        data = json.load(f)

    amountVariables = len(data['station']['variables'])
    station_id = data['station']['id']
    stationName = data['station']['name']
    stationName = stationName.replace(" ", "_")
    stationElevation = str(data['station']['elevation'])
    stationLong = str(data['station']['location']['lat'])
    stationLat = str(data['station']['location']['lng'])
    log_file = station_id + "-posting.log"
    log = open(log_file, "a+")
    count = 0
    # create varialbe name array
    for variable in data['station']['variables']:
        count = count + 1
        variableName[count] = variable
    #print data['station']['variables']
    #print stationName
    print json_file
    data_points = []
    payload_line = ""
    logLine = ""
    for time in sorted(data['timeseries'][variableName[1]].keys()):
            payload_line = "reading,station_id=" + station_id + ",station_name=" + stationName
            payload_line = payload_line + ",station_elevation=" + stationElevation + \
                ",station_long=" + stationLong + ",station_lat=" + stationLat
            payload_line = payload_line + " "
#            print time, payload_line
#            print data['timeseries'].keys()
#            for key in sorted(data['timeseries'].keys()):
#                print key
#            print "test line: ", test_line
            count = 0
            missing_variables = ""
            while count < amountVariables:
                count = count + 1
                if count == 1:
                    payload_line = payload_line + \
                        str(variableName[count]) + "=" + \
                        str(data['timeseries'][variableName[count]][time])
                else:
                    if str(variableName[count]) in data['timeseries'].keys():
                        try:
                            payload_line = payload_line + "," + \
                                str(variableName[count]) + "=" + \
                                str(data['timeseries'][variableName[count]][time])
                        except:
                            missing_variables = missing_variables + str(variableName[count]) + " "


            payload_line = payload_line + " " + convertToUnixTimeStamp(time)
            logLine =  "READINGTIME : "+ time + " : FROM : " + json_file + " : POSTING : " + payload_line
            logLine = logLine + " : MISSINGVARS : " + missing_variables + "\n"
            print logLine
            log.write(logLine)
            data_points.append(payload_line)
            payload = "\n".join(data_points)
            #print payload

    count = 0
    for payload in data_points:
        count = count + 1
        #print count, payload
        url = "http://" + influxDbHost + ":8086/write"
        qs = {"db": "env_readings", 'precision': 's'}
        response = requests.request("POST", url, data=payload, params=qs)
#                                    auth=('admin', '2015IS4us!'))
        #print payload
#        print url, data, qs
#        print response.text
