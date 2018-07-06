#!/usr/bin/env python2.7
from influxdb import InfluxDBClient



#client = InfluxDBClient('amina.uhurulabs.org', 8086, 'admin', '2015IS4us!', 'env_readings')
client = InfluxDBClient('localhost', 8086, '', '', 'env_readings')

result = client.query('SELECT electricalconductivity FROM "reading" WHERE "station_id" = \'TA00100\' GROUP BY * ORDER BY DESC LIMIT 1')


print result
print "X"
print (result['time'])
#print("Result: {0}".format(result))
