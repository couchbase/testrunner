#!/usr/bin/python
import sys
import os
import argparse

#print " Apply the indexer changes before creating indexes"
#m=os.system("curl  http://10.6.2.167:8091/pools/default/ -u Administrator:password -d \"indexMemoryQuota=22000\" ")
#print m
#r=os.system("curl -X POST -u Administrator:password http://10.6.2.167:8091/settings/indexes -d indexerThreads=3")
#print r
parser = argparse.ArgumentParser(description='This script changes Indexer settings, create appropriate settings.json to upload')
parser.add_argument('-idx', '--index_node', help='Index Node IP', default="10.6.2.167")

args = vars(parser.parse_args())
index_node = str(args['index_node'])

url = index_node + ":9102/settings"
path="/root/systest-worker/testrunner/pysystests/tests/n1ql"
print(url)
print("Apply changes to indexer_settings_scan_timeout to 1400000 and index memory to 21G and indexer threads to 8")
t=os.system("curl {0} -u Administrator:password -d  @'{1}/settings.json'".format(url, path))
print(("curl {0} -u Administrator:password -d  @settings.json".format(url)))
print(t)
