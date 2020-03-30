"""pushes stats from serieslydb to CBFS_HOST

This module works by collecting stats from the seriesly database specified in testcfg.py
and furthermore uses the pandas (python data-anysis) module to store the stats into a dataframe
that is compatibale with version comparisions in the report generator.  Once in-memory as a
dataframe the stats are dumped to a csv file, compressed and pushed to CBFS.

usage: push_stats.py [-h] --spec <dir>/<test.js>
                          --version VERSION
                          --build BUILD
                          [--name NAME]
                          [--cluster default]

Where spec name is a required argument specifying the file used to generate stats.

"""

import sys
sys.path.append(".")
import argparse
import json
import gzip
import testcfg as cfg
import pandas as pd
import os
import shutil
from seriesly import Seriesly, exceptions
import requests

# cbfs
CBFS_HOST = 'http://10.5.0.128:8484'

# archives array for keeping track of files to push (archive) into cbfs
archives = []

# setup parser
parser = argparse.ArgumentParser(description='CB System Test Stat Pusher')
parser.add_argument("--spec", help="path to json file used in test", metavar="<dir>/<test.js>", required = True)
parser.add_argument("--version",  help="couchbase version.. ie (2.2.0)", required = True)
parser.add_argument("--build",  help="build number", required = True)
parser.add_argument("--name", default=None, help="use to override name in test spec")
parser.add_argument("--cluster", default="default", help="should match whatever you set for CB_CLUSTER_TAG")

## connect to seriesly
conn = Seriesly(cfg.SERIESLY_IP, 3133)



""" getDBData

  retrieve timeseries data from seriesly

"""
def getDBData(db):
  data = None

  try:
    db = conn[db]
    data = db.get_all()
    data = stripData(data)
  except exceptions.NotExistingDatabase:
    print("DB Not found: %s" % db)
    print("cbmonitor running?")
    sys.exit(-1)

  return (data, None)[len(data) == 0]

""" stripData

  use data from the event db to collect only data from preceeding test

"""
def stripData(data):
  ev, _ = getSortedEventData()

  if ev is None:
    return data

  start_time = ev[0]
  copy = {}

  # remove data outside of start_time
  for d in data:
    if d >= start_time:
      copy[d] = data[d]

  del data
  return copy

def getSortedEventData():
  keys = data = None

  if 'event' in conn.list_dbs():
    data = conn['event'].get_all()
    if(len(data) > 0):
      keys, data = sortDBData(data)
    else:
      print("warning: eventdb exists but is empty")
  else:
    print("warning: eventdb not found in seriesly db")

  return keys, data


def get_query_params(start_time):
  query_params = { "group": 10000,
                   "reducer": "identity",
                   "from": start_time,
                   "ptr" : ""
                 }
  return query_params


"""
" sort data by its timestamp keys
"""
def sortDBData(data):

  sorted_data = []
  keys = []
  if(data):
    keys = sorted(data.keys())

  for ts in keys:
    sorted_data.append(data[ts])

  return keys, sorted_data

def getSortedDBData(db):
  return sortDBData(getDBData(db))

"""
" make a timeseries dataframe
"""
def _createDataframe(index, data):

  df = None

  try:

    if(data):
      df = pd.DataFrame(data)
      df.index = index

  except ValueError as ex:
    print("unable to create dataframe: has incorrect format")
    raise Exception(ex)

  return df

"""
" get data from seriesly and convert to a 2d timeseries dataframe rows=ts, columns=stats
"""
def createDataframe(db):
  df = None
  data = getDBData(db)

  if data:
    index, data = getSortedDBData(db)
    df = _createDataframe(index, data)
  else:
    print("WARNING: stat db %s is empty!" % db)

  return df


"""
" store stats per-phase to csv
"""
def storePhase(ns_dataframe, version, test, build, bucket):

  path = "system-test-results/%s/%s/%s/%s" % (version, test, build, bucket)
  print("Generating stats: %s" % path)

  phase_dataframe = None
  columns = ns_dataframe.columns
  event_idx, _ = getSortedEventData()
  if event_idx is None:
    print("storing all data in single phase")
    dataframeToCsv(ns_dataframe, path, test, 0)

  else:
    # plot each phase
    for i in range(len(event_idx)):
      if i == 0:
        phase_dataframe = ns_dataframe[ns_dataframe.index < event_idx[i+1]]
      elif i == len(event_idx) - 1:
        phase_dataframe = ns_dataframe[ns_dataframe.index > event_idx[i]]
      else:
        phase_dataframe = ns_dataframe[ (ns_dataframe.index < event_idx[i+1]) &\
          (ns_dataframe.index > event_idx[i])]
      dataframeToCsv(phase_dataframe, path, test, i)

def dataframeToCsv(dataframe, path, test, phase_no):
    ph_csv  = "%s/%s_phase%s.csv" % (path, test, phase_no)
    ph_csv_gz  = "%s.gz" % ph_csv
    dataframe.to_csv(ph_csv)
    f = gzip.open(ph_csv_gz, 'wb')
    f.writelines(open(ph_csv, 'rb'))
    f.close()
    os.remove(ph_csv)
    archives.append(ph_csv_gz)


def generateStats(version, test, build, dbs):

  for db in dbs:
    ns_dataframe = createDataframe('%s' % db.name)

    if ns_dataframe:
      storePhase(ns_dataframe, version, test, build, db.bucket)


def pushStats():

  for data_file in archives:
    url = '%s/%s' % (CBFS_HOST, data_file)
    print("Uploading: " + url)
    suffix = data_file.split('.')[-1]

    if(suffix == 'js'):
      headers = {'content-type': 'text/javascript'}
    else:
      headers = {'content-type': 'application/x-gzip'}
    data = open(data_file, 'rb')
    r = requests.put(url, data=data, headers=headers)
    print(r.text)

def mkdir(path):
  if not os.path.exists(path):
      os.makedirs(path)
  else:
      shutil.rmtree(path)
      os.makedirs(path)

def prepareEnv(version, test, build, dbs):

  for db in dbs:
    path = "system-test-results/%s/%s/%s/%s" % (version, test, build, db.bucket)
    mkdir(path)



def loadSpec(spec):
  try:
    f = open(spec)
    specJS = json.loads(f.read())
    return specJS
  except Exception as ex:
    print("Invalid test spec: "+ str(ex))
    sys.exit(-1)

def setName(name, spec):

  if name is None:
    if 'name' in spec:
      name = str(spec['name'])
    else:
      print("test name missing from spec")
      sys.exit(-1)

  # remove spaces
  name = name.replace(' ', '_')
  return name

def getDBs(cluster = 'default'):

  dbs = []

  if len(conn.list_dbs()) == 0:
    print("seriesly database is empty, check SERIESLY_IP in your testcfg.py")
    sys.exit(-1)

  bucket_dbs = [db_name for db_name in conn.list_dbs() if db_name.find('ns_server'+cluster)==0 ]


  for db in bucket_dbs:
    # filter out dbs with host ip/name attached
    if(len([bucket for bucket in bucket_dbs if bucket.find(db) == 0]) != 1):
      db = DB('ns_server', db)
      dbs.append(db)

  atop_dbs = [db_name for db_name in conn.list_dbs() if db_name.find('atop'+cluster)==0]
  for db in atop_dbs:
    dbs.append(DB('atop'+cluster, db))

  latency_dbs = [db_name for db_name in conn.list_dbs() if db_name.find('latency') > 0]
  for db in latency_dbs:
    dbs.append(DB('', db))

  if(len(dbs) == 0):
    print("no bucket data in seriesly db")
    print("did you try with '--cluster %s' ?" % cfg.CB_CLUSTER_TAG)
    sys.exit(-1)

  return dbs

def createInfoFile(version, test, build, dbs, specPath):

  path = "system-test-results/%s/%s/%s" % (version, test, build)
  fname = '%s/_info.js' % path
  specName = specPath.split('/')[-1]

  info = {'buckets' : [db.bucket for db in dbs],
          'spec' : specName,
          'files' : archives}

  f = open(fname, 'wb')
  f.write(json.dumps(info))

  # archive info for pushing to cbfs
  archives.append(fname)

  # archive spec for pushing to cbfs
  shutil.copy(specPath, path)
  archives.append("%s/%s" % (path, specName))

class DB(object):
  def __init__(self, prefix, name):
    self.prefix = prefix
    self.name = name
    self.bucket= name[len(prefix):]

def main():


  args = parser.parse_args()
  specPath = args.spec
  spec = loadSpec(specPath)
  test = setName(args.name, spec)
  build = args.build
  version = args.version
  cluster = args.cluster
  dbs = getDBs(cluster)

  prepareEnv(version, test, build, dbs)
  generateStats(version, test, build, dbs)
  createInfoFile(version, test, build, dbs, specPath)
  pushStats()

if __name__ == "__main__":
    main()
