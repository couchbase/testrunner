from seriesly import Seriesly
from optparse import OptionParser
import sys
sys.path.append(".")
import testcfg as cfg
import datetime
import pandas as pd
import pygal
import os
import shutil

conn = Seriesly(cfg.SERIESLY_IP, 3133)

"""
" retrieve timeseries data from seriesly
"""
def getDBData(db):
  db=conn[db]
  data = db.get_all()
  return (data, None)[len(data) == 0]

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
" create datetime index array by converting ts strings
"""
def indexFromKeys(keys):
  return [datetime.datetime.strptime(ts[:ts.index('.')], "%Y-%m-%dT%H:%M:%S") for ts in keys]

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
" plot stats per-phase and save to html
"""
def plot_phases(ns_dataframe, test_id = "simple"):

  phase_dataframe = None
  columns = ns_dataframe.columns
  event_idx, _ = getSortedDBData('event')

  # plot each phase
  for i in range(len(event_idx)):
    if i == 0:
      phase_dataframe = ns_dataframe[ns_dataframe.index < event_idx[i+1]]
    elif i == len(event_idx) - 1:
      phase_dataframe = ns_dataframe[ns_dataframe.index > event_idx[i]]
    else:
      phase_dataframe = ns_dataframe[ (ns_dataframe.index < event_idx[i+1]) &\
        (ns_dataframe.index > event_idx[i])]

    ph_html = "%s/%s_phase%s.html" % (test_id, test_id, i)
    ph_cvs = "%s/%s_phase%s.cvs" % (test_id, test_id, i)
    f = open(ph_html, "w")
    print(ph_html)


    for column in columns:

      # filter out no-data columns
      if all([v == 0 for v in phase_dataframe[column].values]):
        continue

      # plot phase data and filter 0's values
      chart=pygal.Line(stroke=False, print_values=False, human_readable=True)
      chart.add(column, [x for x in (phase_dataframe[column].values) if x > 0])

      # write out chart html
      res = chart.render_response()
      f.write(res.data)

    # write out phase data to cvs
    phase_dataframe.to_csv(ph_cvs)


    f.close()

def get_testid():
  test_id = None
  evdata = getDBData('event')
  if not evdata:
    return

  evkeys = list(evdata.keys())
  if(len(evkeys) > 0):
    onephase = list(evdata[evkeys[0]].values())[0]
    if 'name' in onephase:
      test_id = str(onephase['name'])

  return test_id

def mkdir(path):
  if not os.path.exists(path):
      os.makedirs(path)
  else:
      shutil.rmtree(path)
      os.makedirs(path)

def prepare_env():
  test_id = get_testid()
  if test_id is None:
    raise Exception("testid missing from event-db")

  path = "%s" % test_id
  mkdir(path)
  return test_id


def parse_args():
    """Parse CLI arguments"""
    usage = "usage: %prog bucket1,bucket2\n\n" + \
            "Example: python tools/plotter.py default,saslbucket"

    parser = OptionParser(usage)
    options, args = parser.parse_args()

    if len(args) < 1 :
        parser.print_help()
        sys.exit()

    return options, args


def main():
  options, args = parse_args()
  buckets = args[0].split(",")

  test_id = prepare_env()

  for bucket in buckets:
    ns_dataframe = createDataframe('ns_serverdefault%s' % bucket)

    if ns_dataframe:
      ns_dataframe.to_excel('%s/%s.xlsx' % (test_id, test_id), sheet_name=test_id)
      plot_phases(ns_dataframe, test_id)

if __name__ == "__main__":
    main()
