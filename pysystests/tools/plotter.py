import sys
import time
from optparse import OptionParser
sys.path.append(".")
import testcfg as cfg
from seriesly import Seriesly
import store_report
import requests
import datetime
import json
import shutil
import os
import urllib.request, urllib.parse, urllib.error


def get_query_params(metric, start_time, end_time, reducer):
    query_params = { "group": 15000,  # 15 seconds
                     "ptr": '/{0}'.format(metric),
                     "reducer": reducer,
                     "from": start_time,
                     "to": end_time
                   }
    return query_params

def get_cluster_ips():
    ips = []
    for ip in cfg.CLUSTER_IPS:
        ips.append(ip.replace(".", ""))
    return ips

def parse_args():
    """Parse CLI arguments"""
    usage = "usage: %prog cluster-name bucket1,bucket2\n\n" + \
            "Example: python tools/plotter.py cluster_kv default,saslbucket"

    parser = OptionParser(usage)
    options, args = parser.parse_args()

    if len(args) < 2 :
        parser.print_help()
        sys.exit()

    return options, args

def plot_use_cbmonitor(snapshot_name, cluster_name, start_time, end_time):
    # Need a api to delete the existing snapshots

    payload = {'name': snapshot_name, 'cluster': cluster_name, 'ts_from': start_time, 'ts_to': end_time}
    print("Adding snapshot" + json.dumps(payload))

    try:
        r = requests.post('http://%s:8000/cbmonitor/add_snapshot/' % cfg.SERIESLY_IP, data=payload)
        r.raise_for_status()
        time.sleep(30)
    except requests.exceptions.HTTPError as e:
        print(e)
        print("Uable to create snapshot. Snapshot maybe already exists")
        pass

    try:
        #BaseReport, BaseXdcrReport,FullReport
        r = requests.post('http://%s:8000/cbmonitor/pdf/' % cfg.SERIESLY_IP, data={'snapshot':snapshot_name, 'report':'FullReport'}, timeout=300)
        r.raise_for_status()
    except Exception as e:
        print(e)
        print("impossible to generate pdf report")

    os.system('rm -f *.pdf')
    os.system('wget \"http://%s:8000/media/%s.pdf\"' % (cfg.SERIESLY_IP, urllib.parse.quote(snapshot_name, ' ')))

def prepare_folder_report(run_id, i):
    path = "%s/phase%d" % (run_id, i)
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        shutil.rmtree(path)
        os.makedirs(path)

    os.system('mv *.pdf %s/' % path)

def get_data_from_query(db, metric, start_time, end_time, reducer):
    response = db.query(get_query_params(metric, start_time, end_time, reducer))
    data = dict((k, v[0]) for k, v in response.items())
    del response
    values = list()

    for value in data.values():
        if value is not None:
            values.append(float(value))
    del data
    return values

def store_90th_value(db, metric, start_time, end_time):
    values = get_data_from_query(db, metric, start_time, end_time, "max")

    value_90th = None
    if len(values) >= 1:
        values.sort()
        pos = int(len(values) * 0.9)
        value_90th = values[pos]

    return value_90th


def store_avg_value(db, metric, start_time, end_time):
    values = get_data_from_query(db, metric, start_time, end_time, "avg")

    sum = 0
    avg_value = None
    if len(values) >= 1:
        for x in values:
            sum = sum + x
        avg_value = sum // len(values)

    return avg_value


def store_90th_avg_value(buckets, start_time, end_time, run_id, i):
    ips = get_cluster_ips()
    ns_server_stats = None
    atop_stats = None
    latency_stats = ['set_latency', 'get_latency', 'delete_latency', 'query_latency']
    dict_90th = {}
    dict_avg = {}

    dict_90th['ns_server'] = {}
    dict_avg['ns_server'] = {}
    time.sleep(2)
    connection = Seriesly(cfg.SERIESLY_IP, 3133)
    for bucket in buckets:
        toolbar_width = 41
        sys.stdout.write("[%s] indicator\n" % ("*" * toolbar_width))
        sys.stdout.flush()
        dict_90th['ns_server'][bucket] = {}
        dict_avg['ns_server'][bucket] = {}

        #empty ip string appended with bucket name is the ns_server db name for entire cluster
        cluster_ips = ips
        cluster_ips.append('')
        for ip in cluster_ips:
            ns_server_db = "ns_serverdefault" + bucket + ip
            if ":" in ns_server_db:
                ns_server_db = ns_server_db[0:ns_server_db.find(":")]
            if ip == '':
                ip = 'cluster'
            dict_90th['ns_server'][bucket][ip] = {}
            dict_avg['ns_server'][bucket][ip] = {}
            if  ns_server_db not in connection.list_dbs():
                print("db %s was not found" % (ns_server_db))
                continue
            db = connection[ns_server_db]
            if ns_server_stats is None:
                ns_server_stats = list(db.get_all().values())[0].keys()
            print("Store ns server stats for bucket %s on %s" % (bucket, ip))
            sys.stdout.write("[")
            num = 1
            for metric in ns_server_stats:
                dict_90th['ns_server'][bucket][ip][metric] = store_90th_value(db, metric, start_time, end_time)
                dict_avg['ns_server'][bucket][ip][metric] = store_avg_value(db, metric, start_time, end_time)
                if num % (len(ns_server_stats) // toolbar_width) == 0:
                    sys.stdout.write("=")
                    sys.stdout.flush()
                    time.sleep(0.5)
                num += 1
            sys.stdout.write("]\n")


    dict_90th['atop'] = {}
    dict_avg['atop'] = {}
    for ip in ips:
        atop_db = "atopdefault" + ip
        dict_90th['atop'][ip] = {}
        dict_avg['atop'][ip] = {}
        if ":" in atop_db:
           atop_db = atop_db[0:atop_db.find(":")]
        if  atop_db not in connection.list_dbs():
            print("db %s was not found" % (atop_db))
            continue
        db = connection[atop_db]
        if atop_stats is None:
            atop_stats = list(db.get_all().values())[0].keys()
        print("Store atop stats for node %s" % (ip))
        for metric in atop_stats:
            dict_90th['atop'][ip][metric] = store_90th_value(db, metric, start_time, end_time)
            dict_avg['atop'][ip][metric] = store_avg_value(db, metric, start_time, end_time)

    dict_90th['latency'] = {}
    dict_avg['latency'] = {}
    for bucket in buckets:
        dict_90th['latency'][bucket] = {}
        dict_avg['latency'][bucket] = {}
        latency_db = "%slatency" % bucket
        if latency_db not in connection.list_dbs():
            print("db %s was not found" % (latency_db))
            continue
        db = connection[latency_db]
        print("Store latency stats for bucket %s" % (bucket))

        for metric in latency_stats:
            dict_90th['latency'][bucket][metric] = store_90th_value(db, metric, start_time, end_time)
            dict_avg['latency'][bucket][metric] = store_avg_value(db, metric, start_time, end_time)


    os.system('rm -f %s/phase%d/*.txt' % (run_id, i))
    json.dump(dict_90th, open("%s/phase%d/90percentile.txt" % (run_id, i), 'w'))
    json.dump(dict_avg, open("%s/phase%d/average.txt" % (run_id, i), 'w'))
    del dict_90th
    del dict_avg


def plot_all_phases(cluster_name, buckets):

    db_event = Seriesly(cfg.SERIESLY_IP, 3133)['event']

    # Get system test phase info and plot phase by phase
    all_event_docs = db_event.get_all()
    phases_info = {}
    for doc in all_event_docs.values():
        phases_info[int(list(doc.keys())[0])] = list(doc.values())[0]
    list(phases_info.keys()).sort()

    num_phases = len(list(phases_info.keys()))

    run_id = store_report.get_run_info('name')

    if not os.path.exists("%s" % run_id):
        os.makedirs("%s" % run_id)
    else:
        shutil.rmtree("%s" % run_id)
        os.makedirs("%s" % run_id)

    for i in list(phases_info.keys()):
        start_time = phases_info[i][[name for name in list(phases_info[i].keys()) if (name != 'name' and name != 'desc')][0]]
        start_time = int(start_time[:10])
        end_time = 0
        if i == list(phases_info.keys())[-1]:
            end_time = str(time.time())
            end_time = int(end_time[:10])
        else:
            end_time = phases_info[i + 1][[name for name in list(phases_info[i + 1].keys()) if (name != 'name' and name != 'desc')][0]]
            end_time = int(end_time[:10])

        start_time_snapshot = datetime.datetime.fromtimestamp(start_time).strftime('%m/%d/%Y %H:%M')
        end_time_snapshot = datetime.datetime.fromtimestamp(end_time).strftime('%m/%d/%Y %H:%M')

        snapshot_name = "phase-%d-%s" % (i, [name for name in list(phases_info[i].keys()) if (name != 'name' and name != 'desc')][0])

        plot_use_cbmonitor(snapshot_name, cluster_name, start_time_snapshot, end_time_snapshot)

        prepare_folder_report(run_id, i)

        store_90th_avg_value(buckets, start_time, end_time, run_id, i)

    storage_folder = os.getcwd() + "/" + run_id + "/"
    print("data stored in %s" % (storage_folder))
    return storage_folder


def main():
    options, args = parse_args()
    buckets = args[1].split(",")
    plot_all_phases(args[0], buckets)

if __name__ == "__main__":
    main()
