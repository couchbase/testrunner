import json
import os
import re
from threading import Thread
import time
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
import testconstants
import gzip
import urllib

from mc_bin_client import MemcachedError

RETRIES = 10
# The histo dict is returned by add_timing_sample().
# The percentiles must be sorted, ascending, like [0.90, 0.99].
def histo_percentile(histo, percentiles):
   v_sum = 0
   bins = histo.keys()
   bins.sort()
   for bin in bins:
      v_sum += histo[bin]
   v_sum = float(v_sum)
   v_cur = 0 # Running total.
   rv = []
   for bin in bins:
      if not percentiles:
         return rv
      v_cur += histo[bin]
      while percentiles and (v_cur / v_sum) >= percentiles[0]:
         rv.append((percentiles[0], bin))
         percentiles.pop(0)
   return rv

class StatsCollector(object):
    _task = {}
    _verbosity = True
    _mb_stats = {"snapshots": []}   # manually captured memcached stats
    _reb_stats = {}

    def __init__(self, verbosity):
        self._verbosity = verbosity
        self.is_leader = False
        self.active_mergers = 0

    #this function starts collecting stats from all nodes with the given
    #frequency
    def start(self, nodes, bucket, pnames, name, frequency, client_id='',
              collect_server_stats = True):
        self._task = {"state": "running", "threads": []}
        self._task["name"] = name
        self._task["time"] = time.time()
        self._task["ops"] = []
        self._task["totalops"] = []
        self._task["ops-temp"] = []
        self._task["latency"] = {}
        self._task["data_size_stats"] = []
        rest = RestConnection(nodes[0])
        info = rest.get_nodes_self()
        self.data_path = info.storage[0].get_data_path()
        self.client_id = str(client_id)

        if collect_server_stats:
            mbstats_thread = Thread(target=self.membase_stats,
                                    args=(nodes, bucket, 600, self._verbosity))
            mbstats_thread.start()
            sysstats_thread = Thread(target=self.system_stats,
                                     args=(nodes, pnames, frequency, self._verbosity))
            sysstats_thread.start()
            ns_server_stats_thread = Thread(target=self.ns_server_stats,
                                            args=([nodes[0]], bucket, 60, self._verbosity))
            ns_server_stats_thread.start()
            rest = RestConnection(nodes[0])
            bucket_size_thead = Thread(target=self.get_bucket_size,
                                       args=(bucket, rest, frequency))
            bucket_size_thead.start()
#            data_size_thread = Thread(target=self.get_data_file_size,
#                                    args=(nodes, 60, bucket))
#            data_size_thread.start()
            self._task["threads"] = [sysstats_thread, ns_server_stats_thread, bucket_size_thead,
                                     mbstats_thread]
                                     #data_size_thread ]
            # Getting build/machine stats from only one node in the cluster
            self.build_stats(nodes)
            self.machine_stats(nodes)

    def stop(self):
        self._task["state"] = "stopped"
        for t in self._task["threads"]:
            t.join()

        self._task["time"] = time.time() - self._task["time"]

    def sample(self, cur):
        pass

    def export(self, name, test_params):
        for latency in self._task["latency"].keys():

            # save the last histogram snapshot
            histos = self._task["latency"].get(latency, [])

            if histos:
                key = latency + "-histogram"
                self._task["latency"][key] = histos[-1].copy()
                del self._task["latency"][key]["delta"]
                self._task["latency"][key]["client_id"] = self.client_id

            # calculate percentiles
            key = 'percentile-' + latency
            self._task["latency"][key] = []
            for histo in histos:
                # for every sample histogram, produce a temp summary:
                # temp = [90 per, 95 per, 99 per, client_id, delta]
                temp = []
                time = histo['time']
                delta = histo['delta']
                del histo['delta'], histo['time']
                p = histo_percentile(histo, [0.80, 0.90, 0.95, 0.99, 0.999])
                # p is list of tuples
                for val in p:
                    temp.append(val[-1])
                temp.append(self.client_id)
                temp.append(time)
                temp.append(delta)
                self._task["latency"][key].append(temp)

        test_params.update(self._reb_stats)

        obj = {"buildinfo": self._task.get("buildstats", {}),
               "machineinfo": self._task.get("machinestats", {}),
               "membasestats": self._task.get("membasestats", []),
               "systemstats": self._task.get("systemstats", []),
               "name": name,
               "totalops":self._task["totalops"],
               "ops":self._task["ops"],
               "time": self._task["time"],
               "info": test_params,
               "ns_server_data": self._task.get("ns_server_stats", []),
               "ns_server_data_system": self._task.get("ns_server_stats_system", []),
               "timings": self._task.get("timings", []),
               "dispatcher": self._task.get("dispatcher", []),
               "bucket-size":self._task.get("bucket_size", []),
               "data-size": self._task.get("data_size_stats", []),
               "latency-set-histogram":self._task["latency"].get("latency-set-histogram", []),
               "latency-set":self._task["latency"].get('percentile-latency-set', []),
               "latency-set-recent":self._task["latency"].get('percentile-latency-set-recent', []),
               "latency-get-histogram":self._task["latency"].get("latency-get-histogram", []),
               "latency-get":self._task["latency"].get('percentile-latency-get', []),
               "latency-get-recent":self._task["latency"].get('percentile-latency-get-recent', []),
               "latency-delete":self._task["latency"].get('percentile-latency-delete', []),
               "latency-delete-recent":self._task["latency"].get('percentile-latency-delete-recent', []),
               "latency-query-histogram":self._task["latency"].get("latency-query-histogram", []),
               "latency-query":self._task["latency"].get('percentile-latency-query', []),
               "latency-query-recent":self._task["latency"].get('percentile-latency-query-recent', []),
               "latency-obs-persist-server-histogram":self._task["latency"].get("latency-obs-persist-server-histogram", []),
               "latency-obs-persist-server":self._task["latency"].get('percentile-latency-obs-persist-server-server', []),
               "latency-obs-persist-server-recent":self._task["latency"].get('percentile-latency-obs-persist-server-recent', []),
               "latency-obs-persist-client-histogram":self._task["latency"].get("latency-obs-persist-client-histogram", []),
               "latency-obs-persist-client":self._task["latency"].get('percentile-latency-obs-persist-client', []),
               "latency-obs-persist-client-recent":self._task["latency"].get('percentile-latency-obs-persist-client-recent', []),
               "latency-obs-repl-client-histogram":self._task["latency"].get("latency-obs-repl-client-histogram", []),
               "latency-obs-repl-client":self._task["latency"].get('percentile-latency-obs-repl-client', []),
               "latency-obs-repl-client-recent":self._task["latency"].get('percentile-latency-obs-repl-client-recent', []),
               "latency-woq-obs-histogram":self._task["latency"].get("latency-woq-obs-histogram", []),
               "latency-woq-obs":self._task["latency"].get('percentile-latency-woq-obs', []),
               "latency-woq-obs-recent":self._task["latency"].get('percentile-latency-woq-obs-recent', []),
               "latency-woq-query-histogram":self._task["latency"].get("latency-woq-query-histogram", []),
               "latency-woq-query":self._task["latency"].get('percentile-latency-woq-query', []),
               "latency-woq-query-recent":self._task["latency"].get('percentile-latency-woq-query-recent', []),
               "latency-woq-histogram":self._task["latency"].get("latency-woq-histogram", []),
               "latency-woq":self._task["latency"].get('percentile-latency-woq', []),
               "latency-woq-recent":self._task["latency"].get('percentile-latency-woq-recent', [])
               }

        if self.client_id:
            patterns = ['reload$', 'load$', 'warmup$', 'index$']
            phases = ['.reload', '.load', '.warmup', '.index']

            name_picker = lambda (pattern, phase): re.search(pattern, self._task["name"])
            try:
                phase = filter(name_picker, zip(patterns, phases))[0][1]
            except IndexError:
                phase = '.loop'

            filename = str(self.client_id) + phase

            file = gzip.open("{0}.json.gz".format(filename), 'wb')
            file.write("{0}".format(json.dumps(obj)))
            file.close()
        else:
            file = gzip.open("{0}.json.gz".format(name), 'wb')
            file.write("{0}".format(json.dumps(obj)))
            file.close()

    def get_bucket_size(self, bucket, rest, frequency):
        self._task["bucket_size"] = []
        d = []
        while not self._aborted():
            print "Collecting bucket size stats"
            status, db_size = rest.get_database_disk_size(bucket)
            if status:
                d.append(db_size)
            else:
                print "Enable to read bucket stats"
            time.sleep(frequency)

        self._task["bucket_size"] = d
        print "finished bucket size stats"

    def get_data_file_size(self, nodes, frequency, bucket):
        shells = []
        for node in nodes:
            try:
                shells.append(RemoteMachineShellConnection(node))
            except:
                pass
        paths = []
        if shells[0].is_membase_installed():
            paths.append(self.data_path+'/{0}-data'.format(bucket))
        else:
            bucket_path = self.data_path+'/{0}'.format(bucket)
            paths.append(bucket_path)
            view_path = bucket_path +'/set_view_{0}_design'.format(bucket)
            paths.append(view_path)

        d = {"snapshots": []}
        start_time = str(self._task["time"])

        while not self._aborted():
            time.sleep(frequency)
            current_time = time.time()
            i = 0
            for shell in shells:
                node = nodes[i]
                unique_id = node.ip+'-'+start_time
                value = {}
                for path in paths:
                    size = shell.get_data_file_size(path)
                    value["file"] = path.split('/')[-1]
                    value["size"] = size
                    value["unique_id"] = unique_id
                    value["time"] = current_time
                    value["ip"] = node.ip
                    d["snapshots"].append(value.copy())
                i +=  1
        self._task["data_size_stats"] = d["snapshots"]
        print " finished data_size_stats"

    #ops stats
    #{'tot-sets': 899999, 'tot-gets': 1, 'tot-items': 899999, 'tot-creates': 899999}
    def ops_stats(self, ops_stat):
        ops_stat["time"] = time.time()
        self._task["ops-temp"].append(ops_stat)
        if len(self._task["ops-temp"]) >= 500 * (1 + self.active_mergers):
            # Prevent concurrent merge
            while self.active_mergers:
                time.sleep(0.1)

            # Semaphore: +1 active
            self.active_mergers += 1

            # Merge
            merged = self._merge()
            self._task["ops"].append(merged)
            self._task["ops-temp"] = self._task["ops-temp"][500:]

            # Semaphore: -1 active
            self.active_mergers -= 1

        #if self._task["ops"] has more than 1000 elements try to aggregate them ?

    def latency_stats(self, latency_cmd, latency_stat, cur_time=0):
        if self._task["latency"].get(latency_cmd) is None:
            self._task["latency"][latency_cmd] = []
        temp_latency_stat = latency_stat.copy()
        if not cur_time:
            cur_time = time.time()
        temp_latency_stat['time'] = int(cur_time)
        temp_latency_stat['delta'] = cur_time - self._task['time']
        self._task["latency"][latency_cmd].append(temp_latency_stat)

    def _merge(self):
        first = self._task["ops-temp"][0]
        merged = {"startTime":first["start-time"]}
        totalgets = 0
        totalsets = 0
        totalqueries = 0
        delta = 0
        for i in range(499):
            current = self._task["ops-temp"][i]
            next = self._task["ops-temp"][i+1]
            totalgets += current["tot-gets"]
            totalsets += current["tot-sets"]
            totalqueries += current["tot-queries"]
            delta += (next["start-time"] - current["start-time"])
        merged["endTime"] = merged["startTime"] + delta
        merged["totalSets"] = totalsets
        merged["totalGets"] = totalgets
        merged["totalQueries"] = totalqueries
        qps = totalqueries / float(delta)
        merged["queriesPerSec"] = qps
        return merged

    def total_stats(self, ops_stat):
        ops_stat["time"] = time.time()
        self._task["totalops"].append(ops_stat)

    def build_stats(self,nodes):
        json_response = StatUtil.build_info(nodes[0])
        self._task["buildstats"] = json_response

    def machine_stats(self,nodes):
        machine_stats = StatUtil.machine_info(nodes[0])
        self._task["machinestats"] = machine_stats

    def reb_stats(self, start, dur):
        print "[reb_stats] recording reb start = {0}, reb duration = {1}"\
            .format(start, dur)
        self._reb_stats["reb_start"] = start
        self._reb_stats["reb_dur"] = dur

    def _extract_proc_info(self, shell, pid):
        o, r = shell.execute_command("cat /proc/{0}/stat".format(pid))
        fields = ('pid comm state ppid pgrp session tty_nr tpgid flags minflt '
                  'cminflt majflt cmajflt utime stime cutime cstime priority '
                  'nice num_threads itrealvalue starttime vsize rss rsslim '
                  'startcode endcode startstack kstkesp kstkeip signal blocked '
                  'sigignore sigcatch wchan nswap cnswap exit_signal '
                  'processor rt_priority policy delayacct_blkio_ticks '
                  'guest_time cguest_time ').split(' ')

        d = dict(zip(fields, o[0].split(' ')))
        return d

    def system_stats(self, nodes, pnames, frequency, verbosity=False):
        shells = []
        for node in nodes:
            try:
                bucket = RestConnection(node).get_buckets()[0].name
                MemcachedClientHelper.direct_client(node, bucket)
                shells.append(RemoteMachineShellConnection(node))
            except:
                pass
        d = {"snapshots": []}
        #        "pname":"x","pid":"y","snapshots":[{"time":time,"value":value}]

        start_time = str(self._task["time"])
        while not self._aborted():
            time.sleep(frequency)
            current_time = time.time()
            i = 0
            for shell in shells:
                node = nodes[i]
                unique_id = node.ip+'-'+start_time
                for pname in pnames:
                    obj = RemoteMachineHelper(shell).is_process_running(pname)
                    if obj and obj.pid:
                        value = self._extract_proc_info(shell, obj.pid)
                        value["name"] = pname
                        value["id"] = obj.pid
                        value["unique_id"] = unique_id
                        value["time"] = current_time
                        value["ip"] = node.ip
                        d["snapshots"].append(value)
                i +=  1
        self._task["systemstats"] = d["snapshots"]
        print " finished system_stats"

    def couchdb_stats(nodes):
        pass

    def capture_mb_snapshot(self, node):
        """
        Capture membase stats snapshot manually
        """
        print "[capture_mb_snapshot] capturing memcache stats snapshot for {0}"\
            .format(node.ip)
        stats = {}

        try:
            bucket = RestConnection(node).get_buckets()[0].name
            mc = MemcachedClientHelper.direct_client(node, bucket)
            stats = mc.stats()
        except Exception as e:
            print "[capture_mb_snapshot] Exception: {0}".format(str(e))
            return False

        stats["time"] = time.time()
        stats["ip"] = node.ip
        self._mb_stats["snapshots"].append(stats)

        print stats
        print "[capture_mb_snapshot] memcache stats snapshot captured"
        return True

    def membase_stats(self,nodes, bucket, frequency, verbose=False):
        mcs = []
        for node in nodes:
            try:
                bucket = RestConnection(node).get_buckets()[0].name
                mcs.append(MemcachedClientHelper.direct_client(node, bucket))
            except:
                pass
        self._task["membasestats"] = []
        self._task["timings"] = []
        self._task["dispatcher"] = []
        d = {}
        #        "pname":"x","pid":"y","snapshots":[{"time":time,"value":value}]
        for mc in mcs:
            d[mc.host] = {"snapshots": [], "timings":[], "dispatcher":[]}

        while not self._aborted():
            time_left = frequency
            timings = None
            # at minimum we want to check for aborted every minute
            while not self._aborted() and time_left > 0:
                time.sleep(min(time_left, 60))
                time_left -= 60
            for mc in mcs:
                retries = 0
                stats = {}
                while not stats and retries < RETRIES:
                    try:
                        stats = mc.stats()
                        try:
                            mem_stats = mc.stats('raw memory')
                        except MemcachedError:
                            mem_stats = mc.stats('memory')
                        stats.update(mem_stats)
                    except Exception as e:
                        print "[memebase_stats] Exception: {0}, retries = {1}"\
                            .format(str(e), retries)
                        time.sleep(2)
                        mc.reconnect()
                        retries += 1
                        continue
                stats["time"] = time.time()
                stats["ip"] = mc.host
                d[mc.host]["snapshots"].append(stats)
                timings = mc.stats('timings')
                d[mc.host]["timings"].append(timings)
                dispatcher = mc.stats('dispatcher')
                d[mc.host]["dispatcher"].append(dispatcher)
            print "\nDumping disk timing stats: {0}".format(time.strftime('%X %x %Z'))
            if timings:
                # TODO dump timings for all servers
                for key, value in sorted(timings.iteritems()):
                    if key.startswith("disk"):
                        print "{0:50s}:     {1}".format(key, value)

        start_time = str(self._task["time"])
        for mc in mcs:
            ip = mc.host
            unique_id = ip+'-'+start_time
            current_time = time.time()
            if self._mb_stats["snapshots"]:
                # use manually captured stats
                self._task["membasestats"] = self._mb_stats["snapshots"]
            else:
                # use periodically captured stats
                for snapshot in d[mc.host]["snapshots"]:
                    snapshot['unique_id'] = unique_id
                    snapshot['time'] = current_time
                    snapshot['ip'] = ip
                    self._task["membasestats"].append(snapshot)
            for timing in d[mc.host]["timings"]:
                timing['unique_id'] = unique_id
                timing['time'] = current_time
                timing['ip'] = ip
                self._task["timings"].append(timing)
            for dispatcher in d[mc.host]["dispatcher"]:
                dispatcher['unique_id'] = unique_id
                dispatcher['time'] = current_time
                dispatcher['ip'] = ip
                self._task["dispatcher"].append(dispatcher)

        print " finished membase_stats"

    def ns_server_stats(self, nodes, bucket, frequency, verbose=False):

        self._task["ns_server_stats"] = []
        self._task["ns_server_stats_system"] = []
        d = {}
        for node in nodes:
            d[node] = {"snapshots": [], "system_snapshots": [] }

        while not self._aborted():
            time.sleep(frequency)
            print "Collecting ns_server_stats"
            for node in nodes:
                rest = RestConnection(node)
                data_json = rest.fetch_bucket_stats(bucket=bucket, zoom='minute')
                d[node]["snapshots"].append(data_json)

                data_json = rest.fetch_system_stats()
                d[node]["system_snapshots"].append(data_json)

        for node in nodes:
           for snapshot in d[node]["snapshots"]:
               self._task["ns_server_stats"].append(snapshot)
           for snapshot in d[node]["system_snapshots"]:
               self._task["ns_server_stats_system"].append(snapshot)

        print " finished ns_server_stats"

    def _aborted(self):
        return self._task["state"] == "stopped"

# Invokes optional callback when registered levels have been reached
# during stats sample()'ing.
#
class CallbackStatsCollector(StatsCollector):

    def __init__(self, verbosity):
        # Tuples of level_name, level, callback.
        self.level_callbacks = []
        super(CallbackStatsCollector, self).__init__(verbosity)

    def sample(self, cur):
        for level_name, level, callback in self.level_callbacks:
            if level < cur.get(level_name, -1):
                callback(cur)

        return super(CallbackStatsCollector, self).sample(cur)

class StatUtil(object):

    @staticmethod
    def build_info(node):
        rest = RestConnection(node)
        api = rest.baseUrl + 'nodes/self'
        status, content, header = rest._http_request(api)
        json_parsed = json.loads(content)
        return json_parsed

    @staticmethod
    def machine_info(node):
        shell = RemoteMachineShellConnection(node)
        info = shell.extract_remote_info()
        return {"type": info.type, "distribution": info.distribution_type,
                "version": info.distribution_version, "ram": info.ram,
                "cpu": info.cpu, "disk": info.disk, "hostname":info.hostname}
