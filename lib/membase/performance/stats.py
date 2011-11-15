import json
import os
from threading import Thread
import time
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from hashlib import md5

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

    def __init__(self, verbosity):
        self._verbosity = verbosity
        self.is_leader = False

    #this function starts collecting stats from all nodes with the given
    #frequency
    def start(self, nodes, bucket, pnames, name, frequency,
              collect_server_stats = True):
        self._task = {"state": "running", "threads": []}
        self._task["name"] = name
        self._task["time"] = time.time()
        self._task["ops"] = []
        self._task["totalops"] = []
        self._task["ops-temp"] = []
        self._task["latency"] = {}

        if collect_server_stats:
            mbstats_thread = Thread(target=self.membase_stats,
                                    args=(nodes, bucket, frequency, self._verbosity))
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
            self._task["threads"] = [mbstats_thread, sysstats_thread,
                                     ns_server_stats_thread, bucket_size_thead]
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
        commands = ['latency-set', 'latency-get']
        for latency in commands:
            histos = self._task["latency"].get(latency, [])
            key = 'percentile-' + latency
            self._task["latency"][key] = []
            for histo in histos:
                p = histo_percentile(histo, [0.90, 0.99])
                self._task["latency"][key].append(p)

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
               "timings": self._task.get("timings", []),
               "dispatcher": self._task.get("dispatcher", []),
               "bucket_size":self._task.get("bucket_size", []),
               "latency-set":self._task["latency"].get('percentile-latency-set', []),
               "latency-get":self._task["latency"].get('percentile-latency-get', [])
        }
        file = open("{0}.json".format(name), 'w')
        file.write("{0}".format(json.dumps(obj)))

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

    #ops stats
    #{'tot-sets': 899999, 'tot-gets': 1, 'tot-items': 899999, 'tot-creates': 899999}
    def ops_stats(self, ops_stat):
        ops_stat["time"] = time.time()
        self._task["ops-temp"].append(ops_stat)
        if len(self._task["ops-temp"]) >= 100:
            merged = self._merge()
            self._task["ops"].append(merged)
            self._task["ops-temp"] = []

        #if self._task["ops"] has more than 1000 elements try to aggregate them ?

    def latency_stats(self, latency_cmd, latency_stat):
        if self._task["latency"].get(latency_cmd) is None:
            self._task["latency"][latency_cmd] = []
        self._task["latency"][latency_cmd].append(latency_stat)

    def _merge(self):
        first = self._task["ops-temp"][0]
        merged = {"startTime":first["start-time"]}
        totalgets = 0
        totalsets = 0
        delta = 0
        i = 0
        for i in range(len(self._task["ops-temp"])-1):
            current = self._task["ops-temp"][i]
            next = self._task["ops-temp"][i+1]
            totalgets += current["tot-gets"]
            totalsets += current["tot-sets"]
            delta += (next["start-time"] - current["start-time"])
        merged["endTime"] = merged["startTime"] + delta
        merged["totalSets"] = totalsets
        merged["totalGets"] = totalgets
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
                unique_id = start_time
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
            time.sleep(frequency)
            for mc in mcs:
                stats = mc.stats()
                stats["time"] = time.time()
                stats["ip"] = mc.host
                d[mc.host]["snapshots"].append(stats)
                timings = mc.stats('timings')
                d[mc.host]["timings"].append(timings)
                dispatcher = mc.stats('dispatcher')
                d[mc.host]["dispatcher"].append(dispatcher)

        start_time = str(self._task["time"])
        for mc in mcs:
            unique_id = start_time
            ip = mc.host
            current_time = time.time()
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
        d = {}
        for node in nodes:
            d[node] = {"snapshots": [] }

        while not self._aborted():
            time.sleep(frequency)
            print "Collecting ns_server_stats"
            for node in nodes:
                f = os.popen("curl -X GET http://Administrator:password@{1}:8091/pools/{0}/buckets/{0}/stats?zoom=minute -o  ns_server_data".format(bucket, node.ip))
                f.close()
                dict  = open("./ns_server_data","r").read()
                data_json = json.loads(dict)
                d[node]["snapshots"].append(data_json)

        for node in nodes:
           for snapshot in d[node]["snapshots"]:
               self._task["ns_server_stats"].append(snapshot)

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
                callback(self, cur)

        return super(CallbackStatsCollector, self).sample(cur)

class StatUtil(object):

    @staticmethod
    def build_info(node):
        rest = RestConnection(node)
        api = rest.baseUrl + 'nodes/self'
        status, content = rest._http_request(api)
        json_parsed = json.loads(content)
        return json_parsed

    @staticmethod
    def machine_info(node):
        shell = RemoteMachineShellConnection(node)
        info = shell.extract_remote_info()
        return {"type": info.type, "distribution": info.distribution_type,
                "version": info.distribution_version, "ram": info.ram,
                "cpu": info.cpu, "disk": info.disk, "hostname":info.hostname}
