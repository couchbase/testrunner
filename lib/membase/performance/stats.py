import json
import os
from threading import Thread
import time
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper

class StatsCollector(object):
    _task = {}
    _verbosity = True

    def __init__(self, verbosity):
        self._verbosity = verbosity

    #this function starts collecting stats from all nodes with the given
    #frequency
    def start(self, nodes, bucket, pnames, name, frequency):
        self._task = {"state": "running"}
        mbstats_thread = Thread(target=self.membase_stats,args=(nodes, bucket, frequency, self._verbosity))
        mbstats_thread.start()
        sysstats_thread = Thread(target=self.system_stats,args=(nodes, pnames, frequency, self._verbosity))
        sysstats_thread.start()
        ns_server_stats_thread = Thread(target=self.ns_server_stats,args=(nodes, bucket, 60, self._verbosity))
        ns_server_stats_thread.start()
        self._task["threads"] = [mbstats_thread, sysstats_thread, ns_server_stats_thread]
        self._task["name"] = name
        self._task["time"] = time.time()
        self._task["ops"] = []
        self._task["totalops"] = []
        self._task["ops-temp"] = []
        self.build_stats(nodes)
        #create multiple threds for each one required. kick them off. assign an id to each.

    def stop(self):
        self._task["state"] = "stopped"
        for t in self._task["threads"]:
#            print "waiting for thread : {0}".format(t)
            t.join()

        self._task["time"] = time.time() - self._task["time"]

    def export(self, name, test_params):

        obj = {"buildinfo": self._task["buildstats"],
               "membasestats": self._task["membasestats"],
               "systemstats": self._task["systemstats"],
               "name": name,
               "totalops":self._task["totalops"],
               "ops":self._task["ops"],
               "time": self._task["time"],
               "info": test_params,
               "ns_server_data": self._task["ns_server_stats"] }
        file = open("{0}.json".format(name), 'w')
        file.write("{0}".format(json.dumps(obj)))


    #ops stats
    #{'cur-sets': 899999, 'cur-gets': 1, 'cur-items': 899999, 'cur-creates': 899999}
    def ops_stats(self, ops_stat):
        ops_stat["time"] = time.time()
        self._task["ops-temp"].append(ops_stat)
        if len(self._task["ops-temp"]) >= 100:
            merged = self._merge()
            self._task["ops"].append(merged)
            self._task["ops-temp"] = []

        #if self._task["ops"] has more than 1000 elements try to aggregate them ?

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
        machine_stats = [StatUtil.machine_info(node) for node in nodes]
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
        while not self._aborted():
            time.sleep(frequency)
            for shell in shells:
                for pname in pnames:
                    obj = RemoteMachineHelper(shell).is_process_running(pname)
                    if obj and obj.pid:
                        value = self._extract_proc_info(shell, obj.pid)
                        value["time"] = time.time()
                        value["name"] = pname
                        value["id"] = obj.pid
                        d["snapshots"].append(value)
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
        d = {}
        #        "pname":"x","pid":"y","snapshots":[{"time":time,"value":value}]
        for mc in mcs:
            d[mc.host] = {"snapshots": []}


        while not self._aborted():
            time.sleep(frequency)
            for mc in mcs:
                stats = mc.stats()
                stats["time"] = time.time()
                stats["ip"] = mc.host
                d[mc.host]["snapshots"].append(stats)

        for mc in mcs:
            for snapshot in d[mc.host]["snapshots"]:
                self._task["membasestats"].append(snapshot)

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
                dict  = open("./ns_server_data","r").read()
                data_json = json.loads(dict)
                d[node]["snapshots"].append(data_json)
                f.close()

        for node in nodes:
           for snapshot in d[node]["snapshots"]:
               self._task["ns_server_stats"].append(snapshot)

        print " finished ns_server_stats"

    def _aborted(self):
        return self._task["state"] == "stopped"

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
                "version": info.distribution_version, "packaging": info.deliverable_type}
