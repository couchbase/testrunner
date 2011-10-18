import json
from threading import Thread
import time
import uuid
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper

class StatsCollector(object):
    _tasks = {}
    _verbosity = True

    def __init__(self, verbosity):
        self._verbosity = verbosity

    #this function starts collecting stats from all nodes with the given
    #frequency
    def start(self, nodes, bucket, pnames, name, frequency):
        id = str(uuid.uuid4())
        self._tasks[id] = {"state": "running"}
        mbstats_thread = Thread(target=self.membase_stats,args=(id, nodes, bucket, frequency, self._verbosity))
        mbstats_thread.start()
        sysstats_thread = Thread(target=self.system_stats,args=(id, nodes, pnames, frequency, self._verbosity))
        sysstats_thread.start()
        self._tasks[id]["threads"] = [mbstats_thread, sysstats_thread]
        self._tasks[id]["name"] = name
        self._tasks[id]["time"] = time.time()
        self._tasks[id]["ops"] = []
        self.build_stats(id, nodes)
        return id
        #create multiple threds for each one required. kick them off. assign an id to each.

    def stop(self, id):
        if id in self._tasks:
            self._tasks[id]["state"] = "stopped"
        for t in self._tasks[id]["threads"]:
            print "waiting for thread : {0}".format(t)
            t.join()
        self._tasks[id]["time"] = time.time() - self._tasks[id]["time"]


    def export(self, id, name, type="file"):
        if id in self._tasks:
            obj = {"buildinfo": self._tasks[id]["buildstats"],
                   "membasestats": self._tasks[id]["membasestats"],
                   "systemstats": self._tasks[id]["systemstats"],
                   "name": name,
                   "ops":self._tasks[id]["ops"],
                   "time": self._tasks[id]["time"]}
            file = open("{0}.json".format(name), 'w')
            file.write("{0}".format(json.dumps(obj)))


    #ops stats
    #{'cur-sets': 899999, 'cur-gets': 1, 'cur-items': 899999, 'cur-creates': 899999}
    def _ops_stats(self,ops_stat):
        ops_stat["time"] = time.time()
        self._tasks[id]["ops"].append(ops_stat)

    def build_stats(self,id,nodes):
        json_response = StatUtil.build_info(nodes[0])
        self._tasks[id]["buildstats"] = json_response

    def machine_stats(self,id,nodes):
        machine_stats = [StatUtil.machine_info(node) for node in nodes]
        self._tasks[id]["machinestats"] = machine_stats


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


    def system_stats(self, id, nodes, pnames, frequency, verbosity=False):
        shells = [RemoteMachineShellConnection(node) for node in nodes]
        d = {"snapshots": []}
        #        "pname":"x","pid":"y","snapshots":[{"time":time,"value":value}]
        while not self._aborted(id):
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
        self._tasks[id]["systemstats"] = d["snapshots"]
        print " finished system_stats"


    def couchdb_stats(nodes):
        pass

    def membase_stats(self, id, nodes, bucket, frequency, verbose=False):
        mcs = [MemcachedClientHelper.direct_client(node, bucket) for node in nodes]
        self._tasks[id]["membasestats"] = []
        d = {}
        #        "pname":"x","pid":"y","snapshots":[{"time":time,"value":value}]
        for mc in mcs:
            d[mc.host] = {"snapshots": []}


        while not self._aborted(id):
            time.sleep(frequency)
            for mc in mcs:
                stats = mc.stats()
                stats["time"] = time.time()
                stats["ip"] = mc.host
                d[mc.host]["snapshots"].append(stats)

        for mc in mcs:
            for snapshot in d[mc.host]["snapshots"]:
                print "wtf",snapshot
                self._tasks[id]["membasestats"].append(snapshot)

        print " finished membase_stats"

    def _aborted(self, id):
        if id in self._tasks:
            print self._tasks[id]["state"] == "stopped",self._tasks[id]["state"]
            return self._tasks[id]["state"] == "stopped"

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
