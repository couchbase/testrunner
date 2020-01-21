import json
import re
from threading import Thread
import time
import gzip
from collections import defaultdict
import logging
import logging.config
from uuid import uuid4
from itertools import cycle

from lib.membase.api.exception import SetViewInfoNotFound, ServerUnavailableException
from lib.membase.api.rest_client import RestConnection
from lib.memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached
from lib.remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper


RETRIES = 10

logging.config.fileConfig('mcsoda.logging.conf')
logging.getLogger("paramiko").setLevel(logging.WARNING)
log = logging.getLogger()

hex = lambda: uuid4().hex


def histo_percentile(histo, percentiles):
    """The histo dict is returned by add_timing_sample(). The percentiles must
    be sorted, ascending, like [0.90, 0.99]."""
    v_sum = 0
    bins = sorted(list(histo.keys()))
    for bin in bins:
        v_sum += histo[bin]
    v_sum = float(v_sum)
    v_cur = 0  # Running total.
    rv = []
    for bin in bins:
        if not percentiles:
            return rv
        v_cur += histo[bin]
        while percentiles and (v_cur // v_sum) >= percentiles[0]:
            rv.append((percentiles[0], bin))
            percentiles.pop(0)
    return rv


class StatsCollector(object):
    _task = {}
    _verbosity = True
    _mb_stats = {"snapshots": []}  # manually captured memcached stats
    _reb_stats = {}
    _lat_avg_stats = {}     # aggregated top level latency stats
    _xdcr_stats = {}

    def __init__(self, verbosity):
        self._verbosity = verbosity
        self.is_leader = False
        self.active_mergers = 0

    def start(self, nodes, bucket, pnames, name, client_id='',
              collect_server_stats=True, ddoc=None, clusters=None):
        """This function starts collecting stats from all nodes with the given
        interval"""
        self._task = {"state": "running", "threads": [], "name": name,
                      "time": time.time(), "ops": [], "totalops": [],
                      "ops-temp": [], "latency": {}, "data_size_stats": []}
        rest = RestConnection(nodes[0])
        info = rest.get_nodes_self()
        self.data_path = info.storage[0].get_data_path()
        self.client_id = str(client_id)
        self.nodes = nodes
        self.bucket = bucket

        if collect_server_stats:
            self._task["threads"].append(
                Thread(target=self.membase_stats, name="membase")
            )
            self._task["threads"].append(
                Thread(target=self.system_stats, name="system", args=(pnames, ))
            )
            self._task["threads"].append(
                Thread(target=self.iostats, name="iostats")
            )
            self._task["threads"].append(
                Thread(target=self.ns_server_stats, name="ns_server")
            )
            self._task["threads"].append(
                Thread(target=self.get_bucket_size, name="bucket_size")
            )
            self._task["threads"].append(
                Thread(target=self.rebalance_progress, name="rebalance_progress")
            )
            if ddoc is not None:
                self._task["threads"].append(
                    Thread(target=self.indexing_time_stats, name="index_time", args=(ddoc, ))
                )
                self._task["threads"].append(
                    Thread(target=self.indexing_throughput_stats, name="index_thr")
                )
            if clusters:
                self.clusters = clusters
                self._task["threads"].append(
                    Thread(target=self.xdcr_lag_stats, name="xdcr_lag_stats")
                )

            for thread in self._task["threads"]:
                thread.daemon = True
                thread.start()

            # Getting build/machine stats from only one node in the cluster
            self.build_stats(nodes)
            self.machine_stats(nodes)

            # Start atop
            self.start_atop()

    def stop(self):
        self.stop_atop()
        self._task["state"] = "stopped"
        for t in self._task["threads"]:
            t.join(120)
            if t.is_alive():
                log.error("failed to join {0} thread".format(t.name))

        self._task["time"] = time.time() - self._task["time"]

    def sample(self, cur):
        pass

    def get_ns_servers_samples(self, metric):
        for subset in self._task["ns_server_data"]:
            samples = subset["op"]["samples"][metric]
            yield float(sum(samples)) / len(samples)

    def calc_xperf_stats(self):
        metrics = ("replication_changes_left", "xdc_ops")
        for metric in metrics:
            self._xdcr_stats["avg_" + metric] = \
                sum(self.get_ns_servers_samples(metric)) /\
                sum(1 for _ in self.get_ns_servers_samples(metric))

    def export(self, name, test_params):
        for latency in list(self._task["latency"].keys()):
            # save the last histogram snapshot
            per_90th_tot = 0
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
                per_90th_tot += temp[1]
                temp.append(self.client_id)
                temp.append(time)
                temp.append(delta)
                self._task["latency"][key].append(temp)
            if per_90th_tot:
                self._lat_avg_stats["%s-90th-avg" % latency] \
                    = per_90th_tot // len(histos) * 1000000

        # XDCR stats
        try:
            self.calc_xperf_stats()
        except KeyError:
            pass

        test_params.update(self._xdcr_stats)
        test_params.update(self._reb_stats)
        test_params.update(self._lat_avg_stats)

        obj = {
            "buildinfo": self._task.get("buildstats", {}),
            "machineinfo": self._task.get("machinestats", {}),
            "membasestats": self._task.get("membasestats", []),
            "systemstats": self._task.get("systemstats", []),
            "iostats": self._task.get("iostats", []),
            "name": name,
            "totalops": self._task["totalops"],
            "ops": self._task["ops"],
            "time": self._task["time"],
            "info": test_params,
            "ns_server_data": self._task.get("ns_server_stats", []),
            "ns_server_data_system": self._task.get("ns_server_stats_system", []),
            "view_info": self._task.get("view_info", []),
            "indexer_info": self._task.get("indexer_info", []),
            "xdcr_lag": self._task.get("xdcr_lag", []),
            "rebalance_progress": self._task.get("rebalance_progress", []),
            "timings": self._task.get("timings", []),
            "dispatcher": self._task.get("dispatcher", []),
            "bucket-size": self._task.get("bucket_size", []),
            "data-size": self._task.get("data_size_stats", []),
            "latency-set-histogram": self._task["latency"].get("latency-set-histogram", []),
            "latency-set": self._task["latency"].get('percentile-latency-set', []),
            "latency-set-recent": self._task["latency"].get('percentile-latency-set-recent', []),
            "latency-get-histogram": self._task["latency"].get("latency-get-histogram", []),
            "latency-get": self._task["latency"].get('percentile-latency-get', []),
            "latency-get-recent": self._task["latency"].get('percentile-latency-get-recent', []),
            "latency-delete": self._task["latency"].get('percentile-latency-delete', []),
            "latency-delete-recent": self._task["latency"].get('percentile-latency-delete-recent', []),
            "latency-query-histogram": self._task["latency"].get("latency-query-histogram", []),
            "latency-query": self._task["latency"].get('percentile-latency-query', []),
            "latency-query-recent": self._task["latency"].get('percentile-latency-query-recent', []),
            "latency-obs-persist-server-histogram": self._task["latency"].get("latency-obs-persist-server-histogram", []),
            "latency-obs-persist-server": self._task["latency"].get('percentile-latency-obs-persist-server-server', []),
            "latency-obs-persist-server-recent": self._task["latency"].get('percentile-latency-obs-persist-server-recent', []),
            "latency-obs-persist-client-histogram": self._task["latency"].get("latency-obs-persist-client-histogram", []),
            "latency-obs-persist-client": self._task["latency"].get('percentile-latency-obs-persist-client', []),
            "latency-obs-persist-client-recent": self._task["latency"].get('percentile-latency-obs-persist-client-recent', []),
            "latency-obs-repl-client-histogram": self._task["latency"].get("latency-obs-repl-client-histogram", []),
            "latency-obs-repl-client": self._task["latency"].get('percentile-latency-obs-repl-client', []),
            "latency-obs-repl-client-recent": self._task["latency"].get('percentile-latency-obs-repl-client-recent', []),
            "latency-woq-obs-histogram": self._task["latency"].get("latency-woq-obs-histogram", []),
            "latency-woq-obs": self._task["latency"].get('percentile-latency-woq-obs', []),
            "latency-woq-obs-recent": self._task["latency"].get('percentile-latency-woq-obs-recent', []),
            "latency-woq-query-histogram": self._task["latency"].get("latency-woq-query-histogram", []),
            "latency-woq-query": self._task["latency"].get('percentile-latency-woq-query', []),
            "latency-woq-query-recent": self._task["latency"].get('percentile-latency-woq-query-recent', []),
            "latency-woq-histogram": self._task["latency"].get("latency-woq-histogram", []),
            "latency-woq": self._task["latency"].get('percentile-latency-woq', []),
            "latency-woq-recent": self._task["latency"].get('percentile-latency-woq-recent', []),
            "latency-cor-histogram": self._task["latency"].get("latency-cor-histogram", []),
            "latency-cor": self._task["latency"].get('percentile-latency-cor', []),
            "latency-cor-recent": self._task["latency"].get('percentile-latency-cor-recent', [])}

        if self.client_id:
            patterns = ('reload$', 'load$', 'warmup$', 'index$')
            phases = ('.reload', '.load', '.warmup', '.index')
            name_picker = lambda pattern_phase: re.search(pattern_phase[0], self._task["name"])
            try:
                phase = list(filter(name_picker, list(zip(patterns, phases))))[0][1]
            except IndexError:
                phase = '.loop'
            name = str(self.client_id) + phase

        file = gzip.open("{0}.json.gz".format(name), 'wb')
        file.write(json.dumps(obj))
        file.close()

    def get_bucket_size(self, interval=60):
        self._task["bucket_size"] = []
        retries = 0
        nodes_iterator = (node for node in self.nodes)
        node = next(nodes_iterator)
        rest = RestConnection(node)
        while not self._aborted():
            time.sleep(interval)
            log.info("collecting bucket size stats")
            try:
                status, db_size = rest.get_database_disk_size(self.bucket)
                if status:
                    self._task["bucket_size"].append(db_size)
            except IndexError as e:
                retries += 1
                log.error("unable to get bucket size {0}: {1}"
                          .format(self.bucket, e))
                log.warning("retries: {0} of {1}".format(retries, RETRIES))
                if retries == RETRIES:
                    try:
                        node = next(nodes_iterator)
                        rest = RestConnection(node)
                        retries = 0
                    except StopIteration:
                        log.error("no nodes available: stop collecting bucket_size")
                        return

        log.info("finished bucket size stats")

    def get_data_file_size(self, nodes, interval, bucket):
        shells = []
        for node in nodes:
            try:
                shells.append(RemoteMachineShellConnection(node))
            except Exception as error:
                log.error(error)
        paths = []
        if shells[0].is_couchbase_installed():
            bucket_path = self.data_path + '/{0}'.format(bucket)
            paths.append(bucket_path)
            view_path = bucket_path + '/set_view_{0}_design'.format(bucket)
            paths.append(view_path)
        else:
            paths.append(self.data_path + '/{0}-data'.format(bucket))

        d = {"snapshots": []}
        start_time = str(self._task["time"])

        while not self._aborted():
            time.sleep(interval)
            current_time = time.time()
            i = 0
            for shell in shells:
                node = nodes[i]
                unique_id = node.ip + '-' + start_time
                value = {}
                for path in paths:
                    size = shell.get_data_file_size(path)
                    value["file"] = path.split('/')[-1]
                    value["size"] = size
                    value["unique_id"] = unique_id
                    value["time"] = current_time
                    value["ip"] = node.ip
                    d["snapshots"].append(value.copy())
                i += 1
        self._task["data_size_stats"] = d["snapshots"]
        log.info("finished data_size_stats")

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
        merged = {"startTime": first["start-time"]}
        totalgets = 0
        totalsets = 0
        totalqueries = 0
        delta = 0
        for i in range(499):
            current = self._task["ops-temp"][i]
            next = self._task["ops-temp"][i + 1]
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

    def build_stats(self, nodes):
        json_response = StatUtil.build_info(nodes[0])
        self._task["buildstats"] = json_response

    def machine_stats(self, nodes):
        machine_stats = StatUtil.machine_info(nodes[0])
        self._task["machinestats"] = machine_stats

    def reb_stats(self, start, dur):
        log.info("recording reb start = {0}, reb duration = {1}".format(start, dur))
        self._reb_stats["reb_start"] = start
        self._reb_stats["reb_dur"] = dur

    def _extract_proc_info(self, shell, pid):
        output, error = shell.execute_command("cat /proc/{0}/stat".format(pid))
        fields = (
            'pid', 'comm', 'state', 'ppid', 'pgrp', 'session', 'tty_nr',
            'tpgid', 'flags', 'minflt', 'cminflt', 'majflt', 'cmajflt',
            'utime', 'stime', 'cutime', 'cstime', 'priority ' 'nice',
            'num_threads', 'itrealvalue', 'starttime', 'vsize', 'rss',
            'rsslim', 'startcode', 'endcode', 'startstack', 'kstkesp',
            'kstkeip', 'signal', 'blocked ', 'sigignore', 'sigcatch', 'wchan',
            'nswap', 'cnswap', 'exit_signal', 'processor', 'rt_priority',
            'policy', 'delayacct_blkio_ticks', 'guest_time', 'cguest_time')

        return {} if error else dict(list(zip(fields, output[0].split(' '))))

    def _extract_io_info(self, shell):
        """
        Extract info from iostat

        Output:

        [kB_read, kB_wrtn, %util, %iowait, %idle]

        Rough Benchmarks:
        My local box (WIFI LAN - VM), took ~1.2 sec for this routine
        """
        CMD = "iostat -dk | grep 'sd. ' | " \
              "awk '{read+=$5; write+=$6} END { print read, write }'"
        out, err = shell.execute_command(CMD)
        results = out[0]

        CMD = "iostat -dkx | grep 'sd. ' | "\
              "awk '{util+=$12} END { print util/NR }'"
        out, err = shell.execute_command(CMD)
        results = "%s %s" % (results, out[0])

        CMD = "iostat 1 2 -c | awk 'NR == 7 { print $4, $6 }'"
        out, err = shell.execute_command(CMD)
        results = "%s %s" % (results, out[0])

        return results.split(' ')

    def system_stats(self, pnames, interval=10):
        shells = []
        for node in self.nodes:
            try:
                shells.append(RemoteMachineShellConnection(node))
            except Exception as error:
                log.error(error)
        d = {"snapshots": []}
        #        "pname":"x","pid":"y","snapshots":[{"time":time,"value":value}]

        start_time = str(self._task["time"])
        while not self._aborted():
            time.sleep(interval)
            current_time = time.time()
            i = 0
            for shell in shells:
                node = self.nodes[i]
                unique_id = node.ip + '-' + start_time
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
                i += 1
        self._task["systemstats"] = d["snapshots"]
        log.info("finished system_stats")

    def iostats(self, interval=10):
        shells = []
        for node in self.nodes:
            try:
                shells.append(RemoteMachineShellConnection(node))
            except Exception as error:
                log.error(error)

        self._task["iostats"] = []

        log.info("started capturing io stats")

        while not self._aborted():
            time.sleep(interval)
            log.info("collecting io stats")
            for shell in shells:
                try:
                    kB_read, kB_wrtn, util, iowait, idle = \
                        self._extract_io_info(shell)
                except (ValueError, TypeError, IndexError):
                    continue
                if kB_read and kB_wrtn:
                    self._task["iostats"].append({"time": time.time(),
                                                 "ip": shell.ip,
                                                 "read": kB_read,
                                                 "write": kB_wrtn,
                                                 "util": util,
                                                 "iowait": iowait,
                                                 "idle": idle})
        log.info("finished capturing io stats")

    def capture_mb_snapshot(self, node):
        """Capture membase stats snapshot manually"""
        log.info("capturing memcache stats snapshot for {0}".format(node.ip))
        stats = {}

        try:
            bucket = RestConnection(node).get_buckets()[0].name
            mc = MemcachedClientHelper.direct_client(node, bucket)
            stats = mc.stats()
            stats.update(mc.stats("warmup"))
        except Exception as e:
            log.error(e)
            return False
        finally:
            stats["time"] = time.time()
            stats["ip"] = node.ip
            self._mb_stats["snapshots"].append(stats)
            print(stats)

        log.info("memcache stats snapshot captured")
        return True

    def membase_stats(self, interval=60):
        mcs = []
        for node in self.nodes:
            try:
                bucket = RestConnection(node).get_buckets()[0].name
                mc = MemcachedClientHelper.direct_client(node, bucket)
                mcs.append(mc)
            except Exception as error:
                log.error(error)
        self._task["membasestats"] = []
        self._task["timings"] = []
        self._task["dispatcher"] = []
        data = dict()
        for mc in mcs:
            data[mc.host] = {"snapshots": [], "timings": [], "dispatcher": []}

        while not self._aborted():
            time.sleep(interval)
            log.info("collecting membase stats")
            for mc in mcs:
                for rerty in range(RETRIES):
                    try:
                        stats = mc.stats()
                    except Exception as e:
                        log.warn("{0}, retries = {1}".format(str(e), rerty))
                        time.sleep(2)
                        mc.reconnect()
                    else:
                        break
                else:
                    stats = {}
                data[mc.host]["snapshots"].append(stats)

                for arg in ("timings", "dispatcher"):
                    try:
                        stats = mc.stats(arg)
                        data[mc.host][arg].append(stats)
                    except EOFError as e:
                        log.error("unable to get {0} stats {1}: {2}"
                                  .format(arg, mc.host, e))

        for host in (mc.host for mc in mcs):
            unique_id = host + '-' + str(self._task["time"])
            current_time = time.time()

            if self._mb_stats["snapshots"]:  # use manually captured stats
                self._task["membasestats"] = self._mb_stats["snapshots"]
            else:  # use periodically captured stats
                for snapshot in data[host]["snapshots"]:
                    snapshot["unique_id"] = unique_id
                    snapshot["time"] = current_time
                    snapshot["ip"] = host
                    self._task["membasestats"].append(snapshot)

            for timing in data[host]["timings"]:
                timing["unique_id"] = unique_id
                timing["time"] = current_time
                timing["ip"] = host
                self._task["timings"].append(timing)

            for dispatcher in data[host]["dispatcher"]:
                dispatcher["unique_id"] = unique_id
                dispatcher["time"] = current_time
                dispatcher["ip"] = host
                self._task["dispatcher"].append(dispatcher)

            if data[host]["timings"]:
                log.info("dumping disk timing stats: {0}".format(host))
                latests_timings = data[host]["timings"][-1]
                for key, value in sorted(latests_timings.items()):
                    if key.startswith("disk"):
                        print("{0:50s}: {1}".format(key, value))

        log.info("finished membase_stats")

    def ns_server_stats(self, interval=60):
        self._task["ns_server_stats"] = []
        self._task["ns_server_stats_system"] = []
        nodes_iterator = (node for node in self.nodes)
        node = next(nodes_iterator)
        retries = 0
        not_null = lambda v: v if v is not None else 0

        rest = RestConnection(node)
        while not self._aborted():
            time.sleep(interval)
            log.info("collecting ns_server_stats")
            try:
                # Bucket stats
                ns_server_stats = rest.fetch_bucket_stats(bucket=self.bucket)
                for key, value in ns_server_stats["op"]["samples"].items():
                    ns_server_stats["op"]["samples"][key] = not_null(value)
                self._task["ns_server_stats"].append(ns_server_stats)
                # System stats
                ns_server_stats_system = rest.fetch_system_stats()
                self._task["ns_server_stats_system"].append(ns_server_stats_system)
            except ServerUnavailableException as e:
                log.error(e)
            except (ValueError, TypeError) as e:
                log.error("unable to parse json object {0}: {1}".format(node, e))
            else:
                continue
            retries += 1
            if retries <= RETRIES:
                log.warning("retries: {0} of {1}".format(retries, RETRIES))
            else:
                try:
                    node = next(nodes_iterator)
                    rest = RestConnection(node)
                    retries = 0
                except StopIteration:
                    log.error("no nodes available: stop collecting ns_server_stats")
                    return

        log.info("finished ns_server_stats")

    def indexing_time_stats(self, ddoc, interval=60):
        """Collect view indexing stats"""
        self._task['view_info'] = list()

        rests = [RestConnection(node) for node in self.nodes]
        while not self._aborted():
            time.sleep(interval)
            log.info("collecting view indexing stats")
            for rest in rests:
                try:
                    data = rest.set_view_info(self.bucket, ddoc)
                except (SetViewInfoNotFound, ServerUnavailableException) as error:
                    log.error(error)
                    continue
                try:
                    update_history = data[1]['stats']['update_history']
                    indexing_time = \
                        [event['indexing_time'] for event in update_history]
                    avg_time = sum(indexing_time) // len(indexing_time)
                except (IndexError, KeyError, ValueError):
                    avg_time = 0
                finally:
                    self._task['view_info'].append({'node': rest.ip,
                                                    'indexing_time': avg_time,
                                                    'timestamp': time.time()})

        log.info("finished collecting view indexing stats")

    def indexing_throughput_stats(self, interval=15):
        self._task['indexer_info'] = list()
        indexers = defaultdict(dict)
        rests = [RestConnection(node) for node in self.nodes]
        while not self._aborted():
            time.sleep(interval)  # 15 seconds by default

            # Grab indexer tasks from all nodes
            tasks = list()
            for rest in rests:
                try:
                    active_tasks = rest.active_tasks()
                except ServerUnavailableException as error:
                    log.error(error)
                    continue
                indexer_tasks = [t for t in active_tasks if t['type'] == 'indexer']
                tasks.extend(indexer_tasks)

            # Calculate throughput for every unique PID
            thr = 0
            for task in tasks:
                uiid = task['pid'] + str(task['started_on'])

                changes_delta = \
                    task['changes_done'] - indexers[uiid].get('changes_done', 0)
                time_delta = \
                    task['updated_on'] - indexers[uiid].get('updated_on',
                                                            task['started_on'])
                if time_delta:
                    thr += changes_delta // time_delta
                indexers[uiid]['changes_done'] = task['changes_done']
                indexers[uiid]['updated_on'] = task['updated_on']

            # Average throughput
            self._task['indexer_info'].append({
                'indexing_throughput': thr,
                'timestamp': time.time()
            })

    def _get_xdcr_latency(self, src_client, dst_client, multi=False):
        PREFIX = "xdcr_track_"
        kvs = dict((PREFIX + hex(), hex()) for _ in range(10))
        key = PREFIX + hex()
        persisted = False

        t0 = t1 = time.time()
        if multi:
            src_client.setMulti(0, 0, kvs)
            while True:
                try:
                    dst_client.getMulti(list(kvs.keys()), timeout_sec=120,
                                        parallel=False)
                    break
                except ValueError:
                    time.sleep(0.05)
        else:
            src_client.set(key, 0, 0, key)
            while not persisted:
                _, _, _, persisted, _ = src_client.observe(key)
            t1 = time.time()
            while time.time() - t1 < 300:  # 5 minutes timeout
                try:
                    dst_client.get(key)
                    break
                except:
                    time.sleep(0.05)
        total_time = (time.time() - t0) * 1000
        persist_time = (t1 - t0) * 1000

        if multi:
            return {"multi_100_xdcr_lag": total_time}
        else:
            return {
                "xdcr_lag": total_time,
                "xdcr_persist_time": persist_time,
                "xdcr_diff": total_time - persist_time,
                "timestamp": time.time()
            }

    def xdcr_lag_stats(self, interval=5):
        master = self.clusters[0][0]
        slave = self.clusters[1][0]
        src_client = VBucketAwareMemcached(RestConnection(master), self.bucket)
        dst_client = VBucketAwareMemcached(RestConnection(slave), self.bucket)

        log.info("started xdcr lag measurements")
        self._task["xdcr_lag"] = list()
        while not self._aborted():
            single_stats = self._get_xdcr_latency(src_client, dst_client)
            multi_stats = self._get_xdcr_latency(src_client, dst_client, True)
            multi_stats.update(single_stats)
            self._task['xdcr_lag'].append(multi_stats)
            time.sleep(interval)

        filename = time.strftime("%Y%m%d_%H%M%S_xdcr_lag.json",
                                 time.localtime())
        with open(filename, "w") as fh:
            fh.write(json.dumps(self._task['xdcr_lag'],
                                indent=4, sort_keys=True))
        log.info("finished xdcr lag measurements")

    def rebalance_progress(self, interval=15):
        self._task["rebalance_progress"] = list()
        nodes = cycle(self.nodes)
        rest = RestConnection(next(nodes))
        while not self._aborted():
            try:
                tasks = rest.ns_server_tasks()
            except ServerUnavailableException as error:
                log.error(error)
                rest = RestConnection(next(nodes))
                continue
            for task in tasks:
                if task["type"] == "rebalance":
                    self._task["rebalance_progress"].append({
                        "rebalance_progress": task.get("progress", 0),
                        "timestamp": time.time()
                    })
                    break
            time.sleep(interval)
        log.info("finished active_tasks measurements")

    def _aborted(self):
        return self._task["state"] == "stopped"

    def start_atop(self):
        """Start atop collector"""
        for node in self.nodes:
            try:
                shell = RemoteMachineShellConnection(node)
            except SystemExit:
                log.error("can't establish SSH session with {0}".format(node.ip))
            else:
                cmd = "killall atop; rm -fr /tmp/*.atop;" + \
                    "atop -w /tmp/{0}.atop -a 15".format(node.ip) + \
                    " > /dev/null 2> /dev.null < /dev/null &"
                shell.execute_command(cmd)

    def stop_atop(self):
        """Stop atop collector"""
        for node in self.nodes:
            try:
                shell = RemoteMachineShellConnection(node)
            except SystemExit:
                log.error("can't establish SSH session with {0}".format(node.ip))
            else:
                shell.execute_command("killall atop")

class CallbackStatsCollector(StatsCollector):

    """Invokes optional callback when registered levels have been reached
    during stats sample()'ing."""

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
                "cpu": info.cpu, "disk": info.disk, "hostname": info.hostname}
