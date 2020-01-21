import sys
import time
import json
import os
import gzip
import copy
import threading
from multiprocessing import Process
from functools import wraps

from TestInput import TestInputSingleton
from lib.membase.performance.stats import CallbackStatsCollector
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.perf_engines import mcsoda
from scripts.perf.rel_cri_stats import CBStatsCollector

from pytests.performance import perf
from pytests.performance.perf_defaults import PerfDefaults
from pytests.performance.viewgen import ViewGen
from pytests.performance.nru_moninor import NRUMonitor
from pytests.performance.cbtop import cbtop

try:
    from seriesly import Seriesly
    import seriesly.exceptions
except ImportError:
    print("unable to import seriesly: see http://pypi.python.org/pypi/seriesly")
    Seriesly = None


def multi_buckets(test):
    """
    Enable multiple buckets for perf tests

    - buckets are named as bucket-0, bucket-1,...
    - the whole key space is splitted and distributed to each bucket, w.r.t. bucket-id
    - different buckets are handled by different client processes
    """
    @wraps(test)
    def wrapper(self, *args, **kwargs):

        num_buckets = self.parami('num_buckets', 1)
        if num_buckets <= 1:
            return test(self, *args, **kwargs)

        self.log.info("multi_buckets: started")
        buckets = self._get_bucket_names(num_buckets)

        num_clients = self.parami("num_clients", 1)
        self.set_param("num_clients", num_clients * num_buckets)

        prefix = self.parami("prefix", 0)

        procs = []
        for bucket_id in reversed(list(range(num_buckets))):
            self.set_param("bucket", buckets[bucket_id])
            new_prefix = prefix + bucket_id * num_clients
            self.set_param("prefix", str(new_prefix))
            self.is_leader = new_prefix == 0

            if bucket_id == 0:
                self.log.info("multi_buckets: start test for {0}"
                              .format(buckets[bucket_id]))
                test(self, *args, **kwargs)
            else:
                self.log.info("multi_buckets: start test in a new process for {0}"
                              .format(buckets[bucket_id]))
                proc = Process(target=test, args=(self, ))
                proc.daemon = True
                proc.start()
                procs.append(proc)

        self.log.info("multi_buckets: {0} finished, waiting for others"
                      .format(buckets[bucket_id]))

        for proc in procs:
            proc.join()

        self.log.info("multi_buckets: stopped")

    return wrapper


def measure_sched_delays(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.parami("prefix", -1) or \
                not self.parami("sched_delays", PerfDefaults.sched_delays):
            return func(self, *args, **kwargs)

        file = "%s-%s" % (self.param("spec", ""),
                          time.strftime(PerfDefaults.strftime))

        self.remove_sched_delay_logs()
        self.start_measure_sched_delay(file)
        self.log.info("started")

        ret = func(self, *args, **kwargs)

        self.stop_measure_sched_delay()
        self.log.info("stopped")

        return ret

    return wrapper


class EPerfMaster(perf.PerfBase):

    """
    specURL = http://hub.internal.couchbase.org/confluence/pages/viewpage.action?pageId=1901816

    """

    def setUp(self):
        self.dgm = False
        self.is_master = True
        self.input = TestInputSingleton.input
        self.mem_quota = self.parami("mem_quota",
                                     PerfDefaults.mem_quota)  # for 10G system
        self.level_callbacks = []
        self.latched_rebalance_done = False
        self.is_multi_node = False
        self.is_leader = self.parami("prefix", 0) == 0
        self.superSetUp()

    def superSetUp(self):
        super(EPerfMaster, self).setUp()

    def tearDown(self):
        self.superTearDown()

    def superTearDown(self):
        super(EPerfMaster, self).tearDown()

    def get_all_stats(self):
        """One node, the 'master', should aggregate stats from client and
        server nodes and push results to couchdb.
        Assuming clients ssh_username/password are same as server
        Save servers list as clients
        Replace servers ip with clients ip"""
        clients = self.input.servers
        for i in range(len(clients)):
            clients[i].ip = self.input.clients[i]
        remotepath = '/tmp'

        i = 0
        for client in clients:
            shell = RemoteMachineShellConnection(client)
            filename = '{0}.json'.format(i)
            destination = "{0}/{1}".format(os.getcwd(), filename)
            self.log.info("getting client stats file {0} from {1}"
                          .format(filename, client))
            if not shell.get_file(remotepath, filename, destination):
                self.log.error("unable to fetch the json file {0} on Client {1} @ {2}"
                               .format(remotepath + '/' + filename, i, client.ip))
                exit(1)
            i += 1

        self.aggregate_all_stats(len(clients))

    def merge_dict(self, output, input):
        """
        Merge two dicts. Add values together if key exists in both dicts.
        """
        if output is None or not output:
            return input

        if input is None or not input:
            return output

        for key, value in input.items():
            if key in list(output.keys()):
                output[key] += value
            else:
                output[key] = value

        return output

    def calc_avg_qps(self, ops_array):
        qps = (ops.get('queriesPerSec', 0) for ops in ops_array)
        try:
            # Access phase w/ fg thread
            return sum(qps) // len(ops_array)
        except:
            # Otherwise
            return 0

    def _merge_query_ops(self, base_ops, new_ops):
        """Normalize and merge query ops arrays into single one.

        Arguments:
        base_ops -- first array of ops stats
        new_ops  -- second array of ops stats
        """

        # Ignore empty arrays
        if not base_ops and not new_ops:
            return base_ops

        # Define normalized timeline
        end_time = 0
        start_time = float('inf')
        num_steps = 0
        for ops_array in [base_ops, new_ops]:
            end_time = max(max(ops['endTime'] for ops in ops_array),
                           end_time)
            start_time = min(min(ops['startTime'] for ops in ops_array),
                             start_time)
        num_steps = max(len(base_ops), len(new_ops))
        step = float(end_time - start_time) // num_steps

        # Merge (terrible time complexity)
        merged_ops = list()
        indexes = [0, 0]
        for i in range(num_steps):
            # Current time frame
            start_of_step = start_time + step * i
            end_of_step = start_of_step + step
            total_queries = 0
            # Aggregation
            for idx, ops_array in enumerate([base_ops[indexes[0]:],
                                             new_ops[indexes[1]:]]):
                for ops in ops_array:
                    if ops['endTime'] <= end_of_step:
                        total_queries += ops['totalQueries']
                        ops['totalQueries'] = 0
                        indexes[idx] += 1
                    else:
                        break
            qps = total_queries // step
            merged_ops.append({'startTime': start_of_step,
                               'endTime': end_of_step,
                               'totalQueries': total_queries,
                               'queriesPerSec': qps})
        return merged_ops

    def aggregate_all_stats(self, len_clients, type="loop"):
        i = 0

        # read [client #].[type].json.gz file until find one
        file = None
        while i < len_clients:
            try:
                file = gzip.open("{0}.{1}.json.gz".format(i, type), 'rb')
                break
            except IOError:
                self.log.warn("cannot open file {0}.{1}.json.gz".format(i, type))
                i += 1

        if file is None:
            return

        final_json = file.read()
        file.close()
        final_json = json.loads(final_json)
        i += 1
        merge_keys = []
        for latency in list(final_json.keys()):
            if latency.startswith('latency'):
                merge_keys.append(str(latency))

        # Average QPS
        if type == 'loop':
            average_qps = self.calc_avg_qps(final_json['ops'])
            final_json['qps'] = {'average': average_qps}
        else:
            final_json['qps'] = {'average': 0}

        for i in range(i, len_clients):
            try:
                file = gzip.open("{0}.{1}.json.gz".format(i, type), 'rb')
            except IOError:
                # cannot find stats produced by this client, check stats
                # collection. the results might be incomplete.
                self.log.warn("cannot open file: {0}.{1}.json.gz".format(i, type))
                continue

            dict = file.read()
            file.close()
            dict = json.loads(dict)
            for key, value in list(dict.items()):
                if key in merge_keys:
                    if key.endswith("histogram"):
                        self.merge_dict(final_json[key], value)
                        continue
                    final_json[key].extend(value)
            if type == 'loop' and self.parami("fg_max_ops", 0):
                final_json['qps']['average'] += self.calc_avg_qps(dict['ops'])
                final_json['ops'] = self._merge_query_ops(final_json['ops'],
                                                          dict['ops'])

        with open("{0}.{1}.json".format('final', type), 'w') as file:
            file.write(json.dumps(final_json))

    def min_value_size(self, avg=2048):
        # Returns an array of different value sizes so that
        # the average value size is 2k and the ratio of
        # sizes is 33% 1k, 33% 2k, 33% 3k, 1% 10k.
        mvs = []
        for i in range(33):
            mvs.append(avg // 2)
            mvs.append(avg)
            mvs.append(avg * 1.5)
        mvs.append(avg * 5)
        return mvs

    def set_nru_freq(self, freq):
        """Set up NRU access scanner running frequency"""
        for server in self.input.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "/opt/couchbase/bin/cbepctl localhost:11210 "\
                  "set flush_param alog_sleep_time {0}".format(freq)
            self._exec_and_log(shell, cmd)
            shell.disconnect()

    def build_alog(self):
        """
        Build access log based on current working set
        TODO: check # access scanner runs
        """
        # schedule to the next minute
        self.set_nru_freq(1)

        # wait for access scanner
        nru_wait = self.parami("nru_wait", PerfDefaults.nru_wait)
        self.log.info("waiting {0} seconds for access scanner".format(nru_wait))
        time.sleep(nru_wait)

        # set back to default
        self.set_nru_freq(PerfDefaults.nru_freq)

        self.log.info("finished")

    def set_nru_task(self, tm_hour=0):
        """UTC/GMT time (hour) to schedule NRU access scanner"""
        gmt = time.gmtime()
        if not tm_hour:
            tm_hour = (gmt.tm_hour + 1) % 24

        min_left = 60 - gmt.tm_min
        gmt_now = time.strftime(PerfDefaults.strftime, time.gmtime())
        for server in self.input.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "/opt/couchbase/bin/cbepctl localhost:11210 "\
                  "set flush_param alog_task_time {0}".format(tm_hour)
            self._exec_and_log(shell, cmd)
            shell.disconnect()
            self.log.info("access scanner will be executed on {0} "
                          "in {1} minutes, at {2} UTC, curr_time is {3}"
                          .format(server.ip, min_left, tm_hour, gmt_now))

    def set_ep_mem_wat(self, percent, high=True):
        """Set ep engine high/low water marks for all nodes"""
        n_bytes = self.parami("mem_quota", PerfDefaults.mem_quota) * \
            percent // 100 * 1024 * 1024
        self.log.info("mem_{0}_wat = {1} percent, {2} bytes"
                      .format("high" if high else "low", percent, n_bytes))
        self.set_ep_param("flush_param",
                          "mem_%s_wat" % ("high" if high else "low"),
                          n_bytes)

    def set_ep_mutation_mem_threshold(self, percent):
        """Set ep engine tmp oom threshold for all nodes"""
        self.log.info("mutation_mem_threshold = {0} percent".format(percent))
        self.set_ep_param("flush_param", "mutation_mem_threshold", percent)

    def set_reb_cons_view(self, node, disable=False):
        """Set up consistent view for rebalance task"""
        rest = RestConnection(node)
        rest.set_reb_cons_view(disable=disable)
        self.log.info("disable consistent view = {0}".format(disable))

    def set_reb_index_waiting(self, node, disable=False):
        """Set up index waiting for rebalance task"""
        rest = RestConnection(node)
        rest.set_reb_index_waiting(disable=disable)
        self.log.info("disable index waiting = {0}".format(disable))

    def start_measure_sched_delay(self, file="sched-delay"):
        for server in self.input.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "nohup collect-sched-delays.rb %s > /dev/null 2>&1 &" % file
            self._exec_and_log(shell, cmd)
            shell.disconnect()

    def stop_measure_sched_delay(self):
        for server in self.input.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "killall -9 -r .*measure-sched-delays"
            self._exec_and_log(shell, cmd)
            shell.disconnect()

    def remove_sched_delay_logs(self):
        for server in self.input.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "rm -rf *.cpu*"
            self._exec_and_log(shell, cmd)
            shell.disconnect()

    # Gets the vbucket count
    def gated_start(self, clients):
        """
        @deprecate
        Remove when we delete test functions.
        No useful? we are calling setupBased1() on SetUp() in perf.py
        """
        if not self.is_master:
            self.setUpBase1()

    @cbtop
    def load_phase(self, num_nodes):

        if self.parami("hot_load_phase", 0) == 1:
            num_items = self.parami("hot_init_items", PerfDefaults.items)
            if self.parami("alog_hot_load", PerfDefaults.alog_hot_load):
                self.build_alog()
        else:
            num_items = self.parami("items", PerfDefaults.items)

        # Cluster nodes if master
        if self.is_master:
            self.rebalance_nodes(num_nodes)

        if self.is_leader:
            mem_high_wat = self.parami('mem_high_wat', PerfDefaults.mem_high_wat)
            mem_low_wat = self.parami('mem_low_wat', PerfDefaults.mem_low_wat)
            mutation_mem_threshold = self.parami('mutation_mem_threshold',
                                                 PerfDefaults.mutation_mem_threshold)
            if mem_high_wat != PerfDefaults.mem_high_wat:
                self.set_ep_mem_wat(mem_high_wat)
            if mem_low_wat != PerfDefaults.mem_low_wat:
                self.set_ep_mem_wat(mem_low_wat, high=False)
            if mutation_mem_threshold != PerfDefaults.mutation_mem_threshold:
                self.set_ep_mutation_mem_threshold(mutation_mem_threshold)

        if self.parami("load_phase", 1) > 0:
            self.log.info("Loading")

            nru_freq = self.parami('nru_freq', PerfDefaults.nru_freq)
            if nru_freq != PerfDefaults.nru_freq and self.is_leader:
                self.set_nru_freq(nru_freq)

            num_clients = self.parami("num_clients",
                                      len(self.input.clients) or 1)
            start_at = self.load_phase_clients_start_at(num_items, num_clients)
            items = self.parami("num_items", num_items) // num_clients
            self.is_multi_node = False
            mvs = self.min_value_size(self.parami("avg_value_size",
                                                  PerfDefaults.avg_value_size))
            if 'xperf' in self.param('conf_file', '') and 'bi' in self.id():
                prefix = self.params('cluster_prefix', '')
            else:
                prefix = ''

            self.load(items,
                      self.param('size', mvs),
                      kind=self.param('kind', 'json'),
                      protocol=self.mk_protocol(host=self.input.servers[0].ip,
                                                port=self.input.servers[0].port),
                      use_direct=self.parami('use_direct', 1),
                      doc_cache=self.parami('doc_cache', 0),
                      prefix=prefix,
                      start_at=start_at,
                      is_eperf=True)
            self.restartProxy()

    def load_phase_clients_start_at(self, num_items, num_clients):
        """
        - normal load-phase or hot-load-phase with only one bucket:
            keep the normal multi-client start_at calculation

        - hot-load-phase with multiple buckets:
            - shift an 'offset' distance to match the key space in the load phase
            - apply the normal multi-client calculation
        """
        num_buckets = self.parami('num_buckets', 1)
        if self.parami("hot_load_phase", 0) == 0 or num_buckets <= 1:
            return int(self.paramf("start_at", 1.0) *
                       self.parami("prefix", 0) * num_items // num_clients)

        # multi-bucket, hot-load-phase
        num_loaded_items = self.parami("items", PerfDefaults.items)
        cpb = num_clients // num_buckets
        offset = num_loaded_items // num_buckets * \
            (self.parami("prefix", 0) // cpb)
        return int(offset + self.paramf("start_at", 1.0) *
                   self.parami("prefix", 0) % cpb * num_items // num_clients)

    def access_phase_clients_start_at(self):
        self.access_phase_items = self.parami("items", PerfDefaults.items)
        num_clients = self.parami("num_clients", len(self.input.clients) or 1)
        start_at = int(self.paramf("start_at", 1.0) *
                       self.parami("prefix", 0) * self.access_phase_items /
                       num_clients)
        return num_clients, start_at

    @measure_sched_delays
    @cbtop
    def access_phase(self,
                     ratio_sets=0,
                     ratio_misses=0,
                     ratio_creates=0,
                     ratio_deletes=0,
                     ratio_hot=0,
                     ratio_hot_gets=0,
                     ratio_hot_sets=0,
                     ratio_expirations=0,
                     max_creates=0,
                     hot_shift=10,
                     ratio_queries=0,
                     queries=None,
                     proto_prefix="membase-binary",
                     host=None,
                     port=None,
                     ddoc=None):
        if self.parami("access_phase", 1) > 0:
            self.log.info("Accessing")
            items = self.parami("items", PerfDefaults.items)
            num_clients, start_at = self.access_phase_clients_start_at()
            start_delay = self.parami("start_delay", PerfDefaults.start_delay)
            if start_delay > 0:
                time.sleep(start_delay * self.parami("prefix", 0))
            max_creates = self.parami("max_creates", max_creates) // num_clients
            self.is_multi_node = False
            if self.param('reb_protocol', None) == 'memcached-binary':
                proto_prefix = 'memcached-binary'
                if self.input.servers[0].port == '8091':
                    port = '11211'
                else:
                    port = '12001'
            if host is None:
                host = self.input.servers[0].ip
            if port is None:
                port = self.input.servers[0].port
            mvs = self.min_value_size(self.parami("avg_value_size",
                                      PerfDefaults.avg_value_size))

            if self.parami('nru_task', PerfDefaults.nru_task) and self.is_leader:
                self.set_nru_task()
                self.nru_monitor = NRUMonitor(
                    self.parami("nru_polling_freq", PerfDefaults.nru_polling_freq),
                    self.parami("nru_reb_delay", PerfDefaults.nru_reb_delay),
                    self)
                self.nru_monitor.daemon = True
                self.nru_monitor.start()

            if 'xperf' in self.param('conf_file', '') and 'bi' in self.id():
                prefix = self.params('cluster_prefix', '')
            else:
                prefix = ''

            # Optionally start rebalancer thread
            if self.is_leader and hasattr(self, "rebalance"):
                t = threading.Thread(target=self.rebalance, name="rebalancer")
                t.daemon = True
                if hasattr(self, "get_region"):
                    if self.get_region() == 'east':
                        t.start()  # rebalance only one cluster
                else:
                    t.start()

            self.loop(num_ops=0,
                      num_items=items,
                      max_items=items + max_creates + 1,
                      max_creates=max_creates,
                      min_value_size=self.param('size', mvs),
                      kind=self.param('kind', 'json'),
                      protocol=self.mk_protocol(host, port, proto_prefix),
                      clients=self.parami('clients', 1),
                      ratio_sets=ratio_sets,
                      ratio_misses=ratio_misses,
                      ratio_creates=ratio_creates,
                      ratio_deletes=ratio_deletes,
                      ratio_hot=ratio_hot,
                      ratio_hot_gets=ratio_hot_gets,
                      ratio_hot_sets=ratio_hot_sets,
                      ratio_expirations=ratio_expirations,
                      expiration=self.parami('expiration', 60 * 5),  # 5 minutes.
                      test_name=self.id(),
                      use_direct=self.parami('use_direct', 1),
                      doc_cache=self.parami('doc_cache', 0),
                      prefix=prefix,
                      collect_server_stats=self.is_leader,
                      start_at=start_at,
                      report=self.parami('report', int(max_creates * 0.1)),
                      exit_after_creates=self.parami('exit_after_creates', 1),
                      hot_shift=self.parami('hot_shift', hot_shift),
                      is_eperf=True,
                      ratio_queries=ratio_queries,
                      queries=queries,
                      ddoc=ddoc)

    def clear_hot_keys(self):
        """
        callback to clear hot keys from the stack
        """
        stack = self.cur.get('hot-stack', None)
        if not stack:
            self.log.warn("unable to clear hot stack : stack does not exist")
            return False

        stack.clear()
        return True

    # restart the cluster and wait for it to warm up
    @cbtop
    def warmup_phase(self):
        if not self.is_leader:
            return

        self.build_alog()
        server = self.input.servers[0]
        client_id = self.parami("prefix", 0)
        test_params = {'test_time': time.time(),
                       'test_name': self.id(),
                       'json': 0}

        sc = self.start_stats(self.spec_reference + ".warmup",
                              test_params=test_params, client_id=client_id)

        start_time = time.time()

        self.warmup(collect_stats=False, flush_os_cache=True)
        sc.capture_mb_snapshot(server)

        self.warmup(collect_stats=False, flush_os_cache=False)
        sc.capture_mb_snapshot(server)

        end_time = time.time()

        ops = {'tot-sets': 0,
               'tot-gets': 0,
               'tot-items': 0,
               'tot-creates': 0,
               'tot-misses': 0,
               "start-time": start_time,
               "end-time": end_time}

        self.end_stats(sc, ops, self.spec_reference + ".warmup")

    @cbtop
    def index_phase(self, ddocs):
        """Create design documents and views"""

        if self.parami("index_phase", 1) > 0:
            # Start stats collector
            if self.parami("collect_stats", 1):
                client_id = self.parami("prefix", 0)
                test_params = {'test_time': time.time(),
                               'test_name': self.id(),
                               'json': 0}

                sc = self.start_stats(self.spec_reference + ".index",
                                      test_params=test_params,
                                      client_id=client_id)

                start_time = time.time()

            # Create design documents and views
            master = self.input.servers[0]
            self.set_up_rest(master)

            bucket = self.param('bucket', 'default')

            for ddoc_name, d in list(ddocs.items()):
                d = copy.copy(d)
                d["language"] = "javascript"
                d_json = json.dumps(d)
                api = "{0}{1}/_design/{2}".format(self.rest.capiBaseUrl,
                                                  bucket, ddoc_name)
                self.rest._http_request(api, 'PUT', d_json,
                                        headers=self.rest._create_capi_headers())

            # Initialize indexing
            for ddoc_name, d in list(ddocs.items()):
                for view_name, x in list(d["views"].items()):
                    for attempt in range(10):
                        try:
                            self.rest.query_view(ddoc_name, view_name, bucket,
                                                 {"limit": 10})
                        except Exception as e:
                            time.sleep(2)
                        else:
                            break
                    else:
                        raise e

            # Wait until there are no active indexer and view compaction tasks
            self.wait_for_task_completion('indexer')
            self.wait_for_task_completion('view_compaction')

            # Stop stats collector
            if self.parami("collect_stats", 1):
                end_time = time.time()

                ops = {'start-time': start_time,
                       'end-time': end_time}

                self.end_stats(sc, ops, self.spec_reference + ".index")

    def debug_phase(self, ddocs=None):
        """Run post-access phase debugging tasks"""

        if ddocs:
            # Query with debug argument equals true
            bucket = self.param('bucket', 'default')
            ddoc_name = list(ddocs.keys())[0]
            view_name = list(ddocs[ddoc_name]['views'].keys())[0]

            response = self.rest.query_view(ddoc_name, view_name, bucket,
                                            {'debug': 'true'})
            debug_info = response['debug_info']

            for subset, data in debug_info.items():
                self.log.info(subset)
                self.log.info(data)

    def latched_rebalance(self, cur=None, delay=0.01, sync=False):
        if not self.latched_rebalance_done:
            self.latched_rebalance_done = True
            self.delayed_rebalance(self.parami("num_nodes_after",
                                               PerfDefaults.num_nodes_after),
                                   delay,
                                   self.parami("reb_max_retries",
                                               PerfDefaults.reb_max_retries),
                                   self.parami("reb_mode",
                                               PerfDefaults.reb_mode),
                                   sync)

    # ---------------------------------------------

    @multi_buckets
    def test_eperf_read(self):
        """
        Eperf read test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_read")

        self.gated_start(self.input.clients)
        if self.parami("load_phase", 0):
            self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes))

        if self.parami("access_phase", 1) == 1:
            self.access_phase(ratio_sets=self.paramf('ratio_sets',
                                                     PerfDefaults.ratio_sets),
                              ratio_misses=self.paramf('ratio_misses',
                                                       PerfDefaults.ratio_misses),
                              ratio_creates=self.paramf('ratio_creates',
                                                        PerfDefaults.ratio_creates),
                              ratio_deletes=self.paramf('ratio_deletes',
                                                        PerfDefaults.ratio_deletes),
                              ratio_hot=self.paramf('ratio_hot',
                                                    PerfDefaults.ratio_hot),
                              ratio_hot_gets=self.paramf('ratio_hot_gets',
                                                         PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets=self.paramf('ratio_hot_sets',
                                                         PerfDefaults.ratio_hot_sets),
                              ratio_expirations=self.paramf('ratio_expirations',
                                                            PerfDefaults.ratio_expirations),
                              max_creates=self.parami("max_creates",
                                                      PerfDefaults.max_creates))

    @multi_buckets
    def test_eperf_write(self):
        """
        Eperf write test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_write")

        self.gated_start(self.input.clients)
        if self.parami("load_phase", 0):
            self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes))

        if self.parami("access_phase", 1) == 1:
            self.access_phase(ratio_sets=self.paramf('ratio_sets',
                                                     PerfDefaults.ratio_sets),
                              ratio_misses=self.paramf('ratio_misses',
                                                       PerfDefaults.ratio_misses),
                              ratio_creates=self.paramf('ratio_creates',
                                                        PerfDefaults.ratio_creates),
                              ratio_deletes=self.paramf('ratio_deletes',
                                                        PerfDefaults.ratio_deletes),
                              ratio_hot=self.paramf('ratio_hot',
                                                    PerfDefaults.ratio_hot),
                              ratio_hot_gets=self.paramf('ratio_hot_gets',
                                                         PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets=self.paramf('ratio_hot_sets',
                                                         PerfDefaults.ratio_hot_sets),
                              ratio_expirations=self.paramf('ratio_expirations',
                                                            PerfDefaults.ratio_expirations),
                              max_creates=self.parami("max_creates",
                                                      PerfDefaults.max_creates))

    @multi_buckets
    def test_eperf_mixed(self, save_snapshot=False):
        """
        Eperf mixed test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_mixed")

        self.gated_start(self.input.clients)
        if self.parami("load_phase", 0):
            self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes))

        if self.parami("index_phase", 0) and self.param("woq_pattern", 0):
            view_gen = ViewGen()
            ddocs = view_gen.generate_ddocs([1])
            self.index_phase(ddocs)

        if self.parami("access_phase", 1) == 1:

            if self.parami("cb_stats", PerfDefaults.cb_stats) == 1:
                # starts cbstats collection
                cbStatsCollector = CBStatsCollector()
                cb_exc = self.param("cb_stats_exc", PerfDefaults.cb_stats_exc)
                frequency = self.parami("cb_stats_freq",
                                        PerfDefaults.cb_stats_freq)
                cbStatsCollector.collect_cb_stats(servers=self.input.servers,
                                                  cb_exc=cb_exc,
                                                  frequency=frequency)

            self.access_phase(ratio_sets=self.paramf('ratio_sets',
                                                     PerfDefaults.ratio_sets),
                              ratio_misses=self.paramf('ratio_misses',
                                                       PerfDefaults.ratio_misses),
                              ratio_creates=self.paramf('ratio_creates',
                                                        PerfDefaults.ratio_creates),
                              ratio_deletes=self.paramf('ratio_deletes',
                                                        PerfDefaults.ratio_deletes),
                              ratio_hot=self.paramf('ratio_hot',
                                                    PerfDefaults.ratio_hot),
                              ratio_hot_gets=self.paramf('ratio_hot_gets',
                                                         PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets=self.paramf('ratio_hot_sets',
                                                         PerfDefaults.ratio_hot_sets),
                              ratio_expirations=self.paramf('ratio_expirations',
                                                            PerfDefaults.ratio_expirations),
                              max_creates=self.parami("max_creates",
                                                      PerfDefaults.max_creates))

            if self.parami("cb_stats", PerfDefaults.cb_stats) == 1:
                cbStatsCollector.stop()

        if self.parami("warmup", PerfDefaults.warmup) == 1:
            self.warmup_phase()

        if save_snapshot:
            self.save_snapshots(self.param("snapshot_filename", ""),
                                self.param("bucket", PerfDefaults.bucket))

    def test_eperf_warmup(self):
        """
        Eperf warmup test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_warmup")
        file_base = self.param("snapshot_file_base", "")
        bucket = self.param("bucket", PerfDefaults.bucket)

        if self.load_snapshots(file_base, bucket) \
                and self.parami("warmup", PerfDefaults.warmup):
            self.gated_start(self.input.clients)
            self.log.info("loaded snapshot, start warmup phase")
            self.warmup_phase()
            return

        self.log.warn("unable to find snapshot file, rerun the test")
        self.test_eperf_mixed(save_snapshot=True)

    @multi_buckets
    def test_eperf_rebalance(self):
        """
        Eperf rebalance test, using parameters from conf/*.conf file.
        """
        self.spec("test_eperf_rebalance")

        self.gated_start(self.input.clients)
        if self.parami("load_phase", 0):
            self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes))

        num_clients = self.parami("num_clients", len(self.input.clients) or 1)

        if not self.parami("nru_task", PerfDefaults.nru_task) and \
                not self.parami("reb_no_fg", PerfDefaults.reb_no_fg):
            rebalance_after = self.parami("rebalance_after",
                                          PerfDefaults.rebalance_after)
            self.level_callbacks = [('cur-creates', rebalance_after // num_clients,
                                     getattr(self, "latched_rebalance"))]

        reb_cons_view = self.parami("reb_cons_view", PerfDefaults.reb_cons_view)
        if reb_cons_view != PerfDefaults.reb_cons_view:
            self.set_reb_cons_view(self.input.servers[0],
                                   disable=(reb_cons_view == 0))

        reb_index_waiting = self.parami("reb_index_waiting", PerfDefaults.reb_index_waiting)
        if reb_index_waiting != PerfDefaults.reb_index_waiting:
            self.set_reb_index_waiting(self.input.servers[0],
                                       disable=(reb_index_waiting == 0))

        if self.parami("access_phase", 1) == 1:
            if self.parami("cb_stats", PerfDefaults.cb_stats) == 1:
                # starts cbstats collection
                cbStatsCollector = CBStatsCollector()
                cb_exc = self.param("cb_stats_exc", PerfDefaults.cb_stats_exc)
                frequency = self.parami("cb_stats_freq",
                                        PerfDefaults.cb_stats_freq)
                cbStatsCollector.collect_cb_stats(servers=self.input.servers,
                                                  cb_exc=cb_exc,
                                                  frequency=frequency)

            if self.parami("reb_no_fg", PerfDefaults.reb_no_fg):
                max_creates = 1
            else:
                max_creates = \
                    self.parami("max_creates", PerfDefaults.max_creates)

            self.access_phase(ratio_sets=self.paramf('ratio_sets',
                                                     PerfDefaults.ratio_sets),
                              ratio_misses=self.paramf('ratio_misses',
                                                       PerfDefaults.ratio_misses),
                              ratio_creates=self.paramf('ratio_creates',
                                                        PerfDefaults.ratio_creates),
                              ratio_deletes=self.paramf('ratio_deletes',
                                                        PerfDefaults.ratio_deletes),
                              ratio_hot=self.paramf('ratio_hot',
                                                    PerfDefaults.ratio_hot),
                              ratio_hot_gets=self.paramf('ratio_hot_gets',
                                                         PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets=self.paramf('ratio_hot_sets',
                                                         PerfDefaults.ratio_hot_sets),
                              ratio_expirations=self.paramf('ratio_expirations',
                                                            PerfDefaults.ratio_expirations),
                              max_creates=max_creates,
                              proto_prefix="memcached-binary",
                              port="11211")

            if self.parami("reb_no_fg", PerfDefaults.reb_no_fg):

                start_time = time.time()

                if not self.parami("prefix", 0):
                    self.latched_rebalance(delay=0, sync=True)

                if self.parami("loop_wait_until_drained",
                               PerfDefaults.loop_wait_until_drained):
                    self.wait_until_drained()

                if self.parami("loop_wait_until_repl",
                               PerfDefaults.loop_wait_until_repl):
                    self.wait_until_repl()

                if self.parami("collect_stats", 1):
                    ops = {"tot-sets": max_creates,
                           "tot-gets": 0,
                           "tot-items": 0,
                           "tot-creates": max_creates,
                           "tot-misses": 0,
                           "start-time": start_time,
                           "end-time": time.time()}

                    self.end_stats(self.sc, ops, self.spec_reference + ".loop")

            if self.parami("cb_stats", PerfDefaults.cb_stats) == 1:
                cbStatsCollector.stop()

    def test_eperf_thruput(self):
        """test front-end throughput"""
        self.spec("test_eperf_thruput")

        self.gated_start(self.input.clients)
        if self.parami("load_phase", 0):
            self.load_phase(self.parami("num_nodes", PerfDefaults.num_nodes))

        if self.parami("access_phase", 1) == 1:
            self.access_phase(ratio_sets=self.paramf('ratio_sets',
                                                     PerfDefaults.ratio_sets),
                              ratio_misses=self.paramf('ratio_misses',
                                                       PerfDefaults.ratio_misses),
                              ratio_creates=self.paramf('ratio_creates',
                                                        PerfDefaults.ratio_creates),
                              ratio_deletes=self.paramf('ratio_deletes',
                                                        PerfDefaults.ratio_deletes),
                              ratio_hot=self.paramf('ratio_hot',
                                                    PerfDefaults.ratio_hot),
                              ratio_hot_gets=self.paramf('ratio_hot_gets',
                                                         PerfDefaults.ratio_hot_gets),
                              ratio_hot_sets=self.paramf('ratio_hot_sets',
                                                         PerfDefaults.ratio_hot_sets),
                              ratio_expirations=self.paramf('ratio_expirations',
                                                            PerfDefaults.ratio_expirations),
                              max_creates=self.parami("max_creates",
                                                      PerfDefaults.max_creates))

    def test_vperf(self):
        """View performance test"""
        self.spec(self.__str__().split(" ")[0])

        # Configuration
        self.gated_start(self.input.clients)
        view_gen = ViewGen()
        views = self.param("views", None)
        if self.parami("disabled_updates", 0):
            options = {"updateMinChanges": 0, "replicaUpdateMinChanges": 0}
        else:
            options = None
        try:
            if isinstance(views, str):
                views = eval(views)
                if isinstance(views, int):
                    views = [views]
            elif isinstance(views, list):
                views = [int(v) for v in views]
            else:
                raise TypeError
            ddocs = view_gen.generate_ddocs(views, options)
        except (SyntaxError, TypeError):
            self.log.error("Wrong or missing views parameter")
            sys.exit()

        # Load phase
        if self.parami("load_phase", 0):
            num_nodes = self.parami("num_nodes", 10)
            self.load_phase(num_nodes)

        # Index phase
        if self.parami("index_phase", 0):
            self.index_phase(ddocs)

        # Access phase
        if self.parami("access_phase", 0) == 1:
            bucket = self.params("bucket", "default")
            stale = self.params("stale", "update_after")
            queries = view_gen.generate_queries(ddocs, bucket, stale)

            self.bg_max_ops_per_sec = self.parami("bg_max_ops_per_sec", 100)
            self.fg_max_ops = self.parami("fg_max_ops", 1000000)

            # Rotate hosts so multiple clients don't hit the same HTTP/REST
            # server
            if self.parami('num_nodes_before', 0):
                server_sn = self.parami('prefix', 0) % \
                            self.parami('num_nodes_before', 0)
            else:
                server_sn = self.parami('prefix', 0) % len(self.input.servers)
            host = self.input.servers[server_sn].ip

            self.access_phase(
                ratio_sets=self.paramf('ratio_sets', PerfDefaults.ratio_sets),
                ratio_misses=self.paramf('ratio_misses', PerfDefaults.ratio_misses),
                ratio_creates=self.paramf('ratio_creates', PerfDefaults.ratio_creates),
                ratio_deletes=self.paramf('ratio_deletes', PerfDefaults.ratio_deletes),
                ratio_hot=self.paramf('ratio_hot', PerfDefaults.ratio_hot),
                ratio_hot_gets=self.paramf('ratio_hot_gets', PerfDefaults.ratio_hot_gets),
                ratio_hot_sets=self.paramf('ratio_hot_sets', PerfDefaults.ratio_hot_sets),
                ratio_expirations=self.paramf('ratio_expirations', PerfDefaults.ratio_expirations),
                max_creates=self.parami("max_creates", PerfDefaults.max_creates),
                queries=queries,
                proto_prefix="couchbase",
                host=host,
                ddoc=next(view_gen.ddocs)
            )

        # Incremental index phase
        if self.parami("incr_index_phase", 0) and \
                self.parami("disabled_updates", 0):
            self.index_phase(ddocs)

        if self.parami("debug_phase", 0):
            self.debug_phase(ddocs)

    def test_stats_collector(self):
        """Fake test for stats collection"""

        test_params = {'test_time': time.time(),
                       'test_name': self.id(),
                       'json': 0}

        sc = self.start_stats('loop',
                              test_params=test_params,
                              client_id=0)

        start_time = time.time()

        try:
            time.sleep(self.parami('sleep_time', 3600))
        except KeyboardInterrupt:
            self.log.warn("ctats collection was interrupted")

        end_time = time.time()

        ops = {'start-time': start_time,
               'end-time': end_time}

        self.end_stats(sc, ops, 'loop')

    def test_minimal(self):
        """Minimal performance test which covers load and access phases"""

        self.spec("test-minimal")

        self.rest.create_bucket(bucket='default', ramQuotaMB=128,
                                bucketType='couchbase')

        self.gated_start(self.input.clients)

        # Load phase
        self.load_phase(self.parami('num_nodes', 10))

        # Access phase
        max_creates = self.parami('max_creates', 100)

        # Rotate host so multiple clients don't hit the same HTTP/REST server.
        server_sn = self.parami("prefix", 0) % len(self.input.servers)
        host = self.input.servers[server_sn].ip

        self.access_phase(ratio_sets=self.paramf('ratio_sets', 0.5),
                          ratio_misses=self.paramf('ratio_misses', 0.2),
                          ratio_creates=self.paramf('ratio_creates', 0.5),
                          ratio_deletes=self.paramf('ratio_deletes', 0.5),
                          ratio_hot=self.paramf('ratio_hot', 0.2),
                          ratio_hot_gets=self.paramf('ratio_hot_gets', 0.95),
                          ratio_hot_sets=self.paramf('ratio_hot_sets', 0.95),
                          ratio_expirations=self.paramf('ratio_expirations',
                                                        0.03),
                          max_creates=max_creates,
                          host=host)

        self.rest.delete_bucket(bucket='default')


class EPerfClient(EPerfMaster):

    def setUp(self):
        self.dgm = False
        self.is_master = False
        self.level_callbacks = []
        self.latched_rebalance_done = False
        self.setUpBase0()
        self.is_leader = self.parami("prefix", 0) == 0
        self.bg_max_ops_per_sec = 0
        self.fg_max_ops = 0
        self.get_bucket_conf()
        self.seriesly = None

        pass  # Skip super's setUp().  The master should do the real work.

    def tearDown(self):
        if self.sc is not None:
            self.sc.stop()
            self.sc = None

        pass  # Skip super's tearDown().  The master should do the real work.

    def mk_stats(self, verbosity):
        if self.parami("prefix", 0) == 0 and self.level_callbacks:
            sc = CallbackStatsCollector(verbosity)
            sc.level_callbacks = self.level_callbacks
        else:
            sc = super(EPerfMaster, self).mk_stats(verbosity)
        return sc

    def test_wait_until_drained(self):
        self.wait_until_drained()

    def seriesly_event(self):
        servers = [server.ip for server in self.input.servers]
        return {"test_params": self.input.test_params,
                "servers": servers}

    def init_seriesly(self, host, db):
        if not Seriesly:
            self.log.warn("unable to initialize seriesly: library not installed")
            return False

        if self.seriesly:
            return True

        self.seriesly = Seriesly(host=host)

        try:
            dbs = self.seriesly.list_dbs()
        except seriesly.exceptions.ConnectionError as e:
            self.log.error("unable to connect to seriesly server {0}: {1}"
                           .format(host, e))
            return False

        if db and db not in dbs:
            self.seriesly.create_db(db)

        return True


class EVPerfClient(EPerfClient):

    """The EVPerfClient subclass deploys another background thread to drive a
    concurrent amount of memcached traffic, during the "loop" or access phase.
    Otherwise, with the basic EPerfClient, the HTTP queries would gate the
    ops/second performance due to mcsoda'slockstep batching.
    """

    def mcsoda_run(self, cfg, cur, protocol, host_port, user, pswd,
                   stats_collector=None, stores=None, ctl=None,
                   heartbeat=0, why=None, bucket="default", backups=None):
        self.bg_thread = None
        self.bg_thread_ctl = None

        num_clients, start_at = self.access_phase_clients_start_at()

        # If non-zero background ops/second, the background thread,
        # with cloned parameters, doesn't do any queries, only basic
        # key-value ops.
        if why == "loop" and \
                self.parami("bg_max_ops_per_sec", self.bg_max_ops_per_sec):
            self.bg_thread_cfg = copy.deepcopy(cfg)
            self.bg_thread_cfg['max-ops-per-sec'] = \
                self.parami("bg_max_ops_per_sec", self.bg_max_ops_per_sec)
            self.bg_thread_cfg['ratio-queries'] = \
                self.paramf("bg_ratio_queries", 0.0)
            self.bg_thread_cfg['queries'] = self.param("bg_queries", "")
            self.bg_thread_cur = copy.deepcopy(cur)
            self.bg_thread_ctl = {'run_ok': True}

            bg_protocol = self.param('bg_protocol', 'membase-binary')
            if bg_protocol == 'membase-binary':
                self.bg_stores = [mcsoda.StoreMembaseBinary()]
                bg_host_port = host_port
                bg_protocol = protocol
            elif bg_protocol == 'memcached-binary':
                self.bg_stores = [mcsoda.StoreMemcachedBinary()]
                if self.input.servers[0].port == '8091':
                    bg_host_port = host_port.split(':')[0] + ':11211'
                else:
                    bg_host_port = host_port.split(':')[0] + ':12001'

            self.bg_thread = threading.Thread(target=mcsoda.run,
                                              args=(self.bg_thread_cfg,
                                                    self.bg_thread_cur,
                                                    bg_protocol, bg_host_port,
                                                    user, pswd,
                                                    stats_collector,
                                                    self.bg_stores,
                                                    self.bg_thread_ctl,
                                                    heartbeat, "loop-bg",
                                                    bucket))
            self.bg_thread.daemon = True
            self.bg_thread.start()

            # Also, the main mcsoda run should do no memcached/key-value
            # requests.
            cfg['ratio-sets'] = self.paramf("fg_ratio_sets", 0.0)
            cfg['ratio-queries'] = self.paramf("fg_ratio_queries", 1.0)

        if why == "loop" and self.parami("fg_max_ops", self.fg_max_ops):
            cfg['max-ops'] = start_at + \
                self.parami("fg_max_ops", self.fg_max_ops) // num_clients
            ctl = {'run_ok': True}
            if self.parami("fg_max_ops_per_sec", 0):
                cfg['max-ops-per-sec'] = self.parami("fg_max_ops_per_sec", 0)
            if getattr(self, "active_fg_workers", None) is not None:
                self.active_fg_workers.value += 1
                cfg['active_fg_workers'] = self.active_fg_workers
            cfg['node_prefix'] = self.parami('prefix', 0)

        if why == "loop" and getattr(self, "shutdown_event", None) is not None:
            if ctl is None:
                ctl = {"shutdown_event": getattr(self, "shutdown_event")}
            else:
                ctl["shutdown_event"] = getattr(self, "shutdown_event")

        cfg['stats_ops'] = self.parami("mcsoda_fg_stats_ops",
                                       PerfDefaults.mcsoda_fg_stats_ops)
        rv_cur, start_time, end_time = \
            super(EVPerfClient, self).mcsoda_run(cfg, cur, protocol, host_port,
                                                 user, pswd,
                                                 stats_collector=stats_collector,
                                                 stores=stores,
                                                 ctl=ctl,
                                                 heartbeat=heartbeat,
                                                 why="loop-fg",
                                                 bucket=bucket,
                                                 backups=backups)
        if self.bg_thread_ctl:
            self.log.info("trying to stop background thread")
            self.bg_thread_ctl['run_ok'] = False
            while self.bg_thread.is_alive():
                self.log.info('waiting for background thread {0}.'
                              .format(self.parami("prefix", 0)))
                time.sleep(5)

        return rv_cur, start_time, end_time
