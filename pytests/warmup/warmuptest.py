import time
from membase.api.rest_client import RestConnection, Bucket
from couchbase.documentgenerator import DocumentGenerator
from memcached.helper.data_helper import MemcachedClientHelper
from basetestcase import BaseTestCase
from memcached.helper.kvstore import KVStore
from mc_bin_client import MemcachedError
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection

class WarmUpTests(BaseTestCase):
    def setUp(self):
        super(WarmUpTests, self).setUp()
        self.pre_warmup_stats = {}
        self.timeout = 120
        self.bucket_name = self.input.param("bucket", "default")
        self.bucket_size = self.input.param("bucket_size", 256)
        self.data_size = self.input.param("data_size", 2048)
        self.nodes_in = int(self.input.param("nodes_in", 1))
        self.access_log = self.input.param("access_log", False)
        self.servs_in = [self.servers[i + 1] for i in range(self.nodes_in)]
        rebalance = self.cluster.async_rebalance(self.servers[:1], self.servs_in, [])
        rebalance.result()
        self.cluster.create_default_bucket(self.servers[0], self.bucket_size, self.num_replicas)
        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
             num_replicas=self.num_replicas, bucket_size=self.bucket_size))

    def tearDown(self):
        super(WarmUpTests, self).tearDown()

    def _load_doc_data_all_buckets(self, op_type='create', start=0, expiry=0):
        loaded = False
        count = 0
        gen_load = BlobGenerator('warmup', 'warmup-', self.data_size, start=start, end=self.num_items)
        while not loaded and count < 60:
            try :
                self._load_all_buckets(self.servers[0], gen_load, op_type, expiry)
                loaded = True
            except MemcachedError as error:
                if error.status == 134:
                    loaded = False
                    self.log.error("Memcached error 134, wait for 5 seconds and then try again")
                    count += 1
                    time.sleep(5)

    def _async_load_doc_data_all_buckets(self, op_type='create', start=0):
        gen_load = BlobGenerator('warmup', 'warmup-', self.data_size, start=start, end=self.num_items)
        tasks = self._async_load_all_buckets(self.servers[0], gen_load, op_type, 0)
        return tasks

    def _stats_befor_warmup(self):
        if not self.access_log:
            self.stat_str = ""
        else:
            self.stat_str = "warmup"
        self.stats_monitor = self.input.param("stats_monitor", "ep_warmup_key_count")
        self.stats_monitor = self.stats_monitor.split(";")
        for server in self.nodes_server:
            mc_conn = MemcachedClientHelper.direct_client(server, self.bucket_name, self.timeout)
            self.pre_warmup_stats["{0}:{1}".format(server.ip, server.port)] = {}
            for stat_to_monitor in self.stats_monitor:
                self.pre_warmup_stats["%s:%s" % (server.ip, server.port)][stat_to_monitor] = mc_conn.stats(self.stat_str)[stat_to_monitor]
                self.pre_warmup_stats["%s:%s" % (server.ip, server.port)]["uptime"] = mc_conn.stats("")["uptime"]
                self.pre_warmup_stats["%s:%s" % (server.ip, server.port)]["curr_items_tot"] = mc_conn.stats("")["curr_items_tot"]
                self.log.info("memcached %s:%s has %s value %s" % (server.ip, server.port, stat_to_monitor , mc_conn.stats(self.stat_str)[stat_to_monitor]))
            mc_conn.close()

    def _kill_nodes(self, nodes):
        is_partial = self.input.param("is_partial", "True")
        _nodes = []
        if len(self.servers) > 1 :
            skip = 2
        else:
            skip = 1
        if is_partial:
            _nodes = nodes[0:len(nodes):skip]
        else:
            _nodes = nodes
        for node in _nodes:
            _node = {"ip": node.ip, "port": node.port, "username": self.servers[0].rest_username,
                     "password": self.servers[0].rest_password}
            _mc = MemcachedClientHelper.direct_client(_node, self.bucket_name)
            self.log.info("restarted the node %s:%s" % (node.ip, node.port))
            pid = _mc.stats()["pid"]
            _mc.close()
            node_rest = RestConnection(_node)
            for _server in self.servers:
                if _server.ip == node.ip:
                    self.log.info("Returned Server index %s" % _server)
                    shell = RemoteMachineShellConnection(_server)
                    break

            info = shell.extract_remote_info()
            os_type = info.type.lower()
            if os_type == 'windows':
                shell.terminate_process(info, 'memcached.exe')
                self.log.info("killed ??  node %s " % node.ip)
                # command = "taskkill /F /T /IM memcached.exe*"
            else:
                command = "os:cmd(\"kill -9 {0} \")".format(pid)
                self.log.info(command)
                killed = node_rest.diag_eval(command)
                self.log.info("killed ??  {0} ".format(killed))

    def _restart_memcache(self):
        rest = RestConnection(self.nodes_server[0])
        nodes = rest.node_statuses()
        self._kill_nodes(nodes)
        start = time.time()
        memcached_restarted = False
        for server in self.nodes_server:
            mc = None
            while time.time() - start < 60:
                try:
                    mc = MemcachedClientHelper.direct_client(server, self.bucket_name)
                    stats = mc.stats()
                    new_uptime = int(stats["uptime"])
                    if new_uptime < self.pre_warmup_stats["%s:%s" % (server.ip, server.port)]["uptime"]:
                        self.log.info("memcached restarted...")
                        memcached_restarted = True
                        break;
                except Exception:
                    self.log.error("unable to connect to %s:%s" % (server.ip, server.port))
                    if mc:
                        mc.close()
                    time.sleep(1)
            if not memcached_restarted:
                self.fail("memcached did not start %s:%s" % (server.ip, server.port))

    def _wait_for_stats_all_buckets(self, servers):
        tasks = []
        for server in servers:
            for bucket in self.buckets:
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_queue_size', '==', 0))
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_uncommitted_items', '==', 0))
        for task in tasks:
            task.result()

    def _warmup(self):
        warmed_up = False
        for server in self.nodes_server:
            mc = MemcachedClientHelper.direct_client(server, self.bucket_name)
            start = time.time()
            if server == self.nodes_server[0]:
                wait_time = 300
            else:
                wait_time = 60
                # Try to get the stats for 5 minutes, else hit out.
            while time.time() - start < wait_time:
                # Get the wamrup time for each server
                try:
                    stats = mc.stats()
                    if stats is not None:
                        warmup_time = int(stats["ep_warmup_time"])
                        self.log.info("ep_warmup_time is %s " % warmup_time)
                        self.log.info(
                            "Collected the stats %s for server %s:%s" % (stats["ep_warmup_time"], server.ip,
                                server.port))
                        break
                    else:
                        self.log.info(" Did not get the stats from the server yet, trying again.....")
                        time.sleep(2)
                except Exception as e:
                    self.log.error(
                        "Could not get warmup_time stats from server %s:%s, exception %s" % (server.ip,
                            server.port, e))
            else:
                self.fail(
                    "Fail! Unable to get the warmup-stats from server %s:%s after trying for %s seconds." % (
                        server.ip, server.port, wait_time))

            # Waiting for warm-up
            start = time.time()
            warmed_up = False
            while time.time() - start < self.timeout and not warmed_up:
                if mc.stats()["ep_warmup_thread"] == "complete":
                    self.log.info("warmup completed, awesome!!! Warmed up. %s items " % (mc.stats()["curr_items_tot"]))
                    time.sleep(5)
                    if mc.stats()["curr_items_tot"] == self.pre_warmup_stats["%s:%s" % (server.ip, server.port)]["curr_items_tot"]:
                        self._stats_report(server, mc.stats(self.stat_str))
                        warmed_up = True
                    else:
                        continue
                elif mc.stats()["ep_warmup_thread"] == "running":
                    self.log.info(
                                "still warming up .... curr_items_tot : %s" % (mc.stats()["curr_items_tot"]))
                else:
                    self.fail("Value of ep warmup thread does not exist, exiting from this server")
                time.sleep(5)
            mc.close()
        if warmed_up:
            return True
        else:
            return False

    def _stats_report(self, server, after_warmup_stat):
        self.log.info("******** Stats before Warmup **********")
        for stat_to_monitor in self.stats_monitor:
            self.log.info("%s on, %s:%s is %s" % \
                           (stat_to_monitor, server.ip, server.port, self.pre_warmup_stats["%s:%s" % \
                           (server.ip, server.port)][stat_to_monitor]))
        self.log.info("******** Stats after Warmup **********")
        for stat_to_monitor in self.stats_monitor:
                self.log.info("%s on, %s:%s is %s" % \
                               (stat_to_monitor, server.ip, server.port, after_warmup_stat[stat_to_monitor]))

    def _wait_for_access_run(self, access_log_time, access_scanner_runs, mc):
        access_log_created = False
        new_scanner_run = int(mc.stats()["ep_num_access_scanner_runs"])
        time.sleep(access_log_time * 60)
        self.log.info("new access scanner run is %s" % new_scanner_run)
        count = 0
        while not access_log_created and count < 5:
            if new_scanner_run <= access_scanner_runs:
                count += 1
                time.sleep(5)
                new_scanner_run = int(mc.stats()["ep_num_access_scanner_runs"])
            else:
                access_log_created = True
        return access_log_created

    def test_warmup(self):
        ep_threshold = self.input.param("ep_threshold", "ep_mem_low_wat")
        active_resident_threshold = int(self.input.param("active_resident_threshold", 110))
        access_log_time = self.input.param("access_log_time", 2)
        mc = MemcachedClientHelper.direct_client(self.servers[0], self.bucket_name)
        stats = mc.stats()
        threshold = int(self.input.param('threshold', stats[ep_threshold]))
        threshold_reached = False
        self.num_items = self.input.param("items", 10000)
        self._load_doc_data_all_buckets('create')

        # load items till reached threshold or mem-ratio is less than resident ratio threshold
        while not threshold_reached :
            mem_used = int(mc.stats()["mem_used"])
            if mem_used < threshold or int(mc.stats()["vb_active_perc_mem_resident"]) >= active_resident_threshold:
                self.log.info("mem_used and vb_active_perc_mem_resident_ratio reached at %s/%s and %s " % (mem_used, threshold, mc.stats()["vb_active_perc_mem_resident"]))
                items = self.num_items
                self.num_items += self.input.param("items", 10000)
                self._load_doc_data_all_buckets('create', items)
            else:
                threshold_reached = True
                self.log.info("DGM state achieved!!!!")
        # parallel load of data
        items = self.num_items
        self.num_items += 10000
        tasks = self._async_load_doc_data_all_buckets('create', items)
        # wait for draining of data before restart and warm up
        rest = RestConnection(self.servers[0])
        self.nodes_server = rest.get_nodes()
        self._wait_for_stats_all_buckets(self.nodes_server)
        self._stats_befor_warmup()
        for task in tasks:
            task.result()
        # If warmup is done through access log then run access scanner
        if self.access_log :
            scanner_runs = int(mc.stats()["ep_num_access_scanner_runs"])
            self.log.info("setting access scanner time %s minutes" % access_log_time)
            self.log.info("current access scanner run is %s" % scanner_runs)
            ClusterOperationHelper.flushctl_set(self.nodes_server[0], "alog_sleep_time", access_log_time , self.bucket_name)
            if not self._wait_for_access_run(access_log_time, scanner_runs, mc):
                self.fail("Not able to create access log within %s" % access_log_time)
        self._restart_memcache()
        if self._warmup():
            self._load_doc_data_all_buckets('update', self.num_items - items)

    ''' This test will load items with an expiration and then restart the node once items are
        expired and then check the curr_items_tot which reduced to 0 '''
    def test_warmup_with_expiration(self):
        self.num_items = self.input.param("items", 1000)
        expiry = self.input.param("expiry", 120)
        self._load_doc_data_all_buckets('create', expiry=expiry)
        # wait for draining of data before restart and warm up
        rest = RestConnection(self.servers[0])
        self.nodes_server = rest.get_nodes()
        self._wait_for_stats_all_buckets(self.nodes_server)
        self._stats_befor_warmup()
        time.sleep(120)
        self._restart_memcache()
        self._warmup()
