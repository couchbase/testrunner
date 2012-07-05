import time
from membase.api.rest_client import RestConnection
from couchbase.documentgenerator import DocumentGenerator
from memcached.helper.data_helper import MemcachedClientHelper
from basetestcase import BaseTestCase
from memcached.helper.kvstore import KVStore

class WarmUpTests(BaseTestCase):
    def setUp(self):
        super(WarmUpTests, self).setUp()
        self.pre_warmup_stats = {}
        self.timeout = 120
        self.bucket_name = self.input.param("bucket", "default")
        self.bucket_size = 256
        self.nodes_in = self.input.param("nodes_in", 1)
        servs_in = [self.servers[i + 1] for i in range(self.nodes_in)]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])
        rebalance.result()
        self.cluster.create_default_bucket(self.servers[0], self.bucket_size, self.num_replicas)
        self.buckets[self.bucket_name] = {1 : KVStore()}

    def tearDown(self):
        super(WarmUpTests, self).tearDown()
        #pass

    def _load_doc_data_all_buckets(self, op_type='create', start=0):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('test_docs', template, age, first, start=start, end=self.num_items)
        self._load_all_buckets(self.servers[0], gen_load, op_type, 0)

    def _async_load_doc_data_all_buckets(self, op_type='create', start=0):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('test_docs', template, age, first, start=start, end=self.num_items)
        self._load_all_buckets(self.servers[0], gen_load, op_type, 0)
        tasks = self._async_load_all_buckets(self.servers[0], gen_load, op_type, 0)
        return tasks

    def _stats_befor_warmup(self):
        self.stats_monitor = self.input.param("stats_monitor", "mem_used")
        self.stats_monitor = self.stats_monitor.split(";")
        self.stats_monitor.append("uptime")
        self.stats_monitor.append("curr_items_tot")
        for server in self.servers:
            mc_conn = MemcachedClientHelper.direct_client(server, self.bucket_name, self.timeout)
            self.pre_warmup_stats["{0}:{1}".format(server.ip, server.port)] = {}
            for stat_to_monitor in self.stats_monitor:
                self.pre_warmup_stats["{0}:{1}".format(server.ip, server.port)][stat_to_monitor] = mc_conn.stats("")[stat_to_monitor]
                self.log.info("memcached {0}:{1} has {2} value {3}".format(server.ip, server.port, stat_to_monitor , mc_conn.stats("")[stat_to_monitor]))
            mc_conn.close()

    def _restart_memcache(self):
        rest = RestConnection(self.servers[0])
        nodes = rest.node_statuses()
        for node in nodes:
            _node = {"ip": node.ip, "port": node.port, "username": self.servers[0].rest_username,
                     "password": self.servers[0].rest_password}
            _mc = MemcachedClientHelper.direct_client(_node, self.bucket_name)
            pid = _mc.stats()["pid"]
            node_rest = RestConnection(_node)
            command = "os:cmd(\"kill -9 {0} \")".format(pid)
            self.log.info(command)
            killed = node_rest.diag_eval(command)
            self.log.info("killed ??  {0} ".format(killed))
            _mc.close()

        start = time.time()
        memcached_restarted = False
        for server in self.servers:
            mc = None
            while time.time() - start < 60:
                try:
                    mc = MemcachedClientHelper.direct_client(server, self.bucket_name)
                    stats = mc.stats()
                    new_uptime = int(stats["uptime"])
                    if new_uptime < self.pre_warmup_stats["{0}:{1}".format(server.ip, server.port)]["uptime"]:
                        self.log.info("memcached restarted...")
                        memcached_restarted = True
                        break;
                except Exception:
                    self.log.error("unable to connect to {0}:{1}".format(server.ip, server.port))
                    if mc:
                        mc.close()
                    time.sleep(1)
            if not memcached_restarted:
                self.fail("memcached did not start {0}:{1}".format(server.ip, server.port))

    def _wait_for_stats_all_buckets(self, servers):
        tasks = []
        for server in servers:
            for bucket in self.buckets:
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_queue_size', '==', 0))
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_flusher_todo', '==', 0))
        for task in tasks:
            task.result()

    def _warmup(self):
        warmed_up = False
        for server in self.servers:
            mc = MemcachedClientHelper.direct_client(server, self.bucket_name)
            start = time.time()
            if server == self.servers[0]:
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
                            "Collected the stats {0} for server {1}:{2}".format(stats["ep_warmup_time"], server.ip,
                                server.port))
                        break
                    else:
                        self.log.info(" Did not get the stats from the server yet, trying again.....")
                        time.sleep(2)
                except Exception as e:
                    self.log.error(
                        "Could not get warmup_time stats from server {0}:{1}, exception {2}".format(server.ip,
                            server.port, e))
            else:
                self.fail(
                    "Fail! Unable to get the warmup-stats from server {0}:{1} after trying for {2} seconds.".format(
                        server.ip, server.port, wait_time))

            # Waiting for warm-up
            start = time.time()
            warmed_up = False
            while time.time() - start < self.timeout and not warmed_up:
                if mc.stats()["ep_warmup_thread"] == "complete":
                    self.log.info("warmup completed, awesome!!! Warmed up. {0} items ".format(mc.stats()["curr_items_tot"]))
                    time.sleep(5)
                    if mc.stats()["curr_items_tot"] == self.pre_warmup_stats["{0}:{1}".format(server.ip, server.port)]["curr_items_tot"]:
                        self._stats_report(server, mc.stats())
                        warmed_up = True
                    else:
                        continue
                elif mc.stats()["ep_warmup_thread"] == "running":
                    self.log.info(
                                "still warming up .... curr_items_tot : {0}".format(mc.stats()["curr_items_tot"]))
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
            self.log.info("{0} on, {1}:{2} is {3}".\
                           format(stat_to_monitor, server.ip, server.port, self.pre_warmup_stats["{0}:{1}".\
                           format(server.ip, server.port)][stat_to_monitor]))
        self.log.info("******** Stats after Warmup **********")
        for stat_to_monitor in self.stats_monitor:
                self.log.info("{0} on, {1}:{2} is {3}".\
                               format(stat_to_monitor, server.ip, server.port, after_warmup_stat[stat_to_monitor]))

    def test_warmup(self):
        ep_threshold = self.input.param("ep_threshold", "ep_mem_low_wat")
        mc = MemcachedClientHelper.direct_client(self.servers[0], self.bucket_name)
        stats = mc.stats()
        threshold = self.input.param('threshold', stats[ep_threshold])
        threshold_reached = False
        i = 1
        self.num_items = self.input.param("items", 200)
        self._load_doc_data_all_buckets('create')
        #load items till reached threshold or mem-ratio is less than 100
        while not threshold_reached :
            mem_used = stats["mem_used"]
            if mem_used < threshold or stats["vb_active_perc_mem_resident"] == 100:
                items = self.num_items
                self.num_items += self.num_items
                self._load_doc_data_all_buckets('create', items * i)
                i += 1
            else:
                threshold_reached = True
        #parallel load of data
        tasks = self._async_load_doc_data_all_buckets('create', self.num_items)
        #wait for draining of data before restart and warm up
        self._wait_for_stats_all_buckets(self.servers)
        self._stats_befor_warmup()
        for task in tasks:
            task.result()
        self._restart_memcache()
        if self._warmup():
            self._load_doc_data_all_buckets('update')
