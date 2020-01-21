import unittest
import datetime
from TestInput import TestInputSingleton
import time
import uuid
import logger
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper

class WarmUpClusterTest(unittest.TestCase):
    input = None
    servers = None
    log = None
    membase = None
    shell = None
    remote_tmp_folder = None
    master = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.master = TestInputSingleton.input.servers[0]
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.num_of_docs = self.input.param("num_of_docs", 1000)

        rest = RestConnection(self.master)
        for server in self.servers:
            rest.init_cluster(server.rest_username, server.rest_password)

        info = rest.get_nodes_self()

        for server in self.servers:
            rest.init_cluster_memoryQuota(server.rest_username, server.rest_password,
                memoryQuota=info.mcdMemoryReserved)

        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        self._create_default_bucket()

        #Rebalance the nodes
        ClusterOperationHelper.begin_rebalance_in(self.master, self.servers)
        ClusterOperationHelper.end_rebalance(self.master)
        self._log_start()

    def tearDown(self):
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        self._log_finish()

    def _create_default_bucket(self):
        name = "default"
        master = self.master
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
        info = rest.get_nodes_self()
        available_ram = info.memoryQuota * node_ram_ratio
        rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram))
        ready = BucketOperationHelper.wait_for_memcached(master, name)
        self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
            msg="unable to create {0} bucket".format(name))
        self.load_thread = None
        self.shutdown_load_data = False

    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def _insert_data(self, howmany):
        self.onenodemc = MemcachedClientHelper.proxy_client(self.master, "default")
        items = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, howmany)]
        try:
            for item in items:
                self.onenodemc.set(item, 0, 0, item)
            self.log.info("inserted {0} items".format(howmany))
            self.onenodemc.close()
        except Exception as e:
            print(e)
            self.fail("could not set item to bucket")

    def do_warmup(self):
        howmany = self.num_of_docs
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self._insert_data(howmany)

        RebalanceHelper.wait_for_persistence(self.master, "default")
        time.sleep(5)
        rest = RestConnection(self.master)

        map = {}
        #collect curr_items from all nodes
        for server in self.servers:
            mc_conn = MemcachedClientHelper.direct_client(server, "default")
            map["{0}:{1}".format(server.ip, server.port)] = {}
            map["{0}:{1}".format(server.ip, server.port)]["curr_items_tot"] = mc_conn.stats("")["curr_items_tot"]
            map["{0}:{1}".format(server.ip, server.port)]["previous_uptime"] = mc_conn.stats("")["uptime"]

            self.log.info(
                "memcached {0}:{1} has {2} items".format(server.ip, server.port, mc_conn.stats("")["curr_items_tot"]))
            mc_conn.close()

        # Killing Memcached
        nodes = rest.node_statuses()

        for node in nodes:
            _node = {"ip": node.ip, "port": node.port, "username": self.servers[0].rest_username,
                     "password": self.servers[0].rest_password}
            _mc = MemcachedClientHelper.direct_client(_node, "default")
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
                    mc = MemcachedClientHelper.direct_client(server, "default")
                    stats = mc.stats()

                    new_uptime = int(stats["uptime"])
                    if new_uptime < map["{0}:{1}".format(server.ip, server.port)]["previous_uptime"]:
                        self.log.info("memcached restarted...")
                        memcached_restarted = True
                        break
                except Exception:
                    self.log.error("unable to connect to {0}:{1}".format(server.ip, server.port))
                    if mc:
                        mc.close()
                    time.sleep(1)
            if not memcached_restarted:
                self.fail("memcached did not start {0}:{1}".format(server.ip, server.port))

        for server in self.servers:
            mc = MemcachedClientHelper.direct_client(server, "default")
            expected_curr_items_tot = map["{0}:{1}".format(server.ip, server.port)]["curr_items_tot"]
            now_items = 0
            start = time.time()

            if server == self.servers[0]:
                wait_time = 600
            else:
                wait_time = 60
                # Try to get the stats for 10 minutes, else hit out.
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

            # Verify the item count from each server, if you get repeated same count(< expected count) for over
            # 3 minutes, then fail. Try to get the items from the server for 30 mins in total, else fail
            start = time.time()
            while time.time() - start < 1800:
                time.sleep(2)
                if mc.stats()["curr_items_tot"] < expected_curr_items_tot:
                    self.log.info("still warming up .... curr_items_tot : {0}".format(mc.stats()["curr_items_tot"]))
                    while now_items == mc.stats()["curr_items_tot"]:
                        if time.time() - start <= 180:
                            self.log.info(
                                "still warming up .... curr_items_tot : {0}".format(mc.stats()["curr_items_tot"]))
                        else:
                            self.fail("Getting repetitive data, exiting from this server")
                else:
                    self.log.info(
                        "warmup completed, awesome!!! Warmed up. {0} items ".format(mc.stats()["curr_items_tot"]))
                    break
                now_items = mc.stats()["curr_items_tot"]
            mc.close()


    def test_warmUpCluster(self):
        self.do_warmup()
