from threading import Thread
import unittest
import time
from TestInput import TestInputSingleton
import logger
import datetime
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from security.rbac_base import RbacBase

class DrainRateTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.master = self.input.servers[0]
        self.bucket = "default"
        self.number_of_items = -1
        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)

        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.master), 'builtin')
        
        self._create_default_bucket()
        self.drained_in_seconds = -1
        self.drained = False
        self.reader_shutdown = False


        self._log_start()

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)
        rest = RestConnection(self.master)
        # Remove rbac user in teardown
        role_del = ['cbadminbucket']
        temp = RbacBase().remove_user_role(role_del, rest)
        self._log_finish()

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

    def _create_default_bucket(self, replica=1):
        name = "default"
        master = self.input.servers[0]
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.input.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio
            if(available_ram < 256):
                available_ram = 256
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram), replicaNumber=replica)
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))


    def _load_data_for_buckets(self):
        rest = RestConnection(self.master)
        buckets = rest.get_buckets()
        distribution = {128: 1.0}
        self.bucket_data = {}
        for bucket in buckets:
            name = bucket.name.encode("ascii", "ignore")
            self.bucket_data[name] = {}
            self.bucket_data[name]["inserted_keys"], self.bucket_data[name]["rejected_keys"] = \
            MemcachedClientHelper.load_bucket_and_return_the_keys(name=self.bucket,
                                                                  servers=[self.master],
                                                                  value_size_distribution=distribution,
                                                                  number_of_threads=1,
                                                                  number_of_items=self.number_of_items,
                                                                  write_only=True,
                                                                  moxi=True)

    def _parallel_read(self):
        rest = RestConnection(self.master)
        buckets = rest.get_buckets()
        while not self.reader_shutdown:
            for bucket in buckets:
                name = bucket.name.encode("ascii", "ignore")
                mc = MemcachedClientHelper.direct_client(self.master, name)
                for key in self.bucket_data[name]["inserted_keys"]:
                    mc.get(key)


    def _monitor_drain_queue(self):
        #start whenever drain_queue is > 0
        rest = RestConnection(self.master)
        start = time.time()
        self.log.info("wait 2 seconds for bucket stats are up")
        time.sleep(2)
        stats = rest.get_bucket_stats(self.bucket)
        self.log.info("current ep_queue_size: {0}".format(stats["ep_queue_size"]))
        self.drained = RebalanceHelper.wait_for_persistence(self.master, self.bucket, timeout=300)
        self.drained_in_seconds = time.time() - start


    def _test_drain(self, parallel_read=False):
        reader = None
        loader = Thread(target=self._load_data_for_buckets)
        loader.start()
        self.log.info("waiting for loader thread to insert {0} items".format(self.number_of_items))
        loader.join()
        wait_for_queue = Thread(target=self._monitor_drain_queue)
        wait_for_queue.start()
        if parallel_read:
            reader = Thread(target=self._parallel_read)
            reader.start()
        self.log.info("waiting for ep_queue == 0")
        wait_for_queue.join()
        self.log.info("took {0} seconds to drain {1} items".format(self.drained_in_seconds, self.number_of_items))
        if parallel_read:
            self.reader_shutdown = True
            reader.join()
        self.assertTrue(self.drained, "failed to drain all items")

    def test_drain_10k_items_parallel_read(self):
        self.number_of_items = 10 * 1000
        self._test_drain(True)

    def test_drain_10k_items(self):
        self.number_of_items = 10 * 1000
        self._test_drain()

    def test_drain_100k_items(self):
        self.number_of_items = 100 * 1000
        self._test_drain()

    def test_drain_100k_items_parallel_read(self):
        self.number_of_items = 100 * 1000
        self._test_drain(True)

    def test_drain_1M_items(self):
        self.number_of_items = 1 * 1000 * 1000
        self._test_drain()

    def test_drain_1M_items_parallel_read(self):
        self.number_of_items = 1 * 1000 * 1000
        self._test_drain(True)

