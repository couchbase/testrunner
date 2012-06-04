import unittest
import logger
import random
import time

from testconstants import MIN_COMPACTION_THRESHOLD
from testconstants import MAX_COMPACTION_THRESHOLD
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from memcached.helper.data_helper import MemcachedClientHelper


class AutoCompactionTests(unittest.TestCase):

    servers = None
    clients = None
    log = None
    input = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers,self)

    def insert_key(self, serverInfo, bucket_name, count, size):
        client = MemcachedClientHelper.proxy_client(serverInfo, bucket_name)
        value = MemcachedClientHelper.create_value("*", size)
        for i in range(count*1000):
            key = "key_" + str(i)
            flag = random.randint(1, 999)
            client.set(key, 0, flag, value)

    def _database_fragmentation(self, percent_threshold):
        bucket_name = "default"
        MAX_RUN = 99
        item_size = 1024
        update_item_size = item_size*((float(97 - percent_threshold))/100)
        serverInfo = self.servers[0]
        self.log.info(serverInfo)
        rest = RestConnection(serverInfo)
        remote_client = RemoteMachineShellConnection(serverInfo)

        rest.set_autoCompaction(dbFragmentThreshold=80, viewFragmntThreshold=80)
        parallelDBAndView = "false"
        output = rest.set_autoCompaction(parallelDBAndView, percent_threshold, 100)
        if not output and percent_threshold < MIN_COMPACTION_THRESHOLD:
            self.log.error("Need to set minimum threshold above {0}%".format(MIN_COMPACTION_THRESHOLD))
        elif not output and percent_threshold > MAX_COMPACTION_THRESHOLD:
            self.log.error("Need to set maximum threshold under {0}".format(MAX_COMPACTION_THRESHOLD))
        elif output and percent_threshold == MAX_COMPACTION_THRESHOLD:
            self.log.info("Auto compaction will not run at {0}% setting".format(MAX_COMPACTION_THRESHOLD))
        elif (output and percent_threshold >= MIN_COMPACTION_THRESHOLD
                     and percent_threshold <= MAX_RUN):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
            info = rest.get_nodes_self()

            available_ram = info.memoryQuota * (node_ram_ratio)/2
            items = (int(available_ram*1000)/2)/item_size
            rest.create_bucket(bucket= bucket_name, ramQuotaMB=int(available_ram), authType='sasl',
                               saslPassword='password', replicaNumber=1, proxyPort=11211)
            BucketOperationHelper.wait_for_memcached(serverInfo, bucket_name)
            BucketOperationHelper.wait_for_vbuckets_ready_state(serverInfo, bucket_name)

            self.log.info("start to load {0}K keys with {1} bytes/key".format(items, item_size))
            self.insert_key(serverInfo, bucket_name, items, item_size)
            self.log.info("sleep 10 seconds before the next run")
            time.sleep(10)

            self.log.info("start to update {0}K keys with smaller value {1} bytes/key".format(items,
                                                                             int(update_item_size)))
            self.insert_key(serverInfo, bucket_name, items, int(update_item_size))
            compact_run = remote_client.wait_till_compaction_end(rest, bucket_name, timeout_in_seconds=180)
            if not compact_run:
                self.log.error("auto compaction does not run")
            elif compact_run:
                self.log.info("auto compaction runs successfully")
        else:
            self.log.error("Unknown error")

    def test_database_fragmentation_0_percents(self):
            # parameters: items, item_size, update_item_size, percent_threshold
            self._database_fragmentation(0)

    def test_database_fragmentation_30_percents(self):
            self._database_fragmentation(30)

    def test_database_fragmentation_50_percents(self):
            self._database_fragmentation(50)

    def test_database_fragmentation_70_percents(self):
            self._database_fragmentation(70)

    def test_database_fragmentation_90_percents(self):
            self._database_fragmentation(90)

    def test_database_fragmentation_100_percents(self):
            self._database_fragmentation(100)

    def _viewFragmentationThreshold(self):
        for serverInfo in self.servers:
            self.log.info(serverInfo)
            rest = RestConnection(serverInfo)
            rest.set_autoCompaction(dbFragmentThreshold=80, viewFragmntThreshold=80)
