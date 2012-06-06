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
        self.autocompaction_value = TestInputSingleton.input.param("autocompaction_value", 0)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers,self)

    def insert_key(self, serverInfo, bucket_name, count, size):
        client = MemcachedClientHelper.proxy_client(serverInfo, bucket_name)
        value = MemcachedClientHelper.create_value("*", size)
        for i in range(count*1000):
            key = "key_" + str(i)
            flag = random.randint(1, 999)
            client.set(key, 0, flag, value)

    def test_database_fragmentation(self):
        percent_threshold = self.autocompaction_value
        bucket_name = "default"
        MAX_RUN = 100
        item_size = 1024
        update_item_size = item_size*((float(97 - percent_threshold))/100)
        serverInfo = self.servers[0]
        self.log.info(serverInfo)
        rest = RestConnection(serverInfo)
        remote_client = RemoteMachineShellConnection(serverInfo)

        output, rq_content = rest.set_auto_compaction("false", dbFragmentThresholdPercentage=percent_threshold, viewFragmntThresholdPercentage=100,log_error=True)

        if not output and (percent_threshold <= MIN_COMPACTION_THRESHOLD or percent_threshold >= MAX_COMPACTION_THRESHOLD):
            self.assertFalse(output, "it should be  impossible to set compaction value = {0}%".format(percent_threshold))
            import json
            self.assertTrue(json.loads(rq_content).has_key("errors"), "Error is not present in response")
            self.assertTrue(json.loads(rq_content)["errors"].find("Allowed range is 2 - 100") > -1, \
                            "Error 'Allowed range is 2 - 100' expected, but was '{0}'".format(json.loads(rq_content)["errors"]))

            self.log.info("Response contains error = '%(errors)s' as expected" % json.loads(rq_content))
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


    def _viewFragmentationThreshold(self):
        for serverInfo in self.servers:
            self.log.info(serverInfo)
            rest = RestConnection(serverInfo)
            rest.set_auto_compaction(dbFragmentThresholdPercentage=80, viewFragmntThresholdPercentage=80)
