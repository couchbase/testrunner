import unittest
import logger
import random
import time
import json

from threading import Thread, Event

from testconstants import MIN_COMPACTION_THRESHOLD
from testconstants import MAX_COMPACTION_THRESHOLD
from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase.documentgenerator import BlobGenerator
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached


class AutoCompactionTests(BaseTestCase):

    servers = None
    clients = None
    log = None
    input = None

    def setUp(self):
        super(AutoCompactionTests, self).setUp()
        self.autocompaction_value = self.input.param("autocompaction_value", 0)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        self.is_crashed = Event()

    @staticmethod
    def insert_key(serverInfo, bucket_name, count, size):
        rest = RestConnection(serverInfo)
        smart = VBucketAwareMemcached(rest, bucket_name)
        for i in xrange(count * 1000):
            key = "key_" + str(i)
            flag = random.randint(1, 999)
            value = {"value" : MemcachedClientHelper.create_value("*", size)}
            smart.memcached(key).set(key, 0, 0, json.dumps(value))

    def load(self, server, compaction_value, bucket_name, gen):
        monitor_fragm = self.cluster.async_monitor_db_fragmentation(server, compaction_value, bucket_name)
        end_time = time.time() + self.wait_timeout * 50
        # generate load until fragmentation reached
        while monitor_fragm.state != "FINISHED":
            if self.is_crashed.is_set():
                self.cluster.shutdown()
                return
            if end_time < time.time():
               self.fail("Fragmentation level is not reached in %s sec" % self.wait_timeout * 50)
            # update docs to create fragmentation
            try:
                self._load_all_buckets(server, gen, "update", 0)
            except Exception, ex:
               self.is_crashed.set()
               self.log.error("Load cannot be performed: %s" % str(ex))
        monitor_fragm.result()

    def test_database_fragmentation(self):
        percent_threshold = self.autocompaction_value
        bucket_name = "default"
        MAX_RUN = 100
        item_size = 1024
        update_item_size = item_size * ((float(100 - percent_threshold)) / 100)
        serverInfo = self.servers[0]
        self.log.info(serverInfo)
        rest = RestConnection(serverInfo)
        remote_client = RemoteMachineShellConnection(serverInfo)

        output, rq_content, header = rest.set_auto_compaction("false", dbFragmentThresholdPercentage=percent_threshold, viewFragmntThresholdPercentage=None)

        if not output and (percent_threshold <= MIN_COMPACTION_THRESHOLD or percent_threshold >= MAX_COMPACTION_THRESHOLD):
            self.assertFalse(output, "it should be  impossible to set compaction value = {0}%".format(percent_threshold))
            import json
            self.assertTrue(json.loads(rq_content).has_key("errors"), "Error is not present in response")
            self.assertTrue(str(json.loads(rq_content)["errors"]).find("Allowed range is 2 - 100") > -1, \
                            "Error 'Allowed range is 2 - 100' expected, but was '{0}'".format(str(json.loads(rq_content)["errors"])))

            self.log.info("Response contains error = '%(errors)s' as expected" % json.loads(rq_content))
        elif (output and percent_threshold >= MIN_COMPACTION_THRESHOLD
                     and percent_threshold <= MAX_RUN):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
            info = rest.get_nodes_self()

            available_ram = info.memoryQuota * (node_ram_ratio) / 2
            items = (int(available_ram * 1000) / 2) / item_size
            print "ITEMS =============%s" % items
            rest.create_bucket(bucket=bucket_name, ramQuotaMB=int(available_ram), authType='sasl',
                               saslPassword='password', replicaNumber=1, proxyPort=11211)
            BucketOperationHelper.wait_for_memcached(serverInfo, bucket_name)
            BucketOperationHelper.wait_for_vbuckets_ready_state(serverInfo, bucket_name)

            self.log.info("start to load {0}K keys with {1} bytes/key".format(items, item_size))
            #self.insert_key(serverInfo, bucket_name, items, item_size)
            generator = BlobGenerator('compact', 'compact-', int(item_size), start=0, end=(items * 1000))
            self._load_all_buckets(self.master, generator, "create", 0, 1)
            self.log.info("sleep 10 seconds before the next run")
            time.sleep(10)

            self.log.info("start to update {0}K keys with smaller value {1} bytes/key".format(items,
                                                                             int(update_item_size)))

            generator_update = BlobGenerator('compact', 'compact-', int(update_item_size), start=0, end=(items * 1000))
            insert_thread = Thread(target=self.load,
                                   name="insert",
                                   args=(self.master, self.autocompaction_value,
                                         self.default_bucket_name, generator_update))
            try:
                insert_thread.start()
                compact_run = remote_client.wait_till_compaction_end(rest, bucket_name,
                                                                     timeout_in_seconds=(self.wait_timeout * 50))
                if not compact_run:
                    self.fail("auto compaction does not run")
                elif compact_run:
                    self.log.info("auto compaction run successfully")
            except Exception, ex:
                if str(ex).find("enospc") != -1:
                    self.is_crashed.set()
                    self.log.error("Disk is out of space, unable to load more data")
                    insert_thread._Thread__stop()
                else:
                    insert_thread._Thread__stop()
                    raise ex
            else:
                insert_thread.join()
        else:
            self.log.error("Unknown error")


    def _viewFragmentationThreshold(self):
        for serverInfo in self.servers:
            self.log.info(serverInfo)
            rest = RestConnection(serverInfo)
            rest.set_auto_compaction(dbFragmentThresholdPercentage=80, viewFragmntThresholdPercentage=80)
