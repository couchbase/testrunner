import unittest
import time
import uuid
import logger
import copy
from TestInput import TestInputSingleton
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.api.rest_client import RestConnection, Bucket
from memcached.helper.data_helper import VBucketAwareMemcached
from couchbase_helper.documentgenerator import BlobGenerator, KVGenerator
from basetestcase import BaseTestCase
from xdcr.xdcrbasetests import XDCRReplicationBaseTest

class UTF_16_Generator(KVGenerator):
    def __init__(self, name, seed, value_size, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.seed = seed
        self.value_size = value_size
        self.itr = self.start

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration

        key = self.name + str(self.itr)
        value = self.seed + str(self.itr)
        extra = self.value_size - len(value)
        if extra > 0:
            value += 'a' * extra
        self.itr += 1
        return key.encode("utf-16"), value.encode("utf-16")

class NegativeTests1(BaseTestCase):
    def setUp(self):
        if str(self.__class__).find('negativetests.NegativeTests1.invalid_buckets') != -1:
            self.log = logger.Logger.get_logger()
            self.input = TestUnputSingleton.input
            self.servers = self.input.servers
            self.buckets = []
            self.master = self.servers[0]
            self.cluster = Cluster()
            self.case_number = self.input.param("case_number", 0)
            # special setup for this test ?
        else:
            super(NegativeTests1, self).setUp()
        self.log.info("==============  NegativeTests1 setUp was started for test #{0} {1}=============="\
                .format(self.case_number, self._testMethodName))

    def tearDown(self):
        self.log.info("==============  NegativeTests1 tearDown was started for test #{0} {1}=============="\
                .format(self.case_number, self._testMethodName))
        if str(self.__class__).find('negativetests.NegativeTests1.invalid_buckets') != -1:
            # special teardown for this test ?
            pass
        else:
            super(NegativeTests1, self).tearDown()

    """
        This test creates 4 buckets with invalid names,
        expected result should be a denied error, the bucket
        names are: isasl.pw, ns_log, _replicator.couch.1, _users.couch.1
    """
    def invalid_buckets(self):
        # TO-DO: Wait for fix - MB-5844
        pass

    """
        This test is to create keys with utf-16 encoding rather than
        the traditional utf-8 encoding.
    """
    def test_utf_16_keys(self):
        gen_create = UTF_16_Generator('load', 'load_', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)

        self._wait_for_stats_all_buckets(self.servers)
        rest = RestConnection(self.servers[0])
        for bucket in self.buckets:
            item_count = rest.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
            if self.num_items != item_count:
                self.fail("Item count didn't match on bucket {0} => expected {1}:seen {2}"
                        .format(bucket.name, self.num_items, item_count))
            else:
                self.log.info("Expected item count seen on bucket {0}: {1}=={2}"
                        .format(bucket.name, self.num_items, item_count))

class NegativeTests2(XDCRReplicationBaseTest):
    def setUp(self):
        super(NegativeTests2, self).setUp()
        self.log.info("==============  Negatives2 setUp was started for test #{0} {1}=============="\
                .format(self.case_number, self._testMethodName))

    def tearDown(self):
        self.log.info("==============  Negatives2 tearDown was started for test #{0} {1}=============="\
                .format(self.case_number, self._testMethodName))
        super(NegativeTests2, self).tearDown()

    """
        This test is to create keys with utf-16 encoding rather than
        the traditional utf-8 encoding, with xdcr enabled.
    """
    def test_utf_16_keys_with_xdcr(self):
        gen_create = UTF_16_Generator('loadOne', 'loadOne_', self._value_size, end=self.num_items)
        self._load_all_buckets(self.src_master, gen_create, "create", 0)
        if "update" in self._doc_ops:
            gen_update = UTF_16_Generator('loadOne', 'loadOne_', self._value_size, start=0,
                    end=int(self.num_items * (float)(self._percent_update) / 100))
            self._load_all_buckets(self.src_master, gen_update, "update", self._expires)
        if "delete" in self._doc_ops:
            gen_delete = UTF_16_Generator('loadOne', 'loadOne_', self._value_size,
                    start=int((self.num_items) * (float)(100 - self._percent_delete) / 100),
                    end=self.num_items)
            self._load_all_buckets(self.src_master, gen_delete, "delete", 0)
        self._verify_item_count(self.src_master, self.src_nodes, timeout=120)

        if self._replication_direction_str == "bidirection":
            gen_create = UTF_16_Generator('loadTwo', 'loadTwo_', self._value_size, end=self.num_items)
            self._load_all_buckets(self.dest_master, gen_create, "create", 0)
            if "update" in self._doc_ops_dest:
                gen_update = UTF_16_Generator('loadTwo', 'loadTwo_', self._value_size, start=0,
                        end=int(self.num_items * (float)(self._percent_update) / 100))
                self._load_all_buckets(self.dest_master, gen_update, "update", self._expires)
            if "delete" in self._doc_ops_dest:
                gen_delete = UTF_16_Generator('loadTwo', 'loadTwo_', self._value_size,
                        start=int((self.num_items) * (float)(100 - self._percent_delete) / 100),
                        end=self.num_items)
                self._load_all_buckets(self.dest_master, gen_delete, "delete", 0)
            self._verify_item_count(self.dest_master, self.dest_nodes, timeout=120)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
        self.sleep(self.wait_timeout)
        self.verify_results()

