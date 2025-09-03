import json
import time

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError
from threading import Thread
from memcached.helper.data_helper import MemcachedClientHelper


class BucketFlushTests(BaseTestCase):

    def setUp(self):
        super(BucketFlushTests, self).setUp()
        self.nodes_in = self.input.param("nodes_in", 0)
        self.value_size = self.input.param("value_size", 256)
        self.data_op = self.input.param("data_op", "create")
        self.use_ascii = self.input.param("use_ascii", "False")
        self.gen_create = BlobGenerator('bucketflush', 'bucketflush-',
                                        self.value_size, end=self.num_items)
        try:
            self.default_test_setup()
        except Exception as e:
            self.tearDown()
            self.fail(e)

    def tearDown(self):
        super(BucketFlushTests, self).tearDown()

    """Helper function to perform initial default setup for the tests"""
    def default_test_setup(self, load_data=True):

        if self.nodes_in:
            servs_in = self.servers[1:self.nodes_in + 1]
            self.cluster.rebalance([self.master], servs_in, [])

        if load_data:
            self._load_all_buckets(self.master, self.gen_create, "create", 0)
            self.persist_and_verify()

    """Helper function to wait for persistence and then verify data/stats on all buckets"""
    def persist_and_verify(self):
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in + 1])
        self._verify_all_buckets(self.master, max_verify=self.max_verify,
                                 timeout=360)
        self._verify_stats_all_buckets(self.servers[:self.nodes_in + 1])

    """Basic test for bucket flush functionality. Test loads data in bucket and then calls Flush. Verify curr_items=0 after flush.
        Works with multiple nodes/buckets."""
    def bucketflush(self):
        # MB-18068 known issue with bucket flush
        time.sleep(5)
        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket)

        for bucket in self.buckets:
            self.cluster.wait_for_stats(self.servers[:self.nodes_in + 1],
                                        bucket, '', 'curr_items', '==', 0)

    """Test case for empty bucket. Work with multiple nodes/buckets."""
    def bucketflush_empty(self):

        self._load_all_buckets(self.master, self.gen_create, "delete", 0)
        self.persist_and_verify()

        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket)

        self._load_all_buckets(self.master, self.gen_create, "create", 0)
        self.persist_and_verify()

    """Test case to check client behavior with bucket flush while loading/updating/deleting data"""
    def bucketflush_with_data_ops(self):
        try:
            tasks = self._async_load_all_buckets(self.master, self.gen_create,
                                                 self.data_op, 0)
            for bucket in self.buckets:
                self.cluster.bucket_flush(self.master, bucket)
            for task in tasks:
                task.result()
        except MemcachedError as exp:
            self.assertEqual(exp.status, 134,
                             msg="Unexpected Exception - {0}".format(exp))
            self.log.info("Expected Exception Caught - {0}".format(exp))
        except Exception as exp:
            self.log.info("Unxpected Exception Caught - {0}".format(exp))
            self.fail("Unexpected exception caught- {0}".format(exp))
