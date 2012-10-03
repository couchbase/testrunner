import json
import time
from basetestcase import BaseTestCase
from couchbase.documentgenerator import BlobGenerator

class BucketFlushTests(BaseTestCase):

    def setUp(self):
        super(BucketFlushTests, self).setUp()
        self.nodes_in = self.input.param("nodes_in", 0)
        self.value_size = self.input.param("value_size", 256)
        self.gen_create = BlobGenerator('bucketflush', 'bucketflush-', self.value_size, end=self.num_items)

    def tearDown(self):
        super(BucketFlushTests, self).tearDown()

    """Helper function to perform initial default setup for the tests"""
    def default_test_setup(self):

        if self.nodes_in:
            servs_in = self.servers[1:self.nodes_in + 1]
            self.cluster.rebalance([self.master], servs_in, [])

        self._load_all_buckets(self.master, self.gen_create, "create", 0)
        self.persist_and_verify()

    """Helper function to wait for persistence and then verify data/stats on all buckets"""
    def persist_and_verify(self):

        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in + 1])
        self._verify_all_buckets(self.master, max_verify=self.max_verify)
        self._verify_stats_all_buckets(self.servers[:self.nodes_in + 1])

    """Basic test for bucket flush functionality. Test loads data in bucket and then calls Flush. Verify curr_items=0 after flush.
        Works with multiple nodes/buckets."""
    def bucketflush(self):

        self.default_test_setup()

        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket)

        for bucket in self.buckets:
            self.cluster.wait_for_stats(self.servers[:self.nodes_in + 1], bucket, '', 'curr_items', '==', 0)

    """Test case for empty bucket. Work with multiple nodes/buckets."""
    def bucketflush_empty(self):

        self.default_test_setup()

        self._load_all_buckets(self.master, self.gen_create, "delete", 0)
        self.persist_and_verify()

        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket)

        self._load_all_buckets(self.master, self.gen_create, "create", 0)
        self.persist_and_verify()
