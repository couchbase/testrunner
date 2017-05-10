from cbas_base import *
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection


class CBASClusterOperations(CBASBaseTest):
    def setUp(self):
        super(CBASClusterOperations, self).setUp()

    def tearDown(self):
        super(CBASClusterOperations, self).tearDown()

    def setup_for_test(self, skip_data_loading=False):
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                                   self.num_items)

        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        if not skip_data_loading:
            # Validate no. of items in CBAS dataset
            if not self.validate_cbas_dataset_items_count(
                    self.cbas_dataset_name,
                    self.num_items):
                self.fail(
                    "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_in(self):
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        self.setup_for_test()
        self._cb_cluster.async_rebalance([self.master], [self.input.servers[1]],
                                         [])

        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        self.async_query_execute(query, "immediate", 2000)

        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_out(self):
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        self.setup_for_test()
        self._cb_cluster.async_rebalance([self.master], [], [self.input.servers[1]])

        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        self.async_query_execute(query, "immediate", 2000)

        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_swap_rebalance(self):
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        self.setup_for_test()
        self._cb_cluster.async_rebalance([self.master], [self.input.servers[2]],
                                         [self.input.servers[1]])

        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        self.async_query_execute(query, "immediate", 2000)

        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_failover(self):
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        graceful_failover = self.input.param("graceful_failover", False)
        self.setup_for_test()
        failover_task = self._cb_cluster.async_failover(self.input.servers,
                                                        [self.input.servers[1]],
                                                        graceful_failover)
        failover_task.result()
        self._cb_cluster.async_rebalance([self.master], [],
                                         [self.input.servers[1]])

        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 3 / 2)

        self.async_query_execute(query, "immediate", 2000)

        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 / 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")
