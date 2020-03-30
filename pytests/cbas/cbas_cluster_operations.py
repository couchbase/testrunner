# -*- coding: utf-8 -*-
from .cbas_base import *
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection


class CBASClusterOperations(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        self.rebalanceServers = None
        self.nodeType = "KV"        
        self.wait_for_rebalance=True
        super(CBASClusterOperations, self).setUp()
        
        self.create_default_bucket()

        if 'nodeType' in self.input.test_params:
            self.nodeType = self.input.test_params['nodeType']
        
        if self.nodeType == "KV":
            self.rebalanceServers = self.kv_servers
            self.wait_for_rebalance=False
        elif self.nodeType == "CBAS":
            self.rebalanceServers = [self.cbas_node] + self.cbas_servers
            
        self.assertTrue(len(self.rebalanceServers)>1, "Not enough %s servers to run tests."%self.rebalanceServers)
        self.log.info("This test will be running in %s context."%self.nodeType)
        
    def tearDown(self):
        super(CBASClusterOperations, self).tearDown()

    def setup_for_test(self, skip_data_loading=False):
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                                   self.num_items)

        # Create bucket on CBAS
        self.assertTrue(self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip), "bucket creation failed on cbas")

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
        '''
        Description: This will test the rebalance in feature i.e. one node coming in to the cluster.
        Then Rebalance. Verify that is has no effect on the data ingested to cbas.
        
        Steps:
        1. Setup cbas. bucket, datasets/shadows, connect.
        2. Add a node and rebalance. Don't wait for rebalance completion.
        3. During rebalance, do mutations and execute queries on cbas.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 18/07/2017
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        self.setup_for_test()
        self.add_node(node=self.rebalanceServers[1], rebalance=True, wait_for_rebalance_completion=self.wait_for_rebalance)
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)
        
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        self._run_concurrent_queries(query, "immediate", 2000)
        
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_out(self):
        '''
        Description: This will test the rebalance out feature i.e. one node going out of cluster.
        Then Rebalance.
        
        Steps:
        1. Add a node, Rebalance.
        2. Setup cbas. bucket, datasets/shadows, connect.
        3. Remove a node and rebalance. Don't wait for rebalance completion.
        4. During rebalance, do mutations and execute queries on cbas.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 18/07/2017
        '''
        self.add_node(node=self.rebalanceServers[1])
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()
        otpnodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if node.ip == self.rebalanceServers[1].ip:
                otpnodes.append(node)
        self.remove_node(otpnodes, wait_for_rebalance=self.wait_for_rebalance)
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        self._run_concurrent_queries(query, "immediate", 2000)

        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_swap_rebalance(self):
        '''
        Description: This will test the swap rebalance feature i.e. one node going out and one node coming in cluster.
        Then Rebalance. Verify that is has no effect on the data ingested to cbas.
        
        Steps:
        1. Setup cbas. bucket, datasets/shadows, connect.
        2. Add a node that is to be swapped against the leaving node. Do not rebalance.
        3. Remove a node and rebalance.
        4. During rebalance, do mutations and execute queries on cbas.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 20/07/2017
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()
        
        otpnodes=[]
        nodes = self.rest.node_statuses()
        if self.nodeType == "KV":
            service = ["kv"]
        else:
            service = ["cbas"]
        otpnodes.append(self.add_node(node=self.servers[1], services=service))
        self.add_node(node=self.servers[3], services=service, rebalance=False)
        self.remove_node(otpnodes, wait_for_rebalance=self.wait_for_rebalance)
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        self._run_concurrent_queries(query, "immediate", 2000)

        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_failover(self):
        '''
        Description: This will test the node failover both graceful and hard failover based on
        graceful_failover param in testcase conf file.
        
        Steps:
        1. Add node to the cluster which will be failed over.
        2. Create docs, setup cbas.
        3. Mark the node for fail over.
        4. Do rebalance asynchronously. During rebalance perform mutations.
        5. Run some CBAS queries.
        6. Check for correct number of items in CBAS datasets.
        
        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 20/07/2017
        '''
        
        #Add node which will be failed over later.
        self.add_node(node=self.rebalanceServers[1])
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        graceful_failover = self.input.param("graceful_failover", False)
        self.setup_for_test()
        failover_task = self._cb_cluster.async_failover(self.input.servers,
                                                        [self.rebalanceServers[1]],
                                                        graceful_failover)
        failover_task.result()
        
        self.rebalance()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 3 // 2)

        self._run_concurrent_queries(query, "immediate", 2000)

        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 // 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")
