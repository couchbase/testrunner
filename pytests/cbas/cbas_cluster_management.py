from .cbas_base import *
from membase.api.rest_client import RestHelper
from couchbase_cli import CouchbaseCLI

class CBASClusterManagement(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket":False})
        super(CBASClusterManagement, self).setUp(add_defualt_cbas_node = False)
        self.assertTrue(len(self.cbas_servers)>=1, "There is no cbas server running. Please provide 1 cbas server atleast.")
        
    def setup_cbas_bucket_dataset_connect(self, cb_bucket, num_docs):
        # Create bucket on CBAS
        self.assertTrue(self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                       cb_bucket_name=cb_bucket), "bucket creation failed on cbas")
        
        self.assertTrue(self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                          cbas_dataset_name=self.cbas_dataset_name), "dataset creation failed on cbas")
        
        self.assertTrue(self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name), "Connecting cbas bucket to cb bucket failed")
        
        self.assertTrue(self.wait_for_ingestion_complete([self.cbas_dataset_name], num_docs), "Data ingestion to cbas couldn't complete in 300 seconds.")
        
        return True
    
    def test_add_cbas_node_one_by_one(self):
        '''
        Description: Add cbas nodes 1 by 1 and rebalance on every add.
        
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them 1by1 and Rebalance.
        
        Author: Ritesh Agarwal
        '''
        nodes_before = len(self.rest.get_nodes_data_from_cluster())
        added = 0
        for node in self.cbas_servers:
            if node.ip != self.master.ip:
                self.add_node(node=node, rebalance=True)
                added += 1
        nodes_after = len(self.rest.get_nodes_data_from_cluster())
        self.assertTrue(nodes_before+added == nodes_after, "While adding cbas nodes seems like some nodes were removed during rebalance.")
        
    def test_add_all_cbas_nodes_in_cluster(self):
        '''
        Description: Add all cbas nodes and then rebalance.

        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them in one go and Rebalance.
        
        Author: Ritesh Agarwal
        '''
        self.add_all_cbas_node_then_rebalance()
#         
    def test_add_remove_all_cbas_nodes_in_cluster(self):
        '''
        Description: First add all cbas nodes and then rebalance. Remove all added cbas node, rebalance.
        
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them in one go and Rebalance.
        2. Remove all nodes together and then rebalance.
        
        Author: Ritesh Agarwal
        '''
        cbas_otpnodes = self.add_all_cbas_node_then_rebalance()
        self.remove_all_cbas_node_then_rebalance(cbas_otpnodes)

    def test_concurrent_sevice_existence_with_cbas(self):
        '''
        Description: Test add/remove nodes via REST APIs.
        
        Steps:
        1. Add nodes by randomly picking up the services from the service_list.
        2. Check that correct services are running after the node is added.
        
        Author: Ritesh Agarwal
        '''
        service_list = [["kv", "cbas", "index", "n1ql"],
                        ["cbas", "n1ql", "index"],
                        ["kv", "cbas", "n1ql"],
                        ["n1ql", "cbas", "fts"]
                        ]
        for cbas_server in self.servers:
            if cbas_server.ip == self.master.ip:
                continue
            from random import randint
            service = service_list[randint(0, len(service_list)-1)]
            self.log.info("Adding %s to the cluster with services %s"%(cbas_server, service))
            otpNode = self.add_node(node=cbas_server, services=service)
            
            '''Check for the correct services alloted to the nodes.'''
            nodes = self.rest.get_nodes_data_from_cluster()
            for node in nodes:
                if node["otpNode"] == otpNode.id:
                    self.assertTrue(set(node["services"]) == set(service), "Service setting failed") 
                    self.log.info("Successfully added %s to the cluster with services %s"%(otpNode.id, service))
                    
    def test_add_delete_cbas_nodes_CLI(self):
        '''
        Description: Test add/remove nodes via CLI.
        
        Steps:
        1. Add nodes by randomly picking up the services from the service_list.
        2. Check that correct services are running after the node is added.
        
        Author: Ritesh Agarwal
        '''
        service_list = {"data,analytics,index": ["kv", "cbas", "index"],
                        "analytics,query,index": ["cbas", "n1ql", "index"],
                        "data,analytics,query": ["kv", "cbas", "n1ql"],
                        "analytics,query,fts": ["cbas", "n1ql", "fts"],
                        }
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            import random
            service = random.choice(list(service_list.keys()))
            self.log.info("Adding %s to the cluster with services %s to cluster %s"%(cbas_server, service, self.master))
            
            stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).server_add(cbas_server.ip+":"+cbas_server.port, cbas_server.rest_username, cbas_server.rest_password, None, service, None)
            self.assertTrue(result, "Server %s is not added to the cluster %s . Error: %s"%(cbas_server, self.master, stdout+stderr))
            self.rebalance()
            
            '''Check for the correct services alloted to the nodes.'''
            nodes = self.rest.get_nodes_data_from_cluster()
            for node in nodes:
                if node["otpNode"].find(cbas_server.ip) != -1:
                    actual_services = set(node["services"])
                    expected_servcies = set(service_list[service])
                    self.log.info("Expected:%s Actual:%s"%(expected_servcies, actual_services))
                    self.assertTrue(actual_services == expected_servcies, "Service setting failed") 
                    self.log.info("Successfully added %s to the cluster with services %s"%(node["otpNode"], service))
        
        to_remove = []
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            else:
                to_remove.append(cbas_server.ip)
        self.log.info("Removing: %s from the cluster: %s"%(to_remove, self.master))
        stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).rebalance(",".join(to_remove))
        if not result:
            self.log.info(15*"#"+"THIS IS A BUG: MB-24968. REMOVE THIS TRY-CATCH ONCE BUG IS FIXED."+15*"#")
            stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).rebalance(",".join(to_remove))
        self.assertTrue(result, "Server %s are not removed from the cluster %s . Console Output: %s , Error: %s"%(to_remove, self.master, stdout, stderr))

    def test_add_another_cbas_node_rebalance(self):
        set_up_cbas = False
        wait_for_rebalance = True
        test_docs = self.num_items
        docs_to_verify = test_docs
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(test_docs, "create", 0, test_docs)

        if self.cbas_node.ip == self.master.ip:
            set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", docs_to_verify)
            wait_for_rebalance = False
        i = 1
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            from random import randint
            service = ["kv", "cbas"]
            self.log.info("Adding %s to the cluster with services %s"%(cbas_server, service))
            self.add_node(node=cbas_server, services=service, wait_for_rebalance_completion=wait_for_rebalance)
            
            if not set_up_cbas:
                set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", docs_to_verify)
                wait_for_rebalance = False
            
            # Run some queries while rebalance is in progress after adding further cbas nodes    
            self.assertTrue((self.get_num_items_in_cbas_dataset(self.cbas_dataset_name))[0] == docs_to_verify,
                            "Number of items in CBAS is different from CB after adding further cbas node.")
            
#             self.disconnect_from_bucket(self.cbas_bucket_name)
            self.perform_doc_ops_in_all_cb_buckets(test_docs, "create", test_docs*i, test_docs*(i+1))
#             self.connect_to_bucket(self.cbas_bucket_name, self.cb_bucket_name)
            
#             if self.rest._rebalance_progress_status() == 'running':
#                 self.assertTrue((self.get_num_items_in_cbas_dataset(self.cbas_dataset_name))[0] == docs_to_verify,
#                             "Number of items in CBAS is different from CB after adding further cbas node.")
            
            docs_to_verify = docs_to_verify + test_docs
            # Wait for the rebalance to be completed.
            result = self.rest.monitorRebalance()
            self.assertTrue(result, "Rebalance operation failed after adding %s cbas nodes,"%self.cbas_servers)
            self.log.info("successfully rebalanced cluster {0}".format(result))
            
            self.assertTrue(self.wait_for_ingestion_complete([self.cbas_dataset_name], docs_to_verify, 300),
                            "Data ingestion could'nt complete after rebalance completion.")
            i+=1
            
    def test_add_cbas_rebalance_runqueries(self):
        '''
        Description: Add CBAS node, rebalance. Run concurrent queries.
        
        Steps:
        1. Add cbas node then do rebalance.
        2. Once rebalance is completed, on cbas node connect to bucket, create shadows.
        3. Data ingestion should start. Run queries.
        
        Author: Ritesh Agarwal
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        self.add_node(node=self.cbas_node)
        self.setup_cbas_bucket_dataset_connect("default", self.num_items)
        self._run_concurrent_queries(query, "immediate", 500)
        
    def test_add_data_rebalance_runqueries(self):
        '''
        Description: Add data node rebalance. During rebalance setup cbas. Run concurrent queries.
        
        Steps:
        1. Add data node then do rebalance.
        2. While rebalance is happening, on cbas node connect to bucket, create shadows and Run queries.
        
        Author: Ritesh Agarwal
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        self.add_node(node=self.cbas_node)
        self.add_node(node=self.kv_servers[1], wait_for_rebalance_completion=False)
        self.setup_cbas_bucket_dataset_connect("default", self.num_items)
        self._run_concurrent_queries(query, "immediate", 500)
        
    def test_all_cbas_node_running_queries(self):
        '''
        Description: Test that all the cbas nodes are capable to serve queries.
        
        Steps:
        1. Perform doc operation on the KV node.
        2. Add 1 cbas node and setup cbas.
        3. Add all other cbas nodes.
        4. Verify all cbas nodes should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        set_up_cbas = False
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        if self.cbas_node.ip == self.master.ip:
            set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", self.num_items)
            self._run_concurrent_queries(query, "immediate", 1000, RestConnection(self.cbas_node))
            
        for node in self.cbas_servers:
            if node.ip != self.master.ip:
                self.add_node(node=node)
                if not set_up_cbas:
                    set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", self.num_items)
                self._run_concurrent_queries(query, "immediate", 1000, RestConnection(node))

    def test_add_first_cbas_restart_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance.
        3. While rebalance is in progress, stop rebalancing. Again start rebalance
        4. Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node, services=["kv", "cbas"], wait_for_rebalance_completion=False)
        
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.rebalance()
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)

    def test_add_data_node_cancel_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node. Start rebalance.
        2. Create bucket, datasets, connect bucket. Data ingestion should start.
        3. Add another data node. Rebalance, while rebalance is in progress, stop rebalancing.
        4. Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node)
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        self.add_node(self.kv_servers[1], wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.assertTrue(self.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count), "Data loss in CBAS.")
    
    
    def test_add_data_node_restart_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node. Start rebalance.
        2. Create bucket, datasets, connect bucket. Data ingestion should start.
        3. Add another data node. Rebalance, while rebalance is in progress, stop rebalancing. Again start rebalance.
        4. Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node)
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        self.add_node(self.kv_servers[1], wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.rebalance()
        self.assertTrue(self.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count), "Data loss in CBAS.")
        
    def test_add_first_cbas_stop_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance.
        3. While rebalance is in progress, stop rebalancing.
        4. Verify that the cbas node is not added to the cluster and should not accept queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node, services=["kv", "cbas"], wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.assertFalse(self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                       cb_bucket_name="travel-sample"), "bucket creation failed on cbas")

    def test_add_second_cbas_stop_rebalance(self):
        '''
        Description: This test will add the second cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Add another cbas node, rebalance and while rebalance is in progress, stop rebalancing.
        4. Verify that the second cbas node is not added to the cluster and should not accept queries.
        5. First cbas node should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[0], services=["kv", "cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        self.add_node(self.cbas_servers[1], services=["kv", "cbas"], wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
#         self.assertFalse(self.execute_statement_on_cbas_via_rest(query, rest=RestConnection(self.cbas_servers[1])),
#                          "Successfully executed a cbas query from a node which is not part of cluster.")
        
        self.assertTrue(self.execute_statement_on_cbas_via_rest(query, rest=RestConnection(self.cbas_servers[0])),
                         "Successfully executed a cbas query from a node which is not part of cluster.")
    
    def test_reboot_cbas(self):
        '''
        Description: This test will add the second cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Create bucket, datasets, connect bucket. Data ingestion should start.
        4. Reboot CBAS node addd in Step 1.
        5. After reboot cbas node should be able to serve queries, validate items count.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node, services=["kv", "cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        from fts.fts_base import NodeHelper
        NodeHelper.reboot_server(self.cbas_node, self)
        
        self.assertTrue(self.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count), "Data loss in CBAS.")

    def test_restart_cb(self):
        '''
        Description: This test will restart CB and verify that CBAS is also up and running with CB.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Stop Couchbase service, Start Couchbase Service. Wait for service to get started.
        4. Verify that CBAS service is also up Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[0], services=["cbas"])
        
        from fts.fts_base import NodeHelper
        NodeHelper.stop_couchbase(self.cbas_servers[0])
        NodeHelper.start_couchbase(self.cbas_servers[0])
        NodeHelper.wait_service_started(self.cbas_servers[0])
        
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        self.assertTrue(self.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count), "Data loss in CBAS.")

    def test_run_queries_cbas_shutdown(self):
        '''
        Description: This test the ongoing queries while cbas node goes down.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Create bucket, datasets, connect bucket. Data ingestion should start.
        4. Add another cbas node, rebalance.
        5. Start concurrent queries on first cbas node.
        6. Second cbas node added in step 4 should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        otpNode = self.add_node(self.cbas_servers[0], services=["cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[1], services=["cbas"])
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self._run_concurrent_queries(query, "immediate", 2000, rest=RestConnection(self.cbas_servers[0]))
        
        from fts.fts_base import NodeHelper
        NodeHelper.stop_couchbase(self.cbas_servers[0])
        self.rest.fail_over(otpNode=otpNode.id)
        self.rebalance()
        NodeHelper.start_couchbase(self.cbas_servers[0])
        NodeHelper.wait_service_started(self.cbas_servers[0])
        
    def test_primary_cbas_shutdown(self):
        '''
        Description: This test will add the second cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Create bucket, datasets, connect bucket. Data ingestion should start.
        4. Add another cbas node, rebalance.
        5. Stop Couchbase service for Node1 added in step 1. Failover the node and rebalance.
        6. Second cbas node added in step 4 should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        otpNode = self.add_node(self.cbas_servers[0], services=["cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[1], services=["cbas"])
        from fts.fts_base import NodeHelper
        NodeHelper.stop_couchbase(self.cbas_servers[0])
        self.rest.fail_over(otpNode=otpNode.id)
        self.rebalance()
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self._run_concurrent_queries(query, "immediate", 100, rest=RestConnection(self.cbas_servers[1]))
        NodeHelper.start_couchbase(self.cbas_servers[0])
        NodeHelper.wait_service_started(self.cbas_servers[0])
        
    def test_remove_all_cbas_nodes_in_cluster_add_last_node_back(self):
        '''
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them in one go and Rebalance.
        2. Remove all nodes together and then rebalance.
        
        Author: Ritesh Agarwal
        '''
        cbas_otpnodes = []
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        cbas_otpnodes.append(self.add_node(self.cbas_servers[0], services=["cbas"]))
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        for node in self.cbas_servers[1:]:
            cbas_otpnodes.append(self.add_node(node, services=["cbas"]))
        cbas_otpnodes.reverse()
        for node in cbas_otpnodes:
            self.remove_node([node])
            
        self.add_node(self.cbas_servers[0], services=["cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
            
    def test_create_bucket_with_default_port(self):
        query = "create bucket " + self.cbas_bucket_name + " with {\"name\":\"" + self.cb_bucket_name + "\",\"nodes\":\"" + self.master.ip + ":" +"8091" +"\"};"
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[0], services=["cbas"])
        result = self.execute_statement_on_cbas_via_rest(query, "immediate")[0]
        self.assertTrue(result == "success", "CBAS bucket cannot be created with provided port: %s"%query)
        
        self.assertTrue(self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                          cbas_dataset_name=self.cbas_dataset_name), "dataset creation failed on cbas")
        self.assertTrue(self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name, cb_bucket_password="password", cb_bucket_username="Administrator"),
                        "Connecting cbas bucket to cb bucket failed")
        self.assertTrue(self.wait_for_ingestion_complete([self.cbas_dataset_name], self.travel_sample_docs_count), "Data ingestion to cbas couldn't complete in 300 seconds.")
