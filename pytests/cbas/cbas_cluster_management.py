from cbas_base import *
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
                       cb_bucket_name=cb_bucket,
                       cb_server_ip=self.cbas_node.ip),"bucket creation failed on cbas")
        
        self.assertTrue(self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                          cbas_dataset_name=self.cbas_dataset_name), "dataset creation failed on cbas")
        
        self.assertTrue(self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                   cb_bucket_password=self.cb_bucket_password),"Connecting cbas bucket to cb bucket failed")
        
        self.assertTrue(self.wait_for_ingestion_complete([self.cbas_dataset_name], num_docs),"Data ingestion to cbas couldn't complete in 300 seconds.")
        
        return True
    
    def test_add_cbas_node_one_by_one(self):
        '''
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them 1by1 and Rebalance.
        '''
        nodes_before = len(self.rest.get_nodes_data_from_cluster())
        added = 0
        for node in self.cbas_servers:
            if node.ip != self.master.ip:
                self.add_node(node=node,rebalance=True)
                added += 1
        nodes_after = len(self.rest.get_nodes_data_from_cluster())
        self.assertTrue(nodes_before+added == nodes_after, "While adding cbas nodes seems like some nodes were removed during rebalance.")
        
    def test_add_all_cbas_nodes_in_cluster(self):
        self.add_all_cbas_node_then_rebalance()
#         
    def test_add_remove_all_cbas_nodes_in_cluster(self):
        '''
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them in one go and Rebalance.
        2. Remove all nodes together and then rebalance.
        '''
        cbas_otpnodes = self.add_all_cbas_node_then_rebalance()
        self.remove_all_cbas_node_then_rebalance(cbas_otpnodes)

    def test_concurrent_sevice_existence_with_cbas(self):
        '''
        Steps:
        1. Add nodes by randomly picking up the services from the service_list.
        2. Check that correct services are running after the node is added.
        '''
        service_list = [["kv","cbas","index","n1ql"],
                        ["cbas","n1ql","index"],
                        ["kv","cbas","n1ql"],
                        ["n1ql","cbas","fts"]
                        ]
        for cbas_server in self.servers:
            if cbas_server.ip == self.master.ip:
                continue
            from random import randint
            service = service_list[randint(0, len(service_list)-1)]
            self.log.info("Adding %s to the cluster with services %s"%(cbas_server,service))
            otpNode = self.add_node(node=cbas_server,services=service)
            
            '''Check for the correct services alloted to the nodes.'''
            nodes = self.rest.get_nodes_data_from_cluster()
            for node in nodes:
                if node["otpNode"] == otpNode.id:
                    self.assertTrue(set(node["services"]) == set(service), "Service setting failed") 
                    self.log.info("Successfully added %s to the cluster with services %s"%(otpNode.id,service))
                    
    def test_add_delete_cbas_nodes_CLI(self):
        service_list = {"data,analytics,index":["kv","cbas","index"],
                        "analytics,query,index":["cbas","n1ql","index"],
                        "data,analytics,query":["kv","cbas","n1ql"],
                        "analytics,query,fts":["cbas","n1ql","fts"],
                        }
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            import random
            service = random.choice(service_list.keys())
            self.log.info("Adding %s to the cluster with services %s to cluster %s"%(cbas_server,service,self.master))
            
            stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).server_add(cbas_server.ip+":"+cbas_server.port, cbas_server.rest_username, cbas_server.rest_password, None, service, None)
            self.assertTrue(result, "Server %s is not added to the cluster %s . Error: %s"%(cbas_server,self.master,stdout+stderr))
            self.rebalance()
            
            '''Check for the correct services alloted to the nodes.'''
            nodes = self.rest.get_nodes_data_from_cluster()
            for node in nodes:
                if node["otpNode"].find(cbas_server.ip) != -1:
                    actual_services = set(node["services"])
                    expected_servcies = set(service_list[service])
                    self.log.info("Expected:%s Actual:%s"%(expected_servcies,actual_services))
                    self.assertTrue(actual_services == expected_servcies, "Service setting failed") 
                    self.log.info("Successfully added %s to the cluster with services %s"%(node["otpNode"],service))
        
        to_remove = []
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            else:
                to_remove.append(cbas_server.ip)
        self.log.info("Removing: %s from the cluster: %s"%(to_remove,self.master))
        stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).rebalance(",".join(to_remove))
        if not result:
            self.log.info(15*"#"+"THIS IS A BUG: MB-24968. REMOVE THIS TRY-CATCH ONCE BUG IS FIXED."+15*"#")
            stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).rebalance(",".join(to_remove))
        self.assertTrue(result, "Server %s are not removed from the cluster %s . Console Output: %s , Error: %s"%(to_remove,self.master,stdout,stderr))

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
            service = ["kv","cbas"]
            self.log.info("Adding %s to the cluster with services %s"%(cbas_server,service))
            self.add_node(node=cbas_server,services=service,wait_for_rebalance_completion=wait_for_rebalance)
            
            if not set_up_cbas:
                set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", docs_to_verify)
                wait_for_rebalance = False
            
            # Run some queries while rebalance is in progress after adding further cbas nodes    
            self.assertTrue((self.get_num_items_in_cbas_dataset(self.cbas_dataset_name))[0] == docs_to_verify,
                            "Number of items in CBAS is different from CB after adding further cbas node.")
            
#             self.disconnect_from_bucket(self.cbas_bucket_name)
            self.perform_doc_ops_in_all_cb_buckets(test_docs, "create", test_docs*i, test_docs*(i+1))
#             self.connect_to_bucket(self.cbas_bucket_name, self.cb_bucket_name)
            
            if self.rest._rebalance_progress_status() == 'running':
                self.assertTrue((self.get_num_items_in_cbas_dataset(self.cbas_dataset_name))[0] == docs_to_verify,
                            "Number of items in CBAS is different from CB after adding further cbas node.")
            
            docs_to_verify = docs_to_verify + test_docs
            # Wait for the rebalance to be completed.
            try:
                result = self.rest.monitorRebalance()
                self.assertTrue(result, "Rebalance operation failed after adding %s cbas nodes,"%self.cbas_servers)
                self.log.info("successfully rebalanced cluster {0}".format(result))
            except Exception as e:
                self.log.info(15*"#"+"THIS IS A BUG: MB-25006. REMOVE THIS TRY-CATCH ONCE BUG IS FIXED."+15*"#")
                pass
            
            self.assertTrue(self.wait_for_ingestion_complete([self.cbas_dataset_name], docs_to_verify, 300),
                            "Data ingestion could'nt complete after rebalance completion.")
            i+=1
            
    def test_add_cbas_rebalance_runqueries(self):
        '''
        Steps:
        1. Add cbas node then do rebalance.
        2. Once rebalance is completed, on cbas node connect to bucket, create shadows.
        3. Data ingestion should start. Run queries.
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        self.add_node(node=self.cbas_node)
        self.setup_cbas_bucket_dataset_connect("default", self.num_items)
        self._run_concurrent_queries(query,"immediate",500)
        
    def test_add_data_rebalance_runqueries(self):
        '''
        Steps:
        1. Add data node then do rebalance.
        2. While rebalance is happening, on cbas node connect to bucket, create shadows and Run queries.
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        self.add_node(node=self.cbas_node)
        self.add_node(node=self.kv_servers[1],wait_for_rebalance_completion=False)
        self.setup_cbas_bucket_dataset_connect("default", self.num_items)
        self._run_concurrent_queries(query,"immediate",500)
        
    def test_all_cbas_node_running_queries(self):
        set_up_cbas = False
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        if self.cbas_node.ip == self.master.ip:
            set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", self.num_items)
            self._run_concurrent_queries(query,"immediate",1000,RestConnection(self.cbas_node))
            
        for node in self.cbas_servers+[self.cbas_node]:
            if node.ip != self.master.ip:
                self.add_node(node=node,rebalance=True)
                if not set_up_cbas:
                    set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", self.num_items)
                self._run_concurrent_queries(query,"immediate",1000,RestConnection(node))
