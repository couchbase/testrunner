import copy
import json
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL, HANDLER_CODE_ONDEPLOY
from pytests.eventing.eventing_base import EventingBaseTest
from membase.helper.cluster_helper import ClusterOperationHelper
import logging

log = logging.getLogger()


class EventingRebalanceCollection(EventingBaseTest):
    def setUp(self):
        super(EventingRebalanceCollection, self).setUp()
        self.hostname="http://qa.sc.couchbase.com/"
        self.create_n_scope(self.dst_bucket_name,5)
        self.create_n_scope(self.src_bucket_name,5)
        self.create_n_collections(self.dst_bucket_name,"scope_1",5)
        self.create_n_collections(self.src_bucket_name,"scope_1",5)
        self.handler_code="handler_code/ABO/insert_rebalance.js"
        force_disable_new_orchestration = self.input.param('force_disable_new_orchestration', False)
        if force_disable_new_orchestration:
            self.rest.diag_eval("ns_config:set(force_disable_new_orchestration, true).")
        if self.n1ql_server:
            query = "create primary index on dst_bucket.scope_1.coll_4"
            self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)

    def tearDown(self):
        super(EventingRebalanceCollection, self).tearDown()

    def create_save_handlers(self):
        self.create_function_with_collection("bucket_op", "handler_code/ABO/insert_rebalance.js",
                                             collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_0.rw"])
        self.create_function_with_collection("timers", "handler_code/ABO/insert_timer.js",
                                            collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_1.rw"])
        self.create_function_with_collection("sbm", "handler_code/ABO/insert_sbm.js",src_namespace="src_bucket.scope_1.coll_1",
                                             collection_bindings=["src_bucket.src_bucket.scope_1.coll_1.rw"])
        self.create_function_with_collection("curl", "handler_code/ABO/curl_get.js",
                                             collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_3.rw"],is_curl=True)
        self.create_function_with_collection("n1ql", "handler_code/collections/n1ql_insert_rebalance.js",
                                             collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_4.rw"])

    def deploy_all_handlers(self):
        self.deploy_handler_by_name("bucket_op")
        self.deploy_handler_by_name("timers")
        self.deploy_handler_by_name("sbm")
        self.deploy_handler_by_name("curl")
        self.deploy_handler_by_name("n1ql")

    def verify_all_handler(self,number_of_docs):
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_0", number_of_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_1", number_of_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", number_of_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", number_of_docs)

    def test_eventing_rebalance_in_when_existing_eventing_node_is_processing_mutations(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_eventing_rebalance_out_when_existing_eventing_node_is_processing_mutations(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # Fail over all eventing nodes and rebalance them out
        self.cluster.failover([self.master], failover_nodes=nodes_out_list, graceful=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_eventing_swap_rebalance_when_existing_eventing_node_is_processing_mutations(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # swap rebalance an eventing node when eventing is processing mutations
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 nodes_out_ev, services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # Fail over all eventing nodes and rebalance them out
        self.cluster.failover([self.master], failover_nodes=nodes_out_list, graceful=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_kv_rebalance_in_when_existing_eventing_node_is_processing_mutations(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # rebalance in a kv node when eventing is processing mutations
        services_in = ["kv"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_kv_rebalance_out_when_existing_eventing_node_is_processing_mutations(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # rebalance out kv node when eventing is processing mutations
        nodes_out_kv = self.servers[1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_kv])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # Fail over all eventing nodes and rebalance them out
        self.cluster.failover([self.master], failover_nodes=nodes_out_list, graceful=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_kv_swap_rebalance_when_existing_eventing_node_is_processing_mutations(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # swap rebalance an kv node when eventing is processing mutations
        services_in = ["kv"]
        nodes_out_kv = self.servers[1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 [nodes_out_kv], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",is_delete=True)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # Fail over all eventing nodes and rebalance them out
        self.cluster.failover([self.master], failover_nodes=nodes_out_list, graceful=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_eventing_rebalance_with_multiple_eventing_nodes(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        task1=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        task2=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        # rebalance in a eventing nodes when eventing is processing mutations
        services_in = ["eventing", "eventing"]
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        task1.result()
        task2.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        task1=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True,
                                     wait_for_loading=False)
        task2=self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True,
                                     wait_for_loading=False)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # Remove 2 eventing nodes
        to_remove_nodes = nodes_out_list[0:2]
        self.log.info("Rebalance out eventing nodes {}".format(to_remove_nodes))
        # rebalance out 2 eventing nodes
        rebalance1 = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached1 = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached1, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        task1.result()
        task2.result()
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        all_eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        self.log.info("Eventing Nodes after rebalance out {}".format(all_eventing_nodes))
        #self.master = self.get_nodes_from_services_map(service_type="kv")
        # add the previously removed nodes as part of swap rebalance
        for node in to_remove_nodes:
            self.rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port,
                               services=["eventing"])
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], all_eventing_nodes)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs*3)
        self.undeploy_delete_all_functions()

    def test_eventing_rebalance_with_multiple_kv_nodes(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        # rebalance in a eventing nodes when eventing is processing mutations
        services_in = ["kv", "kv"]
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True,
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True,
                                     wait_for_loading=False)
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        # Remove 2 eventing nodes
        to_remove_nodes = nodes_out_list[0:2]
        # rebalance out 2 eventing nodes
        rebalance1 = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached1 = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached1, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        all_kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        # add the previously removed nodes as part of swap rebalance
        for node in to_remove_nodes:
            self.rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port,
                               services=["kv"])
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], all_kv_nodes[1:3])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.master = all_kv_nodes[0]
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 3)
        self.undeploy_delete_all_functions()

    def test_rebalance_in_with_different_topologies(self):
        self.services_in = self.input.param("services_in")
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=[self.services_in])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True,
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True,
                                     wait_for_loading=False)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()

    def test_rebalance_out_with_different_topologies(self):
        self.server_out = self.input.param("server_out")
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        nodes_out_list = self.servers[self.server_out]
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True,
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True,
                                     wait_for_loading=False)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()

    def test_swap_rebalance_with_different_topologies(self):
        self.server_out = self.input.param("server_out")
        self.services_in = self.input.param("services_in")
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        nodes_out_list = self.servers[self.server_out]
        # do a swap rebalance
        self.rest.add_node(self.master.rest_username, self.master.rest_password, self.servers[self.nodes_init].ip,
                           self.servers[self.nodes_init].port, services=[self.services_in])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True,
                                     wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True,
                                     wait_for_loading=False)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()


    def test_eventing_rebalance_in_delete_recreate_collections(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        self.collection_rest.delete_collection("dst_bucket","scope_1","coll_0")
        self.collection_rest.delete_collection("dst_bucket","scope_1","coll_1")
        self.collection_rest.delete_collection("dst_bucket","scope_1","coll_2")
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        # self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs*2)
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init+1], [self.servers[self.nodes_init+1]], [],
                                                 services=services_in)
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_0")
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_1")
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_2")
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", 0)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", 0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()

    def test_eventing_rebalance_out_delete_recreate_collections(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        self.collection_rest.delete_collection("dst_bucket", "scope_1", "coll_0")
        self.collection_rest.delete_collection("dst_bucket", "scope_1", "coll_1")
        self.collection_rest.delete_collection("dst_bucket", "scope_1", "coll_2")
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_0")
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_1")
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_2")
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", 0)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", 0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()

    def test_eventing_rebalance_swap_delete_recreate_collections(self):
        self.create_save_handlers()
        self.deploy_all_handlers()
        # load data
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",wait_for_loading=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",wait_for_loading=False)
        # swap rebalance an eventing node when eventing is processing mutations
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 nodes_out_ev, services=services_in)
        self.collection_rest.delete_collection("dst_bucket", "scope_1", "coll_0")
        self.collection_rest.delete_collection("dst_bucket", "scope_1", "coll_1")
        self.collection_rest.delete_collection("dst_bucket", "scope_1", "coll_2")
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # rebalance out a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init+1], [self.servers[self.nodes_init+1]],
                                                 nodes_out_ev, services=services_in)
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_0")
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_1")
        self.collection_rest.create_collection("dst_bucket", "scope_1", "coll_2")
        # delete json documents
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()

    def test_eventing_ondeploy_swap_rebalance_when_existing_eventing_node_is_processing_mutations(self):
        #handler code addining 10000 documents to the dst_bucket
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_REBALANCE)
        self.deploy_function(body)
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 nodes_out_ev, services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_doc_count_collections("dst_bucket._default._default", 10000)
        self.undeploy_function(body)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", 20000)
        self.undeploy_and_delete_function(body)