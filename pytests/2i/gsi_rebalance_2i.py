from base_2i import BaseSecondaryIndexingTests, log
from membase.api.rest_client import RestConnection, RestHelper
from pytests.query_tests_helper import QueryHelperTests


class SecondaryIndexingRebalanceTests(BaseSecondaryIndexingTests,QueryHelperTests):
    def setUp(self):
        super(SecondaryIndexingRebalanceTests, self).setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.create_primary_index = False

    def tearDown(self):
        super(SecondaryIndexingRebalanceTests, self).tearDown()

    def test_gsi_rebalance_out_indexer_node(self):
        self.run_async_index_operations(operation_type="create_index")
        self.sleep(30)
        map_before_rebalance = self.get_index_map()
        log.info(map_before_rebalance)
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        map_after_rebalance = self.get_index_map()
        log.info(map_after_rebalance)
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance, [], [nodes_out_list])

    def test_gsi_rebalance_in_indexer_node(self):
        self.run_async_index_operations(operation_type="create_index")
        self.sleep(30)
        map_before_rebalance = self.get_index_map()
        log.info(map_before_rebalance)
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        map_after_rebalance = self.get_index_map()
        log.info(map_after_rebalance)
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [])

    def test_gsi_rebalance_swap_rebalance(self):
        self.run_async_index_operations(operation_type="create_index")
        self.sleep(30)
        map_before_rebalance = self.get_index_map()
        log.info(map_before_rebalance)
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index"]
        log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init+1], [], to_remove_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        map_after_rebalance = self.get_index_map()
        log.info(map_after_rebalance)
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance, to_add_nodes,
                                                      to_remove_nodes, swap_rebalance=True)

