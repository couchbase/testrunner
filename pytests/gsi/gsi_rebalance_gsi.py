from datetime import datetime

import random
import threading
from membase.api.rest_client import RestConnection, RestHelper
from queue import Queue

from lib import testconstants
from lib.couchbase_helper.query_definitions import SQLDefinitionGenerator, QueryDefinition, RANGE_SCAN_TEMPLATE
from lib.couchbase_helper.tuq_generators import TuqGenerators
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.fts.fts_base import NodeHelper
from pytests.query_tests_helper import QueryHelperTests
from .base_gsi import BaseSecondaryIndexingTests, log


#class SecondaryIndexingRebalanceTests(BaseSecondaryIndexingTests, QueryHelperTests,  NodeHelper,
#                                      EnterpriseBackupRestoreBase):
class SecondaryIndexingRebalanceTests(BaseSecondaryIndexingTests, QueryHelperTests,  NodeHelper):
#class SecondaryIndexingRebalanceTests(BaseSecondaryIndexingTests):
    def setUp(self):
        #super(SecondaryIndexingRebalanceTests, self).setUp()
        super().setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.create_primary_index = False
        self.retry_time = self.input.param("retry_time", 300)
        self.rebalance_out = self.input.param("rebalance_out", False)
        self.sleep_time = self.input.param("sleep_time", 1)
        self.num_retries = self.input.param("num_retries", 1)
        self.build_index = self.input.param("build_index", False)
        self.rebalance_out = self.input.param("rebalance_out", False)
        if not self.capella_run:
            shell = RemoteMachineShellConnection(self.servers[0])
            info = shell.extract_remote_info().type.lower()
            if info == 'linux':
                if self.nonroot:
                    self.cli_command_location = testconstants.LINUX_NONROOT_CB_BIN_PATH
                else:
                    self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
            elif info == 'windows':
                self.cmd_ext = ".exe"
                self.cli_command_location = testconstants.WIN_COUCHBASE_BIN_PATH_RAW
            elif info == 'mac':
                self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
            else:
                raise Exception("OS not supported.")
        self.rand = random.randint(1, 1000000000)
        self.alter_index = self.input.param("alter_index", None)
        if self.ansi_join:
            self.rest.load_sample("travel-sample")
            self.sleep(10)

    def tearDown(self):
        super(SecondaryIndexingRebalanceTests, self).tearDown()

    def test_gsi_rebalance_out_indexer_node(self):
        self.run_operation(phase="before")
        expected_result = None
        if self.ansi_join:
            self.sleep(10)
            expected_result = self.ansi_join_query(stage="pre_rebalance")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        if self.ansi_join:
            self.ansi_join_query(stage="post_rebalance", expected=expected_result)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        if self.ansi_join:
            self.ansi_join_query()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                      [nodes_out_list])
        self.run_operation(phase="after")

    def test_gsi_rebalance_in_indexer_node(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # rebalance in a node
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in, cluster_config=self.cluster_config)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [])
        self.run_operation(phase="after")

    def test_gsi_rebalance_swap_rebalance(self):
        self.run_operation(phase="before")
        if self.ansi_join:
            expected_result = self.ansi_join_query(stage="pre_rebalance")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index"]
        log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        if self.ansi_join:
            self.ansi_join_query(stage="post_rebalance", expected=expected_result)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        for i in range(20):
            output = self.rest.list_indexer_rebalance_tokens(server=index_server)
            if "rebalancetoken" in output:
                log.info(output)
                break
            self.sleep(2)
        if i == 19 and "rebalancetoken" not in output:
            self.log.warning("rebalancetoken was not returned by /listRebalanceTokens during gsi rebalance")
        self.run_async_index_operations(operation_type="query")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        if self.ansi_join:
            self.ansi_join_query()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_cbindex_move_after_rebalance_in(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes, alter_index=self.alter_index)
        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_cbindex_move_with_mutations_and_query(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        rebalance.result()

        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes,
                           alter_index=self.alter_index)

        self.run_operation(phase="during")
        tasks = self.async_run_doc_ops()
        for task in tasks:
            task.result()
        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_create_index_when_gsi_rebalance_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(4)
        try:
            # when rebalance is in progress, run create index
            self.n1ql_helper.run_cbq_query(
                query="CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {'defer_build': True};",
                server=self.n1ql_node)
        except Exception as ex:
            log.info(str(ex))
            if "Create index or Alter replica cannot proceed due to rebalance in progress" not in str(ex):
                self.fail("index creation did not fail with expected error : {0}".format(str(ex)))
        else:
            self.fail("index creation did not fail as expected")
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_drop_index_when_gsi_rebalance_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(15)
        try:
            # when rebalance is in progress, run drop index
            self._drop_index(self.query_definitions[0], self.buckets[0])
        except Exception as ex:
            log.info(str(ex))
            if "Indexer Cannot Process Drop Index - Rebalance In Progress" not in str(ex):
                self.fail("drop index did not fail with expected error : {0}".format(str(ex)))
        else:
            log.info("drop index did not fail, check if the index is dropped in the retry mechanism")
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(120)
        # Validate that the index is dropped after retry
        try:
            self._drop_index(self.query_definitions[0], self.buckets[0])
        except Exception as ex:
            log.info(str(ex))
            if "not found" not in str(ex):
                self.fail("drop index did not fail with expected error : {0}".format(str(ex)))
        else:
            self.fail("drop index did not fail, It should have as it would already have been deleted by retry")

    def test_bucket_delete_and_flush_when_gsi_rebalance_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(2)
        # try deleting and flushing bucket during gsi rebalance
        status1 = self.rest.delete_bucket(bucket=self.buckets[0])
        if status1:
            self.fail("deleting bucket succeeded during gsi rebalance")
        try:
            status2 = self.rest.flush_bucket(bucket=self.buckets[0])
        except Exception as ex:
            if "unable to flush bucket" not in str(ex):
                self.fail("flushing bucket failed with unexpected error message")
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_works_when_querying_is_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # start querying
        t1 = threading.Thread(target=self.run_async_index_operations, args=("query",))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_works_when_mutations_are_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # start kv mutations
        results = []
        tasks = self.async_run_doc_ops()
        for task in tasks:
            results.append(threading.Thread(target=task.result()))
        for result in results:
            result.start()
        # rebalance out a indexer node when kv mutations are in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        for result in results:
            result.join()
        self.sleep(60)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_build_index_when_gsi_rebalance_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self._create_index_with_defer_build()
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(2)
        exceptions = self._build_index()
        if not [x for x in exceptions if 'Indexer Cannot Process Build Index - Rebalance In Progress' in x]:
            self.fail(
                "build index did not fail during gsi rebalance with expected error message: See MB-23452 for more details")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.run_operation(phase="after")

    def test_hard_failover_and_full_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        if self.ansi_join:
            expected_result = self.ansi_join_query(stage="pre_rebalance")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=False)
        failover_task.result()
        self.sleep(30)
        # do a full recovery and rebalance
        add_back_ip = index_server.ip
        if add_back_ip.startswith("["):
            hostname = add_back_ip[add_back_ip.find("[") + 1:add_back_ip.find("]")]
            add_back_ip = hostname
        self.rest.set_recovery_type('ns_1@' + add_back_ip, "full")
        self.rest.add_back_node('ns_1@' + add_back_ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        if self.ansi_join:
            self.ansi_join_query(stage="post_rebalance", expected=expected_result)
        self.sleep(240)
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        if self.ansi_join:
            self.ansi_join_query()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [], [])
        self.run_operation(phase="after")

    def test_hard_failover_and_delta_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        if self.ansi_join:
            self.sleep(10)
            expected_result = self.ansi_join_query(stage="pre_rebalance")
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=False)
        failover_task.result()
        self.sleep(30)
        # do a delta recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + index_server.ip, "delta")
        self.rest.add_back_node('ns_1@' + index_server.ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        if self.ansi_join:
            self.ansi_join_query(stage="post_rebalance", expected=expected_result)
        self.sleep(240)
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        if self.ansi_join:
            self.ansi_join_query()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [], [])
        self.run_operation(phase="after")

    def test_hard_failover_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # failover the indexer node which had all the indexes
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=False)
        failover_task.result()
        self.sleep(30)
        # rebalance out the indexer node
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init + 1], [], [index_server])
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, _ = self._return_maps()
        self.assertEqual(len(list(map_after_rebalance.keys())), 0)

    def test_graceful_failover_and_full_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        if self.ansi_join:
            self.sleep(10)
            expected_result = self.ansi_join_query(stage="pre_rebalance")
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=True)
        failover_task.result()
        self.sleep(30)
        # do a full recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + index_server.ip, "full")
        self.rest.add_back_node('ns_1@' + index_server.ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        if self.ansi_join:
            self.ansi_join_query(stage="post_rebalance", expected=expected_result)
        self.sleep(240)
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        if self.ansi_join:
            self.ansi_join_query()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [], [])
        self.run_operation(phase="after")

    def test_graceful_failover_and_delta_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        if self.ansi_join:
            self.sleep(10)
            expected_result = self.ansi_join_query(stage="pre_rebalance")
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=True)
        failover_task.result()
        self.sleep(30)
        # do a delta recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + index_server.ip, "delta")
        self.rest.add_back_node('ns_1@' + index_server.ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        if self.ansi_join:
            self.ansi_join_query(stage="post_rebalance", expected=expected_result)
        self.sleep(240)
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        if self.ansi_join:
            self.ansi_join_query()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [], [])
        self.run_operation(phase="after")

    def test_gsi_rebalance_works_with_mutations_query_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        # start kv mutations and query in parallel
        results = []
        tasks = self.async_run_doc_ops()
        for task in tasks:
            results.append(threading.Thread(target=task.result()))
        for result in results:
            result.start()
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        for result in results:
            result.join()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_stop_rebalance_and_start_again(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        # stop the rebalance
        stopped = RestConnection(self.master).stop_rebalance(wait_timeout=self.wait_timeout // 3)
        self.assertTrue(stopped, msg="unable to stop rebalance")
        rebalance.result()
        # start rebalance again
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(30)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_verify_gsi_rebalance_does_not_work_during_create_drop_and_build_index(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.sleep(30)
        services_in = ["index"]
        self.run_operation(phase="before")
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 [],
                                                 services=services_in)
        rebalance.result()
        tasks = self.async_run_doc_ops()
        self.sleep(60)
        # start create index, build index and drop index
        # self._build_index(sleep=0)
        for task in tasks:
            task.result()
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(address) USING GSI WITH {'num_replica': 1}"
        t1 = threading.Thread(target=self._create_replica_index, args=(create_index_query,))
        t1.start()
        self.sleep(0.5)
        # while create index is running ,rebalance out a indexer node
        try:
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [index_server])
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail(
                "rebalance did not fail during create index or create index completed before rebalance started")
        t1.join()
        # do a cbindex move after a indexer failure
        # self.sleep(60)
        # map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        # self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        # self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        # self.run_operation(phase="during")
        # map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # # validate the results
        # self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
        #                                               stats_map_before_rebalance, stats_map_after_rebalance,
        #                                               [self.servers[self.nodes_init]], [], swap_rebalance=True,
        #                                               use_https=self.use_https)
        # index_servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # # run a /cleanupRebalance after a rebalance failure
        # for index_server in index_servers:
        #     output = self.rest.cleanup_indexer_rebalance(server=index_server)
        #     log.info(output)

    def test_cbindex_move_after_kv_rebalance(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)

        to_add_nodes1 = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes1, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        to_add_nodes2 = [self.servers[self.nodes_init + 1]]
        services_in = ["kv"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], to_add_nodes2, [],
                                                 services=services_in)
        self.sleep(2)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(60)

        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes,
                           alter_index=self.alter_index)

        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)

        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes1, [], swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_cbindex_move_when_gsi_rebalance_is_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], [index_server],
                                                 services=services_in)
        self.sleep(3)

        output, error = self._cbindex_move(index_server, self.servers[self.nodes_init], indexes,
                                           expect_failure=True, alter_index=self.alter_index)
        if "Cannot Process Move Index - Rebalance/MoveIndex In Progress" not in str(error):
            self.fail("cbindex move succeeded during a rebalance")
        else:
            self.log.info("Index alteration failed as expected")

        rebalance.result()
        self.run_operation(phase="during")
        self.sleep(60)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_rebalance_of_kv_node_during_index_creation_and_building(self):
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["kv"]
        # start create index and build index
        self.run_operation(phase="before")

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(name,age,address) USING GSI WITH {'num_replica': 1}"
        t1 = threading.Thread(target=self._create_replica_index, args=(create_index_query,))
        t1.start()
        try:
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], to_add_nodes, [],
                                                     services=services_in)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            log.info(
                "If there are multiple services in the cluster and rebalance is done, all services get the request to rebalance.\
                 As indexer is running DDL, it will fail with : indexer rebalance failure - ddl in progress")
        else:
            self.fail("indexer rebalance succeeded when it should have failed")
        t1.join()
        self.run_operation(phase="after")
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        # kv node should succeed
        self.assertTrue(len(kv_nodes), 2)

    def test_network_partitioning_during_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        try:
            self.start_firewall_on_node(index_server)
            self.start_firewall_on_node(self.servers[self.nodes_init])
            self.sleep(20)
            # rebalance out a indexer node
            log.info("start rebalance during network partitioning")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            log.info("rebalance failed during network partitioning: {0}".format(str(ex)))
        finally:
            self.stop_firewall_on_node(index_server)
            self.stop_firewall_on_node(self.servers[self.nodes_init])
        self.run_operation(phase="after")

    def test_cbindex_move_when_ddl_is_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self._create_index_with_defer_build()
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)

        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        # start create index, build index
        t1 = threading.Thread(target=self._build_index)
        t1.start()

        self.sleep(60)
        output, error = self._cbindex_move(index_server,
                                           self.servers[self.nodes_init],
                                           indexes,
                                           expect_failure=True,
                                           alter_index=self.alter_index)
        if error:
            self.fail("Alter index failed. Error: %s" % error)
        else:
            self.log.info("Index alteration succeed as expected")

        t1.join()
        self.run_operation(phase="after")

    def test_indexer_compaction_when_gsi_rebalance_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(2)
        # start indexer compaction when rebalance is running
        self._set_indexer_compaction()
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_bucket_compaction_when_gsi_rebalance_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # Do some kv mutations for so that compaction can kick in
        tasks = self.async_run_doc_ops()
        for task in tasks:
            task.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(2)
        # start indexer compaction when rebalance is running
        self._set_bucket_compaction()
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_when_indexer_is_in_paused_state(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        self.run_operation(phase="before")

        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # Do some kv mutations
        tasks = self.async_run_doc_ops()
        for task in tasks:
            task.result()
        # Ensure indexer reaches to paused state
        failover_nodes = [kv_server[1], index_server]
        self._push_indexer_off_the_cliff()
        # Try kv and index failover when indexer is in paused state
        failover_task = self.cluster.async_failover([self.master], failover_nodes=failover_nodes, graceful=False)
        failover_task.result()
        for failover_node in failover_nodes:
            self.rest.add_back_node("ns_1@" + failover_node.ip)
            self.rest.set_recovery_type(otpNode="ns_1@" + failover_node.ip, recoveryType="full")
        # rebalance out a node
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            RestHelper(self.rest).rebalance_reached()
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail when indexer is in paused state")

    def test_rebalance_deferred_index_then_build_index(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # create index with defer_build = True
        self._create_index_with_defer_build()
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        self.sleep(2)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(60)
        # Now build the index on the new node
        exceptions = self._build_index()
        if exceptions:
            self.fail("build index after rebalance failed")
        self.sleep(60)
        self.run_operation(phase="after")

    def test_drop_index_during_kv_rebalance(self):
        self.run_operation(phase="before")
        to_add_nodes1 = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes1, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        to_add_nodes2 = [self.servers[self.nodes_init + 1]]
        services_in = ["kv"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], to_add_nodes2, [],
                                                 services=services_in)
        self.sleep(2)
        self.run_async_index_operations(operation_type="drop_index")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    # TODO : Relook at it after MB-23135 is fixed
    def test_cbindex_move_with_not_active_indexes(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self._create_index_with_defer_build()
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        rebalance.result()
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes, alter_index=self.alter_index)
        tasks = self.async_run_doc_ops()
        for task in tasks:
            task.result()
        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)
        self.sleep(30)
        exceptions = self._build_index()
        self.sleep(30)
        if exceptions:
            self.fail("build index after cbindex move failed. See MB-23135 for more details")
        self.run_operation(phase="after")

    def test_cbindex_move_negative(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self._create_index_with_defer_build()
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        rebalance.result()

        #  cbindex move with invalid source host. Not applicable for alter index
        output, error = self._cbindex_move(self.servers[self.nodes_init + 1],
                                           self.servers[self.nodes_init], indexes,
                                           expect_failure=True,
                                           alter_index=False)

        if not [x for x in error if 'Error occured' in x]:
            self.fail("cbindex move did not fail with expected error message")

        # cbindex move with invalid destination host
        output, error = self._cbindex_move(index_server, self.servers[
            self.nodes_init + 1], indexes, expect_failure=True,
                                           alter_index=self.alter_index)

        if "Unable to find Index service for destination" not in str(error):
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(error)))
        else:
            self.log.info("Index creation failed as expected")


        # cbindex move with destination host not reachable
        output, error = self._cbindex_move(index_server, "some_junk_value",
                                           indexes, expect_failure=True,
                                           alter_index=self.alter_index)

        if "Unable to find Index service for destination" not in str(error):
            self.fail(
                "index creation did not fail with expected error : {0}".format(
                    str(error)))
        else:
            self.log.info("Index creation failed as expected")

    def test_gsi_rebalance_with_1_node_out_and_2_nodes_in(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index,n1ql", "index,fts"]
        log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(60)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_with_2_nodes_out_and_1_node_in(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, nodes_out_list,
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(60)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, nodes_out_list, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_mulitple_equivalent_index_on_same_node_and_rebalance_to_multiple_nodes(self):
        # Generate multiple equivalent indexes on the same node
        self.run_operation(phase="before")
        query_definition_generator = SQLDefinitionGenerator()
        self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index", "index"]
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(60)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_mulitple_equivalent_index_on_multiple_nodes_and_rebalance_to_single_node(self):
        # Generate multiple equivalent indexes on the same node
        self.run_operation(phase="before")
        query_definition_generator = SQLDefinitionGenerator()
        self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(60)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, nodes_out_list, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_cbindex_move_negative_indexnames(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self._create_index_with_defer_build()
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        rebalance.result()
        #  cbindex move with invalid src host not valid for alter index query
        _, error = self._cbindex_move(self.servers[self.nodes_init + 1], "", indexes, expect_failure=True)
        if not [x for x in error if 'Error occured invalid index specified' in x]:
            self.fail("cbindex move did not fail with expected error message")

    def test_kv_failover_when_ddl_in_progress(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        t1 = threading.Thread(target=self.run_operation, args=("before",))
        t1.start()
        self.sleep(5)
        # failover the kv node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[kv_node], graceful=False)
        failover_task.result()
        t1.join()
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # do a full recovery and rebalance
        self.sleep(30)
        self.rest.set_recovery_type('ns_1@' + kv_node.ip, "full")
        self.rest.add_back_node('ns_1@' + kv_node.ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [], [])
        self.run_operation(phase="after")

    def test_index_failover_when_ddl_in_progress(self):
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        t1 = threading.Thread(target=self.run_operation, args=("before",))
        t1.start()
        self.sleep(5)
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_node], graceful=False)
        failover_task.result()
        t1.join()
        self.sleep(30)
        # do a full recovery and rebalance
        self.sleep(30)
        self.rest.set_recovery_type('ns_1@' + index_node.ip, "full")
        self.rest.add_back_node('ns_1@' + index_node.ip)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        log.info(map_before_rebalance)
        log.info(stats_map_before_rebalance)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        log.info(map_after_rebalance)
        log.info(stats_map_after_rebalance)
        self.run_operation(phase="after")

    def test_build_index_when_kv_rebalance_in_progress(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        self.sleep(30)
        services_in = ["kv"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        try:
            # rebalance out a node
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [kv_node])
            self.run_operation(phase="before")
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception:
            # See MB-22983 for more details
            log.info(
                "If there are multiple services in the cluster and rebalance is done, all services get the request to rebalance.\
                As indexer is running DDL, it will fail with : indexer rebalance failure - ddl in progress")
        self.run_operation(phase="after")

    def test_erl_crash_on_indexer_node_during_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node when querying is in progress
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            self.sleep(2)
            self.kill_erlang(index_server)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after erl crash")
        self.sleep(180)
        self.run_operation(phase="after")

    def test_erl_crash_on_kv_node_during_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        kv_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node when querying is in progress
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            self.sleep(2)
            self.kill_erlang(kv_server)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after erl crash")
        # Allow time for the cluster to recover from the failure
        self.sleep(60)
        self.run_operation(phase="after")

    def test_memcache_crash_on_kv_node_during_gsi_rebalance(self):
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        # Indexes might take more time to build, so sleep for 3 mins
        self.sleep(180)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node when querying is in progress
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            self.sleep(2)
            self.kill_memcached1(kv_server)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            self.fail("rebalance failed after memcached got killed: {0}".format(str(ex)))
        self.run_operation(phase="after")

    def test_kv_rebalance_when_cbindex_move_in_progress(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes,
                           alter_index=self.alter_index)
        services_in = ["kv"]
        to_add_nodes1 = [self.servers[self.nodes_init + 1]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes1, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True,
                                                      use_https=self.use_https)

    def test_kv_failover_when_cbindex_move_in_progress(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()

        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes,
                           alter_index=self.alter_index)
        # failover the kv node when cbindex move is in progress
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[kv_server], graceful=False)
        failover_task.result()
        self.sleep(30)
        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True,
                                                      use_https=self.use_https)
        # do a full recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + kv_server.ip, "full")
        self.rest.add_back_node('ns_1@' + kv_server.ip)
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

    # Relook once MB-23399 is fixed
    def test_index_failover_when_cbindex_move_in_progress(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init+2]
        services_in = ["index", "index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        rebalance.result()
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes,
                           alter_index=self.alter_index)
        # failover the indexer node when cbindex move is in progress which is not involved in cbindex move
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[self.servers[self.nodes_init + 1]],
                                                    graceful=False)
        failover_task.result()
        self.sleep(30)
        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [], swap_rebalance=False)
        # do a rebalance
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init + 1], [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

    def test_reboot_on_kv_node_during_gsi_rebalance(self):
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            # self.sleep(7)
            # reboot a kv node during gsi rebalance
            self.reboot_node(kv_server[1])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            if "Rebalance Failed" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after kv node reboot")
        self.sleep(30)
        self.run_operation(phase="after")
        index_servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # run a /cleanupRebalance after a rebalance failure
        for index_server in index_servers:
            output = self.rest.cleanup_indexer_rebalance(server=index_server)
            log.info(output)

    def test_cbindex_move_invalid_data(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self._create_index_with_defer_build()
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        rebalance.result()

        #  cbindex move with destination host not specified
        output, error = self._cbindex_move(index_server,
                                           " ",
                                           indexes,
                                           expect_failure=True,
                                           alter_index=self.alter_index)

        if "Unable to find Index service for destination" not in str(error):
            self.fail(
                "cbindex move did not fail with expected error message")
        else:
            self.log.info("Index alteration failed as expected")

        # cbindex move with index names not specified
        output, error = self._cbindex_move(index_server,
                                           self.servers[self.nodes_init],
                                           " ",
                                           expect_failure=True,
                                           alter_index=self.alter_index)

        expected_err_msg = "invalid index specified"
        if self.alter_index:
            expected_err_msg = "Index Not Found"

        if expected_err_msg not in str(error):
            self.fail(
                "cbindex move did not fail with expected error message")
        else:
            self.log.info("Index alteration failed as expected")

        # cbindex move with index name which does not exist
        self.run_async_index_operations(operation_type="drop_index")
        output, error = self._cbindex_move(index_server,
                                           self.servers[self.nodes_init],
                                           indexes,
                                           expect_failure=True,
                                           alter_index=self.alter_index)

        expected_err_msg = "invalid index specified"
        if self.alter_index:
            expected_err_msg = "Index Not Found"

        if expected_err_msg not in str(error):
            self.fail(
                "cbindex move did not fail with expected error message")
        else:
            self.log.info("Index alteration failed as expected")

    def test_rebalance_in_with_different_topologies(self):
        self.services_in = self.input.param("services_in")
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=[self.services_in])
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [])
        self.run_operation(phase="after")

    def test_rebalance_out_with_different_topologies(self):
        self.server_out = self.input.param("server_out")
        # remove the n1ql node which is being rebalanced out
        all_n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        for n1ql_node in all_n1ql_nodes:
            if n1ql_node.ip not in str(self.servers[self.server_out]):
                self.n1ql_server = n1ql_node
                self.n1ql_node = n1ql_node
                break
        self.run_operation(phase="before")
        self.sleep(30)
        nodes_out_list = self.servers[self.server_out]
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        self.run_operation(phase="after")

    def test_swap_rebalance_with_different_topologies(self):
        self.server_out = self.input.param("server_out")
        self.services_in = self.input.param("services_in")
        self.run_operation(phase="before")
        self.sleep(30)
        nodes_out_list = self.servers[self.server_out]
        # remove the n1ql node which is being rebalanced out
        all_n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        for n1ql_node in all_n1ql_nodes:
            if n1ql_node.ip not in str(self.servers[self.server_out]):
                self.n1ql_server = n1ql_node
                self.n1ql_node = n1ql_node
                break
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 [], services=[self.services_in])
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                 [nodes_out_list])
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        self.run_operation(phase="after")

    def test_backup_restore_after_gsi_rebalance(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                      [nodes_out_list])
        self.run_operation(phase="after")
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        self._create_backup(kv_node)
        self._create_restore(kv_node)

    def test_backup_restore_while_gsi_rebalance_is_running(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        self._create_backup(kv_node)
        self._create_restore(kv_node)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                      [nodes_out_list])
        self._run_prepare_statement()
        self.run_operation(phase="after")

    def test_gsi_rebalance_using_couchbase_cli(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # rebalance out a node
        shell = RemoteMachineShellConnection(kv_node)

        command = "{0}couchbase-cli rebalance -c {1} -u {2} -p {3} --server-remove={4}:{5}".format(
            self.cli_command_location,
            kv_node.ip, kv_node.rest_username,
            kv_node.rest_password,
            nodes_out_list.ip,
            self.node_port)
        o, e = shell.execute_non_sudo_command(command)
        shell.log_command_output(o, e)
        self.sleep(30)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                      [nodes_out_list])
        # rebalance in a node
        command = "{0}couchbase-cli server-add -c {1} -u {2} -p {3} --server-add={4} --server-add-username={5} " \
                  "--server-add-password={6}".format(
            self.cli_command_location,
            kv_node.ip, kv_node.rest_username,
            kv_node.rest_password,
            self.servers[self.nodes_init].ip, self.servers[self.nodes_init].rest_username,
            self.servers[self.nodes_init].rest_password)
        o, e = shell.execute_non_sudo_command(command)
        shell.log_command_output(o, e)
        if e or not [x for x in o if 'SUCCESS: Server added' in x]:
            self.fail("server-add failed")
        self.sleep(30)
        command = "{0}couchbase-cli rebalance -c {1} -u {2} -p {3}".format(
            self.cli_command_location,
            kv_node.ip, kv_node.rest_username,
            kv_node.rest_password)
        o, e = shell.execute_non_sudo_command(command)
        shell.log_command_output(o, e)
        if e or not [x for x in o if 'SUCCESS: Rebalance complete' in x]:
            self.fail("rebalance failed")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.run_operation(phase="after")

    def test_long_running_scan_with_gsi_rebalance(self):
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        emit_fields = "*"
        body = {"stale": "False"}
        query_definition = QueryDefinition(index_name="multiple_field_index",
                                           index_fields=["name", "age", "email", "premium_customer"],
                                           query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                     "name > \"Adara\" AND "
                                                                                     "name < \"Winta\" "
                                                                                     "AND age > 0 AND age "
                                                                                     "< 100 ORDER BY _id"),
                                           groups=["multiple_field_index"], index_where_clause=" name IS NOT NULL ")
        self.rest = RestConnection(nodes_out_list)
        id_map = self.create_index_using_rest(self.buckets[0], query_definition)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # run a long running scan during a gsi rebalance
        t1 = threading.Thread(target=RestConnection(nodes_out_list).full_table_scan_gsi_index_with_rest,
                              args=(id_map["id"], body,))
        t1.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()
        t2 = threading.Thread(target=RestConnection(self.servers[self.nodes_init]).full_table_scan_gsi_index_with_rest,
                              args=(id_map["id"], body,))
        t2.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        self.run_async_index_operations(operation_type="query")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t2.join()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_reboot_on_indexer_node_during_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            self.sleep(2)
            # reboot a kv node during gsi rebalance
            self.reboot_node(self.servers[self.nodes_init])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after index node reboot")
        self.sleep(30)
        self.run_operation(phase="after")
        index_servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # run a /cleanupRebalance after a rebalance failure
        for index_server in index_servers:
            output = self.rest.cleanup_indexer_rebalance(server=index_server)
            log.info(output)

    # yet to confirm if parallel cbindex moves are supported, hoping to catch some panic if its not
    def test_cbindex_move_when_one_move_index_is_already_runnning(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                indexes.append(index)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        threads = []
        # start multiple cbindex moves in parallel

        for index in indexes:
          threads.append(
              threading.Thread(target=self._cbindex_move, args=(index_server, self.servers[self.nodes_init], index, self.alter_index)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.sleep(60)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_kill_n1ql_during_gsi_rebalance(self):
        self.run_operation(phase="before")
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index"]
        log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        # kill n1ql while rebalance is running
        self.sleep(5)
        for i in range(20):
            self._kill_all_processes_cbq(n1ql_node)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_kill_indexer_during_gsi_rebalance(self):
        self.run_operation(phase="before")
        self.sleep(30)
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # do a swap rebalance
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
            # kill indexer while rebalance is running
            for i in range(20):
                self._kill_all_processes_index(self.servers[self.nodes_init])
                self.sleep(2)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after killing indexer node")
        self.sleep(60)
        index_servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # run a /cleanupRebalance after a rebalance failure
        for index_server in index_servers:
            output = self.rest.cleanup_indexer_rebalance(server=index_server)
            log.info(output)

    def test_autofailover_with_gsi_rebalance(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        RestConnection(self.master).update_autofailover_settings(True, 30)
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        remote = RemoteMachineShellConnection(kv_node[1])
        remote.stop_server()
        self.sleep(40, "Wait for autofailover")
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], [index_node, kv_node[1]])
            self.run_operation(phase="during")
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            self.fail("rebalance failed with  error : {0}".format(str(ex)))
        finally:
            remote.start_server()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                      [index_node])
        self.run_operation(phase="after")
        self.sleep(30)

    def test_gsi_rebalance_can_be_resumed_after_failed_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a indexer node
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            self.sleep(2)
            # reboot a kv node during gsi rebalance
            self.reboot_node(index_server)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after index node reboot")
        self.sleep(60)
        # Rerun rebalance to check if it can recover from failure
        for i in range(5):
            try:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
                self.sleep(2)
                reached = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                rebalance.result()
            except Exception as ex:
                if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex) and i == 4:
                    self.fail("rebalance did not recover from failure : {0}".format(str(ex)))
                    # Rerun after MB-23900 is fixed.
            else:
                break
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                      [index_server])
        self.run_operation(phase="after")

    def test_induce_fts_failure_during_gsi_rebalance(self):
        self.run_operation(phase="before")
        fts_node = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index"]
        log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        self.sleep(5)
        for i in range(20):
            self._kill_fts_process(fts_node)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        # kill fts while rebalance is running
        self.sleep(5)
        for i in range(20):
            self._kill_fts_process(fts_node)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_cbindex_move_on_deferred_index_then_build_index(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # create index with defer_build = True
        self._create_index_with_defer_build()
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # Move the indexes
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                indexes.append(index)
        log.info(indexes)
        self.sleep(2)
        i = 1
        for index in indexes:
            self._cbindex_move(index_server, self.servers[self.nodes_init], index, alter_index=self.alter_index)
            if not self.alter_index:
                self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], i)
            else:
                # Allow index movement via alter index to be completed.
                self.sleep(120)
            i += 1
        self.sleep(60)
        # Now build the index on the new node
        exceptions = self._build_index()
        if exceptions:
            self.fail("build index after rebalance failed")
        # Move the indexes again
        j = 1
        for index in indexes:
            self._cbindex_move(self.servers[self.nodes_init], index_server, index, alter_index=self.alter_index)
            if not self.alter_index:
                self.wait_for_cbindex_move_to_complete(index_server, j)
            else:
                # Allow index movement via alter index to be completed.
                self.sleep(120)
            j += 1
        self.sleep(60)
        self.run_operation(phase="after")

    def test_cbindex_move_with_reboot_of_destination_node(self):
        queue = Queue()
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                indexes.append(index)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        threads = []
        # start multiple cbindex moves in parallel
        for index in indexes:
            t1 = threading.Thread(target=self._cbindex_move,
                                  args=(index_server, self.servers[self.nodes_init], index, self.alter_index, queue))
            threads.append(t1)
            t1.start()
            self.sleep(1)
            self.reboot_node(self.servers[self.nodes_init])
            t1.join()
            for item in iter(queue.get, None):
                log.info(item)
                if [x for x in item if "dial tcp {0}:9100: getsockopt: connection refused".format(
                        self.servers[self.nodes_init].ip) in x]:
                    msg = "error found"
                    log.info("error found")
                    break
                else:
                    pass
            queue.queue.clear()
            if msg == "error found":
                break
        if msg != "error found":
            self.fail("cbindex move did not fail during a reboot")

    def test_cbindex_move_after_network_partitioning(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        try:
            self.start_firewall_on_node(index_server)
            output, error = self._cbindex_move(index_server, self.servers[self.nodes_init], indexes, self.alter_index,
                                               expect_failure=True)
            if not [x for x in error if 'Client.Timeout exceeded while awaiting headers' in x]:
                if not [x for x in error if 'i/o timeout' in x]:
                    self.fail("cbindex move did not fail during network partition with expected error message : {0}".format(
                        error))
        except Exception as ex:
            self.fail(str(ex))
        finally:
            self.stop_firewall_on_node(index_server)
        self.run_operation(phase="after")

    def test_partition_n1ql_during_gsi_rebalance(self):
        self.run_operation(phase="before")
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index"]
        log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
            # partition n1ql node while running gsi rebalance
            self.sleep(5)
            for i in range(5):
                self.start_firewall_on_node(n1ql_node)
            self.stop_firewall_on_node(n1ql_node)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        except Exception as ex:
            self.fail("gsi rebalance failed because firewall was enabled on n1ql node : {0}".format(str(ex)))
        finally:
            self.stop_firewall_on_node(n1ql_node)
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_cbindex_move_with_reboot_of_source_node(self):
        queue = Queue()
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                indexes.append(index)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        threads = []
        # start multiple cbindex moves in parallel
        for index in indexes:
            t1 = threading.Thread(target=self._cbindex_move,
                                  args=(index_server, self.servers[self.nodes_init], index, self.alter_index, queue, True))
            threads.append(t1)
            t1.start()
            self.sleep(2)
            self.reboot_node(index_server)
            t1.join()
            for item in iter(queue.get, None):
                log.info(item)
                if [x for x in item if "dial tcp {0}:9100: getsockopt: connection refused".format(index_server.ip) in x]:
                    msg = "error found"
                    log.info("error found")
                    break
                else:
                    pass
            # queue.queue.clear()
            if msg == "error found":
                break
        if msg != "error found":
            self.fail("cbindex move did not fail during a reboot")

    def test_network_partitioning_between_kv_indexer_during_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        kv_node_partition = kv_server[1]
        if kv_server[1] == self.servers[0]:
            kv_node_partition = kv_server[0]
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        try:
            # rebalance out a indexer node
            log.info("start rebalance during network partitioning")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            self.sleep(2)
            self.start_firewall_on_node(index_server)
            self.start_firewall_on_node(self.servers[self.nodes_init])
            self.start_firewall_on_node(kv_node_partition)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after kv index network partitioning")
        finally:
            self.stop_firewall_on_node(index_server)
            self.stop_firewall_on_node(self.servers[self.nodes_init])
            self.stop_firewall_on_node(kv_node_partition)
        self.run_operation(phase="after")

    def test_cbindex_move_with_index_server_being_killed(self):
        queue = Queue()
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes = []
        for bucket in map_before_rebalance:
            for index in map_before_rebalance[bucket]:
                indexes.append(index)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        threads = []
        # start multiple cbindex moves
        for index in indexes:
            t1 = threading.Thread(target=self._cbindex_move,
                                  args=(index_server, self.servers[self.nodes_init], index, self.alter_index, queue, True))
            threads.append(t1)
            t1.start()
            self.sleep(2)
            self._kill_all_processes_index(index_server)
            self._kill_all_processes_index(self.servers[self.nodes_init])
            t1.join()
            for item in iter(queue.get, None):
                log.info(item)
                if [x for x in item if "WatcherServer.runOnce() : Watcher terminated unexpectedly".format(self.servers[self.nodes_init].ip) in x]:
                    msg = "error found"
                    log.info("error found")
                    break
                else:
                    pass
            # queue.queue.clear()
            if msg == "error found":
                break
        if msg != "error found":
            self.fail("cbindex move did not fail when indexer server was killed")

    def test_gsi_rebalance_with_multiple_nodes_in_swap_rebalance(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        to_remove_nodes = nodes_out_list[1:3]
        services_in = ["index", "index"]
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_with_different_scan_consistency(self):
        n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        to_remove_nodes = nodes_out_list[1:3]
        services_in = ["index", "index"]
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 2], [], to_remove_nodes)
        query = "SELECT * FROM default where `question_values` IS NOT NULL ORDER BY  _id"
        result_rp = self.n1ql_helper.run_cbq_query(query=query, server=n1ql_server, scan_consistency="request_plus")
        result_sp = self.n1ql_helper.run_cbq_query(query=query, server=n1ql_server, scan_consistency="statement_plus")
        result_nb = self.n1ql_helper.run_cbq_query(query=query, server=n1ql_server, scan_consistency="not_bounded")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_out_last_indexer_node_and_add_one_indexer_node(self):
        self.run_operation(phase="before")
        self.sleep(30)
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out_list)
        # Queries running while this rebalance operation is going on will fail as the only indexer node is going out.
        #self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 2]
        services_in = ["index", "index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.run_operation(phase="before")

    def test_gsi_rebalance_in_indexer_node_with_node_eject_only_as_false(self):
        self.run_operation(phase="before")
        self.sleep(30)
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for indexer_node in indexer_nodes:
            rest = RestConnection(indexer_node)
            rest.set_index_settings({"indexer.rebalance.node_eject_only": False})
            rest.set_index_settings({"indexer.settings.rebalance.redistribute_indexes": True})
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # rebalance in a node
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results, indexes should be redistributed even in case of rebalance in
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [], swap_rebalance=True,
                                                      use_https=self.use_https)
        self.run_operation(phase="after")

    def test_gsi_rebalance_with_disable_index_move_as_true(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for indexer_node in indexer_nodes:
            rest = RestConnection(indexer_node)
            rest.set_index_settings({"indexer.rebalance.disable_index_move": True})
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index"]
        log.info(self.servers[:self.nodes_init])
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)

        # Dont run queries after removing a node because due to the above indexer rebalance, some indexes required by
        # the queries could be dropped resulting into failed queries and failure in test
        #self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        #self.run_async_index_operations(operation_type="query")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results, Indexes should not be redistributed as disable_index_move was set as True
        try:
            self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                          stats_map_before_rebalance, stats_map_after_rebalance,
                                                          to_add_nodes, to_remove_nodes, swap_rebalance=True,
                                                          use_https=self.use_https)
        except Exception as ex:
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail("gsi rebalance failed with unexpected error: {0}".format(str(ex)))
        else:
            self.fail("gsi rebalance distributed indexes even after disable_index_move is set as true")
        #self.run_operation(phase="after")

    def test_nest_and_intersect_queries_after_gsi_rebalance(self):
        self.run_operation(phase="before")
        intersect_query = "select name from {0} intersect select name from {0} s where s.age>20".format(self.buckets[0],
                                                                                                        self.buckets[0])
        nest_query = "select * from {0} b1 nest `default` b2 on keys b1._id where b1._id like 'airline_record%' limit 5".format(
            self.buckets[0])
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_list])
        for i in range(0, 2):
            try:
                self.n1ql_helper.run_cbq_query(query=intersect_query, server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=nest_query, server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                raise Exception("query with nest and intersect failed")
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                      [nodes_out_list])
        # Run intesect/nest queries post index redistribution
        for i in range(0, 10):
            try:
                self.n1ql_helper.run_cbq_query(query=intersect_query, server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=nest_query, server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                raise Exception("query with nest and intersect failed")
        self.run_operation(phase="after")

    def test_gsi_rebalance_out_indexer_node_when_other_indexer_is_in_paused_state(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=256)
        for i in range(2):
            query_definition_generator = SQLDefinitionGenerator()
            self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
            self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
            self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # Do some kv mutations
        tasks = self.async_run_doc_ops()
        for task in tasks:
            task.result()
        # Ensure indexer reaches to paused state
        self._push_indexer_off_the_cliff()
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[self.nodes_init]])
            RestHelper(self.rest).rebalance_reached()
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail when indexer is in paused state")

    def test_alter_index_when_src_indexer_is_in_paused_state(
            self):
        kv_server = self.get_nodes_from_services_map(service_type="kv",
                                                     get_all_nodes=False)
        to_add_nodes = [self.servers[self.nodes_init]]
        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [self.servers[
                                                      self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()

        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(
            map_before_rebalance)

        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        # Ensure indexer reaches to paused state
        self._push_indexer_off_the_cliff(index_server)

        output, error = self._cbindex_move(index_server,
                                           self.servers[self.nodes_init],
                                           indexes, self.alter_index,
                                           remote_host=kv_server)

        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(
                self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)

        if error:
            self.log.info("ERROR : %s" % error)
            self.fail("Alter index resulted in error")

    def test_alter_index_when_dest_indexer_is_in_paused_state(
            self):
        kv_server = self.get_nodes_from_services_map(service_type="kv",
                                                     get_all_nodes=False)

        to_add_nodes = [self.servers[self.nodes_init]]
        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)
        self.run_operation(phase="before")

        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 to_add_nodes, [],
                                                 services=services_in)
        rebalance.result()
        query = "CREATE PRIMARY INDEX p1 on default USING GSI with {{'nodes':\"{0}:{1}\"}}".format(
            self.servers[self.nodes_init].ip,
            self.node_port)
        self.n1ql_helper.run_cbq_query(query=query,
                                       server=self.n1ql_node)

        self.sleep(60)

        index_map, stats_map = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(
            index_map)

        index_hostname_before = self.n1ql_helper.get_index_details_using_index_name(
            "p1", index_map)

        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        # Ensure indexer reaches to paused state
        self._push_indexer_off_the_cliff(index_server)

        output, error = self._cbindex_move(self.servers[self.nodes_init],
                                           index_server,
                                           "p1",
                                           self.alter_index,
                                           remote_host=kv_server)

        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(
                self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)

        if error:
            self.log.info("ERROR : %s" % error)
            self.fail("Alter index resulted in error")
        else:
            # Alter index query will succeed, but it should later error out since the dest node is in OOM.
            # Validate that the index is not moved.
            index_map, stats_map = self._return_maps()
            index_hostname_after = self.n1ql_helper.get_index_details_using_index_name(
                "p1", index_map)
            self.assertEqual(index_hostname_before, index_hostname_after,
                             "Alter index moved the index to an indexer in paused state")

    def test_alter_index_without_action(self):
        kv_server = self.get_nodes_from_services_map(service_type="kv",
                                                     get_all_nodes=False)

        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)

        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [self.servers[
                                                      self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()

        query = "CREATE PRIMARY INDEX p1 on default USING GSI with {{'nodes':\"{0}:{1}\"}}".format(
            self.servers[self.nodes_init].ip,
            self.node_port)
        self.n1ql_helper.run_cbq_query(query=query,
                                       server=self.n1ql_node)

        alter_idx_query = "ALTER INDEX default.p1 with {{'action':'','nodes':\"{0}:{1}\"}}".format(
            index_server.ip, self.node_port)
        try:
            result = self.n1ql_helper.run_cbq_query(query=alter_idx_query,
                                                    server=self.n1ql_node)
            self.log.info(result)
        except Exception as ex:
            if not "Unsupported action value" in str(ex):
                self.log.info(str(ex))
                self.fail(
                    "Alter index did not fail with expected error message")

    def test_alter_index_when_src_indexer_is_in_dgm(
            self):
        kv_server = self.get_nodes_from_services_map(service_type="kv",
                                                     get_all_nodes=False)

        to_add_nodes = [self.servers[self.nodes_init]]
        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)
        self.run_operation(phase="before")

        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [self.servers[
                                                      self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()

        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(
            map_before_rebalance)

        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        # Ensure indexer reaches to DGM
        self.get_dgm_for_plasma(index_server)

        output, error = self._cbindex_move(index_server,
                                           self.servers[self.nodes_init],
                                           indexes, self.alter_index,
                                           remote_host=kv_server)

        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(
                self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)

        if error:
            self.log.info("ERROR : %s" % error)
            self.fail("Alter index resulted in error")

    def test_alter_index_when_dest_indexer_is_in_dgm(
            self):
        kv_server = self.get_nodes_from_services_map(service_type="kv",
                                                     get_all_nodes=False)

        to_add_nodes = [self.servers[self.nodes_init]]
        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)
        self.run_operation(phase="before")

        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [self.servers[
                                                      self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()

        query = "CREATE PRIMARY INDEX p1 on default USING GSI with {{'nodes':\"{0}:{1}\"}}".format(
            self.servers[self.nodes_init].ip,
            self.node_port)
        self.n1ql_helper.run_cbq_query(query=query,
                                       server=self.n1ql_node)

        self.sleep(60)

        index_map, stats_map = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(
            index_map)

        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        # Ensure indexer reaches to DGM
        self.get_dgm_for_plasma(index_server)

        output, error = self._cbindex_move(self.servers[self.nodes_init],
                                           index_server,
                                           "p1",
                                           self.alter_index,
                                           remote_host=kv_server)

        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(
                self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)

        if error:
            self.log.info("ERROR : %s" % error)
            self.fail("Alter index resulted in error")

    def test_explain_query_while_alter_index_is_running(self):
        kv_server = self.get_nodes_from_services_map(service_type="kv",
                                                     get_all_nodes=False)

        to_add_nodes = [self.servers[self.nodes_init]]
        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)
        self.run_operation(phase="before")

        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [self.servers[
                                                      self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()

        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(
            map_before_rebalance)

        output, error = self._cbindex_move(index_server,
                                           self.servers[self.nodes_init],
                                           indexes,
                                           self.alter_index,
                                           remote_host=kv_server)

        explain_query = "EXPLAIN SELECT DISTINCT(age) from `default` where age > 18 LIMIT 10"
        try:
            result = self.n1ql_helper.run_cbq_query(query=explain_query,
                                                    server=self.n1ql_node)
            self.log.info(result)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Alter index did not fail with expected error message")


    def test_cbindex_move_from_any_node_apart_from_src_dest(self):
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # run cbindex move from any host other than src or dest,not valid for alter index
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes, self.alter_index, remote_host=kv_server)
        if not self.alter_index:
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        else:
            # Allow index movement via alter index to be completed.
            self.sleep(120)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True, use_https=self.use_https)
        self.run_operation(phase="after")

    def test_retry_rebalance(self):
        body = {"enabled": "true", "afterTimePeriod": self.retry_time , "maxAttempts" : self.num_retries}
        rest = RestConnection(self.master)
        rest.set_retry_rebalance_settings(body)
        result = rest.get_retry_rebalance_settings()
        self.shell.execute_cbworkloadgen(rest.username, rest.password, 1000000, 100, "default", 1024, '-j')
        if not self.build_index:
            self.run_operation(phase="before")
            self.sleep(30)
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["index"]
        if self.rebalance_out:
            # rebalance in a node
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
            rebalance.result()
        try:
            if self.build_index:
                thread1 = threading.Thread(name='ddl', target=self.create_workload_index)
                thread1.start()
                self.sleep(5)
            if self.rebalance_out:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            else:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                         [self.servers[self.nodes_init]], [],
                                                         services=services_in)
            self.sleep(4)
            # stop an index node during gsi rebalance
            if not self.build_index:
                self.stop_server(index_server)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception as ex:
            self.start_server(index_server)
            if "Rebalance failed" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.start_server(index_server)
            self.fail("rebalance did not fail")
        # Rerun rebalance to check if it can recover from failure
        if self.build_index:
            thread1.join()
        self.check_retry_rebalance_succeeded()
        self.sleep(20)
        if self.rebalance_out and not self.build_index:
            map_after_rebalance, stats_map_after_rebalance = self._return_maps()
            # validate the results
            self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                          stats_map_before_rebalance, stats_map_after_rebalance, [],
                                                          [index_server])
    def create_workload_index(self):
        workload_index = "CREATE INDEX idx12345 ON default(name)"
        self.n1ql_helper.run_cbq_query(query=workload_index,
                                       server=self.n1ql_node)
        return

    def _return_maps(self):
        index_map = self.get_index_map()
        stats_map = self.get_index_stats(perNode=False)
        return index_map, stats_map

    def _cbindex_move(self, src_node, dst_node, index_list, alter_index=False, queue=None,run_from_dst=False,username="Administrator", password="password",
                      expect_failure=False, bucket="default", remote_host=None):

        try:
            ip_address = f"{dst_node.ip}:{self.node_port}"
        except AttributeError:
            ip_address = dst_node
        if alter_index:
            alter_index_query = f'ALTER INDEX `{index_list}` ON default' \
                                f' WITH {{"action":"move","nodes": ["{ip_address}"]}}'
            try:
                self.n1ql_helper.run_cbq_query(query=alter_index_query,
                                               server=self.n1ql_node)
                return "success", ""
            except Exception as ex:
                self.log.info(str(ex))
                return "", str(ex)
        else:

            cmd = """cbindex -type move -index '{0}' -bucket {1} -with '{{"nodes":"{2}"}}' -auth '{3}:{4}'""".format(
                index_list,
                bucket,
                ip_address,
                username,
                password)
            log.info(cmd)
            if run_from_dst:
                connection_node = dst_node
            else:
                connection_node = src_node
            if remote_host is not None:
                connection_node = remote_host
            remote_client = RemoteMachineShellConnection(connection_node)
            command = "{0}/{1}".format(self.cli_command_location, cmd)
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)
            if error and not [x for x in output if 'Moving Index for' in x]:
                if expect_failure:
                    log.info("cbindex move failed")
                    if queue is not None:
                        queue.put(output, error)
                    return output, error
                else:
                    self.fail("cbindex move failed")
            else:
                log.info("cbindex move started successfully : {0}".format(output))
            if queue is not None:
                queue.put(output, error)
            return output, error

    def _get_indexes_in_move_index_format(self, index_map):
        for bucket in index_map:
            for index in index_map[bucket]:
                return index, 1

    def wait_for_cbindex_move_to_complete(self, dst_node, count):
        no_of_indexes_moved = 0
        exit_count = 0
        while no_of_indexes_moved != count and exit_count != 10:
            index_map = self.get_index_map()
            host_names_after_rebalance = []
            index_distribution_map_after_rebalance = {}
            for bucket in index_map:
                for index in index_map[bucket]:
                    host_names_after_rebalance.append(index_map[bucket][index]['hosts'])
            for node in host_names_after_rebalance:
                index_distribution_map_after_rebalance[node] = index_distribution_map_after_rebalance.get(node, 0) + 1
            if self.use_https:
                port = '18091'
            else:
                port = '8091'
            ip_address = f"{dst_node.ip}:{port}"
            log.info(ip_address)
            log.info(index_distribution_map_after_rebalance)
            if ip_address in index_distribution_map_after_rebalance:
                no_of_indexes_moved = index_distribution_map_after_rebalance[ip_address]
            else:
                no_of_indexes_moved = 0
            log.info("waiting for cbindex move to complete")
            self.sleep(30)
            exit_count += 1
        if no_of_indexes_moved == count:
            log.info("cbindex move completed")
        else:
            self.fail("timed out waiting for cbindex move to complete")

    def run_operation(self, phase="before"):
        if phase == "before":
            self.run_async_index_operations(operation_type="create_index")
        elif phase == "during":
            self.run_async_index_operations(operation_type="query")
        else:
            self.run_async_index_operations(operation_type="query")
            self.run_async_index_operations(operation_type="drop_index")

    def _drop_index(self, query_definition, bucket):
        query = query_definition.generate_index_drop_query(namespace=bucket,
                                                           use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                           use_gsi_for_primary=self.use_gsi_for_primary)
        log.info(query)
        actual_result = self.n1ql_helper.run_cbq_query(query=query,
                                                       server=self.n1ql_server)

    def _create_index_with_defer_build(self, defer_build=True):
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                query = query_definition.generate_index_create_query(namespace=bucket,
                                                                     use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                     deploy_node_info=None, defer_build=defer_build)
                log.info(query)
                create_index_task = self.cluster.async_create_index(
                    server=self.n1ql_server, bucket=bucket, query=query,
                    n1ql_helper=self.n1ql_helper,
                    index_name=query_definition.index_name,
                    defer_build=defer_build)
                create_index_task.result()

    def _build_index(self, sleep=30):
        exceptions = []
        try:
            for bucket in self.buckets:
                for query_definition in self.query_definitions:
                    query = self.n1ql_helper.gen_build_index_query(
                        bucket=bucket, index_list=[query_definition.index_name])
                    build_index_task = self.cluster.async_build_index(
                        server=self.n1ql_server, bucket=bucket, query=query,
                        n1ql_helper=self.n1ql_helper)
                    build_index_task.result()
                    self.sleep(sleep)
        except Exception as ex:
            exceptions.append(str(ex))
        finally:
            return exceptions

    def _set_indexer_compaction(self):
        DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        date = datetime.now()
        dayOfWeek = (date.weekday() + (date.hour + ((date.minute + 5) // 60)) // 24) % 7
        status, content, header = self.rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                                                   indexFromHour=date.hour + ((date.minute + 2) // 60),
                                                                   indexFromMinute=(date.minute + 2) % 60,
                                                                   indexToHour=date.hour + ((date.minute + 3) // 60),
                                                                   indexToMinute=(date.minute + 3) % 60,
                                                                   abortOutside=True)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))

    def _set_bucket_compaction(self):
        compact_tasks = []
        for bucket in self.buckets:
            compact_tasks.append(self.cluster.async_compact_bucket(self.master, bucket))
        for task in compact_tasks:
            task.result()

    def _push_indexer_off_the_cliff(self, index_server=None):
        cnt = 0
        docs = 3000
        while cnt < 20:
            if self._validate_indexer_status_oom(index_server):
                log.info("OOM on index server is achieved")
                return True
            for task in self.kv_mutations(docs):
                task.result()
            self.sleep(30)
            cnt += 1
            docs += 3000
        return False

    def _validate_indexer_status_oom(self, index_server=None):
        if not index_server:
            index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(index_server)
        index_stats = rest.get_indexer_stats()
        if index_stats["indexer_state"].lower() == "paused":
            return True
        else:
            return False

    def kv_mutations(self, docs=1):
        if not docs:
            docs = self.docs_per_day
        gens_load = self.generate_docs(docs)
        self.full_docs_list = self.generate_full_docs_list(gens_load)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        tasks = self.async_load(generators_load=gens_load, op_type="create",
                                batch_size=self.batch_size)
        return tasks

    def kill_erlang(self, server):
        """Kill erlang process running on server.
        """
        NodeHelper._log.info("Killing erlang on server: {0}".format(server))
        shell = RemoteMachineShellConnection(server)
        os_info = shell.extract_remote_info()
        shell.kill_erlang(os_info)
        shell.start_couchbase()
        shell.disconnect()

    def kill_memcached1(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.kill_memcached()
        remote_client.disconnect()



    def _create_backup(self, server, username="Administrator", password="password"):
        remote_client = RemoteMachineShellConnection(server)
        command = self.cli_command_location + "cbbackupmgr config --archive /data/backups --repo example{0}".format(
            self.rand)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not [x for x in output if 'created successfully in archive' in x]:
            self.fail("cbbackupmgr config failed")
        cmd = "cbbackupmgr backup --archive /data/backups --repo example{0} --cluster couchbase://127.0.0.1 --username {1} --password {2}".format(
            self.rand, username, password)
        command = "{0}{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not [x for x in output if 'Backup successfully completed' in x]:
            self.fail("cbbackupmgr backup failed")

    def _create_restore(self, server, username="Administrator", password="password"):
        remote_client = RemoteMachineShellConnection(server)
        cmd = "cbbackupmgr restore --archive /data/backups --repo example{0} --cluster couchbase://127.0.0.1 --username {1} --password {2} --force-updates".format(
            self.rand, username, password)
        command = "{0}{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not [x for x in output if 'Restore completed successfully' in x]:
            self.fail("cbbackupmgr restore failed")

    def _run_prepare_statement(self):
        query = "SELECT * FROM system:indexes"
        result_no_prepare = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)['results']
        query = "PREPARE %s" % query
        prepared = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)['results'][0]
        result_with_prepare = self.n1ql_helper.run_cbq_query(query=prepared, is_prepared=True, server=self.n1ql_server)[
            'results']
        msg = "Query result with prepare and without doesn't match.\nNo prepare: " \
              "%s \nWith prepare: %s" % (result_no_prepare, result_with_prepare)
        for item in result_with_prepare:
            self.assertTrue(item in result_no_prepare, msg)

    def _kill_all_processes_cbq(self, server):
        shell = RemoteMachineShellConnection(server)
        o = shell.execute_command("ps -aef| grep cbq-engine")
        if len(o):
            for cbq_engine in o[0]:
                if cbq_engine.find('grep') == -1:
                    pid = [item for item in cbq_engine.split(' ') if item][1]
                    shell.execute_command("kill -9 %s" % pid)

    def _kill_all_processes_index(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("killall indexer")

    def _kill_fts_process(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.kill_cbft_process()
        shell.disconnect()

    def _create_replica_index(self, query):
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

##############################################################################################
#
#   N1QL Tests
##############################################################################################
    def ansi_join_query(self, stage="", expected=""):
        query = "select * from (select default.country from default d unnest d.travel_details as default limit 1000) d1 " \
                "inner join `travel-sample` t on (d1.country == t.country)"
        if stage == "pre_rebalance":
            self.n1ql_helper.run_cbq_query(query="CREATE INDEX idx ON `travel-sample`(country)", server=self.n1ql_node)
            expected_results = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
            return expected_results
        elif stage =="post_rebalance":
            actual_results = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
            self.assertEqual(expected['metrics']['resultCount'], actual_results['metrics']['resultCount'])
        else:
            self.n1ql_helper.run_cbq_query(query="DROP INDEX `travel-sample`.idx", server=self.n1ql_node)

    def test_kv_and_gsi_rebalance_with_high_ops(self):
        self.rate_limit = self.input.param("rate_limit", 100000)
        self.batch_size = self.input.param("batch_size", 1000)
        self.doc_size = self.input.param("doc_size", 100)
        self.instances = self.input.param("instances", 1)
        self.threads = self.input.param("threads", 1)
        self.use_replica_to = self.input.param("use_replica_to", False)
        self.kv_node_out = self.input.param("kv_node_out")
        self.index_node_out = self.input.param("index_node_out")
        self.num_docs = self.input.param("num_docs", 30000)
        # self.run_operation(phase="before")
        create_index_queries = ["CREATE INDEX idx_body ON default(body) USING GSI",
                              "CREATE INDEX idx_update ON default(`update`) USING GSI",
                              "CREATE INDEX idx_val ON default(val) USING GSI",
                              "CREATE INDEX idx_body1 ON default(body) USING GSI",
                              "CREATE INDEX idx_update1 ON default(`update`) USING GSI",
                              "CREATE INDEX idx_val1 ON default(val) USING GSI"]
        for create_index_query in create_index_queries:
            self._create_replica_index(create_index_query)
        load_thread = threading.Thread(target=self.load_buckets_with_high_ops,
                             name="gen_high_ops_load",
                             args=(self.master, self.buckets[0], self.num_docs,
                                   self.batch_size,
                                   self.threads, 0,
                                   self.instances, 0))
        load_thread.start()
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        services_in = ["kv"]
        # do a swap rebalance of kv
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 [self.servers[self.kv_node_out]], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        load_thread.join()
        errors = self.check_dataloss_for_high_ops_loader(self.master, self.buckets[0],
                                                         self.num_docs,
                                                         self.batch_size,
                                                         self.threads,
                                                         0,
                                                         False, 0, 0,
                                                         False, 0)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print(error)
        if self.num_docs + self.docs_per_day != self.rest.get_active_key_count(self.buckets[0]):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self.num_docs + self.docs_per_day, self.rest.get_active_key_count(self.buckets[0])))
        load_thread1 = threading.Thread(target=self.load_buckets_with_high_ops,
                             name="gen_high_ops_load",
                             args=(self.master, self.buckets[0], self.num_docs * 2,
                                   self.batch_size,
                                   self.threads, 0,
                                   self.instances, 0))
        load_thread1.start()
        # do a swap rebalance of index
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [self.servers[self.nodes_init+1]]
                                                 , [self.servers[self.index_node_out]], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        load_thread1.join()
        errors = self.check_dataloss_for_high_ops_loader(self.master, self.buckets[0],
                                                         self.num_docs * 2,
                                                         self.batch_size,
                                                         self.threads,
                                                         0,
                                                         False, 0, 0,
                                                         False, 0)
        if errors:
            self.log.info("Printing missing keys:")
        for error in errors:
            print(error)
        if self.num_docs * 2 +  self.docs_per_day != self.rest.get_active_key_count(self.buckets[0]):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self.num_docs * 2 +  self.docs_per_day, self.rest.get_active_key_count(self.buckets[0])))
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init + 1]],
                                                      [self.servers[self.index_node_out]], swap_rebalance=True,
                                                      use_https=self.use_https)

    def load_buckets_with_high_ops(self, server, bucket, items, batch=20000,
                                   threads=5, start_document=0, instances=1, ttl=0):
        import subprocess
        cmd_format = "python3 scripts/high_ops_doc_gen.py  --node {0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --instances {9} --ttl {10}"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                items, batch, threads, start_document,
                                cb_version, instances, ttl)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items".format(
                                 items,
                                 total_loaded))

    def check_dataloss_for_high_ops_loader(self, server, bucket, items,
                                           batch=20000, threads=5,
                                           start_document=0,
                                           updated=False, ops=0, ttl=0, deleted=False, deleted_items=0):
        import subprocess
        from lib.memcached.helper.data_helper import VBucketAwareMemcached

        cmd_format = "python3 scripts/high_ops_doc_gen.py  --node {0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} " \
                     "--batch_size {5} --threads {6} --start_document {7} --cb_version {8} --validate"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if updated:
            cmd_format = "{} --updated --ops {}".format(cmd_format, ops)
        if deleted:
            cmd_format = "{} --deleted --deleted_items {}".format(cmd_format, deleted_items)
        if ttl > 0:
            cmd_format = "{} --ttl {}".format(cmd_format, ttl)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                int(items), batch, threads, start_document, cb_version)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        errors = []
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen validator.")
        if output:
            loaded = output.split('\n')[:-1]
            for load in loaded:
                if "Missing keys:" in load:
                    keys = load.split(":")[1].strip().replace('[', '').replace(']', '')
                    keys = keys.split(',')
                    for key in keys:
                        key = key.strip()
                        key = key.replace('\'', '').replace('\\', '')
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append(
                            ("Missing key: {0}, VBucketId: {1}".format(key, vBucketId)))
                if "Mismatch keys: " in load:
                    keys = load.split(":")[1].strip().replace('[', '').replace(']', '')
                    keys = keys.split(',')
                    for key in keys:
                        key = key.strip()
                        key = key.replace('\'', '').replace('\\', '')
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append((
                            "Wrong value for key: {0}, VBucketId: {1}".format(
                                key, vBucketId)))
        return errors
