from datetime import datetime

from base_2i import BaseSecondaryIndexingTests, log
from membase.api.rest_client import RestConnection, RestHelper
import random
import threading
from lib import testconstants
from lib.couchbase_helper.query_definitions import SQLDefinitionGenerator, QueryDefinition, RANGE_SCAN_TEMPLATE
from lib.couchbase_helper.tuq_generators import TuqGenerators
from lib.membase.helper.cluster_helper import ClusterOperationHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase, Backupset
from pytests.fts.fts_base import NodeHelper
from pytests.query_tests_helper import QueryHelperTests
from pytests.tuqquery.tuq import QueryTests


class SecondaryIndexingRebalanceTests(BaseSecondaryIndexingTests, QueryHelperTests, NodeHelper,
                                      EnterpriseBackupRestoreBase):
    def setUp(self):
        super(SecondaryIndexingRebalanceTests, self).setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.create_primary_index = False
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
        elif info == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = testconstants.WIN_COUCHBASE_BIN_PATH_RAW
        elif info == 'mac':
            self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
        else:
            raise Exception("OS not supported.")
        self.rand = random.randint(1, 1000000000)

    def tearDown(self):
        super(SecondaryIndexingRebalanceTests, self).tearDown()

    def test_gsi_rebalance_out_indexer_node(self):
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

    def test_gsi_rebalance_in_indexer_node(self):
        self.run_operation(phase="before")
        self.sleep(30)
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
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [])
        self.run_operation(phase="after")

    def test_gsi_rebalance_swap_rebalance(self):
        self.run_operation(phase="before")
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
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        for i in xrange(20):
            output = self.rest.list_indexer_rebalance_tokens(server=index_server)
            if "rebalancetoken" in output:
                log.info(output)
                break
            self.sleep(2)
        if i == 19 and "rebalancetoken" not in output:
            self.log.warn("rebalancetoken was not returned by /listRebalanceTokens during gsi rebalance")
        self.run_async_index_operations(operation_type="query")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True)
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
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True)
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
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        self.run_operation(phase="during")
        tasks = self.async_run_doc_ops()
        for task in tasks:
            task.result()
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True)
        self.run_operation(phase="after")

    def test_create_index_when_gsi_rebalance_in_progress(self):
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
        self.sleep(2)
        try:
            # when rebalance is in progress, run create index
            index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
            self.n1ql_helper.run_cbq_query(
                query="CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {'defer_build': True};",
                server=self.n1ql_node)
        except Exception, ex:
            log.info(str(ex))
            if "Indexer Cannot Process Create Index - Rebalance In Progress" not in str(ex):
                self.fail("index creation did not fail with expected error : {0}".format(str(ex)))
        else:
            self.fail("index creation did not fail as expected")
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    # rerun once MB-22886 is fixed
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
        self.sleep(2)
        try:
            # when rebalance is in progress, run drop index
            self._drop_index(self.query_definitions[0], self.buckets[0])
        except Exception, ex:
            log.info(str(ex))
            if "Indexer Cannot Process Create Index - Rebalance In Progress" not in str(ex):
                self.fail("index creation did not fail with expected error : {0}".format(str(ex)))
        else:
            self.fail("drop index did not fail as expected: See MB-22886 for more details")
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

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
        except Exception, ex:
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
                                                      swap_rebalance=True)
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
                                                      swap_rebalance=True)
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
                                                      swap_rebalance=True)
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
        if not filter(lambda x: 'Indexer Cannot Process Build Index - Rebalance In Progress' in x, exceptions):
            self.fail(
                "build index did not fail during gsi rebalance with expected error message: See MB-23452 for more details")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.run_operation(phase="after")

    def test_hard_failover_and_full_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=False)
        failover_task.result()
        self.sleep(30)
        # do a full recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + index_server.ip, "full")
        self.rest.add_back_node('ns_1@' + index_server.ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [], [], )
        self.run_operation(phase="after")

    def test_hard_failover_and_delta_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
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
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [], [], )
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
        self.assertEqual(len(map_after_rebalance.keys()), 0)

    def test_graceful_failover_and_full_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=True)
        failover_task.result()
        self.sleep(120)
        # do a full recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + index_server.ip, "full")
        self.rest.add_back_node('ns_1@' + index_server.ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [], [], )
        self.run_operation(phase="after")

    def test_graceful_failover_and_delta_recovery_and_gsi_rebalance(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # failover the indexer node
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=True)
        failover_task.result()
        self.sleep(120)
        # do a delta recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + index_server.ip, "delta")
        self.rest.add_back_node('ns_1@' + index_server.ip)
        reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        self.run_operation(phase="during")
        if reb1:
            result = self.rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [], [], )
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
                                                      swap_rebalance=True)
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
        stopped = RestConnection(self.master).stop_rebalance(wait_timeout=self.wait_timeout / 3)
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
                                                      swap_rebalance=True)
        self.run_operation(phase="after")

    def test_verify_gsi_rebalance_does_not_work_during_create_drop_and_build_index(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.sleep(30)
        services_in = ["index"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # start create index, build index and drop index
        t1 = threading.Thread(target=self._create_index_with_defer_build)
        t1.start()
        self.sleep(2)
        t2 = threading.Thread(target=self._build_index)
        t2.start()
        self.sleep(2)
        t3 = threading.Thread(target=self.run_async_index_operations, args=("drop_index"))
        t3.start()
        self.sleep(2)
        # while create index is running ,rebalance out a indexer node
        try:
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [index_server])
        except Exception, ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail during create index or create index completed before rebalance started")
        t1.join()
        t2.join()
        t3.join()
        # do a cbindex move after a indexer failure
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [], swap_rebalance=True)
        index_servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # run a /cleanupRebalance after a rebalance failure
        for index_server in index_servers:
            output = self.rest.cleanup_indexer_rebalance(server=index_server)
            log.info(output)

    def test_cbindex_move_after_kv_rebalance(self):
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        indexes, no_of_indexes = self._get_indexes_in_move_index_format(map_before_rebalance)
        log.info(indexes)
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
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes1, [], swap_rebalance=True)
        self.run_operation(phase="after")

    # TODO : the behavior of backup/restore during gsi rebalance is still under progress.
    def test_backup_restore_when_gsi_rebalance_in_progress(self):
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
        tasks = self.async_run_doc_ops()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
        # try running enterprise backup
        self._run_backup()
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init]], [index_server],
                                                      swap_rebalance=True)
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
        output, error = self._cbindex_move(index_server, self.servers[self.nodes_init], indexes, expect_failure=True)
        if "Error occured Cannot Process Move Index - Rebalance/MoveIndex In Progress" not in error:
            self.fail("cbindex move succeeded during a rebalance")
        rebalance.result()
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True)
        self.run_operation(phase="after")

    def test_rebalance_of_kv_node_during_index_creation_and_building(self):
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["kv"]
        # start create index and build index
        t1 = threading.Thread(target=self.run_operation, args=("before",))
        t1.start()
        self.sleep(2)
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [],
                                                     services=services_in)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception, ex:
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
        except Exception, ex:
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
        log.info(indexes)
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], [index_server],
                                                 services=services_in)
        self.sleep(10)
        rebalance.result()
        # start create index, build index
        t1 = threading.Thread(target=self._build_index)
        t1.start()
        self.sleep(3)
        output, error = self._cbindex_move(index_server, self.servers[self.nodes_init], indexes, expect_failure=True)
        # TODO : Relook at this after fixing MB-23004
        if "cannot unmarshal array into Go value of type string" not in output[0]:
            self.fail("cbindex move succeeded during a rebalance. See MB-23004 for more details")
        self.run_operation(phase="during")
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
                                                      swap_rebalance=True)
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
                                                      swap_rebalance=True)
        self.run_operation(phase="after")

    def test_gsi_rebalance_when_indexer_is_in_paused_state(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        self.run_operation(phase="before")
        for i in xrange(5):
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
        failover_nodes = [kv_server[1], index_server]
        self._push_indexer_off_the_cliff()
        # Try kv and index failover when indexer is in paused state
        failover_task = self.cluster.async_failover([self.master], failover_nodes=failover_nodes, graceful=False)
        failover_task.result()
        for failover_node in failover_nodes:
            self.rest.add_back_node(failover_node.id)
            self.rest.set_recovery_type(otpNode=failover_node.id, recoveryType="full")
        # rebalance out a node
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
            RestHelper(self.rest).rebalance_reached()
            rebalance.result()
        except Exception, ex:
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
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        self.run_operation(phase="during")
        tasks = self.async_run_doc_ops()
        for task in tasks:
            task.result()
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
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
        #  cbindex move with invalid source host
        output, error = self._cbindex_move(self.servers[self.nodes_init + 1], self.servers[self.nodes_init], indexes,
                                           expect_failure=True)
        if not filter(lambda x: 'Error occured' in x, error):
            self.fail("cbindex move did not fail with expected error message")
        # cbindex move with invalid destination host
        output, error = self._cbindex_move(index_server, self.servers[self.nodes_init + 1], indexes,
                                           expect_failure=True)
        if not filter(lambda x: 'Error occured Unable to find Index service for destination' in x, error):
            self.fail("cbindex move did not fail with expected error message")
        # cbindex move with destination host not reachable
        output, error = self._cbindex_move(index_server, "some_junk_value", indexes, expect_failure=True)
        if not filter(lambda x: 'Error occured Unable to find Index service for destination' in x, error):
            self.fail("cbindex move did not fail with expected error message")

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
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True)
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
                                                      to_add_nodes, nodes_out_list, swap_rebalance=True)
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
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True)
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
                                                      to_add_nodes, nodes_out_list, swap_rebalance=True)
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
        #  cbindex move with invalid src host
        _, error = self._cbindex_move(self.servers[self.nodes_init + 1], "", indexes, expect_failure=True)
        if not filter(lambda x: 'Error occured invalid index specified' in x, error):
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
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [], [], )
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

    # TODO: See MB-23265. Rerun once that is fixed.
    def test_build_index_when_kv_rebalance_in_progress(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        self.sleep(30)
        services_in = ["kv"]
        # rebalance in a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        rebalance.result()
        # rebalance out a node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [kv_node])
        self.run_operation(phase="before")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
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
        except Exception, ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after erl crash")
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
        except Exception, ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after erl crash")
        self.run_operation(phase="after")

    def test_memcache_crash_on_kv_node_during_gsi_rebalance(self):
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
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
            self.kill_memcached(kv_server)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception, ex:
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
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        services_in = ["kv"]
        to_add_nodes1 = [self.servers[self.nodes_init + 1]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes1, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True)

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
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        # failover the kv node when cbindex move is in progress
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[kv_server], graceful=False)
        failover_task.result()
        self.sleep(30)
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, [], swap_rebalance=True)
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
        to_add_nodes = [self.servers[self.nodes_init]]
        services_in = ["index"]
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        to_add_nodes = [self.servers[self.nodes_init + 1]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], to_add_nodes, [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        # failover the indexer node when cbindex move is in progress which is not involved in cbindex move
        failover_task = self.cluster.async_failover([self.master], failover_nodes=[self.servers[self.nodes_init + 1]],
                                                    graceful=False)
        failover_task.result()
        self.sleep(30)
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        self.run_operation(phase="during")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [self.servers[self.nodes_init + 1]], [], swap_rebalance=True)
        # do a full recovery and rebalance
        self.rest.set_recovery_type('ns_1@' + index_server.ip, "full")
        self.rest.add_back_node('ns_1@' + index_server.ip)
        rebalance = self.cluster.rebalance(self.servers[:self.nodes_init + 1], [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

    def test_reboot_on_kv_node_during_gsi_rebalance(self):
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
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
            self.reboot_node(kv_server)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception, ex:
            if "Rebalance stopped by janitor" not in str(ex):
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
        _, error = self._cbindex_move(index_server, " ", indexes, expect_failure=True)
        if not filter(lambda x: 'Error occured Unable to find Index service for destination' in x, error):
            self.fail("cbindex move did not fail with expected error message")
        # cbindex move with index names not specified
        _, error = self._cbindex_move(index_server, self.servers[self.nodes_init], " ", expect_failure=True)
        if not filter(lambda x: 'Error occured invalid index specified' in x, error):
            self.fail("cbindex move did not fail with expected error message")
        # cbindex move with index name which does not exist
        self.run_async_index_operations(operation_type="drop_index")
        _, error = self._cbindex_move(index_server, self.servers[self.nodes_init], indexes, expect_failure=True)
        if not filter(lambda x: 'Error occured invalid index specified' in x, error):
            self.fail("cbindex move did not fail with expected error message")

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
        # do a swap rebalance
        nodes_out_list = self.servers[self.server_out]
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
        command = "{0}couchbase-cli rebalance -c {1} -u {2} -p {3} --server-remove={4}:8091".format(
            self.cli_command_location,
            kv_node.ip, kv_node.rest_username,
            kv_node.rest_password,
            nodes_out_list.ip)
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
        command = "{0}couchbase-cli server-add -c {1} -u {2} -p {3} --server-add={4}:8091 --server-add-username={5} " \
                  "--server-add-password={6}".format(
            self.cli_command_location,
            kv_node.ip, kv_node.rest_username,
            kv_node.rest_password,
            self.servers[self.nodes_init].ip, self.servers[self.nodes_init].rest_username,
            self.servers[self.nodes_init].rest_password)
        o, e = shell.execute_non_sudo_command(command)
        shell.log_command_output(o, e)
        if e or not filter(lambda x: 'SUCCESS: Server added' in x, o):
            self.fail("server-add failed")
        self.sleep(30)
        command = "{0}couchbase-cli rebalance -c {1} -u {2} -p {3}".format(
            self.cli_command_location,
            kv_node.ip, kv_node.rest_username,
            kv_node.rest_password)
        o, e = shell.execute_non_sudo_command(command)
        shell.log_command_output(o, e)
        if e or not filter(lambda x: 'SUCCESS: Rebalance complete' in x, o):
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
        query_definition = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")
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
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True)
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
        except Exception, ex:
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
                threading.Thread(target=self._cbindex_move, args=(index_server, self.servers[self.nodes_init], index,)))
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
                                                      to_add_nodes, [], swap_rebalance=True)
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
        for i in xrange(20):
            self._kill_all_processes_cbq(n1ql_node)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True)
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
            for i in xrange(20):
                self._kill_all_processes_index(self.servers[self.nodes_init])
                self.sleep(2)
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        except Exception, ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after killing indexer node")
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
        except Exception, ex:
            self.fail("rebalance failed with  error : {0}".format(str(ex)))
        finally:
            remote.start_server()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [], [index_node])
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
        except Exception, ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail after index node reboot")
        self.sleep(60)
        # Rerun rebalance to check if it can recover from failure
        for i in xrange(5):
            try:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_server])
                self.sleep(2)
                reached = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                rebalance.result()
            except Exception, ex:
                if "Rebalance failed. See logs for detailed reason. You can try again" in str(ex) and i == 4:
                    self.fail("rebalance did not recover from failure : {0}".format(str(ex)))
                    # Rerun after MB-23900 is fixed.
            else:
                break
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      [], [index_server])
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
        for i in xrange(20):
            self._kill_fts_process(fts_node)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        # kill fts while rebalance is running
        self.sleep(5)
        for i in xrange(20):
            self._kill_fts_process(fts_node)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance, map_after_rebalance,
                                                      stats_map_before_rebalance, stats_map_after_rebalance,
                                                      to_add_nodes, to_remove_nodes, swap_rebalance=True)
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
            self._cbindex_move(index_server, self.servers[self.nodes_init], index)
            self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], i)
            i += 1
        self.sleep(60)
        # Now build the index on the new node
        exceptions = self._build_index()
        if exceptions:
            self.fail("build index after rebalance failed")
        # Move the indexes again
        j = 1
        for index in indexes:
            self._cbindex_move(self.servers[self.nodes_init], index_server, index)
            self.wait_for_cbindex_move_to_complete(index_server, j)
            j += 1
        self.sleep(60)
        self.run_operation(phase="after")

    def _return_maps(self):
        index_map = self.get_index_map()
        stats_map = self.get_index_stats(perNode=False)
        return index_map, stats_map

    def _cbindex_move(self, src_node, dst_node, index_list, username="Administrator", password="password",
                      expect_failure=False, bucket="default"):
        ip_address = str(dst_node).replace("ip:", "").replace(" port", "").replace(" ssh_username:root", "")
        cmd = """cbindex -type move -index '{0}' -bucket {1} -with '{{"nodes":"{2}"}}' -auth '{3}:{4}'""".format(
            index_list,
            bucket,
            ip_address,
            username,
            password)
        log.info(cmd)
        remote_client = RemoteMachineShellConnection(src_node)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not filter(lambda x: 'Moving Index for' in x, output):
            if expect_failure:
                log.info("cbindex move failed")
                return output, error
            else:
                self.fail("cbindex move failed")
        else:
            log.info("cbindex move started successfully : {0}".format(output))
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
            ip_address = str(dst_node).replace("ip:", "").replace(" port", "").replace(" ssh_username:root", "")
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
        try:
            query = query_definition.generate_index_drop_query(
                bucket=bucket,
                use_gsi_for_secondary=self.use_gsi_for_secondary,
                use_gsi_for_primary=self.use_gsi_for_primary)
            log.info(query)
            actual_result = self.n1ql_helper.run_cbq_query(query=query,
                                                           server=self.n1ql_server)
        except Exception, ex:
            log.info(ex)
            query = "select * from system:indexes"
            actual_result = self.n1ql_helper.run_cbq_query(query=query,
                                                           server=self.n1ql_server)
            log.info(actual_result)
        finally:
            return actual_result

    def _create_index_with_defer_build(self, defer_build=True):
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                query = query_definition.generate_index_create_query(
                    bucket=bucket, use_gsi_for_secondary=self.use_gsi_for_secondary,
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
        except Exception, ex:
            exceptions.append(str(ex))
        finally:
            return exceptions

    def _set_indexer_compaction(self):
        DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        date = datetime.now()
        dayOfWeek = (date.weekday() + (date.hour + ((date.minute + 5) / 60)) / 24) % 7
        status, content, header = self.rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                                                   indexFromHour=date.hour + ((date.minute + 2) / 60),
                                                                   indexFromMinute=(date.minute + 2) % 60,
                                                                   indexToHour=date.hour + ((date.minute + 3) / 60),
                                                                   indexToMinute=(date.minute + 3) % 60,
                                                                   abortOutside=True)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))

    def _set_bucket_compaction(self):
        compact_tasks = []
        for bucket in self.buckets:
            compact_tasks.append(self.cluster.async_compact_bucket(self.master, bucket))
        for task in compact_tasks:
            task.result()

    def _push_indexer_off_the_cliff(self):
        cnt = 0
        docs = 3000
        while cnt < 20:
            if self._validate_indexer_status_oom():
                log.info("OOM on index server is achieved")
                return True
            for task in self.kv_mutations(docs):
                task.result()
            self.sleep(30)
            cnt += 1
            docs += 3000
        return False

    def _validate_indexer_status_oom(self):
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

    def kill_memcached(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.kill_memcached()
        remote_client.disconnect()

    def reboot_node(self, node):
        self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 5)
        # disable firewall on these nodes
        self.stop_firewall_on_node(node)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self, wait_if_warmup=True)

    def _create_backup(self, server, username="Administrator", password="password"):
        remote_client = RemoteMachineShellConnection(server)
        command = self.cli_command_location + "cbbackupmgr config --archive /data/backups --repo example{0}".format(
            self.rand)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not filter(lambda x: 'created successfully in archive' in x, output):
            self.fail("cbbackupmgr config failed")
        cmd = "cbbackupmgr backup --archive /data/backups --repo example{0} --cluster couchbase://127.0.0.1 --username {1} --password {2}".format(
            self.rand, username, password)
        command = "{0}{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not filter(lambda x: 'Backup successfully completed' in x, output):
            self.fail("cbbackupmgr backup failed")

    def _create_restore(self, server, username="Administrator", password="password"):
        remote_client = RemoteMachineShellConnection(server)
        cmd = "cbbackupmgr restore --archive /data/backups --repo example{0} --cluster couchbase://127.0.0.1 --username {1} --password {2}".format(
            self.rand, username, password)
        command = "{0}{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not filter(lambda x: 'Restore completed successfully' in x, output):
            self.fail("cbbackupmgr restore failed")

    def _run_prepare_statement(self):
        query = "SELECT * FROM system:indexes"
        result_no_prepare = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)['results']
        query = "PREPARE %s" % query
        prepared = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)['results'][0]
        result_with_prepare = self.n1ql_helper.run_cbq_query(query=prepared, is_prepared=True, server=self.n1ql_server)[
            'results']
        msg = "Query result with prepare and without doesn't match.\nNo prepare: %s ... %s\nWith prepare: %s ... %s"
        self.assertTrue(sorted(result_no_prepare) == sorted(result_with_prepare),
                        msg % (result_no_prepare[:100], result_no_prepare[-100:],
                               result_with_prepare[:100], result_with_prepare[-100:]))

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
