from datetime import datetime

from base_2i import BaseSecondaryIndexingTests, log
from membase.api.rest_client import RestConnection, RestHelper
import random
import threading
from lib import testconstants
from lib.couchbase_helper.tuq_generators import TuqGenerators
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase, Backupset
from pytests.query_tests_helper import QueryHelperTests


class SecondaryIndexingRebalanceTests(BaseSecondaryIndexingTests, QueryHelperTests):
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
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [])
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
            self.fail("drop index did not fail as expected")
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
            self.fail("build index did not fail during gsi rebalance with expected error message")
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
        self._cbindex_move(index_server, self.servers[self.nodes_init], indexes)
        self.wait_for_cbindex_move_to_complete(self.servers[self.nodes_init], no_of_indexes)
        self.run_operation(phase="during")
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
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
        t1 = threading.Thread(target=self.run_operation, args=("before", ))
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
        self.sleep(3)
        rebalance.result()
        # start create index, build index
        t1 = threading.Thread(target=self._build_index)
        t1.start()
        output, error = self._cbindex_move(index_server, self.servers[self.nodes_init])
        # TODO : Relook at this after fixing MB-23004
        if "cannot unmarshal array into Go value of type string" not in output[0]:
            self.fail("cbindex move succeeded during a rebalance")
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

    def test_when_indexer_is_paused_when_gsi_rebalance_in_progress(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
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

    def _return_maps(self):
        index_map = self.get_index_map()
        stats_map = self.get_index_stats(perNode=False)
        return index_map, stats_map

    def _cbindex_move(self, src_node, dst_node, index_list, username="Administrator", password="password",
                      expect_failure=False):
        ip_address = str(dst_node).replace("ip:", "").replace(" port", "").replace(" ssh_username:root", "")
        cmd = """cbindex -type move -indexes '{0}' -with '{{"dest":"{1}"}}' -auth '{2}:{3}'""".format(index_list,
                                                                                                      ip_address,
                                                                                                      username,
                                                                                                      password)
        log.info(cmd)
        remote_client = RemoteMachineShellConnection(src_node)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            if expect_failure:
                log.info("cbindex move failed")
                return output, error
            else:
                self.fail("cbindex move failed")
        else:
            log.info("cbindex move started successfully : {0}".format(output))
        return output, error

    def _get_indexes_in_move_index_format(self, index_map):
        bucket_index_array = []
        for bucket in index_map:
            for index in index_map[bucket]:
                bucket_index_array.append(bucket + ":" + index)
        bucket_index_string = ",".join(bucket_index_array[:len(bucket_index_array) / 2])
        return bucket_index_string, len(bucket_index_array) / 2

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

    def _build_index(self):
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
        except Exception, ex:
            exceptions.append(str(ex))
        finally:
            return exceptions

    def _run_backup(self):
        remote_client = RemoteMachineShellConnection(self.servers[0])
        command = "{0}/cbbackupmgr config --archive /data/backups --repo example ".format(self.cli_command_location)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        command = "{0}/cbbackupmgr backup --archive /data/backups --repo example --cluster {1} --username Administrator --password password".format(
            self.cli_command_location, self.servers[0].ip)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)

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