from .xdcrnewbasetests import XDCRNewBaseTest
from membase.api.rest_client import RestConnection

import random
import time


class XDCRCollectionsTests(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        self.initial_xdcr = random.choice([True, False])
        try:
            if self.initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()
        except Exception as e:
            self.fail(str(e))
        self.dest_cluster.flush_buckets(buckets=["default"])
        if "C1" in self._disable_compaction:
            self.src_cluster.disable_compaction()
        if "C2" in self._disable_compaction:
            self.dest_cluster.disable_compaction()

    def tearDown(self):
        XDCRNewBaseTest.tearDown(self)

    def test_migration_implicit_mapping(self):
        try:
            self.src_rest.set_xdcr_param('default', 'default', 'collectionsMigrationMode', "true")
        except Exception as e:
            self.fail(str(e))

    def test_migration_empty_mapping(self):
        try:
            setting_val_map = {"collectionsMigrationMode": "true",
                               "collectionsExplicitMapping": "true",
                               "colMappingRules": "{}"}
            self.src_rest.set_xdcr_params('default', 'default', setting_val_map)
        except Exception as e:
            if "Migration and Explicit mapping cannot both be active" in str(e):
                self.log.info("Migration with explicit mapping failed as expected")
        else:
            self.fail("Migration with explicit mapping did not fail as expected")

    def test_migration_adv_filter(self):
        try:
            setting_val_map = {"filterExpression": "age>0",
                               "filterSkipRestream": "false",
                               "collectionsMigrationMode": "true",
                               "colMappingRules": '{' + "\"REGEXP_CONTAINS(META().id,'[a-z].*[0-9]')\""
                                                  + ':'
                                                  + "\"scope_1.collection_2\"" + '}'
                               }
            self.src_rest.set_xdcr_params('default', 'default', setting_val_map)
        except Exception as e:
            self.fail(str(e))
        self.verify_results()

    def test_migration_1rule_mapping(self):
        try:
            lhs = "\"REGEXP_CONTAINS(META().id,'1$')\""
            rhs = '"scope_1.collection_1"'
            setting_val_map = {"collectionsMigrationMode": "true",
                               "colMappingRules": '{' + lhs + ':' + rhs + '}',
                               }
            self.src_rest.set_xdcr_params('default', 'default', setting_val_map)
        except Exception as e:
            self.fail(str(e))
        self.verify_results()

    def test_migration_2rule_mapping(self):
        try:
            lhs = "\"REGEXP_CONTAINS(META().id,'^d') AND 'country' IS NOT MISSING\""
            rhs = '"scope_1.collection_1"'
            setting_val_map = {"collectionsMigrationMode": "true",
                               "colMappingRules": '{' + lhs + ':' + rhs + '}',
                               }
            self.src_rest.set_xdcr_params('default', 'default', setting_val_map)
        except Exception as e:
            self.fail(str(e))
        self.verify_results()

    def test_migration_incremental(self):
        try:
            for batch in range(10):
                self.log.info("Migration batch {}:".format(batch + 1))
                # first migrate doc ids ending with 0, then 1 and so on until all docs are migrated
                lhs = "\"REGEXP_CONTAINS(META().id,'" + str(batch) + "$')\""
                rhs = '"scope_1.collection_1"'
                setting_val_map = {"collectionsMigrationMode": "true",
                                   "collectionsExplicitMapping": "false",
                                   "colMappingRules": '{' + lhs + ':' + rhs + '}',
                                   }
                self.src_rest.set_xdcr_params('default', 'default', setting_val_map)
                # wait_for_replication_to_catchup()
                time.sleep(60)
        except Exception as e:
            self.fail(str(e))
        self.verify_results()

    def get_cluster_objects_for_input(self, input):
        """returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated by ':'
           eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            clusters.append(self.get_cb_cluster_by_name(cluster_name))
        return clusters

    def test_migration_incremental_with_clusterop(self):
        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)

        try:
            for batch in range(10):
                self.log.info("Migration batch {}:".format(batch + 1))
                lhs = "\"REGEXP_CONTAINS(META().id,'^doc_" + str(batch) + "')\""
                rhs = '"scope_1.collection_1"'
                setting_val_map = {"collectionsMigrationMode": "true",
                                   "colMappingRules": '{' + lhs + ':' + rhs + '}',
                                   }
                self.src_rest.set_xdcr_params('default', 'default', setting_val_map)
                # Do one cluster op in the middle of migration
                if batch == 5:
                    if pause:
                        self.src_rest.set_xdcr_param('default', 'default', 'pauseRequested', 'true')
                        time.sleep(30)
                        self.src_rest.set_xdcr_param('default', 'default', 'pauseRequested', 'false')

                    if rebalance_in:
                        for cluster in self.get_cluster_objects_for_input(rebalance_in):
                            cluster.async_rebalance_in()

                    if failover:
                        for cluster in self.get_cluster_objects_for_input(failover):
                            cluster.failover_and_rebalance_nodes(graceful=graceful,
                                                                 rebalance=True)

                    if rebalance_out:
                        for cluster in self.get_cluster_objects_for_input(rebalance_out):
                            cluster.async_rebalance_out()

                    if swap_rebalance:
                        for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                            cluster.async_swap_rebalance()

                    if reboot:
                        for cluster in self.get_cluster_objects_for_input(reboot):
                            cluster.warmup_node()
                    #wait for clusterop to finish
                    time.sleep(60)

                #wait_for_replication_to_catchup()
                time.sleep(60)
        except Exception as e:
            self.fail(str(e))
        self.verify_results()
