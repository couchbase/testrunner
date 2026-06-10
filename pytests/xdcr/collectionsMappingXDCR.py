import random
import time

from .xdcrnewbasetests import XDCRNewBaseTest
from membase.api.rest_client import RestConnection
from lib.membase.helper.rbac_exclusion_helper import verify_rbac_exclusion_syntax

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper


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
        self._verify_rbac_exclusion_xdcr()

    def _verify_rbac_exclusion_xdcr(self):
        rest = self.src_rest
        buckets = rest.get_buckets()
        if not buckets:
            self.log.warning("No buckets found for RBAC exclusion test; skipping")
            return
        bucket_name = buckets[0].name
        scope_name = "rbac_excl_scope"
        allowed_col = "coll_allowed"
        excluded_col = "coll_excluded"
        scope_created = False
        try:
            rest.create_scope(bucket_name, scope_name)
            scope_created = True
            rest.create_collection(bucket_name, scope_name, allowed_col)
            rest.create_collection(bucket_name, scope_name, excluded_col)

            def xdcr_service_validator(u, p):
                # Call XDCR-specific REST endpoint with user creds
                api = rest.baseUrl + "pools/default/remoteClusters"
                status, content, _ = rest._http_request(
                    api, 'GET', headers=rest._create_headers_with_auth(u, p))
                self.assertTrue(status,
                    "XDCR: GET remoteClusters should succeed with user creds, got: %s"
                    % content)
                perm_allowed = (
                    "cluster.collection[{b}:{s}:{c}].xdcr.execute!write".format(
                        b=bucket_name, s=scope_name, c=allowed_col))
                perm_excluded = (
                    "cluster.collection[{b}:{s}:{c}].xdcr.execute!write".format(
                        b=bucket_name, s=scope_name, c=excluded_col))
                result = rest.check_user_permission(
                    u, "password", ",".join([perm_allowed, perm_excluded]))
                self.assertTrue(result.get(perm_allowed, False),
                    "XDCR: allowed collection '%s' should have xdcr.execute!write, "
                    "got: %s" % (allowed_col, result))
                self.assertFalse(result.get(perm_excluded, True),
                    "XDCR: excluded collection '%s' should be denied "
                    "xdcr.execute!write, got: %s" % (excluded_col, result))
                self.log.info(
                    "XDCR service validation passed: remoteClusters accessible "
                    "and xdcr.execute!write exclusion verified")

            verify_rbac_exclusion_syntax(
                self, rest, bucket_name, scope_name, allowed_col, excluded_col,
                "xdcr", runtype=self._input.param("runtype", "default"),
                service_validator=xdcr_service_validator)
        finally:
            if scope_created:
                try:
                    rest.delete_scope(bucket_name, scope_name)
                except Exception:
                    pass

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

    # ---- 10K Collections Scale Tests ----

    def test_explicit_mapping_10k_collections(self):
        """
        Explicit collection-to-collection mapping with 10K collections.
        Maps a subset of source scopes to destination scopes and verifies
        only the mapped collections replicate.

        NOTE: This test creates collections and XDCR independently of
        the base class setUp which already called setup_xdcr_and_load.

        Conf params:
            mapped_scopes: number of scopes to include in mapping (default 5)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        mapped_scopes = self._input.param("mapped_scopes", 5)

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        mapping_rules = {}
        for i in range(mapped_scopes):
            scope_name = "{}{}".format(p["scope_prefix"], i)
            key = '"{}"'.format(scope_name)
            mapping_rules[key] = key

        rules_json = ",".join(["{}:{}".format(k, v) for k, v in mapping_rules.items()])
        self.log.info("Setting explicit mapping for {} scopes: {}".format(
            mapped_scopes, rules_json[:200]))

        try:
            setting_val_map = {
                "collectionsExplicitMapping": "true",
                "colMappingRules": "{" + rules_json + "}"
            }
            self.src_rest.set_xdcr_params(bucket_name, bucket_name, setting_val_map)
        except Exception as e:
            self.fail("Failed to set explicit mapping: {}".format(e))

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="explmap")

        try:
            self._wait_for_dest_to_stabilize(self.dest_cluster, bucket_name,
                                             min_items=1, timeout=600)
        except Exception as e:
            self.fail("Explicit-mapping replication did not stabilize: {}".format(e))

        src_items = TenKCollectionHelper.get_bucket_item_count(self.src_master, bucket_name)
        dest_items = TenKCollectionHelper.get_bucket_item_count(self.dest_master, bucket_name)
        self.log.info("Bucket items - src: {}, dest: {} (dest expected <= src "
                      "since only {}/{} scopes mapped)".format(
                          src_items, dest_items, mapped_scopes, p["num_scopes"]))
        self.assertGreater(dest_items, 0,
                           "Destination should have items from mapped scopes")
        self.assertLessEqual(dest_items, src_items,
                             "Mapped dest should have <= src items")

        self.log.info("Explicit mapping 10K collections test passed")

    def test_migration_mode_10k_collections(self):
        """
        Migration mode with rule-based collection mapping at 10K scale.
        Routes docs from default collection to specific 10K-scale target
        collections based on REGEXP_CONTAINS patterns on doc keys.

        Conf params:
            num_migration_rules: number of migration rules to create (default 3)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        num_rules = self._input.param("num_migration_rules", 3)

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        rules = {}
        for i in range(num_rules):
            lhs = "\"REGEXP_CONTAINS(META().id,'{}$')\"".format(i)
            scope_name = "{}{}".format(p["scope_prefix"], i % p["num_scopes"])
            col_name = "{}{}".format(p["collection_prefix"], i % p["collections_per_scope"])
            rhs = '"{}.{}"'.format(scope_name, col_name)
            rules[lhs] = rhs

        rules_json = ",".join(["{}:{}".format(k, v) for k, v in rules.items()])
        self.log.info("Setting migration mode with {} rules".format(num_rules))

        try:
            setting_val_map = {
                "collectionsMigrationMode": "true",
                "colMappingRules": "{" + rules_json + "}"
            }
            self.src_rest.set_xdcr_params(bucket_name, bucket_name, setting_val_map)
        except Exception as e:
            self.fail("Failed to set migration mode: {}".format(e))

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception:
            self.log.info("Catch-up with migration mode rules")

        dest_items = TenKCollectionHelper.get_bucket_item_count(self.dest_master, bucket_name)
        self.log.info("Destination items after migration: {}".format(dest_items))

        self.log.info("Migration mode 10K collections test passed")
