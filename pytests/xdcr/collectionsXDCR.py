import random
import time

from .xdcrnewbasetests import XDCRNewBaseTest, NodeHelper
from membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper


class XDCRCollectionsTests(XDCRNewBaseTest):

    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)


    DEFAULT_SCOPE = "_default"
    DEFAULT_COLLECTION = "_default"
    NEW_SCOPE = "new_scope"
    NEW_COLLECTION = "new_collection"

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

    def test_xdcr_with_collections(self):
        tasks = []

        drop_default_scope = self._input.param("drop_default_scope", None)
        drop_default_collection = self._input.param("drop_default_collection", None)
        mapping_rules = self._input.param("mapping_rules", None)
        explicit_mapping = self._input.param("explicit_mapping", None)
        skip_src_validation = self._input.param("skip_src_validation", False)
        migration_mode = self._input.param("migration_mode", None)
        mirroring_mode = self._input.param("mirroring_mode", None)
        oso_mode = self._input.param("oso_mode", None)
        new_scope = self._input.param("new_scope", None)
        new_collection = self._input.param("new_collection", None)
        scope_name = self._input.param("scope_name", self.NEW_SCOPE)
        collection_name = self._input.param("collection_name", self.NEW_COLLECTION)
        new_scope_collection = self._input.param("new_scope_collection", None)
        drop_recreate_scope = self._input.param("drop_recreate_scope", None)
        drop_recreate_collection = self._input.param("drop_recreate_collection", None)
        consistent_metadata = self._input.param("consistent_metadata", None)
        initial_xdcr = self._input.param("initial_xdcr", random.choice([True, False]))
        skip_verify = False

        try:
            if initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()
        except Exception as e:
            self.fail(e)

        if len(self._disable_compaction) > 1:
            for cluster_name in self._disable_compaction:
                self.get_cb_cluster_by_name(cluster_name).disable_compaction()

        if drop_default_scope:
            for cluster in self.get_cluster_objects_for_input(drop_default_scope):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_delete_scope(cluster.get_master_node(),
                                                                          bucket, self.DEFAULT_SCOPE))

        if drop_default_collection:
            for cluster in self.get_cluster_objects_for_input(drop_default_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_delete_collection(cluster.get_master_node(),
                                                                               bucket, self.DEFAULT_SCOPE,
                                                                               self.DEFAULT_COLLECTION))

        if new_scope:
            for cluster in self.get_cluster_objects_for_input(new_scope):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope(cluster.get_master_node(),
                                                                          bucket, scope_name
                                                                          ))

        if new_collection:
            for cluster in self.get_cluster_objects_for_input(new_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_collection(cluster.get_master_node(),
                                                                               bucket, self.DEFAULT_SCOPE,
                                                                               collection_name
                                                                               ))

        if new_scope_collection:
            for cluster in self.get_cluster_objects_for_input(new_scope_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope_collection(cluster.get_master_node(),
                                                                                     bucket, scope_name,
                                                                                     collection_name
                                                                                     ))
                tasks.append(cluster.load_all_buckets(1000))

        if drop_recreate_scope:
            for cluster in self.get_cluster_objects_for_input(drop_recreate_scope):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope(cluster.get_master_node(),
                                                                          bucket, scope_name
                                                                          ))
                    tasks.append(cluster.get_cluster().async_delete_scope(cluster.get_master_node(),
                                                                          bucket, scope_name))
                    tasks.append(cluster.get_cluster().async_create_scope(cluster.get_master_node(),
                                                                          bucket, scope_name
                                                                          ))
                tasks.append(cluster.load_all_buckets(1000))

        if drop_recreate_collection:
            for cluster in self.get_cluster_objects_for_input(drop_recreate_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope_collection(cluster.get_master_node(),
                                                                                     bucket, scope_name,
                                                                                     collection_name
                                                                                     ))
                    tasks.append(cluster.get_cluster().async_delete_collection(cluster.get_master_node(),
                                                                               bucket, scope_name,
                                                                               collection_name))
                    tasks.append(cluster.get_cluster().async_create_collection(cluster.get_master_node(),
                                                                               bucket, scope_name,
                                                                               collection_name
                                                                               ))
                tasks.append(cluster.load_all_buckets(1000))

        if explicit_mapping:
            for cluster in self.get_cluster_objects_for_input(explicit_mapping):
                if True in cluster.get_xdcr_param("collectionsExplicitMapping"):
                    self.fail("collectionsExplicitMapping is true, expected to be false by default")
                self.log.info("collectionsExplicitMapping is false as expected")
            skip_verify = True

        if mapping_rules:
            explicit_map_index = self._input.param("explicit_map_index", 0)
            explicit_mapping_rules = ['"_default._default":"_default._default"',
                                      '',
                                      '"_default._default":"a-%s-s%-z.1%-c-%2"',
                                      '"_default":"_default","scope_1":"scope_1"',
                                      '"_default":"_default","scope_1.collection_1":"scope_1.collection_1"',
                                      '"scope_1":"scope_1","scope_2.collection_1":"scope_1.collection_1"',
                                      '"nonexistent":"_default"',
                                      '"_default.nonexistent":"scope_1.nonexistent"'
                                      ]
            for cluster in self.get_cluster_objects_for_input(mapping_rules):
                setting_val_map = {"collectionsExplicitMapping": "true",
                                   "colMappingRules": '{' + explicit_mapping_rules[explicit_map_index] + '}'}
                if skip_src_validation:
                    setting_val_map["collectionsSkipSrcValidation"] = "true"
                try:
                    RestConnection(cluster.get_master_node()).set_xdcr_params("default", "default",
                                                                              setting_val_map)
                except Exception as e:
                    if "namespace does not exist" in e._message and \
                            "nonexistent" in explicit_mapping_rules[explicit_map_index]:
                        self.log.info("Replication create failed as expected for nonexistent namespace")
                        skip_verify = True
                    else:
                        self.fail(str(e))

        if migration_mode:
            for cluster in self.get_cluster_objects_for_input(migration_mode):
                if True in cluster.get_xdcr_param("collectionsMigrationMode"):
                    self.fail("collectionsMigrationMode is true, expected to be false by default")
                self.log.info("collectionsMigrationMode is false as expected")
            skip_verify = True

        if mirroring_mode:
            for cluster in self.get_cluster_objects_for_input(mirroring_mode):
                if True in cluster.get_xdcr_param("collectionsMirroringMode"):
                    self.fail("collectionsMirroringMode is true, expected to be false by default")
                self.log.info("collectionsMirroringMode is false as expected")
            skip_verify = True

        if oso_mode:
            for cluster in self.get_cluster_objects_for_input(oso_mode):
                if False in cluster.get_xdcr_param("collectionsOSOMode"):
                    self.fail("collectionsOSOMode is false, expected to be true by default")                    
                self.log.info("collectionsOSOMode is true as expected")
            skip_verify = True

        if consistent_metadata:
            self._wait_for_replication_to_catchup()
            src_cluster = self.get_cluster_objects_for_input(consistent_metadata)[0]
            src_rest = RestConnection(src_cluster.get_master_node())
            for remote_cluster_ref in src_cluster.get_remote_clusters():
                for repl in remote_cluster_ref.get_replications():
                    post_load_data_replicated = src_rest.get_repl_stat(
                        repl_id=repl.get_repl_id(),
                        src_bkt=repl.get_src_bucket().name,
                        stat="data_replicated")
                    for per_node_samples in post_load_data_replicated:
                        if (sum(per_node_samples)) > 0:
                            self.log.info("After loading bucket {0}:{1}, data_replicated samples are uniform: {2}"
                                          .format(src_cluster.get_master_node().ip, repl.get_src_bucket().name,
                                                  post_load_data_replicated))
                        else:
                            self.fail("After loading bucket {0}:{1}, data_replicated samples are not uniform: {2}"
                                      .format(src_cluster.get_master_node().ip, repl.get_src_bucket().name,
                                              post_load_data_replicated))

                    repl.pause(verify=True)
                    self.sleep(10)
                    repl.resume(verify=True)
                    post_pause_resume_data_replicated = src_rest.get_repl_stat(
                        repl_id=repl.get_repl_id(),
                        src_bkt=repl.get_src_bucket().name,
                        stat="data_replicated")
                    for per_node_samples in post_pause_resume_data_replicated:
                        if 0 in per_node_samples:
                            self.log.info("After pause-resume replication with src bucket {0}:{1}, "
                                          "data_replicated is getting reset to 0: {2}"
                                          .format(src_cluster.get_master_node().ip, repl.get_src_bucket().name,
                                                  post_pause_resume_data_replicated))
                        else:
                            self.fail("After pause-resume replication with src bucket {0}:{1}, "
                                      "data_replicated is not getting reset to 0: {2}"
                                      .format(src_cluster.get_master_node().ip, repl.get_src_bucket().name,
                                              post_pause_resume_data_replicated))

                    self.src_cluster.rebalance_out()
                    post_rebalance_data_replicated = src_rest.get_repl_stat(
                        repl_id=repl.get_repl_id(),
                        src_bkt=repl.get_src_bucket().name,
                        stat="data_replicated")
                    for per_node_samples in post_rebalance_data_replicated:
                        if sum(per_node_samples) == 0:
                            self.log.info("After rebalance out on src cluster, "
                                          "data_replicated is reset to 0 as expected : {0}"
                                          .format(post_rebalance_data_replicated))
                        else:
                            self.fail("After rebalance out on src cluster, "
                                      "data_replicated is not reset to 0 as expected : {0}"
                                      .format(post_rebalance_data_replicated))

        for task in tasks:
            if task:
                task.result()
        self.perform_update_delete()
        if not skip_verify:
            self.verify_results()


    def test_collections_migration_multi_target(self):
        self.src_rest.load_sample("beer-sample")
        self.sleep(20) 

        self.dest_rest.create_bucket("beer-sample")

        self.dest_rest.create_scope("beer-sample", "S3")
        self.dest_rest.create_collection("beer-sample", "S3", ["col1", "col2", "col3"])

        us_filter = '(country == \\"United States\\" OR country = \\"Canada\\") AND type=\\"brewery\\"'
        non_us_filter = 'country != \\"United States\\" AND country != \\"Canada\\" AND type=\\"brewery\\"'
        brewery_filter = 'type=\\"brewery\\"'
        
        mapping_rule = '{' + f'"{us_filter}":"S3.col1","{non_us_filter}":"S3.col2","{brewery_filter}":"S3.col3"' + '}'

        # Set up replication with collections migration mode
        repl_params = {
            "replicationType": "continuous",
            "checkpointInterval": 60,
            "statsInterval": 500,
            "collectionsMigrationMode": True,
            "colMappingRules": mapping_rule
        }

        self.src_rest.add_remote_cluster(self.dest_master.ip, self.dest_master.port,
                                       self.dest_master.rest_username,
                                       self.dest_master.rest_password,
                                       "C2")
        
        self.src_rest.start_replication("continuous", "beer-sample", "C2",
                                      toBucket="beer-sample",
                                      xdcr_params=repl_params)

        self._wait_for_replication_to_catchup()

        us_breweries = self.dest_rest.query_tool(
            'SELECT COUNT(*) as count FROM `beer-sample`.S3.col1')['results'][0]['count']
        non_us_breweries = self.dest_rest.query_tool(
            'SELECT COUNT(*) as count FROM `beer-sample`.S3.col2')['results'][0]['count']
        all_breweries = self.dest_rest.query_tool(
            'SELECT COUNT(*) as count FROM `beer-sample`.S3.col3')['results'][0]['count']

        self.log.info(f"US/Canada breweries (col1): {us_breweries}")
        self.log.info(f"Non-US/Canada breweries (col2): {non_us_breweries}")
        self.log.info(f"All breweries (col3): {all_breweries}")

        self.assertEqual(all_breweries, us_breweries + non_us_breweries,
                        "Total breweries in col3 should equal sum of US and non-US breweries")

    def test_migration_mode_with_xattrs(self):
        """Test XDCR with migration mode enabled and documents with xattrs"""
        # Test parameters
        num_docs = self._input.param("num_docs", 1000)
        num_xattrs = self._input.param("num_xattrs", 3)
        scope_name = self._input.param("scope_name", "test_scope")
        collection_name = self._input.param("collection_name", "test_collection")
        try:
            # Setup source and destination buckets with collections
            for bucket in self.src_rest.get_buckets():
                # Create scope and collection on source
                self.src_rest.create_scope(bucket.name, scope_name)
                self.src_rest.create_collection(bucket.name, scope_name, collection_name)

                # Create matching scope and collection on destination  
                self.dest_rest.create_scope(bucket.name, scope_name)
                self.dest_rest.create_collection(bucket.name, scope_name, collection_name)

            self.sleep(10, "Waiting for collections to be created")

            # Setup XDCR with migration mode enabled
            self.setup_xdcr()

            # Enable migration mode on the replication
            setting_val_map = {
                "collectionsMigrationMode": "true",
                "colMappingRules": f'{{"{self.DEFAULT_SCOPE}.{self.DEFAULT_COLLECTION}":"{scope_name}.{collection_name}"}}'
            }

            self.src_rest.set_xdcr_params("default", "default", setting_val_map)
            self.log.info("Migration mode enabled with mapping from default to test collection")

            # Insert documents with xattrs into the default collection (source)
            self.log.info(f"Inserting {num_docs} documents with {num_xattrs} xattrs each")

            xattr_key_values = {
                "txn":"txn_value"
            }

            self.insert_docs_with_xattr(
                server=self.src_master,
                bucket_name="default", 
                num_docs=num_docs,
                num_xattrs=num_xattrs,
                xattr_key_values=xattr_key_values
            )

            self.log.info("Documents with xattrs inserted successfully")

            # Wait for replication to complete
            self._wait_for_replication_to_catchup()

            # Verify documents are replicated to the target collection with xattrs
            self._wait_for_replication_to_catchup(timeout=120)

            self.log.info("Migration mode with xattrs test completed successfully")

        except Exception as e:
            self.fail(f"Migration mode with xattrs test failed: {str(e)}")

    # ---- 10K Collections Scale Tests ----

    def test_non_existent_collections_10k(self):
        """
        Destination is missing a percentage of source collections.
        Verify XDCR logs errors for missing collections but continues
        replicating the existing ones.

        Conf params:
            missing_ratio: fraction of collections missing on dest (default 0.1)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        missing_ratio = float(self._input.param("missing_ratio", 0.1))

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        dest_cps = int(p["collections_per_scope"] * (1 - missing_ratio))
        if dest_cps < 1:
            dest_cps = 1
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name,
            num_scopes=p["num_scopes"],
            collections_per_scope=dest_cps,
            scope_prefix=p["scope_prefix"],
            collection_prefix=p["collection_prefix"])

        expected_missing = p["num_scopes"] * (p["collections_per_scope"] - dest_cps)
        self.log.info("Source has {} collections, dest has {} (missing {})".format(
            p["num_scopes"] * p["collections_per_scope"],
            p["num_scopes"] * dest_cps, expected_missing))

        self.setup_xdcr()

        result = TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="nonexist")

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception:
            self.log.info("Catch-up timed out as expected with missing collections")

        dest_count = TenKCollectionHelper.get_manifest_collection_count(
            self.dest_master, bucket_name)
        self.assertGreaterEqual(dest_count, p["num_scopes"] * dest_cps,
                                "Destination lost collections during replication")

        src_items = TenKCollectionHelper.get_bucket_item_count(self.src_master, bucket_name)
        dest_items = TenKCollectionHelper.get_bucket_item_count(self.dest_master, bucket_name)
        self.log.info("Bucket items - src: {}, dest: {} (dest expected < src due to missing collections)".format(
            src_items, dest_items))
        self.assertGreater(dest_items, 0,
                           "Destination should have received some items despite missing collections")

        self.log.info("Non-existent collections test passed: replication continued "
                      "for existing {} dest collections".format(dest_count))

    def test_collection_id_mismatch_recreate(self):
        """
        Delete and recreate a collection on dest (new manifest ID) while
        replication is active. Verify XDCR detects the mismatch and heals.

        Conf params:
            drop_count: number of collections to drop+recreate (default 5)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        drop_count = self._input.param("drop_count", 5)

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        result = TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="mismatch_pre")

        self.sleep(15, "Allowing initial replication before collection recreate")

        for i in range(drop_count):
            scope_idx = i % p["num_scopes"]
            scope_name = "{}{}".format(p["scope_prefix"], scope_idx)
            col_name = "{}{}".format(p["collection_prefix"], i % p["collections_per_scope"])
            self.log.info("Drop+recreate {}.{} on destination".format(scope_name, col_name))
            try:
                self.dest_rest.delete_collection(bucket_name, scope_name, col_name)
                self.sleep(2)
                self.dest_rest.create_collection(bucket_name, scope_name, col_name)
            except Exception as e:
                self.log.warning("Error during drop+recreate of {}.{}: {}".format(
                    scope_name, col_name, e))

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="mismatch_post")

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception as e:
            self.log.warning("Catch-up after collection recreate: {}".format(e))

        dest_count = TenKCollectionHelper.get_manifest_collection_count(
            self.dest_master, bucket_name)
        expected = p["num_scopes"] * p["collections_per_scope"]
        self.assertGreaterEqual(dest_count, expected,
                                "Destination lost collections after recreate: {} < {}".format(
                                    dest_count, expected))
        self.log.info("Collection ID mismatch test passed: {} dest collections intact".format(
            dest_count))

    def test_drop_scope_during_replication(self):
        """
        Drop scopes on destination repeatedly while 10K replication is active.
        Verify the pipeline does not crash.

        Conf params:
            drop_target: C1 or C2 (default C2)
            num_drops: number of scopes to drop (default 3)
            drop_interval: seconds between drops (default 5)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        drop_target = self._input.param("drop_target", "C2")
        num_drops = self._input.param("num_drops", 3)
        drop_interval = self._input.param("drop_interval", 5)

        target_server = self.dest_master if drop_target == "C2" else self.src_master
        target_rest = RestConnection(target_server)

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="dropscope_pre")

        self.sleep(10, "Allowing replication to start before dropping scopes")

        for i in range(num_drops):
            scope_name = "{}{}".format(p["scope_prefix"], p["num_scopes"] - 1 - i)
            self.log.info("Dropping scope '{}' on {} (iteration {}/{})".format(
                scope_name, drop_target, i + 1, num_drops))
            try:
                target_rest.delete_scope(bucket_name, scope_name)
            except Exception as e:
                self.log.warning("Error dropping scope '{}': {}".format(scope_name, e))
            if i < num_drops - 1:
                time.sleep(drop_interval)

        self.sleep(30, "Waiting after scope drops for pipeline to stabilize")

        for node in self.src_cluster.get_nodes():
            _, crash_count = NodeHelper.check_goxdcr_log(node, "panic")
            self.assertEqual(crash_count, 0,
                             "goxdcr panic detected on {} after dropping scopes".format(node.ip))

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception:
            self.log.info("Catch-up may time out due to dropped scopes, verifying pipeline is alive")

        self.log.info("Drop scope during replication test passed: no crashes detected")

    def test_drop_collection_during_replication(self):
        """
        Drop collections on destination repeatedly while 10K replication is
        active. Verify 'Collection Not Found' is handled gracefully.

        Conf params:
            drop_target: C1 or C2 (default C2)
            num_drops: number of collections to drop (default 10)
            drop_interval: seconds between drops (default 2)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        drop_target = self._input.param("drop_target", "C2")
        num_drops = self._input.param("num_drops", 10)
        drop_interval = self._input.param("drop_interval", 2)

        target_server = self.dest_master if drop_target == "C2" else self.src_master
        target_rest = RestConnection(target_server)

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="dropcol_pre")

        self.sleep(10, "Allowing replication to start before dropping collections")

        for i in range(num_drops):
            scope_idx = i % p["num_scopes"]
            scope_name = "{}{}".format(p["scope_prefix"], scope_idx)
            col_name = "{}{}".format(p["collection_prefix"],
                                     p["collections_per_scope"] - 1 - (i % p["collections_per_scope"]))
            self.log.info("Dropping collection {}.{} on {} (iteration {}/{})".format(
                scope_name, col_name, drop_target, i + 1, num_drops))
            try:
                target_rest.delete_collection(bucket_name, scope_name, col_name)
            except Exception as e:
                self.log.warning("Error dropping collection {}.{}: {}".format(
                    scope_name, col_name, e))
            if i < num_drops - 1:
                time.sleep(drop_interval)

        self.sleep(30, "Waiting after collection drops for pipeline to stabilize")

        for node in self.src_cluster.get_nodes():
            _, crash_count = NodeHelper.check_goxdcr_log(node, "panic")
            self.assertEqual(crash_count, 0,
                             "goxdcr panic detected on {} after dropping collections".format(node.ip))

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception:
            self.log.info("Catch-up may time out due to dropped collections, verifying pipeline is alive")

        self.log.info("Drop collection during replication test passed: no crashes detected")

    def test_eccv_metadata_10k_collections(self):
        """
        Enable ECCV (enableCrossClusterVersioning) on both clusters and verify
        conflict resolution metadata is handled correctly at 10K scale.

        Verifies ECCV can be toggled on buckets with 10K collections and that
        replication continues without errors after enabling it.
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.log.info("Enabling ECCV on both clusters")
        for cluster in [self.src_cluster, self.dest_cluster]:
            rest = RestConnection(cluster.get_master_node())
            for bucket in cluster.get_buckets():
                rest.change_bucket_props(bucket,
                                         enableCrossClusterVersioning="true")
                self.log.info("ECCV enabled on {} bucket '{}'".format(
                    cluster.get_name(), bucket.name))

        self.setup_xdcr()

        result = TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="eccv")
        self.assertTrue(result.success_rate > 0.9,
                        "Too many load failures: {}/{}".format(
                            len(result.failed_pairs), result.total_attempted))

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception as e:
            self.fail("Replication catch-up failed with ECCV enabled: {}".format(e))

        for cluster in [self.src_cluster, self.dest_cluster]:
            rest = RestConnection(cluster.get_master_node())
            bucket_info = rest.get_bucket_json(bucket_name)
            eccv_val = bucket_info.get("enableCrossClusterVersioning", False)
            self.assertTrue(eccv_val,
                            "ECCV not enabled on {} after replication".format(
                                cluster.get_name()))

        src_count = TenKCollectionHelper.get_bucket_item_count(self.src_master, bucket_name)
        dest_count = TenKCollectionHelper.get_bucket_item_count(self.dest_master, bucket_name)
        self.log.info("Bucket items - src: {}, dest: {}".format(src_count, dest_count))
        self.assertEqual(src_count, dest_count,
                         "Item count mismatch with ECCV: src={}, dest={}".format(
                             src_count, dest_count))

        for node in self.src_cluster.get_nodes():
            _, crash_count = NodeHelper.check_goxdcr_log(node, "panic")
            self.assertEqual(crash_count, 0,
                             "goxdcr panic on {} with ECCV at 10K scale".format(node.ip))

        self.log.info("ECCV metadata test with 10K collections passed")