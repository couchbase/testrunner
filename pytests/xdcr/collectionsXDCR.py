from .xdcrnewbasetests import XDCRNewBaseTest
from membase.api.rest_client import RestConnection
import random


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
                if True in cluster.get_xdcr_param("collectionsOSOMode"):
                    self.log.info("collectionsOSOMode is true as expected")
                else:
                    self.fail("collectionsOSOMode is false, expected to be true by default")
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

            self.log.info("Migration mode with xattrs test completed successfully")

        except Exception as e:
            self.fail(f"Migration mode with xattrs test failed: {str(e)}")