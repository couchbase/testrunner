from .xdcrnewbasetests import XDCRNewBaseTest
from membase.api.rest_client import RestConnection
import random


class XDCRCollectionsTests(XDCRNewBaseTest):
    DEFAULT_SCOPE = "_default"
    DEFAULT_COLLECTION = "_default"
    NEW_SCOPE = "new_scope"
    NEW_COLLECTION = "new_collection"

    def setUp(self):
        XDCRNewBaseTest.setUp(self)

    def tearDown(self):
        XDCRNewBaseTest.tearDown(self)

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
        migration_mode = self._input.param("migration_mode", None)
        mirroring_mode = self._input.param("mirroring_mode", None)
        new_scope = self._input.param("new_scope", None)
        new_collection = self._input.param("new_collection", None)
        scope_name = self._input.param("scope_name", self.NEW_SCOPE)
        collection_name = self._input.param("collection_name", self.NEW_COLLECTION)
        new_scope_collection = self._input.param("new_scope_collection", None)
        drop_recreate_scope = self._input.param("drop_recreate_scope", None)
        drop_recreate_collection = self._input.param("drop_recreate_collection", None)
        initial_xdcr = self._input.param("initial_xdcr", random.choice([True, False]))

        try:
            if initial_xdcr:
                self.load_and_setup_xdcr()
            else:
                self.setup_xdcr_and_load()
        except Exception as e:
            self.fail(str(e))

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
        if explicit_mapping:
            for cluster in self.get_cluster_objects_for_input(explicit_mapping):
                if True in cluster.get_xdcr_param("collectionsExplicitMapping"):
                    self.fail("collectionsExplicitMapping is true, expected to be false by default")
                self.log.info("collectionsExplicitMapping is false as expected")

        if mapping_rules:
            for cluster in self.get_cluster_objects_for_input(mapping_rules):
                rules = cluster.get_xdcr_param("colMappingRules")
                for rule in rules:
                    if rule:
                        self.fail("colMappingRules is expected to be empty by default but it is {0}".format(rules))
                self.log.info("colMappingRules is empty as expected")

        if migration_mode:
            for cluster in self.get_cluster_objects_for_input(migration_mode):
                if True in cluster.get_xdcr_param("collectionsMigrationMode"):
                    self.fail("collectionsMigrationMode is true, expected to be false by default")
                self.log.info("collectionsMigrationMode is false as expected")

        if mirroring_mode:
            for cluster in self.get_cluster_objects_for_input(mirroring_mode):
                if True in cluster.get_xdcr_param("collectionsMirroringMode"):
                    self.fail("collectionsMirroringMode is true, expected to be false by default")
                self.log.info("collectionsMirroringMode is false as expected")

        for task in tasks:
            task.result()
        self.perform_update_delete()
        self.verify_results()
