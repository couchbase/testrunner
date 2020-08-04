from .xdcrnewbasetests import XDCRNewBaseTest
from membase.api.rest_client import RestConnection
import time


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
        colMappingRules = self._input.param("colMappingRules", None)
        collectionsExplicitMapping = self._input.param("collectionsExplicitMapping", None)
        collectionsMigrationMode = self._input.param("collectionsMigrationMode", None)
        collectionsMirroringMode = self._input.param("collectionsMirroringMode", None)
        new_scope = self._input.param("new_scope", None)
        new_collection = self._input.param("new_collection", None)
        new_scope_collection = self._input.param("new_scope_collection", None)
        drop_recreate_scope = self._input.param("drop_recreate_scope", None)
        drop_recreate_collection = self._input.param("drop_recreate_collection", None)

        initial_xdcr = self._input.param("initial_xdcr", False)
        if initial_xdcr:
            self.load_and_setup_xdcr()
        else:
            self.setup_xdcr_and_load()

        if drop_default_scope:
            for cluster in self.get_cluster_objects_for_input(drop_default_scope):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_delete_scope(cluster.get_master_node(),
                                                                          bucket, self.DEFAULT_SCOPE))

        if drop_default_collection:
            for cluster in self.get_cluster_objects_for_input(drop_default_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    print("Deleting default collection in {}:{}".format(cluster, bucket))
                    tasks.append(cluster.get_cluster().async_delete_collection(cluster.get_master_node(),
                                                                               bucket, self.DEFAULT_SCOPE,
                                                                               self.DEFAULT_COLLECTION))

        if new_scope:
            for cluster in self.get_cluster_objects_for_input(new_scope):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope(cluster.get_master_node(),
                                                                          bucket, self.NEW_SCOPE,
                                                                          ))

        if new_collection:
            for cluster in self.get_cluster_objects_for_input(new_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_collection(cluster.get_master_node(),
                                                                               bucket, self.DEFAULT_SCOPE,
                                                                               self.NEW_COLLECTION
                                                                               ))
        if new_scope_collection:
            for cluster in self.get_cluster_objects_for_input(new_scope_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope_collection(cluster.get_master_node(),
                                                                                     bucket, self.NEW_SCOPE,
                                                                                     self.NEW_COLLECTION
                                                                                     ))
        if drop_recreate_scope:
            for cluster in self.get_cluster_objects_for_input(drop_recreate_scope):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope(cluster.get_master_node(),
                                                                          bucket, self.NEW_SCOPE,
                                                                          ))
                    tasks.append(cluster.get_cluster().async_delete_scope(cluster.get_master_node(),
                                                                          bucket, self.NEW_SCOPE))
                    tasks.append(cluster.get_cluster().async_create_scope(cluster.get_master_node(),
                                                                          bucket, self.NEW_SCOPE,
                                                                          ))

        if drop_recreate_collection:
            for cluster in self.get_cluster_objects_for_input(drop_recreate_collection):
                for bucket in RestConnection(cluster.get_master_node()).get_buckets():
                    tasks.append(cluster.get_cluster().async_create_scope_collection(cluster.get_master_node(),
                                                                                     bucket, self.NEW_SCOPE,
                                                                                     self.NEW_COLLECTION
                                                                                     ))
                    tasks.append(cluster.get_cluster().async_delete_collection(cluster.get_master_node(),
                                                                               bucket, self.NEW_SCOPE,
                                                                               self.NEW_COLLECTION))
                    tasks.append(cluster.get_cluster().async_create_collection(cluster.get_master_node(),
                                                                               bucket, self.NEW_SCOPE,
                                                                               self.NEW_COLLECTION
                                                                               ))
        for task in tasks:
            task.result()
        self.perform_update_delete()
        self.verify_results()
