import unittest
import uuid
import logger

from membase.helper.spatial_helper import SpatialHelper
from membase.helper.cluster_helper import ClusterOperationHelper


class SpatialRebalanceTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SpatialHelper(self, "default")
        # Setup, but don't rebalance cluster
        self.helper.setup_cluster(False)


    def tearDown(self):
        self.log.info("tear down test")
        self.helper.cleanup_cluster()


    def test_insert_x_delete_y_docs_create_cluster(self):
        num_docs = self.helper.input.param("num-docs", 100000)
        num_deleted_docs = self.helper.input.param("num-deleted-docs", 10000)
        msg = "description : have a single node, insert {0} docs, "\
            "delete {1} docs while creating a cluster and query it"
        self.log.info(msg.format(num_docs, num_deleted_docs))
        design_name = "dev_test_delete_10k_docs_create_cluster"
        prefix = str(uuid.uuid4())[:7]

        # Make sure we are fully de-clustered
        ClusterOperationHelper.cleanup_cluster(self.helper.servers)

        self.helper.create_index_fun(design_name, prefix)
        inserted_keys = self.helper.insert_docs(num_docs, prefix)

        # Start creating the cluster and rebalancing it without waiting until
        # it's finished
        ClusterOperationHelper.add_and_rebalance(self.helper.servers, False)

        deleted_keys = self.helper.delete_docs(num_deleted_docs, prefix)
        self._wait_for_rebalance()

        # Verify that the docs got delete and are no longer part of the
        # spatial view
        results = self.helper.get_results(design_name, num_docs)
        result_keys = self.helper.get_keys(results)
        self.assertEqual(len(result_keys), num_docs - len(deleted_keys))
        self.helper.verify_result(inserted_keys, deleted_keys + result_keys)


    def test_insert_x_delete_y_docs_destroy_cluster(self):
        num_docs = self.helper.input.param("num-docs", 100000)
        num_deleted_docs = self.helper.input.param("num-deleted-docs", 10000)
        msg = "description : have a cluster, insert {0} docs, delete "\
            "{1} docs while destroying the cluster into a single node "\
            "and query it"
        self.log.info(msg.format(num_docs, num_deleted_docs))
        design_name = "dev_test_delete_{0}_docs_destroy_cluster".format(
            num_deleted_docs)
        prefix = str(uuid.uuid4())[:7]

        # Make sure we are fully clustered
        ClusterOperationHelper.add_and_rebalance(self.helper.servers)

        self.helper.create_index_fun(design_name, prefix)
        inserted_keys = self.helper.insert_docs(num_docs, prefix)

        # Start destroying the cluster and rebalancing it without waiting
        # until it's finished
        ClusterOperationHelper.cleanup_cluster(self.helper.servers,
                                                    False)

        deleted_keys = self.helper.delete_docs(num_deleted_docs, prefix)
        self._wait_for_rebalance()

        # Verify that the docs got delete and are no longer part of the
        # spatial view
        results = self.helper.get_results(design_name, num_docs)
        result_keys = self.helper.get_keys(results)
        self.assertEqual(len(result_keys), num_docs - len(deleted_keys))
        self.helper.verify_result(inserted_keys, deleted_keys + result_keys)


    def test_insert_x_docs_during_rebalance(self):
        num_docs = self.helper.input.param("num-docs", 100000)
        msg = "description : have a single node, insert {0} docs, "\
            "query it, add another node, start rebalancing, insert {0} "\
            "docs, finish rebalancing, keep on adding nodes..."
        self.log.info(msg.format(num_docs))
        design_name = "dev_test_insert_{0}_docs_during_rebalance".format(
            num_docs)
        prefix = str(uuid.uuid4())[:7]

        # Make sure we are fully de-clustered
        ClusterOperationHelper.cleanup_cluster(self.helper.servers)

        self.helper.create_index_fun(design_name)
        inserted_keys = self.helper.insert_docs(num_docs, prefix)

        # Add all servers to the master server one by one and start
        # rebalacing
        for server in self.helper.servers[1:]:
            ClusterOperationHelper.add_and_rebalance(
                [self.helper.master, server], False)
            # Docs with the same prefix are overwritten and not newly created
            prefix = str(uuid.uuid4())[:7]
            inserted_keys.extend(self.helper.insert_docs(
                    num_docs, prefix, wait_for_persistence=False))
            self._wait_for_rebalance()

        # Make sure data is persisted
        self.helper.wait_for_persistence()

        # Verify that all documents got inserted
        self.helper.query_index_for_verification(design_name, inserted_keys)


    # Block until the rebalance is done
    def _wait_for_rebalance(self):
        self.assertTrue(self.helper.rest.monitorRebalance(),
                        "rebalance operation failed after adding nodes")
        self.log.info("rebalance finished")
