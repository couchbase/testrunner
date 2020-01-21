from .tuq import QueryTests
from membase.api.exception import CBQError
from lib.membase.api.rest_client import RestConnection
from pytests.fts.fts_base import CouchbaseCluster
from remote.remote_util import RemoteMachineShellConnection
import json
from pytests.security.rbac_base import RbacBase
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.tuqquery.n1ql_fts_integration_phase2 import N1qlFTSIntegrationPhase2Test
import threading

class N1qlFTSIntegrationPhase2ClusteropsTest(QueryTests):

    def suite_setUp(self):
        super(N1qlFTSIntegrationPhase2ClusteropsTest, self).suite_setUp()


    def setUp(self):
        super(N1qlFTSIntegrationPhase2ClusteropsTest, self).setUp()

        self.log.info("==============  N1qlFTSIntegrationPhase2ClusteropsTest setup has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2ClusteropsTest setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  N1qlFTSIntegrationPhase2ClusteropsTest tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2ClusteropsTest tearDown has completed ==============")
        super(N1qlFTSIntegrationPhase2ClusteropsTest, self).tearDown()


    def suite_tearDown(self):
        self.log.info("==============  N1qlFTSIntegrationPhase2ClusteropsTest suite_tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2ClusteropsTest suite_tearDown has completed ==============")
        super(N1qlFTSIntegrationPhase2ClusteropsTest, self).suite_tearDown()


    def get_rest_client(self, user, password):
        rest = RestConnection(self.cbcluster.get_random_fts_node())
        rest.username = user
        rest.password = password
        return rest

    def test_cluster_config_stable(self):
        self.load_test_buckets()
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)

        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        n1ql_node = self.find_child_node_with_service("n1ql")
        if n1ql_node is None:
            self.log("Cannot find n1ql child node!")
        fts_node = self.find_child_node_with_service("fts")
        if fts_node is None:
            self.log("Cannot find fts child node!")

        n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"query\":{\"field\":\"state\", \"match\":\"California\"}, \"size\":1000})"
        fts_request = {"query":{"field":"state", "match":"California"}, "size":1000}
        n1ql_results = self.run_cbq_query(n1ql_query, server=n1ql_node)['results']
        n1ql_doc_ids = []
        for result in n1ql_results:
            n1ql_doc_ids.append(result['id'])

        total_hits, hits, took, status = \
            rest.run_fts_query(index_name="idx_beer_sample_fts",
                               query_json = fts_request)

        fts_doc_ids = []
        for hit in hits:
            fts_doc_ids.append(hit['id'])

        self.assertEqual(len(n1ql_doc_ids), len(fts_doc_ids),
                          "Results count does not match for test . FTS - " + str(
                              len(fts_doc_ids)) + ", N1QL - " + str(len(n1ql_doc_ids)))
        self.assertEqual(sorted(fts_doc_ids), sorted(n1ql_doc_ids),
                          "Found mismatch in results for test .")

        self.remove_all_fts_indexes()

    def test_cluster_replicas_failover_rebalance(self):
        self.load_test_buckets()
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_idx = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        number_of_replicas = self.input.param("num_replicas", 0)
        self._update_replica_for_fts_index(fts_idx, number_of_replicas)
        self.sleep(60)
        n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"query\":{\"field\":\"state\", \"match\":\"California\"}, \"size\":10000})"
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        n1ql_doc_ids_before_failover = []
        for result in n1ql_results:
            n1ql_doc_ids_before_failover.append(result['id'])

        self.cluster.failover(servers=self.servers, failover_nodes=[self.servers[2]], graceful=False)
        rebalance = self.cluster.rebalance(self.servers, [], [self.servers[2]])
        self.assertEqual(rebalance, True, "Rebalance is failed.")
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        n1ql_doc_ids_after_rebalance = []
        for result in n1ql_results:
            n1ql_doc_ids_after_rebalance.append(result['id'])

        self.assertEqual(sorted(n1ql_doc_ids_before_failover), sorted(n1ql_doc_ids_after_rebalance), "Results after rebalance does not match.")

    def test_fts_node_failover_partial_results(self):
        self.load_test_buckets()
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_idx = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"query\":{\"field\":\"state\", \"match\":\"California\"}, \"size\":10000})"
        n1ql_results_before_failover = self.run_cbq_query(n1ql_query)['results']
        n1ql_doc_ids_before_failover = []
        for result in n1ql_results_before_failover:
            n1ql_doc_ids_before_failover.append(result['id'])

        self.cluster.failover(servers=self.servers, failover_nodes=[self.servers[2]], graceful=False)
        error_found = False
        try:
            n1ql_result_after_failover = self.run_cbq_query(n1ql_query)
        except CBQError as err:
            self.assertTrue("pindex not available" in str(err), "Partial results error message is not graceful.")
            error_found = True
        self.assertEqual(error_found, True, "Partial result set is not allowed for SEARCH() queries.")


    def test_cluster_add_new_fts_node(self):
        self.load_test_buckets()
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_idx = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        number_of_replicas = self.input.param("num_replicas", 0)
        self._update_replica_for_fts_index(fts_idx, number_of_replicas)
        self.sleep(60)
        n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"query\":{\"field\":\"state\", \"match\":\"California\"}, \"size\":10000})"
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        n1ql_doc_ids_before_rebalance = []
        for result in n1ql_results:
            n1ql_doc_ids_before_rebalance.append(result['id'])

        self.cluster.rebalance(self.servers, [self.servers[4]], [], services=["fts"])

        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        n1ql_doc_ids_after_rebalance = []
        for result in n1ql_results:
            n1ql_doc_ids_after_rebalance.append(result['id'])

        self.assertEqual(sorted(n1ql_doc_ids_before_rebalance), sorted(n1ql_doc_ids_after_rebalance), "Results after rebalance does not match.")

    def test_partitioning(self):
        partitions_number = self.input.param("partitions_num")
        self.load_test_buckets()
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_idx = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"query\":{\"field\":\"state\", \"match\":\"California\"}, \"size\":10000})"
        default_results = self.run_cbq_query(n1ql_query)
        self._update_partiotions_for_fts_index(fts_idx, partitions_number)
        self.sleep(60)
        new_partitioning_result = self.run_cbq_query(n1ql_query)

        n1ql_doc_ids_before_partitioning = []
        for result in default_results['results']:
            n1ql_doc_ids_before_partitioning.append(result['id'])

        n1ql_doc_ids_after_partitioning = []
        for result in new_partitioning_result['results']:
            n1ql_doc_ids_after_partitioning.append(result['id'])

        self.assertEqual(sorted(n1ql_doc_ids_before_partitioning), sorted(n1ql_doc_ids_after_partitioning), "Results after partitioning do not match.")


    def _update_replica_for_fts_index(self, idx, replicas):
        idx.update_num_replicas(replicas)

    def _update_partiotions_for_fts_index(self, idx, partitions_num):
        idx.update_num_pindexes(partitions_num)

    def remove_all_fts_indexes(self):
        indexes = self.cbcluster.get_indexes()
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        for index in indexes:
            rest.delete_fts_index(index.name)

    def find_child_node_with_service(self, service=""):
        services_map = self._get_services_map()
        for node in list(services_map.keys()):
            if node == (str(self.servers[0].ip)+":"+str(self.servers[0].port)):
                continue
            if service in services_map[node]:
                for server in self.servers:
                    if (str(server.ip)+":"+str(server.port)) == node:
                        return server
        return None

    def load_test_buckets(self):
        self.rest.load_sample("beer-sample")
        self.wait_for_buckets_status({"beer-sample": "healthy"}, 5, 120)
        self.wait_for_bucket_docs({"beer-sample": 7303}, 5, 120)

    def _create_fts_index(self, index_name='', doc_count=0, source_name=''):
        fts_index = self.cbcluster.create_fts_index(name=index_name, source_name=source_name)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        indexed_doc_count = fts_index.get_indexed_doc_count(rest)
        #indexed_doc_count = rest.get_fts_stats(index_name, source_name, "doc_count")
        while indexed_doc_count < doc_count:
            try:
                indexed_doc_count = fts_index.get_indexed_doc_count(rest)
                #indexed_doc_count = rest.get_fts_stats(index_name, source_name, "doc_count")
            except KeyError as k:
                continue

        return fts_index

    def _get_services_map(self):
        rest = RestConnection(self.servers[0])
        return rest.get_nodes_services()

