import itertools
import logging
import threading

from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests


log = logging.getLogger(__name__)

AGGREGATE_FUNCTIONS = ["SUM", "MIN", "MAX", "COUNT", "COUNTN", "AVG"]
DISTINCT_AGGREGATE_FUNCTIONS = ["SUM", "COUNT", "AVG"]

class AggregatePushdownRecoveryClass(QueryTests):
    def setUp(self):
        super(AggregatePushdownRecoveryClass, self).setUp()
        self.n1ql_helper = N1QLHelper(master=self.master)
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.aggr_distinct = self.input.param("aggr_distinct", False)
        self.graceful = self.input.param("graceful", False)

    def tearDown(self):
        super(AggregatePushdownRecoveryClass, self).tearDown()

    def test_indexer_rebalance_in(self):
        self.find_nodes_in_list()
        index_names_defn = self._create_array_index_definitions()
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list,
                                                     [], services=self.services_in)
            mid_recovery_tasks = threading.Thread(target=self._aggregate_query_using_index, args=(index_names_defn,))
            mid_recovery_tasks.start()
            rebalance.result()
            mid_recovery_tasks.join()
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self._wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self.sleep(60)
            self._aggregate_query_using_index(index_names_defn)
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_indexer_rebalance_out(self):
        self.generate_map_nodes_out_dist()
        index_names_defn = self._create_array_index_definitions()
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], self.nodes_out_list)
            mid_recovery_tasks = threading.Thread(target=self._aggregate_query_using_index, args=(index_names_defn,))
            mid_recovery_tasks.start()
            rebalance.result()
            mid_recovery_tasks.join()
            # Check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self._wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self.sleep(60)
            self._aggregate_query_using_index(index_names_defn)
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_indexer_rebalance_in_out(self):
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()
        index_names_defn = self._create_array_index_definitions()
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list,
                                                     self.nodes_out_list, services=self.services_in)
            mid_recovery_tasks = threading.Thread(target=self._aggregate_query_using_index, args=(index_names_defn,))
            mid_recovery_tasks.start()
            rebalance.result()
            mid_recovery_tasks.join()
            # Check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self._wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self.sleep(60)
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_server_crash(self):
        self.generate_map_nodes_out_dist()
        index_names_defn = self._create_array_index_definitions()
        try:
            self.targetProcess= self.input.param("targetProcess", 'memcached')
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                if self.targetProcess == "memcached":
                    remote.kill_memcached()
                else:
                    remote.terminate_process(process_name=self.targetProcess)
            self.sleep(60)
            if "n1ql" not in self.nodes_out_dist:
                mid_recovery_tasks = threading.Thread(target=self._aggregate_query_using_index, args=(index_names_defn,))
                mid_recovery_tasks.start()
                mid_recovery_tasks.join()
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self._wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self.sleep(60)
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_indexer_failover_add_back(self):
        rest = RestConnection(self.master)
        self.generate_map_nodes_out_dist()
        index_names_defn = self._create_array_index_definitions()
        try:
            failover_task =self.cluster.async_failover([self.master],
                                                       failover_nodes=self.nodes_out_list,
                                                       graceful=self.graceful)
            failover_task.result()
            nodes_all = rest.node_statuses()
            nodes = []
            if self.nodes_out_list:
                if self.nodes_out_list[0].ip == "127.0.0.1":
                    for failover_node in self.nodes_out_list:
                        nodes.extend([node for node in nodes_all if (str(node.port) == failover_node.port)])
                else:
                    for failover_node in self.nodes_out_list:
                        nodes.extend([node for node in nodes_all
                            if node.ip == failover_node.ip])
                    for node in nodes:
                        log.info("Adding back {0} with recovery type Full...".format(node.ip))
                        rest.add_back_node(node.id)
                        rest.set_recovery_type(otpNode=node.id, recoveryType="full")
            log.info("Rebalancing nodes in...")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
            mid_recovery_tasks = threading.Thread(target=self._aggregate_query_using_index, args=(index_names_defn,))
            mid_recovery_tasks.start()
            rebalance.result()
            mid_recovery_tasks.join()
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self._wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self.sleep(60)
        except Exception as ex:
            log.info(str(ex))
            raise

    def _create_array_index_definitions(self):
        index_fields = [{"name": "name", "where_clause": "name = 'Kala'"},
                        {"name": "age", "where_clause": "age < 85"},
                        {"name": "debt", "where_clause": "debt > -500000"}]
        indexes = []
        for first_field in index_fields:
            for second_field in index_fields:
                if first_field == second_field:
                    continue
                for third_field in index_fields:
                    if second_field == third_field or first_field == third_field:
                        continue
                    index_names_defn = {}
                    index_name = "{0}_{1}_{2}".format(first_field["name"], second_field["name"], third_field["name"])
                    index_names_defn["index_name"] = index_name
                    index_names_defn["fields"] = [first_field, second_field, third_field]
                    create_index_clause = "CREATE INDEX {0} on %s({1}, {2}, {3})".format(
                        index_name, first_field["name"], second_field["name"], third_field["name"])
                    drop_index_clause = "DROP INDEX %s.{0}".format(index_name)
                    index_names_defn["create_definitions"] = [(create_index_clause % bucket.name) for bucket in self.buckets]
                    index_names_defn["drop_definitions"] = [(drop_index_clause % bucket.name) for bucket in self.buckets]
                    for create_def in index_names_defn["create_definitions"]:
                        result = self.run_cbq_query(create_def)
                    indexes.append(index_names_defn)
        return indexes

    def _aggregate_query_using_index(self, index_names_defn):
        failed_queries_in_result = []
        for index_name_def in index_names_defn:
            query_definitions = []
            index_fields = index_name_def["fields"]
            for aggr_func in AGGREGATE_FUNCTIONS:
                select_clause = "SELECT " + aggr_func + "({0}) from %s where {1} GROUP BY {2}"
                query_definitions = [select_clause.format(tup[0]["name"], tup[1]["where_clause"], index_fields[0]["name"])
                                     for tup in itertools.permutations(index_fields)]
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    query = query_definition % bucket.name
                    result = self.run_cbq_query(query)
                    query_verification = self._verify_aggregate_query_results(result, query_definition,
                                                                              bucket.name)
                    if not query_verification:
                        failed_queries_in_result.append(query)
            self.assertEqual(len(failed_queries_in_result), 0, "Failed Queries: {0}".format(failed_queries_in_result))

    def _wait_until_cluster_is_healthy(self):
        master_node = self.master
        if self.targetMaster:
            if len(self.servers) > 1:
                master_node = self.servers[1]
        rest = RestConnection(master_node)
        is_cluster_healthy = False
        count = 0
        while not is_cluster_healthy and count < 10:
            count += 1
            cluster_nodes = rest.node_statuses()
            for node in cluster_nodes:
                if node.status != "healthy":
                    is_cluster_healthy = False
                    log.info("Node {0} is in {1} state...".format(node.ip,
                                                                  node.status))
                    self.sleep(5)
                    break
                else:
                    is_cluster_healthy = True
        return is_cluster_healthy

    def _verify_aggregate_query_results(self, result, query, bucket):
        def _gen_dict(res):
            result_set = []
            if res is not None and len(res) > 0:
                for val in res:
                    for key in list(val.keys()):
                        result_set.append(val[key])
            return result_set

        self.restServer = self.get_nodes_from_services_map(service_type="n1ql")
        self.rest = RestConnection(self.restServer)
        self.rest.set_query_index_api_mode(1)
        query = query % bucket
        primary_result = self.run_cbq_query(query)
        self.rest.set_query_index_api_mode(3)
        self.log.info(" Analyzing Actual Result")

        actual_result = _gen_dict(sorted(primary_result["results"]))
        self.log.info(" Analyzing Expected Result")
        expected_result = _gen_dict(sorted(result["results"]))
        if len(actual_result) != len(expected_result):
            return False
        if actual_result != expected_result:
            return False
        return True