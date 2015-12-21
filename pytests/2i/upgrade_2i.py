import logging
from threading import Thread

from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition, SQLDefinitionGenerator
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection
from testconstants import STANDARD_BUCKET_PORT

log = logging.getLogger(__name__)


class UpgradeSecondaryIndex(BaseSecondaryIndexingTests, NewUpgradeBaseTest):
    def setUp(self):
        super(UpgradeSecondaryIndex, self).setUp()
        self.use_replica = False
        self.upgrade_to = self.input.param("upgrade_to")
        self.use_replica = self.input.param("use_replica", False)
        self.load_query_definitions = []
        self.initial_index_number = self.input.param("initial_index_number", 10)
        for x in range(1,self.initial_index_number):
            index_name = "index_name_"+str(x)
            query_definition = QueryDefinition(index_name=index_name, index_fields = ["join_mo"], \
                        query_template = "", groups = ["simple"])
            self.load_query_definitions.append(query_definition)
        self.initialize_multi_create_index(buckets = self.buckets,
                query_definitions = self.load_query_definitions)
        self.drop_indexes_in_between = self.input.param("drop_indexes_in_between", False)

    def tearDown(self):
        super(UpgradeSecondaryIndex, self).tearDown()

    def test_offline_upgrade(self):
        """
        Offline Upgrade.
        1) Perform Operations
        2) Stop cb service on all nodes.
        3) Upgrade all nodes.
        4) Start cb service on all nodes.
        5) Perform Operations
        """

        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()  # Upgrade kv node
        self._run_tasks([kv_ops, before_index_ops])
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            self.upgrade_servers.append(server)
        upgrade_threads = self._async_update(self.upgrade_to, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        kv_ops = self._run_kvops_tasks()
        self._run_tasks([kv_ops])
        self._run_after_index_tasks()


    def test_online_kv_upgrade(self):
        """
        1) Perform Operations
        2) Bring down kv node
        4) Upgrade node
        5) Perform Operations
        6) Bring up kv node
        8) Perform Operations

        Configuration: kv, kv, index, n1ql
        """
        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()
        self._run_tasks([kv_ops, before_index_ops])
        kv_node = self.get_nodes_from_services_map(service_type="kv")
        log.info("Upgrading {ip} to {ver}".format(ip=kv_node.ip, ver=self.upgrade_to))
        upgrade_th = self._async_update(self.upgrade_to, [kv_node])
        self.upgrade_servers = [kv_node]
        kv_ops = self._run_kvops_tasks()
        in_between_ops = self._run_in_between_tasks()
        self._run_tasks([kv_ops, in_between_ops])
        for th in upgrade_th:
            th.join()
        node_version = RestConnection(kv_node).get_nodes_versions()
        log.info("Upgraded to: {ver}".format(ver=node_version))
        kv_ops = self._run_kvops_tasks()
        self._run_tasks([kv_ops])
        self._run_after_index_tasks()

    def test_online_kv_upgrade_with_rebalance(self):
        """
        1) Perform Operations
        2) Bring down kv node
        3) Rebalance Cluster
        4) Upgrade node
        5) Perform Operations
        6) Bring up kv node
        7) Rebalance
        8) Perform Operations

        Configuration: kv, kv, index, n1ql
        """
        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()
        self._run_tasks([kv_ops, before_index_ops])
        # Upgrade kv node
        kv_node = self.get_nodes_from_services_map(service_type="kv")
        # Rebalance Node Out
        log.info("Rebalancing KV node {ip} out...".format(ip=kv_node.ip))
        rebalance = self.cluster.async_rebalance(self.servers, [], [kv_node])
        kv_ops = self._run_kvops_tasks()
        in_between_ops = self._run_in_between_tasks()
        self._run_tasks([kv_ops, in_between_ops])
        rebalance.result()
        log.info("Upgrading {ip} to {ver}".format(ip=kv_node.ip, ver=self.upgrade_to))
        upgrade_th = self._async_update(self.upgrade_to, [kv_node])
        self.upgrade_servers = [kv_node]
        log.info("IO During Upgrade...")
        for th in upgrade_th:
            th.join()
        node_version = RestConnection(kv_node).get_nodes_versions()
        log.info("Upgraded to: {ver}".format(ver=node_version))
        # Rebalance Node in
        log.info("Rebalancing KV node {ip} in...".format(ip=kv_node.ip))
        self.servers.remove(kv_node)
        rebalance = self.cluster.async_rebalance(self.servers, [kv_node], [])
        rebalance.result()
        self.servers.append(kv_node)
        log.info("Post Upgrade IO...")
        kv_ops = self._run_kvops_tasks()
        self._run_tasks([kv_ops])
        self._run_after_index_tasks()

    def test_online_index_upgrade(self):
        """
        1) Perform Operations
        2) Bring down index node
        4) Upgrade node
        5) Perform Operations
        6) Bring up index node
        8) Perform Operations

        Configuration: kv, kv, index, n1ql
        """
        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()
        self._run_tasks([kv_ops, before_index_ops])
        index_node = self.get_nodes_from_services_map(service_type="index")
        log.info("Upgrading {ip} to {ver}".format(ip=index_node.ip, ver=self.upgrade_to))
        upgrade_th = self._async_update(self.upgrade_to, [index_node])
        self.upgrade_servers = [index_node]
        kv_ops = self._run_kvops_tasks()
        if self.use_replica:
            in_between_ops = self._run_in_between_tasks()
        self._run_tasks([kv_ops])
        for th in upgrade_th:
            th.join()
        node_version = RestConnection(index_node).get_nodes_versions()
        log.info("Upgraded to: {ver}".format(ver=node_version))
        kv_ops = self._run_kvops_tasks()
        self._run_tasks([kv_ops])
        self._run_after_index_tasks()


    def test_online_n1ql_upgrade(self):
        """
        1) Perform Operations
        2) Bring down n1ql node
        4) Upgrade node
        5) Perform Operations
        6) Bring up n1ql node
        8) Perform Operations

        Configuration: kv, kv, index, n1ql
        """
        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()
        self._run_tasks([kv_ops, before_index_ops])
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        log.info("Upgrading {ip} to {ver}".format(ip=n1ql_node.ip, ver=self.upgrade_to))
        upgrade_th = self._async_update(self.upgrade_to, [n1ql_node])
        self.upgrade_servers = [n1ql_node]
        kv_ops = self._run_kvops_tasks()
        self._run_tasks([kv_ops])
        for th in upgrade_th:
            th.join()
        node_version = RestConnection(n1ql_node).get_nodes_versions()
        log.info("Upgraded to: {ver}".format(ver=node_version))
        kv_ops = self._run_kvops_tasks()
        self._run_tasks([kv_ops])
        self._run_after_index_tasks()

    def test_online_kv_n1ql_upgrade(self):
        """
        1) Perform Operations
        2) Bring down n1ql node
        4) Upgrade node
        5) Perform Operations
        6) Bring up n1ql node
        8) Perform Operations

        Configuration: kv:n1ql, kv, index, index
        """
        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()
        self._run_tasks([kv_ops, before_index_ops])
        kv_n1ql_nodes = []
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if kv_nodes and n1ql_nodes:
            kv_n1ql_nodes = [x for x in kv_nodes if x in n1ql_nodes]
        if kv_n1ql_nodes:
            kv_n1ql_node = kv_n1ql_nodes[0]
            log.info("Upgrading {ip} to {ver}".format(ip=kv_n1ql_node.ip, ver=self.upgrade_to))
            upgrade_th = self._async_update(self.upgrade_to, [kv_n1ql_node])
            self.upgrade_servers = [kv_n1ql_node]
            kv_ops = self._run_kvops_tasks()
            self._run_tasks([kv_ops])
            for th in upgrade_th:
                th.join()
            node_version = RestConnection(kv_n1ql_node).get_nodes_versions()
            log.info("Upgraded to: {ver}".format(ver=node_version))
            kv_ops = self._run_kvops_tasks()
            self._run_tasks([kv_ops])
            self._run_after_index_tasks()
        else:
            log.info("Found no node with kv and n1ql service")

    def test_online_kv_index_upgrade(self):
        """
        1) Perform Operations
        2) Bring down n1ql node
        4) Upgrade node
        5) Perform Operations
        6) Bring up n1ql node
        8) Perform Operations

        Configuration: kv, kv-index, index, n1ql
        """
        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()
        self._run_tasks([kv_ops, before_index_ops])
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        kv_index_nodes = []
        if kv_nodes and index_nodes:
            kv_index_nodes = [x for x in kv_nodes if x in index_nodes]
        if kv_index_nodes:
            kv_index_node = kv_index_nodes[0]
            log.info("Upgrading {ip} to {ver}".format(ip=kv_index_node.ip, ver=self.upgrade_to))
            upgrade_th = self._async_update(self.upgrade_to, [kv_index_node])
            self.upgrade_servers = [kv_index_node]
            kv_ops = self._run_kvops_tasks()
            self._run_tasks([kv_ops])
            for th in upgrade_th:
                th.join()
            node_version = RestConnection(kv_index_node).get_nodes_versions()
            log.info("Upgraded to: {ver}".format(ver=node_version))
            kv_ops = self._run_kvops_tasks()
            self._run_tasks([kv_ops])
            self._run_after_index_tasks()
        else:
            log.info("Found no node with kv and index service")

    def test_online_n1ql_index_upgrade(self):
        """
        1) Perform Operations
        2) Bring down n1ql node
        4) Upgrade node
        5) Perform Operations
        6) Bring up n1ql node
        8) Perform Operations

        Configuration: kv, kv, index-n1ql, index
        """
        # Perform pre_upgrade operations on cluster
        self._run_initial_index_tasks()
        kv_ops = self._run_kvops_tasks()
        before_index_ops = self._run_before_index_tasks()
        self._run_tasks([kv_ops, before_index_ops])
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        n1ql_index_nodes = []
        if n1ql_nodes and index_nodes:
            n1ql_index_nodes = [x for x in n1ql_nodes if x in index_nodes]
        if n1ql_index_nodes:
            n1ql_index_node = n1ql_index_node[]
            log.info("Upgrading {ip} to {ver}".format(ip=n1ql_index_node.ip, ver=self.upgrade_to))
            upgrade_th = self._async_update(self.upgrade_to, [n1ql_index_node])
            self.upgrade_servers = [n1ql_index_node]
            kv_ops = self._run_kvops_tasks()
            self._run_tasks([kv_ops])
            for th in upgrade_th:
                th.join()
            node_version = RestConnection(n1ql_index_node).get_nodes_versions()
            log.info("Upgraded to: {ver}".format(ver=node_version))
            kv_ops = self._run_kvops_tasks()
            self._run_tasks([kv_ops])
            self._run_after_index_tasks()
        else:
            log.info("Found no node with n1ql and index service")

    def _run_initial_index_tasks(self):
        self.log.info("<<<<< START INITIALIZATION PHASE >>>>>>")
        self._calculate_scan_vector()
        tasks = self.async_check_and_run_operations(buckets=self.buckets, initial=True,
                                                    scan_consistency=self.scan_consistency, scan_vectors=self.scan_vectors)
        self._run_tasks([tasks])
        self.log.info("<<<<< END INITIALIZATION PHASE >>>>>>")


    def _run_before_index_tasks(self):
        if self.ops_map["before"]["create_index"]:
            self.index_nodes_out = []
        self._redefine_index_usage()
        tasks = self.async_check_and_run_operations(buckets=self.buckets, before=True,
                                                    scan_consistency=self.scan_consistency, scan_vectors=self.scan_vectors)
        return tasks


    def _run_in_between_tasks(self):
        tasks_ops = []
        if self.ops_map["in_between"]["create_index"]:
            self.index_nodes_out = []
        self._redefine_index_usage()
        tasks = self.async_check_and_run_operations(buckets=self.buckets, in_between=True,
                                                    scan_consistency=self.scan_consistency, scan_vectors=self.scan_vectors)
        tasks += self._drop_indexes_in_between()
        return tasks


    def _drop_indexes_in_between(self):
        drop_tasks = []
        if self.drop_indexes_in_between:
            drop_tasks = self.async_multi_drop_index(buckets=self.buckets,
                                                     query_definitions=self.load_query_definitions)
        return drop_tasks


    def _run_kvops_tasks(self):
        tasks_ops = []
        if self.doc_ops:
            tasks_ops = self.async_run_doc_ops()
        return tasks_ops


    def _run_after_index_tasks(self):
        tasks = self.async_check_and_run_operations(buckets=self.buckets, after=True,
                                                    scan_consistency=self.scan_consistency, scan_vectors=self.scan_vectors)
        self._run_tasks([tasks])

    def _calculate_scan_vector(self):
        self.scan_vectors = None
        if self.scan_vectors != None:
            self.scan_vectors = self.gen_scan_vector(use_percentage=self.scan_vector_per_values,
                                                     use_random=self.random_scan_vector)
    def _run_tasks(self, tasks_list):
        for tasks in tasks_list:
            for task in tasks:
                task.result()

    def _redefine_index_usage(self):
        qdfs = []
        self.index_lost_during_move_out = []
        if not self.use_replica :
            return
        if self.use_replica_when_active_down and \
            (self.ops_map["before"]["query_ops"] or self.ops_map["in_between"]["query_ops"]):
            #and not self.all_index_nodes_lost:
            for query_definition in self.query_definitions:
                if query_definition.index_name in self.index_lost_during_move_out:
                    query_definition.index_name = query_definition.index_name+"_replica"
                qdfs.append(query_definition)
            self.query_definitions = qdfs
        elif self.ops_map["before"]["query_ops"] \
         or self.ops_map["in_between"]["query_ops"]:
         #or self.all_index_nodes_lost:
            for query_definition in self.query_definitions:
                if query_definition.index_name in self.index_lost_during_move_out:
                    query_definition.index_name = "#primary"
                qdfs.append(query_definition)
            self.query_definitions = qdfs
