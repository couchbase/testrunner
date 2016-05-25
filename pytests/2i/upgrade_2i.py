import logging

from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition, SQLDefinitionGenerator
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection

log = logging.getLogger(__name__)
QUERY_TEMPLATE = "SELECT {0} FROM %s "

class UpgradeSecondaryIndex(BaseSecondaryIndexingTests, NewUpgradeBaseTest):
    def setUp(self):
        self.all_index_nodes_lost = False
        super(UpgradeSecondaryIndex, self).setUp()
        self.initial_version = self.input.param('initial_version', '4.1.0-5005')
        self.upgrade_to = self.input.param("upgrade_to")
        self.use_replica = self.input.param("use_replica", False)
        query_template = QUERY_TEMPLATE
        query_template = query_template.format("job_title")
        self.whereCondition= self.input.param("whereCondition"," job_title != \"Sales\" ")
        query_template += " WHERE {0}".format(self.whereCondition)
        self.load_query_definitions = []
        self.initial_index_number = self.input.param("initial_index_number", 2)
        for x in range(1,self.initial_index_number):
            index_name = "index_name_"+str(x)
            query_definition = QueryDefinition(index_name=index_name, index_fields=["job_title"],
                                query_template=query_template, groups=["simple"])
            self.load_query_definitions.append(query_definition)
        self.initialize_multi_create_index(buckets = self.buckets,
                query_definitions = self.load_query_definitions)


    def tearDown(self):
        self.upgrade_servers = self.servers
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
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            self.upgrade_servers.append(server)
        upgrade_threads = self._async_update(self.upgrade_to, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        kv_ops = self.kv_mutations()
        for kv_op in kv_ops:
            kv_op.result()
        self.multi_query_using_index(buckets=self.buckets,
                    query_definitions=self.load_query_definitions)

    def test_online_upgrade(self):
        index_task = []
        services_in = []
        if self.index_op != "create":
            create_task = self.async_index_operations("create")
            self._run_tasks([create_task])
        server_out = self.nodes_out_list
        if not self.all_index_nodes_lost:
            index_task = self.async_index_operations()
        kv_ops = self.kv_mutations()
        log.info("Upgrading servers to {0}...".format(self.upgrade_to))
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[],self.nodes_out_list)
        rebalance.result()
        self.upgrade_servers = self.nodes_out_list
        upgrade_th = self._async_update(self.upgrade_to, server_out)
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        node_version = RestConnection(server_out[0]).get_nodes_versions()
        for service in self.services_map.keys():
            for node in self.nodes_out_list:
                node = "{0}:{1}".format(node.ip, node.port)
                if node in self.services_map[service]:
                    services_in.append(service)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.nodes_out_list, [],
                                                 services=services_in)
        rebalance.result()
        self._run_tasks([kv_ops, index_task])
        log.info("Upgraded to: {0}".format(node_version))
        if self.index_op == "drop" or self.all_index_nodes_lost:
            create_task = self.async_index_operations("create")
            self._run_tasks([create_task])
        self._verify_bucket_count_with_index_count()
        index_task = self.async_index_operations("query")
        self._run_tasks([index_task])

    def test_online_upgrade_swap_rebalance(self):
        """

        :return:
        """
        index_task = []
        self._install(self.nodes_in_list,version=self.upgrade_to)
        if self.index_op != "create":
            create_task = self.async_index_operations("create")
            self._run_tasks([create_task])
        kv_ops = self.kv_mutations()
        if not self.all_index_nodes_lost:
            index_task = self.async_index_operations()
        log.info("Swapping servers...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.nodes_in_list,
                                                 self.nodes_out_list)
        rebalance.result()
        log.info("===== Nodes Swapped with Upgraded versions =====")
        self.upgrade_servers = self.nodes_in_list
        self._run_tasks([kv_ops, index_task])
        if self.index_op != "drop":
            self._verify_bucket_count_with_index_count()
            index_task = self.async_index_operations("query")
            self._run_tasks([index_task])

    def test_upgrade_with_memdb(self):
        """
        Keep N1ql node on one of the kv nodes
        :return:
        """
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        log.info("Upgrading all kv nodes...")
        for node in kv_nodes:
            log.info("Rebalancing kv node {0} out to upgrade...".format(node.ip))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                 [node])
            rebalance.result()
            self.servers.remove(node)
            upgrade_th = self._async_update(self.upgrade_to, [node])
            for th in upgrade_th:
                th.join()
            log.info("Rebalancing kv node {0} in after upgrade...".format(node.ip))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [node], [],
                                                     services=['kv'])
            self.servers.insert(0, node)
            rebalance.result()
        log.info("===== KV Nodes Upgrade Complete =====")
        log.info("Upgrading all query nodes...")
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        log.info("Rebalancing query nodes out to upgrade...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                             query_nodes)
        rebalance.result()
        upgrade_th = self._async_update(self.upgrade_to, query_nodes)
        for th in upgrade_th:
            th.join()
        services_in = ["n1ql" for x in range(len(query_nodes))]
        log.info("Rebalancing query nodes in after upgrade...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 query_nodes, [],
                                                 services=services_in)
        rebalance.result()
        log.info("===== Query Nodes Upgrade Complete =====")
        kv_ops = self.kv_mutations()
        log.info("Upgrading all index nodes...")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        log.info("Rebalancing index nodes out to upgrade...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                             index_nodes)
        rebalance.result()
        upgrade_th = self._async_update(self.upgrade_to, index_nodes)
        rest = RestConnection(self.master)
        log.info("Setting indexer storage mode to {0}...".format(self.gsi_type))
        status = rest.set_indexer_storage_mode(storageMode=self.gsi_type)
        if status:
            log.info("====== Indexer Mode Set to {0}=====".format(self.gsi_type))
        else:
            self.info("====== Indexer Mode is not set to {0}=====".format(self.gsi_type))
            raise
        for th in upgrade_th:
            th.join()
        self._run_tasks([kv_ops])
        log.info("===== Index Nodes Upgrade Complete =====")
        services_in = ["index" for x in range(len(index_nodes))]
        log.info("Rebalancing index nodes in after upgrade...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 index_nodes, [],
                                                 services=services_in)
        rebalance.result()
        create_task = self.async_index_operations("create")
        self._run_tasks([create_task])
        self._verify_bucket_count_with_index_count()
        index_task = self.async_index_operations("query")
        self._run_tasks([index_task])

    def kv_mutations(self, docs=None):
        if not docs:
            docs = self.docs_per_day
        gens_load = self.generate_docs(docs)
        tasks = self.async_load(generators_load=gens_load, batch_size=self.batch_size)
        return tasks

    def _run_tasks(self, tasks_list):
        for tasks in tasks_list:
            if tasks:
                for th in tasks:
                    th.result()
