import logging
from datetime import datetime

from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition, SQLDefinitionGenerator
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

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
        self.initial_index_number = self.input.param("initial_index_number", 3)
        for x in range(self.initial_index_number):
            index_name = "index_name_"+str(x)
            query_definition = QueryDefinition(index_name=index_name, index_fields=["job_title"],
                                query_template=query_template, groups=["simple"])
            self.load_query_definitions.append(query_definition)
        self.multi_create_index(buckets = self.buckets,
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
        services_in = []
        before_tasks = self.async_run_operations(buckets=self.buckets, phase="before")
        server_out = self.nodes_out_list
        self._run_tasks([before_tasks])
        in_between_tasks = self.async_run_operations(buckets=self.buckets, phase="in_between")
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
        self._run_tasks([kv_ops, in_between_tasks])
        self.sleep(60)
        log.info("Upgraded to: {0}".format(node_version))
        nodes_out = []
        for service in self.nodes_out_dist.split("-"):
            nodes_out.append(service.split(":")[0])
        if "index" in nodes_out or "n1ql" in nodes_out:
            self._verify_bucket_count_with_index_count(query_definitions=self.load_query_definitions)
        else:
            self._verify_bucket_count_with_index_count()
        after_tasks = self.async_run_operations(buckets=self.buckets, phase="after")
        self.sleep(180)
        self._run_tasks([after_tasks])

    def test_online_upgrade_swap_rebalance(self):
        """
        :return:
        """
        before_tasks = self.async_run_operations(buckets=self.buckets, phase="before")
        self._run_tasks([before_tasks])
        self._install(self.nodes_in_list,version=self.upgrade_to)
        in_between_tasks = self.async_run_operations(buckets=self.buckets, phase="in_between")
        kv_ops = self.kv_mutations()
        log.info("Swapping servers...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.nodes_in_list,
                                                 self.nodes_out_list)
        rebalance.result()
        log.info("===== Nodes Swapped with Upgraded versions =====")
        self.upgrade_servers = self.nodes_in_list
        self._run_tasks([kv_ops, in_between_tasks])
        self.sleep(60)
        nodes_out = []
        for service in self.nodes_out_dist.split("-"):
            nodes_out.append(service.split(":")[0])
        if "index" in nodes_out or "n1ql" in nodes_out:
            self._verify_bucket_count_with_index_count(query_definitions=self.load_query_definitions)
        else:
            self._verify_bucket_count_with_index_count()
        after_tasks = self.async_run_operations(buckets=self.buckets, phase="after")
        self.sleep(180)
        self._run_tasks([after_tasks])

    def test_upgrade_with_memdb(self):
        """
        Keep N1ql node on one of the kv nodes
        :return:
        """
        self.set_circular_compaction = self.input.param("set_circular_compaction", False)
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
        self.sleep(60)
        if self.set_circular_compaction:
            DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            rest = RestConnection(servers[0])
            date = datetime.now()
            dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)/60))/24)%7
            status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+1)/60),
                                              indexFromMinute=(date.minute+1)%60)
            self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.multi_create_index(self.buckets, self.query_definitions)
        self._verify_bucket_count_with_index_count()
        self.multi_query_using_index(self.buckets, self.query_definitions)

    def test_online_upgrade_with_two_query_nodes(self):
        query_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        upgrade_node = query_nodes[0]
        self.assertGreater(len(query_nodes), 1, "Test requires more than 1 Query Node")
        before_tasks = self.async_run_operations(buckets=self.buckets, phase="before")
        self._run_tasks([before_tasks])
        in_between_tasks = self.async_run_operations(buckets=self.buckets, phase="in_between")
        kv_ops = self.kv_mutations()
        log.info("Upgrading servers to {0}...".format(self.upgrade_to))
        self.n1ql_node = query_nodes[1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[], [upgrade_node])
        rebalance.result()
        self.upgrade_servers = self.nodes_out_list
        upgrade_th = self._async_update(self.upgrade_to, [upgrade_node])
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        node_version = RestConnection(upgrade_node).get_nodes_versions()
        services_in = ["n1ql"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [upgrade_node], [],
                                                 services=services_in)
        rebalance.result()
        self._run_tasks([kv_ops, in_between_tasks])
        self.sleep(60)
        log.info("Upgraded to: {0}".format(node_version))
        for node in query_nodes:
            if node == upgrade_node:
                self.n1ql_node = node
                try:
                    self._verify_bucket_count_with_index_count()
                    after_tasks = self.async_run_operations(buckets=self.buckets, phase="after")
                    self._run_tasks([after_tasks])
                except Exception, ex:
                    log.info(str(ex))
                else:
                    self._verify_bucket_count_with_index_count()
                    after_tasks = self.async_run_operations(buckets=self.buckets, phase="after")
                    self._run_tasks([after_tasks])

    def test_online_upgrade_with_mixed_mode_cluster(self):
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        upgrade_node = kv_nodes[0]
        self.assertGreater(len(kv_nodes), 1, "Test requires more than 1 kv Node")
        before_tasks = self.async_run_operations(buckets=self.buckets, phase="before")
        self._run_tasks([before_tasks])
        self.n1ql_node = kv_nodes[1]
        in_between_tasks = self.async_run_operations(buckets=self.buckets, phase="in_between")
        kv_ops = self.kv_mutations()
        log.info("Upgrading servers to {0}...".format(self.upgrade_to))
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[], [upgrade_node])
        rebalance.result()
        self.upgrade_servers = self.nodes_out_list
        upgrade_th = self._async_update(self.upgrade_to, [upgrade_node])
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        node_version = RestConnection(upgrade_node).get_nodes_versions()
        services_in = self.services_in
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [upgrade_node], [],
                                                 services=services_in)
        rebalance.result()
        self._run_tasks([kv_ops, in_between_tasks])
        self.sleep(60)
        log.info("Upgraded to: {0}".format(node_version))
        for node in kv_nodes:
            if node == upgrade_node:
                self.n1ql_node = node
                try:
                    self._verify_bucket_count_with_index_count()
                    after_tasks = self.async_run_operations(buckets=self.buckets, phase="after")
                    self._run_tasks([after_tasks])
                except Exception, ex:
                    log.info(str(ex))
                else:
                    self._verify_bucket_count_with_index_count()
                    after_tasks = self.async_run_operations(buckets=self.buckets, phase="after")
                    self._run_tasks([after_tasks])

    def kv_mutations(self, docs=None):
        if not docs:
            docs = self.docs_per_day
        gens_load = self.generate_docs(docs)
        tasks = self.async_load(generators_load=gens_load, batch_size=self.batch_size)
        return tasks

    def _run_tasks(self, tasks_list):
        for tasks in tasks_list:
            for task in tasks:
                task.result()
