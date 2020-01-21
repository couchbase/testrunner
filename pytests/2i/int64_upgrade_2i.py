import copy
import logging
import random

from couchbase.bucket import Bucket
from couchbase_helper.data import FIRST_NAMES, COUNTRIES
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.query_definitions import QueryDefinition, SQLDefinitionGenerator
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from .upgrade_2i import UpgradeSecondaryIndex
from .base_2i import BaseSecondaryIndexingTests

log = logging.getLogger(__name__)
INT64_VALUES = [-9223372036854775808, 9223372036854775807, 9223372036854770000, 9000000000000000000,
                -9000000000000000000, -9223372036854770000, 5464748874972.17865, -5464748874972.17865,
                -2147483648, 2147483647, -2147483600, 2147483600, 32767, -32768, 30000, -30000, 100, -100, 0,
                110000000003421999, 9223372036852775807, 9007199254740991]
UPGRADE_VERS = ["5.0.0", "5.0.1", "5.1.0"]

class UpgradeSecondaryIndexInt64(UpgradeSecondaryIndex):
    def setUp(self):
        super(UpgradeSecondaryIndexInt64, self).setUp()
        self.disable_plasma_upgrade = self.input.param("disable_plasma_upgrade", False)
        self.rebalance_empty_node = self.input.param("rebalance_empty_node", True)
        self.initial_version = self.input.param('initial_version', '4.6.0-3653')
        self.post_upgrade_gsi_type = self.input.param('post_upgrade_gsi_type', 'memory_optimized')
        self.upgrade_to = self.input.param("upgrade_to")
        self.int64_verify_results = self.input.param("int64_verify_results", False)
        self.index_batch_size = self.input.param("index_batch_size", -1)
        self.query_results = {}
        self._create_int64_dataset()
        query_definition_generator = QueryDefs()
        self.query_definitions = query_definition_generator.generate_query_definition_for_aggr_data()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self._create_indexes()
        self._query_index("pre_upgrade")

    def tearDown(self):
        super(UpgradeSecondaryIndexInt64, self).tearDown()

    def test_offline_upgrade(self):
        upgrade_nodes = self.servers[:self.nodes_init]
        if self.disable_plasma_upgrade:
            self._install(self.nodes_in_list, version=self.upgrade_to)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [self.nodes_in_list[0]], [],
                                                     services=["index"])
            rebalance.result()
            self.sleep(100)
            self.disable_upgrade_to_plasma(self.nodes_in_list[0])
        for server in upgrade_nodes:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            upgrade_threads = self._async_update(self.upgrade_to, [server])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            self.upgrade_servers.append(server)
        self.sleep(100)
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("Cluster is healthy")
        self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
        log.info("All indexes are online")
        self.add_built_in_server_user()
        self.sleep(20)
        if self.initial_version.split("-")[0] in UPGRADE_VERS:
            self.multi_drop_index()
            self.sleep(100)
            self._create_indexes()
            self.sleep(100)
        self._query_index("post_upgrade")
        self._verify_post_upgrade_results()
        self._update_int64_dataset()
        self._query_for_long_num()

    def test_online_upgrade_with_failover(self):
        upgrade_nodes = self.servers[:self.nodes_init]
        if self.disable_plasma_upgrade:
            self._install(self.nodes_in_list, version=self.upgrade_to)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [self.nodes_in_list[0]], [],
                                                     services=["index"])
            rebalance.result()
            self.sleep(100)
            self.disable_upgrade_to_plasma(self.nodes_in_list[0])
        for node in upgrade_nodes:
            node_rest = RestConnection(node)
            node_info = "{0}:{1}".format(node.ip, node.port)
            node_services_list = node_rest.get_nodes_services()[node_info]
            if "index" in node_services_list:
                self._create_equivalent_indexes(node)
            failover_task = self.cluster.async_failover([self.master], failover_nodes=[node], graceful=False)
            failover_task.result()
            self.sleep(100)
            log.info("Node Failed over...")
            upgrade_th = self._async_update(self.upgrade_to, [node])
            for th in upgrade_th:
                th.join()
            log.info("==== Upgrade Complete ====")
            self.sleep(120)
            rest = RestConnection(self.master)
            nodes_all = rest.node_statuses()
            for cluster_node in nodes_all:
                if cluster_node.ip == node.ip:
                    log.info("Adding Back: {0}".format(node))
                    rest.add_back_node(cluster_node.id)
                    rest.set_recovery_type(otpNode=cluster_node.id, recoveryType="full")
            log.info("Adding node back to cluster...")
            active_nodes = [srvr for srvr in self.servers if srvr.ip != node.ip]
            rebalance = self.cluster.async_rebalance(active_nodes, [], [])
            rebalance.result()
            self.sleep(100)
            self._remove_equivalent_indexes(node)
            self.sleep(60)
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("Cluster is healthy")
        self.add_built_in_server_user()
        self.sleep(20)
        if self.initial_version.split("-")[0] in UPGRADE_VERS:
            self.multi_drop_index()
            self.sleep(100)
            self._create_indexes()
            self.sleep(100)
        self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
        log.info("All indexes are online")
        self._query_index("post_upgrade")
        self._verify_post_upgrade_results()
        self._update_int64_dataset()
        self._query_for_long_num()

    def test_online_upgrade_with_rebalance(self):
        upgrade_nodes = self.servers[:self.nodes_init]
        if self.disable_plasma_upgrade:
            self._install(self.nodes_in_list, version=self.upgrade_to)
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [self.nodes_in_list[0]], [],
                services=["index"])
            rebalance.result()
            self.sleep(100)
            self.disable_upgrade_to_plasma(self.nodes_in_list[0])
        for node in upgrade_nodes:
            node_rest = RestConnection(node)
            node_info = "{0}:{1}".format(node.ip, node.port)
            node_services_list = node_rest.get_nodes_services()[node_info]
            node_services = [",".join(node_services_list)]
            log.info("Rebalancing the node out...")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node])
            rebalance.result()
            self.sleep(100)
            active_nodes = [srvr for srvr in self.servers if srvr.ip != node.ip]
            log.info("Upgrading the node...")
            upgrade_th = self._async_update(self.upgrade_to, [node])
            for th in upgrade_th:
                 th.join()
            self.sleep(120)
            log.info("==== Upgrade Complete ====")
            log.info("Adding node back to cluster...")
            rebalance = self.cluster.async_rebalance(active_nodes,
                                             [node], [],
                                             services=node_services)
            rebalance.result()
            self.sleep(100)
            node_version = RestConnection(node).get_nodes_versions()
            log.info("{0} node Upgraded to: {1}".format(node.ip, node_version))
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("Cluster is healthy")
        self.add_built_in_server_user()
        self.sleep(20)
        if self.initial_version.split("-")[0] in UPGRADE_VERS:
            self.multi_drop_index()
            self.sleep(100)
            self._create_indexes()
            self.sleep(100)
        else:
            self._create_indexes()
            self.sleep(100)
        self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
        log.info("All indexes are online")
        self._query_index("post_upgrade")
        self._verify_post_upgrade_results()
        self._update_int64_dataset()
        self._query_for_long_num()

    def test_online_upgrade_with_swap_rebalance(self):
        """
        :return:
        """
        old_servers = self.servers[:self.nodes_init]
        if self.disable_plasma_upgrade:
            self._install(self.nodes_in_list, version=self.upgrade_to)
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [self.nodes_in_list[1]], [],
                services=["index"])
            rebalance.result()
            self.sleep(100)
            self.disable_upgrade_to_plasma(self.nodes_in_list[1])
            old_servers.append(self.nodes_in_list[1])
        log.info("Swapping servers...")

        service_map = self.get_nodes_services()
        i = 0
        swap_server = self.nodes_in_list[0]
        upgrade_nodes_list = self.servers[:self.nodes_init]

        for node in upgrade_nodes_list:
            self._install([swap_server], version=self.upgrade_to)
            service_on_node = service_map[node.ip]
            log.info("Swapping %s with %s with %s services" % (node, swap_server, service_on_node))
            servers = old_servers
            rebalance = self.cluster.async_rebalance(servers,
                                                     [swap_server], [node],
                                                     services=[service_on_node])
            rebalance.result()
            self.sleep(30)
            i += 1
            old_servers.append(swap_server)
            old_servers.remove(node)
            if (self.master not in old_servers) and ("kv" in service_on_node):
                self.master = swap_server
            swap_server = node
            self.n1ql_node = self.get_nodes_from_services_map(
                service_type="n1ql", master=old_servers[0])
            log.info("Master : %s", self.master)

        log.info("===== Nodes Swapped with Upgraded versions =====")
        self.upgrade_servers = self.nodes_in_list
        if self.initial_version.split("-")[0] in UPGRADE_VERS:
            self.multi_drop_index()
            self.sleep(100)
            self._create_indexes()
            self.sleep(100)
        else:
            self._create_indexes()
            self.sleep(100)
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("Cluster is healthy")
        self.add_built_in_server_user()
        self.sleep(20)
        self.assertTrue(self.wait_until_indexes_online(),
                        "Some indexes are not online")
        log.info("All indexes are online")
        self._query_index("post_upgrade")
        self._verify_post_upgrade_results()
        self._update_int64_dataset()
        self._query_for_long_num()

    def test_rolling_upgrade(self):
        upgrade_versions = ["5.0.0-5003", self.upgrade_to]
        for ver in upgrade_versions:
            for server in self.servers:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
                self.upgrade_servers.append(server)
            upgrade_threads = self._async_update(self.upgrade_to, self.servers)
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            self.sleep(120)
            self.add_built_in_server_user()
            self.sleep(20)
            if ver.startswith("5.5") or ver.startswith("5.1"):
                self.multi_drop_index()
                self.sleep(100)
                self._create_indexes()
                self.sleep(100)
            msg = "Cluster is not healthy after upgrade"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("Cluster is healthy")
            self.add_built_in_server_user()
            self.sleep(20)
            self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
            log.info("All indexes are online")
            self._query_index("post_upgrade")
            if ver == self.upgrade_to:
                self._verify_post_upgrade_results()
                self._update_int64_dataset()
                self._query_for_long_num()

    def test_online_upgrade_with_rebalance_stats(self):
        upgrade_nodes = self.servers[:self.nodes_init]
        create_index_query1 = "CREATE INDEX idx ON default(name) USING GSI  WITH {'nodes': ['%s:%s']}" % (self.servers[1].ip, self.servers[1].port)
        self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                       server=self.n1ql_node)
        if self.disable_plasma_upgrade:
            self._install(self.nodes_in_list, version=self.upgrade_to)
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [self.nodes_in_list[0]], [],
                services=["index"])
            rebalance.result()
            self.sleep(30)
            self.disable_upgrade_to_plasma(self.nodes_in_list[0])
        for node in upgrade_nodes:
            node_rest = RestConnection(node)
            node_info = "{0}:{1}".format(node.ip, node.port)
            node_services_list = node_rest.get_nodes_services()[node_info]
            node_services = [",".join(node_services_list)]
            log.info("Rebalancing the node out...")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node])
            rebalance.result()
            self.sleep(30)
            active_nodes = [srvr for srvr in self.servers if srvr.ip != node.ip]
            log.info("Upgrading the node...")
            upgrade_th = self._async_update(self.upgrade_to, [node])
            for th in upgrade_th:
                 th.join()
            self.sleep(60)
            log.info("==== Upgrade Complete ====")
            log.info("Adding node back to cluster...")
            rebalance = self.cluster.async_rebalance(active_nodes,
                                             [node], [],
                                             services=node_services)
            rebalance.result()
            self.sleep(30)
            node_version = RestConnection(node).get_nodes_versions()
            log.info("{0} node Upgraded to: {1}".format(node.ip, node_version))
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("Cluster is healthy")
        self.add_built_in_server_user()
        self.sleep(20)
        self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
        log.info("All indexes are online")

        index_map = self.get_index_stats()
        self.log.info(index_map)
        for index in index_map['default']:
            self.assertTrue("key_size_distribution" in str(index_map['default'][index]))
        self.rest.flush_bucket("default")
        self.sleep(60)
        string_70 = "x" * 70
        string_3000 = "x" * 3000
        string_103000 = "x" * 103000

        insert_query1 = 'INSERT INTO default (KEY, VALUE) VALUES ("id1", { "name" : "%s" })' % string_70
        insert_query2 = 'INSERT INTO default (KEY, VALUE) VALUES ("id2", { "name" : {"name": "%s", "fake": "%s"} })' % (
            string_70, string_3000)
        insert_query5 = 'INSERT INTO default (KEY, VALUE) VALUES ("id5", { "name" : "%s" })' % string_103000

        self.n1ql_helper.run_cbq_query(query=insert_query1,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query2,
                                       server=self.n1ql_node)
        self.n1ql_helper.run_cbq_query(query=insert_query5,
                                       server=self.n1ql_node)
        index_map = self.get_index_stats()
        self.log.info(index_map)
        for index in index_map['default']:
            if index == 'idx':
                self.log.info(index_map['default'][index]['key_size_distribution'])
                self.assertTrue(str(index_map['default'][index]['key_size_distribution']) == "{u'(0-64)': 0, u'(257-1024)': 0, u'(65-256)': 1, u'(4097-102400)': 0, u'(1025-4096)': 1, u'(102401-max)': 1}")


    def _create_int64_dataset(self):
        generators = []
        document_template = '{{"name":"{0}", "int_num": {1}, "long_num":{2}, "long_num_partial":{3}, "long_arr": {4},' \
                            '"int_arr": {5}}}'
        num_items = len(self.full_docs_list)
        for i in range(num_items):
            name = random.choice(FIRST_NAMES)
            int_num = random.randint(-100, 100)
            long_num = random.choice(INT64_VALUES)
            long_arr = [random.choice(INT64_VALUES) for i in range(10)]
            int_arr = [random.randint(-100, 100) for i in range(10)]
            doc_id = "int64_" + str(random.random()*100000)
            generators.append(DocumentGenerator(doc_id, document_template, [name], [int_num], [long_num], [long_num],
                                                [long_arr], [int_arr], start=0, end=1))
        self.load(generators, buckets=self.buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)

    def _update_int64_dataset(self):
        testuser = []
        rolelist = []
        for bucket in self.buckets:
            testuser.append({'id': bucket.name, 'name': bucket.name, 'password': 'password'})
            rolelist.append({'id': bucket.name, 'name': bucket.name, 'roles': 'admin'})
        self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)
        for doc in self.full_docs_list:
            doc["name"] = random.choice(FIRST_NAMES)
            self._update_document(doc["_id"], doc)

    def _update_document(self, key, document):
        url = 'couchbase://{ip}/default'.format(ip=self.master.ip)
        if self.upgrade_to.startswith("4"):
            bucket = Bucket(url)
        else:
            bucket = Bucket(url, username="default", password="password")
        bucket.upsert(key, document)

    def _query_for_long_num(self):
        wrong_results = []
        for query_def in self.query_definitions:
            if query_def.index_name.endswith("_long_num"):
                query_def.query_template.extend("select long_num from default use index ({0}) where long_num = " + str(t) for t in INT64_VALUES)
                query_def.query_template.extend("select long_num from default use index ({0}) where long_num > " + str(t) for t in INT64_VALUES)
                query_def.query_template.extend("select long_num from default use index ({0}) where long_num > " + str(t-1) + " and long_num < " + str(t+1) for t in INT64_VALUES)
                query_def.query_template.extend("select long_num from default use index ({0}) where long_num between " + str(t-1) + " and " + str(t+1) for t in INT64_VALUES)
                for query in query_def.query_template:
                    query = query.format(query_def.index_name)
                    if " = " in query:
                        if_cond = 'num["long_num"]' + query.split("where long_num")[1].replace("=", "==")
                    elif ">" in query:
                        if_cond = query.split("where ")[1].replace("long_num", "num['long_num']")
                    elif "between" in query:
                        if_cond = 'num["long_num"] > ' + query.split("where long_num between ")[1].split("and")[0] \
                                  + ' and num["long_num"] < ' + query.split("where long_num between ")[1].split("and")[1]
                    elif "null" in query:
                        if_cond = query.split("where ")[1].replace("long_num", "num['long_num']").replace("null", "None")
                    self.gen_results.query = query
                    expected_result = [{"long_num": num["long_num"]} for num in self.full_docs_list if eval(if_cond)]
                    msg, check = self.n1ql_helper.run_query_and_verify_result(query=query, server=self.n1ql_node,
                                                                              timeout=500,
                                                                              expected_result=expected_result,
                                                                              scan_consistency="request_plus",
                                                                              verify_results=self.int64_verify_results)
                    if not check:
                        wrong_results.append(query)
                    self.assertEqual(len(wrong_results), 0, str(wrong_results))

    def _create_indexes(self):
        for query_def in self.query_definitions:
            for bucket in self.buckets:
                self.create_index(bucket.name, query_def)

    def _query_index(self, phase):
        if phase not in list(self.query_results.keys()):
            self.query_results[phase] = {}
        query_results = {}
        for query_def in self.query_definitions:
            if query_def.index_name not in list(query_results.keys()):
                query_results[query_def.index_name] = []
                for query in query_def.query_template:
                    query = query.format(query_def.index_name)
                    results = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
                    query_results[query_def.index_name].append(results["results"])
        self.query_results[phase] = query_results

    def _verify_post_upgrade_results(self):
        wrong_results = {}
        for query_def in self.query_definitions:
            index_name = query_def.index_name
            if "long_num" in index_name or "long_arr" in index_name:
                continue
            elif not index_name.endswith("_long_num_name"):
                for i in range(len(self.query_results["pre_upgrade"][index_name])):
                    if sorted(self.query_results["pre_upgrade"][index_name][i]) != sorted(self.query_results["post_upgrade"][index_name][i]):
                        if index_name not in list(wrong_results.keys()):
                            wrong_results[index_name] = query_def.query_template
                        else:
                            wrong_results[index_name].extend(query_def.query_template)
        self.assertEqual(len(wrong_results), 0, str(wrong_results))

class SecondaryIndexIndexInt64(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexIndexInt64, self).setUp()
        self.query_results = {}
        self._create_int64_dataset()
        query_definition_generator = QueryDefs()
        self.query_definitions = query_definition_generator.generate_query_definition_for_aggr_data()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)

    def tearDown(self):
        super(SecondaryIndexIndexInt64, self).tearDown()

    def test_fresh_install_int64(self):
        for query_def in self.query_definitions:
            for bucket in self.buckets:
                self.create_index(bucket.name, query_def)

        for query_def in self.query_definitions:
            for query in query_def.query_template:
                index_query = query.format(query_def.index_name)
                query_results = self.n1ql_helper.run_cbq_query(query=index_query, server=self.n1ql_node)
                primary_query = query.format("#primary")
                primary_results = self.n1ql_helper.run_cbq_query(query=primary_query, server=self.n1ql_node)
                self._verify_aggregate_pushdown_results(query_def.index_name, index_query, query_results, primary_results)

    def _verify_aggregate_pushdown_results(self, index_name, query, query_results, primary_results):
        wrong_results = {}
        if "long_num" in index_name or "long_arr" in index_name or not index_name.endswith("_long_num_name"):
            return
        if sorted(query_results) != sorted(primary_results):
                if index_name not in list(wrong_results.keys()):
                    wrong_results[index_name] = [query]
                else:
                    wrong_results[index_name].append(query)
        self.assertEqual(len(wrong_results), 0, str(wrong_results))

class QueryDefs(SQLDefinitionGenerator):
    def generate_query_definition_for_aggr_data(self):
        definitions_list = []
        index_name_prefix = "int_64_" + str(random.randint(100000, 999999))
        # simple index on string
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_name",
                            index_fields=["name"],
                            query_template=["SELECT name FROM default use index ({0}) where name = 'Ciara'",
                                            "SELECT name FROM default use index ({0}) where name > 'Ciara'",
                                            "SELECT name FROM default where name is not null"],
                            groups=["all", "simple_index"]))
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_long_num",
                            index_fields=["long_num"],
                            query_template=["SELECT long_num FROM default use index ({0}) where long_num = 2147483600",
                                            "SELECT long_num FROM default use index ({0}) where long_num > 2147483600",
                                            "SELECT long_num FROM default use index ({0}) where long_num > 2147483599 and long_num < 2147483601",
                                            "SELECT long_num FROM default where long_num is not null"],
                            groups=["all", "simple_index"]))
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_long_num_partial",
                            index_fields=["long_num_partial"],
                            query_template=["SELECT long_num_partial FROM default use index ({0}) where long_num = 2147483600",
                                            "SELECT long_num_partial FROM default use index ({0}) where long_num > 2147483600",
                                            "SELECT long_num_partial FROM default use index ({0}) where (long_num % 10 != 0)",
                                            "SELECT long_num_partial FROM default where long_num is not null"],
                            groups=["all", "simple_index"],
                            index_where_clause=" long_num > 20 "))
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_long_arr",
                            index_fields=["ALL ARRAY t FOR t in `long_arr` END"],
                            query_template=["SELECT t FROM default use index ({0}) where any t in `long_arr` satisfies t = 2147483600 END",
                                            "SELECT t FROM default use index ({0}) where any t in `long_arr` satisfies t > 2147483600 END",
                                            "SELECT t FROM default use index ({0}) where any t in `long_arr` satisfies t is not null END"],
                            groups=["all", "simple_index"]))
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_long_num_name",
                            index_fields=["long_num", "name"],
                            query_template=[
                                            # Commented out because of MB-30207
                                            #"SELECT sum(long_num) as long_num, name FROM default use index ({0}) where long_num > 2147483600 group by name",
                                            "SELECT min(long_num) as long_num, name FROM default use index ({0}) where long_num > 2147483600 group by name",
                                            "SELECT long_num, name FROM default use index ({0}) where long_num > 2147483600"],
                            groups=["all", "simple_index"]))
        return definitions_list
