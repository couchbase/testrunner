import json
import re

from Cb_constants import CbServer
from collection.collections_stats import CollectionsStats
from .base_gsi import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
import random
from lib import testconstants
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from threading import Thread
from pytests.query_tests_helper import QueryHelperTests
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.query_definitions import QueryDefinition


class GSIReplicaIndexesTests(BaseSecondaryIndexingTests, QueryHelperTests):
    def setUp(self):
        super(GSIReplicaIndexesTests, self).setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")
        self.create_primary_index = False
        info = "linux"
        if not self.capella_run:
            shell = RemoteMachineShellConnection(self.servers[0])
            info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
            self.backup_path = testconstants.LINUX_BACKUP_PATH
        elif info == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = testconstants.WIN_COUCHBASE_BIN_PATH_RAW
            self.backup_path = testconstants.WIN_BACKUP_C_PATH
        elif info == 'mac':
            self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
            self.backup_path = testconstants.LINUX_BACKUP_PATH
        else:
            raise Exception("OS not supported.")

        self.expected_err_msg = self.input.param("expected_err_msg", None)
        self.nodes = self.input.param("nodes", None)
        self.override_default_num_replica_with_num = self.input.param(
            "override_with_num", 0)
        self.override_default_num_replica_with_nodes = self.input.param(
            "override_with_nodes", None)
        if self.override_default_num_replica_with_nodes:
            self.nodes = self.override_default_num_replica_with_nodes
        self.node_out = self.input.param("node_out", 0)
        self.server_grouping = self.input.param("server_grouping", None)
        self.eq_index_node = self.input.param("eq_index_node", None)
        self.recovery_type = self.input.param("recovery_type", None)
        self.dest_node = self.input.param("dest_node", None)
        self.rand = random.randint(1, 1000000000)
        self.expected_nodes = self.input.param("expected_nodes", None)
        self.failure_in_node = self.input.param("failure_in_node", None)
        self.alter_index = self.input.param("alter_index", None)

    def tearDown(self):
        super(GSIReplicaIndexesTests, self).tearDown()

    def test_create_replica_index_with_num_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

    def test_create_replica_index_one_failed_node_num_replica(self):
        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

    def test_failover_during_create_index_with_replica(self):
        node_out = self.servers[self.node_out]
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': true}};".format(
            self.num_index_replica)

        threads = [
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(create_index_query, 10, self.n1ql_node)),
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful))]
        for thread in threads:
            thread.start()
            self.sleep(1)
        for thread in threads:
            thread.join()
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)

        try:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")

    def test_create_replica_index_with_node_list(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

    def test_create_replica_index_one_failed_node_with_node_list(self):
        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

    def test_create_replica_index_with_num_replicas_and_node_list(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes': {1}}};".format(
            self.num_index_replica, nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica,
                                                    nodes)

    def test_rebalance_of_failed_server_group(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
        # Default source zone
        zones = list(self.rest.get_zone_names().keys())
        source_zone = zones[0]
        self._create_server_groups()
        num_of_docs = 10 ** 2
        self.bucket_params = self._create_bucket_params(server=self.master, size=256,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1']
        index_lists = []
        for index_fields, idx_num in zip(index_field_list, range(10)):
            index_name = f'{idx_prefix}_{idx_num}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=[index_fields])
            index_gen_list.append(index_gen)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_replica=1)
            index_gen_query_list.append(query)
            index_lists.append(index_name)

        tasks = []
        err_msg1 = 'Create index or Alter replica cannot proceed due to another concurrent create index request'
        err_msg2 = 'will retry building in the background'
        err_msg3 = 'Encountered transient error'


        with ThreadPoolExecutor() as executor:
            for count, query in enumerate(index_gen_query_list):
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    if err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        self.log.info(f"{index_name} is scheduled for background")
                    elif err_msg2 in str(err) or err_msg3 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online(timeout=1800)
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), len(index_lists) * (self.num_replicas + 1))
        index_hosts = set()
        for index in index_meta_info:
            host = index['hosts'][0]
            index_hosts.add(host.split(':')[0])

        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        failover_task = self.cluster.async_failover(self.servers[:self.nodes_init],
                                                    failover_nodes=self.servers[2:self.nodes_init], graceful=False,
                                                    wait_for_pending=180)
        failover_task.result()

        # Adding new nodes to server_group_3
        node_in = [self.servers[4], self.servers[5]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], node_in, [],
                                                 services=["kv,index,n1ql", "index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(60, "Waiting before checking for Index map and stats")
        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [node_in],
                [])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.log.error(f"Error in index distribution post rebalance : {ex})")
            else:
                self.log.info(str(ex))

    def test_rebalance_of_failed_server_group_with_partitioned_index(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        # Default source zone
        zones = list(self.rest.get_zone_names().keys())
        source_zone = zones[0]
        self._create_server_groups()
        num_of_docs = 10 ** 2
        self.bucket_params = self._create_bucket_params(server=self.master, size=256,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        idx_prefix = 'idx'
        index_gen_list = []
        index_gen_query_list = []
        regex_pattern = re.compile('.*?Index creation for index (.*?),.*')
        index_field_list = ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                            'suffix', 'filler1']
        index_lists = []
        index_gen = QueryDefinition(index_name='partitioned_idx', index_fields=['age', 'city'],
                                    partition_by_fields=['meta().id'])
        partitioned_index_query = index_gen.generate_index_create_query(namespace=collection_namespace, num_replica=1)
        index_gen_query_list.append(partitioned_index_query)
        index_lists.append('partitioned_idx')
        tasks = []
        err_msg1 = 'The index is scheduled for background creation'
        err_msg2 = 'Index creation will be retried in background'
        with ThreadPoolExecutor() as executor:
            for count, query in enumerate(index_gen_query_list):
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    if err_msg1 in str(err):
                        out = re.search(regex_pattern, str(err))
                        index_name = out.groups()[0]
                        self.log.info(f"{index_name} is scheduled for background")
                    elif err_msg2 in str(err):
                        continue
                    else:
                        self.fail(err)
        self.wait_until_indexes_online()
        index_meta_info = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_meta_info), len(index_lists) * (self.num_replicas + 1))

        failover_task = self.cluster.async_failover(self.servers[:self.nodes_init],
                                                    failover_nodes=self.servers[2:self.nodes_init], graceful=False,
                                                    wait_for_pending=180)
        failover_task.result()

        # Adding new nodes to group1
        node_in = [self.servers[4], self.servers[5]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], node_in, [],
                                                 services=["kv,index,n1ql", "index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        index_meta_info = index_metadata = self.rest.get_indexer_metadata()['status']
        for index in index_meta_info:
            hosts = index['hosts']
            self.assertTrue(f"{self.servers[2].ip}:{self.node_port}" not in hosts,
                            f"Index stats are not correct. {index}")
            self.assertTrue(f"{self.servers[3].ip}:{self.node_port}" not in hosts,
                            f"Index stats are not correct. {index}")
            self.assertTrue((f"{self.servers[4].ip}:{self.node_port}" in hosts) or (
                        f"{self.servers[5].ip}:{self.node_port}" in hosts),
                            f"Index stats are not correct. {index}")
            self.assertEqual(index['numPartition'], 8)

        query = f'select count(*) from {collection_namespace}  where age > 20 and city like "%%"'
        result = self.run_cbq_query(query=query)['results'][0]['$1']
        self.assertNotEqual(result, 0)

    def test_create_replica_index_with_server_groups(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        self._create_server_groups()
        self.sleep(5)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map = self.get_index_map()

        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

    def test_create_index_while_another_index_building(self):
        index_name_age = "age_index_" + str(
            random.randint(100000, 999999))
        index_name_name = "name_index_" + str(
            random.randint(100000, 999999))
        create_index_query_age = "CREATE INDEX " + index_name_age + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': true}};".format(
            self.num_index_replica)
        create_index_query_name = "CREATE INDEX " + index_name_name + " ON default(name) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query_age,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        build_index_query_age = "BUILD INDEX on `default`(" + index_name_age + ")"

        threads = [
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_build_index_query",
                   args=(build_index_query_age, 10, self.n1ql_node)),
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_create_index_query",
                   args=(create_index_query_name, 10, self.n1ql_node))]

        for thread in threads:
            thread.start()
            self.sleep(1)

        # Check for thread completion and any errors
        for thread in threads:
            thread.join()
            if hasattr(thread, '_exception') and thread._exception:
                self.log.error(f"Thread {thread.name} ended with error: {thread._exception}")
            elif thread.is_alive():
                self.log.warning(f"Thread {thread.name} is still alive after join")
            else:
                self.log.info(f"Thread {thread.name} completed successfully")

        self.sleep(120)
        index_map = self.get_index_map()
        self.log.info(index_map)

        try:
            self.n1ql_helper.verify_replica_indexes([index_name_age],
                                                    index_map,
                                                    self.num_index_replica)
            self.n1ql_helper.verify_replica_indexes([index_name_name],
                                                    index_map,
                                                    self.num_index_replica)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")

    def test_default_num_indexes(self):
        self.rest.set_indexer_num_replica(self.num_index_replica)
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age)"

        if self.override_default_num_replica_with_nodes and self.override_default_num_replica_with_num:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes': {1}}};".format(
                self.override_default_num_replica_with_num, nodes)

        elif self.override_default_num_replica_with_nodes:
            create_index_query += "USING GSI  WITH {{'nodes': {0}}};".format(
                nodes)

        elif self.override_default_num_replica_with_num:
            create_index_query += "USING GSI  WITH {{'num_replica': {0}}};".format(
                self.override_default_num_replica_with_num)

        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        expected_num_replicas = self.num_index_replica
        if self.override_default_num_replica_with_num > 0:
            expected_num_replicas = self.override_default_num_replica_with_num
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    expected_num_replicas,
                                                    nodes)

        # Reset the default value for num_replica
        self.rest.set_indexer_num_replica(0)

    def test_build_index_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': true}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             self.num_index_replica,
                                                             defer_build=True)

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             self.num_index_replica,
                                                             defer_build=False)

    def test_build_index_with_replica_one_failed_node(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}, 'defer_build': true}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=True)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        try:
            self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                                 len(nodes) - 1,
                                                                 defer_build=False)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

    def test_failover_during_build_index(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}, 'defer_build': true}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=True)

        node_out = self.servers[self.node_out]
        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"
        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(build_index_query, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful)))
        for thread in threads:
            thread.start()
            self.sleep(1)

        for thread in threads:
            thread.join()

        self.sleep(30)

        index_map = self.get_index_map()
        try:
            self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                                 len(nodes) - 1,
                                                                 defer_build=False)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

    def test_build_index_with_replica_failover_addback(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}, 'defer_build': true}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=True)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        self.sleep(30)

        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(180)

        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=False)

    def test_build_index_with_network_partitioning(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}, 'defer_build': true}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=True)

        node_out = self.servers[self.node_out]
        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.start_firewall_on_node(node_out)
            self.sleep(10)
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        finally:
            # Heal network partition and wait for some time to allow indexes
            # to get built automatically on that node
            self.stop_firewall_on_node(node_out)
            self.sleep(360)

            index_map = self.get_index_map()
            self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                                 len(nodes) - 1,
                                                                 defer_build=False)

    def test_build_index_while_another_index_building(self):
        index_name_age = "age_index_" + str(
            random.randint(100000, 999999))
        index_name_name = "name_index_" + str(
            random.randint(100000, 999999))
        create_index_query_age = "CREATE INDEX " + index_name_age + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build':true}};".format(
            self.num_index_replica)
        create_index_query_name = "CREATE INDEX " + index_name_name + " ON default(name) USING GSI  WITH {{'num_replica': {0}, 'defer_build':true}};".format(
            self.num_index_replica)

        self.n1ql_helper.run_cbq_query(query=create_index_query_age,
                                       server=self.n1ql_node)

        self.n1ql_helper.run_cbq_query(query=create_index_query_name,
                                       server=self.n1ql_node)

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)

        try:
            self.n1ql_helper.verify_replica_indexes([index_name_age],
                                                    index_map,
                                                    self.num_index_replica)
            self.n1ql_helper.verify_replica_indexes([index_name_name],
                                                    index_map,
                                                    self.num_index_replica)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")

        build_index_query_age = "BUILD INDEX on `default`(" + index_name_age + ")"
        build_index_query_name = "BUILD INDEX on `default`(" + index_name_name + ")"

        threads = [
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query_1",
                   args=(build_index_query_age, 10, self.n1ql_node)),
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query_2",
                   args=(build_index_query_name, 10, self.n1ql_node))]

        for thread in threads:
            thread.start()
            self.sleep(1)

        # Check for thread completion and any errors
        for thread in threads:
            thread.join()
            if hasattr(thread, '_exception') and thread._exception:
                self.log.error(f"Thread {thread.name} ended with error: {thread._exception}")
            elif thread.is_alive():
                self.log.warning(f"Thread {thread.name} is still alive after join")
            else:
                self.log.info(f"Thread {thread.name} completed successfully")

        # Index building in the background can take longer, so wait for some time
        self.sleep(360)

        try:
            index_map = self.get_index_map()
            self.log.info(index_map)
            self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                                 self.num_index_replica,
                                                                 defer_build=False)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

    def test_drop_index_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_index_replica, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_index_with_replica_one_failed_node(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_index_replica, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_failover_during_drop_index(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        node_out = self.servers[self.node_out]
        drop_index_query = "DROP INDEX `default`." + index_name_prefix
        self.log.info(f"Drop index query: {drop_index_query}")
        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(drop_index_query, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful)))

        for thread in threads:
            thread.start()
        self.sleep(5)
        # Check for thread completion and any errors
        for thread in threads:
            thread.join()
            if hasattr(thread, '_exception') and thread._exception:
                self.log.error(f"Thread {thread.name} ended with error: {thread._exception}")
            elif thread.is_alive():
                self.log.warning(f"Thread {thread.name} is still alive after join")
            else:
                self.log.info(f"Thread {thread.name} completed successfully")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_index_with_replica_failover_addback(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_index_replica, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(180)

        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_index_with_network_partitioning(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_index_replica, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        node_out = self.servers[self.node_out]
        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.start_firewall_on_node(node_out)
            self.sleep(10)
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        finally:
            # Heal network partition and wait for some time to allow indexes
            # on the node to get dropped automatically
            self.stop_firewall_on_node(node_out)
            self.sleep(180)

            index_map = self.get_index_map()
            self.log.info("Index map after drop index: %s", index_map)
            if not index_map == {}:
                self.fail("Indexes not dropped correctly")

    def test_replica_movement_with_rebalance_out(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out], indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_dropped_replica_add_new_node(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance1 = self.get_index_map()
        stats_map_after_rebalance1 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance1,
                stats_map_before_rebalance,
                stats_map_after_rebalance1,
                [],
                [node_out], indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

        node_in = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [node_in], [],
                                                 services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance2 = self.get_index_map()
        stats_map_after_rebalance2 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance2,
                stats_map_before_rebalance,
                stats_map_after_rebalance2,
                [node_in],
                [])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    # This test is for MB-25609 : Fix wait for index build in rebalancer for replica repair
    def test_rebalance_in_out_same_node_with_deferred_and_non_deferred_indexes(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_non_deferred_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_non_deferred_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_non_deferred_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        create_deferred_index_query = "CREATE INDEX " + index_name_prefix + "_deferred ON default(age) USING GSI  WITH {{'nodes': {0},'defer_build':true}};".format(
            nodes)
        self.log.info(create_deferred_index_query)
        try:
            self.n1ql_helper.run_cbq_query(
                query=create_deferred_index_query,
                server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance1 = self.get_index_map()
        stats_map_after_rebalance1 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance1,
                stats_map_before_rebalance,
                stats_map_after_rebalance1,
                [],
                [node_out], indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

        node_in = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [node_in], [],
                                                 services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance2 = self.get_index_map()
        stats_map_after_rebalance2 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance2,
                stats_map_before_rebalance,
                stats_map_after_rebalance2,
                [node_in],
                [])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_rebalance_out_and_server_groups(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        self._create_server_groups()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_dropped_replica_add_new_node_with_server_group(self):
        # Remove the last node from the cluster
        node_out = self.servers[self.nodes_init - 1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        nodes = self._get_node_list()
        self.log.info(nodes)

        # Default source zone
        zones = list(self.rest.get_zone_names().keys())
        source_zone = zones[0]
        self._create_server_groups()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance1 = self.get_index_map()
        stats_map_after_rebalance1 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance1,
                stats_map_before_rebalance,
                stats_map_after_rebalance1,
                [],
                [node_out])
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

        self.rest.add_zone("server_group_3")

        # Add back the node that was recently removed.
        node_in = [self.servers[self.node_out]]
        rebalance1 = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            node_in, [],
            services=["index"])

        # Add back the node that was previously removed.
        node_in = [self.servers[self.nodes_init - 1]]
        rebalance2 = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            node_in, [],
            services=["index"])

        # Add nodes to server groups
        self.rest.shuffle_nodes_in_zones([self.servers[self.node_out].ip],
                                         source_zone, "server_group_1")
        self.rest.shuffle_nodes_in_zones([self.servers[3].ip],
                                         source_zone, "server_group_3")

        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        rebalance2.result()
        self.sleep(30)

        index_map_after_rebalance2 = self.get_index_map()
        stats_map_after_rebalance2 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance2,
                stats_map_before_rebalance,
                stats_map_after_rebalance2,
                [node_in],
                [])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_rebalance_out_and_equivalent_index(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        eq_index_node = self.servers[int(self.eq_index_node)].ip + ":" + \
                        self.node_port

        # Create Equivalent Index
        equivalent_index_query = "CREATE INDEX eq_index ON default(age) USING GSI  WITH {{'nodes': '{0}'}};".format(
            eq_index_node)
        self.log.info(equivalent_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=equivalent_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_failover(self):
        nodes = self._get_node_list()
        node_out = self.servers[self.node_out]
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_failover = self.get_index_map()
        stats_map_before_failover = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_failover)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_failover,
                                                    len(nodes) - 1, nodes)

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_failover = self.get_index_map()
        stats_map_after_failover = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_failover,
                index_map_after_failover,
                stats_map_before_failover,
                stats_map_after_failover,
                [],
                [node_out], indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post failover : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_failover_and_addback(self):
        nodes = self._get_node_list()
        node_out = self.servers[self.node_out]
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_failover = self.get_index_map()
        stats_map_before_failover = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_failover)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_failover,
                                                    len(nodes) - 1, nodes)

        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        self.sleep(60)

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_failover = self.get_index_map()
        stats_map_after_failover = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_failover,
                index_map_after_failover,
                stats_map_before_failover,
                stats_map_after_failover,
                [],
                [])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post failover : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_rebalance_out_with_replica_with_concurrent_querying(self):
        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()

        # start querying
        t1 = Thread(target=self.run_async_index_operations,
                    args=("query",))
        t1.start()
        # rebalance out a indexer node when querying is in progress
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [index_server])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        try:
            self.n1ql_helper.verify_indexes_redistributed(
                map_before_rebalance,
                map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [index_server], indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

        self.run_operation(phase="after")

    def test_failure_in_rebalance(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        try:
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [node_out])

            remote = RemoteMachineShellConnection(
                self.servers[self.failure_in_node])
            remote.stop_server()
            self.sleep(30)
            remote.start_server()
            rebalance.result()

        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(
                    ex):
                self.fail(
                    "rebalance failed with some unexpected error : {0}".format(
                        str(ex)))
        else:
            self.fail("rebalance did not fail")

        self.sleep(60)
        index_servers = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        # run a /cleanupRebalance after a rebalance failure
        for index_server in index_servers:
            output = self.rest.cleanup_indexer_rebalance(server=index_server)
            self.log.info(output)
        self.log.info("Retrying rebalance")
        rebalance1 = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                  [], [node_out])
        self.sleep(30)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        self.sleep(30)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_failover_with_replica_with_concurrent_querying(self):
        index_server = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=False)
        self.run_operation(phase="before")
        self.sleep(30)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()

        # start querying
        t1 = Thread(target=self.run_async_index_operations,
                    args=("query",))
        t1.start()
        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [node_out], self.graceful,
            wait_for_pending=180)
        failover_task.result()
        t1.join()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        try:
            self.n1ql_helper.verify_indexes_redistributed(
                map_before_rebalance,
                map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [index_server], indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

        self.run_operation(phase="after")

    def test_load_balancing_amongst_replicas(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        select_query = "SELECT count(age) from default"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        # Run select query 100 times
        for i in range(0, 100):
            self.n1ql_helper.run_cbq_query(query=select_query,
                                           server=self.n1ql_node)

        index_stats = self.get_index_stats(perNode=True)

        load_balanced = True
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            try:
                num_request_served = index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
            except Exception as err:
                if 'KeyError:' in str(err):
                    hostname = hostname.replace('18091', '8091')
                    num_request_served = index_stats[hostname]['default'][index_name][
                        "num_completed_requests"]
                else:
                    self.fail(err)
            self.log.info("# Requests served by %s = %s" % (
                index_name, num_request_served))
            if num_request_served == 0:
                load_balanced = False

        if not load_balanced:
            self.fail("Load is not balanced amongst index replicas")

    def test_load_balancing_with_use_index_clause(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        select_query = "SELECT count(age) from default USE INDEX ({0} USING GSI) where age > 10".format(
            index_name_prefix)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        # Run select query 100 times
        for i in range(0, 100):
            self.n1ql_helper.run_cbq_query(query=select_query,
                                           server=self.n1ql_node)

        index_stats = self.get_index_stats(perNode=True)

        load_balanced = True
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            try:
                num_request_served = index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
            except Exception as err:
                if 'KeyError:' in str(err):
                    hostname = hostname.replace('18091', '8091')
                    num_request_served = index_stats[hostname]['default'][index_name][
                        "num_completed_requests"]
                else:
                    self.fail(err)
            self.log.info("# Requests served by %s = %s" % (
                index_name, num_request_served))
            if num_request_served == 0:
                load_balanced = False

        if not load_balanced:
            self.fail("Load is not balanced amongst index replicas")

    def test_load_balancing_with_prepared_statements(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        prepared_statement = "PREPARE prep_stmt AS SELECT count(age) from default USE INDEX ({0} USING GSI) where age > 10".format(
            index_name_prefix)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        prepared_statement = "PREPARE prep_stmt AS SELECT count(age) from default USE INDEX ({0} USING GSI) where age > 10".format(
            index_name_prefix)
        self.n1ql_helper.run_cbq_query(query=prepared_statement,
                                       server=self.n1ql_node)
        execute_prepared_query = "EXECUTE prep_stmt"

        # Run select query 100 times
        for i in range(0, 100):
            self.n1ql_helper.run_cbq_query(query=execute_prepared_query,
                                           server=self.n1ql_node)

        index_stats = self.get_index_stats(perNode=True)

        load_balanced = True
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            try:
                num_request_served = index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
            except Exception as err:
                if 'KeyError:' in str(err):
                    hostname = hostname.replace('18091', '8091')
                    num_request_served = index_stats[hostname]['default'][index_name][
                        "num_completed_requests"]
                else:
                    self.fail(err)
            self.log.info("# Requests served by %s = %s" % (
                index_name, num_request_served))
            if num_request_served == 0:
                load_balanced = False

        if not load_balanced:
            self.fail("Load is not balanced amongst index replicas")

    def test_move_index(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)
        dest_nodes = self._get_node_list(self.dest_node)
        self.log.info(dest_nodes)
        expect_failure = False
        if self.expected_err_msg:
            expect_failure = True

        output, error = self._cbindex_move(src_node=self.servers[0],
                                           node_list=dest_nodes,
                                           index_list=index_name_prefix,
                                           expect_failure=expect_failure,
                                           alter_index=self.alter_index)
        self.sleep(30)
        if self.expected_err_msg:
            if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error")
        else:
            index_map = self.get_index_map()
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(dest_nodes) - 1,
                                                    dest_nodes)

    def test_move_index_failed_node(self):
        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful)

        failover_task.result()
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        dest_nodes = self._get_node_list(self.dest_node)
        self.log.info(dest_nodes)
        expect_failure = False
        if self.expected_err_msg:
            expect_failure = True

        output, error = self._cbindex_move(src_node=self.servers[0],
                                           node_list=dest_nodes,
                                           index_list=index_name_prefix,
                                           expect_failure=expect_failure, alter_index=self.alter_index)
        self.sleep(30)
        if self.expected_err_msg:
            if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error")
            else:
                index_map = self.get_index_map()
                self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                        index_map,
                                                        len(dest_nodes) - 1,
                                                        dest_nodes)

    def test_dest_node_fails_during_move_index(self):
        node_out = self.servers[self.node_out]

        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        dest_nodes = self._get_node_list(self.dest_node)
        self.log.info(dest_nodes)
        expect_failure = False
        if self.expected_err_msg:
            expect_failure = True

        threads = []
        if self.alter_index:
            alter_index_query = 'ALTER INDEX default.' + index_name_prefix + ' WITH {{"action":"move","nodes": ["{0}","{1}"]}}'.format(
                dest_nodes[0], dest_nodes[1])
            threads.append(
                Thread(target=self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node),
                       name="alter_index",
                       args=(self.servers[0], dest_nodes, index_name_prefix,
                             expect_failure)))
        else:
            threads.append(
                Thread(target=self._cbindex_move, name="move_index",
                       args=(self.servers[0], dest_nodes, index_name_prefix,
                             expect_failure)))
        threads.append(
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful)))

        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.sleep(30)

        except Exception as ex:
            self.log.info("***** Exception : %s", str(ex))

    def test_index_metadata_replicated(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)

        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map,
                                                self.num_index_replica)

        index_metadata = None
        for i in range(0, self.nodes_init - 1):
            node_index_metadata = RestConnection(
                self.servers[i]).get_indexer_metadata()
            self.log.info("Index metadata for %s : %s" % (
                self.servers[i].ip, node_index_metadata))
            if index_metadata:
                if node_index_metadata != index_metadata:
                    self.fail("Index metadata not replicated properly")
            else:
                index_metadata = node_index_metadata

    def test_replica_for_different_index_types(self):
        self.run_operation(phase="before")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_names = self.n1ql_helper.get_index_names()

        for index_name in index_names:
            self.n1ql_helper.verify_replica_indexes([index_name], index_map,
                                                    self.num_index_replica)
        self.run_operation(phase="after")

    def test_replica_for_primary_index(self):
        create_index_query = "CREATE PRIMARY INDEX primary_index on default USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(["primary_index"],
                                                    index_map,
                                                    self.num_index_replica)

    def test_replica_for_dynamic_index(self):
        create_index_query = "CREATE INDEX dynamic ON default(DISTINCT PAIRS({{name, age}})) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes(["dynamic"],
                                                    index_map,
                                                    self.num_index_replica)

    def test_rollback_to_zero_with_replicas(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        self.cluster.bucket_flush(self.master)
        self.sleep(60)

        index_stats = self.get_index_stats(perNode=True)

        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            try:
                num_docs_processed = index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
            except Exception as err:
                if 'KeyError:' in str(err):
                    hostname = hostname.replace('18091', '8091')
                    num_docs_processed = index_stats[hostname]['default'][index_name][
                        "num_completed_requests"]
                else:
                    self.fail(err)
            self.log.info("# Docs processed by %s = %s" % (
                index_name, num_docs_processed))
            if num_docs_processed != 0:
                self.fail("Rollback to zero fails")

    def test_partial_rollback_with_replicas(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA & NodeB")
        mem_client = MemcachedClientHelper.direct_client(self.servers[0],
                                                         "default")
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self.servers[1],
                                                         "default")
        mem_client.stop_persistence()

        self.run_doc_ops()

        self.sleep(10)

        # Get count before rollback
        bucket_count_before_rollback = self.get_item_count(self.servers[0],
                                                           "default")
        self.log.info("# Items in bucket before rollback = %s",
                      bucket_count_before_rollback)
        index_stats = self.get_index_stats(perNode=True)
        num_docs_processed_before_rollback = {}

        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            try:
                num_docs_processed_before_rollback[index_name] = \
                    index_stats[hostname]['default'][index_name][
                        "items_count"]
            except Exception as err:
                if 'KeyError:' in str(err):
                    hostname = hostname.replace('18091', '8091')
                    num_docs_processed_before_rollback[index_name] = \
                        index_stats[hostname]['default'][index_name][
                            "items_count"]
                else:
                    self.fail(err)

            self.log.info("# Before Rollback Docs processed by %s = %s" % (
                index_name, num_docs_processed_before_rollback[index_name]))

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on NodeB")
        mem_client = MemcachedClientHelper.direct_client(
            self.input.servers[1], "default")
        mem_client.start_persistence()

        # Failover Node B
        self.log.info("Failing over NodeB")
        self.sleep(10)
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [self.servers[1]], self.graceful,
            wait_for_pending=120)

        failover_task.result()

        # Wait for a couple of mins to allow rollback to complete
        # self.sleep(120)

        # Get count after rollback
        self.collection_stats = CollectionsStats(self.servers[0])
        cbo_doc_items = self.collection_stats.get_collection_item_count_cumulative("default",
                                                                                   CbServer.system_scope,
                                                                                   CbServer.query_collection)
        bucket_count_after_rollback = self.get_item_count(self.servers[0],"default") - cbo_doc_items

        self.log.info("# Items in bucket after rollback = %s",
                      bucket_count_after_rollback)
        index_stats = self.get_index_stats(perNode=True)
        num_docs_processed_after_rollback = {}

        if bucket_count_before_rollback == bucket_count_after_rollback:
            self.log.info("Looks like KV rollback did not happen at all.")

        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            num_docs_processed_after_rollback[index_name] = \
                index_stats[hostname]['default'][index_name][
                    "items_count"]
            self.log.info("# After rollback Docs processed by %s = %s" % (
                index_name, num_docs_processed_after_rollback[index_name]))
            if num_docs_processed_after_rollback[
                index_name] != bucket_count_after_rollback:
                self.fail("# items in index do not match # items in bucket")

    def test_backup_restore_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map_before_backup = self.get_index_map()
        self.log.info(index_map_before_backup)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_backup,
                                                    self.num_index_replica)

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)
        self._create_backup(kv_node)
        self._create_restore(kv_node)

        index_map_after_restore = self.get_index_map()
        self.log.info(index_map_after_restore)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_after_restore,
                                                    self.num_index_replica)

    def test_backup_restore_with_replica_one_node_less(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map_before_backup = self.get_index_map()
        self.log.info(index_map_before_backup)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_backup,
                                                    self.num_index_replica)

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)
        self._create_backup(kv_node)

        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        self._create_restore(kv_node)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out], indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_backup_restore_add_back_dropped_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map_before_backup = self.get_index_map()
        self.log.info(index_map_before_backup)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_backup,
                                                    self.num_index_replica)

        # Rebalance out one node so that one index replica drops
        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        # Take a backup of the cluster
        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)
        self._create_backup(kv_node)

        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        # Add back the node to the cluster
        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [node_out], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        # Restore from the backup
        self._create_restore(kv_node)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [node_out],
                [])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_backup_restore_with_server_groups(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        self._create_server_groups()
        self.sleep(5)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map = self.get_index_map()

        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)

        self._create_backup(kv_node)
        self._create_restore(kv_node)

        index_map_after_restore = self.get_index_map()
        self.log.info(index_map_after_restore)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_after_restore,
                                                    self.num_index_replica)

    def test_backup_restore_with_server_groups_one_node_less(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        self._create_server_groups()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)
        self._create_backup(kv_node)

        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        self._create_restore(kv_node)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out])
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_cbindexplan(self):
        if self.server_grouping:
            self._create_server_groups()

        if self.eq_index_node is not None:
            eq_index_node = self.servers[int(self.eq_index_node)].ip + ":" + \
                            self.node_port

            # Create Equivalent Index
            equivalent_index_query = "CREATE INDEX eq_index ON default(age) USING GSI  WITH {{'nodes': '{0}'}};".format(
                eq_index_node)
            self.log.info(equivalent_index_query)
            try:
                self.n1ql_helper.run_cbq_query(query=equivalent_index_query,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                self.fail("Index creation Failed : %s", str(ex))

        output, error, json = self._cbindexplan_plan(self.servers[0],
                                                     self.num_index_replica,
                                                     "age")

        if error:
            self.fail("cbindexplan errored out")
        else:
            expected_node_list = None
            if self.expected_nodes:
                expected_node_list = self._get_node_list(self.expected_nodes)
            if json:
                self._validate_cbindexplan_result(json, "plan",
                                                  expected_node_list)

    def test_failover_with_replica_with_concurrent_querying_using_use_index(self):
        self.run_operation(phase="before")
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # start querying using USE index
        t1 = Thread(target=self._run_use_index,
                    args=(index_name_prefix, 500))
        t1.start()
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [index_server], self.graceful,
            wait_for_pending=180)
        failover_task.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [index_server])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        try:
            self.n1ql_helper.verify_indexes_redistributed(
                map_before_rebalance,
                map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [index_server])
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))
        self.run_operation(phase="after")

    def test_failover_with_replica_with_concurrent_querying_using_prepare_statement(self):
        self.run_operation(phase="before")
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        prepared_statement = "PREPARE prep_stmt AS SELECT count(age) from default USE INDEX ({0} USING GSI) where age > " \
                             "10".format(index_name_prefix)
        self.n1ql_helper.run_cbq_query(query=prepared_statement,
                                       server=self.n1ql_node)
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # start querying using prepared statement
        t1 = Thread(target=self._run_prepare_statement,
                    args=("prep_stmt", 500))
        t1.start()
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [index_server], self.graceful,
            wait_for_pending=180)
        failover_task.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [index_server])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        try:
            self.n1ql_helper.verify_indexes_redistributed(
                map_before_rebalance,
                map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [index_server],
                indexes_changed=True)
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))
        self.run_operation(phase="after")

    def test_prepare_statement_on_failedover_equivalent_index_with_replicas(self):
        self.run_operation(phase="before")
        eq_index_node = self.servers[int(self.eq_index_node)].ip + ":" + \
                        self.node_port

        # Create Equivalent Index
        equivalent_index_query = "CREATE INDEX eq_index ON default(age) USING GSI  WITH {{'nodes': '{0}'}};".format(
            eq_index_node)
        self.log.info(equivalent_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=equivalent_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        # Create prepare statement on Equivalent Index
        prepared_statement = "PREPARE prep_stmt AS SELECT count(age) from default USE INDEX (eq_index USING GSI) where age > " \
                             "10"
        self.n1ql_helper.run_cbq_query(query=prepared_statement,
                                       server=self.n1ql_node)
        self.sleep(30)

        # Create replicas
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        # start querying using prepared statement
        t1 = Thread(target=self._run_prepare_statement,
                    args=("prep_stmt", 500))
        t1.start()
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [self.servers[int(self.eq_index_node)]], self.graceful,
            wait_for_pending=180)
        failover_task.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [self.servers[int(self.eq_index_node)]])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        t1.join()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        # validate the results
        try:
            self.n1ql_helper.verify_indexes_redistributed(
                map_before_rebalance,
                map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [self.servers[int(self.eq_index_node)]])
        except Exception as ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))
        self.run_operation(phase="after")

    def test_create_replica_index_with_num_replica_using_cbindex_create(self):
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        self._cbindex_create(index_server, index_name_prefix, "name", num_replica=2)
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    2)

    def test_load_balancing_amongst_replicas_drop_replica_and_add_back(self):
        node_out = self.servers[self.node_out]
        hash = {}
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        select_query = "SELECT count(age) from default"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        # Run select query 100 times
        for i in range(0, 100):
            self.n1ql_helper.run_cbq_query(query=select_query,
                                           server=self.n1ql_node)

        index_stats = self.get_index_stats(perNode=True)

        load_balanced = True
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            try:
                num_request_served = index_stats[hostname]['default'][index_name]["num_completed_requests"]
            except Exception as err:
                if 'KeyError:' in str(err):
                    hostname = hostname.replace('18091', '8091')
                    num_request_served = index_stats[hostname]['default'][index_name]["num_completed_requests"]
                else:
                    self.fail(err)
            self.log.info("# Requests served by %s = %s" % (
                index_name, num_request_served))
            hash["index_name"] = num_request_served
            if num_request_served == 0:
                load_balanced = False

        if not load_balanced:
            self.fail("Load is not balanced amongst index replicas")

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        # Run select query 100 times
        for i in range(0, 100):
            self.n1ql_helper.run_cbq_query(query=select_query,
                                           server=self.n1ql_node)

        index_stats = self.get_index_stats(perNode=True)
        index_map = self.get_index_map()

        load_balanced = True
        count = 0
        for i in range(0, self.num_index_replica + 1):
            try:
                if i == 0:
                    index_name = index_name_prefix
                else:
                    index_name = index_name_prefix + " (replica {0})".format(str(i))
                hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                    index_name, index_map)
                num_request_served = index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
                self.log.info("# Requests served by %s = %s" % (
                    index_name, num_request_served))
                if num_request_served > hash["index_name"]:
                    count += 1
                hash["index_name"] = num_request_served
                if num_request_served == 0:
                    load_balanced = False
            except Exception as e:
                self.log.info("snapshot doesn't exist")
        if not load_balanced and count != 2:
            self.fail("Load is not balanced amongst index replicas")

        self.sleep(30)
        self.servers[:self.nodes_init].pop(self.node_out)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [node_out], [], services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(100)
        # Run select query 100 times
        for i in range(0, 100):
            self.n1ql_helper.run_cbq_query(query=select_query,
                                           server=self.n1ql_node)

        index_stats = self.get_index_stats(perNode=True)
        index_map = self.get_index_map()

        load_balanced = True
        count = 0
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            num_request_served = index_stats[hostname]['default'][index_name][
                "num_completed_requests"]
            self.log.info("# Requests served by %s = %s" % (
                index_name, num_request_served))
            if num_request_served > hash["index_name"]:
                count += 1
            hash["index_name"] = num_request_served
            if num_request_served == 0:
                load_balanced = False

        if not load_balanced and count != 3:
            self.fail("Load is not balanced amongst index replicas")

    # Testcase for MB-23778
    def test_load_balancing_with_replica_with_concurrent_querying_and_failover(
            self):
        hash_before = {}
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_index_replica)
        select_query = "SELECT count(age) from default"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_index_replica)

        # start querying
        t1 = Thread(target=self._run_use_index,
                    args=(index_name_prefix, 100))
        t1.start()
        self.sleep(5)

        # Get num_requests_completed per index before failover
        self.log.info("======BEFORE=====")
        index_stats = self.get_index_stats(perNode=True)
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(
                    str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            num_request_served = \
                index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
            self.log.info("# Requests served by %s = %s" % (
                index_name, num_request_served))
            hash_before[index_name] = num_request_served

        # Kill indexer process on one node
        node_out = self.servers[self.node_out]
        self._kill_all_processes_index(node_out)

        self.sleep(10)
        t1.join()

        # Get num_requests_completed per index after failover
        self.sleep(10)
        self.log.info("======AFTER=====")
        hash_after = {}
        index_stats = self.get_index_stats(perNode=True)
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(
                    str(i))

            hostname, _ = self.n1ql_helper.get_index_details_using_index_name(
                index_name, index_map)
            num_request_served = \
                index_stats[hostname]['default'][index_name][
                    "num_completed_requests"]
            self.log.info("# Requests served by %s = %s" % (
                index_name, num_request_served))
            hash_after[index_name] = num_request_served

        # Validate
        load_balanced = True
        for i in range(0, self.num_index_replica + 1):
            if i == 0:
                index_name = index_name_prefix
            else:
                index_name = index_name_prefix + " (replica {0})".format(
                    str(i))

            if hash_after[index_name] <= hash_before[index_name]:
                load_balanced = False

        if not load_balanced:
            self.fail("Load balancing not proper after failover")

    def test_alter_index_with_prepared_statements(self):
        nodes = self._get_node_list()
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")

        # Create prepare statement on index
        prepared_statement = "PREPARE prep_stmt AS SELECT count(age) from default USE INDEX ({0} USING GSI) where age > " \
                             "10".format(index_name_prefix)
        self.n1ql_helper.run_cbq_query(query=prepared_statement,
                                       server=self.n1ql_node)
        self.sleep(30)

        result_before = \
            self.n1ql_helper.run_cbq_query(query="prep_stmt", is_prepared=True,
                                           server=self.n1ql_server)['results']

        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)
        dest_nodes = self._get_node_list(self.dest_node)
        self.log.info(dest_nodes)
        expect_failure = False
        if self.expected_err_msg:
            expect_failure = True

        output, error = self._cbindex_move(src_node=self.servers[0],
                                           node_list=dest_nodes,
                                           index_list=index_name_prefix,
                                           expect_failure=expect_failure,
                                           alter_index=self.alter_index)
        self.sleep(30)
        if self.expected_err_msg:
            if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error")
        else:
            index_map = self.get_index_map()
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(dest_nodes) - 1,
                                                    dest_nodes)

        result_after = self.n1ql_helper.run_cbq_query(query="prep_stmt",
                                                      is_prepared=True,
                                                      server=self.n1ql_server)[
            'results']

        msg = "Query result with prepare and without doesn't match.\nBefore move index: %s ... %s\nAfter move index: %s ... %s"
        self.assertTrue(
            sorted(result_before) == sorted(result_after),
            msg % (result_before[:100], result_before[-100:],
                   result_after[:100], result_after[-100:]))

    def _get_node_list(self, node_list=None):
        # 1. Parse node string
        if node_list:
            src_nodes = node_list
        else:
            src_nodes = self.nodes
        nodes = []
        invalid_ip = "10.111.151.256"
        if src_nodes:
            nodes = src_nodes.split(":")
            for i in range(0, len(nodes)):
                if nodes[i] not in ("empty", "invalid"):
                    nodes[i] = self.servers[int(nodes[i])].ip + ":" + \
                               self.node_port
                elif nodes[i] == "invalid":
                    nodes[i] = invalid_ip + ":" + self.node_port
                elif nodes[i] == "empty":
                    nodes[i] = ""
        else:
            self.log.info("No nodes in list")

        return nodes

    def run_operation(self, phase="before"):
        if phase == "before":
            self.run_async_index_operations(operation_type="create_index")
        elif phase == "during":
            self.run_async_index_operations(operation_type="query")
        else:
            self.run_async_index_operations(operation_type="query")
            self.run_async_index_operations(operation_type="drop_index")

    def _return_maps(self):
        index_map = self.get_index_map()
        stats_map = self.get_index_stats(perNode=False)
        return index_map, stats_map

    def _cbindex_move(self, src_node, node_list, index_list,
                      expect_failure=False, bucket="default",
                      username="Administrator", password="password",
                      alter_index=False):
        node_list = str(node_list).replace("\'", "\"")
        src_node_ip = src_node.ip + ":" + self.node_port
        output = None
        error = []

        if alter_index:
            alter_index_query = 'ALTER INDEX default.' + index_list + ' WITH {{"action":"move","nodes": {0}}}'.format(
                node_list)
            try:
                self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node)
            except Exception as ex:
                output = ""
                error.append(str(ex))
        else:
            cmd = """cbindex -type move -server '{0}' -auth '{4}:{5}' -index '{1}' -bucket {2} -with '{{"nodes":{3}}}'""".format(
                src_node_ip,
                index_list,
                bucket,
                str(node_list),
                username,
                password)
            self.log.info(cmd)
            remote_client = RemoteMachineShellConnection(src_node)
            command = "{0}/{1}".format(self.cli_command_location, cmd)
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)

        if error:
            if expect_failure:
                self.log.info("cbindex move failed as expected")
                self.log.info("Output : %s", output)
                self.log.info("Error : %s", error)
            else:
                self.log.info("Output : %s", output)
                self.log.info("Error : %s", error)
                self.fail("cbindex move failed")
        else:
            self.log.info(
                "cbindex move started successfully : {0}".format(output))
        return output, error

    def _cbindex_create(self, node, index_name, fields, bucket="default", username="Administrator", password="password",
                        num_replica=1):
        cmd = """cbindex -type=create -auth '{0}:{1}' -bucket {2}  -index {3} -fields={4} -using gsi -with '{{"num_replica":{5}}}'""".format(
            username, password, bucket, index_name, fields, num_replica)
        self.log.info(cmd)
        remote_client = RemoteMachineShellConnection(node)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            self.fail("cbindex create failed")
        else:
            self.log.info("cbindex create started successfully : {0}".format(output))
        return output, error

    def _cbindexplan_plan(self, src_node, num_replica, field, bucket="default",
                          username="Administrator", password="password"):
        expect_failure = self.input.param("expect_failure", False)
        return_val = None

        idx_json = self._generate_index_json_for_cbindex_plan(num_replica,
                                                              bucket, field)
        src_node_ip = src_node.ip + ":" + self.node_port
        cmd = """cbindexplan -command=plan -indexes='{0}/index.json' -cluster='{1}' -username='{2}' -password='{3}' -output='plan.json'""".format(
            self.cli_command_location, src_node_ip, username, password)
        self.log.info(cmd)
        remote_client = RemoteMachineShellConnection(src_node)
        command = """echo -e "{0}" > {1}/index.json""".format(
            idx_json.replace("\"", "\\x22").replace("\'", "\\x22"),
            self.cli_command_location)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            self.fail("failure creating index.json")

        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error or "Error" in str(output) or "Fatal" in str(output):
            if expect_failure:
                self.log.info("cbindexplan failed as expected")
            else:
                self.log.info("Output : %s", output)
                self.log.info("Error : %s", error)
                self.fail("cbindexplan move failed")
        else:
            command = "cat plan.json"
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)
            if error:
                self.fail("failure accessing plan.json")
            else:
                return_val = json.loads(''.join(output))
        return output, error, return_val

    def _validate_cbindexplan_result(self, result, mode="plan",
                                     expected_node_list=None):
        if mode == "plan":
            # Validate if the placement recommendations is for all replicas
            if "placement" in list(result.keys()):
                num_recommended_nodes = 0
                actual_node_list = []
                for node in result["placement"]:
                    if node["indexes"] != []:
                        num_recommended_nodes += 1
                        actual_node_list.append(str(node["nodeId"]))

                expected_num_recommended_nodes = self.num_index_replica + 1
                if self.eq_index_node:
                    expected_num_recommended_nodes += 1
                    if expected_node_list:
                        expected_node_list.append(self.servers[
                                                      int(self.eq_index_node)].ip + ":" + \
                                                  self.node_port)

                self.assertEqual(expected_num_recommended_nodes, num_recommended_nodes,
                                 "cbindexplan doesnt give recommendations for all replicas")

                # Validate if there is an expected node list, the placement recommendation matches it
                if expected_node_list:
                    actual_node_list.sort()
                    expected_node_list.sort()
                    self.assertListEqual(actual_node_list, expected_node_list,
                                         "Placement recommendation not matching expected node list")

    def _generate_index_json_for_cbindex_plan(self, num_replica, bucket, field):
        idx_json = {}
        idx_json['name'] = bucket + '_idx'
        idx_json['bucket'] = bucket
        idx_json['isPrimary'] = False
        idx_json['secExprs'] = [field]
        idx_json['isArrayIndex'] = False
        idx_json['replica'] = int(num_replica) + 1
        idx_json['numDoc'] = 1000
        idx_json['DocKeySize'] = 200
        idx_json['SecKeySize'] = 200
        idx_json['ArrKeySize'] = 0
        idx_json['ArrSize'] = 0
        idx_json['MutationRate'] = 0
        idx_json['ScanRate'] = 0
        idx_json = [idx_json]
        # return str(idx_json).replace("\'","\x22").replace("False", "false").replace("\"","\x22")
        return json.dumps(idx_json, ensure_ascii=True).replace("\"", "\x22")

    def _create_backup(self, server, username="Administrator",
                       password="password"):
        remote_client = RemoteMachineShellConnection(server)

        command = "rm -rf {0}".format(self.backup_path)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)

        command = self.cli_command_location + "cbbackupmgr config --archive {0} --repo example{1}".format(
            self.backup_path, self.rand)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not [x for x in output if 'created successfully in archive' in x]:
            self.fail("cbbackupmgr config failed")
        if self.use_https:
            cmd = "cbbackupmgr backup --archive {0} --repo example{1} --cluster https://127.0.0.1:18091 --username {2} --password {3} --no-ssl-verify".format(
                self.backup_path, self.rand, username, password)
        else:
            cmd = "cbbackupmgr backup --archive {0} --repo example{1} --cluster couchbase://127.0.0.1 --username {2} --password {3}".format(
                self.backup_path, self.rand, username, password)
        command = "{0}{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error and not [x for x in output if
                          'Backup successfully completed' in x or 'Backup completed successfully' in x]:
            self.fail("cbbackupmgr backup failed")
        elif not [x for x in output if 'Backup successfully completed' in x or 'Backup completed successfully' in x]:
            self.log.error(output)
            raise Exception("cbbackupmgr backup failed")

    def _create_restore(self, server, username="Administrator",
                        password="password"):
        remote_client = RemoteMachineShellConnection(server)
        if self.use_https:
            cmd = "cbbackupmgr restore --archive {0} --repo example{1} --cluster https://127.0.0.1:18091 --username {2} --password {3} --force-updates --no-ssl-verify".format(
                self.backup_path, self.rand, username, password)
        else:
            cmd = "cbbackupmgr restore --archive {0} --repo example{1} --cluster couchbase://127.0.0.1 --username {2} --password {3} --force-updates".format(
                self.backup_path, self.rand, username, password)
        command = "{0}{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)

        if error and not [x for x in output if 'Restore completed successfully' in x]:
            self.fail("cbbackupmgr restore failed")
        elif not [x for x in output if 'Restore completed successfully' in x]:
            self.fail("cbbackupmgr restore failed")
        else:
            command = "rm -rf {0}".format(self.backup_path)
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)

    def _run_use_index(self, index, count=10):
        select_query = "SELECT count(age) from default USE INDEX ({0} USING GSI) where age > 10".format(index)
        for i in range(0, count):
            try:
                self.n1ql_helper.run_cbq_query(query=select_query,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                raise Exception("query with USE INDEX failed")
            self.sleep(1)

    def _run_prepare_statement(self, prepare_statement, count=10):
        prepared_query = "EXECUTE " + prepare_statement
        for i in range(0, count):
            try:
                self.n1ql_helper.run_cbq_query(query=prepared_query,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                raise Exception("query with prepared statement failed")
            self.sleep(1)

    def _kill_all_processes_index(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("killall indexer")
