"""planner_gsi.py: description

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "04/08/21 11:26 am"

"""
import json
import os
import random

from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestHelper, RestConnection
from .base_gsi import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from gsi.collections_concurrent_indexes import powerset
from lib.testconstants import \
    LINUX_COUCHBASE_BIN_PATH, LINUX_NONROOT_CB_BIN_PATH, \
    WIN_COUCHBASE_BIN_PATH

class PlannerGSI(BaseSecondaryIndexingTests):
    def setUp(self):
        super(PlannerGSI, self).setUp()
        self.log.info("==============  PlannerGSI setup has started ==============")
        self.enable_gsi_planner = self.input.param("enable_gsi_planner", True)
        if not self.enable_gsi_planner:
            self.index_rest.set_index_settings("indexer.planner.useGreedyPlanner", False)
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.new_index_nodes = self.input.param('new_index_nodes', 1)
        self.new_indexes = self.input.param('new_indexes', 1)
        self.initial_index_num = self.input.param('initial_index_num', 1)
        self.num_indexes_to_delete = self.input.param('num_indexes_to_delete', 0)
        self.index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                         'suffix', 'filler1', 'phone', 'zipcode'])
        self.log.info("==============  PlannerGSI setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  PlannerGSI tearDown has started ==============")
        super(PlannerGSI, self).tearDown()
        self.log.info("==============  PlannerGSI tearDown has completed ==============")

    def _find_least_loaded_index_node(self, count=1):
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        remote = RemoteMachineShellConnection(index_node)
        output_file = '/tmp/index_plan.log'
        dest_file = 'index_plan.log'
        del_cmd = f'rm -rf {output_file}'
        self.log.info("Deleting index_plan.log from Remote host")
        remote.execute_command(del_cmd)
        if self.use_https:
            port = '18091'
        else:
            port = '8091'
        bin_path = os.path.join(LINUX_COUCHBASE_BIN_PATH,'cbindexplan')
        if self.shell.extract_remote_info().type.lower() == 'linux':
            if self.nonroot:
                bin_path = os.path.join(LINUX_NONROOT_CB_BIN_PATH,'cbindexplan')
        elif self.shell.extract_remote_info().type.lower() == 'windows':
            bin_path = os.path.join(WIN_COUCHBASE_BIN_PATH,'cbindexplan')
        cmd = f'{bin_path}  -command=retrieve -cluster="127.0.0.1:{port}" ' \
              f'-username="Administrator" -password="password" -getUsage -numNewReplica {count}' \
              f' -output {output_file}'
        remote.execute_command(cmd)
        if os.path.exists(dest_file):
            self.log.info("Deleting index_plan.log from slave")
            os.remove(dest_file)
        remote.copy_file_remote_to_local(rem_path=output_file, des_path=dest_file)

        # cleaning file on remote host
        self.log.info("Deleting index_plan.log from Remote host after copying")
        remote.execute_command(del_cmd)

        if os.path.exists(dest_file):
            fh = open(dest_file)
            json_obj = json.load(fh)
            index_loads = {}

            for index in json_obj["placement"]:
                curr_index_load = index['usageRatio']
                index_node = index['nodeId'].split(':')[0]
                index_loads[index_node] = curr_index_load

            sorted_indexer_nodes = sorted(index_loads.items(), key=lambda x: x[1])
            ll_nodes_list = []
            for item in range(count):
                ll_nodes_list.append(sorted_indexer_nodes[item][0])
            return ll_nodes_list

        self.fail("Couldn't copy cbindexplan output to local directory")

    def test_new_index_placement_by_greedy_planner(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Need at least 3 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                      num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        least_loaded_node = self._find_least_loaded_index_node()[0]

        # creating new index and checking where does planner place it
        idx_new = 'idx_new'
        new_index_gen = QueryDefinition(index_name=idx_new, index_fields=['age'])
        query = new_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query=query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            if index['indexName'] == idx_new:
                host = index['hosts'][0].split(':')[0]
                self.assertEqual(host, least_loaded_node, "Index not created on Least Loaded Index node")
                break
        else:
            self.fail("new Index stats not available in index_metadata")

    def test_placement_for_new_index_with_replica(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Need at least 3 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                      num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        self.log.info(index_metadata)

        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                           num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        least_loaded_nodes = self._find_least_loaded_index_node(count=2)

        # creating new index and checking where does planner place it
        idx_new = 'idx_new'
        new_index_gen = QueryDefinition(index_name=idx_new, index_fields=['age'])
        query = new_index_gen.generate_index_create_query(namespace=collection_namespace, num_replica=self.num_replicas,
                                                          defer_build=self.defer_build)
        self.run_cbq_query(query=query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            if index['indexName'] == idx_new:
                host = index['hosts'][0].split(':')[0]
                self.assertTrue(host in least_loaded_nodes, "Index not created on Least Loaded Index node")

    def test_index_placement_on_new_indexer_node(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 3 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                      num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        self.sleep(10)

        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                           num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        # Adding in a new Index node
        add_nodes = self.servers[self.nodes_init:self.nodes_init + self.new_index_nodes]
        add_node_services = ['index' for _ in range(len(add_nodes))]

        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                      to_remove=[], services=add_node_services)
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        for item in range(self.new_indexes):
            least_loaded_node = self._find_least_loaded_index_node(count=2)
            # creating new index and checking where does planner place it
            idx_new = f'idx_new_{item}'
            index_fields = self.index_field_set[item % len(self.index_field_set)]
            new_index_gen = QueryDefinition(index_name=idx_new, index_fields=index_fields)
            query = new_index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              num_replica=self.num_replicas,
                                                              defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            if self.defer_build:
                build_query = index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(build_query)
            self.wait_until_indexes_online()
            self.sleep(30)

            index_metadata = self.index_rest.get_indexer_metadata()['status']
            for index in index_metadata:
                if index['indexName'] == idx_new:
                    host = index['hosts'][0].split(':')[0]
                    self.assertTrue(host in least_loaded_node, "Index not created on Least Loaded Index node")

    def test_index_placements_with_server_group(self):
        self._create_server_groups()
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                      to_remove=[], services=[])
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                      num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        self.sleep(10)

        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                           num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        index_hosts_dict = {}
        for index in indexer_metadata:
            index_name = index['indexName']
            host = index['hosts'][0].split(':')[0]
            if index_name in index_hosts_dict:
                index_hosts_dict[index_name].append(host)
            else:
                index_hosts_dict[index_name] = [host]

        zoneinfo = self.rest.get_all_zones_info()['groups']
        groupinfo = {}
        for zone in zoneinfo:
            if zone['nodes']:
                name = zone['name']
                groupinfo[name] = []
                for node in zone['nodes']:
                    host = node['hostname'].split(':')[0]
                    groupinfo[name].append(host)
        group_1, group_2 = [groupinfo[item] for item in groupinfo]
        for index in index_hosts_dict:
            hosts = index_hosts_dict[index]
            if hosts[0] in group_1 and hosts[1] in group_2:
                self.log.info("Replicas are created on different server group")
            elif hosts[0] in group_2 and hosts[1] in group_1:
                self.log.info("Replicas are created on different server group")
            else:
                self.fail("Replicas are created on the same server group")

    def test_index_placements_with_skewed_server_group(self):
        self._create_server_groups()
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                      to_remove=[], services=[])
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                      num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        self.sleep(10)

        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                           num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        index_hosts_dict = {}
        for index in indexer_metadata:
            index_name = index['indexName']
            host = index['hosts'][0].split(':')[0]
            if index_name in index_hosts_dict:
                index_hosts_dict[index_name].append(host)
            else:
                index_hosts_dict[index_name] = [host]

        hosts = [server.ip for server in self.servers[:self.nodes_init]]
        for index in index_hosts_dict:
            index_hosts = sorted(index_hosts_dict[index])
            self.assertEqual(sorted(hosts), index_hosts, "Index Placement is not matching expected value")

    def test_index_placements_with_skewed_load(self):
        is_new_index_equivalent = self.input.param("is_new_index_equivalent", False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Need at least 3 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        try:
            for item, index_fields in zip(range(self.initial_index_num), self.index_field_set):
                index_gen = QueryDefinition(index_name=f'idx_{item}', index_fields=list(index_fields))
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=self.num_replicas)
                self.run_cbq_query(query)
                if self.defer_build:
                    build_query = index_gen.generate_build_query(namespace=collection_namespace)
                    self.run_cbq_query(build_query)
                self.wait_until_indexes_online()
        except Exception as err:
            if 'Build Already In Progress' not in str(err):
                self.fail(err)
        self.sleep(10)

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        random_index_node = random.choice(index_nodes)
        random_index_node = random_index_node.ip

        # finding all indexes on selected index node
        indexes_to_be_dropped = {}
        for item in indexer_metadata:
            host = item['hosts'][0].split(':')[0]
            if random_index_node == host:
                indexes_to_be_dropped[item['indexName']] = item['replicaId']

        # Running alter index with drop_replica action
        for item in indexes_to_be_dropped:
            query = f'ALTER INDEX {item} ON {collection_namespace} WITH' \
                    f' {{"action": "drop_replica", "replicaId": {indexes_to_be_dropped[item]}}};'
            self.run_cbq_query(query=query)

        self.sleep(60, "Giving some time for clean-up after replicas drop")
        least_loaded_node = self._find_least_loaded_index_node()[0]
        self.assertEqual(random_index_node, least_loaded_node)

        # Now adding indexes on a skewed cluster
        if is_new_index_equivalent:
            new_index_set = self.index_field_set
        else:
            new_index_set = self.index_field_set[self.initial_index_num:]
        for item, index_fields in zip(range(len(indexes_to_be_dropped)),
                                      new_index_set):
            index_fields = [f"`{field}`" for field in index_fields]
            least_loaded_node = self._find_least_loaded_index_node(count=self.num_replicas+1)
            index_name = f'new_idx_{item}'
            new_index_gen = QueryDefinition(index_name=index_name, index_fields=list(index_fields))
            query = new_index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=self.num_replicas)
            self.run_cbq_query(query)
            if self.defer_build:
                build_query = new_index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(build_query)
            self.wait_until_indexes_online()

            indexer_metadata = self.index_rest.get_indexer_metadata()['status']
            index_hosts = []
            for index in indexer_metadata:
                if index_name == index['indexName']:
                    index_host = index['hosts'][0].split(':')[0]
                    index_hosts.append(index_host)
            if not set(index_hosts) == set(least_loaded_node):
                node_miss = list(set(least_loaded_node) - set(index_hosts))[0]
                if is_new_index_equivalent:
                    # Checking if node miss has equivalent index
                    equivalent_index_found = False
                    for index in indexer_metadata:
                        fields = index['secExprs']
                        host = index['hosts'][0].split(':')[0]
                        if list(index_fields) == fields and host == node_miss:
                            equivalent_index_found = True
                            break
                    if not equivalent_index_found:
                        self.fail("No equivalent index on least loaded node but Index was not created on it.")
                else:
                    self.fail("Index replica was not placed on least loaded node")

    def test_index_placement_scaled_up_with_deferred_indexes(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Need at least 3 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        index_names = []
        for item in range(self.initial_index_num):
            index_fields = self.index_field_set[item % len(self.index_field_set)]
            index_name = f'idx_{item}'
            index_names.append(index_name)
            index_gen = QueryDefinition(index_name=index_name, index_fields=list(index_fields))
            defer_build = random.choice([True, False])
            try:
                query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build,
                                                              num_replica=0)
                self.run_cbq_query(query)
            except Exception as err:
                if 'Build Already In Progress.' not in str(err):
                    self.fail(err)
                self.sleep(10)
            if self.defer_build:
                build_query = index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(build_query)
            self.wait_until_indexes_online(defer_build=True)
        self.sleep(10)

        # dropping some random indexes
        indexes_to_delete = []
        out = int(self.num_indexes_to_delete/self.num_replicas)
        for item in range(out):
            idx = random.choice(index_names)
            indexes_to_delete.append(idx)

            drop_idx_query = f"DROP index {idx} on {collection_namespace}"
            self.run_cbq_query(drop_idx_query)
            index_names.remove(idx)
        self.wait_until_indexes_online(defer_build=True)

        # creating new indexes to check distribution
        hits = 0
        miss = 0
        for item in range(self.new_indexes):
            least_loaded_node = self._find_least_loaded_index_node(count=2)
            # creating new index and checking where does planner place it
            idx_new = f'idx_new_{item}'
            index_fields = self.index_field_set[self.initial_index_num + item % len(self.index_field_set)]
            defer_build = random.choice([True, False])
            new_index_gen = QueryDefinition(index_name=idx_new, index_fields=index_fields)
            query = new_index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              num_replica=self.num_replicas,
                                                              defer_build=defer_build)
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online(defer_build=True)

            index_metadata = self.index_rest.get_indexer_metadata()['status']
            self.log.info(index_metadata)
            for index in index_metadata:
                if index['indexName'] == idx_new:
                    host = index['hosts'][0].split(':')[0]
                    # self.assertTrue(host in least_loaded_node, "Index not created on Least Loaded Index node")
                    if host in least_loaded_node:
                        hits += 1
                    else:
                        miss += 1
                        self.log.info(f"Least loaded nodes were: {least_loaded_node}")
                        self.log.info(f"Index not created on Least Loaded Index node. index created on host {host}")
        self.log.info(f"Hits: {hits}")
        self.log.info(f"Miss: {miss}")
        hits_percentage = hits / (hits + miss) * 100
        self.log.error("Hits percentage is lower than 80%")

    def test_index_placement_with_existing_partitioned_index(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        for item in range(self.initial_index_num):
            index_fields = list(self.index_field_set[item % len(self.index_field_set)])
            index_gen = QueryDefinition(index_name=f'idx_{item}', index_fields=index_fields,
                                        partition_by_fields=index_fields)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                          num_partition=12)
            self.run_cbq_query(query)
            if self.defer_build:
                build_query = index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(build_query)
            self.wait_until_indexes_online()
        self.sleep(10)

        # Creating non-partitioned indexes now
        for item in range(self.new_indexes):
            least_loaded_node = self._find_least_loaded_index_node()[0]
            index_fields = list(self.index_field_set[self.initial_index_num + item % len(self.index_field_set)])
            idx_new = f'new_idx_{item}'
            index_gen = QueryDefinition(index_name=idx_new, index_fields=index_fields)
            query = index_gen.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query)
            self.wait_until_indexes_online()
            self.sleep(30)

            index_metadata = self.index_rest.get_indexer_metadata()['status']
            for index in index_metadata:
                if index['indexName'] == idx_new:
                    host = index['hosts'][0].split(':')[0]
                    self.assertTrue(host in least_loaded_node, "Index not created on Least Loaded Index node")

    def test_index_placement_with_deferred_indexes(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) != 2:
            self.fail("Need 2 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        for item in range(self.initial_index_num):
            index_fields = list(self.index_field_set[item % len(self.index_field_set)])
            index_gen = QueryDefinition(index_name=f'idx_{item}', index_fields=index_fields)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query)
            if self.defer_build:
                build_query = index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(build_query)
            self.wait_until_indexes_online()
        self.sleep(10)

        # creating a deferred index
        index_fields = list(self.index_field_set[self.initial_index_num % len(self.index_field_set)])
        deferred_idx = 'deferred_idx'
        index_gen = QueryDefinition(index_name=deferred_idx, index_fields=index_fields)
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query)
        self.wait_until_indexes_online()

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        deferred_index_host = None
        for index in indexer_metadata:
            if index['indexName'] == deferred_idx:
                deferred_index_host = index['hosts'][0].split(':')[0]
                break

        # create another index and check index placement
        least_loaded_node = self._find_least_loaded_index_node()
        new_idx = 'new_idx'
        meta_index_gen = QueryDefinition(index_name=new_idx, index_fields=['meta_idx'])
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)

        self.assertTrue(deferred_index_host != least_loaded_node)
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in indexer_metadata:
            if index['indexName'] == new_idx:
                host = index['hosts'][0].split(':')[0]
                self.assertTrue(host in least_loaded_node, "Index not created on Least Loaded Index node")

    def test_index_placement_with_partitioned_deferred_index(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) != 2:
            self.fail("Need 2 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'city', 'country'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        self.sleep(10)

        # creating a deferred index
        index_fields = ['age', 'country', 'name']
        deferred_idx = 'deferred_idx'
        index_gen = QueryDefinition(index_name=deferred_idx, index_fields=index_fields,
                                    partition_by_fields=index_fields)
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=True, num_partition=5)
        self.run_cbq_query(query)
        self.wait_until_indexes_online(defer_build=True)

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        partitioned_loaded_node = None
        num_partition_on_node = 0
        for index in indexer_metadata:
            if index['partitioned']:
                for node in index['partitionMap']:
                    if num_partition_on_node < len(index['partitionMap'][node]):
                        num_partition_on_node = len(index['partitionMap'][node])
                        partitioned_loaded_node = node.split(':')[0]
                break

        self.sleep(30)
        # create another index and check index placement
        least_loaded_node = self._find_least_loaded_index_node()[0]
        new_idx = 'new_idx'
        meta_index_gen = QueryDefinition(index_name=new_idx, index_fields=['meta_idx'])
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query)

        self.assertTrue(partitioned_loaded_node != least_loaded_node)
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in indexer_metadata:
            if index['indexName'] == new_idx:
                host = index['hosts'][0].split(':')[0]
                self.assertTrue(host in least_loaded_node, "Index not created on Least Loaded Index node")

    def test_index_placement_for_equivalent_indexes(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) != 3:
            self.fail("Need 3 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        for item in range(self.initial_index_num):
            index_fields = list(self.index_field_set[item % len(self.index_field_set)])
            index_gen = QueryDefinition(index_name=f'idx_{item}', index_fields=index_fields)
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query)
            if self.defer_build:
                build_query = index_gen.generate_build_query(namespace=collection_namespace)
                self.run_cbq_query(build_query)
            self.wait_until_indexes_online()
        self.sleep(10)

        least_loaded_node = self._find_least_loaded_index_node()
        indexer_meta_data = self.index_rest.get_indexer_metadata()['status']
        equivalent_index_field = None
        for index in indexer_meta_data:
            host = index['hosts'][0].split(':')[0]
            if host in least_loaded_node:
                equivalent_index_field = index['secExprs'][0]
                break

        # creating equivalent index with one replica, so that index replicas be place on other than least loaded node
        new_idx = "new_idx"
        new_index_gen = QueryDefinition(index_name=new_idx, index_fields=[equivalent_index_field])
        query = new_index_gen.generate_index_create_query(namespace=collection_namespace,
                                                          num_replica=0)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        indexer_meta_data = self.index_rest.get_indexer_metadata()['status']
        self.log.info("Indexer metadata: %s", indexer_meta_data)
        for index in indexer_meta_data:
            if index['indexName'] == new_idx:
                host = index['hosts'][0].split(':')[0]
                self.assertTrue(host not in least_loaded_node,
                                "Equivalent index replica created on least loaded node, not maintaining HA")

    def test_alter_index_equivalent_partitioned_index(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need atleast 3 index nodes")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection, json_template=self.json_template)
        collection_namespace = self.namespaces[0]
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        for _ in range(7):
            idx_1 = QueryDefinition(index_name='idx_1', index_fields=['name'], partition_by_fields=['meta().id'])
            idx_2 = QueryDefinition(index_name='idx_2', index_fields=['name'], partition_by_fields=['meta().id'])
            query_1 = idx_1.generate_index_create_query(namespace=collection_namespace, num_replica=1, num_partition=50, deploy_node_info=deploy_nodes)
            query_2 = idx_2.generate_index_create_query(namespace=collection_namespace, num_replica=0, num_partition=50, deploy_node_info=deploy_nodes)
            for query in [query_1, query_2]:
                self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            alter_query = f'ALTER INDEX {idx_2.index_name} ON {collection_namespace} WITH' \
                        f'{{"action":"replica_count", "num_replica": 1}};'
            try:
                self.run_cbq_query(query=alter_query)
            except Exception as e:
                self.log.info(str(e))
                self.fail(str(e))

            drop_query_1 = idx_1.generate_index_drop_query(namespace=collection_namespace)
            drop_query_2 = idx_2.generate_index_drop_query(namespace=collection_namespace)
            for query in [drop_query_1, drop_query_2]:
                self.run_cbq_query(query=query)

