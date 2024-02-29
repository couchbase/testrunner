"""tenant_management.py: "This class test cluster affinity  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "08/04/22 12:27 pm"

"""
import random
import string
from concurrent.futures import ThreadPoolExecutor

from couchbase_helper.query_definitions import QueryDefinition
from gsi.base_gsi import BaseSecondaryIndexingTests
from gsi.collections_concurrent_indexes import powerset
from membase.api.on_prem_rest_client import RestHelper, RestConnection
from serverless.gsi_utils import GSIUtils
from testconstants import GSI_UNITS_HWM, GSI_UNITS_LWM, GSI_MEMORY_LWM, GSI_MEMORY_HWM
from testconstants import INDEX_MAX_CAP_PER_TENANT, INDEX_SUB_CLUSTER_LENGTH
from deepdiff import DeepDiff


class TenantManagement(BaseSecondaryIndexingTests):
    def setUp(self):
        super(TenantManagement, self).setUp()
        self.log.info("==============  TenantManagement setup has started ==============")
        self.rest.delete_all_buckets()
        self.use_defer_build = self.input.param("use_defer_build", False)
        self.index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                         'suffix', 'filler1', 'phone', 'zipcode'])
        self.batch_size = self.input.param("batch_size", 1000)
        self.node_to_swap = self.input.param("node_to_swap", 1)
        self.recover_failed_over_node = self.input.param("recover_failed_over_node", False)
        self.gsi_util_obj = GSIUtils(self.run_cbq_query)
        self.namespaces = []
        self._create_server_groups()
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)[0]
        self.create_S3_config()

    def tearDown(self):
        self.log.info("==============  TenantManagement tearDown has started ==============")
        super(TenantManagement, self).tearDown()
        self.log.info("==============  TenantManagement tearDown has completed ==============")

    def is_lwm_reached(self, node):
        return (self.get_gsi_memory_ratio(node=node) > GSI_MEMORY_LWM) or \
               (self.get_gsi_usage_ratio(node=node) > GSI_UNITS_LWM)

    def is_hwm_reached(self, node):
        return (self.get_gsi_memory_ratio(node=node) > GSI_MEMORY_HWM) or \
               (self.get_gsi_usage_ratio(node=node) > GSI_UNITS_HWM)

    def get_gsi_memory_ratio(self, node):
        if node is None:
            rest = self.index_rest
        else:
            rest = RestConnection(node)
        stats = rest.get_all_index_stats()
        ratio = stats['memory_used_actual'] / stats['memory_quota'] * 100
        self.log.info(f"Current GSI memory ratio is : {ratio}")
        return ratio

    def get_gsi_usage_ratio(self, node):
        if node is None:
            rest = self.index_rest
        else:
            rest = RestConnection(node)
        stats = rest.get_all_index_stats()
        ratio = stats['units_used_actual'] / stats['units_quota'] * 100
        self.log.info(f"Current GSI units ratio is : {ratio}")
        return ratio

    def _check_cluster_affinity(self):
        idx_host_list_dict = self.get_host_list()
        # Validating the cluster affinity
        for bucket in idx_host_list_dict:
            self.assertEqual(len(idx_host_list_dict[bucket]), 2,
                             "Indexes are hosted on more than 2 node of sub-cluster")

    def _check_index_count(self, num_indexes=None):
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        index_list = []
        for index in indexer_metadata:
            if index["indexName"] != "#primary" and index["scope"] != '_system':
                index_list.append(index)
        if num_indexes:
            num_of_indexes = num_indexes
        else:
            # (No. of indexes created by Tenant + system primary indexes) * num index replicas (1)
            indexes_per_tenant = self.num_scopes * self.num_collections * self.gsi_util_obj.batch_size
            if indexes_per_tenant > INDEX_MAX_CAP_PER_TENANT:
                indexes_per_tenant = INDEX_MAX_CAP_PER_TENANT
            num_of_indexes = self.num_of_tenants * indexes_per_tenant * 2
        self.assertEqual(len(index_list), num_of_indexes, "No. of expected indexes with replica not matching")

    def _check_index_distribution(self):
        sub_cluster_list = self.get_sub_cluster_list()
        # Validating uniform distribution across sub-clusters
        if len(self.buckets) > 1:
            self.assertTrue(len(sub_cluster_list) > 1, "Tenant are not distributed across sub-clusters")

    def _check_subcluster_rebalance_swap(self, num_of_nodes_to_remove=1):
        sub_cluster_list = self.get_sub_cluster_list()
        # identifying the node for the subcluster for different  availability zone
        remove_nodes = []
        remove_node_ips = [node.split(':')[0] for node in sub_cluster_list[0]]
        for remove_node_ip in remove_node_ips:
            for server in self.servers:
                if remove_node_ip == server.ip:
                    remove_nodes.append(server)
                    break
            if len(remove_nodes) == num_of_nodes_to_remove:
                break

        # Find Server group of removing nodes
        nodes_zone_dict = {}
        zone_info = self.rest.get_zone_and_nodes()
        for zone_name, node_list in zone_info.items():
            for node_ip in node_list:
                nodes_zone_dict[node_ip] = zone_name

        remove_nodes_zone_dict = {}
        for node in remove_nodes:
            remove_nodes_zone_dict[node.ip] = nodes_zone_dict[node.ip]

        # Adding new node to the same availability zone from where node is being removed
        # and re-balance out one other node
        for counter, server in enumerate(remove_nodes_zone_dict):
            self.rest.add_node(user=self.rest.username, password=self.rest.password,
                               remoteIp=self.servers[self.nodes_init + counter].ip,
                               zone_name=remove_nodes_zone_dict[server],
                               services=['index'])
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=[],
                                                      to_remove=remove_nodes,
                                                      services=['index'] * len(remove_nodes))
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        # Getting the new configuration of cluster
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        new_sub_cluster_list = []
        new_idx_host_list_dict = {str(bucket): set() for bucket in self.buckets}
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']

        for idx in indexer_metadata:
            idx_bucket = idx['bucket']
            idx_host = idx['hosts'][0]
            new_idx_host_list_dict[idx_bucket].add(idx_host)

        for bucket in new_idx_host_list_dict:
            host_list = sorted(list(new_idx_host_list_dict[bucket]))
            if host_list not in new_sub_cluster_list:
                new_sub_cluster_list.append(host_list)

        # validating if swap-rebalance has adhere to Tenant rules
        new_zone_info = self.rest.get_zone_and_nodes()
        for sub_cluster in new_sub_cluster_list:
            zone_1, zone_2 = None, None

            self.assertEqual(len(sub_cluster), INDEX_SUB_CLUSTER_LENGTH,
                             f"Indexes distributed on sub-cluster of more"
                             f"than 2 index nodes: {new_sub_cluster_list}")

            for node in sub_cluster:
                node_ip = node.split(':')[0]
                for server_group in new_zone_info:
                    if node_ip in new_zone_info[server_group] and zone_1 is None:
                        zone_1 = server_group
                    elif node_ip in new_zone_info[server_group] and zone_2 is None:
                        zone_2 = server_group
                    elif zone_1 and zone_2:
                        break
            self.assertNotEqual(zone_1, zone_2,
                                f"Both the index nodes belong to same availability zone. {indexer_metadata}")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def _check_failed_over_subcluster(self, num_of_nodes_to_remove=1):
        sub_cluster_list = self.get_sub_cluster_list()
        # identifying the node for the subcluster for different  availability zone
        failover_nodes = []
        failover_nodes_ips = [node.split(':')[0] for node in sub_cluster_list[0]]
        for failover_nodes_ip in failover_nodes_ips:
            for server in self.servers:
                if failover_nodes_ip == server.ip:
                    failover_nodes.append(server)
                    break
            if len(failover_nodes) == num_of_nodes_to_remove:
                break

        # Find Server group of removing nodes
        nodes_zone_dict = {}
        zone_info = self.rest.get_zone_and_nodes()
        for zone_name, node_list in zone_info.items():
            for node_ip in node_list:
                nodes_zone_dict[node_ip] = zone_name

        failover_nodes_zones_dict = {}
        for node in failover_nodes:
            failover_nodes_zones_dict[node.ip] = nodes_zone_dict[node.ip]

        # Adding new node to the same availability zone from where node is being removed
        # and re-balance out one other node
        for counter, server in enumerate(failover_nodes_zones_dict):
            otp_node = f'ns_1@{server}'
            recovery_type = random.choice(['full', 'delta'])
            self.rest.fail_over(otpNode=otp_node)
            if self.recover_failed_over_node:
                self.sleep(60, "Waiting for a min after failing the node")
                self.rest.set_recovery_type(otpNode=otp_node, recoveryType=recovery_type)
            else:
                self.log.info(f"Adding new node {self.servers[self.nodes_init + counter].ip}")
                self.log.info(f"Adding new in zone {failover_nodes_zones_dict[server]}")
                self.rest.add_node(user=self.rest.username, password=self.rest.password,
                                   remoteIp=self.servers[self.nodes_init + counter].ip,
                                   zone_name=failover_nodes_zones_dict[server],
                                   services=['index'])
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=[], to_remove=[],
                                                      services=[])
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        # Getting the new configuration of cluster
        new_sub_cluster_list = []
        new_idx_host_list_dict = {str(bucket): set() for bucket in self.buckets}
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']

        for idx in indexer_metadata:
            idx_bucket = idx['bucket']
            idx_host = idx['hosts'][0]
            new_idx_host_list_dict[idx_bucket].add(idx_host)

        for bucket in new_idx_host_list_dict:
            host_list = sorted(list(new_idx_host_list_dict[bucket]))
            if host_list not in new_sub_cluster_list:
                new_sub_cluster_list.append(host_list)

        if self.recover_failed_over_node:
            self.assertEqual(sorted(sub_cluster_list), sorted(new_sub_cluster_list))
        else:
            # validating if swap-rebalance has adhere to Tenant rules
            new_zone_info = self.rest.get_zone_and_nodes()
            for new_sub_cluster in new_sub_cluster_list:
                zone_1, zone_2 = None, None

                self.assertEqual(len(new_sub_cluster_list), len(sub_cluster_list),
                                 f"Indexes distributed on sub-cluster of more"
                                 f"than 2 index nodes: {new_sub_cluster_list}")

                for node in new_sub_cluster:
                    node_ip = node.split(':')[0]
                    for server_group in new_zone_info:
                        if node_ip in new_zone_info[server_group] and zone_1 is None:
                            zone_1 = server_group
                        elif node_ip in new_zone_info[server_group] and zone_2 is None:
                            zone_2 = server_group
                        elif zone_1 and zone_2:
                            break
                self.assertNotEqual(zone_1, zone_2,
                                    f"Both the index nodes belong to same availability zone. {indexer_metadata}")

    def _validate_metatadata_token(self):
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        token_rest = RestConnection(index_node)
        counter = 0
        metadata_tokens = token_rest.get_metadata_tokens()
        while counter <= 5 * 60:  # 5 mins to get the create token cleared up
            for token in metadata_tokens:
                if "delete" not in token:
                    break
            else:
                break
            self.sleep(10, "Checking for metadata token clean up")
            counter += 10
            metadata_tokens = token_rest.get_metadata_tokens()
        if counter > 5 * 60:
            self.fail(f"Unprocessed tokens apart from delete tokens available after rebalance: {metadata_tokens}")

    def validate_tenant_management(self, check):
        if check == 'cluster_affinity':
            self._check_cluster_affinity()

        elif check == "index_count":
            self._check_index_count()

        elif check == 'index_distribution':
            self._check_index_distribution()

        elif check == "subcluster_reblance_swap":
            self._check_subcluster_rebalance_swap(num_of_nodes_to_remove=self.node_to_swap)

        elif check == "failed_over_subcluster":
            self._check_failed_over_subcluster(num_of_nodes_to_remove=self.node_to_swap)

    def get_server_group(self, node_ip):
        # Find Server group of removing nodes
        zone_info = self.rest.get_zone_and_nodes()
        for zone_name, node_list in zone_info.items():
            if node_ip in node_list:
                return zone_name
        return

    def get_host_list(self):
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        idx_host_list_dict = {str(bucket): set() for bucket in self.buckets}

        for idx in indexer_metadata:
            idx_bucket = idx['bucket']
            idx_host = idx['hosts'][0]
            idx_host_list_dict[idx_bucket].add(idx_host)
        return idx_host_list_dict

    def get_tenant_idx_list(self):
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        tenant_idx_dict = {str(bucket): [] for bucket in self.buckets}

        for idx in indexer_metadata:
            idx_bucket = idx['bucket']
            idx_name = idx['name']
            tenant_idx_dict[idx_bucket].append(idx_name)
        return tenant_idx_dict

    def get_sub_cluster_list(self):
        idx_host_list_dict = self.get_host_list()
        sub_cluster_list = list()
        for bucket in idx_host_list_dict:
            host_list = sorted(list(idx_host_list_dict[bucket]))
            if host_list not in sub_cluster_list:
                sub_cluster_list.append(host_list)
        return sub_cluster_list

    def check_kv_gsi_catchup(self):
        for namespace in self.namespaces:
            kv_query = f'SELECT COUNT(*) FROM {namespace}'
            gsi_query = f'SELECT COUNT(*) FROM {namespace} where age >=0'
            kv_result = self.run_cbq_query(kv_query)['results'][0]['$1']
            gsi_result = self.run_cbq_query(gsi_query)['results'][0]['$1']
            self.log.info(gsi_result)
            self.log.info(kv_result)

    def test_cluster_affinity(self):
        self.prepare_tenants()
        self.validate_tenant_management(check='cluster_affinity')
        self.validate_tenant_management(check='index_count')
        self.validate_tenant_management(check='index_distribution')
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_index_cap_per_tenant(self):
        self.prepare_tenants(index_creations=False)
        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                if self.use_defer_build:
                    defer_build = random.choice([True, False])
                else:
                    defer_build = False
                idx = f'idx_{item}_{collection_namespace.split(":")[1].replace(".", "_")}'
                index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=defer_build)
                try:
                    self.run_cbq_query(query=query, server=self.query_node)
                except Exception as err:
                    error = 'Build Already In Progress'
                    err_limit = 'Limit for number of indexes that can be created per bucket has been reached.'
                    if error not in str(err):
                        if err_limit in str(err):
                            self.log.info(f"Index created: {item}")
                            break
                        self.fail(err)

                self.wait_until_indexes_online(defer_build=True)
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_rebalance_sub_cluster(self):
        self.prepare_tenants()
        scopes = [f'{self.scope_prefix}_{num + 1}' for num in range(self.num_scopes)]
        collections = [f'{self.collection_prefix}_{num + 1}' for num in range(self.num_collections)]
        tasks = []
        with ThreadPoolExecutor() as executor:
            task = executor.submit(self.load_data_on_all_tenants, scopes=scopes, collections=collections,
                                   num_docs=self.num_of_docs_per_collection * 2)
            tasks.append(task)

            task = executor.submit(self.gsi_util_obj.index_operations_during_phases,
                                   namespaces=self.namespaces, phase="during", timeout=120, query_node=self.query_node)
            tasks.append(task)
            self.validate_tenant_management(check="subcluster_reblance_swap")

            for task in tasks:
                task.result()
        for namespace in self.namespaces:
            doc_count_query = f'Select count(*) from {namespace}'
            doc_count_result = self.run_cbq_query(query=doc_count_query)['results'][0]['$1']
            select_count_query = f'Select count(*) from {namespace} where age >= 0'
            select_count_result = self.run_cbq_query(query=select_count_query)['results'][0]['$1']
            self.assertEqual(doc_count_result, select_count_result)
            self.assertEqual(doc_count_result, self.num_of_docs_per_collection * 2)
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_failed_over_sub_cluster(self):
        self.prepare_tenants()
        scopes = [f'{self.scope_prefix}_{num + 1}' for num in range(self.num_scopes)]
        collections = [f'{self.collection_prefix}_{num + 1}' for num in range(self.num_collections)]
        tasks = []
        with ThreadPoolExecutor() as executor:
            task = executor.submit(self.load_data_on_all_tenants, scopes=scopes, collections=collections,
                                   num_docs=self.num_of_docs_per_collection * 2)
            tasks.append(task)

            task = executor.submit(self.gsi_util_obj.index_operations_during_phases,
                                   namespaces=self.namespaces, phase="during", timeout=120, query_node=self.query_node)
            tasks.append(task)

            task = executor.submit(self.gsi_util_obj.index_operations_during_phases,
                                   namespaces=self.namespaces, phase="before", timeout=120,
                                   query_node=self.query_node, num_of_batches=2)
            tasks.append(task)

            self.validate_tenant_management(check="failed_over_subcluster")

            for task in tasks:
                task.result()
        for namespace in self.namespaces:
            doc_count_query = f'Select count(*) from {namespace}'
            doc_count_result = self.run_cbq_query(query=doc_count_query)['results'][0]['$1']
            select_count_query = f'Select count(*) from {namespace} where age >= 0'
            select_count_result = self.run_cbq_query(query=select_count_query)['results'][0]['$1']
            self.assertEqual(doc_count_result, select_count_result)
            self.assertEqual(doc_count_result, self.num_of_docs_per_collection * 2)
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_hit_thresholds(self):
        self.prepare_tenants(data_load=False)
        scopes = [f'{self.scope_prefix}_{num + 1}' for num in range(self.num_scopes)]
        collections = [f'{self.collection_prefix}_{num + 1}' for num in range(self.num_collections)]
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        while not self.is_lwm_reached(node=index_node):
            random_prefix = 'doc_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
            self.load_data_on_all_tenants(scopes=scopes, collections=collections, key_prefix=random_prefix,
                                          num_docs=self.num_of_docs_per_collection)

        self.log.info("Add validation for LWM")

        try:
            self.prepare_tenants(bucket_num_offset=len(self.buckets))
        except Exception as err:
            self.log.info(err)
        self.log.info("Adding new sub-cluster")
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name='ServerGroup_2',
                           services=['index'])
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init + 1].ip,
                           zone_name='ServerGroup_3',
                           services=['index'])
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=[],
                                                      to_remove=[],
                                                      services=[])
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.prepare_tenants(bucket_num_offset=len(self.buckets))
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_rebalance_swap_in_multi_sub_cluster(self):
        self.prepare_tenants(data_load=False)
        sub_clusters = self.get_sub_cluster_list()
        remove_node_ips = [sub_cluster[0].split(':')[0] for sub_cluster in sub_clusters]
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        node_zone_dict = {}

        remove_nodes = []
        for remove_node_ip in remove_node_ips:
            for server in self.servers:
                if remove_node_ip == server.ip:
                    remove_nodes.append(server)
                    break

        for node in remove_node_ips:
            node_zone_dict[node] = self.get_server_group(node)

        for counter, server in enumerate(node_zone_dict):
            self.rest.add_node(user=self.rest.username, password=self.rest.password,
                               remoteIp=self.servers[self.nodes_init + counter].ip,
                               zone_name=node_zone_dict[server],
                               services=['index'])
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=[],
                                                      to_remove=remove_nodes,
                                                      services=['index'] * len(remove_nodes))
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        # Getting the new configuration of cluster
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)

        # validating cluster after swap
        new_indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        new_sub_clusters = self.get_sub_cluster_list()
        self.assertEqual(len(new_sub_clusters), len(sub_clusters))
        self.assertEqual(len(indexer_metadata), len(new_indexer_metadata))

        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_fast_rebalance_sub_cluster_move(self):
        self.prepare_tenants()
        sub_clusters = self.get_sub_cluster_list()
        remove_node_ips = [node.split(':')[0] for node in sub_clusters[0]]
        self.sleep(10)
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']

        node_zone_dict = {}

        remove_nodes = []
        for remove_node_ip in remove_node_ips:
            for server in self.servers:
                if remove_node_ip == server.ip:
                    remove_nodes.append(server)
                    break

        for node in remove_node_ips:
            node_zone_dict[node] = self.get_server_group(node)

        scopes = [f'{self.scope_prefix}_{num + 1}' for num in range(self.num_scopes)]
        collections = [f'{self.collection_prefix}_{num + 1}' for num in range(self.num_collections)]

        with ThreadPoolExecutor() as executor:
            loader_task = executor.submit(self.load_data_on_all_tenants, scopes=scopes, collections=collections,
                                          num_docs=self.num_of_docs_per_collection, key_prefix='new_doc_b4_reb_')
            scan_task = executor.submit(self.gsi_util_obj.index_operations_during_phases,
                                        namespaces=self.namespaces, phase="during", timeout=120,
                                        query_node=self.query_node)
            for counter, server in enumerate(node_zone_dict):
                self.rest.add_node(user=self.rest.username, password=self.rest.password,
                                   remoteIp=self.servers[self.nodes_init + counter].ip,
                                   zone_name=node_zone_dict[server],
                                   services=['index'])
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=remove_nodes,
                                                          services=['index'] * len(remove_nodes))
            rebalance_task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            scan_task.result()
            loader_task.result()

        # Checking mutation catchup
        with ThreadPoolExecutor() as executor:
            self.check_kv_gsi_catchup()
            loader_task = executor.submit(self.load_data_on_all_tenants, scopes=scopes, collections=collections,
                                          num_docs=self.num_of_docs_per_collection, key_prefix='new_doc_aft_reb_')
            scan_task = executor.submit(self.gsi_util_obj.index_operations_during_phases, namespaces=self.namespaces,
                                        phase="during", timeout=60, query_node=self.query_node)
            scan_task.result()
            loader_task.result()
            self.sleep(10)
            self.check_kv_gsi_catchup()

        # Getting the new configuration of cluster
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)

        # validating cluster after swap
        new_indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        new_sub_clusters = self.get_sub_cluster_list()
        self.assertEqual(len(new_sub_clusters), len(sub_clusters))
        self.assertEqual(len(indexer_metadata), len(new_indexer_metadata))

        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_cancel_resume_auto_scale(self):
        self.prepare_tenants()
        sub_clusters = self.get_sub_cluster_list()
        index_node_ips = [node.split(':')[0] for node in sub_clusters[0]]
        node_zone_dict = {}
        index_nodes = []
        for index_node_ip in index_node_ips:
            for server in self.servers:
                if index_node_ip == server.ip:
                    index_nodes.append(server)
                    break

        for node in index_node_ips:
            node_zone_dict[node] = self.get_server_group(node)
        for counter, server in enumerate(node_zone_dict):
            self.rest.add_node(user=self.rest.username, password=self.rest.password,
                               remoteIp=self.servers[self.nodes_init + counter].ip,
                               zone_name=node_zone_dict[server],
                               services=['index'])
        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=[],
                                                          services=['index'] * len(index_nodes))
            self.sleep(2, "Allowing sometime for rebalance to make progress")
            if self.cancel_rebalance:
                self.rest.stop_rebalance()
            else:
                self.stop_server(self.servers[self.nodes_init])
            result = rebalance_task.result()
            self.log.error(f"Rebalance didn't fail: {result}")
        except Exception as err:
            self.log.info('Rebalance failed. See logs for detailed reason' in str(err))
        finally:
            if self.cancel_rebalance is False:
                self.start_server(self.servers[self.nodes_init])

        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=[],
                                                      to_remove=[],
                                                      services=['index'] * len(index_nodes))
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_cancel_resume_swap_rebalance(self):
        self.prepare_tenants()
        indexder_metadata_b4_swap = self.index_rest.get_indexer_metadata()['status']
        sub_clusters = self.get_sub_cluster_list()
        index_node_ips = [node.split(':')[0] for node in sub_clusters[0]]
        node_zone_dict = {}
        index_nodes = []
        for index_node_ip in index_node_ips:
            for server in self.servers:
                if index_node_ip == server.ip:
                    index_nodes.append(server)
                    break

        for node in index_node_ips:
            node_zone_dict[node] = self.get_server_group(node)
        for counter, server in enumerate(node_zone_dict):
            self.rest.add_node(user=self.rest.username, password=self.rest.password,
                               remoteIp=self.servers[self.nodes_init + counter].ip,
                               zone_name=node_zone_dict[server],
                               services=['index'])
        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=index_nodes,
                                                          services=['index'] * len(index_nodes))
            self.sleep(5, "Allowing sometime for rebalance to make progress")

            if self.cancel_rebalance:
                self.rest.stop_rebalance()
            else:
                self.stop_server(self.servers[self.nodes_init])
            result = rebalance_task.result()
            self.log.error(f"Rebalance didn't fail: {result}")
        except Exception as err:
            self.log.info('Rebalance failed. See logs for detailed reason' in str(err))
        finally:
            if self.cancel_rebalance is False:
                self.start_server(self.servers[self.nodes_init])

        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                      to_add=[],
                                                      to_remove=index_nodes,
                                                      services=['index'] * len(index_nodes))

        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexder_metadata_aft_swap = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_b4_swap), len(indexder_metadata_aft_swap))
        prim_index = None
        sec_index = None
        for index in indexder_metadata_aft_swap:
            if 'primary_' in index['indexName']:
                prim_index = index['indexName']
            elif 'age_gender' in index['indexName']:
                sec_index = index['indexName']

            if prim_index and sec_index:
                break

        for namespace in self.namespaces:
            primary_scan_query = f'Select count(*) from {namespace} use index (`{prim_index}`) where age >=0'
            secondary_scan_query = f'Select count(*) from {namespace} use index (`{sec_index}`) where age >=0'
            prim_result = self.run_cbq_query(query=primary_scan_query, server=self.query_node)['results'][0]['$1']
            sec_result = self.run_cbq_query(query=secondary_scan_query, server=self.query_node)['results'][0]['$1']
            self.assertEqual(prim_result, sec_result, "Doc count differ between Primary and Secondary Index")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_fast_rebalance_conflict_same_tenant(self):
        same_collection_conflict = self.input.param("same_collection_conflict", False)
        self.prepare_tenants()
        index_metadata_b4_swap = self.index_rest.get_indexer_metadata()['status']
        sub_clusters = self.get_sub_cluster_list()
        swap_node_ip = sub_clusters[0][0].split(':')[0]
        swap_node = ''

        for server in self.servers:
            if swap_node_ip == server.ip:
                swap_node = server

        swap_node_zone = self.get_server_group(swap_node_ip)
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name=swap_node_zone,
                           services=['index'])

        create_queries = []
        if same_collection_conflict:
            namespace = self.namespaces[0]
        else:
            namespace = self.namespaces[1]
        batch_iter = 5
        for _ in range(batch_iter):
            query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template)
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, defer_build_mix=True)
            create_queries.extend(queries)
        tasks = []
        try:
            with ThreadPoolExecutor() as executor:
                for query in create_queries:
                    task = executor.submit(self.run_cbq_query, query=query, server=self.query_node)
                    tasks.append(task)
                rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                              to_add=[],
                                                              to_remove=[swap_node],
                                                              services=['index'])
                rebalance_task.result()
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.fail(err)
        self.sleep(30)
        result = self.wait_until_indexes_online(defer_build=True)
        if not result:
            self.log.info("Timed out. Hence re-checking the index status")
            self.wait_until_indexes_online(defer_build=True)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexder_metadata_aft_swap = self.index_rest.get_indexer_metadata()['status']
        expected_index_count = len(index_metadata_b4_swap) + batch_iter * self.gsi_util_obj.batch_size * 2
        self.assertEqual(len(indexder_metadata_aft_swap), expected_index_count, f"Indexer meta data {indexder_metadata_aft_swap}")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_fast_rebalance_conflict_different_tenant(self):
        self.prepare_tenants(index_creations=False)

        # creating imbalance tenant indexes workload
        self.gsi_util_obj.index_operations_during_phases(namespaces=[self.namespaces[0]], dataset=self.json_template,
                                                         query_node=self.query_node, capella_run=False,
                                                         num_of_batches=3)
        self.wait_until_indexes_online(defer_build=False)

        self.gsi_util_obj.index_operations_during_phases(namespaces=[self.namespaces[1]], dataset=self.json_template,
                                                         query_node=self.query_node, capella_run=False,
                                                         num_of_batches=1)
        self.wait_until_indexes_online(defer_build=False)

        index_metadata_b4_swap = self.index_rest.get_indexer_metadata()['status']
        sub_clusters = self.get_sub_cluster_list()
        swap_node_ip = sub_clusters[0][0].split(':')[0]
        swap_node = ''

        for server in self.servers:
            if swap_node_ip == server.ip:
                swap_node = server

        swap_node_zone = self.get_server_group(swap_node_ip)
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name=swap_node_zone,
                           services=['index'])

        create_queries = []

        namespace = self.namespaces[1]
        for _ in range(10):
            query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template)
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, defer_build_mix=True)
            create_queries.extend(queries)
        tasks = []
        try:
            with ThreadPoolExecutor() as executor:
                for query in create_queries:
                    task = executor.submit(self.run_cbq_query, query=query, server=self.query_node)
                    tasks.append(task)
                rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                              to_add=[],
                                                              to_remove=[swap_node],
                                                              services=['index'])
                rebalance_task.result()
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.log.info(err)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexder_metadata_aft_swap = self.index_rest.get_indexer_metadata()['status']
        self.log.info(f"{len(indexder_metadata_aft_swap)}, {len(index_metadata_b4_swap)}")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_ddl_conflict_same_tenant(self):
        same_collection_conflict = self.input.param("same_collection_conflict", False)
        self.prepare_tenants()
        index_metadata_b4_swap = self.index_rest.get_indexer_metadata()['status']
        sub_clusters = self.get_sub_cluster_list()
        swap_node_ip = sub_clusters[0][0].split(':')[0]
        swap_node = ''

        for server in self.servers:
            if swap_node_ip == server.ip:
                swap_node = server

        swap_node_zone = self.get_server_group(swap_node_ip)
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name=swap_node_zone,
                           services=['index'])

        create_queries = []
        if same_collection_conflict:
            namespace = self.namespaces[0]
        else:
            namespace = self.namespaces[1]
        for _ in range(10):
            query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template)
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, defer_build_mix=True)
            create_queries.extend(queries)
        tasks = []
        try:
            with ThreadPoolExecutor() as executor:
                rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                              to_add=[],
                                                              to_remove=[swap_node],
                                                              services=['index'])
                self.sleep(10)
                for query in create_queries:
                    task = executor.submit(self.run_cbq_query, query=query, server=self.query_node)
                    tasks.append(task)

                rebalance_task.result()
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.log.info(err)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexder_metadata_aft_swap = self.index_rest.get_indexer_metadata()['status']
        self.log.info(f"{len(indexder_metadata_aft_swap)}, {len(index_metadata_b4_swap)}")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_ddl_conflict_different_tenant(self):
        self.prepare_tenants()

        # creating imbalance tenant indexes workload
        self.gsi_util_obj.index_operations_during_phases(namespaces=[self.namespaces[0]], dataset=self.json_template,
                                                         query_node=self.query_node, capella_run=False,
                                                         num_of_batches=3)
        self.wait_until_indexes_online(defer_build=False)

        self.gsi_util_obj.index_operations_during_phases(namespaces=[self.namespaces[1]], dataset=self.json_template,
                                                         query_node=self.query_node, capella_run=False,
                                                         num_of_batches=1)
        self.wait_until_indexes_online(defer_build=False)

        index_metadata_b4_swap = self.index_rest.get_indexer_metadata()['status']
        sub_clusters = self.get_sub_cluster_list()
        swap_node_ip = sub_clusters[0][0].split(':')[0]
        swap_node = ''

        for server in self.servers:
            if swap_node_ip == server.ip:
                swap_node = server

        swap_node_zone = self.get_server_group(swap_node_ip)
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name=swap_node_zone,
                           services=['index'])

        create_queries = []

        namespace = self.namespaces[1]
        for _ in range(10):
            query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template)
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, defer_build_mix=True)
            create_queries.extend(queries)
        tasks = []
        try:
            with ThreadPoolExecutor() as executor:
                rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                              to_add=[],
                                                              to_remove=[swap_node],
                                                              services=['index'])
                self.sleep(10)
                for query in create_queries:
                    task = executor.submit(self.run_cbq_query, query=query, server=self.query_node)
                    tasks.append(task)

                rebalance_task.result()
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.log.info(err)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexder_metadata_aft_swap = self.index_rest.get_indexer_metadata()['status']
        self.log.info(f"{len(indexder_metadata_aft_swap)}, {len(index_metadata_b4_swap)}")
        self.s3_utils_obj.check_s3_cleanup(folder=self.s3_utils_obj)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_clean_up_on_failure(self):
        rebalance_failure_sources = self.input.param('rebalance_failure_sources', 'src_sub')
        self.prepare_tenants()
        index_metadata_b4_swap = self.index_rest.get_indexer_metadata()['status']
        sub_clusters = self.get_sub_cluster_list()
        swap_node_ip = sub_clusters[0][1].split(':')[0]
        swap_node = ''

        for server in self.servers:
            if swap_node_ip == server.ip:
                swap_node = server

        swap_node_zone = self.get_server_group(swap_node_ip)
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name=swap_node_zone,
                           services=['index'])
        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=[swap_node],
                                                          services=['index'])
            self.sleep(10)
            if rebalance_failure_sources == 'src_sub':
                self.stop_server(swap_node)
            else:
                self.stop_server(self.servers[self.nodes_init])
            rebalance_task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        except Exception as err:
            self.log.info(err)
        finally:
            if rebalance_failure_sources == 'src_sub':
                self.start_server(swap_node)
            else:
                self.start_server(self.servers[self.nodes_init])
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexder_metadata_aft_swap = self.index_rest.get_indexer_metadata()['status']
        self.log.info(f"{len(indexder_metadata_aft_swap)}, {len(index_metadata_b4_swap)}")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_stats_validation(self):
        mutation_during_rebalance = self.input.param("mutation_during_rebalance", False)
        self.prepare_tenants()
        sub_clusters = self.get_sub_cluster_list()
        swap_node_ip = sub_clusters[0][0].split(':')[0]
        swap_node = ''

        for server in self.servers:
            if swap_node_ip == server.ip:
                swap_node = server
                break

        swap_node_zone = self.get_server_group(swap_node_ip)
        add_node = self.servers[self.nodes_init]
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=add_node.ip,
                           zone_name=swap_node_zone,
                           services=['index'])
        indexer_metadata_b4_reb = self.index_rest.get_indexer_metadata()['status']
        index_reb_stats_b4_reb = self.get_rebalance_related_stats(index_node=swap_node)

        # Todo: Persisted stat is not available currently. Check with Varun on changes
        # index_persisted_stats_b4_reb = self.get_persisted_stats(index_node=swap_node)
        scopes = [f'{self.scope_prefix}_{num + 1}' for num in range(self.num_scopes)]
        collections = [f'{self.collection_prefix}_{num + 1}' for num in range(self.num_collections)]
        with ThreadPoolExecutor() as executor:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=[swap_node],
                                                          services=['index'])
            if mutation_during_rebalance:
                data_task = executor.submit(self.load_data_on_all_tenants, scopes=scopes, collections=collections,
                                            num_docs=self.num_of_docs_per_collection * 2)

                scan_task = executor.submit(self.gsi_util_obj.index_operations_during_phases,
                                            namespaces=self.namespaces, phase="during", timeout=120,
                                            query_node=self.query_node)
                data_task.result()
                scan_task.result()
            rebalance_task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        for namespace in self.namespaces:
            doc_count_query = f'Select count(*) from {namespace}'
            doc_count_result = self.run_cbq_query(query=doc_count_query)['results'][0]['$1']
            select_count_query = f'Select count(*) from {namespace} where age >= 0'
            select_count_result = self.run_cbq_query(query=select_count_query)['results'][0]['$1']
            self.assertEqual(doc_count_result, select_count_result)
            if mutation_during_rebalance:
                self.assertEqual(doc_count_result, self.num_of_docs_per_collection * 2)
        index_reb_stats_aft_reb = self.get_rebalance_related_stats(index_node=add_node)
        # Todo: Persisted stat is not available currently. Check with Varun on changes
        # index_persisted_stats_aft_reb = self.get_persisted_stats(index_node=add_node)
        self.index_rest = RestConnection(add_node)
        indexer_metadata_aft_reb = self.index_rest.get_indexer_metadata()['status']

        reb_stats_diff = DeepDiff(index_reb_stats_b4_reb, index_reb_stats_aft_reb)
        # persisted_stats_diff = DeepDiff(index_persisted_stats_b4_reb, index_persisted_stats_aft_reb)

        if mutation_during_rebalance:
            if reb_stats_diff:
                self.log.info(reb_stats_diff)
                self.assertEqual(len(index_reb_stats_b4_reb), len(index_reb_stats_b4_reb))
            # if persisted_stats_diff:
            #     self.log.info(persisted_stats_diff)
        else:
            if reb_stats_diff:
                self.fail(f"reb_stats_diff: {reb_stats_diff}")
            # if persisted_stats_diff:
            #     self.log.info(f"persisted_stats_diff: {persisted_stats_diff}")
        self.assertEqual(len(indexer_metadata_b4_reb), len(indexer_metadata_aft_reb))
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_schedule_ddl_during_rebalance(self):
        self.prepare_tenants(defer_build_mix=True)
        indexer_metadata_b4_reb = self.index_rest.get_indexer_metadata()['status']
        defered_indexes = {}
        index_dict = {}
        for index in indexer_metadata_b4_reb:
            if index['status'] == 'Created':
                namespace = f'{index["bucket"]}.{index["scope"]}.{index["collection"]}'
                index_name = index['indexName']
                if namespace in defered_indexes:
                    defered_indexes[namespace].append(f'`{index_name}`')
                else:
                    defered_indexes[namespace] = [f'`{index_name}`']
            index_dict[index['indexName']] = f'{index["bucket"]}.{index["scope"]}.{index["collection"]}'

        # Dropping 20% of indexes during rebalance, building deferred and creating new indexes
        drop_queries = []
        build_queries = []
        for index in index_dict:
            if index == '#primary':
                continue
            for namespace in defered_indexes:
                if index not in defered_indexes[namespace]:
                    drop_query = f'Drop index `{index}` on {namespace}'
                    drop_queries.append(drop_query)
            if len(drop_queries) >= len(index_dict) / 5:
                break

        for namespace in defered_indexes:
            defered_indexes_list = ', '.join(defered_indexes[namespace])
            build_queries.append(f'Build index on {namespace}({defered_indexes_list})')

        create_queries = []
        batch_size = 2
        for namespace in self.namespaces:
            for _ in range(batch_size):
                query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template)
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace, defer_build_mix=True)
                create_queries.extend(queries)
        queries = build_queries + drop_queries + create_queries

        sub_clusters = self.get_sub_cluster_list()
        swap_node_ip = sub_clusters[0][0].split(':')[0]
        swap_node = ''
        for server in self.servers:
            if swap_node_ip == server.ip:
                swap_node = server
                break

        swap_node_zone = self.get_server_group(swap_node_ip)
        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name=swap_node_zone,
                           services=['index'])

        tasks = []
        try:
            with ThreadPoolExecutor() as executor:
                for query in queries:
                    task = executor.submit(self.run_cbq_query, query=query, server=self.query_node)
                    tasks.append(task)
                rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                              to_add=[],
                                                              to_remove=[swap_node],
                                                              services=['index'])
                self.sleep(15)
                while self.rest._rebalance_progress() < 25:
                    self.sleep(5)
                rebalance_task.result()
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.fail(err)
        result = self.wait_until_indexes_online(defer_build=True)
        if not result:
            self.log.info("Re-attempting to check index status")
            result = self.wait_until_indexes_online(defer_build=True)
            self.log.info(f"Index status check:{result}")
        self._validate_metatadata_token()
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_auto_rebalance_defrag(self):
        self.prepare_tenants()

        # Get skewed sub-cluster
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        sub_cluster_map = {}
        for index in indexer_metadata:
            if index['scope'] == '_system':
                continue
            host = index['hosts'][0].split(":")[0]
            tenant_namespace = f'default:{index["bucket"]}.{index["scope"]}.{index["collection"]}'
            if tenant_namespace in sub_cluster_map:
                sub_cluster_map[tenant_namespace].add(host)
            else:
                sub_cluster_map[tenant_namespace] = {host}

        tenants_on_skewed_sub_cluster = []
        if sub_cluster_map[self.namespaces[0]] == sub_cluster_map[self.namespaces[1]]:
            tenants_on_skewed_sub_cluster.append(self.namespaces[0])
            tenants_on_skewed_sub_cluster.append(self.namespaces[1])
            skewed_sub_cluster_node = list(sub_cluster_map[self.namespaces[0]])[0]
        elif sub_cluster_map[self.namespaces[0]] == sub_cluster_map[self.namespaces[2]]:
            tenants_on_skewed_sub_cluster.append(self.namespaces[0])
            tenants_on_skewed_sub_cluster.append(self.namespaces[2])
            skewed_sub_cluster_node = list(sub_cluster_map[self.namespaces[0]])[0]
        else:
            tenants_on_skewed_sub_cluster.append(self.namespaces[1])
            tenants_on_skewed_sub_cluster.append(self.namespaces[2])
            skewed_sub_cluster_node = list(sub_cluster_map[self.namespaces[1]])[0]

        # Pushing a sub-cluster towards HWM
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        node = ''
        for index_node in index_nodes:
            if index_node.ip == skewed_sub_cluster_node:
                node = index_node
                break

        node_rest = RestConnection(node)
        loaded_tenant_namespace = tenants_on_skewed_sub_cluster[0]

        # Increase the work load on one of the tenant of skewed sub-cluster, so that on rebalance, less loaded tenant
        # would move to other sub-cluster triggering auto-rebalance and implementing defrag api suggestion.
        while not self.is_lwm_reached(node=node):
            self.gsi_util_obj.index_operations_during_phases(namespaces=[loaded_tenant_namespace],
                                                             dataset=self.json_template)

        # Now increasing the combined load so that we cross HWM
        while not self.is_hwm_reached(node=node):
            self.gsi_util_obj.index_operations_during_phases(namespaces=tenants_on_skewed_sub_cluster,
                                                             dataset=self.json_template)

        self.sleep(30)
        defrag_result_b4_reb = node_rest.get_index_defrag_output()
        self.log.info(f"Defrag result before Rebalance:{defrag_result_b4_reb}")
        # calling rebalance to run defrag
        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=[],
                                                          services=[])
            rebalance_task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.fail(err)
        defrag_result_aft_reb = node_rest.get_index_defrag_output()
        self.log.info(f"Defrag result after Rebalance:{defrag_result_aft_reb}")
        self.assertTrue(defrag_result_b4_reb != defrag_result_aft_reb)

        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)

    def test_defrag_replica_repair(self):
        self.prepare_tenants()

        # Adding index Node to the same server group so that replica repair can be done
        indexer_metadata_b4_reb = self.index_rest.get_indexer_metadata()['status']
        sub_clusters = self.get_sub_cluster_list()
        remove_node_ip = sub_clusters[0][0].split(':')[0]
        node_zone = self.get_server_group(remove_node_ip)

        self.rest.add_node(user=self.rest.username, password=self.rest.password,
                           remoteIp=self.servers[self.nodes_init].ip,
                           zone_name=node_zone,
                           services=['index'])
        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=[],
                                                          services=[])
            rebalance_task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.fail(err)

        # Making replica repair available in cluster and on reblance replica should be available
        otp_node = f'ns_1@{remove_node_ip}'
        self.rest.fail_over(otpNode=otp_node)
        self.sleep(10)

        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        node_rest = RestConnection(index_node)
        defrag_result_b4_reb = node_rest.get_index_defrag_output()
        self.log.info(defrag_result_b4_reb)
        for node in defrag_result_b4_reb:
            if node.split(':')[0] != self.servers[self.nodes_init].ip:
                continue
            num_index_repaired = defrag_result_b4_reb[node]['num_index_repaired']
            self.assertTrue(num_index_repaired > 0, "Node is not marked for Replica repair")

        # calling rebalance to run replica repair
        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=[],
                                                          services=[])
            rebalance_task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        except Exception as err:
            self.fail(err)

        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        indexer_metadata_after_reb = self.index_rest.get_indexer_metadata()['status']
        defrag_result_aft_reb = self.index_rest.get_index_defrag_output()
        self.log.info(f"Defrag result before HWM:{defrag_result_aft_reb}")
        for node in defrag_result_aft_reb:
            num_index_repaired = defrag_result_aft_reb[node]['num_index_repaired']
            self.assertEqual(num_index_repaired, 0, "Node is marked for Replica repair")

        self.assertEqual(len(indexer_metadata_after_reb), len(indexer_metadata_b4_reb),
                         "No. of index replicas after repair is not matching")
        self.s3_utils_obj.check_s3_cleanup(folder=self.storage_prefix)
        self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)
