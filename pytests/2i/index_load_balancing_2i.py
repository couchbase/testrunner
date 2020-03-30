import logging
from .base_2i import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection

log = logging.getLogger(__name__)


class SecondaryIndexingLoadBalancingTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingLoadBalancingTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingLoadBalancingTests, self).tearDown()

    def test_load_balance_when_index_node_down_stop(self):
        index_dist_factor = 1
        index_servers = []
        #Create Indexes
        index_servers = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        num_indexes = len(index_servers)*index_dist_factor
        self.query_definitions = self._create_query_definitions(
            index_count=num_indexes)
        node_count = 0
        tasks = []
        self.defer_build = False
        for query_definition in self.query_definitions:
            for bucket in self.buckets:
                deploy_node_info = ["{0}:{1}".format(
                    index_servers[node_count].ip, index_servers[node_count].port)]
                self.create_index(bucket.name,
                    query_definition, deploy_node_info=deploy_node_info)
                node_count += 1
        remote = RemoteMachineShellConnection(index_servers[0])
        remote.stop_server()
        unavailable_query = self.query_definitions.pop(0)
        self.sleep(10)
        # Query to see the other
        self.run_multi_operations(buckets=self.buckets,
            query_definitions=self.query_definitions, query_with_explain=True, query=True)
        remote.start_server()
        self.sleep(30)
        self.query_definitions.append(unavailable_query)
        # Drop indexes
        self.run_multi_operations(buckets = self.buckets,
            query_definitions=self.query_definitions, create_index=False, drop_index=True)

    def test_load_balance_when_index_node_down_network_partition(self):
        index_dist_factor = 1
        #Create Indexes
        index_servers = self.get_nodes_from_services_map(service_type="index",
            get_all_nodes=True)
        num_indexes=len(index_servers)*index_dist_factor
        self.query_definitions = self._create_query_definitions(index_count=num_indexes)
        node_count = 0
        for query_definition in self.query_definitions:
            for bucket in self.buckets:
                deploy_node_info = ["{0}:{1}".format(index_servers[node_count].ip,
                    index_servers[node_count].port)]
                self.log.info("Creating {0} index on bucket {1} on node {2}...".format(
                    query_definition.index_name, bucket.name, deploy_node_info[0]
                ))
                self.create_index(bucket.name, query_definition,
                    deploy_node_info=deploy_node_info)
                node_count += 1
                self.sleep(30)
        # Bring down one node
        self.log.info("Starting firewall on node {0}...".format(index_servers[0].ip))
        self.start_firewall_on_node(index_servers[0])
        self.sleep(60)
        # Remove index for offline node.
        unavailable_query = self.query_definitions.pop(0)
        # Query to see the other
        self.run_multi_operations(buckets=self.buckets,
             query_definitions=self.query_definitions, query_with_explain=False, query=True)
        self.log.info("Stopping firewall on node {0}...".format(index_servers[0].ip))
        self.stop_firewall_on_node(index_servers[0])
        self.sleep(60)
        # Add removed index from query
        self.query_definitions.append(unavailable_query)
        # Drop indexes
        self.run_multi_operations(buckets=self.buckets,
            query_definitions=self.query_definitions, create_index=False, drop_index=True)

    def test_index_create_sync(self):
        index_dist_factor = 2
        try:
            #Create Index
            servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
            num_indexes=len(servers)*index_dist_factor
            self.query_definitions = self._create_query_definitions(index_count = num_indexes)
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = False)
            indexList = [query_definition.index_name for query_definition in self.query_definitions]
            #Verify that the indexes are equally distributed
            expected_index_count_per_node =  len(indexList)//len(servers)
            index_map = self.get_index_stats(perNode=True)
            for node in list(index_map.keys()):
                total_indexes_per_node=0
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in list(index_map[node].keys()):
                    total_indexes_per_node+=len(list(index_map[node][bucket_name].keys()))
                self.assertTrue(expected_index_count_per_node == total_indexes_per_node,
                 " Index not correctly distributed || expected :: {0} != actual :: {1} \n with index map is {2} ".format(
                    expected_index_count_per_node, total_indexes_per_node, index_map))
        except Exception as ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = False, drop_index = True)

    def test_index_create_async(self):
        index_dist_factor = 1
        try:
            #Create Index
            servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
            num_indexes=len(servers)*index_dist_factor
            self.query_definitions = self._create_query_definitions(index_count = num_indexes)
            self.log.info(self.query_definitions)
            tasks = self.async_run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions,
             create_index = True, drop_index = False)
            for task in tasks:
                task.result()
            indexList = [query_definition.index_name for query_definition in self.query_definitions]
            #Verify that the indexes are equally distributed
            expected_index_count_per_node =  len(indexList)//len(servers)
            index_map = self.get_index_stats(perNode=True)
            for node in list(index_map.keys()):
                total_indexes_per_node=0
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in list(index_map[node].keys()):
                    total_indexes_per_node+=len(list(index_map[node][bucket_name].keys()))
                self.assertTrue(expected_index_count_per_node == total_indexes_per_node,
                 " Index not correctly distributed || expected :: {0} != actual :: {1} \n with index map is {2} ".format(
                    expected_index_count_per_node, total_indexes_per_node, index_map))
        except Exception as ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = False, drop_index = True)

    def test_index_create_drop_sync(self):
        index_dist_factor = 4
        try:
            #Create Index
            servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
            num_indexes=len(servers)*index_dist_factor
            self.query_definitions = self._create_query_definitions(index_count = num_indexes)
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = False)
            indexList = [query_definition.index_name for query_definition in self.query_definitions]
            #Verify that the indexes are equally distributed
            expected_index_count_per_node =  len(indexList)//len(servers)
            #drop the indexes
            query_definitions = [self.query_definitions[x] for x in range(0, num_indexes//2)]
            delete_query_definitions = [self.query_definitions[x] for x in range(num_indexes//2, num_indexes)]
            self.run_multi_operations(buckets = self.buckets, query_definitions = query_definitions, create_index = False, drop_index = True)
            query_definitions = self._create_query_definitions(start= num_indexes, index_count=num_indexes//2)
            self.run_multi_operations(buckets = self.buckets, query_definitions = query_definitions, create_index = True, drop_index = False)
            index_map = self.get_index_stats(perNode=True)
            for node in list(index_map.keys()):
                total_indexes_per_node=0
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in list(index_map[node].keys()):
                    total_indexes_per_node+=len(list(index_map[node][bucket_name].keys()))
                self.assertTrue(expected_index_count_per_node == total_indexes_per_node,
                 " Index not correctly distributed || expected :: {0} != actual :: {1} \n with index map is {2} ".format(
                    expected_index_count_per_node, total_indexes_per_node, index_map))
        except Exception as ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets, query_definitions = delete_query_definitions, create_index = False, drop_index = True)
            self.run_multi_operations(buckets = self.buckets, query_definitions = query_definitions, create_index = False, drop_index = True)

    def test_scan_when_replica_index_drop(self):
        """
        CBQE-3152
        1. Create two index defined on the same bucket/fields so that it will behave as replica.
        2. Drop one of the index.
        3. start issuing the scans and validate that the scan does not pick the dropped replica.
        """
        index_dist_factor = 1
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        num_indexes = len(servers)*index_dist_factor
        self.query_definitions = self._create_query_definitions(index_count=num_indexes)
        self.run_multi_operations(buckets=self.buckets, query_definitions=self.query_definitions,
                                  create_index=True, drop_index=False)
        #drop fewindexes
        query_definitions = [self.query_definitions[x] for x in range(num_indexes//2)]
        delete_query_definitions = [self.query_definitions[x] for x in range(num_indexes//2, num_indexes)]
        self.run_multi_operations(buckets=self.buckets, query_definitions=query_definitions,
                                  create_index=False, drop_index=True)
        #Run Queries
        self.run_multi_operations(buckets=self.buckets, query_definitions=self.query_definitions, query=True)
        #Finally Drop All Indexes
        self.run_multi_operations(buckets=self.buckets, query_definitions=delete_query_definitions,
                                  create_index=False, drop_index=True)

    def test_query_using_index_sync(self):
        index_dist_factor = 1
        try:
            #Create Index
            servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            num_indexes=len(servers)*index_dist_factor
            self.query_definitions = self._create_query_definitions(index_count=num_indexes)
            self.multi_create_index(buckets=self.buckets,
                                         query_definitions=self.query_definitions)
            index_map = self.get_index_stats(perNode=True)
            self.log.info(index_map)
            for node in list(index_map.keys()):
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in list(index_map[node].keys()):
                    for index_name in list(index_map[node][bucket_name].keys()):
                        count = 0
                        scan_duration = index_map[node][bucket_name][index_name]['total_scan_duration']
                        while not scan_duration and count < 5:
                            self.multi_query_using_index(buckets=self.buckets,
                                                         query_definitions=self.query_definitions)
                            index_map = self.get_index_stats(perNode=True)
                            scan_duration = index_map[node][bucket_name][index_name]['total_scan_duration']
                            count += 1
                        self.assertTrue(index_map[node][bucket_name][index_name]['total_scan_duration']  > 0,
                                        " scan did not happen at node {0}".format(node))
        except Exception as ex:
            self.log.info(str(ex))
            raise
        finally:
            self.run_multi_operations(buckets=self.buckets, query_definitions=self.query_definitions,
                                      create_index=False, drop_index=True)

    def test_index_drop_folder_cleanup(self):
        index_dist_factor = 1
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        num_indexes = len(servers)*index_dist_factor
        self.query_definitions = self._create_query_definitions(index_count=num_indexes)
        self.run_multi_operations(buckets=self.buckets, query_definitions=self.query_definitions,
                                  create_index=True, drop_index=False)
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            rest = RestConnection(server)
            data_path = rest.get_data_path()
            before_deletion_files = shell.list_files(data_path + "/@2i/")
            log.info("Files on node {0}: {1}".format(server.ip, before_deletion_files))
            self.assertTrue((len(before_deletion_files) > 1), "Index Files not created on node {0}".format(server.ip))
        self.run_multi_operations(buckets=self.buckets, query_definitions=self.query_definitions,
                                  create_index=False, drop_index=True)
        self.sleep(20)
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            rest = RestConnection(server)
            data_path = rest.get_data_path()
            after_deletion_files = shell.list_files(data_path + "/@2i/")
            log.info("Files on node {0}: {1}".format(server.ip, after_deletion_files))
            self.assertEqual(len(after_deletion_files), 1,
                        "Index directory not clean after drop Index on node {0}".format(server.ip))

    def _create_query_definitions(self, start= 0, index_count=2):
        query_definitions = []
        for ctr in range(start, start+index_count):
            index_name = "index_name_{0}".format(ctr)
            query_definition = QueryDefinition(index_name=index_name,
                                               index_fields=["join_yr"],
                                               query_template="SELECT * from %s WHERE join_yr == 2010 ",
                                               groups=[])
            query_definitions.append(query_definition)
        return query_definitions
