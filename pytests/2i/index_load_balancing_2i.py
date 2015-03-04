from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition

class SecondaryIndexingLoadBalancingTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingLoadBalancingTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingLoadBalancingTests, self).tearDown()

    def test_index_create_sync(self):
        index_dist_factor = 2
        #Create Index
        servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        num_indexes=len(servers)*index_dist_factor
        self.query_definitions = self._create_query_definitions(index_count = num_indexes)
        self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = False)
        indexList = [query_definition.index_name for query_definition in self.query_definitions]
        #Verify that the indexes are equally distributed
        expected_index_count_per_node =  len(indexList)/len(servers)
        index_map = self.get_index_stats(perNode=True)
        try:
            for node in index_map.keys():
                total_indexes_per_node=0
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in index_map[node].keys():
                    total_indexes_per_node+=len(index_map[node][bucket_name].keys())
                self.assertTrue(expected_index_count_per_node == total_indexes_per_node,
                 " Index not correctly distributed || expected :: {0} != actual :: {1} \n with index map is {2} ".format(
                    expected_index_count_per_node,total_indexes_per_node,index_map))
        except Exception, ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = False, drop_index = True)

    def test_index_create_async(self):
        index_dist_factor = 1
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
        expected_index_count_per_node =  len(indexList)/len(servers)
        index_map = self.get_index_stats(perNode=True)
        try:
            for node in index_map.keys():
                total_indexes_per_node=0
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in index_map[node].keys():
                    total_indexes_per_node+=len(index_map[node][bucket_name].keys())
                self.assertTrue(expected_index_count_per_node == total_indexes_per_node,
                 " Index not correctly distributed || expected :: {0} != actual :: {1} \n with index map is {2} ".format(
                    expected_index_count_per_node, total_indexes_per_node,index_map))
        except Exception, ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = False, drop_index = True)

    def test_index_create_drop_sync(self):
        index_dist_factor = 4
        #Create Index
        servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        num_indexes=len(servers)*index_dist_factor
        self.query_definitions = self._create_query_definitions(index_count = num_indexes)
        self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = False)
        indexList = [query_definition.index_name for query_definition in self.query_definitions]
        #Verify that the indexes are equally distributed
        expected_index_count_per_node =  len(indexList)/len(servers)
        #drop the indexes
        query_definitions = [self.query_definitions[x] for x in range(0,num_indexes/2)]
        delete_query_definitions = [self.query_definitions[x] for x in range(num_indexes/2,num_indexes)]
        try:
            self.run_multi_operations(buckets = self.buckets, query_definitions = query_definitions, create_index = False, drop_index = True)
            index_map = self.get_index_stats(perNode=True)
            query_definitions = self._create_query_definitions(start= num_indexes, index_count=num_indexes/2)
            self.run_multi_operations(buckets = self.buckets, query_definitions = query_definitions, create_index = True, drop_index = False)
            index_map = self.get_index_stats(perNode=True)
            for node in index_map.keys():
                total_indexes_per_node=0
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in index_map[node].keys():
                    total_indexes_per_node+=len(index_map[node][bucket_name].keys())
                self.assertTrue(expected_index_count_per_node == total_indexes_per_node,
                 " Index not correctly distributed || expected :: {0} != actual :: {1} \n with index map is {2} ".format(
                    expected_index_count_per_node,total_indexes_per_node,index_map))
        except Exception, ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets, query_definitions = delete_query_definitions, create_index = False, drop_index = True)
            self.run_multi_operations(buckets = self.buckets, query_definitions = query_definitions, create_index = False, drop_index = True)

    def test_query_using_index_sync(self):
        index_dist_factor = 1
        #Create Index
        servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        num_indexes=len(servers)*index_dist_factor
        self.query_definitions = self._create_query_definitions(index_count = num_indexes)
        self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, query = True)
        index_map = self.get_index_stats(perNode=True)
        try:
            for node in index_map.keys():
                self.log.info(" verifying node {0}".format(node))
                for bucket_name in index_map[node].keys():
                    for index_name in index_map[node][bucket_name].keys():
                        self.assertTrue(index_map[node][bucket_name][index_name]['total_scan_duration']  > 0, " scan did not happen at node {0}".format(node))
        except Exception, ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = False, drop_index = True)

    def _create_query_definitions(self, start= 0, index_count=2):
        query_definitions = []
        for ctr in range(start,start+index_count):
            index_name = "index_name_{0}".format(ctr)
            query_definition = QueryDefinition(index_name=index_name, index_fields = ["join_yr"], \
                    query_template = "SELECT * from %s WHERE join_yr == 2010 ", groups = [])
            query_definitions.append(query_definition)
        return query_definitions
