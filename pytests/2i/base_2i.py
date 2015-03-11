from newtuq import QueryTests
import random
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from couchbase_helper.query_definitions import QueryDefinition

class BaseSecondaryIndexingTests(QueryTests):

    def setUp(self):
        super(BaseSecondaryIndexingTests, self).setUp()
        self.initial_stats = None
        self.final_stats = None
        self.index_lost_during_move_out =[]
        self.use_where_clause_in_index= self.input.param("use_where_clause_in_index",False)
        self.check_stats= self.input.param("check_stats",True)
        self.scan_consistency= self.input.param("scan_consistency",None)
        self.scan_vector_per_values= self.input.param("scan_vector_per_values",None)
        self.timeout_for_index_online= self.input.param("timeout_for_index_online",120)
        self.max_attempts_check_index= self.input.param("max_attempts_check_index",10)
        self.max_attempts_query_and_validate= self.input.param("max_attempts_query_and_validate",10)
        self.index_present= self.input.param("index_present",True)
        self.run_create_index= self.input.param("run_create_index",True)
        self.defer_build= self.input.param("defer_build",None)
        self.deploy_on_particular_node= self.input.param("deploy_on_particular_node",None)
        self.run_drop_index= self.input.param("run_drop_index",True)
        self.run_query_with_explain= self.input.param("run_query_with_explain",True)
        self.run_query= self.input.param("run_query",True)
        self.graceful = self.input.param("graceful",False)
        self.groups = self.input.param("groups", "simple").split(":")
        query_definition_generator = SQLDefinitionGenerator()
        if self.dataset == "default" or self.dataset == "employee":
            self.query_definitions = query_definition_generator.generate_employee_data_query_definitions()
        if self.dataset == "simple":
            self.query_definitions = query_definition_generator.generate_simple_data_query_definitions()
        if self.dataset == "sabre":
            self.query_definitions = query_definition_generator.generate_sabre_data_query_definitions()
        if self.dataset == "bigdata":
            self.query_definitions = query_definition_generator.generate_big_data_query_definitions()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.ops_map = self._create_operation_map()
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()
        self.memory_create_list = []
        self.memory_drop_list = []

    def tearDown(self):
        super(BaseSecondaryIndexingTests, self).tearDown()

    def create_index(self, bucket, query_definition, deploy_node_info = None):
        index_where_clause = None
        if self.use_where_clause_in_index:
            index_where_clause = query_definition.index_where_clause
        self.query = query_definition.generate_index_create_query(bucket = bucket,
         use_gsi_for_secondary = self.use_gsi_for_secondary, deploy_node_info= deploy_node_info,
         defer_build = self.defer_build, index_where_clause = index_where_clause )
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        if not self.defer_build:
            check = self.n1ql_helper.is_index_online_and_in_list(bucket, query_definition.index_name, server = server)
            self.assertTrue(check, "index {0} failed to be created".format(query_definition.index_name))

    def async_create_index(self, bucket, query_definition, deploy_node_info = None):
        index_where_clause = None
        if self.use_where_clause_in_index:
            index_where_clause = query_definition.index_where_clause
        self.query = query_definition.generate_index_create_query(bucket = bucket,
            use_gsi_for_secondary = self.use_gsi_for_secondary, deploy_node_info = deploy_node_info,
            defer_build = self.defer_build, index_where_clause = index_where_clause)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        create_index_task = self.cluster.async_create_index(
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name, defer_build = self.defer_build)
        return create_index_task

    def async_monitor_index(self, bucket, index_name = None):
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        monitor_index_task = self.cluster.async_monitor_index(
                 server = server, bucket = bucket,
                 n1ql_helper = self.n1ql_helper,
                 index_name = index_name)
        return monitor_index_task

    def async_build_index(self, bucket = "default", index_list = []):
        self.query = self.n1ql_helper.gen_build_index_query(bucket = bucket, index_list = index_list)
        self.log.info(self.query)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        build_index_task = self.cluster.async_build_index(
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper)
        return build_index_task

    def sync_create_index(self, bucket, query_definition, deploy_node_info = None):
        self.query = query_definition.generate_index_create_query(bucket = bucket,
            use_gsi_for_secondary = self.use_gsi_for_secondary, deploy_node_info = deploy_node_info,
            defer_build = self.defer_build)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        create_index_task = self.cluster.create_index(self,
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name,  defer_build = self.defer_build)
        return create_index_task

    def multi_create_index(self, buckets = [], query_definitions =[]):
        self.index_lost_during_move_out =[]
        for bucket in buckets:
            index_node_count = 0
            for query_definition in query_definitions:
                index_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.deploy_node_info = None
                    if index_node_count < len(self.index_nodes_out):
                        self.deploy_node_info = "{0}:{1}".format(self.index_nodes_out[bucket.name][index_node_count].ip,
                        self.index_nodes_out[bucket.name][index_node_count].port)
                        self.index_lost_during_move_out.append(query_definition.index_name)
                        index_node_count += 1
                    self.create_index(bucket.name, query_definition, deploy_node_info = self.deploy_node_info)

    def async_multi_create_index(self, buckets = [], query_definitions =[]):
        create_index_tasks = []
        self.index_lost_during_move_out =[]
        for bucket in buckets:
            index_node_count = 0
            for query_definition in query_definitions:
                index_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.deploy_node_info = None
                    if index_node_count < len(self.index_nodes_out):
                        self.deploy_node_info = "{0}:{1}".format(self.index_nodes_out[bucket.name][index_node_count].ip,
                        self.index_nodes_out[bucket.name][index_node_count].port)
                        self.index_lost_during_move_out.append(query_definition.index_name)
                        index_node_count += 1
                    create_index_tasks.append(self.async_create_index(bucket.name, query_definition, deploy_node_info = self.deploy_node_info))
                    self.sleep(3)
        if self.defer_build:
            index_list = []
            for task in create_index_tasks:
                task.result()
            for query_definition in query_definitions:
                if query_definition.index_name not in index_list:
                    index_list.append(query_definition.index_name)
            build_index_task = self.async_build_index(bucket, index_list)
            build_index_task.result()
            monitor_index_tasks = []
            for index_name in index_list:
                for bucket in self.buckets:
                    monitor_index_tasks.append(self.async_monitor_index(bucket.name, index_name))
            return monitor_index_tasks
        else:
            return create_index_tasks

    def sync_multi_create_index(self, buckets = [], query_definitions =[]):
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_create_query(bucket = bucket.name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.sync_create_index(bucket.name, query_definition)


    def multi_drop_index(self, buckets = [], query_definitions =[]):
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(bucket = bucket.name)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    self.drop_index(bucket.name, query_definition)

    def async_multi_drop_index(self, buckets = [], query_definitions =[]):
        drop_index_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(bucket = bucket.name)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    drop_index_tasks.append(self.async_drop_index(bucket.name, query_definition))
        return drop_index_tasks

    def sync_multi_drop_index(self, buckets = [], query_definitions =[]):
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(bucket = bucket.name)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    self.sync_drop_index(bucket.name, query_definition)

    def drop_index(self, bucket, query_definition, verifydrop = True):
        check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name)
        self.assertTrue(check," cannot drop index {0} as it does not exist ".format(query_definition.index_name))
        self.query = query_definition.generate_index_drop_query(bucket = bucket,
          use_gsi_for_secondary = self.use_gsi_for_secondary, use_gsi_for_primary = self.use_gsi_for_primary)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        if verifydrop:
            check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name)
            self.assertFalse(check, "index {0} failed to be deleted".format(query_definition.index_name))

    def async_drop_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(bucket = bucket,
          use_gsi_for_secondary = self.use_gsi_for_secondary, use_gsi_for_primary = self.use_gsi_for_primary)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        drop_index_task = self.cluster.async_drop_index(
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name)
        return drop_index_task

    def sync_drop_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(bucket = bucket,
          use_gsi_for_secondary = self.use_gsi_for_secondary, use_gsi_for_primary = self.use_gsi_for_primary)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.cluster.drop_index(self,
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name)
        return drop_index_task

    def query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket = bucket)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        self.log.info(actual_result)
        check = self.n1ql_helper.verify_index_with_explain(actual_result, query_definition.index_name)
        self.assertTrue(check, "Index %s not found" % (query_definition.index_name))

    def async_query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket = bucket)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        query_with_index_task = self.cluster.async_n1ql_query_verification(
                 server = server, bucket = bucket,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 is_explain_query=True, index_name = query_definition.index_name)
        return query_with_index_task

    def sync_query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket = bucket)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.cluster.sync_n1ql_query_verification(
                 server = server, bucket = bucket.name,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 is_explain_query=True, index_name = query_definition.index_name)

    def multi_query_using_index_with_explain(self, buckets =[], query_definitions = []):
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_explain(bucket.name,
                    query_definition)

    def async_multi_query_using_index_with_explain(self, buckets =[], query_definitions = []):
        async_query_with_explain_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                async_query_with_explain_tasks.append(self.async_query_using_index_with_explain(bucket.name,
                    query_definition))
        return async_query_with_explain_tasks

    def sync_multi_query_using_index_with_explain(self, buckets =[], query_definitions = []):
        async_query_with_explain_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                self.sync_query_using_index_with_explain(bucket.name,query_definition)

    def query_using_index(self, bucket, query_definition, expected_result = None, scan_consistency = None, scan_vector = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        msg, check = self.n1ql_helper.run_query_and_verify_result(query = self.query, server = server, timeout = 420,
         expected_result = expected_result,scan_consistency = scan_consistency, scan_vector = scan_vector)
        self.assertTrue(check, msg)

    def async_query_using_index(self, bucket, query_definition, expected_result = None, scan_consistency = None, scan_vector = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        query_with_index_task = self.cluster.async_n1ql_query_verification(
                 server = server, bucket = bucket,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 expected_result=expected_result, index_name = query_definition.index_name,
                  scan_consistency = scan_consistency, scan_vector = scan_vector)
        return query_with_index_task

    def sync_query_using_index(self, bucket, query_definition, expected_result = None, scan_consistency = None, scan_vector = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.cluster.n1ql_query_verification(
                 server = server, bucket = bucket.name,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 expected_result=expected_result, scan_consistency = scan_consistency, scan_vector = scan_vector)

    def query_using_index_with_emptyset(self, bucket, query_definition):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        self.query = self.gen_results.query
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        self.verify_result_set_isempty(actual_result["results"])

    def multi_query_using_index_with_emptyresult(self, buckets =[], query_definitions = []):
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_emptyset(bucket.name, query_definition)

    def multi_query_using_index(self, buckets =[], query_definitions = [],
     expected_results = {}, scan_consistency = None, scan_vectors = None):
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    self.query_using_index(bucket.name, query_definition, expected_results[query_definition.index_name],
                     scan_consistency = scan_consistency, scan_vector = scan_vector)
                else:
                     self.query_using_index(bucket.name,query_definition, None,
                      scan_consistency = scan_consistency, scan_vector = scan_vector)

    def async_multi_query_using_index(self, buckets =[], query_definitions = [], expected_results = {}, scan_consistency = None, scan_vectors = None):
        multi_query_tasks = []
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    multi_query_tasks.append(self.async_query_using_index(bucket.name, query_definition, expected_results[query_definition.index_name],
                     scan_consistency = scan_consistency, scan_vector = scan_vector))
                else:
                    multi_query_tasks.append(self.async_query_using_index(bucket.name,query_definition, None,
                     scan_consistency = scan_consistency, scan_vector = scan_vector))
        return multi_query_tasks

    def sync_multi_query_using_index(self, buckets =[], query_definitions = [], expected_results = {}, scan_consistency = None, scan_vector = None):
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    self.sync_query_using_index(bucket.name,
                        query_definition, expected_results[query_definition.index_name],
                        scan_consistency = scan_consistency, scan_vector = scan_vector)
                else:
                     self.sync_query_using_index(bucket.name,query_definition, None,
                        scan_consistency = scan_consistency, scan_vector = scan_vector)

    def check_and_run_operations(self, buckets = [], before = False, after = False, in_between = False):
        if before:
            self._run_operations(buckets = buckets, create_index = self.ops_map["before"]["create_index"],
                drop_index = self.ops_map["before"]["drop_index"],
                run_queries = self.ops_map["before"]["query_ops"])
        if after:
            self._run_operations(buckets = buckets, create_index = self.ops_map["after"]["create_index"],
                drop_index = self.ops_map["after"]["drop_index"],
                run_queries = self.ops_map["after"]["query_ops"])
        if in_between:
            self._run_operations(buckets = buckets, create_index = self.ops_map["in_between"]["create_index"],
                drop_index = self.ops_map["in_between"]["drop_index"],
                run_queries = self.ops_map["in_between"]["query_ops"])

    def async_check_and_run_operations(self, buckets = [], before = False, after = False, in_between = False,
     scan_consistency = None, scan_vectors = None):
        # verify the stats
        if before:
            return self._async_run_operations(buckets = buckets, create_index = self.ops_map["before"]["create_index"],
                drop_index = self.ops_map["before"]["drop_index"],
                run_queries = self.ops_map["before"]["query_ops"], scan_consistency = scan_consistency, scan_vectors = scan_vectors)
        if after:
            return self._async_run_operations(buckets = buckets, create_index = self.ops_map["after"]["create_index"],
                drop_index = self.ops_map["after"]["drop_index"],
                run_queries = self.ops_map["after"]["query_ops"], scan_consistency = None, scan_vectors = None)
        if in_between:
            return self._async_run_operations(buckets = buckets, create_index = self.ops_map["in_between"]["create_index"],
                drop_index = self.ops_map["in_between"]["drop_index"],
                run_queries = self.ops_map["in_between"]["query_ops"], scan_consistency = None, scan_vectors = None)

    def run_multi_operations(self, buckets = [], query_definitions = [], expected_results = {},
        create_index = False, drop_index = False, query_with_explain = False, query = False,
         scan_consistency = None, scan_vectors = None):
        try:
            if create_index:
                self.multi_create_index(buckets, query_definitions)
                self.initial_stats = self.get_index_stats(perNode=True)
            if query_with_explain:
                self.multi_query_using_index_with_explain(buckets, query_definitions)
            if query:
                self.multi_query_using_index(buckets, query_definitions,
                 expected_results, scan_consistency = scan_consistency,
                 scan_vectors = scan_vectors)
                self._get_final_stats_snap_shot()
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            if drop_index:
                self.multi_drop_index(self.buckets,query_definitions)

    def async_run_multi_operations(self, buckets = [], query_definitions = [], expected_results = {},
        create_index = False, drop_index = False, query_with_explain = False, query = False,
         scan_consistency = None, scan_vectors = None):
        tasks = []
        try:
            if create_index:
                tasks += self.async_multi_create_index(buckets, query_definitions)
            if query:
                tasks  += self.async_multi_query_using_index(buckets, query_definitions, expected_results,
                 scan_consistency = scan_consistency, scan_vectors = scan_vectors)
            if query_with_explain:
                tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
            if drop_index:
                tasks += self.async_multi_drop_index(self.buckets, query_definitions)
        except Exception, ex:
            self.log.info(ex)
            raise
        return tasks

    def _run_operations(self, buckets = [], create_index = False, run_queries = False, drop_index = False):
        self.run_multi_operations(buckets, query_definitions = self.query_definitions,
            create_index = create_index, drop_index = drop_index,
            query_with_explain = run_queries, query = run_queries)

    def _async_run_operations(self, buckets = [], create_index = False, run_queries = False, drop_index = False,
     scan_consistency = None, scan_vectors = None):
        return self.async_run_multi_operations(buckets, query_definitions = self.query_definitions,
            create_index = create_index, drop_index = drop_index,
            query_with_explain = run_queries, query = run_queries,
            scan_consistency = scan_consistency, scan_vectors = scan_vectors)

    def run_operations(self, bucket, query_definition, expected_results,
        create_index = False, drop_index = False, query_with_explain = False, query = False):
        self.run_multi_operations(buckets = [bucket], query_definitions = [query_definition],
            expected_results = {"0": expected_results},
            create_index = create_index, drop_index = drop_index,
            query_with_explain = query_with_explain, query = query)

    def gen_scan_vector(self, use_percentage = 1.0, use_random = False):
        servers = self.get_kv_nodes(servers= self.servers[:self.nodes_init])
        sequence_bucket_map = self.get_vbucket_seqnos(servers,self.buckets)
        scan_vectors ={}
        if use_percentage == 1.0:
            for bucket in self.buckets:
                scan_vector = []
                self.log.info("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for key in map.keys():
                    value = {"seqno":map[key]["abs_high_seqno"],"guard":map[key]["uuid"]}
                    scan_vector.append(value)
                scan_vectors[bucket.name] = scan_vector
        else:
            for bucket in self.buckets:
                scan_vector = {}
                total = int(self.vbuckets*use_percentage)
                vbuckets_number_list = range(0,total)
                if use_random:
                    vbuckets_number_list  =  random.sample(xrange(0,self.vbuckets), total)
                self.log.info("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for key in map.keys():
                    vb = int(key.split("vb_")[1])
                    if vb in vbuckets_number_list:
                        value = {"seqno":map[key]["abs_high_seqno"],"guard":map[key]["uuid"]}
                        scan_vector[str(vb)] = value
                scan_vectors[bucket.name] = scan_vector
        return scan_vectors

    def check_missing_and_extra(self, actual, expected):
        missing = []
        extra = []
        for item in actual:
            if not (item in expected):
                 extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def _verify_results(self, actual_result, expected_result, missing_count = 1, extra_count = 1):
        actual_result = self._gen_dict(actual_result)
        expected_result = self._gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result, expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:missing_count], extra[:extra_count]))
            self.fail("Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                                            len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" +\
        "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(actual_result == expected_result,
                          msg % (actual_result[:100],actual_result[-100:],
                                 expected_result[:100],expected_result[-100:]))

    def verify_index_absence(self, query_definitions, buckets):
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        for bucket in buckets:
            for query_definition in query_definitions:
                check = self.n1ql_helper._is_index_in_list(bucket.name, query_definition.index_name, server = server)
                self.assertFalse(check, " {0} was not absent as expected".format(query_definition.index_name))

    def verify_result_set_isempty(self,result):
        self.assertTrue(len(result) == 0, "Result is not empty {0}".format(result))

    def _gen_dict(self, result):
        result_set = []
        if result != None and len(result) > 0:
            for val in result:
                for key in val.keys():
                    result_set.append(val[key])
        return result_set

    def _get_stats_snap_shot_after_create_index(self):
        self.initial_stats = self.get_index_stats(perNode=True)

    def _get_final_stats_snap_shot(self):
        if self.initial_stats != None:
            self.final_stats = self.get_index_stats(perNode=True)

    def _verify_items_count(self):
        index_map = self.get_index_stats()
        bucketMap = self.get_buckets_itemCount()
        for bucket_name in index_map.keys():
            for index_name in index_map[bucket_name].keys():
                actual_item_count = index_map[bucket_name][index_name]["items_count"]
                expected_item_count = bucketMap[bucket_name]
                self.assertTrue(str(actual_item_count) == str(expected_item_count), "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                    (bucket_name, index_name, expected_item_count, actual_item_count))

    def _verify_stats_before_after(self):
        if self.check_stats and self.initial_stats and self.final_stats:
            for node in self.final_stats.keys():
                for bucket_name in self.final_stats[node]:
                    for index_name in self.final_stats[node][bucket_name].keys():
                        final_stats = self.final_stats[node][bucket_name][index_name]
                        initial_stats = self.final_stats[node][bucket_name][index_name]
                        self.assertTrue(final_stats["items_count"] ==  initial_stats["items_count"], \
                            " expect items_count mismatch, expected {0} != actual {1}".format(final_stats["items_count"] ,initial_stats["items_count"]))

    def _create_operation_map(self):
        map_before = {"create_index":False, "query_ops": False, "drop_index": False}
        map_in_between = {"create_index":False, "query_ops": False, "drop_index": False}
        map_after = {"create_index":False, "query_ops": False, "drop_index": False}
        before = self.input.param("before", "create_index")
        for op_type in before.split(":"):
            if op_type != '':
                map_before[op_type] = True
        in_between = self.input.param("in_between", "query_ops")
        for op_type in in_between.split(":"):
            if op_type != '':
                map_in_between[op_type] = True
        after = self.input.param("after", "drop_index")
        for op_type in after.split(":"):
            if op_type != '':
                map_after[op_type] = True
        return {"before":map_before, "in_between": map_in_between, "after": map_after}

    def _generate_employee_data_query_definitions_for_index_where_clause(self):
        definitions_list = []
        emit_fields = "*"
        and_conditions = ["job_title == \"Sales\"","job_title != \"Sales\""]
        definitions_list.append(
            QueryDefinition(
                index_name="index_0",
                             index_fields = ["job_title"],
                             query_template = QueryDefinition.RANGE_SCAN_ORDER_BY_TEMPLATE.format(emit_fields,"job_title IS NOT NULL","job_title"),
                             groups = [SIMPLE_INDEX, FULL_SCAN, ORDER_BY, "employee","isnotnull"], index_where_clause = " job_title IS NOT NULL "))
        definitions_list.append(
            QueryDefinition(
                index_name="index_1",
                             index_fields = ["job_title"],
                             query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title != \"Sales\""),
                             groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, EQUALS,"employee"], index_where_clause = " job_title != \"Sales\" "))
        definitions_list.append(
            QueryDefinition(
                index_name="index_2",
                             index_fields = ["job_title"],
                             query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % " job_title == \"Sales\" "),
                             groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, NOTEQUALS,"employee"], index_where_clause = " job_title == \"Sales\" "))
        definitions_list.append(
            QueryDefinition(
                index_name="index_3",
                             index_fields = ["job_title"],
                             query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\" or job_title == \"Engineer\""),
                             groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, OR,"employee"], index_where_clause = " job_title == \"Sales\" or job_title == \"Engineer\" "))
        definitions_list.append(
            QueryDefinition(
                index_name="index_4",
                             index_fields = ["join_yr"],
                             query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "join_yr > 2010 and join_yr < 2014"),
                             groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, AND,"employee"], index_where_clause = " join_yr > 2010 and join_yr < 2014 "))
        definitions_list.append(
            QueryDefinition(
                index_name="index_5",
                             index_fields = ["join_yr"],
                             query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "join_yr > 1999"),
                             groups = [SIMPLE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, GREATER_THAN,"employee"], index_where_clause = " join_yr > 1999 "))
        definitions_list.append(
            QueryDefinition(
                index_name="index_6",
                             index_fields = ["join_yr","job_title"],
                             query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\" and join_yr > 2010 and join_yr < 2014"),
                             groups = [COMPOSITE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, EQUALS,AND,"employee"], index_where_clause = " job_title == \"Sales\" and join_yr > 2010 and join_yr < 2014 "))
        definitions_list.append(
            QueryDefinition(
                index_name="index_7",
                             index_fields = ["join_yr","job_title"],
                             query_template = RANGE_SCAN_TEMPLATE.format(emit_fields," %s " % "job_title == \"Sales\" or join_yr > 2010 and join_yr < 2014 ORDER BY job_title"),
                             groups = [COMPOSITE_INDEX,RANGE_SCAN, NO_ORDERBY_GROUPBY, EQUALS,OR,"employee"], index_where_clause = "job_title == \"Sales\" or join_yr > 2010 and join_yr < 2014"))
        return definitions_list

