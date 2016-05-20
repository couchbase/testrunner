import logging
import random

from newtuq import QueryTests
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from membase.api.rest_client import RestConnection

log = logging.getLogger(__name__)

class BaseSecondaryIndexingTests(QueryTests):

    def setUp(self):
        super(BaseSecondaryIndexingTests, self).setUp()
        self.initial_stats = None
        self.final_stats = None
        self.index_lost_during_move_out =[]
        self.verify_using_index_status = self.input.param("verify_using_index_status",False)
        self.use_replica_when_active_down = self.input.param("use_replica_when_active_down",True)
        self.use_where_clause_in_index= self.input.param("use_where_clause_in_index",False)
        self.check_stats= self.input.param("check_stats",True)
        self.create_index_usage= self.input.param("create_index_usage","no_usage")
        self.scan_consistency= self.input.param("scan_consistency","request_plus")
        self.scan_vector_per_values= self.input.param("scan_vector_per_values",None)
        self.timeout_for_index_online= self.input.param("timeout_for_index_online",600)
        self.max_attempts_check_index= self.input.param("max_attempts_check_index",10)
        self.max_attempts_query_and_validate= self.input.param("max_attempts_query_and_validate",10)
        self.index_present= self.input.param("index_present",True)
        self.run_create_index= self.input.param("run_create_index",True)
        self.verify_query_result= self.input.param("verify_query_result",True)
        self.verify_explain_result= self.input.param("verify_explain_result",True)
        self.defer_build= self.input.param("defer_build",True)
        self.deploy_on_particular_node= self.input.param("deploy_on_particular_node",None)
        self.run_drop_index= self.input.param("run_drop_index",True)
        self.run_query_with_explain= self.input.param("run_query_with_explain",True)
        self.run_query= self.input.param("run_query",True)
        self.graceful = self.input.param("graceful",False)
        self.groups = self.input.param("groups", "all").split(":")
        self.use_rest = self.input.param("use_rest", False)
        if not self.use_rest:
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
        self.log.info(self.ops_map)
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()
        self.memory_create_list = []
        self.memory_drop_list = []
        self.n1ql_node = self.get_nodes_from_services_map(service_type = "n1ql")
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.index_loglevel = self.input.param("index_loglevel", None)
        if self.index_loglevel:
            self.set_indexer_logLevel(self.index_loglevel)
        if self.dgm_run:
            self._load_doc_data_all_buckets(gen_load=self.gens_load)

    def tearDown(self):
        super(BaseSecondaryIndexingTests, self).tearDown()

    def create_index(self, bucket, query_definition, deploy_node_info = None):
        index_where_clause = None
        self._defer_build_analyze()
        if self.use_where_clause_in_index:
            index_where_clause = query_definition.index_where_clause
        self.query = query_definition.generate_index_create_query(bucket = bucket,
         use_gsi_for_secondary = self.use_gsi_for_secondary, deploy_node_info= deploy_node_info,
         defer_build = self.defer_build, index_where_clause = index_where_clause)
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        if self.defer_build:
            build_index_task = self.async_build_index(bucket, [query_definition.index_name])
            build_index_task.result()
        check = self.n1ql_helper.is_index_online_and_in_list(bucket, query_definition.index_name,server = self.n1ql_node, timeout = self.timeout_for_index_online)
        self.assertTrue(check, "index {0} failed to be created".format(query_definition.index_name))

    def async_create_index(self, bucket, query_definition, deploy_node_info = None):
        self._defer_build_analyze()
        index_where_clause = None
        if self.use_where_clause_in_index:
            index_where_clause = query_definition.index_where_clause
        self.query = query_definition.generate_index_create_query(bucket = bucket,
            use_gsi_for_secondary = self.use_gsi_for_secondary, deploy_node_info = deploy_node_info,
            defer_build = self.defer_build, index_where_clause = index_where_clause)
        create_index_task = self.cluster.async_create_index(
                 server = self.n1ql_node, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name, defer_build = self.defer_build,
                  timeout = self.timeout_for_index_online)
        return create_index_task

    def async_monitor_index(self, bucket, index_name = None):
        monitor_index_task = self.cluster.async_monitor_index(
                 server = self.n1ql_node, bucket = bucket,
                 n1ql_helper = self.n1ql_helper,
                 index_name = index_name,
                 timeout = self.timeout_for_index_online)
        return monitor_index_task

    def async_build_index(self, bucket = "default", index_list = []):
        self.query = self.n1ql_helper.gen_build_index_query(bucket = bucket, index_list = index_list)
        self.log.info(self.query)
        build_index_task = self.cluster.async_build_index(
                 server = self.n1ql_node, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper)
        return build_index_task

    def multi_create_index(self, buckets = [], query_definitions =[]):
        self.index_lost_during_move_out =[]
        index_node_count = 0
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.deploy_node_info = None
                    if index_node_count < len(self.index_nodes_out):
                        self.deploy_node_info = "{0}:{1}".format(node.ip, node.port)
                        self.index_lost_during_move_out.append(query_definition.index_name)
                        index_node_count += 1
                    self.create_index(bucket.name, query_definition, deploy_node_info = self.deploy_node_info)

    def initialize_multi_create_index(self, buckets = [], query_definitions =[], deploy_node_info = None):
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.create_index(bucket.name, query_definition, deploy_node_info)

    def async_multi_create_index(self, buckets = [], query_definitions =[]):
        create_index_tasks = []
        self.index_lost_during_move_out =[]
        self.log.info(self.index_nodes_out)
        index_node_count = 0
        for query_definition in query_definitions:
                index_info = "{0}".format(query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.deploy_node_info = None
                    if index_node_count < len(self.index_nodes_out):
                        node_index = index_node_count
                        self.deploy_node_info = ["{0}:{1}".format(self.index_nodes_out[index_node_count].ip,
                        self.index_nodes_out[index_node_count].port)]
                        if query_definition.index_name not in self.index_lost_during_move_out:
                            self.index_lost_during_move_out.append(query_definition.index_name)
                        index_node_count += 1
                    for bucket in buckets:
                        create_index_tasks.append(self.async_create_index(bucket.name,
                            query_definition, deploy_node_info = self.deploy_node_info))
                    self.sleep(3)
        if self.defer_build:
            index_list = []
            for task in create_index_tasks:
                task.result()
            for query_definition in query_definitions:
                if query_definition.index_name not in index_list:
                    index_list.append(query_definition.index_name)
            for bucket in self.buckets:
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
                index_info = query_definition.generate_index_create_query(bucket=bucket.name)
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
        try:
            self.query = query_definition.generate_index_drop_query(bucket = bucket,
            use_gsi_for_secondary = self.use_gsi_for_secondary, use_gsi_for_primary = self.use_gsi_for_primary)
            actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            if verifydrop:
                check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server = self.n1ql_node)
                self.assertFalse(check, "index {0} failed to be deleted".format(query_definition.index_name))
        except Exception, ex:
                self.log.info(ex)
                query = "select * from system:indexes"
                actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_node)
                self.log.info(actual_result)


    def async_drop_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(bucket = bucket,
          use_gsi_for_secondary = self.use_gsi_for_secondary, use_gsi_for_primary = self.use_gsi_for_primary)
        drop_index_task = self.cluster.async_drop_index(
                 server = self.n1ql_node, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name)
        return drop_index_task

    def sync_drop_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(bucket = bucket,
          use_gsi_for_secondary = self.use_gsi_for_secondary, use_gsi_for_primary = self.use_gsi_for_primary)
        self.cluster.drop_index(self,
                 server = self.n1ql_node, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name)
        return drop_index_task

    def query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket = bucket)
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.log.info(actual_result)
        if self.verify_explain_result:
            check = self.n1ql_helper.verify_index_with_explain(actual_result, query_definition.index_name)
            self.assertTrue(check, "Index %s not found" % (query_definition.index_name))

    def async_query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket = bucket)
        query_with_index_task = self.cluster.async_n1ql_query_verification(
                 server = self.n1ql_node, bucket = bucket,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 is_explain_query=True, index_name = query_definition.index_name,
                 verify_results = self.verify_explain_result)
        return query_with_index_task

    def sync_query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket = bucket)
        self.cluster.sync_n1ql_query_verification(
                 server = self.n1ql_node, bucket = bucket.name,
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

    def query_using_index(self, bucket, query_definition, expected_result=None, scan_consistency=None,
                          scan_vector=None, verify_results=True):
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result=False)
        self.query = self.gen_results.query
        msg, check = self.n1ql_helper.run_query_and_verify_result(query=self.query, server=self.n1ql_node, timeout=420,
                                            expected_result=expected_result, scan_consistency=scan_consistency,
                                            scan_vector=scan_vector, verify_results=verify_results)
        self.assertTrue(check, msg)

    def async_query_using_index(self, bucket, query_definition, expected_result = None, scan_consistency = None, scan_vector = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        query_with_index_task = self.cluster.async_n1ql_query_verification(
                 server = self.n1ql_node, bucket = bucket,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 expected_result=expected_result, index_name = query_definition.index_name,
                  scan_consistency = scan_consistency, scan_vector = scan_vector,
                  verify_results = self.verify_query_result)
        return query_with_index_task

    def sync_query_using_index(self, bucket, query_definition, expected_result = None, scan_consistency = None, scan_vector = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        self.cluster.n1ql_query_verification(
                 server = self.n1ql_node, bucket = bucket.name,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 expected_result=expected_result, scan_consistency = scan_consistency, scan_vector = scan_vector)

    def query_using_index_with_emptyset(self, bucket, query_definition):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        self.query = self.gen_results.query
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(len(actual_result["results"]) == 0, "Result is not empty {0}".format(actual_result["results"]))

    def multi_query_using_index_with_emptyresult(self, buckets =[], query_definitions = []):
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_emptyset(bucket.name, query_definition)

    def multi_query_using_index(self, buckets=[], query_definitions=[],
                                expected_results={}, scan_consistency=None,
                                scan_vectors=None, verify_results=True):
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    self.query_using_index(bucket.name, query_definition, expected_results[query_definition.index_name],
                                           scan_consistency=scan_consistency, scan_vector=scan_vector,
                                           verify_results=verify_results)
                else:
                     self.query_using_index(bucket.name,query_definition, None, scan_consistency=scan_consistency,
                                            scan_vector=scan_vector, verify_results=verify_results)

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

    def check_and_run_operations(self, buckets = [], initial = False, before = False, after = False, in_between = False):
        #self.verify_query_result = True
        #self.verify_explain_result = True
        if initial:
            self._set_query_explain_flags("initial")
            self._run_operations(buckets = buckets, create_index = self.ops_map["initial"]["create_index"],
                drop_index = self.ops_map["initial"]["drop_index"],
                queries = self.ops_map["initial"]["query_ops"],
                queries_with_explain = self.ops_map["initial"]["query_explain_ops"])
        if before:
            self._set_query_explain_flags("before")
            self._run_operations(buckets = buckets, create_index = self.ops_map["before"]["create_index"],
                drop_index = self.ops_map["before"]["drop_index"],
                queries = self.ops_map["before"]["query_ops"],
                queries_with_explain = self.ops_map["before"]["query_explain_ops"])
        if in_between:
            self._set_query_explain_flags("in_between")
            self._run_operations(buckets = buckets, create_index = self.ops_map["in_between"]["create_index"],
                drop_index = self.ops_map["in_between"]["drop_index"],
                queries = self.ops_map["in_between"]["query_ops"],
                queries_with_explain = self.ops_map["before"]["query_explain_ops"])
        if after:
            self._set_query_explain_flags("after")
            self._run_operations(buckets = buckets, create_index = self.ops_map["after"]["create_index"],
                drop_index = self.ops_map["after"]["drop_index"],
                queries = self.ops_map["after"]["query_ops"],
                queries_with_explain = self.ops_map["before"]["query_explain_ops"])

    def async_check_and_run_operations(self, buckets = [],
     initial = False, before = False, after = False, in_between = False,
     scan_consistency = None, scan_vectors = None):
        #self.verify_query_result = True
        #self.verify_explain_result = True
        if initial:
            self._set_query_explain_flags("initial")
            return self._async_run_operations(buckets = buckets,
                create_index = self.ops_map["initial"]["create_index"],
                drop_index = self.ops_map["initial"]["drop_index"],
                queries = self.ops_map["initial"]["query_ops"],
                queries_with_explain = self.ops_map["initial"]["query_explain_ops"],
                scan_consistency = scan_consistency,
                scan_vectors = scan_vectors)
        if before:
            self._set_query_explain_flags("before")
            return self._async_run_operations(buckets = buckets,
                create_index = self.ops_map["before"]["create_index"] ,
                drop_index = self.ops_map["before"]["drop_index"],
                queries = self.ops_map["before"]["query_ops"],
                queries_with_explain = self.ops_map["before"]["query_explain_ops"],
                scan_consistency = scan_consistency,
                scan_vectors = scan_vectors)
        if in_between:
            self._set_query_explain_flags("in_between")
            return self._async_run_operations(buckets = buckets,
                create_index = self.ops_map["in_between"]["create_index"],
                drop_index = self.ops_map["in_between"]["drop_index"],
                queries = self.ops_map["in_between"]["query_ops"],
                queries_with_explain = self.ops_map["in_between"]["query_explain_ops"],
                scan_consistency = scan_consistency,
                scan_vectors = scan_vectors)
        if after:
            self._set_query_explain_flags("after")
            return self._async_run_operations(buckets = buckets,
                create_index = self.ops_map["after"]["create_index"],
                drop_index = self.ops_map["after"]["drop_index"],
                queries = self.ops_map["after"]["query_ops"],
                queries_with_explain = self.ops_map["after"]["query_explain_ops"],
                scan_consistency = "request_plus",
                scan_vectors = scan_vectors)

    def run_multi_operations(self, buckets = [], query_definitions = [], expected_results = {},
        create_index = False, drop_index = False, query_with_explain = False, query = False,
         scan_consistency = None, scan_vectors = None):
        try:
            if create_index:
                self.multi_create_index(buckets, query_definitions)
            if query_with_explain:
                self.multi_query_using_index_with_explain(buckets, query_definitions)
            if query:
                self.multi_query_using_index(buckets, query_definitions,
                 expected_results, scan_consistency = scan_consistency,
                 scan_vectors = scan_vectors)
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            if drop_index and not self.skip_cleanup:
                self.multi_drop_index(buckets,query_definitions)

    def async_run_multi_operations(self, buckets = [], query_definitions = [], expected_results = {},
        create_index = False, drop_index = False, query_with_explain = False, query = False,
         scan_consistency = None, scan_vectors = None):
        tasks = []
        try:
            if create_index:
                tasks += self.async_multi_create_index(buckets, query_definitions)
            if query_with_explain:
                tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
            if query:
                tasks  += self.async_multi_query_using_index(buckets, query_definitions, expected_results,
                 scan_consistency = scan_consistency, scan_vectors = scan_vectors)
            if drop_index:
                tasks += self.async_multi_drop_index(self.buckets, query_definitions)
        except Exception, ex:
            self.log.info(ex)
            raise
        return tasks

    def _run_operations(self, buckets = [], create_index = False, queries = False, queries_explain= False, drop_index = False):
        self.run_multi_operations(buckets, query_definitions = self.query_definitions,
            create_index = create_index, drop_index = drop_index,
            query_with_explain = queries_explain, query = queries)

    def _async_run_operations(self, buckets = [], create_index = False,
     queries_with_explain = False, queries = False, drop_index = False,
     scan_consistency = None, scan_vectors = None):
        return self.async_run_multi_operations(buckets, query_definitions = self.query_definitions,
            create_index = create_index, drop_index = drop_index,
            query_with_explain = queries_with_explain, query = queries,
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
                for i in range(1024):
                    key = "vb_" + str(i)
                    value = [ int(map[key]["abs_high_seqno"]), map[key]["uuid"] ]
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
                        value = [ int(map[key]["abs_high_seqno"]), map[key]["uuid"] ]
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

    def _gen_dict(self, result):
        result_set = []
        if result != None and len(result) > 0:
            for val in result:
                for key in val.keys():
                    result_set.append(val[key])
        return result_set

    def _verify_index_map(self):
        if not self.verify_using_index_status:
            return
        index_map = self.get_index_map()
        index_bucket_map = self.n1ql_helper.gen_index_map(self.n1ql_node)
        msg = "difference in index map found, expected {0} \n actual {1}".format(index_bucket_map,index_map)
        self.assertTrue(len(index_map.keys()) == len(self.buckets),
            "numer of buckets mismatch :: "+msg)
        for bucket in self.buckets:
            self.assertTrue((bucket.name in index_map.keys()), " bucket name not present in index map {0}".format(index_map))
        for bucket_name in index_bucket_map.keys():
            self.assertTrue(len(index_bucket_map[bucket_name].keys()) == len(index_map[bucket_name].keys()),"number of indexes mismatch ::"+msg)
            for index_name in index_bucket_map[bucket_name].keys():
                msg1 ="index_name {0} not found in {1}".format(index_name, index_map[bucket_name].keys())
                self.assertTrue(index_name in index_map[bucket_name].keys(), msg1+" :: "+ msg)

    def _verify_primary_index_count(self):
        bucket_map = self.get_buckets_itemCount()
        count = 0
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(5)
            count += 1
        self.assertTrue(self._verify_items_count(), "All Items didn't get Indexed...")
        self.log.info("All the documents are indexed...")
        self.sleep(10)
        index_bucket_map = self.n1ql_helper.get_index_count_using_primary_index(self.buckets, self.n1ql_node)
        self.log.info(bucket_map)
        self.log.info(index_bucket_map)
        for bucket_name in bucket_map.keys():
            actual_item_count = index_bucket_map[bucket_name]
            expected_item_count = bucket_map[bucket_name]
            self.assertTrue(str(actual_item_count) == str(expected_item_count),
                "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                    (bucket_name, "primary", expected_item_count, actual_item_count))

    def _verify_items_count(self):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        index_map = self.get_index_stats()
        for bucket_name in index_map.keys():
            for index_name, index_val in index_map[bucket_name].iteritems():
                if index_val["num_docs_pending"] and index_val["num_docs_queued"]:
                    return False
        return True

    def _verify_bucket_count_with_index_count(self, query_definitions, buckets=[]):
        """

        :param bucket:
        :param index:
        :return:
        """
        if not buckets:
            buckets = self.buckets
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(5)
            count += 1
        if not self._verify_items_count():
            self.log.info("All Items didn't get Indexed...")
            raise
        bucket_map = self.get_buckets_itemCount()
        for bucket in buckets:
            bucket_count = bucket_map[bucket.name]
            for query in query_definitions:
                index_count = self.n1ql_helper.get_index_count_using_index(
                                                        bucket, query.index_name)
                self.assertTrue(int(index_count) == int(bucket_count),
                        "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                        (bucket.name, query.index_name, bucket_count, index_count))
        self.log.info("Items Indexed Verified with bucket count...")

    def _create_operation_map(self):
        map_initial = {"create_index":False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_before = {"create_index":False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_in_between = {"create_index":False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_after = {"create_index":False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        initial = self.input.param("initial", "")
        for op_type in initial.split(":"):
            if op_type != '':
                map_initial[op_type] = True
        before = self.input.param("before", "")
        for op_type in before.split(":"):
            if op_type != '':
                map_before[op_type] = True
        in_between = self.input.param("in_between", "")
        for op_type in in_between.split(":"):
            if op_type != '':
                map_in_between[op_type] = True
        after = self.input.param("after", "")
        for op_type in after.split(":"):
            if op_type != '':
                map_after[op_type] = True
        return {"initial":map_initial, "before":map_before, "in_between": map_in_between, "after": map_after}

    def _create_index_in_async(self, query_definitions = None, buckets = None, index_nodes = None):
        refer_index = []
        self._defer_build_analyze()
        if buckets == None:
            buckets = self.buckets
        if query_definitions == None:
            query_definitions = self.query_definitions
        if not self.run_async:
            self.run_multi_operations(buckets = buckets, query_definitions = query_definitions, create_index = True)
            return
        if index_nodes == None:
            index_nodes = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        check = True
        x =  len(query_definitions)-1
        while x > -1:
            tasks = []
            build_index_map ={}
            for bucket in buckets:
                build_index_map[bucket.name]=[]
            for server in index_nodes:
                for bucket in buckets:
                    if (x > -1):
                        key = "{0}:{1}".format(bucket.name, query_definitions[x].index_name)
                        if (key not in refer_index):
                            refer_index.append(key)
                            refer_index.append(query_definitions[x].index_name)
                            deploy_node_info = None
                            if self.use_gsi_for_secondary:
                                deploy_node_info = ["{0}:{1}".format(server.ip,server.port)]
                            build_index_map[bucket.name].append(query_definitions[x].index_name)
                            tasks.append(self.async_create_index(bucket.name, query_definitions[x], deploy_node_info = deploy_node_info))
                x-=1
            for task in tasks:
                task.result()
            if self.defer_build:
                for bucket_name in build_index_map.keys():
                    if len(build_index_map[bucket_name]) > 0:
                        build_index_task = self.async_build_index(bucket_name, build_index_map[bucket_name])
                        build_index_task.result()
                monitor_index_tasks = []
                for bucket_name in build_index_map.keys():
                    for index_name in build_index_map[bucket_name]:
                        monitor_index_tasks.append(self.async_monitor_index(bucket_name, index_name))
                for task in monitor_index_tasks:
                    task.result()

    def _query_explain_in_async(self):
        tasks = self.async_run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = False, drop_index = False,
            query_with_explain = self.run_query_with_explain,
            query = False, scan_consistency = self.scan_consistency)
        for task in tasks:
            task.result()
        tasks = self.async_run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = False, drop_index = False,
            query_with_explain = False, query = self.run_query,
            scan_consistency = self.scan_consistency)
        for task in tasks:
            task.result()

    def _set_query_explain_flags(self, phase):
        if ("query_ops" in self.ops_map[phase].keys()) and self.ops_map[phase]["query_ops"]:
            self.ops_map[phase]["query_explain_ops"] = True
        if ("do_not_verify_query_result" in self.ops_map[phase].keys()) and self.ops_map[phase]["do_not_verify_query_result"]:
            self.verify_query_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        if ("do_not_verify_explain_result" in self.ops_map[phase].keys()) and self.ops_map[phase]["do_not_verify_explain_result"]:
            self.verify_explain_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        self.log.info(self.ops_map)

    def _defer_build_analyze(self):
        self.defer_build = self.defer_build and self.use_gsi_for_secondary
        if not self.defer_build:
            self.defer_build = None

    def set_indexer_logLevel(self, loglevel="info"):
        """
        :param loglevel:
        Possible Values
            -- info
            -- debug
            -- warn
            -- verbose
            -- Silent
            -- Fatal
            -- Error 
            -- Timing
            -- Trace
        """
        self.log.info("Setting indexer log level to {0}".format(loglevel))
        server = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(server)
        status = rest.set_indexer_params("logLevel", loglevel)

