from newtuq import QueryTests
from couchbase_helper.query_definitions import SQLDefinitionGenerator

class BaseSecondaryIndexingTests(QueryTests):

    def setUp(self):
        super(BaseSecondaryIndexingTests, self).setUp()
        self.run_create_index= self.input.param("run_create_index",True)
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
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.ops_map = self._create_operation_map()
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()
        self.memory_create_list = []
        self.memory_drop_list = []

    def tearDown(self):
        super(BaseSecondaryIndexingTests, self).tearDown()

    def create_index(self, bucket, query_definition, verifycreate = True):
        self.query = query_definition.generate_index_create_query(bucket = bucket)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        if verifycreate:
            check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server = server)
            self.assertTrue(check, "index {0} failed to be created".format(query_definition.index_name))

    def async_create_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_create_query(bucket = bucket,
            use_gsi_for_secondary = self.use_gsi_for_secondary)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        create_index_task = self.cluster.async_create_index(
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name)
        return create_index_task

    def sync_create_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(bucket = bucket)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        create_index_task = self.cluster.create_index(self,
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name)
        return create_index_task

    def multi_create_index(self, buckets = [], query_definitions =[]):
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_create_query(bucket = bucket.name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.create_index(bucket.name, query_definition)

    def async_multi_create_index(self, buckets = [], query_definitions =[]):
        create_index_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_create_query(bucket = bucket.name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    create_index_tasks.append(self.async_create_index(bucket.name, query_definition))
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
        self.query = query_definition.generate_index_drop_query(bucket = bucket)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        if verifydrop:
            check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name)
            self.assertFalse(check, "index {0} failed to be deleted".format(query_definition.index_name))

    def async_drop_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(bucket = bucket)
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        drop_index_task = self.cluster.async_drop_index(
                 server = server, bucket = bucket,
                 query = self.query , n1ql_helper = self.n1ql_helper,
                 index_name = query_definition.index_name)
        return drop_index_task

    def sync_drop_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(bucket = bucket)
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

    def query_using_index(self, bucket, query_definition, expected_result = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        self._verify_results(sorted(actual_result['results']), sorted(expected_result))

    def async_query_using_index(self, bucket, query_definition, expected_result = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        query_with_index_task = self.cluster.async_n1ql_query_verification(
                 server = server, bucket = bucket,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 expected_result=expected_result)
        return query_with_index_task

    def sync_query_using_index(self, bucket, query_definition, expected_result = None):
        self.gen_results.query = query_definition.generate_query(bucket = bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result = False)
        self.query = self.gen_results.query
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.cluster.n1ql_query_verification(
                 server = server, bucket = bucket.name,
                 query = self.query, n1ql_helper = self.n1ql_helper,
                 expected_result=expected_result)

    def multi_query_using_index(self, buckets =[], query_definitions = [], expected_results = {}):
        for bucket in buckets:
            for query_definition in query_definitions:
                if expected_results:
                    self.query_using_index(bucket.name,
                        query_definition, expected_results[query_definition.index_name])
                else:
                     self.query_using_index(bucket.name,query_definition, None)

    def async_multi_query_using_index(self, buckets =[], query_definitions = [], expected_results = {}):
        multi_query_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                if expected_results:
                    multi_query_tasks.append(self.async_query_using_index(bucket.name,
                        query_definition, expected_results[query_definition.index_name]))
                else:
                     multi_query_tasks.append(self.async_query_using_index(bucket.name,query_definition, None))
        return multi_query_tasks

    def sync_multi_query_using_index(self, buckets =[], query_definitions = [], expected_results = {}):
        for bucket in buckets:
            for query_definition in query_definitions:
                if expected_results:
                    self.sync_query_using_index(bucket.name,
                        query_definition, expected_results[query_definition.index_name])
                else:
                     self.sync_query_using_index(bucket.name,query_definition, None)


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

    def async_check_and_run_operations(self, buckets = [], before = False, after = False, in_between = False):
        if before:
            return self._async_run_operations(buckets = buckets, create_index = self.ops_map["before"]["create_index"],
                drop_index = self.ops_map["before"]["drop_index"],
                run_queries = self.ops_map["before"]["query_ops"])
        if after:
            return self._async_run_operations(buckets = buckets, create_index = self.ops_map["after"]["create_index"],
                drop_index = self.ops_map["after"]["drop_index"],
                run_queries = self.ops_map["after"]["query_ops"])
        if in_between:
            return self._async_run_operations(buckets = buckets, create_index = self.ops_map["in_between"]["create_index"],
                drop_index = self.ops_map["in_between"]["drop_index"],
                run_queries = self.ops_map["in_between"]["query_ops"])

    def run_multi_operations(self, buckets = [], query_definitions = [], expected_results = {},
        create_index = False, drop_index = False, query_with_explain = False, query = False):
        try:
            if create_index:
                self.multi_create_index(buckets, query_definitions)
            if query:
                self.multi_query_using_index(buckets, query_definitions, expected_results)
            if query_with_explain:
                self.multi_query_using_index_with_explain(buckets, query_definitions)
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            if drop_index:
                self.multi_drop_index(self.buckets,query_definitions)

    def async_run_multi_operations(self, buckets = [], query_definitions = [], expected_results = {},
        create_index = False, drop_index = False, query_with_explain = False, query = False):
        tasks = []
        try:
            if create_index:
                tasks += self.async_multi_create_index(buckets, query_definitions)
            if query:
                tasks  += self.async_multi_query_using_index(buckets, query_definitions, expected_results)
            if query_with_explain:
                tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
            if drop_index:
                tasks += self.async_multi_drop_index(self.buckets, query_definitions)
        except Exception, ex:
            self.log.info(ex)
            raise Exception(ex)
        return tasks

    def _run_operations(self, buckets = [], create_index = False, run_queries = False, drop_index = False):
        self.run_multi_operations(buckets, query_definitions = self.query_definitions,
            create_index = create_index, drop_index = drop_index,
            query_with_explain = run_queries, query = run_queries)

    def _async_run_operations(self, buckets = [], create_index = False, run_queries = False, drop_index = False):
        return self.async_run_multi_operations(buckets, query_definitions = self.query_definitions,
            create_index = create_index, drop_index = drop_index,
            query_with_explain = run_queries, query = run_queries)

    def run_operations(self, bucket, query_definition, expected_results,
        create_index = False, drop_index = False, query_with_explain = False, query = False):
        self.run_multi_operations(buckets = [bucket], query_definitions = [query_definition],
            expected_results = {"0": expected_results},
            create_index = create_index, drop_index = drop_index,
            query_with_explain = query_with_explain, query = query)

    def negative_common_body(self, queries_errors={}):
        if not queries_errors:
            self.fail("No queries to run!")
        for bucket in self.buckets:
            for query_template, error in queries_errors.iteritems():
                try:
                    query = self.gen_results.generate_query(query_template)
                    actual_result = self.run_cbq_query(query.format(bucket.name))
                except CBQError as ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find(error) != -1,
                                    "Error is incorrect.Actual %s.\n Expected: %s.\n" %(
                                                                str(ex).split(':')[-1], error))
                else:
                    self.fail("There was no errors. Error expected: %s" % error)

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

    def _gen_dict(self, result):
        result_set = []
        if result != None and len(result) > 0:
            for val in result:
                for key in val.keys():
                    result_set.append(val[key])
        return result_set

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

