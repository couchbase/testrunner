from tuqquery.newtuq import QueryTests

class BaseSecondaryIndexingTests(QueryTests):

    def setUp(self):
        super(BaseSecondaryIndexingTests, self).setUp()
        self.groups = self.input.param("groups", "simple").split(":")
        sql_definition_generator = SQLDefinitionGenerator()
        if self.dataset == "default" or self.dataset == "employee":
            self.sql_definitions = sql_definition_generator.generate_default_data_sql_definitions()
        self.sql_definitions = sql_definition_generator.filter_by_group(self.groups, self.sql_definitions)
        self.ops_map = self._create_operation_map()
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()

    def suite_setUp(self):
        super(BaseSecondaryIndexingTests, self).suite_setUp()

    def tearDown(self):
        super(BaseSecondaryIndexingTests, self).tearDown()

    def suite_tearDown(self):
        super(BaseSecondaryIndexingTests, self).suite_tearDown()

    def create_index(self, bucket, query_definition, verifycreate = True):
        self.query = query_definition.generate_indexcreate_query(bucket = bucket)
        actual_result = self.run_cbq_query()
        if verifycreate:
            check = self._is_index_in_list(bucket, query_definition.index_name)
            self.assertTrue(check, "index {0} failed to be created".format(query_definition.index_name))

    def multi_create_index(self, buckets = [], query_definitions =[]):
        for bucket in buckets:
            for query_definition in query_definitions:
                self.create_index(bucket.name, query_definition)

    def multi_drop_index(self, buckets = [], query_definitions =[]):
        for bucket in buckets:
            for query_definition in query_definitions:
                self.drop_index(bucket.name, query_definition)

    def drop_index(self, bucket, query_definition, verifydrop = True):
        self.query = query_definition.generate_indexcreate_query(bucket = bucket)
        actual_result = self.run_cbq_query()
        if verifydrop:
            check = self._is_index_in_list(bucket, query_definition.index_name)
            self.assertFalse(check, "index {0} failed to be deleted".format(query_definition.index_name))

    def query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket = bucket)
        actual_result = self.run_cbq_query()
        keys = res["results"][0].keys()
        res = res["results"][0]
        while "index" not in keys:
            keys = res["input"].keys()
            res = res["input"]
        actual_index_name = res["index"]
        self.assertTrue(actual_index_name == index_name,
                                "Index should be %s, but is: %s" % (index_name,res["results"]))

    def multi_query_using_index_with_explain(self, buckets =[], query_definitions = []):
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_explain(bucket.name,
                    query_definition)

    def query_using_index(self, bucket, query_definition, expected_result):
        self.query = query_definition.generate_query(bucket = bucket)
        if expected_result != None:
            actual_result = self.run_cbq_query()
        else:
            expected_result = self.gen_results.generate_expected_result()
            actual_result = self.run_cbq_query()
        self._verify_results(actual_result['results'], expected_result)

    def multi_query_using_index(self, buckets =[], query_definitions = [], expected_results = {}):
        for bucket in buckets:
            for query_definition in query_definitions:
                if expected_results:
                    self.query_using_index(bucket.name,
                        query_definition, expected_results[query_definition.index_name])
                else:
                     self.query_using_index(bucket.name,query_definition, None)

    def check_and_run_operations(self, before = False, after = False, in_between = False):
        if before:
            self._run_operations(create_index = self.ops_map["before"]["create_index"],
                drop_index = self.ops_map["before"]["drop_index"],
                query_ops = self.ops_map["before"]["query_ops"])
        if after:
            self._run_operations(create_index = self.ops_map["after"]["create_index"],
                drop_index = self.ops_map["after"]["drop_index"],
                query_ops = self.ops_map["after"]["query_ops"])
        if in_between:
            self._run_operations(create_index = self.ops_map["in_between"]["create_index"],
                drop_index = self.ops_map["in_between"]["drop_index"],
                query_ops = self.ops_map["in_between"]["query_ops"])

    def run_multi_operations(self, buckets = [], query_definitions = [], expected_results = {},
        create_index = False, drop_index = False, query_with_explain = False, query = False):
        if create_index:
            self.multi_create_index(buckets,query_definitions)
        if query_with_explain:
            self.multi_query_using_index_with_explain(bucket, query_definition)
        if query:
            self.multi_query_using_index(bucket, query_definition, expected_result)
        if drop_index:
            self.multi_drop_index(buckets,query_definitions)

    def _run_operations(self, create_index = False, run_queries = False, drop_index = False):
        self.run_multi_operations(self, buckets = self.buckets, query_definitions = self.query_definitions,
            create_index = create_index, drop_index = drop_index,
            query_with_explain = run_queries, query = run_queries)

    def run_operations(self, bucket, query_definition, expected_results,
        create_index = False, drop_index = False, query_with_explain = False, query = False):
        self.run_multi_operations(self, buckets = [bucket], query_definitions = [query_definition],
            expected_results = {"0": expected_results},
            create_index = create_index, drop_index = drop_index,
            query_with_explain = query_with_explain, query = query)

    def _create_operation_map(self):
        map_before = {"create_index":False, "query_ops": False, "drop_index": False}
        map_in_between = {"create_index":False, "query_ops": False, "drop_index": False}
        map_after = {"create_index":False, "query_ops": False, "drop_index": False}
        before = self.input.param("before", "create_index")
        for op_type in before.split(":"):
            map_before[op_type] = True
        in_between = self.input.param("in_between", "query_ops")
        for op_type in in_between.split(":"):
            map_in_between[op_type] = True
        after = self.input.param("after", "drop_index")
        for op_type in in_between.split(":"):
            map_after[op_type] = True
        return {"before":map_before, "in_between": map_in_between, "after": map_after}