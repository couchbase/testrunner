from .fts_base import FTSBaseTest, INDEX_DEFAULTS, QUERY, download_from_s3

class FlexFeaturesFTS(FTSBaseTest):

    def setUp(self):
        super(FlexFeaturesFTS, self).setUp()

    def tearDown(self):
        super(FlexFeaturesFTS, self).tearDown()

    def test_pushdown_in_expression(self):
        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items)
        check_pushdown = False

        _data_types = {
            "text":  {"field": "type", "vals": ["emp", "emp1"]},
            "number": {"field": "mutated", "vals": [0, 1]},
            "boolean": {"field": "is_manager", "vals": [True, False]},
            "datetime":    {"field": "join_date", "vals": ["1970-07-02T11:50:10", "1951-11-16T13:37:10"]}
        }
        index_configuration = self._input.param("index_config", "FTS_GSI")
        custom_mapping = self._input.param("custom_mapping", False)
        if index_configuration == "FTS":
            index_hint = "USING FTS"
        elif index_configuration == "GSI":
            index_hint = "USING GSI"
            check_pushdown = True
        else:
            index_hint = "USING FTS, USING GSI"

        if "FTS" in index_configuration:
            self._create_fts_indexes(custom_mapping=custom_mapping, _data_types=_data_types)

        if "GSI" in index_configuration:
            self.create_gsi_indexes(_data_types)

        tests = []
        for _key in _data_types.keys():
            flex_query = "select count(*) from `default`.scope1.collection1 USE INDEX({0}) where {1} in {2}".\
                format(index_hint, _data_types[_key]['field'], _data_types[_key]['vals'])
            gsi_query = "select count(*) from `default`.scope1.collection1 where {1} in {2}".\
                format(index_hint, _data_types[_key]['field'], _data_types[_key]['vals'])
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        self._cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection1")
        self.sleep(10)
        for test in tests:
            result = self._cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self._cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection1")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            self.fail("Errors are detected for IN/NOT Flex queries. Check logs for details.")

    def test_pushdown_like_expression(self):
        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items)
        check_pushdown = False

        like_types = ["left", "right", "left_right"]
        like_conditions = ["LIKE"]
        _data_types = {
            "text":  {"field": "type", "vals": "emp"},
        }
        index_configuration = self._input.param("index_config", "FTS_GSI")
        custom_mapping = self._input.param("custom_mapping", False)
        if index_configuration == "FTS":
            index_hint = "USING FTS"
        elif index_configuration == "GSI":
            index_hint = "USING GSI"
        else:
            index_hint = "USING FTS, USING GSI"

        if "FTS" in index_configuration:
            self._create_fts_indexes(custom_mapping=custom_mapping, _data_types=_data_types)
        if "GSI" in index_configuration:
            self.create_gsi_indexes(_data_types)

        tests = []
        for _key in _data_types.keys():
            for like_type in like_types:
                for like_condition in like_conditions:
                    if like_type == "left":
                        like_expression = "'%"+_data_types[_key]['vals']+"'"
                    elif like_type == "right":
                        like_expression = "'" + _data_types[_key]['vals'] + "%'"
                    else:
                        like_expression = "'%" + _data_types[_key]['vals'] + "%'"
                    flex_query = "select count(*) from `default`.scope1.collection1 USE INDEX({0}) where {1} {2} {3}".\
                        format(index_hint, _data_types[_key]['field'], like_condition, like_expression)
                    gsi_query = "select count(*) from `default`.scope1.collection1 where {1} {2} {3}".\
                        format(index_hint, _data_types[_key]['field'], like_condition, like_expression)
                    test = {}
                    test['flex_query'] = flex_query
                    test['gsi_query'] = gsi_query
                    test['flex_result'] = {}
                    test['flex_explain'] = {}
                    test['gsi_result'] = {}
                    test['errors'] = []
                    tests.append(test)

        self._cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection1")
        self.sleep(10)
        for test in tests:
            result = self._cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self._cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection1")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)

        if errors_found:
            self.fail("Errors are detected for LIKE Flex queries. Check logs for details.")

    def test_pushdown_sort_expression(self):
        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items)

        sort_directions = ["ASC", "DESC", ""]
        limits = ["LIMIT 10", ""]
        offsets = ["OFFSET 5", ""]
        custom_mapping = self._input.param("custom_mapping", False)
        _data_types = {
            "text": {"field": "type", "flex_condition": "type='emp'"},
            "number": {"field": "mutated", "flex_condition": "mutated=0"},
            "boolean": {"field": "is_manager", "flex_condition": "is_manager=true"},
            "datetime": {"field": "join_date", "flex_condition": "join_date > '2001-10-09' AND join_date < '2020-10-09'"}
        }
        index_configuration = self._input.param("index_config", "FTS_GSI")
        if index_configuration == "FTS":
            index_hint = "USING FTS"
        elif index_configuration == "GSI":
            index_hint = "USING GSI"
        else:
            index_hint = "USING FTS, USING GSI"

        if "FTS" in index_configuration:
            self._create_fts_indexes(custom_mapping=custom_mapping, _data_types=_data_types)
        if "GSI" in index_configuration:
            self.create_gsi_indexes(_data_types)

        tests = []
        for _key in _data_types.keys():
            for sort_direction in sort_directions:
                for limit in limits:
                    for offset in offsets:
                        flex_query = "select meta().id from `default`.scope1.collection1 USE INDEX({0}) where {1} order by {2} {3} {4} {5}".\
                            format(index_hint, _data_types[_key]['flex_condition'], "meta().id", sort_direction, limit, offset)
                        gsi_query = "select meta().id from `default`.scope1.collection1 USE INDEX({0}) where {1} order by {2} {3} {4} {5}".\
                            format(index_hint, _data_types[_key]['flex_condition'], "meta().id", sort_direction, limit, offset)
                        test = {}
                        test['flex_query'] = flex_query
                        test['gsi_query'] = gsi_query
                        test['flex_result'] = {}
                        test['flex_explain'] = {}
                        test['gsi_result'] = {}
                        test['errors'] = []
                        tests.append(test)

        self._cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection1")
        self.sleep(10)
        for test in tests:
            result = self._cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self._cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection1")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=False)
        if errors_found:
            self.fail("Errors are detected for ORDER BY Flex queries. Check logs for details.")

    def test_doc_id_prefix(self):
        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items)

        check_pushdown = False
        like_expressions = ["LIKE"]
        index_configuration = self._input.param("index_config", "FTS_GSI")
        custom_mapping = False
        if index_configuration == "FTS":
            index_hint = "USING FTS"
        else:
            index_hint = "USING FTS, USING GSI"

        if "FTS" in index_configuration:
            plan_params = self.construct_plan_params()
            self.create_fts_indexes_all_buckets(plan_params=plan_params, analyzer="keyword")
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True)
            indexes = self._cb_cluster.get_indexes()
            for idx in indexes:
                idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
                idx.index_definition['params']['doc_config']['docid_prefix_delim'] = "_"
                idx.index_definition['params']['doc_config']['docid_regexp'] = ""
                idx.index_definition['params']['doc_config']['mode'] = "scope.collection.docid_prefix"
                idx.index_definition['params']['doc_config']['type_field'] = "type"
                idx.index_definition['uuid'] = idx.get_uuid()


                idx.update()

        tests = []
        for like_expression in like_expressions:
            flex_query = "select count(*) from `default`.scope1.collection1 USE INDEX({0}) where meta().id {1} 'emp_%' and type='emp'".\
                        format(index_hint, like_expression)
            gsi_query = "select count(*) from `default`.scope1.collection1 where meta().id {1} 'emp_%' and type='emp'".\
                        format(index_hint, like_expression)
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        self._cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection1")
        self.sleep(10)
        for test in tests:
            result = self._cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self._cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection1")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            self.fail("Errors are detected for DOC_ID prefix Flex queries. Check logs for details.")

    def test_pushdown_negative_numeric_ranges(self):
        errors = []
        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items)

        check_pushdown = False
        relations = ['<', '<=', '=', '>', '>=']
        _data_types = {
            "number": {"field": "salary"}
        }
        index_configuration = self._input.param("index_config", "FTS_GSI")
        custom_mapping = self._input.param("custom_mapping", False)
        if index_configuration == "FTS":
            index_hint = "USING FTS"
        elif index_configuration == "GSI":
            index_hint = "USING GSI"
            check_pushdown = True
        else:
            index_hint = "USING FTS, USING GSI"

        if "FTS" in index_configuration:
            self._create_fts_indexes(custom_mapping=custom_mapping, _data_types=_data_types)
        if "GSI" in index_configuration:
            self.create_gsi_indexes(_data_types)

        tests = []
        for relation in relations:
            condition = ""
            if relation == '<':
                condition = ' salary > -100 and salary < -10'
            elif relation == '<=':
                condition = ' salary >= -100 and salary <= -10'
            elif relation == '>':
                condition = ' salary > -10 and salary < -1'
            elif relation == '>=':
                condition = ' salary >= -10 and salary <= -1'
            elif relation == "=":
                condition = ' salary = -10 '

            flex_query = "select count(*) from `default`.scope1.collection1 USE INDEX({0}) where {1}" .\
                    format(index_hint, condition)
            gsi_query = "select count(*) from `default`.scope1.collection1 USE INDEX({0}) where {1}" .\
                    format(index_hint, condition)
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        self._cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection1")
        self.sleep(10)
        for test in tests:
            result = self._cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self._cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection1")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            self.fail("Errors are detected for negative numeric ranges Flex queries. Check logs for details.")

    def test_pushdown_flex_and_search_together(self):
        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items)

        check_pushdown = False
        _data_types = {
            "text":  {"field": "type",
                        "search_condition": "{'query':{'field': 'type', 'match':'emp'}}",
                        "flex_condition": "a.`type`='emp'"},
            "number": {"field": "salary",
                        "search_condition": "{'query':{'min': 1000, 'max': 100000, 'field': 'salary'}}",
                        "flex_condition": "a.salary>1000 and a.salary<100000"},
            "boolean": {"field": "is_manager",
                        "search_condition": "{'query':{'bool': true, 'field': 'is_manager'}}",
                        "flex_condition": "a.is_manager=true"},
            "datetime":    {"field": "join_date",
                        "search_condition": "{'start': '2001-10-09', 'end': '2016-10-31', 'field': 'join_date'}",
                        "flex_condition": "a.join_date > '2001-10-09' and a.join_date < '2016-10-31'"}
        }
        index_configuration = self._input.param("index_config", "FTS_GSI")
        custom_mapping = self._input.param("custom_mapping", False)
        if index_configuration == "FTS":
            index_hint = "USING FTS"
        elif index_configuration == "GSI":
            index_hint = "USING GSI"
        else:
            index_hint = "USING FTS, USING GSI"

        if "FTS" in index_configuration:
            self._create_fts_indexes(custom_mapping=custom_mapping, _data_types=_data_types)
        if "GSI" in index_configuration:
            self.create_gsi_indexes(_data_types)

        tests = []
        for _key1 in _data_types.keys():
            for _key2 in _data_types.keys():
                flex_query = "select count(*) from `default`.scope1.collection1 a USE INDEX({0}) where {1} and search(a, {2})".\
                        format(index_hint, _data_types[_key1]['flex_condition'], _data_types[_key2]['search_condition'])
                gsi_query = "select count(*) from `default`.scope1.collection1 a where {0} and search(a, {1})".\
                        format(_data_types[_key1]['flex_condition'], _data_types[_key2]['search_condition'])
                test = {}
                test['flex_query'] = flex_query
                test['gsi_query'] = gsi_query
                test['flex_result'] = {}
                test['flex_explain'] = {}
                test['gsi_result'] = {}
                test['errors'] = []
                tests.append(test)

        self._cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection1")
        self.sleep(10)
        for test in tests:
            result = self._cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self._cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection1")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            self.fail("Errors are detected for Flex + Search queries. Check logs for details.")

    def create_gsi_indexes(self, indexing_fields):
        _data_types = {
            "text":  {"field": "type", "vals": ["emp", "emp1"]},
            "number": {"field": "mutated", "vals": [0, 1]},
            "boolean": {"field": "is_manager", "vals": [True, False]},
            "datetime":    {"field": "join_date", "vals": ["1970-07-02T11:50:10", "1951-11-16T13:37:10"]}
        }


        if type(self.collection) is list:
            for c in self.collection:
                for _tp in indexing_fields.keys():
                    data_type = indexing_fields[_tp]
                    query = "CREATE INDEX `idx_gsi_"+c+"_"+data_type['field']+"` ON `default`.`scope1`.`"+c+"`(`"+data_type['field']+"`)"
                    self._cb_cluster.run_n1ql_query(query)
        else:
            for _tp in indexing_fields.keys():
                data_type = indexing_fields[_tp]
                query = "CREATE INDEX `idx_gsi_" + self.collection + "_" + data_type[
                    'field'] + "` ON `default`.`scope1`.`" + self.collection + "`(`" + data_type['field'] + "`)"
                self._cb_cluster.run_n1ql_query(query)

    def _create_fts_indexes(self, custom_mapping=False, _data_types=None):
        plan_params = self.construct_plan_params()
        self.create_fts_indexes_all_buckets(plan_params=plan_params, analyzer='keyword')
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        indexes = self._cb_cluster.get_indexes()
        for idx in indexes:
            idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
            idx.index_definition['uuid'] = idx.get_uuid()
            idx.update()
        if custom_mapping:
            new_types = {}
            new_types['properties'] = {}
            for idx in indexes:
                cur_types = idx.index_definition['params']['mapping']['types']
                for tp in cur_types.keys():
                    cur_types[tp]['properties'] = {}
                    for _key in _data_types.keys():
                        fld = _data_types[_key]['field']
                        #new_types[tp + "." + fld] = val
                        cur_types[tp]['properties'][fld] = {"enabled": True, "dynamic": False, "fields": [{"index": True, "name": fld, "type":_key}]}
                idx.index_definition['params']['mapping']['types'] = cur_types
                idx.index_definition['uuid'] = idx.get_uuid()
                idx.update()

    def _perform_results_checks(self, tests=None, index_configuration="", custom_mapping=False, check_pushdown=True):
        for test in tests:
            result = self._cb_cluster.run_n1ql_query("explain " + test['flex_query'])
            if check_pushdown:
                if "index_group_aggs" not in str(result):
                    error = {}
                    error['error_message'] = "Index aggregate pushdown is not detected."
                    error['query'] = test['flex_query']
                    error['indexing_config'] = index_configuration
                    error['custom_mapping'] = str(custom_mapping)
                    error['collections'] = str(self.collection)
                    test['errors'].append(error)
            result = self._cb_cluster.run_n1ql_query(test['flex_query'])
            test['flex_result'] = result['results']
            if result['status'] != 'success':
                error = {}
                error['error_message'] = "Flex query was not executed successfully."
                error['query'] = test['flex_query']
                error['indexing_config'] = index_configuration
                error['custom_mapping'] = str(custom_mapping)
                error['collections'] = str(self.collection)
                test['errors'].append(error)
            if test['flex_result'] != test['gsi_result']:
                error = {}
                error['error_message'] = "Flex query results and GSI query results are different."
                error['query'] = test['flex_query']
                error['indexing_config'] = index_configuration
                error['custom_mapping'] = str(custom_mapping)
                error['collections'] = str(self.collection)
                test['errors'].append(error)

        errors_found = False
        for test in tests:
            if len(test['errors']) > 0:
                errors_found = True
                self.log.error("The following errors are detected:\n")
                for error in test['errors']:
                    self.log.error("="*10)
                    self.log.error(error['error_message'])
                    self.log.error("query: " + error['query'])
                    self.log.error("indexing config: " + error['indexing_config'])
                    self.log.error("custom mapping: " + str(error['custom_mapping']))
                    self.log.error("collections set: " + str(error['collections']))
        return errors_found
