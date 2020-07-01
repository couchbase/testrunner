import json
import threading

from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import CBQError
from pytests.fts.fts_base import CouchbaseCluster
from pytests.security.rbac_base import RbacBase
from .tuq import QueryTests
import time


class N1qlFTSIntegrationPhase2Test(QueryTests):
    users = {}

    def suite_setUp(self):
        super(N1qlFTSIntegrationPhase2Test, self).suite_setUp()

    def setUp(self):
        super(N1qlFTSIntegrationPhase2Test, self).setUp()
        self.sample_bucket = 'beer-sample'
        self.query_buckets = self.get_query_buckets(sample_buckets=[self.sample_bucket])
        self.query_bucket = self.query_buckets[1]
        self._load_test_buckets()
        self.users = {}

        self.log.info("==============  N1qlFTSIntegrationPhase2Test setup has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2Test setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  N1qlFTSIntegrationPhase2Test tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2Test tearDown has completed ==============")
        super(N1qlFTSIntegrationPhase2Test, self).tearDown()
        self.sample_bucket = 'beer-sample'
        if self.get_bucket_from_name(self.sample_bucket):
            self.delete_bucket(self.sample_bucket)

    def suite_tearDown(self):
        self.log.info("==============  N1qlFTSIntegrationPhase2Test suite_tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2Test suite_tearDown has completed ==============")
        super(N1qlFTSIntegrationPhase2Test, self).suite_tearDown()

    # ======================== tests =====================================================

    def test_keyspace_alias_single_bucket(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name=self.sample_bucket)

        bucket_names = {
            "no_alias": self.query_bucket,
            "alias": "{0} t".format(self.query_bucket),
            "as_alias": "{0} as t".format(self.query_bucket)
        }
        test_name = self.input.param("test_name", '')
        bucket_name = bucket_names[test_name]
        if bucket_name == self.query_bucket:
            aliases = ["", self.query_bucket]
        else:
            aliases = ["t"]

        try:
            for alias in aliases:
                dot = ""
                if alias != "":
                    dot = "."
                fts_query = "select " + str(alias) + str(dot) + "code, " + str(alias) + str(dot) + "state from " + str(
                    bucket_name) + " " \
                                   "where " + str(alias) + str(
                    dot) + "type='brewery' and SEARCH(" + alias + dot + "state, {'query':{'field': 'state', 'match':" \
                                                                        " 'California'}, 'size': 10000}) " \
                                                                        "order by " + str(alias) + str(dot) + "code"
                n1ql_query = "select code, state from {0} where type='brewery' and state like '%California%' " \
                             "order by code".format(self.query_bucket)

                fts_results = self.run_cbq_query(fts_query)['results']
                n1ql_results = self.run_cbq_query(n1ql_query)['results']

                self.assertEqual(fts_results, n1ql_results, "Incorrect query : " + str(fts_query))

        finally:
            self._remove_all_fts_indexes()

    def test_keyspace_alias_two_buckets(self):
        # TODO:
        # Add tests for 2 SEARCH() calls
        # Size 20 limit 10
        # Index UUID
        # Covering-non covering gsi, at least 2 fields
        # option - index can be specified in 2 wais: string, object.
        # 2 key spaces in select, no specs in search() - shopuld fail with appropriate error message.

        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        test_cases = {
            'test_t1_t2_1': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "t1"},
            'test_t1_t2_2': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "`t1`"},
            'test_t1_t2_3': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.state"},
            'test_t1_t2_4': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "t1.`state`"},
            'test_t1_t2_5': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.`state`"},
            'test_ast1_t2_1': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "t1"},
            'test_ast1_t2_2': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "`t1`"},
            'test_ast1_t2_3': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.state"},
            'test_ast1_t2_4': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "t1.`state`"},
            'test_ast1_t2_5': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.`state`"},
            'test_t1_ast2_1': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "t1"},
            'test_t1_ast2_2': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`"},
            'test_t1_ast2_3': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.state"},
            'test_t1_ast2_4': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "t1.`state`"},
            'test_t1_ast2_5': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.`state`"},
            'test_ast1_ast2_1': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "t1"},
            'test_ast1_ast2_2': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`"},
            'test_ast1_ast2_3': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.state"},
            'test_ast1_ast2_4': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "t1.`state`"},
            'test_ast1_ast2_5': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.`state`"},
        }

        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name=self.sample_bucket)

        bucket1_alias = test_cases[test_name]["bucket1_alias"]
        bucket2_alias = test_cases[test_name]["bucket2_alias"]
        keyspace_alias = test_cases[test_name]['keyspace_param']

        if not self.is_index_present(self.sample_bucket, "idx_brewery_id"):
            self.run_cbq_query("create index idx_brewery_id on {0}(brewery_id)".format(self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "idx_type"):
            self.run_cbq_query("create index idx_type on {0}(type)".format(self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "idx_code"):
            self.run_cbq_query("create index idx_code on {0}(code)".format(self.query_bucket))
        self.wait_for_all_indexes_online()

        fts_query = "select t1.code, t1.state, t1.city, t2.name from {0} ".format(self.query_bucket) + bucket1_alias + \
                    " inner join {0} ".format(self.query_bucket) + bucket2_alias + \
                    " on t1.code=t2.brewery_id where t1.type='brewery' and t2.type='beer' and SEARCH(" + \
                    keyspace_alias + ", 'state:California') order by t1.code, t2.name"
        n1ql_query = "select t1.code, t1.state, t1.city, t2.name from {0} t1 inner join " \
                     "{0} t2 on t1.code=t2.brewery_id where t1.type='brewery' " \
                     " and t2.type='beer' and t1.state like '%California%' order by t1.code," \
                     " t2.name".format(self.query_bucket)
        try:
            fts_results = self.run_cbq_query(fts_query)['results']
            n1ql_results = self.run_cbq_query(n1ql_query)['results']
        except CBQError as err:
            self._remove_all_fts_indexes()
            raise Exception("Query: " + fts_query + " is failed." + str(err))

        self._remove_all_fts_indexes()
        self.drop_index_safe(self.sample_bucket, 'idx_brewery_id')
        self.drop_index_safe(self.sample_bucket, 'idx_type')
        self.drop_index_safe(self.sample_bucket, 'idx_code')

        self.assertEqual(fts_results, n1ql_results, "Incorrect query : " + str(fts_query))

    def test_keyspace_alias_two_buckets_negative(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        test_cases = {
            "test1": {"bucket_name": "{0} t1".format(self.query_bucket), "search_alias": "`state`"},
            "test3": {"bucket_name": "{0} as t1".format(self.query_bucket), "search_alias": "state"},
            "test4": {"bucket_name": "{0} as t1".format(self.query_bucket), "search_alias": "`state`"},
        }
        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name=self.sample_bucket)
        bucket_name = test_cases[test_name]["bucket_name"]
        search_alias = test_cases[test_name]["search_alias"]

        fts_query = "select t1.code, t1.state, t1.city, t2.name from " + bucket_name + \
                    " inner join {0} t2 on t1.code=t2.brewery_id " \
                    "where t1.type='brewery' and t2.type='beer' and SEARCH(".format(self.query_bucket) + \
                    search_alias + ", 'state:California') order by t2.name"
        try:
            self.run_cbq_query(fts_query)
        except CBQError as err:
            self._remove_all_fts_indexes()
            self.assertTrue("Ambiguous reference to field" in str(err),
                            "Unexpected error message is found - " + str(err))

        self._remove_all_fts_indexes()

    def test_keyspace_alias_1_bucket_negative(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        test_cases = {
            "star": "t.state[*]",
            "object_values": "OBJECT_VALUES(t.state)",
            "array": "t.[state]"
        }
        search_alias = test_cases[test_name]

        fts_query = "select t.code, t.state from {0} t where t.type='brewery' and SEARCH(".format(self.query_bucket) + \
                    search_alias + ", 'France') order by t.code"
        try:
            self.run_cbq_query(fts_query)
        except CBQError as ex:
            self._remove_all_fts_indexes()
            if test_name in ['star', 'object_values', 'array']:
                self.assertTrue("SEARCH() function operands are invalid." in str(ex),
                                "Unexpected error message is found - " + str(ex))
            else:
                self.assertTrue("Ambiguous reference to field" in str(ex),
                                "Unexpected error message is found - " + str(ex))

        self._remove_all_fts_indexes()

    def test_search_options_index_name(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        test_cases = {
            "index_not_exists": {
                "expected_result": "success",
                "index_in_explain": "beer_primary"
            },
            "single_fts_index": {
                "expected_result": "success",
                "index_in_explain": "idx_beer_sample_fts"
            },
            "two_fts_indexes": {
                "expected_result": "success",
                "index_in_explain": "idx_beer_sample_fts"
            },
            "fts_index_is_not_optimal": {
                "expected_result": "success",
                "index_in_explain": "idx_beer_sample_fts"
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_query = "select meta().id from {0} where search({0}, {{\"field\": \"state\", \"match\":\"California\"}}," \
                    " {{\"index\":\"idx_beer_sample_fts\"}})".format(self.query_bucket)
        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name=self.sample_bucket)

        if test_name == "index_not_exists":
            self._delete_fts_index(index_name="idx_beer_sample_fts")
        else:
            if test_name == "two_fts_indexes":
                self._create_fts_index(index_name='idx_beer_sample_fts_1', doc_count=7303,
                                       source_name=self.sample_bucket)
            elif test_name == "fts_index_is_not_optimal":
                more_suitable_index = self._create_fts_index(index_name='idx_beer_sample_fts_name', doc_count=7303,
                                                             source_name=self.sample_bucket)
                more_suitable_index.add_child_field_to_default_mapping(field_name="name", field_type="text")
                more_suitable_index.index_definition['uuid'] = more_suitable_index.get_uuid()
                more_suitable_index.update()
        if test_cases[test_name]["expected_result"] == "fail":
            result = self.run_cbq_query(fts_query)
            self.assertEqual(result['status'], "errors",
                             "Running SEARCH() query without fts index is successful. Should be failed.")
        elif test_cases[test_name]["expected_result"] == "success":
            result = self.run_cbq_query("explain " + fts_query)
            self._remove_all_fts_indexes()
            self.assertEqual(result['results'][0]['plan']['~children'][0]['index'],
                             test_cases[test_name]["index_in_explain"])

        self._remove_all_fts_indexes()

    # 10 results problem
    def test_search_options(self):
        # todo: have more than one search() call, play with search_meta() and search_score()
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)

        test_cases = {
            # 10 results
            "explain": ["true", "false"],
            # 10 results
            "fields": ["[\"*\"]", "[\"name\"]"],
            # 10 results
            "highlight": ["{\"style\":\"html\", \"fields\":[\"*\"]}", "{\"style\":\"html\", \"fields\":[\"name\"]}",
                          "{\"style\":\"ansi\", \"fields\":[\"name\"]}", "{\"style\":\"ansi\", \"fields\":[\"*\"]}"],
            # 10 results
            "analyzer": ["{\"match\": \"California\", \"field\": \"state\", \"analyzer\": \"standard\"}",
                         "{\"match\": \"California\", \"field\": \"state\", \"analyzer\": \"html\"}"],
            # MB-34005
            "size": [10, 100],
            # 10 results
            "sort": ["[{\"by\": \"field\", \"field\": \"name\", \"mode\":\"max\", \"missing\": \"last\"}]"],
        }

        for option_val in test_cases[test_name]:
            self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name=self.sample_bucket)
            n1ql_query = "select meta().id from {0} where search({0}, {{\"query\":{{\"field\": \"state\", \"match\":" \
                         "\"California\"}}, \"size\": 10000, \"sort\": [\"_id\"]}}, {{\"".format(self.query_bucket) + \
                         test_name + "\": " + str(option_val) + "})"
            if test_name == "size":
                n1ql_query = "select meta().id from {0} where search({0}, {{\"query\":{{\"field\": \"state\"," \
                             " \"match\":\"California\"}}, \"sort\": [\"_id\"], \"".format(self.query_bucket) + \
                             test_name + "\":" + str(option_val) + "})"
            if test_name == 'size':
                fts_request_str = "{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":" + \
                                  str(option_val) + ", \"sort\": [\"_id\"]}"
            else:
                fts_request_str = "{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"sort\": [\"_id\"]," \
                                  " \"size\":10000, \"" + test_name + "\":" + str(option_val) + "}"
            fts_request = json.loads(fts_request_str)
            n1ql_results = self.run_cbq_query(n1ql_query)['results']
            total_hits, hits, took, status = \
                rest.run_fts_query(index_name="idx_beer_sample_fts",
                                   query_json=fts_request)
            comparison_result = self._compare_n1ql_results_against_fts(n1ql_results, hits)
            self.assertEqual(comparison_result, "OK", comparison_result)
            self._remove_all_fts_indexes()
            comparison_result = self._compare_n1ql_results_against_fts(n1ql_results, hits)
            self.assertEqual(comparison_result, "OK", comparison_result)

    def test_use_index_hint(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)

        test_cases = {
            "fts_index_exists": {
                "hint_content": "idx_beer_sample_fts USING FTS",
                "expected_result": "positive",
                "options_content": ""
            },
            "fts_index_does_not_exist": {
                "hint_content": "idx_beer_sample_fts_fake USING FTS",
                "expected_result": "negative",
                "options_content": ""
            },
            "fts_index_busy": {
                "hint_content": "idx_beer_sample_fts USING FTS",
                "expected_result": "positive",
                "options_content": ""
            },
            "fts_gsi_indexes_use": {
                "hint_content": "idx_beer_sample_fts USING FTS, beer_primary using GSI",
                "expected_result": "positive",
                "options_content": ""
            },
            "same_hint_options": {
                "hint_content": "idx_beer_sample_fts USING FTS",
                "expected_result": "positive",
                "options_content": ", {\"index\":\"idx_beer_sample_fts\"}"
            },
            "not_same_hint_options": {
                "hint_content": "idx_beer_sample_fts USING FTS",
                "expected_result": "positive",
                "options_content": ", {\"index\":\"idx_beer_sample_fts_1\"}"
            },
            "hint_good_options_bad": {
                "hint_content": "idx_beer_sample_fts USING FTS",
                "expected_result": "negative",
                "options_content": ", {\"index\":\"idx_beer_sample_fts_fake\"}"
            },
            "hint_bad_options_good": {
                "hint_content": "idx_beer_sample_fts_fake USING FTS",
                "expected_result": "negative",
                "options_content": ", {\"index\":\"idx_beer_sample_fts\"}"
            },
            "hint_bad_options_bad": {
                "hint_content": "idx_beer_sample_fts_fake USING FTS",
                "expected_result": "negative",
                "options_content": ", {\"index\":\"idx_beer_sample_fts_fake\"}"
            },
        }

        try:
            test_results = {}
            test_passed = True
            negatives_expected = 0
            negatives_found = 0

            self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

            if test_name == "not_same_hint_options":
                self._create_fts_index(index_name='idx_beer_sample_fts_1', doc_count=7303,
                                       source_name=self.sample_bucket)

            test_case_dict = test_cases[test_name]
            options_content = test_case_dict['options_content']
            if test_case_dict['expected_result'] == "negative":
                negatives_expected = 1

            n1ql_query = "select meta().id from {0} USE INDEX (".format(self.query_bucket) + \
                         test_case_dict['hint_content'] + \
                         ") where search({0}, {{\"field\": \"state\", \"match\":\"California\"}}".format(
                             self.query_bucket) + \
                         options_content + ")"
            try:
                n1ql_explain_query = "explain " + n1ql_query
                self.run_cbq_query(n1ql_query)
                result = self.run_cbq_query(n1ql_explain_query)
                if test_name == "not_same_hint_options":
                    self.assertTrue(result['results'][0]['plan']['~children'][0]['index'] in ["idx_beer_sample_fts",
                                                                                              "idx_beer_sample_fts_1"])
                else:
                    self.assertEquals(result['results'][0]['plan']['~children'][0]['index'], "idx_beer_sample_fts")
            except CBQError:
                negatives_found = 1
                test_passed = False

            test_results[test_name] = test_passed

            if test_name == "not_same_hint_options":
                self._delete_fts_index(index_name='idx_beer_sample_fts_1')

        finally:
            self._remove_all_fts_indexes()

        self.assertEqual(negatives_found, negatives_expected, "Some test case results differ from expected.")

    def test_index_selection(self):
        # gsi indexes - primary, secondary - field, seconadary - field,field
        # fts indexes - default, field, type->field

        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        if not self.is_index_present(self.sample_bucket, "idx_state"):
            self.run_cbq_query("create index idx_state on {0}(state)".format(self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "idx_state_city"):
            self.run_cbq_query("create index idx_state_city on {0}(state, city)".format(self.query_bucket))
        self.wait_for_all_indexes_online()

        more_suitable_fts_index = self._create_fts_index(index_name='idx_beer_sample_fts_name', doc_count=7303,
                                                         source_name=self.sample_bucket)
        more_suitable_fts_index.add_child_field_to_default_mapping(field_name="state", field_type="text")
        more_suitable_fts_index.index_definition['uuid'] = more_suitable_fts_index.get_uuid()
        more_suitable_fts_index.update()

        test_cases = {
            # index specified in SEARCH() or in USE INDEX hint must be used
            "use_index_fts": {
                "query": "explain select meta().id from {0} USE INDEX (idx_beer_sample_fts using fts) where search({0},"
                         " {{\"field\": \"state\", \"match\":\"California\"}})".format(self.query_bucket),
                "index": "idx_beer_sample_fts"
            },
            # MB-33677
            "use_index_gsi": {
                "query": "explain select meta().id from {0} USE INDEX (idx_state_city using gsi) where search({0},"
                         " {{\"field\": \"state\", \"match\":\"California\"}}) and "
                         "state='California'".format(self.query_bucket),
                "index": "idx_state_city"
            },
            "search_hint": {
                "query": "explain select meta().id from {0} where search({0}, {{\"field\": \"state\", \"match\":"
                         "\"California\"}}, {{\"index\":\"idx_beer_sample_fts\"}})".format(self.query_bucket),
                "index": "idx_beer_sample_fts"
            },
            "shortest_fts": {
                "query": "explain select meta().id from {0} where search({0}, {{\"field\": \"state\", \"match\":"
                         "\"California\"}})".format(self.query_bucket),
                "index": "idx_beer_sample_fts_name"
            },
            "shortest_gsi": {
                "query": "explain select meta().id from {0} where search({0}, {{\"field\": \"name\", \"match\":"
                         "\"California\"}})".format(self.query_bucket),
                "index": "idx_name"
            },
            "primary_gsi": {
                "query": "explain select meta().id from {0} where search({0}, {{\"field\": \"category\", \"match\":"
                         "\"British\"}})".format(self.query_bucket),
                "index": "PrimaryScan3"
            }
        }

        if test_name == "shortest_gsi":
            self._delete_fts_index("idx_beer_sample_fts")
            if not self.is_index_present(self.sample_bucket, "idx_name"):
                self.run_cbq_query("create index idx_name on {0}(name)".format(self.query_bucket))
            if not self.is_index_present(self.sample_bucket, "idx_state_name"):
                self.run_cbq_query("create index idx_state_name on {0}(state, name)".format(self.query_bucket))
            self.wait_for_all_indexes_online()
        if test_name == "primary_gsi":
            self._delete_fts_index("idx_beer_sample_fts")

        n1ql_query = test_cases[test_name]["query"]
        result = self.run_cbq_query(n1ql_query)
        if test_name in ["primary_gsi", "shortest_gsi"]:
            self.assertEqual(result['results'][0]['plan']['~children'][0]['#operator'], "PrimaryScan3")
        else:
            self.assertEqual(result['results'][0]['plan']['~children'][0]['index'], test_cases[test_name]["index"])

        self._remove_all_fts_indexes()
        self.drop_index_safe(self.sample_bucket, 'idx_state')
        self.drop_index_safe(self.sample_bucket, 'idx_state_city')
        self.drop_index_safe(self.sample_bucket, 'idx_name')
        self.drop_index_safe(self.sample_bucket, 'idx_state_name')

    # 10 results
    def test_logical_predicates(self):
        test_cases = [" = true ", " in [true] ", " in [true, true, true] "]

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        fts_request = {"query": {"field": "state", "match": "California"}, "size": 10000}

        for test_case in test_cases:
            n1ql_query = "select meta().id from {0} where search({0}, {{\"field\": \"state\", \"match\":" \
                         "\"California\"}}) ".format(self.query_bucket) + test_case

            n1ql_results = self.run_cbq_query(n1ql_query)['results']
            total_hits, hits, took, status = \
                rest.run_fts_query(index_name="idx_beer_sample_fts",
                                   query_json=fts_request)
            comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, hits)
            self.assertEqual(comparison_results, "OK", comparison_results)

        n1ql_query = "select meta().id from {0} where not(not(search({0}, {{\"field\": \"state\", \"match\":" \
                     "\"California\"}}))) ".format(self.query_bucket)
        n1ql_results = self.run_cbq_query(n1ql_query)['results']

        total_hits, hits, took, status = rest.run_fts_query(index_name="idx_beer_sample_fts", query_json=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, hits)
        self.assertEqual(comparison_results, "OK", comparison_results)

        self._remove_all_fts_indexes()

    def test_logical_predicates_negative(self):
        test_cases = {
            "case_1": {
                "predicate": " = false ",
                "verification_query": "select meta().id from {0} where state is missing or "
                                      "state!='California'".format(self.query_bucket)
            },
            "case_2": {
                "predicate": " !=false ",
                "verification_query": "select meta().id from {0} where state = 'California'".format(self.query_bucket)
            },
            "case_3": {
                "predicate": " in [false] ",
                "verification_query": "select meta().id from {0} where state is missing or "
                                      "state != 'California'".format(self.query_bucket)
            },
            "case_4": {
                "predicate": " in [true, 1, 2] ",
                "verification_query": "select meta().id from {0} where state = 'California'".format(self.query_bucket)
            },
            "case_5": {
                "predicate": " not in [false] ",
                "verification_query": "select meta().id from {0} where state = 'California'".format(self.query_bucket)
            },
        }
        test_name = self.input.param("test_name", "")
        if test_name == "":
            raise Exception("Test name cannot be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)
        if test_name != 'special_case':
            predicate = test_cases[test_name]['predicate']
            verification_query = test_cases[test_name]['verification_query']

            search_query = "select meta().id from {0} where search({0}, {{\"field\": \"state\", \"match\":" \
                           "\"California\"}}) ".format(self.query_bucket) + predicate
        else:
            search_query = "select meta().id from {0} where not(search({0}, {{\"field\": \"state\", \"match\":" \
                           "\"California\"}})) ".format(self.query_bucket)
            verification_query = "select meta().id from {0} where state is missing or state != " \
                                 "'California'".format(self.query_bucket)

        search_results = self.run_cbq_query(search_query)['results']
        verification_results = self.run_cbq_query(verification_query)['results']

        search_doc_ids = []
        for result in search_results:
            search_doc_ids.append(result['id'])

        verification_doc_ids = []
        for result in verification_results:
            verification_doc_ids.append(result['id'])

        self.assertEqual(len(search_doc_ids), len(verification_doc_ids),
                         "Results count does not match for test . SEARCH() - " + str(
                             len(search_doc_ids)) + ", Verification - " + str(len(verification_doc_ids)))
        self.assertEqual(sorted(search_doc_ids), sorted(verification_doc_ids),
                         "Found mismatch in results for test .")

        self._remove_all_fts_indexes()

    def test_n1ql_syntax_select_from_let(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)

        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        n1ql_query = "select meta().id from {0} let res=true where search({0}, {{\"query\":{{\"field\": \"state\", " \
                     "\"match\":\"California\"}},\"size\":10000}})=res".format(self.query_bucket)
        fts_request = {"query": {"field": "state", "match": "California"}, "size": 10000}
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        total_hits, hits, took, status = rest.run_fts_query(index_name="idx_beer_sample_fts",
                                                            query_json=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, hits)
        self.assertEqual(comparison_results, "OK", comparison_results)

        self._remove_all_fts_indexes()

    def test_n1ql_syntax_select_from_2_buckets(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        if not self.is_index_present(self.sample_bucket, "idx_state"):
            self.run_cbq_query("create index idx_state on {0}(state)".format(self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "idx_city"):
            self.run_cbq_query("create index idx_city on {0}(city)".format(self.query_bucket))

        self.wait_for_all_indexes_online()

        n1ql_query = "select {0}.id, {0}.country, {0}.city, t2.name from {0} " \
                     "inner join {0} t2 on {0}.state=t2.state and {0}.city=t2.city " \
                     "where SEARCH({0}, 'state:California')".format(self.query_bucket)
        n1ql_results = self.run_cbq_query(n1ql_query)
        self.assertEqual(n1ql_results['status'], 'success')

        self._remove_all_fts_indexes()
        self.drop_index_safe(self.sample_bucket, 'idx_state')
        self.drop_index_safe(self.sample_bucket, 'idx_city')

    def test_n1ql_syntax_select_from_double_search_call(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)
        if not self.is_index_present(self.sample_bucket, "idx_state"):
            self.run_cbq_query("create index idx_state on {0}(state)".format(self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "idx_city"):
            self.run_cbq_query("create index idx_city on {0}(city)".format(self.query_bucket))
        self.wait_for_all_indexes_online()

        n1ql_query = "select {0}.id, {0}.country, {0}.city, t2.name from {0} " \
                     "inner join {0} t2 on {0}.state=t2.state and {0}.city=t2.city " \
                     "where SEARCH(t2, 'state:California') and SEARCH({0}," \
                     " 'state:California')".format(self.query_bucket)
        n1ql_results = self.run_cbq_query(n1ql_query)
        self.assertEqual(n1ql_results['status'], 'success')

        self._remove_all_fts_indexes()
        self.drop_index_safe(self.sample_bucket, 'idx_state')
        self.drop_index_safe(self.sample_bucket, 'idx_city')

    def test_n1ql_syntax_from_select(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)

        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        n1ql_query = "from (select meta().id mt from {0} where search({0}, 'state:California')) as t select" \
                     " t.mt as id".format(self.query_bucket)
        fts_request = {"query": {"field": "state", "match": "California"}, "size": 10000}
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        total_hits, hits, took, status = rest.run_fts_query(index_name="idx_beer_sample_fts",
                                                            query_json=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, hits)
        self.assertEqual(comparison_results, "OK", comparison_results)

        self._remove_all_fts_indexes()

    # MB - 34007
    def test_n1ql_syntax_union_intersect_except(self):
        test_cases = {
            "same_buckets_same_idx": {
                "query_left": "select meta().id from {0} where search({0}, 'state:California') ".format(
                    self.query_bucket),
                "query_right": " select meta().id from {0} where search({0}, 'state:Georgia')".format(self.query_bucket)
            },
            "same_buckets_different_idx": {
                "query_left": "select meta().id from {0} where search({0}, 'state:California') ".format(
                    self.query_bucket),
                "query_right": " select meta().id from {0} where search({0}, 'name:Amendment')".format(
                    self.query_bucket)
            },
            "different_buckets_different_idx": {
                "query_left": "select meta().id from {0} where search({0}, 'state:California') ".format(
                    self.query_bucket),
                "query_right": " select meta().id from {0} where search({0}, 'job_title:Engeneer')".format(
                    self.query_bucket)
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index = self._create_fts_index(index_name="idx_beer_sample_fts_name", doc_count=7303,
                                                source_name=self.sample_bucket)
        fts_name_index.add_child_field_to_default_mapping(field_name="name", field_type="text")
        fts_name_index.index_definition['uuid'] = fts_name_index.get_uuid()
        fts_name_index.update()

        fts_state_index = self._create_fts_index(index_name="idx_beer_sample_fts_state", doc_count=7303,
                                                 source_name=self.sample_bucket)
        fts_state_index.add_child_field_to_default_mapping(field_name="state", field_type="text")
        fts_state_index.index_definition['uuid'] = fts_state_index.get_uuid()
        fts_state_index.update()

        # fts_job_index =  self._create_fts_index(index_name="idx_default_fts_job_title",
        # doc_count=2016, source_name='default')
        union_intersect_except = [" union ", " intersect ", " except "]
        test_name = self.input.param("test_name", '')

        for uie in union_intersect_except:
            full_results = self.run_cbq_query(test_cases[test_name]['query_left'] + uie +
                                              test_cases[test_name]['query_right'])['results']
            left_results = self.run_cbq_query(test_cases[test_name]['query_left'])['results']
            right_results = self.run_cbq_query(test_cases[test_name]['query_right'])['results']
            left_right_results = []
            if uie == ' union ':
                left_right_results = left_results
                for r in right_results:
                    if r not in left_right_results:
                        left_right_results.append(r)
            elif uie == ' intersect ':
                for r in left_results:
                    if r in right_results and r not in left_right_results:
                        left_right_results.append(r)
            elif uie == ' except ':
                for r in left_results:
                    if r not in right_results:
                        left_right_results.append(r)

            self.assertEqual(len(full_results), len(left_right_results),
                             "Results count does not match for test " + test_name + ", operation - " + uie +
                             ". Full query - " + str(len(full_results)) + ", sum of 2 queries - " +
                             str(len(left_right_results)))
            self.assertEqual(sorted(full_results), sorted(left_right_results),
                             "Found mismatch in results for test " + test_name + ", operation - " + uie + ".")

        self._remove_all_fts_indexes()

    def test_prepareds(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        test_cases = {
            "simple_prepared": {
                "prepared": "select meta().id from {0} where search({0}, 'state:California')".format(self.query_bucket),
                "params": "",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California')".format(self.query_bucket),
                "expected_result": "success"
            },
            # MB-33724
            "named_prepared_query_definition": {
                "prepared": "select meta().id from {0} where search({0}, $state_val)".format(self.query_bucket),
                "params": "$state_val=\"state:California\"",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California')".format(self.query_bucket),
                "expected_result": "success"
            },
            # MB-33724
            "named_prepared_option_index_name": {
                "prepared": "select meta().id from {0} where search({0}, 'state:California',"
                            " $idx_name)".format(self.query_bucket),
                "params": "$idx_name={\"index\": \"idx_beer_sample_fts\"}",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California', {{'index':"
                        " 'idx_beer_sample_fts'}})".format(self.query_bucket),
                "expected_result": "success"
            },
            "named_prepared_option_settings": {
                "prepared": "select meta().id from {0} where search({0}, {{\"query\":{{\"field\": \"state\", "
                            "\"match\":\"California\"}},'size': $size, \"sort\":[\"_id\"]}})".format(self.query_bucket),
                "params": "$size=15",
                "n1ql": "select meta().id from {0} where search({0}, {{\"query\":{{\"field\": \"state\", "
                        "\"match\":\"California\"}},'size': 15, 'sort':['_id']}})".format(self.query_bucket),
                "expected_result": "success"
            },
            "named_prepared_option_out": {
                "prepared": "select meta().id from {0} where search({0}, 'state:California',"
                            " {{'out': $out_val}})".format(self.query_bucket),
                "params": "$out=\"out_values\"",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California', {{'out': "
                        "'out_values'}})".format(self.query_bucket),
                "expected_result": "cannot_prepare"
            },
            # MB-33724
            "positional_prepared_query_definition": {
                "prepared": "select meta().id from {0} where search({0}, $1)".format(self.query_bucket),
                "params": "args=[\"state:California\"]",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California')".format(self.query_bucket),
                "expected_result": "success"
            },
            # MB-33724
            "positional_prepared_option_index_name": {
                "prepared": "select meta().id from {0} where search({0}, 'state:California',"
                            " $1)".format(self.query_bucket),
                "params": "args=[\"{'index': 'idx_beer_sample_fts'}\"]",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California', {{'index': "
                        "'idx_beer_sample_fts'}})".format(self.query_bucket),
                "expected_result": "cannot_execute"
            },
            "positional_prepared_option_settings": {
                "prepared": "select meta().id from {0} where search({0}, {{\"query\":{{\"field\": \"state\", "
                            "\"match\":\"California\"}},'size': $1, \"sort\":[\"_id\"]}})".format(self.query_bucket),
                "params": "args=[15]",
                "n1ql": "select meta().id from {0} where search({0}, {{\"query\":{{\"field\": \"state\", "
                        "\"match\":\"California\"}},'size': 15, 'sort':['_id']}})".format(self.query_bucket),
                "expected_result": "success"
            },
            "positional_prepared_option_out": {
                "prepared": "select meta().id from {0} where search({0}, 'state:California', "
                            "{{'out': $1}})".format(self.query_bucket),
                "params": "args=[\"out_values\"]",
                "n1ql": "",
                "expected_result": "cannot_prepare"
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)
        self.run_cbq_query("delete from system:prepareds")

        # 1 create prepared
        create_prepared = "prepare " + test_name + " from " + test_cases[test_name]['prepared']
        if test_cases[test_name]["expected_result"] == "cannot_prepare":
            try:
                self.run_cbq_query(create_prepared)
            except CBQError:
                self.assertEquals(True, True)
                return
        else:
            self.run_cbq_query(create_prepared)

        # 2 call prepared
        call_query = "execute " + test_name
        if test_cases[test_name]["params"] != "":
            call_query = call_query + "&" + test_cases[test_name]["params"]

        prepared_results = self.run_cbq_query_curl(query="'" + call_query + "'")['results']

        # 3 compare to n1ql query
        n1ql_results = self.run_cbq_query(test_cases[test_name]['n1ql'])['results']

        prepared_doc_ids = []
        for result in prepared_results:
            prepared_doc_ids.append(result['id'])

        n1ql_doc_ids = []
        for result in n1ql_results:
            n1ql_doc_ids.append(result['id'])

        self.assertEqual(len(n1ql_doc_ids), len(prepared_doc_ids),
                         "Results count does not match for test . N1QL - " + str(
                             len(n1ql_doc_ids)) + ", Prepareds - " + str(len(prepared_doc_ids)))
        self.assertEqual(sorted(prepared_doc_ids), sorted(n1ql_doc_ids),
                         "Found mismatch in results for test .")

        self._remove_all_fts_indexes()

    def test_parameterized_queries(self):
        # TODO - analyze execution plan for covering indexes.
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        test_cases = {
            # MB-33724
            "named_prepared_query_definition": {
                "prepared": "select meta().id from {0} where search({0}, $state_val)".format(self.query_bucket),
                "params": "$state_val=\"state:California\"",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California')".format(self.query_bucket),
                "expected_result": "success"
            },
            # MB-33724
            "named_prepared_option_settings": {
                "prepared": "select meta().id from {0} where search({0}, \"state:California\", {{\"size\": $size,"
                            " \"sort\":[\"_id\"]}})".format(self.query_bucket),
                "params": "$size=15",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California', {{'size': 15,"
                        " 'sort':['_id']}})".format(self.query_bucket),
                "expected_result": "success"
            },
            "named_prepared_option_out": {
                "prepared": "select meta().id from {0} where search({0}, \"state:California\","
                            " $out_param)".format(self.query_bucket),
                "params": "$out_param={\"out\":\"out_values\"}",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California', {{'out':"
                        " 'out_values'}})".format(self.query_bucket),
                "expected_result": "success"
            },
            "positional_prepared_query_definition": {
                "prepared": "select meta().id from {0} where search({0}, $1)".format(self.query_bucket),
                "params": "args=[\"state:California\"]",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California')".format(self.query_bucket),
                "expected_result": "success"
            },
            "positional_prepared_option_index_name": {
                "prepared": "select meta().id from {0} where search({0}, \"state:California\","
                            " $1)".format(self.query_bucket),
                "params": "args=[{\"index\":\"idx_beer_sample_fts\"}]",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California', {{\"index\":"
                        "\"idx_beer_sample_fts\"}})".format(self.query_bucket),
                "expected_result": "success"
            },
            "positional_prepared_option_settings": {
                "prepared": "select meta().id from {0} where search({0}, \"state:California\", {{\"size\": $1, "
                            "\"sort\":[\"_id\"]}})".format(self.query_bucket),
                "params": "args=[15]",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California', {{'size': 15, "
                        "'sort':['_id']}})".format(self.query_bucket),
                "expected_result": "success"
            },
            "positional_prepared_option_out": {
                "prepared": "select meta().id from {0} where search({0}, \"state:California\","
                            " $1)".format(self.query_bucket),
                "params": "args=[{\"out\":\"out_values\"}]",
                "n1ql": "select meta().id from {0} where search({0}, 'state:California')".format(self.query_bucket),
                "expected_result": "success"
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        if test_cases[test_name]['expected_result'] == "success":
            call_query = test_cases[test_name]['prepared']
            if test_cases[test_name]["params"] != "":
                call_query = call_query + "&" + test_cases[test_name]["params"]
            prepared_results = self.run_cbq_query_curl(query="'" + call_query + "'")['results']
            # 3 compare to n1ql query
            n1ql_results = self.run_cbq_query(test_cases[test_name]['n1ql'])['results']

            prepared_doc_ids = []
            for result in prepared_results:
                prepared_doc_ids.append(result['id'])

            n1ql_doc_ids = []
            for result in n1ql_results:
                n1ql_doc_ids.append(result['id'])

            self.assertEqual(len(n1ql_doc_ids), len(prepared_doc_ids),
                             "Results count does not match for test . N1QL - " + str(
                                 len(n1ql_doc_ids)) + ", Prepareds - " + str(len(prepared_doc_ids)))
            self.assertEqual(sorted(prepared_doc_ids), sorted(n1ql_doc_ids),
                             "Found mismatch in results for test .")

        self._remove_all_fts_indexes()

    def test_rbac(self):
        user = self.input.param("user", '')
        if user == '':
            raise Exception("Invalid test configuration! User name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)
        self._create_all_users()

        username = self.users[user]['username']
        password = self.users[user]['password']
        query = "select meta().id from {0} where search({0}, \"state:California\")".format(self.query_bucket)

        master_result = self.run_cbq_query(query=query, server=self.master, username=username, password=password)
        self.assertEqual(master_result['status'], 'success', username + " query run failed on non-fts node")

        self._remove_all_fts_indexes()

    # 10 results in fts
    def test_sorting_pagination(self):
        # inner sort modes: asc, desc
        # inner sort fields: single field, multiple fields, score, id
        # missing values: first, last
        # mode: min, max, offset
        inner_sorting_field_values = ["_id"]
        inner_sorting_order_values = ["", "min", "max"]
        inner_offset_values = ["", "10"]
        outer_sorting_field_values = ["meta().id"]
        outer_sorting_order_values = ["", "asc", "desc"]
        outer_offset_values = ["", "10"]

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)

        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        for inner_field in inner_sorting_field_values:
            for inner_order in inner_sorting_order_values:
                for inner_offset in inner_offset_values:
                    for outer_field in outer_sorting_field_values:
                        for outer_order in outer_sorting_order_values:
                            for outer_offset in outer_offset_values:
                                inner_sort_expression = ""
                                if inner_field != "":
                                    inner_sort_expression = ", \"sort\": [{\"by\": \"field\", \"field\": \"" + \
                                                            inner_field + "\""
                                    if inner_order != "":
                                        inner_sort_expression = inner_sort_expression + ", \"mode\": \"" + \
                                                                inner_order + "\""
                                    if inner_offset != "":
                                        inner_sort_expression = inner_sort_expression + ", \"offset\": " + \
                                                                inner_offset + ""
                                    inner_sort_expression = inner_sort_expression + "}]"

                                outer_sort_expression = ""
                                if outer_field != "":
                                    outer_sort_expression = "order by " + outer_field + " " + outer_order
                                if outer_offset != "":
                                    outer_sort_expression = outer_sort_expression + " offset " + outer_offset

                                search_query = "select meta().id from {0} where search({0}, {{\"query\": {{\"field\":" \
                                               " \"state\", \"match\": \"California\"}}".format(self.query_bucket) + \
                                               inner_sort_expression + "}) " + outer_sort_expression
                                search_results = self.run_cbq_query(search_query)['results']
                                if outer_sort_expression == "":
                                    if inner_sort_expression != "":
                                        fts_request_str = "'{\"query\":{\"field\": \"state\", \"match\":" \
                                                          "\"California\"}, \"size\":1000," + inner_sort_expression + \
                                                          "}'"
                                    else:
                                        fts_request_str = "'{\"query\":{\"field\": \"state\", " \
                                                          "\"match\":\"California\"}, \"size\":10000}'"
                                    fts_request = json.loads(fts_request_str)
                                    total_hits, hits, took, status = rest.run_fts_query(
                                        index_name="idx_beer_sample_fts",
                                        query_json=fts_request)
                                    comparison_results = self._compare_n1ql_results_against_fts(search_results, hits)
                                    self.assertEqual(comparison_results, "OK", comparison_results)
                                else:
                                    n1ql_query = "select meta().id from {0} where search({0}, {{\"query\": " \
                                                 "{{\"field\": \"state\", \"match\": " \
                                                 "\"California\"}}}}) ".format(self.query_bucket) + outer_sort_expression
                                    n1ql_results = self.run_cbq_query(n1ql_query)['results']

                                    search_doc_ids = []
                                    for result in search_results:
                                        search_doc_ids.append(result['id'])

                                    n1ql_doc_ids = []
                                    for result in n1ql_results:
                                        n1ql_doc_ids.append(result['id'])

                                    self.assertEqual(len(n1ql_doc_ids), len(search_doc_ids),
                                                     "SEARCH QUERY - " + search_query + "\nN1QL QUERY - " + n1ql_query)
                                    self.assertEqual(sorted(search_doc_ids), sorted(n1ql_doc_ids),
                                                     "SEARCH QUERY - " + search_query + "\nN1QL QUERY - " + n1ql_query)

        self._remove_all_fts_indexes()

    def test_scan_consistency(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        scan_val = self.input.param("scan_type", '')
        count_before_update = self.run_cbq_query("select count(*) from {0} where search({0},"
                                                 " \"state:California\")".format(self.query_bucket))['results'][0]
        self.scan_consistency = scan_val

        update_query = "update {0} set state='Califffornia' where meta().id in ( select raw meta().id from {0} b " \
                       "where search(b, {{\"query\": {{\"field\": \"state\", \"match\": \"California\"}}, \"sort\":" \
                       " [{{\"by\": \"field\", \"field\": \"city\"}}]}}))".format(self.query_bucket)
        select_query = "select meta().id from {0} where search({0}, \"state:California\")".format(self.query_bucket)

        threads = []
        t = threading.Thread(target=self._update_parallel,
                             args=(update_query, "UPDATE", count_before_update['$1'], scan_val))
        t1 = threading.Thread(target=self._check_scan_parallel,
                              args=(select_query, count_before_update['$1'], scan_val))
        t.daemon = True
        t1.daemon = True
        threads.append(t)
        threads.append(t1)
        t.start()
        t1.start()
        for th in threads:
            th.join()
            threads.remove(th)

        update_query = "update {0} set state='California' where state='Califffornia'".format(self.query_bucket)
        self.run_cbq_query(update_query)

        self._remove_all_fts_indexes()

    def test_drop_fts_index(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)
        select_query = "select * from {0} l join {0} r on l.city=r.city where search(l," \
                       "\"city:San Francisco\")".format(self.query_bucket)
        if not self.is_index_present(self.sample_bucket, "beer_sample_city_idx"):
            self.run_cbq_query("create index beer_sample_city_idx on {0} ({0}.city)".format(self.query_bucket))

        threads = []
        t = threading.Thread(target=self._select_parallel, args=(select_query, 213,))
        t.daemon = True
        threads.append(t)
        t.start()

        t1 = threading.Thread(target=self._delete_fts_index, args=("idx_beer_sample_fts",))
        t1.daemon = True
        t1.start()
        threads.append(t1)

        for th in threads:
            th.join()
            threads.remove(th)

    def test_joins(self):
        tests = {
            "inner_l": {
                "query": "select * from {0} l join {0} r on l.city=r.city where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "inner_r": {
                "query": "select * from {0} l join {0} r on l.city=r.city where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "left_l": {
                "query": "select * from {0} l left join {0} r on l.city=r.city  where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "left_r": {
                "query": "select * from {0} l left join {0} r on l.city=r.city  where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "left_outer_l": {
                "query": "select * from {0} l left outer join {0} r on l.city=r.city  where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "left_outer_r": {
                "query": "select * from {0} l left outer join {0} r on l.city=r.city  where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "right_l": {
                "query": "select * from {0} l right join {0} r on l.city=r.city  where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "right_r": {
                "query": "select * from {0} l right join {0} r on l.city=r.city  where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "right_outer_l": {
                "query": "select * from {0} l right outer join {0} r on l.city=r.city  where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "right_outer_r": {
                "query": "select * from {0} l right outer join {0} r on l.city=r.city  where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_hash_build_l": {
                "query": "select * from {0} l join {0} r use hash(build) on l.city=r.city where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_hash_build_r": {
                "query": "select * from {0} l join {0} r use hash(build) on l.city=r.city  where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_hash_probe_l": {
                "query": "select * from {0} l join {0} r use hash(probe) on l.city=r.city where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_hash_probe_r": {
                "query": "select * from {0} l join {0} r use hash(probe) on l.city=r.city  where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_nl_l": {
                "query": "select * from {0} l join {0} r use nl on l.city=r.city where search(l,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_nl_r": {
                "query": "select * from {0} l join {0} r use nl on l.city=r.city  where search(r,"
                         " \"city:San Francisco\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "use_hash_keys_build_l": {
                "query": "select * from {0} l join {0} r use hash(build) keys [\"512_brewing_company\"] on"
                         " l.city=r.city where search(l, \"city:Austin\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_hash_keys_build_r": {
                "query": "select * from {0} l join {0} r use hash(build) keys [\"512_brewing_company\"] on"
                         " l.city=r.city where search(r, \"city:Austin\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "use_hash_keys_probe_l": {
                "query": "select * from {0} l join {0} r use hash(probe) keys [\"512_brewing_company\"] on"
                         " l.city=r.city where search(l, \"city:Austin\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "use_hash_keys_probe_r": {
                "query": "select * from {0} l join {0} r use hash(probe) keys [\"512_brewing_company\"] on"
                         " l.city=r.city where search(r, \"city:Austin\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "lookup_l": {
                "query": "select * from {0} l join {0} r on keys l.brewery_id where search(l,"
                         " \"city:Austin\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "lookup_r": {
                "query": "select * from {0} l join {0} r on keys l.brewery_id  where search(r,"
                         " \"city:Austin\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "index_l": {
                "query": "select * from {0} l join {0} r on key r.brewery_id for l where search(l,"
                         " \"city:Austin\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "index_r": {
                "query": "select * from {0} l join {0} r on key r.brewery_id for l where search(r,"
                         " \"city:Austin\")".format(self.query_bucket),
                "expected_result": "negative"
            },
            "in_l": {
                "query": "select * from {0} l join {0} r on l.brewery_id in r.code where search(l,"
                         " \"city:Austin\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "in_r": {
                "query": "select * from `beer-sample` l join `beer-sample` r on l.brewery_id in r.code where"
                         " search(r, \"city:Austin\")",
                "expected_result": "negative"
            },
            "any_satisfies_l": {
                "query": "select * from {0} l join {0} r on l.address=r.address and any v in r.address satisfies"
                         " (v='563 Second Street') end where search(l, \"city:Austin\")".format(self.query_bucket),
                "expected_result": "positive"
            },
            "any_satisfies_r": {
                "query": "select * from {0} l join {0} r on l.address=r.address and any v in r.address satisfies"
                         " (v='563 Second Street') end where search(r, \"city:Austin\")".format(self.query_bucket),
                "expected_result": "negative"
            },

        }
        test_name = self.input.param("test_name", '')
        if test_name == "":
            raise Exception("Test name cannot be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name=self.sample_bucket)

        if not self.is_index_present(self.sample_bucket, "beer_sample_city_idx"):
            self.run_cbq_query("create index beer_sample_city_idx on {0} ({0}.city)".format(self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "beer_sample_brewery_id_idx"):
            self.run_cbq_query("create index beer_sample_brewery_id_idx on {0} ({0}.brewery_id)".format(
                self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "beer_sample_address_arr_idx"):
            self.run_cbq_query(
                "create index beer_sample_address_arr_idx on {0} (all array v.address for v in address end)".format(
                    self.query_bucket))
        if not self.is_index_present(self.sample_bucket, "beer_sample_address_idx"):
            self.run_cbq_query("create index beer_sample_address_idx on {0} ({0}.address)".format(self.query_bucket))
        self.wait_for_all_indexes_online()

        n1ql_query = ""
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")
        try:
            n1ql_query = tests[test_name]['query']
            result = self.run_cbq_query(n1ql_query)
            self.assertEqual(result['status'], 'success', "The following query is incorrect - " + n1ql_query)

            explain_result = self.run_cbq_query("explain " + n1ql_query)
            if tests[test_name]['expected_result'] == "positive":
                self.assertTrue("idx_beer_sample_fts" in str(explain_result),
                                "FTS index is not used for query: " + n1ql_query)
            if tests[test_name]['expected_result'] == "negative":
                self.assertTrue("idx_beer_sample_fts" not in str(explain_result),
                                "FTS index is used for query: " + n1ql_query)
        except CBQError as err:
            self.log.info("Incorrect query ::" + n1ql_query + "::" + str(err))

        finally:
            self.drop_index_safe(self.sample_bucket, 'beer_sample_city_idx')
            self.drop_index_safe(self.sample_bucket, 'beer_sample_brewery_id_idx')
            self.drop_index_safe(self.sample_bucket, 'beer_sample_address_arr_idx')
            self.drop_index_safe(self.sample_bucket, 'beer_sample_address_idx')

    def test_expired_docs(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                   replicas=self.num_replicas,
                                                   enable_replica_index=self.enable_replica_index,
                                                   eviction_policy=self.eviction_policy, bucket_priority=None,
                                                   lww=self.lww, maxttl=60,
                                                   compression_mode=self.compression_mode)
        ttl_bucket = "ttl_bucket"
        self.cluster.create_standard_bucket(ttl_bucket, 11222, bucket_params)
        query_bucket = self.get_collection_name(ttl_bucket)
        for i in range(0, 100, 1):
            if i % 100 == 0:
                initial_statement = (" INSERT INTO {0} (KEY, VALUE) VALUES ('primary_key_" + str(i) + "',").format(
                    query_bucket)
                initial_statement += "{"
                initial_statement += "'primary_key':'primary_key_" + str(i) + "','string_field': 'test_string " + str(
                    i) + "','int_field':" + str(i) + "})"
            else:
                initial_statement = (" INSERT INTO {0} (KEY, VALUE) VALUES ('primary_key_" + str(i) + "',").format(
                    self.query_bucket)
                initial_statement += "{"
                initial_statement += "'primary_key':'primary_key_" + str(i) + "','string_field': 'string data " + str(
                    i) + "','int_field':" + str(i) + "})"
            self.run_cbq_query(initial_statement)

        self._create_fts_index(index_name="idx_ttl_bucket_fts", doc_count=100, source_name=ttl_bucket)

        results_before_expiration = self.run_cbq_query(
            "select count(*) from ttl_bucket where search({0}, \"string_field:string\")".format(self.query_bucket))
        self.assertTrue(results_before_expiration['results'][0]['$1'] > 0, "Results before expiration must be positive")
        self.sleep(300)
        results_after_expiration = self.run_cbq_query(
            "select count(*) from ttl_bucket where search({0}, \"string_field:string\")".format(self.query_bucket))
        self.assertTrue(results_after_expiration['results'][0]['$1'] == 0, "Results after expiration must be zero")

    # ============================================ utils =================================
    def _compare_n1ql_results_against_fts(self, n1ql_results, hits):
        n1ql_doc_ids = []
        for result in n1ql_results:
            n1ql_doc_ids.append(result['id'])
        fts_doc_ids = []
        for hit in hits:
            fts_doc_ids.append(hit['id'])

        if len(n1ql_doc_ids) != len(fts_doc_ids):
            return "Results count does not match for test . FTS - " + str(len(fts_doc_ids)) + ", N1QL - " + str(
                len(n1ql_doc_ids))
        if sorted(fts_doc_ids) != sorted(n1ql_doc_ids):
            return "Found mismatch in results for test ."
        return "OK"

    def _check_scan_parallel(self, query, expected_count, scan_type):
        try:
            search_results = self.run_cbq_query(query)['metrics']['resultCount']
            self.assertEquals(expected_count - int(search_results) > 0, True,
                              "Query result is incorrect for " + scan_type + ": \n"
                                                                             "Results before update - " + str(
                                  expected_count) + ", count during update - " + str(search_results))
        except CBQError:
            self.assertEquals('True', 'False', 'Wrong query - ' + str(query))

    def _update_parallel(self, query, operation, expected_count, scan_type):
        try:
            self.run_cbq_query(query)
        except CBQError:
            self.assertEquals('True', 'False', 'Wrong query - ' + str(query))

    def _select_parallel(self, query, expected_count):
        try:
            search_results = self.run_cbq_query(query)['metrics']['resultCount']
            self.assertEquals(expected_count, search_results, "Query result is incorrect")
        except CBQError:
            self.assertEquals('True', 'False', 'Wrong query - ' + str(query))

    def _load_test_buckets(self):
        if self.get_bucket_from_name(self.sample_bucket) is None:
            self.rest.load_sample(self.sample_bucket)
            self.wait_for_buckets_status({self.sample_bucket: "healthy"}, 5, 120)
            self.wait_for_bucket_docs({self.sample_bucket: 7303}, 5, 120)
        self.wait_for_all_indexes_online()
        self.sleep(10, "wait after indexes are online")
        if not self.is_index_present(self.sample_bucket, "beer_sample_code_idx"):
            self.run_cbq_query("create index beer_sample_code_idx on {0} (code)".format(self.query_bucket))
            self.wait_for_all_indexes_online()
            self.sleep(10, "wait after indexes are online")
        if not self.is_index_present(self.sample_bucket, "beer_sample_brewery_id_idx"):
            self.run_cbq_query("create index beer_sample_brewery_id_idx on {0} (brewery_id)".format(
                self.query_bucket))
            self.wait_for_all_indexes_online()
            self.sleep(10, "wait after indexes are online")

    def _create_fts_index(self, index_name='', doc_count=0, source_name='', poll_interval=10, num_retries=30):
        fts_index_type = self.input.param("fts_index_type", "scorch")

        fts_index = self.cbcluster.create_fts_index(name=index_name, source_name=source_name)
        if fts_index_type == 'upside_down':
            fts_index.update_index_to_upside_down()
        else:
            fts_index.update_index_to_scorch()
        indexed_doc_count = 0
        while (indexed_doc_count < doc_count) and num_retries > 0:
            try:
                self.sleep(poll_interval)
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError:
                continue
            finally:
                num_retries -= 1

        return fts_index

    def _delete_fts_index(self, index_name=''):
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        rest.delete_fts_index(index_name)

    def _open_curl_access(self):
        shell = RemoteMachineShellConnection(self.master)

        cmd = (
                self.curl_path + ' -u ' + self.master.rest_username + ':' + self.master.rest_password +
                ' http://' + self.master.ip + ':' + self.master.port +
                '/settings/querySettings/curlWhitelist -d \'{"all_access":true}\'')
        shell.execute_command(cmd)

    def _create_all_users(self):
        admin_user = [{'id': 'admin_user', 'name': 'admin_user', 'password': 'password'}]
        rolelist = [{'id': 'admin_user', 'name': 'admin_user', 'roles': 'admin'}]
        RbacBase().create_user_source(admin_user, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['admin_user'] = {'username': 'admin_user', 'password': 'password'}

        all_buckets_data_reader_search_admin = [
            {'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin',
             'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin',
                     'roles': 'query_select[*],fts_admin[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_search_admin'] = {'username': 'all_buckets_data_reader_search_admin',
                                                              'password': 'password'}

        all_buckets_data_reader_search_reader = [
            {'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader',
             'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader',
                     'roles': 'query_select[*],fts_searcher[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_search_reader'] = {'username': 'all_buckets_data_reader_search_reader',
                                                               'password': 'password'}

        test_bucket_data_reader_search_admin = [
            {'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin',
             'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin',
                     'roles': 'query_select[beer-sample],fts_admin[beer-sample],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_search_admin'] = {'username': 'test_bucket_data_reader_search_admin',
                                                              'password': 'password'}

        test_bucket_data_reader_null = [
            {'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null',
                     'roles': 'query_select[beer-sample],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_null'] = {'username': 'test_bucket_data_reader_null',
                                                      'password': 'password'}

        test_bucket_data_reader_search_reader = [
            {'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader',
             'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader',
                     'roles': 'query_select[beer-sample],fts_searcher[beer-sample],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_search_reader'] = {'username': 'test_bucket_data_reader_search_reader',
                                                               'password': 'password'}

        all_buckets_data_reader_null = [
            {'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null',
                     'roles': 'query_select[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_null'] = {'username': 'all_buckets_data_reader_null',
                                                      'password': 'password'}

    def _remove_all_fts_indexes(self):
        indexes = self.cbcluster.get_indexes()
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        for index in indexes:
            rest.delete_fts_index(index.name)

    def get_rest_client(self, user, password):
        rest = RestConnection(self.cbcluster.get_random_fts_node())
        rest.username = user
        rest.password = password
        return rest
