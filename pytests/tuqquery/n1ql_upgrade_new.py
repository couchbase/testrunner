import copy
import random
import string
from random import randint

import math
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection
from n1ql_callable import N1QLCallable
from remote.remote_util import RemoteMachineShellConnection
from tuq import QueryTests

from pytests.fts.fts_base import CouchbaseCluster
from pytests.upgrade.newupgradebasetest import NewUpgradeBaseTest


class QueriesUpgradeTestsNew(QueryTests, NewUpgradeBaseTest):

    def setUp(self):
        super(QueriesUpgradeTestsNew, self).setUp()
        # ################## UTILITY CLASSES ############
        self.n1ql_callable = N1QLCallable(self.servers)

        # ################## CONSTANTS ##################
        self.scalars_test_data = {'not_null_1': {'key': 'key1', 'value': 'val1'},
                                  'not_null_2': {'key': 'key2', 'value': 'val2'},
                                  'null': {'key': 'key3', 'value': 'null'},
                                  'missing': {'key': 'key4', 'value': 'missing'},
                                  'empty_string': {'key': 'key5', 'value': ''},
                                  'string_null': {'key': 'key6', 'value': 'null'},
                                  'string_NULL': {'key': 'key7', 'value': 'NULL'},
                                  'string_NuLl': {'key': 'key8', 'value': 'NuLl'}
                                  }
        self.test_data = {"int_field": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                                        "16", "17", "18", "19", "20"],
                          "bool_field": ["TRUE", "FALSE"],
                          "varchar_field": ["string1", "string2", "string3", "string4", "string5",
                                            "string6", "string7", "string8", "string9", "string10",
                                            "string11", "string12", "string13", "string14", "string15",
                                            "string16", "string17", "string18", "string19", "string20"]
                          }

        self.primary_index_def = {'name': '#primary', 'bucket': 'default', 'fields': [],
                                  'state': 'online', 'using': self.index_type.lower(),
                                  'is_primary': True}
        self.numbers = [1, 3, 5, 7, 9, 10, 0, 2, 4, 6, 8, 13]
        self.test_bucket_size = 999
        self.fts_indexes = {}
        self.alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
                         'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

        self.pre_upgrade_mode = "pre-upgrade"
        self.post_upgrade_mode = "post-upgrade"

        self.initial_version = self.input.param("initial_version", "")

        if self.initial_version == "":
            raise Exception("Undefined initial version is not supported!")

        self.load(self.gens_load, flag=self.item_flag)
        self.bucket_doc_map = {"default": 2017, "standard_bucket0": 2016}
        self.bucket_status_map = {"default": "healthy", "standard_bucket0": "healthy"}

        self._init_nodes()

        self._load_test_data()
        self._create_fts_indexes()

        self._create_indexes()

        if self._testMethodName == 'suite_setUp':
            return
        self.log.info("==============  QueriesUpgradeTestsNew setup has started ==============")

        self.log.info("==============  QueriesUpgradeTests setup has completed ==============")

    def suite_setUp(self):
        super(QueriesUpgradeTestsNew, self).suite_setUp()
        self.log.info("==============  QueriesUpgradeTestsNew suite_setup has started ==============")
        self.log.info("==============  QueriesUpgradeTestsNew suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueriesUpgradeTestsNew tearDown has started ==============")
        self.upgrade_servers = self.servers
        self.log.info("==============  QueriesUpgradeTestsNew tearDown has completed ==============")
        super(QueriesUpgradeTestsNew, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueriesUpgradeTestsNew suite_tearDown has started ==============")
        self.log.info("==============  QueriesUpgradeTestsNew suite_tearDown has completed ==============")
        super(QueriesUpgradeTestsNew, self).suite_tearDown()

# ################################## TESTS ###############################

    def test_prepareds(self):
        create_prepared = "prepare pstmt from select meta().id from default where email='4-mail@couchbase.com'"
        self.run_cbq_query(create_prepared)
        call_query = "execute pstmt"
        ps_results_initial = self.run_cbq_query_curl(query="'" + call_query + "'")['results']

        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            output, error = self._upgrade(upgrade_version, self.servers[1])
        ps_results_upgraded_partial_1 = self.run_cbq_query_curl(query="'" + call_query + "'")['results']
        ps_results_not_upgraded_partial_1 = self.run_cbq_query_curl(query="'" + call_query + "'")['results']

        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            output, error = self._upgrade(upgrade_version, self.servers[2])
        ps_results_upgraded_partial_2 = self.run_cbq_query_curl(query="'" + call_query + "'")['results']

        for upgrade_version in self.upgrade_versions:
            self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                       format(upgrade_version))
            output, error = self._upgrade(upgrade_version, self.servers[0])
        ps_results_upgraded_totally = self.run_cbq_query_curl(query="'" + call_query + "'")['results']

        self.assertTrue(
            ps_results_initial == ps_results_upgraded_partial_1 and
            ps_results_initial == ps_results_not_upgraded_partial_1 and
            ps_results_initial == ps_results_upgraded_partial_2 and
            ps_results_initial == ps_results_upgraded_totally,
            "Upgrade test for prepareds is failed!")

    def test_all_simple_features(self):
        upgrade_path = self.input.param("upgrade_path", "")
        if upgrade_path == "":
            raise Exception("Incorrect upgrade path")

        steps = upgrade_path.split("-")
        for step in steps:
            upgrade_node_number = step[len("upgrade["):step.find("]:test")]
            if upgrade_node_number != "":
                for upgrade_version in self.upgrade_versions:
                    self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                               format(upgrade_version))
                    output, error = self._upgrade(upgrade_version, self.servers[int(upgrade_node_number)])
            begin = step.find("test[") + len("test[")
            end = len(step) - 1
            test_nodes_str = step[int(begin):int(end)]
            if test_nodes_str != "":
                test_nodes_array = test_nodes_str.split(".")
                for test_node in test_nodes_array:
                    test_node_num, test_mode = test_node.split(":")
                    if test_mode == "post":
                        mode = "post-upgrade"
                    elif test_mode == "pre":
                        mode = "pre-upgrade"
                    else:
                        raise Exception("Unknown test mode! Check config!")
                    errors = self.check_all_simple_features_for_configuration_against_node(
                        node=self.servers[int(test_node_num)],
                        mode=mode)
                    try:
                        self.assertEqual(len(errors.keys()), 0)
                    except AssertionError:
                        error_message = "Some simple features tests are failed.\n"
                        self.log.info("Some simple features tests are failed.")
                        for key in errors.keys():
                            error_message = error_message + ("Test "+str(key)+" is failed. Reason - "+str(errors[key])+"\n")
                        self.assertEqual(True, False, error_message)

    def test_n1ql_fts_integration(self):
        upgrade_path = self.input.param("upgrade_path", "")
        if upgrade_path == "":
            raise Exception("Incorrect upgrade path")

        steps = upgrade_path.split("-")
        for step in steps:
            upgrade_node_number = step[len("upgrade["):step.find("]:test")]
            if upgrade_node_number != "":
                for upgrade_version in self.upgrade_versions:
                    self.sleep(self.sleep_time, "Pre-setup of old version is done. Wait for upgrade to {0} version". \
                               format(upgrade_version))
                    output, error = self._upgrade(upgrade_version, self.servers[int(upgrade_node_number)])
            begin = step.find("test[") + len("test[")
            end = len(step) - 1
            test_nodes_str = step[int(begin):int(end)]
            if test_nodes_str != "":
                test_nodes_array = test_nodes_str.split(".")
                for test_node in test_nodes_array:
                    test_node_num, test_mode = test_node.split(":")
                    if test_mode == "post":
                        mode = "post-upgrade"
                    elif test_mode == "pre":
                        mode = "pre-upgrade"
                    else:
                        raise Exception("Unknown test mode! Check config!")
                    errors = self.check_n1ql_fts_integration(node=self.servers[int(test_node_num)], mode=mode)
                    try:
                        self.assertEqual(len(errors.keys()), 0)
                    except AssertionError:
                        self.log.info("Some n1ql-fts integration tests are failed.")
                        for key in errors.keys():
                            self.log.info("Test "+str(key)+" is failed. Reason - "+str(errors[key]))
                        self.assertEqual(True, False)

    def check_n1ql_fts_integration(self, node, mode):
        errors = {}
        error_message = "Invalid function search"
        self.create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        n1ql_query = "select * from `beer-sample` where search(`beer-sample`, " \
                     "{\"field\": \"type\", \"match\":\"beer\"})"
        try:
            self.run_cbq_query(n1ql_query, server=node)
        except CBQError as e:
            fails_found = 1
            actual_error_message = str(e)
            if mode == "post-upgrade":
                errors["n1ql-fts"] = 'Query was not run successfully'
            else:
                if fails_found == 1:
                    if error_message not in actual_error_message:
                        errors["n1ql-fts"] = 'Unexpected error message'
                else:
                    errors["n1ql-fts"] = "Unexpected success for old version!"

        return errors

    def check_all_simple_features_for_configuration_against_node(self, node, mode):
        report = {}
        error = self.check_ansi_merge(mode, node)
        if error != "":
            report["ansi merge"] = error
        error = self.check_subquery(mode, node)
        if error != "":
            report["subquery"] = error
        error = self.check_scalar_functions(mode, node)
        if error != "":
            report["scalar functions"] = error
        error = self.check_additional_staistics(mode, node)
        if error != "":
            report["additional staistics"] = error
        error = self.check_in_list_operator(mode, node)
        if error != "":
            report["in list operator"] = error
        error = self.check_index_unnest(mode, node)
        if error != "":
            report["index unnest"] = error
        error = self.check_arbitrary_unnest_alias(mode, node)
        if error != "":
            report["arbitrary unnest alias"] = error
        error = self.check_cte(mode, node)
        if error != "":
            report["cte"] = error
        error = self.check_skip_key_composite_index(mode, node)
        if error != "":
            report["skip key composite index"] = error
        error = self.check_new_aggregates(mode, node)
        if error != "":
            report["new aggregates"] = error
        error = self.check_order_by_nulls(mode, node)
        if error != "":
            report["order by nulls"] = error
        error = self.check_group_by_alias(mode, node)
        if error != "":
            report["group by alias"] = error
        error = self.check_chained_let(mode, node)
        if error != "":
            report["chained let"] = error
        error = self.check_decode_array_binary_search(mode, node)
        if error != "":
            report["decode, array binary search"] = error
        error = self.check_window_functions(mode, node)
        if error != "":
            report["window functions"] = error
        error = self.check_build_index_expressions(mode, node)
        if error != "":
            report["build index expressions"] = error
        error = self.check_n1ql_fts_search_query(mode, node)
        if error != "":
            report["n1ql_fts_search_query"] = error

        return report


# ################################# TEST METHODS ##################
    def check_ansi_merge(self, mode, node):
        error_message = "syntax error"
        error = ""
        try:
            query = 'MERGE INTO %s USING %s on meta(%s).id == meta(%s).id when matched then delete' % (self.buckets[0].name, self.buckets[1].name, self.buckets[0].name, self.buckets[1].name)
            actual_result = self.run_cbq_query(query=query, server=node)
            if mode == self.post_upgrade_mode:
                try:
                    self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
                except AssertionError:
                    error = 'Query was not run successfully'
                try:
                    self.assertEqual(actual_result['metrics']['mutationCount'], 1000,
                                     'Merge deleted more data than intended')
                except AssertionError:
                    error = 'Merge deleted more data than intended'
        except CBQError as e:
            fails_found = 1
            actual_error_message = str(e)
            if mode == self.post_upgrade_mode:
                error = 'Query was not run successfully'
            else:
                if fails_found == 1:
                    self.assertTrue(error_message in actual_error_message)
                else:
                    error = "Unexpected success for old version!"
        except Exception:
            pass
        return error

    def check_subquery(self, mode, node):
        fails_found = 0
        error = ""

        actual_result = self.run_cbq_query(query='SELECT meta().id, (SELECT RAW SUM(VMs.memory) '
                                                 'FROM default.VMs AS VMs)[0] AS total '
                                                 'FROM default order by total', server=node)
        expected_result = self.run_cbq_query(query='SELECT meta().id, (SELECT RAW SUM(VMs.memory) '
                                                   'FROM default.VMs)[0] AS total FROM default order by total',
                                             server=node)
        try:
            self.assertTrue(actual_result['results'] == expected_result['results'])
        except AssertionError:
            fails_found = 1

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        return error

    def check_scalar_functions(self, mode, node):
        fails_found = 0
        error = ""
        query = "select nvl(" + self.scalars_test_data['not_null_1']['key'] + ", " + self.scalars_test_data['not_null_2']['key'] + ") from temp_bucket USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['not_null_1']['value']
        error_message = "Invalid function nvl."
        error_message_found = ""
        result = {}
        try:
            result = self.run_cbq_query(query=query, server=node)
        except CBQError as e:
            fails_found = 1
            error_message_found = str(e)

        if mode == self.post_upgrade_mode:
            try:
                self.assertEqual(result["results"][0]["$1"] == expected_result, True)
            except AssertionError:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 1:
                try:
                    self.assertTrue(error_message in error_message_found, True)
                except AssertionError:
                    error = "Incorrect error message."
            else:
                error = "Unexpected success for old version!"
        return error

    def check_additional_staistics(self, mode, node):
        error = ""
        self.rest = RestConnection(node)
        stats = self.rest.query_tool_stats()
        expected_result = False
        if mode == self.post_upgrade_mode:
            expected_result = True
        try:
            self.assertEqual('audit_requests_total.count' in stats.keys(), expected_result,
                              'Audit requests test is failed')
        except AssertionError:
            error = "Unexpected success for " + mode + "d version!"

        return error

    def check_in_list_operator(self, mode, node):
        fails_found = 0
        error = ""
        query = "select varchar_field from temp_bucket where int_field in ["
        for i in range(1001):
            query += str(i)
            if i < 1000:
                query += ","
        query += "]"
        try:
            self.run_cbq_query(query=query, server=node)
        except CBQError:
            fails_found = 1
        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        return error

    def check_index_unnest(self, mode, node):
        fails_found = 0
        error = ""
        self.n1ql_callable.drop_gsi_index(name="array_index", keyspace="default")
        index = "CREATE INDEX array_index ON default(DISTINCT ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index, server=node)
        # First check explain to make sure improvement is happening
        query = 'SELECT v1 FROM default AS d UNNEST d.VMs AS v1 where d.email == "9-mail@couchbase.com" ' \
                'and d.department == "support" and v1.RAM == 10'
        explain_query = 'EXPLAIN ' + query
        explain_plan = self.run_cbq_query(query=explain_query, server=node)
        plan = self.ExplainPlanHelper(explain_plan)
        try:
            self.assertTrue('9-mail@couchbase.com' in str(plan['~children'][0]['scan']['spans'])
                            and 'support' in str(plan['~children'][0]['scan']['spans']),
                            "The non-array index keys are not being used")
        except AssertionError:
            fails_found = 1
        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        return error

    def check_arbitrary_unnest_alias(self, mode, node):
        error = ""
        fails_found = 0
        self.n1ql_callable.drop_gsi_index(name="array_index", keyspace="default")
        index = "CREATE INDEX array_index ON default(ALL ARRAY v1 FOR v1 IN VMs END,email,department)"
        self.run_cbq_query(query=index)
        # First check explain to make sure improvement is happening
        query = 'SELECT nv1 FROM default AS d UNNEST d.VMs AS nv1 where d.email == "9-mail@couchbase.com" ' \
                'and d.department == "support" and nv1.RAM == 10'
        explain_query = 'EXPLAIN ' + query
        explain_plan = self.run_cbq_query(query=explain_query, server=node)
        plan = self.ExplainPlanHelper(explain_plan)
        try:
            self.assertTrue('nv1' in str(plan['~children'][0]['covers'][0]) and
                            plan['~children'][0]['index'] == 'array_index',
                            "The non-array index keys are not being used")
        except KeyError:
            fails_found = 1
        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 0:
                error = "Unexpected success for old version!"

        return error

    def check_cte(self, mode, node):
        fails_found = 0
        error = ""
        # constant expression
        query = 'with a as (10), b as ("bob"), c as (3.3), d as (0), e as (""), f as (missing), g as (null) '
        query += 'select a, b, c, d, e, f, g, join_yr as h, join_yr+a as i, join_yr+b as j, join_yr+c as k, ' \
                 'join_yr+d as l, join_yr+e as m, join_yr+f as n, join_yr+g as o from default '
        query += 'where a > 9 and b == "bob" and c < 4 and d == 0 and e is valued and f is missing and g is null '
        query += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o'

        verify = 'select 10 as a, "bob" as b, 3.3 as c, 0 as d, "" as e, missing as f, null as g, join_yr as h, ' \
                 'join_yr+10 as i, join_yr+"bob" as j, join_yr+3.3 as k, join_yr+0 as l, join_yr+"" as m, ' \
                 'join_yr+missing as n, join_yr+null as o from default '
        verify += 'where 10 > 9 and "bob" == "bob" and 3.3 < 4 and 0 == 0 and "" is valued ' \
                  'and missing is missing and null is null '
        verify += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o'

        try:
            cte_result = self.run_cbq_query(query=query, server=node)
            verification_result = self.run_cbq_query(query=verify, server=node)
            self.assertEqual(cte_result["results"], verification_result["results"])
        except CBQError as e:
            error_message = str(e)
            fails_found = 1
            if "syntax error - at with" not in error_message:
                error = "Incorrect error message, expected ... syntax error - at with ..."
        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 0:
                error = "Unexpected success for old version!"

        return error

    def check_skip_key_composite_index(self, mode, node):
        fails_found = 0

        query_1a = "select * from default where join_yr > 2010 and join_day > 3"
        verify_1 = "select * from default USE INDEX (`#primary` USING GSI) where join_yr > 2010 and join_day > 3"
        query_1b = "explain select * from default where join_yr > 2010 and join_day > 3"
        spans_1 = [{u'inclusion': 0, u'low': u'2010'}, {u'inclusion': 0}, {u'inclusion': 0, u'low': u'3'}]
        self.n1ql_callable.create_gsi_index(keyspace="default", name="idx1", fields="join_yr,join_mo,join_day",
                                            using="gsi", is_primary=False)

        query_1a_results = self.run_cbq_query(query=query_1a, server=node)
        result, error = self.skip_key_composite_index_compare_queries(actual_results=query_1a_results,
                                                                      compare_query=verify_1, node=node)

        query_1b_results = self.run_cbq_query(query=query_1b, server=node)
        result1, error1 = self.skip_key_composite_index_check_plan(explain_results=query_1b_results,
                                                                   check_type="spans", check_data=spans_1)

        result2, error2 = self.skip_key_composite_index_check_plan(explain_results=query_1b_results,
                                                                   check_type="index", check_data="idx1")

        if mode == self.post_upgrade_mode:
            try:
                self.assertEqual(result, result1)
                self.assertEqual(result, result2)
            except AssertionError:
                fails_found = 1
        self.n1ql_callable.drop_gsi_index(keyspace="default", name="idx1", is_primary=False)

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"

        return error

    def check_new_aggregates(self, mode, node):
        fails_found = 0
        error = ""
        query = "select median(int_field) from aggs_bucket where int_field is not null or missing"
        args = map(lambda x: float(x), [1, 3, 5, 7, 9, 10, 0, 2, 4, 6, 8, 13])
        arithmetic_result = self._calculate_median_value(args)
        error_message = ""
        try:
            n1ql_result = self.run_cbq_query(query=query, server=node)
            my_result = "%.9f" % float(arithmetic_result)
            self.assertEqual("%.9f" % float(n1ql_result['results'][0]["$1"]), my_result, "Test is failed")
        except CBQError as e:
            fails_found = 1
            error_message = str(e)

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 1:
                if "Invalid function median" not in error_message:
                    error = "Unexpected error message"
            else:
                error = "Unexpected success for old version!"

        return error

    def check_order_by_nulls(self, mode, node):
        fails_found = 0
        error = ""
        error_message = ""
        query = "select int_field from temp_bucket order by int_field DESC NULLS FIRST"
        try:
            n1ql_result = self.run_cbq_query(query=query, server=node)
            self.assertEqual(True, self._check_order_nulls_first_desc(n1ql_result, "int"))
        except CBQError as e:
            fails_found = 1
            error_message = str(e)

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 1:
                if "syntax error - at NULLS" not in error_message:
                    error = "Unexpected error message"
            else:
                error = "Unexpected success for old version!"

        return error

    def check_group_by_alias(self, mode, node):
        error = ""
        error_message = ""
        fails_found = 0
        alias_query = "select job_title as jt from default where email like '%couchbase.com' " \
                      "group by job_title as jt order by jt asc"
        simple_query = "select job_title from default where email like '%couchbase.com' " \
                       "group by job_title order by job_title asc"
        try:
            alias_result = self.run_cbq_query(query=alias_query, server=node)
            simple_result = self.run_cbq_query(query=simple_query, server=node)
            alias_results = []
            simple_results = []
            for res in alias_result['results']:
                alias_results.append(list(res.values())[0])
            for res in simple_result['results']:
                simple_results.append(list(res.values())[0])
            self.assertEqual(alias_results, simple_results, "Alias in group by is failed.")
        except CBQError as e:
            fails_found = 1
            error_message = str(e)

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 1:
                if "syntax error - at as" not in error_message:
                    error = "Unexpected error message"
            else:
                error = "Unexpected success for old version!"

        return error

    def check_chained_let(self, mode, node):
        fails_found = 0
        queries = dict()
        error = ""

        # constants
        query_1 = 'select a, b, c from default let a=1,b=2,c=3 order by a, b, c limit 10'
        verify_1 = 'select 1 as a, 2 as b, 3 as c from default order by a, b, c limit 10'

        query_2 = 'select a, b, c from default let a=cos(1),b=cos(2),c=cos(3) order by a, b, c limit 10'
        verify_2 = 'select cos(1) as a, cos(2) as b, cos(3) as c from default order by a, b, c limit 10'

        query_3 = 'select a, b, c from default let a=1,b=a+1,c=b+1 order by a, b, c limit 10'
        verify_3 = 'select 1 as a, 2 as b, 3 as c from default order by a, b, c limit 10'

        query_4 = 'select a, b, c from default let a=cos(1),b=cos(a+1),c=cos(b+1) order by a, b, c limit 10'
        verify_4 = 'select cos(1) as a, cos(cos(1)+1) as b, cos(cos(cos(1)+1)+1) as c from default ' \
                   'order by a, b, c ' \
                   'limit 10'

        # fields
        query_5 = 'select a, b, c from default let a=join_yr,b=join_day,c=join_mo order by a, b, c limit 10'
        verify_5 = 'select join_yr as a, join_day as b, join_mo as c from default order by a, b, c limit 10'

        query_6 = 'select a, b, c from default let a=cos(join_yr+1),b=cos(join_day+1),c=cos(join_mo+1) ' \
                  'order by a, b, c ' \
                  'limit 10'
        verify_6 = 'select cos(join_yr+1) as a, cos(join_day+1) as b, cos(join_mo+1) as c from default ' \
                   'order by a, b, c ' \
                   'limit 10'

        query_7 = 'select a, b, c from default let a=join_yr,b=a+join_day,c=b+join_mo order by a, b, c limit 10'
        verify_7 = 'select join_yr as a, join_yr+join_day as b, join_yr+join_day+join_mo as c from default ' \
                   'order by a, b, c limit 10'

        query_8 = 'select a, b, c from default let a=cos(join_yr+1),b=cos(a+join_day+1),c=cos(b+join_mo+1) ' \
                  'order by a, b, c limit 10'
        verify_8 = 'select cos(join_yr+1) as a, cos(cos(join_yr+1)+join_day+1) as b, ' \
                   'cos(cos(cos(join_yr+1)+join_day+1)+join_mo+1) as c from default order by a, b, c limit 10'

        # subqueries
        query_9 = 'select a, b, c from default d0 let a=(select join_yr from default d2 order by join_yr limit 5), ' \
                  'b=(select join_mo from default d3 order by join_mo limit 5), c=(select join_day from default d4 ' \
                  'order by join_day limit 5) order by a, b, c limit 10'
        verify_9 = 'select (select join_yr from default d2 order by join_yr limit 5) as a, ' \
                   '(select join_mo from default d3 order by join_mo limit 5) as b, (select join_day ' \
                   'from default d4 order by join_day limit 5) as c from default d0 order by a, b, c limit 10'

        query_10 = 'select a, b from default d0 let usekeys=(select meta(d1).id, join_day from default d1 limit 10), ' \
                   'a=(select join_day from default d2 limit 10), b=(select join_mo from default d3 use keys ' \
                   'usekeys[*].id where join_day in a[*].join_day limit 10) order by a, b limit 10'
        verify_10 = 'select (select join_day from default d1 limit 10) as a, (select join_mo from default d2 where ' \
                    'join_mo in (select raw join_mo from default d3 limit 10) limit 10) as b from default d7 ' \
                    'order by a, b limit 10'

        query_11 = 'select a, b, c from default d0 let usekeys=(select raw meta(d1).id from default d1 ' \
                   'order by meta(d1).id limit 10),a=1,b=join_mo+a,c=(select join_day from default d2 ' \
                   'use keys usekeys where join_day != b order by join_day limit 10) order by a, b, c limit 10'
        verify_11 = 'select 1 as a, join_mo+1 as b, (select join_day from default d2 ' \
                    'use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 10) ' \
                    'where join_day != (join_mo+1) order by join_day limit 10) as c from ' \
                    'default d0 order by a, b, c limit 10'

        # full query
        query_12 = 'select a, b, c from default let a=join_yr, b=join_mo, c=join_day ' \
                   'where (a > 100 OR c < 100) and b != 200 group by a,b,c ' \
                   'having (a>b and b>c) or a == b+c order by a, b, c limit 10'
        verify_12 = 'select join_yr as a, join_mo as b, join_day as c from default ' \
                    'where (join_yr > 100 OR join_day < 100) and join_mo != 200 ' \
                    'group by join_yr, join_mo, join_day having (join_yr>join_mo and join_mo>join_day) ' \
                    'or join_yr == join_mo+join_day order by join_yr, join_mo, join_day limit 10'

        # mixed
        query_13 = 'select a, b, c from default d0 let usekeys=(select raw meta(d1).id from default d1 ' \
                   'order by meta(d1).id limit 10),a=1,b=cos(join_day)+a,c=(select join_day ' \
                   'from default d1 use keys usekeys where join_day > a + b order by join_day limit 10) ' \
                   'where join_day in c order by a,b,c limit 10'
        verify_13 = 'select 1 as a, cos(join_day)+1 as b, (select join_day from default d1 ' \
                    'use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 10) ' \
                    'where join_day > 1 + cos(join_day)+1 order by join_day limit 10) as c from default d0 ' \
                    'where join_day in (select join_day from default d1 ' \
                    'use keys (select raw meta(d1).id from default d1 order by meta(d1).id limit 10) ' \
                    'where join_day > 1 + cos(join_day)+1 order by join_day limit 10) order by a,b,c limit 10'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier_chained_let(verify_1)], "server": node}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier_chained_let(verify_2)], "server": node}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier_chained_let(verify_3)], "server": node}
        queries["c"] = {"queries": [query_4], "asserts": [self.verifier_chained_let(verify_4)], "server": node}
        queries["e"] = {"queries": [query_5], "asserts": [self.verifier_chained_let(verify_5)], "server": node}
        queries["f"] = {"queries": [query_6], "asserts": [self.verifier_chained_let(verify_6)], "server": node}
        queries["g"] = {"queries": [query_7], "asserts": [self.verifier_chained_let(verify_7)], "server": node}
        queries["h"] = {"queries": [query_8], "asserts": [self.verifier_chained_let(verify_8)], "server": node}
        queries["i"] = {"queries": [query_9], "asserts": [self.verifier_chained_let(verify_9)], "server": node}
        queries["j"] = {"queries": [query_10], "asserts": [self.verifier_chained_let(verify_10)], "server": node}
        queries["k"] = {"queries": [query_11], "asserts": [self.verifier_chained_let(verify_11)], "server": node}
        queries["l"] = {"queries": [query_12], "asserts": [self.verifier_chained_let(verify_12)], "server": node}
        queries["m"] = {"queries": [query_13], "asserts": [self.verifier_chained_let(verify_13)], "server": node}

        try:
            self.query_runner(queries)
        except AssertionError:
            fails_found = 1

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 0:
                error = "Unexpected success for old version!"

        return error

    def check_decode_array_binary_search(self, mode, node):
        error = ""
        fails_found = 0
        queries = dict()

        query_1 = "select decode(join_yr, 2010, 'first_result_term', 2011, 'second_result_term') from default"
        verify_1 = "select case when join_yr = 2010 then 'first_result_term' " \
                   "when join_yr = 2011 then 'second_result_term' end from default"

        query_2 = "select decode(join_yr, 2011, 'first_result_term', 2012, 'second_result_term', 'default_term') " \
                  "from default"
        verify_2 = "select case when join_yr = 2011 then 'first_result_term' " \
                   "when join_yr = 2012 then 'second_result_term' else 'default_term' end from default"

        query_3 = "select decode(join_yr, 2011, 'first_result_term', 2011, 'second_result_term', 'default_term') " \
                  "from default"
        verify_3 = "select case when join_yr = 2011 then 'first_result_term' " \
                   "when join_yr = 2011 then 'second_result_term' else 'default_term' end from default"

        query_4 = "select decode(join_yr, 2011, 'first_result_term', 2014, 'second_result_term', 'default_term') " \
                  "from default"
        verify_4 = "select case when join_yr = 2011 then 'first_result_term' " \
                   "when join_yr = 2014 then 'second_result_term' else 'default_term' end from default"

        query_5 = "select decode(join_yr, 2014, 'first_result_term', 2012, 'second_result_term', 'default_term') " \
                  "from default"
        verify_5 = "select case when join_yr = 2014 then 'first_result_term' " \
                   "when join_yr = 2012 then 'second_result_term' else 'default_term' end from default"

        query_6 = "select decode(join_yr, 2013, 'first_result_term', 2014, 'second_result_term', 'default_term') " \
                  "from default"
        verify_6 = "select case when join_yr = 2013 then 'first_result_term' " \
                   "when join_yr = 2014 then 'second_result_term' else 'default_term' end from default"

        query_7 = "select decode(join_yr, 2010, join_mo, 2011, join_day, 'default_term') from default"
        verify_7 = "select case when join_yr = 2010 then join_mo " \
                   "when join_yr = 2011 then join_day else 'default_term' end from default"

        queries["a"] = {"indexes": [self.primary_index_def], "queries": [query_1],
                        "asserts": [self.verifier_decode(verify_1, 0)], "server": node}
        queries["b"] = {"indexes": [self.primary_index_def], "queries": [query_2],
                        "asserts": [self.verifier_decode(verify_2, 0)], "server": node}
        queries["c"] = {"indexes": [self.primary_index_def], "queries": [query_3],
                        "asserts": [self.verifier_decode(verify_3, 0)], "server": node}
        queries["d"] = {"indexes": [self.primary_index_def], "queries": [query_4],
                        "asserts": [self.verifier_decode(verify_4, 0)], "server": node}
        queries["e"] = {"indexes": [self.primary_index_def], "queries": [query_5],
                        "asserts": [self.verifier_decode(verify_5, 0)], "server": node}
        queries["f"] = {"indexes": [self.primary_index_def], "queries": [query_6],
                        "asserts": [self.verifier_decode(verify_6, 0)], "server": node}
        queries["g"] = {"indexes": [self.primary_index_def], "queries": [query_7],
                        "asserts": [self.verifier_decode(verify_7, 0)], "server": node}

        try:
            self.query_runner(queries)
        except AssertionError:
            fails_found = 1

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 0:
                error = "Unexpected success for old version!"

        return error

    def check_window_functions(self, mode, node):
        error = ""
        fails_found = 0
        error_message = ""
        query = "select first_value(decimal_field) over (partition by char_field " \
                "order by decimal_field) as fv, decimal_field as df, char_field as cf from wf_bucket"
        try:
            result = self.run_cbq_query(query=query, server=node)

            test_dict = dict()
            for alpha in self.alphabet:
                test_result = self.run_cbq_query(query=query, server=node)
                if 'decimal_field' not in test_result['results'][0]:
                    val = None
                else:
                    val = test_result['results'][0]['decimal_field']
                test_dict[alpha] = val

            for res in result['results']:
                ch = res['cf']
                db_val = None
                if 'fv' in res:
                    db_val = res['fv']

                if db_val != test_dict[ch]:
                    self.assertEqual('True', 'False',
                                      "Values are: db - "+str(db_val)+", test - "+str(test_dict[ch])+", "
                                      "letter - "+str(ch))
        except CBQError as e:
            fails_found = 1
            error_message = str(e)
        except AssertionError as e1:
            fails_found = 1
        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 1:
                if "Invalid function first_value." not in error_message:
                    error = "Unexpected error message"
            else:
                error = "Unexpected success for old version!"

        return error

    def check_build_index_expressions(self, mode, node):
        indexes = ["ixx1", "ixx2", "ixx3"]
        for index in indexes:
            self.run_cbq_query("CREATE index %s on %s(join_yr) WITH {'defer_build':true}" % (index, "default"))
        wait_for_index_list = [("default", index, 'deferred') for index in indexes]
        self.wait_for_index_status_bulk(wait_for_index_list)

        error = ""
        fails_found = 0
        initial_num_indexes_online = self.get_index_count('default', 'online')
        error_message = ""

        try:
            results = self.run_cbq_query(query="BUILD index on default("
                                               "(SELECT RAW name FROM system:indexes WHERE keyspace_id = 'default' "
                                               "AND state = 'deferred' and name = 'ixx1'))", server=node)
            build_index_status = results['status']
            self.assertEqual(build_index_status, 'success')
            for index in ['ixx1']:
                self.wait_for_index_status("default", index, "online")
            num_indexes_online = self.get_index_count('default', 'online')
            self.assertEqual(num_indexes_online, initial_num_indexes_online + 1)
            results = self.run_cbq_query(query="BUILD index on default((SELECT RAW name FROM system:indexes "
                                               "WHERE keyspace_id = 'default' "
                                               "AND state = 'deferred' and name in ['ixx2']))", server=node)
            build_index_status = results['status']
            self.assertEqual(build_index_status, 'success')
            for index in ['ixx2']:
                self.wait_for_index_status("default", index, "online")
            num_indexes_online = self.get_index_count('default', 'online')
            self.assertEqual(num_indexes_online, initial_num_indexes_online + 2)
            results = self.run_cbq_query(query="BUILD index on default((select raw 'ixx3' from default limit 10))",
                                         server=node)
            build_index_status = results['status']
            self.assertEqual(build_index_status, 'success')
            for index in ['ixx3']:
                self.wait_for_index_status("default", index, "online")
            num_indexes_online = self.get_index_count('default', 'online')
            self.assertEqual(num_indexes_online, initial_num_indexes_online + 3)


        except CBQError as e:
            fails_found = 1
            error_message = str(e)
        except Exception as e1:
            fails_found = 1
        finally:
            for index in indexes:
                self.n1ql_callable.drop_gsi_index(name=index, keyspace="default")

        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 1:
                if "syntax error - at (" not in error_message:
                    error = "Unexpected error message"
            else:
                error = "Unexpected success for old version!"
        return error

    def check_n1ql_fts_search_query(self, mode, node):
        error = ""
        fails_found = 0
        error_message = ""

        test_fts_query = "select primary_key from `test_bucket` where meta().id in " \
                         "(select raw ht.id from (SELECT result.hits FROM " \
                         "SEARCH_QUERY('idx_test_bucket_fts', " \
                         "{ 'explain' : FALSE, " \
                         "'fields' : [ 'string_field' ], " \
                         "'highlight' : {}, " \
                         "'query' : { 'query' : 'string_field:test_string' }, " \
                         "'size':100}) AS result) " \
                         "aa unnest aa.hits as ht) " \
                         "order by primary_key"

        test_n1ql_query = "select primary_key from test_bucket " \
                          "where string_field like 'test_string%' order by primary_key"
        try:
            fts_result = self.run_cbq_query(query=test_fts_query, server=node)
            n1ql_result = self.run_cbq_query(query=test_n1ql_query, server=node)
            self.assertEqual(fts_result == n1ql_result, True)
        except CBQError as e:
            fails_found = 1
            error_message = str(e)
        if mode == self.post_upgrade_mode:
            if fails_found == 1:
                error = "Unexpected fail for upgraded version!"
        else:
            if fails_found == 1:
                if "Invalid function SEARCH_QUERY" not in error_message:
                    error = "Unexpected error message"
            else:
                error = "Unexpected success for old version!"

        return error

# ################################## GSI INDEXES ####################

    def _create_indexes(self):
        self.n1ql_callable.create_gsi_index(keyspace="default", name="def_primary", is_primary=True)
        self.n1ql_callable.create_gsi_index(keyspace="standard_bucket0", name="def_primary_standard_bucket0",
                                            is_primary=True)
        self.n1ql_callable.create_gsi_index(keyspace="wf_bucket", name="ix_char", fields="char_field")
        self.n1ql_callable.create_gsi_index(keyspace="wf_bucket", name="ix_decimal", fields="decimal_field")
        self.n1ql_callable.create_gsi_index(keyspace="wf_bucket", name="ix_int", fields="int_field")
        self.n1ql_callable.create_gsi_index(keyspace="wf_bucket", name="ix_primary", is_primary=True)
        self.n1ql_callable.create_gsi_index(keyspace="temp_bucket", name="#primary", is_primary=True)
        self.n1ql_callable.create_gsi_index(keyspace="temp_bucket", name="ix1", fields="int_field")
        self.n1ql_callable.create_gsi_index(keyspace="aggs_bucket", name="#primary", is_primary=True)
        self.n1ql_callable.create_gsi_index(keyspace="aggs_bucket", name="ix1", fields="int_field")
        self.n1ql_callable.create_gsi_index(keyspace="aggs_bucket", name="ix3", fields="float_field")
        self.n1ql_callable.create_gsi_index(keyspace="test_bucket", name="#primary", is_primary=True)
        self.n1ql_callable.create_gsi_index(keyspace="beer-sample", name="beer_sample_code_idx", fields="code")
        self.n1ql_callable.create_gsi_index(keyspace="beer-sample", name="beer_sample_brewery_id_idx",
                                            fields="brewery_id")

# ################################## FTS INDEXES ####################

    def _remove_all_fts_indexes(self):
        indexes = self.cbcluster.get_indexes()
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        for index in indexes:
            rest.delete_fts_index(index.name)

    def _create_fts_indexes(self):
        idx1 = self.create_fts_index(index_name="idx_test_bucket_fts", doc_count=self.test_bucket_size)
        self.fts_indexes['index1'] = idx1

    def create_fts_index(self, index_name='idx_test_bucket_fts', doc_count=0, source_name='test_bucket'):
        cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_index = cbcluster.create_fts_index(name=index_name, source_name=source_name)

        indexed_doc_count = 0
        while indexed_doc_count < doc_count:
            try:
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError:
                continue

        return fts_index

    def _create_alias(self, target_index, name):
        cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        alias_def = {"targets": {}}
        alias_def['targets'][target_index.name] = {}
        return cbcluster.create_fts_index(name=name, index_type='fulltext-alias', index_params=alias_def)


# ################################## UTILS ###########################

    def get_rest_client(self, user, password):
        rest = RestConnection(self.cbcluster.get_random_fts_node())
        rest.username = user
        rest.password = password
        return rest

    def _filter_digit_params(self, params):
        ret_val = list(filter(lambda x: isinstance(x, int) or isinstance(x, float), params))
        for i in range(len(ret_val)):
            ret_val[i] = float(ret_val[i])

        return ret_val

    def _calculate_median_value(self, params):
        filtered_params = self._filter_digit_params(params)
        sorted_params = sorted(filtered_params)
        ret_val = 0

        if len(sorted_params) == 0:
            return ret_val

        if len(sorted_params) == 1:
            return sorted_params[0]

        if len(sorted_params) == 2:
            return (sorted_params[0] + sorted_params[1]) / 2

        if len(sorted_params) % 2 != 0:
            ret_val = sorted_params[len(sorted_params) / 2]
        else:
            val_left = sorted_params[len(sorted_params) / 2 - 1]
            val_right = sorted_params[len(sorted_params) / 2]
            ret_val = (val_left + val_right) / 2

        return ret_val

# ################################## DATA LOAD ######################

    def _load_test_data(self):
        for i1 in range(len(self.test_data["int_field"])):
            for i2 in range(len(self.test_data["bool_field"])):
                for i3 in range(len(self.test_data["varchar_field"])):
                    query = "insert into temp_bucket values ('key_"+str(i1)+"_"+str(i2)+"_"+str(i3)+"', {"
                    int_field_insert = "'int_field': "+self.test_data["int_field"][i1]+","
                    bool_field_insert = "'bool_field': "+self.test_data["bool_field"][i2]+","
                    varchar_field_insert = "'varchar_field': '"+self.test_data["varchar_field"][i3]+"'"

                    query += " "+int_field_insert+bool_field_insert+varchar_field_insert+"})"
                    self.run_cbq_query(query)

        for i in range(len(self.numbers)):
            int_val = self.numbers[i]
            float_val = self.numbers[i]*math.pi
            varchar_val = "string"+str(self.numbers[i])
            bool_val = True

            if i % 2 == 0:
                int_val = 'NULL'
                bool_val = False
            if i % 3 == 0:
                float_val = 'NULL'
            if i % 5 == 0:
                varchar_val = 'NULL'
            if i % 11 == 0:
                int_val = ''
            if i % 13 == 0:
                float_val = ''
            if i % 17 == 0:
                varchar_val = ''

            query = "insert into temp_bucket values ('key_" + str(i) + "', {"

            int_field_insert = "'int_field': " + str(int_val) + ","
            if int_val == '':
                int_field_insert = ''

            float_field_insert = "'float_field': " + str(float_val) + ","
            if float_val == '':
                float_field_insert = ''

            varchar_field_insert = "'varchar_field': '"+varchar_val+"',"
            if varchar_val == '':
                varchar_field_insert = ''

            bool_field_insert = "'bool_field': "+str(bool_val)
            if bool_val == '':
                bool_field_insert = ''

            query += " " + int_field_insert + float_field_insert + varchar_field_insert + bool_field_insert + "})"
            self.run_cbq_query(query)

        for i1 in range(len(self.numbers)):
            query = "insert into aggs_bucket values ('key_" + str(i1) + "', {"
            int_field_insert = "'int_field': " + str(self.numbers[i1]) + ","
            float_field_insert = "'float_field': " + str((self.numbers[i1]*math.pi))

            query += " " + int_field_insert + float_field_insert + "})"
            self.run_cbq_query(query)

        counter = 0
        query = "insert into temp_bucket values('plain',{"
        for key, value in self.scalars_test_data.iteritems():
            if key == 'missing':
                continue
            query += "'" + value['key'] + "':"
            if key != 'null':
                query += "'"
            query += value['value']
            if key != 'null':
                query += "'"
            if counter < len(self.scalars_test_data) - 2:
                query += ","
            counter += 1

        query += "})"
        self.run_cbq_query(query)

        for i in range(0, 999, 1):
            if i % 100 == 0:
                initial_statement = (" INSERT INTO test_bucket (KEY, VALUE) VALUES ('primary_key_"+str(i)+"',")
                initial_statement += "{"
                initial_statement += "'primary_key':'primary_key_"+str(i) + "','string_field': 'test_string " \
                                     + str(i) + "','int_field':"+str(i)+"})"
            else:
                initial_statement = (" INSERT INTO test_bucket (KEY, VALUE) VALUES ('primary_key_"+str(i)+"',")
                initial_statement += "{"
                initial_statement += "'primary_key':'primary_key_"+str(i) + "','string_field': 'string data " \
                                     + str(i) + "','int_field':"+str(i)+"})"
            self.run_cbq_query(initial_statement)

        shell = RemoteMachineShellConnection(self.master)
        shell.execute_cbworkloadgen(self.username, self.password, 15000, 100, "big_bucket", 1024, '-j')

        for i in range(0, 999, 1):
            initial_statement = (" INSERT INTO wf_bucket (KEY, VALUE) VALUES ('primary_key_"+str(i)+"',")
            initial_statement += "{"
            initial_statement += "'primary_key':'primary_key_"+str(i) + "','char_field':'" \
                                 + random.choice(string.ascii_uppercase) + \
                                 "','decimal_field':"+str(round(10000*random.random(), 0))\
                                 + ",'int_field':"+str(randint(0, 100000000))+"})"
            self.run_cbq_query(initial_statement)

    # ###################### VERIFIERS ##############################

    def skip_key_composite_index_verifier(self, compare_query, query_index):
        return lambda x: self.skip_key_composite_index_compare_queries(x['q_res'][query_index], compare_query)

    def skip_key_composite_index_compare_queries(self, actual_results=None, compare_query=None, node=None):
        results = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query, server=node)
        compare_docs = compare_results['results']
        try:
            self.assertEqual(len(results), len(compare_docs))
            self.assertEqual(sorted(results), sorted(compare_docs))
            return True, ""
        except AssertionError as e:
            return False, str(e)

    def skip_key_composite_index_plan_verifier(self, check_type=None, check_data=None):
        return lambda x: self.skip_key_composite_index_check_plan(x, check_type, check_data)

    def skip_key_composite_index_check_plan(self, explain_results=None, check_type=None, check_data=None):
        try:
            plan = self.ExplainPlanHelper(explain_results)
            if check_type == "spans":
                spans = plan["~children"][0]['spans'][0]['range']
                self.assertEqual(spans, check_data)
            if check_type == "deep_spans":
                spans = plan["~children"][0]["~children"][0]['spans'][0]['range']
                self.assertEqual(spans, check_data)
            if check_type == "index":
                index = plan["~children"][0]['index']
                self.assertEqual(index, check_data)
            if check_type == "join_index":
                index = plan["~children"][2]['~child']['~children'][0]['~child']['~children'][0]['index']
                self.assertEqual(index, check_data)
            if check_type == "join_span":
                spans = plan["~children"][2]['~child']['~children'][0]['~child']['~children'][0]['spans'][0]['range']
                self.assertEqual(spans, check_data)
            if check_type == "deep_index":
                index = plan["~children"][0]["~children"][0]['index']
                self.assertEqual(index, check_data)
            if check_type == "pushdown":
                self.assertTrue("index_group_aggs" in str(plan))
            return True, ""
        except AssertionError as e:
            return False, str(e)

    def _check_order_nulls_first_desc(self, result, datatype):
        non_null_results = []

        if len(result['results']) == 0:
            return False
        cur_result = result['results'][0]
        for i in range(1, len(result['results'])):
            if str(cur_result) == '{}':
                if str(result['results'][i]) != '{}' and result['results'][i][datatype + '_field'] is None:
                    return False
                else:
                    cur_result = result['results'][i]
                    continue
            elif cur_result[datatype + '_field'] is None:
                cur_result = result['results'][i]
                continue
            else:
                if str(result['results'][i]) == '{}' or result['results'][i][datatype + '_field'] is None:
                    return False
                else:
                    non_null_results.append(cur_result[datatype + '_field'])
                    cur_result = result['results'][i]

        sorted_results = copy.copy(non_null_results)
        sorted_results.sort(reverse=True)
        for i in range(len(non_null_results)):
            if non_null_results[i] != sorted_results[i]:
                return False

        return True

    def verifier_chained_let(self, compare_query):
        return lambda x: self.compare_queries_chained_let(x['q_res'][0], compare_query)

    def compare_queries_chained_let(self, actual_results, compare_query):
        let_letting_docs = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query)
        compare_docs = compare_results['results']
        self.assertEqual(len(let_letting_docs), len(compare_docs))
        self.assertEqual(let_letting_docs, compare_docs)

    def verifier_decode(self, compare_query, query_index):
        return lambda x: self.compare_queries_decode(x['q_res'][query_index], compare_query)

    def compare_queries_decode(self, actual_results, compare_results):
        results = actual_results['results']
        compare_docs = compare_results['results']
        self.assertEqual(len(results), len(compare_docs))
        self.log.info('comparing results...')
        for i in range(0, len(results)):
            self.assertEqual(results[i], compare_docs[i])
        self.log.info('results match')

    # ##################################### INIT NODES ####################

    def _init_nodes(self):
        test_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("test_bucket", 11222, test_bucket_params)

        test_bucket_params = self._create_bucket_params(server=self.master, size=256,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("wf_bucket", 11222, test_bucket_params)

        temp_bucket_params = self._create_bucket_params(server=self.master, size=256,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("aggs_bucket", 11222, temp_bucket_params)

        temp_bucket_params = self._create_bucket_params(server=self.master, size=256,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("temp_bucket", 11222, temp_bucket_params)

        if self.get_bucket_from_name("beer-sample") is None:
            self.rest = RestConnection(self.servers[0])
            self.rest.load_sample("beer-sample")
            self.wait_for_buckets_status({"beer-sample": "healthy"}, 5, 120)
            self.wait_for_bucket_docs({"beer-sample": 7303}, 5, 120)
