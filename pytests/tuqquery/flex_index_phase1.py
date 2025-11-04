from .tuq import QueryTests
from pytests.fts.random_query_generator.rand_query_gen import DATASET
from collections.abc import Mapping, Sequence, Set
from collections import deque
from deepdiff import DeepDiff
import json
import threading
from pytests.fts.fts_base import CouchbaseCluster
from membase.api.exception import CBQError

class FlexIndexTests(QueryTests):

    users = {}

    def suite_setUp(self):
        super(FlexIndexTests, self).suite_setUp()

    def init_flex_object(self, test_object):
        self.log = test_object.log
        self.master = test_object.master
        self.input = test_object.input
        self.buckets = test_object.buckets
        self.testrunner_client = test_object.testrunner_client
        self.use_rest = test_object.use_rest
        self.scan_consistency = test_object.scan_consistency
        self.hint_index = test_object.hint_index
        self.n1ql_port = test_object.n1ql_port
        self.analytics = test_object.analytics
        self.named_prepare = test_object.named_prepare
        self.servers = test_object.servers

    def setUp(self):
        super(FlexIndexTests, self).setUp()
        #self._load_test_buckets()
        self.log.info("==============  FlexIndexTests setuAp has started ==============")
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql")
        if self.query_node:
            self.log_config_info(self.query_node)
        self.dataset = self.input.param("flex_dataset", "emp")
        self.use_index_name_in_query = bool(self.input.param("use_index_name_in_query", True))
        self.expected_gsi_index_map = {}
        self.expected_fts_index_map = {}
        self.gsi_fields = []
        self.custom_map = self.input.param("custom_map", False)
        self.bucket_name = self.input.param("bucket_name", 'default')
        self.flex_query_option = self.input.param("flex_query_option", "flex_use_fts_query")
        self.rebalance_in = self.input.param("rebalance_in", False)
        self.failover_fts = self.input.param("failover_fts", False)
        self.use_fts_query_param = self.input.param("use_fts_query_param", None)
        self.log.info("==============  FlexIndexTests setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  FlexIndexTests tearDown has started ==============")
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql")
        if self.query_node:
            self.log_config_info(self.query_node)
        self.log.info("==============  FlexIndexTests tearDown has completed ==============")
        super(FlexIndexTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  FlexIndexTests suite_tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  FlexIndexTests suite_tearDown has completed ==============")
        super(FlexIndexTests, self).suite_tearDown()

# ============================ # Utils ===========================================

    def compare_results_with_gsi(self, flex_query, gsi_query, use_fts_query_param=None):
        try:
            flex_result = self.run_cbq_query(flex_query, query_params={}, use_fts_query_param=use_fts_query_param,
                                             server=self.query_node)["results"]
            self.log.info("Number of results from flex query: {0} is {1}".format(flex_query, len(flex_result)))
        except Exception as e:
            self.log.info("Failed to run flex query: {0}".format(flex_query))
            self.log.error(e)
            return False

        try:
            gsi_result = self.run_cbq_query(gsi_query, query_params={}, use_fts_query_param=None,
                                            server=self.query_node)["results"]
            self.log.info("Number of results from gsi query: {0} is {1}".format(gsi_query, len(gsi_result)))
        except Exception as e:
            self.log.info("Failed to run gsi query: {0}".format(gsi_query))
            self.log.error(e)
            return False

        if len(flex_result) != len(gsi_result):
            self.log.info("Number of results not matching b/w flex and GSI results")
            return False

        diffs = DeepDiff(flex_result, gsi_result, ignore_order=True)

        if diffs:
            self.log.info(diffs)
            self.log.info("There are differences in the results b/w flex and GSI results")
            return False

        return True

    def get_gsi_fields_partial_sargability(self):
        fts_fields = self.query_gen.fields
        available_fields = DATASET.CONSOLIDATED_FIELDS
        for field in available_fields:
            field_found = False
            for k, v in fts_fields.items():
                if field in v:
                    field_found = True
                    break
            if not field_found:
                self.gsi_fields.append(field)
        self.gsi_fields = list(set(self.gsi_fields))

    def create_gsi_indexes(self):
        count = 0
        self.expected_gsi_index_map = {}
        for field in self.gsi_fields:
            field = self.query_gen.replace_underscores(field)
            field_proxy = field
            #handling array fields
            if field == "languages_known":
                field = "ALL ARRAY v for v in languages_known END"
            if field == "manages.reports":
                field = "ALL ARRAY v for v in manages.reports END"
            if field_proxy not in self.expected_gsi_index_map.keys() and field_proxy is not "type":
                gsi_index_name = "gsi_index_" + str(count)
                self.run_cbq_query("create index {0} on `{2}`({1})".format(gsi_index_name, field, self.bucket_name))
                self.expected_gsi_index_map[field_proxy] = [gsi_index_name]
                count += 1
        self.log.info("expected_gsi_index_map {0}".format(self.expected_gsi_index_map))

    def check_if_predicate_has_gsi_field(self, query):
        for field in self.gsi_fields:
            field = self.query_gen.replace_underscores(field)
            if field in query:
                return True
        return False

    def get_all_indexnames_from_response(self, response_json):
        queue = deque([response_json])
        index_names = []
        while queue:
            node = queue.popleft()
            nodevalue = node
            if type(node) is tuple:
                nodekey = node[0]
                nodevalue = node[1]

            if isinstance(nodevalue, Mapping):
                for k, v in nodevalue.items():
                    queue.extend([(k, v)])
            elif isinstance(nodevalue, (Sequence, Set)) and not isinstance(nodevalue, str):
                queue.extend(nodevalue)
            else:
                if nodekey == "index" and nodevalue not in index_names:
                    index_names.append(nodevalue)
        return index_names

    def check_if_expected_index_exist(self, result, expected_indexes):
        actual_indexes = self.get_all_indexnames_from_response(result)
        found = True
        self.log.info("Actual indexes present: {0}, Expected Indexes: {1}".format(sorted(actual_indexes), sorted(expected_indexes)))
        for index in actual_indexes:
            if index not in expected_indexes:
                found = False
        return found

    def get_expected_indexes(self, flex_query, expected_index_map):
        available_fields = DATASET.CONSOLIDATED_FIELDS
        expected_indexes = []
        for field in available_fields:
            field = self.query_gen.replace_underscores(field)
            if " {0}".format(field) in flex_query and field in expected_index_map.keys():
                for index in expected_index_map[field]:
                    expected_indexes.append(index)

        return list(set(expected_indexes))

    def run_query_and_validate(self, query_list):
        failed_to_run_query = []
        not_found_index_in_response = []
        result_mismatch = []
        iteration = 1
        if not hasattr(self, "query_node"):
            self.query_node = self.get_nodes_from_services_map(service_type="n1ql")

        for query in query_list:
            query_num = iteration
            iteration += 1
            self.log.info("======== Running Query # {0} =======".format(query_num))
            flex_query = query.format("USE INDEX (USING FTS, USING GSI)")
            gsi_query = query.format("")
            explain_query = "explain " + flex_query
            self.log.info("Query : {0}".format(explain_query))
            try:
                result = self.run_cbq_query(explain_query, server=self.query_node)
            except Exception as e:
                self.log.info("Failed to run query")
                self.log.error(e)
                failed_to_run_query.append(query_num)
                continue
            try:
                self.assertTrue(self.check_if_expected_index_exist(result, [self.fts_index.name]))
            except Exception as e:
                self.log.info("Failed to find fts index name in plan query")
                self.log.error(e)
                not_found_index_in_response.append(query_num)
                continue

            if not self.compare_results_with_gsi(flex_query, gsi_query):
                self.log.error("Result mismatch found")
                result_mismatch.append(query_num)

            self.log.info("======== Done =======")

        return failed_to_run_query, not_found_index_in_response, result_mismatch


    def run_queries_and_validate(self, partial_sargability=None):
        iteration = 1
        failed_to_run_query = []
        not_found_index_in_response = []
        result_mismatch = []
        for flex_query_ph, gsi_query in zip(self.query_gen.fts_flex_queries, self.query_gen.gsi_queries):
            query_num = iteration
            iteration += 1
            self.log.info("======== Running Query # {0} =======".format(query_num))
            expected_fts_index = []
            expected_gsi_index = []
            if self.flex_query_option != "flex_use_gsi_query":
                expected_fts_index = self.get_expected_indexes(flex_query_ph, self.expected_fts_index_map)
            expected_gsi_index = self.get_expected_indexes(flex_query_ph, self.expected_gsi_index_map)
            if self.use_fts_query_param:
                flex_query = gsi_query
            else:
                flex_query = self.get_runnable_flex_query(flex_query_ph, expected_fts_index, expected_gsi_index)
            if self.flex_query_option == "flex_use_gsi_query":
                expected_gsi_index.append("primary_gsi_index")
            # issue MB-39493
            if (partial_sargability and self.flex_query_option == "flex_use_fts_query" and flex_query.count("OR") > 1) \
                    or (self.check_if_predicate_has_gsi_field(flex_query) and
                        self.flex_query_option == "flex_use_fts_query" and flex_query.count("OR") >= 1):
               expected_gsi_index.append("primary_gsi_index")
            explain_query = "explain " + flex_query
            self.log.info("Query : {0}".format(explain_query))
            try:
                result = self.run_cbq_query(explain_query, query_params={}, use_fts_query_param=self.use_fts_query_param)
            except Exception as e:
                self.log.info("Failed to run query")
                self.log.error(e)
                failed_to_run_query.append(query_num)
                continue
            try:
                self.assertTrue(self.check_if_expected_index_exist(result, expected_fts_index + expected_gsi_index))
            except Exception as e:
                self.log.info("Failed to find fts index name in plan query")
                self.log.error(e)
                not_found_index_in_response.append(query_num)
                continue

            if not self.compare_results_with_gsi(flex_query, gsi_query, self.use_fts_query_param):
                self.log.error("Result mismatch found")
                result_mismatch.append(query_num)

            self.log.info("======== Done =======")

        return failed_to_run_query, not_found_index_in_response, result_mismatch

    def merge_smart_fields(self, smart_fields1, smart_fields2):
        combined_fields = {}
        for key in smart_fields1.keys():
            if key in smart_fields2.keys():
                combined_fields[key] = list(set(smart_fields1[key] + smart_fields2[key]))
            else:
                combined_fields[key] = smart_fields1[key]

        for key in smart_fields2.keys():
            if key not in smart_fields1.keys():
                combined_fields[key] = smart_fields2[key]
        return combined_fields

    def get_runnable_flex_query(self, flex_query_ph, expected_fts_index, expected_gsi_index):
        use_fts_hint = "USING FTS"
        use_gsi_hint = "USING GSI"
        final_hint = ""
        if self.flex_query_option == "flex_use_fts_query" or self.flex_query_option == "flex_use_fts_gsi_query":
            if self.use_index_name_in_query:
                for index in expected_fts_index:
                    if final_hint == "":
                        final_hint = "{0} {1}". format(index, use_fts_hint)
                    else:
                        final_hint = "{0}, {1} {2}".format(final_hint, index, use_fts_hint)
            else:
                final_hint = use_fts_hint

        if self.flex_query_option == "flex_use_gsi_query" or self.flex_query_option == "flex_use_fts_gsi_query":
            if self.use_index_name_in_query and expected_gsi_index:
                for index in expected_gsi_index:
                    if final_hint == "":
                        final_hint = "{0} {1}". format(index, use_gsi_hint)
                    else:
                        final_hint = "{0}, {1} {2}".format(final_hint, index, use_gsi_hint)
            elif final_hint is not "":
                final_hint = "{0}, {1}".format(final_hint, use_gsi_hint)
            else:
                final_hint = use_gsi_hint

        flex_query = flex_query_ph.format(flex_hint=final_hint)

        return flex_query

    def run_queries_and_validate_clusterops(self):
        failed_to_run_query, not_found_index_in_response,result_mismatch = self.run_queries_and_validate()

        if failed_to_run_query or not_found_index_in_response:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      .format(failed_to_run_query, not_found_index_in_response))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))


# ======================== tests =====================================================

    def test_flex_single_typemapping(self):

        self._load_emp_dataset(end=self.num_items)

        fts_index = self.create_fts_index(
            name="custom_index", source_name=self.bucket_name)
        self.generate_random_queries(fts_index.smart_query_fields)
        self.update_expected_fts_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")

        self.wait_for_fts_indexing_complete(fts_index, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_multi_typemapping(self):

        self._load_emp_dataset(end=(self.num_items/2))
        self._load_wiki_dataset(end=(self.num_items/2))

        fts_index = self.create_fts_index(
            name="custom_index", source_name=self.bucket_name)
        # make sure mutated is indexed in both the type mappings to test the bug: MB-39517
        mutated_field_def = {
            'dynamic': False,
            'enabled': True,
            'properties': {},
            'fields': [
                {
                    'include_in_all': True,
                    'include_term_vectors': False,
                    'index': True,
                    'name': 'mutated',
                    'store': False,
                    'type': 'number',
                    'analyzer': ''
                }
            ]
        }
        fts_index.index_definition['params']['mapping']['types']['emp']['properties']['mutated'] = mutated_field_def
        fts_index.index_definition['params']['mapping']['types']['wiki']['properties']['mutated'] = mutated_field_def
        fts_index.index_definition['uuid'] = fts_index.get_uuid()
        fts_index.update()
        self.generate_random_queries(fts_index.smart_query_fields)
        self.update_expected_fts_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")

        self.wait_for_fts_indexing_complete(fts_index, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate(partial_sargability = True)
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_default_typemapping(self):

        self._load_emp_dataset(end=self.num_items/2)
        self._load_wiki_dataset(end=(self.num_items/2))

        fts_index = self.create_fts_index(
            name="default_index", source_name=self.bucket_name)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries()
        fts_index.smart_query_fields = self.query_gen.fields
        self.update_expected_fts_index_map(fts_index)

        self.wait_for_fts_indexing_complete(fts_index, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))


    def test_flex_single_typemapping_partial_sargability(self):

        self._load_emp_dataset(end=self.num_items)

        fts_index = self.create_fts_index(
            name="custom_index", source_name=self.bucket_name)
        self.generate_random_queries(fts_index.smart_query_fields)
        self.update_expected_fts_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.get_gsi_fields_partial_sargability()
        self.create_gsi_indexes()
        self.generate_random_queries()

        self.wait_for_fts_indexing_complete(fts_index, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate(partial_sargability = True)
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_multi_typemapping_partial_sargability(self):

        self._load_emp_dataset(end=(self.num_items/2))
        self._load_wiki_dataset(end=(self.num_items/2))

        fts_index = self.create_fts_index(
            name="custom_index")
        self.update_expected_fts_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries(fts_index.smart_query_fields)
        self.get_gsi_fields_partial_sargability()
        self.create_gsi_indexes()
        self.generate_random_queries()

        self.wait_for_fts_indexing_complete(fts_index, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate(partial_sargability = True)
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_single_typemapping_2_fts_indexes(self):
        self._load_emp_dataset(end=self.num_items)

        fts_index_1 = self.create_fts_index(
            name="custom_index_1", source_name=self.bucket_name)
        fts_index_2 = self.create_fts_index(
            name="custom_index_2", source_name=self.bucket_name)
        self.log.info("Editing custom index with new map...")
        fts_index_2.generate_new_custom_map(seed=fts_index_2.cm_id+10)
        fts_index_2.index_definition['uuid'] = fts_index_2.get_uuid()
        fts_index_2.update()
        smart_fields = self.merge_smart_fields(fts_index_1.smart_query_fields, fts_index_2.smart_query_fields)
        self.generate_random_queries(smart_fields)

        self.update_expected_fts_index_map(fts_index_1)
        self.update_expected_fts_index_map(fts_index_2)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")

        self.get_gsi_fields_partial_sargability()
        self.create_gsi_indexes()
        self.generate_random_queries()

        self.wait_for_fts_indexing_complete(fts_index_1, self.num_items)
        self.wait_for_fts_indexing_complete(fts_index_2, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_with_napa_dataset(self):

        self._load_napa_dataset(end=self.num_items)

        self.fts_index = self.create_fts_index(
            name="default_index", source_name=self.bucket_name, doc_count=self.num_items)
        if not self.is_index_present("default", "primary_gsi_index", server=self.query_node):
            self.run_cbq_query("create primary index primary_gsi_index on default", server=self.query_node)

        query_list = ['SELECT META().id FROM `default` {0} WHERE address.ecpdId="1" ORDER BY META().id LIMIT 100',
                      'SELECT META().id, email FROM `default` {0} WHERE address.ecpdId="3" OR address.applicationId=300'
                      ' ORDER BY email,META().id LIMIT 10',
                      'SELECT META().id, address.applicationId FROM `default` {0} WHERE address.ecpdId="3" AND'
                      ' address.deviceTypeId=9 ORDER BY address.applicationId,META().id LIMIT 100',
                      'SELECT META().id FROM `default` {0} WHERE (address.ecpdId="3" OR address.applicationId=300) AND '
                      '(address.deviceTypeId=9 OR address.deviceStatus=0) ORDER BY META().id LIMIT 100',
                      'SELECT META().id, address.applicationId FROM `default` {0} WHERE address.applicationId = 1 AND '
                      'address.deviceTypeId=9 AND address.deviceStatus=0 ORDER BY address.applicationId LIMIT 100',
                      'SELECT META().id FROM `default` {0} WHERE address.deviceTypeId=9 AND address.deviceStatus=0'
                      ' ORDER BY address.applicationId,META().id LIMIT 100',
                      'SELECT META().id FROM `default` {0} WHERE address.deviceStatus=0 ORDER BY META().id LIMIT 100',
                      'SELECT META().id, address.activationDate FROM `default` {0} WHERE address.ecpdId="1" ORDER BY'
                      ' address.activationDate,META().id LIMIT 100',
                      'SELECT META().id FROM default {0} WHERE ( ( ( ( ( email LIKE "A%") OR '
                      '( ANY v IN devices SATISFIES v LIKE "2%" END)) OR '
                      '( first_name > "Karianne" AND first_name <= "Qarianne")) OR ( routing_number = 12160)) OR '
                      '( address.activationDate BETWEEN "1995-10-10T21:22:00" AND "2020-05-09T20:08:02.462692")) '
                      'ORDER BY address.city,META().id LIMIT 100',
                      'SELECT META().id, company_name FROM default {0} WHERE (( email = "Aaron.Jaskolski18@yahoo.com") '
                      'AND ( ANY v IN children SATISFIES v.first_name = "Raven" END)) OR '
                      '(( company_code > "IMWW" AND company_code <= "D3IHO") AND ( routing_number = 67473) OR '
                      '( address.activationDate BETWEEN "2019-10-10T21:22:00" AND "2020-05-09T20:08:02.462692")) '
                      'ORDER BY address.activationDate,META().id OFFSET 500 LIMIT 100',
                      'SELECT first_name, last_name, email FROM default {0} WHERE '
                      '( ( SOME v IN children SATISFIES v.first_name LIKE "R%" END) AND '
                      '( age > 20 AND age < 40)) OR '
                      '(( dob BETWEEN "1994-12-08T01:19:00" AND "2020-05-09T20:08:02.469127") AND isActive = FALSE) '
                      'OR (address.deviceTypeId > 3 AND ISNUMBER(address.deviceTypeId)) '
                      'ORDER BY address.activationDate,META().id OFFSET 500 LIMIT 100',
                      'SELECT first_name, last_name, email, address.country FROM default {0} WHERE '
                      'ANY c IN children SATISFIES c.gender = "F" AND (c.age > 5 AND c.age <15) '
                      'OR c.first_name LIKE "a%" END ORDER BY address.country,META().id OFFSET 500 LIMIT 100']

        self.wait_for_fts_indexing_complete(self.fts_index, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_query_and_validate(query_list)
        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(query_list)))

    def test_clusterops_flex_fts_node(self):
        self._load_emp_dataset(end=self.num_items/2)
        self._load_wiki_dataset(end=(self.num_items/2))

        fts_index = self.create_fts_index(
            name="default_index", source_name=self.bucket_name)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries()
        fts_index.smart_query_fields = self.query_gen.fields
        self.update_expected_fts_index_map(fts_index)
        fts_index.update_num_replicas(1)

        self.wait_for_fts_indexing_complete(fts_index, self.num_items)

        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

        thread1 = threading.Thread(name='run_query', target=self.run_queries_and_validate_clusterops)
        thread1.start()
        if self.rebalance_in:

            self.log.info("Now rebalancing in fts node while running queries parallely")

            self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")

            try:
                self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                         [self.servers[self.nodes_init]], [],services=['fts'])
            except Exception as e:
                self.fail("Rebalance in failed with {0}".format(str(e)))

        elif self.failover_fts:
            self.log.info("Now failover fts node while running queries parallely")
            try:
                self.cluster.failover(self.servers[:self.nodes_init],
                                                         [self.servers[self.nodes_init-1]])
            except Exception as e:
                self.fail("node failover failed with {0}".format(str(e)))
        else:
            self.log.info("Now rebalancing out fts node while running queries parallely")
            try:
                self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                         [], [self.servers[self.nodes_init-1]],services=['fts'])
            except Exception as e:
                self.fail("Rebalance out failed with {0}".format(str(e)))

        thread1.join()
        self.cbcluster.delete_all_fts_indexes()

    def test_rbac_flex(self):
        self._load_test_buckets(create_index=False)
        user = self.input.param("user", '')
        if user == '':
            raise Exception("Invalid test configuration! User name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_index = self.create_fts_index(name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        self.wait_for_fts_indexing_complete(fts_index, 7303)

        self._create_user(user, 'beer-sample')

        username = self.users[user]['username']
        password = self.users[user]['password']
        query = "select meta().id from `beer-sample` use index (using fts, using gsi) where state = \"California\""

        master_result = self.run_cbq_query(query=query, server=self.master, username=username, password=password)
        self.assertEquals(master_result['status'], 'success', username+" query run failed on non-fts node")

        self.cbcluster.delete_all_fts_indexes()

    def test_rbac_flex_not_granted_n1ql(self):
        self._load_test_buckets(create_index=False)
        user = self.input.param("user", '')
        if user == '':
            raise Exception("Invalid test configuration! User name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_index = self.create_fts_index(name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        self.wait_for_fts_indexing_complete(fts_index, 7303)

        self._create_user(user, 'beer-sample')

        username = self.users[user]['username']
        password = self.users[user]['password']
        query = "select meta().id from `beer-sample` use index (using fts, using gsi) where state = \"California\""

        try:
            self.run_cbq_query(query=query, server=self.master, username=username, password=password)
            self.fail("Could able to run query without n1ql permissions")
        except CBQError as e:
            self.log.info(str(e))
            if not "User does not have credentials to run SELECT queries" in str(e):
                self.fail("Failed to run query with other CBQ issues: {0}".format(str(e)))
        except Exception as e:
            self.fail("Failed to run query with other issues: {0}".format(str(e)))

        self.cbcluster.delete_all_fts_indexes()
