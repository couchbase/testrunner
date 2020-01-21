import copy
import json
import logging

from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from .base_2i import BaseSecondaryIndexingTests

DATATYPES = [str, "scalar", int, dict, "missing", "empty", "null"]
RANGE_SCAN_TEMPLATE = "SELECT {0} FROM %s WHERE {1}"
NULL_STRING = "~[]{}UnboundedTruenilNA~"

log = logging.getLogger()
emit_fields = "*"


class SecondaryIndexingAscendingDescendingCollations(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexingAscendingDescendingCollations, self).setUp()
        self.restServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.restServer)

    def tearDown(self):
        super(SecondaryIndexingAscendingDescendingCollations, self).tearDown()

    def test_three_indexes_on_different_fields_with_asc_desc_combinations(self):
        failed_scans = []
        query_definition1 = QueryDefinition(
            index_name="index_field1",
            index_fields=["name"],
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
        scan_content1 = [{"Seek": None,
                          "Filter": [{"Low": "Adara", "High": "Winta"}]}]
        query_definition2 = QueryDefinition(
            index_name="index_field2",
            index_fields=["age"],
            groups=["simple"], index_where_clause=" age IS NOT NULL ")
        scan_content2 = [{"Seek": None,
                          "Filter": [{"Low": 20, "High": 60}]}]
        query_definition3 = QueryDefinition(
            index_name="index_field3",
            index_fields=["premium_customer"],
            groups=["simple"], index_where_clause=" premium_customer IS NOT NULL ")
        scan_content3 = [{"Seek": None,
                          "Filter": [{"Low": False, "High": True}]}]
        dict = {query_definition1: scan_content1, query_definition2: scan_content2, query_definition3: scan_content3}
        desc_values = [[True], [False]]
        for bucket in self.buckets:
            for query_definition, scan_content in dict.items():
                for desc_value in desc_values:
                    id_map = self.create_index_using_rest(bucket, query_definition, desc=desc_value)
                    multiscan_content = self._update_multiscan_content(index_fields=1)
                    for inclusion in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = \
                            inclusion
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        print(multiscan_result)
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name"], scan_content, multiscan_result, desc_value, multiscan_count_result, )
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
                    self.drop_index(bucket, query_definition)
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_three_array_indexes_on_different_fields_with_asc_desc_combinations(self):
        failed_scans = []
        query_definition1 = QueryDefinition(
            index_name="index_array_field1",
            index_fields=["ALL ARRAY t FOR t in TO_ARRAY(`travel_history`) END"],
            groups=["array"], index_where_clause=" name IS NOT NULL ")
        scan_content1 = [{"Seek": None,
                          "Filter": [{"Low": "India", "High": "US"}]}]
        query_definition2 = QueryDefinition(
            index_name="index_field2",
            index_fields=["age"],
            groups=["simple"], index_where_clause=" age IS NOT NULL ")
        scan_content2 = [{"Seek": None,
                          "Filter": [{"Low": 20, "High": 60}]}]
        dict = {query_definition1: scan_content1, query_definition2: scan_content2}
        desc_values = [[True], [False]]
        for bucket in self.buckets:
            for query_definition, scan_content in dict.items():
                for desc_value in desc_values:
                    id_map = self.create_index_using_rest(bucket, query_definition, desc=desc_value)
                    multiscan_content = self._update_multiscan_content(index_fields=1)
                    for inclusion in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = \
                            inclusion
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        print(multiscan_result)
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name"], scan_content, multiscan_result, desc_value, multiscan_count_result, )
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
                    self.drop_index(bucket, query_definition)
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_composite_with_asc_desc_combinations(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="three_field_composite_name_age",
            index_fields=["name", "age", "premium_customer"],
            groups=["three_field_index"], index_where_clause=" name IS NOT NULL ")
        # Basic Scan
        scan_content = [{"Seek": None,
                         "Filter": [{"Low": "Adara", "High": "Winta"},
                                    {"Low": 20, "High": 60},
                                    {"Low": False, "High": True}]}]
        desc_values = [[False, False, False], [False, False, True], [False, True, False], [True, False, False],
                       [True, True, False], [True, False, True], [False, True, True], [True, True, True]]
        for bucket in self.buckets:
            for desc_value in desc_values:
                id_map = self.create_index_using_rest(bucket, query_definition, desc=desc_value)
                multiscan_content = self._update_multiscan_content(index_fields=3)
                for name_inclusion in range(4):
                    for age_inclusion in range(4):
                        for cust_inclustion in range(4):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            scan_content[0]["Filter"][2]["Inclusion"] = \
                                cust_inclustion
                            multiscan_content["scans"] = json.dumps(scan_content)
                            multiscan_result = \
                                self.rest.multiscan_for_gsi_index_with_rest(
                                    id_map["id"], json.dumps(multiscan_content))
                            multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                            log.info("Collation order : {0}".format(desc_value))
                            check = self._verify_items_indexed_for_two_field_index(
                                bucket, id_map["id"],
                                ["name"], scan_content, multiscan_result, desc_value, multiscan_count_result, )
                            if not check:
                                failed_scans.append(copy.deepcopy(scan_content))
                self.drop_index(bucket, query_definition)
            msg = "Failed Scans: {0}".format(failed_scans)
            self.assertEqual(len(failed_scans), 0, msg)

    def _verify_items_indexed_for_two_field_index(self, bucket, index_id, index_fields,
                                                  scan_content, multiscan_result, desc, multiscan_count_result=None):
        err_message = "There are more ranges than number"
        if isinstance(multiscan_result, dict):
            if err_message in list(multiscan_result.values()):
                multiscan_result = ''
        expected_results = []
        body = {"stale": "False"}
        for content in scan_content:
            doc_list = self.rest.full_table_scan_gsi_index_with_rest(index_id, body)
            seek = content.get("Seek", None)
            filters = content.get("Filter", None)
            if seek:
                temp_doc_list = copy.deepcopy(doc_list)
                for doc in temp_doc_list:
                    if doc["key"] != seek:
                        doc_list.remove(doc)
            elif filters:
                for i in range(len(filters)):
                    temp_doc_list = copy.deepcopy(doc_list)
                    f_low = filters[i].get("Low", None)
                    f_high = filters[i].get("High", None)
                    f_inclusion = filters[i].get("Inclusion", 3)
                    for doc in temp_doc_list:
                        doc_field = doc["key"][i]
                        if f_low and f_low != NULL_STRING:
                            if doc_field < f_low:
                                doc_list.remove(doc)
                                continue
                        if f_high and f_high != NULL_STRING:
                            if doc_field > f_high:
                                doc_list.remove(doc)
                                continue
                        if f_inclusion == 0:
                            if doc_field == f_low or doc_field == f_high:
                                doc_list.remove(doc)
                                continue
                        if f_inclusion == 1:
                            if doc_field == f_high:
                                doc_list.remove(doc)
                                continue
                        if f_inclusion == 2:
                            if doc_field == f_low:
                                doc_list.remove(doc)
                                continue
            if expected_results:
                for doc in doc_list:
                    if not doc in expected_results:
                        expected_results.append(doc)
            else:
                expected_results = doc_list
        if len(expected_results) != len(multiscan_result):
            print(multiscan_count_result)
            print(len(expected_results))
            print(expected_results)
            print(len(multiscan_result))
            log.info("No. of items mismatch :- expected = {0} and actual = {1}".format(
                len(expected_results), len(multiscan_result)))
            return False
        if multiscan_count_result is not None:
            if len(expected_results) != multiscan_count_result:
                log.info("No. of items mismatch from multiscan count:- expected = {0} and actual = {1}".format(
                    len(expected_results), multiscan_count_result))
                return False
        if expected_results != multiscan_result:
            if len(expected_results) != 0 and multiscan_result:
                log.info("The number of rows match but the results mismatch, please check")
                return False

        temp_doc_list = []
        for tr in multiscan_result:
            temp_doc_val = str(tr["key"])
            " ".join(temp_doc_val)
            temp_doc_list.append(temp_doc_val)
        log.info("Collated list : {0}".format(temp_doc_list))
        # In case of composite index, the results are ordered by the leading key
        # and if the first key is same, then subsequent keys are considered for ordering.
        sorted_arr = sorted(temp_doc_list, reverse=desc[0])
        # check for idempotency
        if sorted_arr != temp_doc_list:
            self.fail(
                "results of mutiscan is not sorted based on the collation order specified: {0} \n {1}".format(
                    sorted_arr, temp_doc_list))
        return True

    def _update_multiscan_content(self, index_fields=2):
        multiscan_content = {}
        projection = {"EntryKeys": list(range(index_fields)), "PrimaryKey": True}
        multiscan_content["projection"] = json.dumps(projection)
        multiscan_content["distinct"] = False
        multiscan_content["reverse"] = False
        multiscan_content["offset"] = 0
        multiscan_content["limit"] = 10000
        multiscan_content["stale"] = "false"
        return multiscan_content
