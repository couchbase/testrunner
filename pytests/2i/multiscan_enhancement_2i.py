import copy
import json
import logging

from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from .base_2i import BaseSecondaryIndexingTests
from deepdiff import DeepDiff

DATATYPES = [str, "scalar", int, dict, "missing", "empty", "null"]
RANGE_SCAN_TEMPLATE = "SELECT {0} FROM %s WHERE {1}"
NULL_STRING = "~[]{}UnboundedTruenilNA~"

log = logging.getLogger()
emit_fields = "*"


class SecondaryIndexingMultiscanTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexingMultiscanTests, self).setUp()
        self.restServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.restServer)

    def tearDown(self):
        super(SecondaryIndexingMultiscanTests, self).tearDown()

    def test_simple_index_seek(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="name_index", index_fields=["name"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" ORDER BY _id"),
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Seek": None}])
        scan_contents.append([{"Seek": ["Kala"]}])
        scan_contents.append([{"Seek": [30]}])
        scan_contents.append([{"Seek": ["Kala", 30]}])
        scan_contents.append([{"Seek": ["Zack"]}])
        scan_contents.append([{"Seek": [None]}])
        multiscan_content = self._update_multiscan_content(1)
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                multiscan_content["scans"] = json.dumps(scan_content)
                multiscan_result = \
                    self.rest.multiscan_for_gsi_index_with_rest(
                        id_map["id"], json.dumps(multiscan_content))
                multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                    id_map["id"], json.dumps(multiscan_content))
                check = self._verify_items_indexed_for_two_field_index(
                    bucket, id_map["id"],
                    ["name"], scan_content, multiscan_result, multiscan_count_result)
                if not check:
                    failed_scans.append(scan_content)
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_simple_index_multiple_seek(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="name_index", index_fields=["name"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" ORDER BY _id"),
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Seek": None}, {"Seek": None}])
        scan_contents.append([{"Seek": None}, {"Seek": ["Kala"]}])
        scan_contents.append([{"Seek": None}, {"Seek": ["Kala", 30]}])
        scan_contents.append([{"Seek": None}, {"Seek": ["Winta"]}])
        scan_contents.append([{"Seek": None}, {"Seek": ["Zack"]}])
        scan_contents.append([{"Seek": None}, {"Seek": [None]}])
        scan_contents.append([{"Seek": ["Kala"]}, {"Seek": None}])
        scan_contents.append([{"Seek": ["Kala"]}, {"Seek": ["Kala"]}])
        scan_contents.append([{"Seek": ["Kala"]}, {"Seek": ["Kala", 30]}])
        scan_contents.append([{"Seek": ["Kala"]}, {"Seek": ["Winta"]}])
        scan_contents.append([{"Seek": ["Kala"]}, {"Seek": ["Zack"]}])
        scan_contents.append([{"Seek": ["Kala"]}, {"Seek": [None]}])
        scan_contents.append([{"Seek": ["Kala", 30]}, {"Seek": None}])
        scan_contents.append([{"Seek": ["Kala", 30]}, {"Seek": ["Kala"]}])
        scan_contents.append([{"Seek": ["Kala", 30]}, {"Seek": ["Kala", 30]}])
        scan_contents.append([{"Seek": ["Kala", 30]}, {"Seek": ["Winta"]}])
        scan_contents.append([{"Seek": ["Kala", 30]}, {"Seek": ["Zack"]}])
        scan_contents.append([{"Seek": ["Kala", 30]}, {"Seek": [None]}])
        scan_contents.append([{"Seek": ["Winta"]}, {"Seek": ["Kala"]}])
        scan_contents.append([{"Seek": ["Winta"]}, {"Seek": ["Kala", 30]}])
        scan_contents.append([{"Seek": ["Winta"]}, {"Seek": ["Winta"]}])
        scan_contents.append([{"Seek": ["Winta"]}, {"Seek": ["Zack"]}])
        scan_contents.append([{"Seek": ["Winta"]}, {"Seek": [None]}])
        scan_contents.append([{"Seek": [None]}, {"Seek": None}])
        scan_contents.append([{"Seek": [None]}, {"Seek": ["Kala"]}])
        scan_contents.append([{"Seek": [None]}, {"Seek": ["Kala", 30]}])
        scan_contents.append([{"Seek": [None]}, {"Seek": ["Winta"]}])
        scan_contents.append([{"Seek": [None]}, {"Seek": ["Zack"]}])
        scan_contents.append([{"Seek": [None]}, {"Seek": [None]}])
        scan_contents.append([{"Seek": ["Adara"]}, {"Seek": ["Winta"]}])
        multiscan_content = self._update_multiscan_content(1)
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                multiscan_content["scans"] = json.dumps(scan_content)
                multiscan_result = \
                    self.rest.multiscan_for_gsi_index_with_rest(
                        id_map["id"], json.dumps(multiscan_content))
                multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                    id_map["id"], json.dumps(multiscan_content))
                check = self._verify_items_indexed_for_two_field_index(
                    bucket, id_map["id"],
                    ["name"], scan_content, multiscan_result, multiscan_count_result)
                if not check:
                    failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_simple_index_filter(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="name_index", index_fields=["name"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" ORDER BY _id"),
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Callia", "High":"Kala"}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Callia"}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Kala"}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Kala"}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low":"Winta", "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Adara"}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low":None, "High":"Callia"}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": None}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": 30}]}])
        scan_contents.append([{"Filter":[{"Low": 30, "High": "Kala"}]}])
        #TODO: Missing Scenarios
        #scan_contents.append([{"Low": MISSING, "High":"Kala"}])
        #scan_contents.append([{"Low": "Kala", "High": MISSING}])
        #scan_contents.append([{"Low": MISSING, "High": MISSING}])
        multiscan_content = self._update_multiscan_content(1)
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                for name_inclusion in range(4):
                    scan_content[0]["Filter"][0]["Inclusion"] = name_inclusion
                    multiscan_content["scans"] = json.dumps(scan_content)
                    multiscan_result = \
                        self.rest.multiscan_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                    multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                        id_map["id"], json.dumps(multiscan_content))
                    check = self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"],
                        ["name"], scan_content, multiscan_result, multiscan_count_result)
                    if not check:
                        failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_simple_index_multiple_filter_spans(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="name_index", index_fields=["name"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" ORDER BY _id"),
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Callia"}]},
                              {"Filter":[{"Low": "Kala", "High": "Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Callia"}]},
                              {"Filter":[{"Low": "Callia", "High": "Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Callia"}]},
                              {"Filter":[{"Low": "Callia", "High": "Callia"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Kala"}]},
                              {"Filter":[{"Low": "Callia", "High": "Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Winta"}]},
                              {"Filter":[{"Low": "Callia", "High": "Kala"}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Callia"}]},
                              {"Filter":[{"Low": "Kala", "High": "Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High": NULL_STRING}]},
                              {"Filter":[{"Low": "Callia", "High": "Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Callia"}]},
                              {"Filter":[{"Low": NULL_STRING, "High": "Winta"}]}])
        scan_contents.append([{"Filter":[{"Low":"Adara", "High":"Callia"}]},
                              {"Filter":[{"Low": "Callia", "High": NULL_STRING}]}])
        multiscan_content = self._update_multiscan_content(1)
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                for name_inclusion_first in range(4):
                    for name_inclusion_second in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = name_inclusion_first
                        scan_content[1]["Filter"][0]["Inclusion"] = name_inclusion_second
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name"], scan_content, multiscan_result, multiscan_count_result)
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_seek(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="name_index", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Seek": ["Kacila", 58]}])
        scan_contents.append([{"Seek": None}])
        scan_contents.append([{"Seek": [None, 30]}])
        scan_contents.append([{"Seek": [None, None]}])
        scan_contents.append([{"Seek": ["Kacila", None]}])
        scan_contents.append([{"Seek": [58, "Kacila"]}])
        scan_contents.append([{"Seek": [NULL_STRING, 58]}])
        scan_contents.append([{"Seek": ["Kacila", NULL_STRING]}])
        scan_contents.append([{"Seek": [NULL_STRING, NULL_STRING]}])
        scan_contents.append([{"Seek": ["Kacila", 58, "Bangalore"]}])
        #scan_contents.append([{"Seek": ["Kacila"]}])
        #TODO Missing Elements
        #scan_contents.append({"Seek": ["Kacila", MISSING]})
        #scan_contents.append({"Seek": [MISSING, 58]})
        #scan_contents.append({"Seek": [MISSING, MISSING]})
        multiscan_content = self._update_multiscan_content()
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                multiscan_content["scans"] = json.dumps(scan_content)
                multiscan_result = self.rest.multiscan_for_gsi_index_with_rest(
                    id_map["id"], json.dumps(multiscan_content))
                multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                    id_map["id"], json.dumps(multiscan_content))
                check = self._verify_items_indexed_for_two_field_index(
                    bucket, id_map["id"],
                    ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                if not check:
                    failed_scans.append(copy.deepcopy(scan_content))
                    log.info("** Scan {0} failed. Result : {1}".format(scan_content, multiscan_result))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_basic_filter(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Callia\" AND "
                                                      "name < \"Kala\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Seek": None, "Filter":[{"Low":"Callia", "High":"Kala"},
                                                 {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Callia", "High":"Kala"},
                                         {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Kala"},
                                         {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Callia"},
                                         {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Callia", "High":"Kala"},
                                         {"Low":40, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Kala"},
                                         {"Low":40, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Callia"},
                                         {"Low":40, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Callia", "High":"Kala"},
                                         {"Low":70, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Kala"},
                                         {"Low":70, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Callia"},
                                         {"Low":70, "High":40}]}])
        multiscan_content = self._update_multiscan_content()
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                for name_inclusion in range(4):
                    for age_inclusion in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = \
                            name_inclusion
                        scan_content[0]["Filter"][1]["Inclusion"] = \
                            age_inclusion
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_unbounded_filter(self):
        failed_scans = []
        query_definition = QueryDefinition(
             index_name="two_field_composite_name_age",
             index_fields=["name", "age"],
             query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
             groups=["two_field_index"],
             index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Winta"},
                                        {"Low":30, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": NULL_STRING},
                                        {"Low":30, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High": NULL_STRING},
                                        {"Low":30, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Winta"},
                                        {"Low": NULL_STRING, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Winta"},
                                        {"Low":30, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Winta"},
                                        {"Low": NULL_STRING, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": NULL_STRING},
                                        {"Low":30, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Winta"},
                                        {"Low": NULL_STRING, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Winta"},
                                        {"Low":30, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": NULL_STRING},
                                        {"Low": NULL_STRING, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": NULL_STRING},
                                        {"Low":NULL_STRING, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Winta"},
                                        {"Low": NULL_STRING, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High": NULL_STRING},
                                        {"Low": 30, "High": NULL_STRING}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High": NULL_STRING},
                                        {"Low": NULL_STRING, "High": 50}]}])
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High": NULL_STRING},
                                        {"Low": NULL_STRING, "High": NULL_STRING}]}])
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            multiscan_content = self._update_multiscan_content()
            for scan_content in scan_contents:
                for name_inclusion in range(4):
                    for age_inclusion in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = \
                            name_inclusion
                        scan_content[0]["Filter"][1]["Inclusion"] = \
                            age_inclusion
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_null_filter(self):
        failed_scans = []
        query_definition = QueryDefinition(
             index_name="two_field_composite_name_age",
             index_fields=["name", "age"],
             query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
             groups=["two_field_index"],
             index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Filter":[{"Low": None, "High":"Winta"},
                                        {"Low":30, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": None},
                                        {"Low":30, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low": None, "High": None},
                                        {"Low":30, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Winta"},
                                        {"Low": None, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Winta"},
                                        {"Low":30, "High": None}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Winta"},
                                        {"Low": None, "High": None}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": None},
                                        {"Low":30, "High": None}]}])
        scan_contents.append([{"Filter":[{"Low": None, "High":"Winta"},
                                        {"Low": None, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low": None, "High":"Winta"},
                                        {"Low":30, "High": None}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": None},
                                        {"Low": None, "High":50}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High": None},
                                        {"Low":None, "High": None}]}])
        scan_contents.append([{"Filter":[{"Low": None, "High":"Winta"},
                                        {"Low": None, "High": None}]}])
        scan_contents.append([{"Filter":[{"Low": None, "High": None},
                                        {"Low": 30, "High": None}]}])
        scan_contents.append([{"Filter":[{"Low": None, "High": None},
                                        {"Low": None, "High": 50}]}])
        scan_contents.append([{"Filter":[{"Low": None, "High": None},
                                        {"Low": None, "High": None}]}])
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            multiscan_content = self._update_multiscan_content()
            for scan_content in scan_contents:
                for name_inclusion in range(4):
                    for age_inclusion in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = \
                            name_inclusion
                        scan_content[0]["Filter"][1]["Inclusion"] = \
                            age_inclusion
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_filter_empty_results(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Callia\" AND "
                                                      "name < \"Kala\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Seek": None, "Filter":[{"Low":"Callia", "High":"Kala"},
                                                 {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Callia", "High":"Kala"},
                                         {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Kala"},
                                         {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Callia"},
                                         {"Low":40, "High":70}]}])
        scan_contents.append([{"Filter":[{"Low":"Callia", "High":"Kala"},
                                         {"Low":40, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Kala"},
                                         {"Low":40, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Callia"},
                                         {"Low":40, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Callia", "High":"Kala"},
                                         {"Low":70, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Kala"},
                                         {"Low":70, "High":40}]}])
        scan_contents.append([{"Filter":[{"Low":"Kala", "High":"Callia"},
                                         {"Low":70, "High":40}]}])
        multiscan_content = self._update_multiscan_content()
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                for name_inclusion in range(4):
                    for age_inclusion in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = \
                            name_inclusion
                        scan_content[0]["Filter"][1]["Inclusion"] = \
                            age_inclusion
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_name_age_composite_index_seek_filter(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Seek": ["Kimberly", 58],
                              "Filter":[{"Low":"A", "High":"Z"},
                                        {"Low":50, "High":50}]}]
        multiscan_content = {}
        projection = {"EntryKeys":[1], "PrimaryKey":False}
        multiscan_content["projection"] = json.dumps(projection)
        multiscan_content["distinct"] = False
        multiscan_content["reverse"] = False
        multiscan_content["offset"] = 0
        multiscan_content["limit"] = 10000
        multiscan_content["stale"] = "false"
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for name_inclusion in range(4):
                for age_inclusion in range(4):
                    scan_content[0]["Filter"][0]["Inclusion"] = \
                        name_inclusion
                    scan_content[0]["Filter"][1]["Inclusion"] = \
                        age_inclusion
                    multiscan_content["scans"] = json.dumps(scan_content)
                    multiscan_result = \
                        self.rest.multiscan_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                    multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                        id_map["id"], json.dumps(multiscan_content))
                    check = self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"],
                        ["name"], scan_content, multiscan_result, multiscan_count_result)
                    if not check:
                        failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_name_age_composite_index_empty_second_filter(self):
        failed_scans = []
        query_definition = QueryDefinition(
             index_name="two_field_composite_name_age",
             index_fields=["name", "age"],
             query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),

             groups=["two_field_index"],
             index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Filter":[{"Low": "M", "High": "Z"}, {}]}]
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            multiscan_content = self._update_multiscan_content()
            for name_inclusion in range(4):
                scan_content[0]["Filter"][0]["Inclusion"] = \
                    name_inclusion
                multiscan_content["scans"] = json.dumps(scan_content)
                log.info(multiscan_content)
                multiscan_result = \
                    self.rest.multiscan_for_gsi_index_with_rest(
                        id_map["id"], json.dumps(multiscan_content))
                multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                    id_map["id"], json.dumps(multiscan_content))
                check = self._verify_items_indexed_for_two_field_index(
                    bucket, id_map["id"], ["name"], scan_content, multiscan_result, multiscan_count_result)
                if not check:
                    failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_name_age_distinct_array_composite_index(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_travel_history_age",
            index_fields=["DISTINCT ARRAY t FOR t in TO_ARRAY(`travel_history`) END", "age"],
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        #Basic Scan
        scan_contents.append([{"Seek": None,
                             "Filter":[{"Low":"India", "High":"US"},
                                       {"Low":30, "High":60}]}])
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                multiscan_content = self._update_multiscan_content()
                for name_inclusion in range(4):
                    for age_inclusion in range(4):
                        scan_content[0]["Filter"][0]["Inclusion"] = \
                            name_inclusion
                        scan_content[0]["Filter"][1]["Inclusion"] = \
                            age_inclusion
                        multiscan_content["scans"] = json.dumps(scan_content)
                        multiscan_result = \
                            self.rest.multiscan_for_gsi_index_with_rest(
                                id_map["id"], json.dumps(multiscan_content))
                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name"], scan_content, multiscan_result, multiscan_count_result)
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_name_age_all_array_composite_index(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_travel_history_age",
            index_fields=["ALL ARRAY t FOR t in TO_ARRAY(`travel_history`) END", "age"],
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        #Basic Scan
        scan_content = [{"Seek": None,
                         "Filter":[{"Low":"India", "High":"US"},
                                   {"Low":30, "High":60}]}]
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            multiscan_content = self._update_multiscan_content()
            for name_inclusion in range(4):
                for age_inclusion in range(4):
                    scan_content[0]["Filter"][0]["Inclusion"] = \
                        name_inclusion
                    scan_content[0]["Filter"][1]["Inclusion"] = \
                        age_inclusion
                    multiscan_content["scans"] = json.dumps(scan_content)
                    multiscan_result = \
                        self.rest.multiscan_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                    multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                        id_map["id"], json.dumps(multiscan_content))
                    check = self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"],
                        ["name"], scan_content, multiscan_result, multiscan_count_result)
                    if not check:
                        failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_name_age_premium_customer_composite_index(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="three_field_composite_name_age",
            index_fields=["name", "age", "premium_customer"],
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        #Basic Scan
        scan_content = [{"Seek": None,
                             "Filter":[{"Low":"India", "High":"US"},
                                       {"Low":30, "High":60},
                                       {"Low": False, "High": True}]}]
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
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
                        check = self._verify_items_indexed_for_two_field_index(
                            bucket, id_map["id"],
                            ["name"], scan_content, multiscan_result, multiscan_count_result)
                        if not check:
                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_multiple_nonoverlapping_scans(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Jerica"},
                                   {"Low":10, "High":100}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Kacila", "High":"Winta"},
                                   {"Low":10, "High":100}]}])
        #non-overlapping second filters
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Winta"},
                                   {"Low":10, "High":50}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Adara", "High":"Winta"},
                                   {"Low":51, "High":100}]}])
        #non-overlapping both Scans
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Jerica"},
                                   {"Low":10, "High":50}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Kacila", "High":"Winta"},
                                   {"Low":51, "High":100}]}])
        # Boundary first Filters
        scan_contents.append([{"Filter":[{"Low": "Adara", "High":"Jerica"},
                                         {"Low":10, "High":100}]},
                              {"Filter":[{"Low":"Jerica", "High":"Winta"},
                                         {"Low":10, "High":100}]}])
        # Boundary Second Filters
        scan_contents.append([{"Filter":[{"Low": "Adara", "High":"Winta"},
                                         {"Low":10, "High":50}]},
                              {"Filter":[{"Low":"Adara", "High":"Winta"},
                                         {"Low":50, "High":100}]}])
        # Boundary Both Filters
        scan_contents.append([{"Filter":[{"Low": "Adara", "High":"Winta"},
                                         {"Low":10, "High":50}]},
                              {"Filter":[{"Low":"Adara", "High":"Winta"},
                                         {"Low":50, "High":100}]}])
        # Unbounded First filter of first scan
        scan_contents.append([{"Filter":[{"Low": NULL_STRING, "High":"Jerica"},
                                         {"Low":10, "High":40}]},
                              {"Filter":[{"Low":"Kala", "High":"Winta"},
                                         {"Low":50, "High":100}]}])
        # Unbounded Second filter of first scan
        scan_contents.append([{"Filter":[{"Low": "Adara", "High": NULL_STRING},
                                         {"Low":10, "High":40}]},
                              {"Filter":[{"Low":"Kala", "High":"Winta"},
                                         {"Low":50, "High":100}]}])
        # Unbounded First filter of Second scan
        scan_contents.append([{"Filter":[{"Low": "Adara", "High": "Jerica"},
                                         {"Low":10, "High":40}]},
                              {"Filter":[{"Low": NULL_STRING, "High":"Winta"},
                                         {"Low":50, "High":100}]}])
        # Unbounded Second filter of Second scan
        scan_contents.append([{"Filter":[{"Low": "Adara", "High": "Jerica"},
                                         {"Low":10, "High":40}]},
                              {"Filter":[{"Low":"Kala", "High": NULL_STRING},
                                         {"Low":50, "High":100}]}])
        #non-overlapping both Scans Reversed
        scan_contents.append([{"Filter":[{"Low":"Kacila", "High":"Winta"},
                                         {"Low":51, "High":100}]},
                              {"Filter":[{"Low": "Adara", "High":"Jerica"},
                                         {"Low":10, "High":50}]}])
        multiscan_content = self._update_multiscan_content()
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                for first_name_inclusion in range(4):
                    for first_age_inclusion in range(4):
                        for second_name_inclusion in range(4):
                            for second_age_inclusion in range(4):
                                if "Filter" in list(scan_content[0].keys()):
                                    scan_content[0]["Filter"][0]["Inclusion"] = \
                                        first_name_inclusion
                                    scan_content[0]["Filter"][1]["Inclusion"] = \
                                        first_age_inclusion
                                if "Filter" in list(scan_content[1].keys()):
                                    scan_content[1]["Filter"][0]["Inclusion"] = \
                                        second_name_inclusion
                                    scan_content[1]["Filter"][1]["Inclusion"] = \
                                        second_age_inclusion
                                multiscan_content["scans"] = json.dumps(scan_content)
                                multiscan_result = \
                                    self.rest.multiscan_for_gsi_index_with_rest(
                                        id_map["id"], json.dumps(multiscan_content))
                                multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                                    id_map["id"], json.dumps(multiscan_content))
                                check = self._verify_items_indexed_for_two_field_index(
                                    bucket, id_map["id"],
                                    ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                                if not check:
                                    failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_multiple_overlapping_scans(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        #Overlapping first Scans
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Jerica"},
                                   {"Low":10, "High":50}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Jerica", "High":"Winta"},
                                   {"Low":50, "High":100}]}])
        #Second is subset of first - first filter
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Winta"},
                                   {"Low":10, "High":100}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Jerica", "High":"Kacila"},
                                   {"Low":10, "High":100}]}])
        #overlapping second Scans
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Winta"},
                                   {"Low":10, "High":50}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Adara", "High":"Winta"},
                                   {"Low":40, "High":100}]}])
        #Second is subset of first - second filter
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Winta"},
                                   {"Low":10, "High":100}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Adara", "High":"Winta"},
                                   {"Low":40, "High":60}]}])

        #Overlapping both Scans
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Kacila"},
                                   {"Low":10, "High":50}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Jerica", "High":"Winta"},
                                   {"Low":40, "High":100}]}])
        #Second is subset of first - both filter
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Winta"},
                                   {"Low":10, "High":70}]},
                        {"Seek": None,
                         "Filter":[{"Low":"Jerica", "High":"Kacila"},
                                   {"Low":40, "High":60}]}])

        #Seek First Scan
        scan_contents.append([{"Seek": ["Kacila", 40]},
                              {"Seek": None,
                               "Filter":[{"Low":"Jerica", "High":"Winta"},
                                         {"Low":40, "High":100}]}])
        #Seek Second Scan
        scan_contents.append([{"Seek": None,
                         "Filter":[{"Low": "Adara", "High":"Kacila"},
                                   {"Low":10, "High":50}]},
                        {"Seek": ["Jerica", 40]}])
        #Seek First And Second Scan
        scan_contents.append([{"Seek": ["Kacila", 40]},
                               {"Seek": ["Jerica", 40]}])
        multiscan_content = self._update_multiscan_content()
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                for first_name_inclusion in range(4):
                    for first_age_inclusion in range(4):
                        for second_name_inclusion in range(4):
                            for second_age_inclusion in range(4):
                                if "Filter" in list(scan_content[0].keys()):
                                    scan_content[0]["Filter"][0]["Inclusion"] = \
                                        first_name_inclusion
                                    scan_content[0]["Filter"][1]["Inclusion"] = \
                                        first_age_inclusion
                                if "Filter" in list(scan_content[1].keys()):
                                    scan_content[1]["Filter"][0]["Inclusion"] = \
                                        second_name_inclusion
                                    scan_content[1]["Filter"][1]["Inclusion"] = \
                                        second_age_inclusion
                                multiscan_content["scans"] = json.dumps(scan_content)
                                multiscan_result = \
                                    self.rest.multiscan_for_gsi_index_with_rest(
                                        id_map["id"], json.dumps(multiscan_content))
                                multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                                    id_map["id"], json.dumps(multiscan_content))
                                check = self._verify_items_indexed_for_two_field_index(
                                    bucket, id_map["id"],
                                    ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                                if not check:
                                    failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

    def test_two_field_composite_index_three_scans(self):
        failed_scans = []
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        # Three Non-Overlapping first Filters
        scan_contents.append([{"Filter": [{"Low": "Adara", "High":"Callia"},
                                         {"Low":10, "High":100}]},
                              {"Filter": [{"Low":"Fantine", "High":"Kacila"},
                                         {"Low":10, "High":100}]},
                              {"Filter": [{"Low": "Perdita", "High": "Winta"},
                                          {"Low":10, "High":100}]}])
        # Three Non-Overlapping second Filters
        scan_contents.append([{"Filter": [{"Low": "Adara", "High":"Winta"},
                                         {"Low":10, "High":30}]},
                              {"Filter": [{"Low": "Adara", "High":"Winta"},
                                         {"Low":40, "High":50}]},
                              {"Filter": [{"Low": "Adara", "High":"Winta"},
                                          {"Low":60, "High":80}]}])
        # Three Non-Overlapping second Filters
        scan_contents.append([{"Filter": [{"Low": "Adara", "High":"Callia"},
                                         {"Low":10, "High":30}]},
                              {"Filter": [{"Low": "Fantine", "High":"Kacila"},
                                         {"Low":40, "High":50}]},
                              {"Filter": [{"Low": "Perdita", "High":"Winta"},
                                          {"Low":60, "High":80}]}])
        multiscan_content = self._update_multiscan_content()
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                for first_name_inclusion in range(4):
                    for first_age_inclusion in range(4):
                        for second_name_inclusion in range(4):
                            for second_age_inclusion in range(4):
                                for third_name_inclusion in range(4):
                                    for third_age_inclusion in range(4):
                                        if "Filter" in list(scan_content[0].keys()):
                                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                                first_name_inclusion
                                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                                first_age_inclusion
                                        if "Filter" in list(scan_content[1].keys()):
                                            scan_content[1]["Filter"][0]["Inclusion"] = \
                                                second_name_inclusion
                                            scan_content[1]["Filter"][1]["Inclusion"] = \
                                                second_age_inclusion
                                        if "Filter" in list(scan_content[2].keys()):
                                            scan_content[2]["Filter"][0]["Inclusion"] = \
                                                third_name_inclusion
                                            scan_content[2]["Filter"][1]["Inclusion"] = \
                                                third_age_inclusion
                                        multiscan_content["scans"] = json.dumps(scan_content)
                                        multiscan_result = \
                                            self.rest.multiscan_for_gsi_index_with_rest(
                                                id_map["id"], json.dumps(multiscan_content))
                                        multiscan_count_result = self.rest.multiscan_count_for_gsi_index_with_rest(
                                            id_map["id"], json.dumps(multiscan_content))
                                        check = self._verify_items_indexed_for_two_field_index(
                                            bucket, id_map["id"],
                                            ["name", "age"], scan_content, multiscan_result, multiscan_count_result)
                                        if not check:
                                            failed_scans.append(copy.deepcopy(scan_content))
        msg = "Failed Scans: {0}".format(failed_scans)
        self.assertEqual(len(failed_scans), 0, msg)

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

    def _verify_items_indexed_for_two_field_index(self, bucket, index_id, index_fields,
                                                  scan_content, multiscan_result, multiscan_count_result=None):
        err_message = "There are more ranges than number"
        if isinstance(multiscan_result, dict):
            if err_message in list(multiscan_result.values()):
                multiscan_result = []
        expected_results = []
        body = {"stale": "False"}
        for content in scan_content:
            doc_list = self.rest.full_table_scan_gsi_index_with_rest(index_id, body)
            seek = content.get("Seek", None)
            filters = content.get("Filter", None)
            if seek:
                if len(seek) < len(index_fields):
                    if len(multiscan_result) != 0:
                        return False
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
            log.info("No. of items mismatch :- expected = {0} and actual = {1}".format(
                len(expected_results), len(multiscan_result)))
            return False
        if multiscan_count_result is not None:
            if len(expected_results) != multiscan_count_result:
                log.info("No. of items mismatch :- expected = {0} and actual = {1}".format(
                    len(expected_results), multiscan_count_result))
                return False

        if multiscan_result == '':
            multiscan_result = []
        diffs = DeepDiff(expected_results, multiscan_result,  ignore_order = True, ignore_string_type_changes=True)
        if diffs:
            log.info("The number of rows match but the results mismatch, please check. Diffs: {}".format(diffs))
            return False

        return True
