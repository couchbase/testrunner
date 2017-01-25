import json
import logging
import random

from string import lowercase
from couchbase.bucket import Bucket
from couchbase_helper.data import FIRST_NAMES, COUNTRIES
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from base_2i import BaseSecondaryIndexingTests

DATATYPES = [unicode, "scalar", int, dict, "missing", "empty", "null"]
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

    def test_name_age_composite_index_basic_span(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Seek": None,
                             "Filter":[{"Low":"Adara", "High":"Z"},
                                       {"Low":30, "High":60}]}]
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
                    self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"], scan_content, multiscan_result)

    def test_name_age_composite_index_same_first_filter(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Seek": None,
                             "Filter":[{"Low":"A", "High":"A"},
                                       {"Low":30, "High":60}]}]
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
                    self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"], scan_content, multiscan_result)

    def test_name_age_composite_index_same_second_filter(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Seek": None,
                             "Filter":[{"Low":"A", "High":"A"},
                                       {"Low":30, "High":60}]}]
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
                    self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"], scan_content, multiscan_result)

    def test_name_age_composite_index_seek_filter(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Seek": "[58,'Kimberly']",
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
                    self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"], scan_content, multiscan_result)

    def test_name_age_composite_index_seek(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Seek": "[58,'Kimberly']"}]
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
                    self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"], scan_content, multiscan_result)

    def test_name_age_composite_index_limits_reversed(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age", index_fields=["name", "age"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\""
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_content = [{"Seek": None, "Filter":[{"Low":"Z", "High":"X"},
                                                 {"Low":0, "High":100}]}]
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
                    self._verify_items_indexed_for_two_field_index(
                        bucket, id_map["id"], scan_content, multiscan_result)

    def test_name_age_distinct_array_composite_index(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age",
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
                multiscan_content = {}
                projection = {"EntryKeys":[1], "PrimaryKey":False}
                multiscan_content["projection"] = json.dumps(projection)
                multiscan_content["distinct"] = False
                multiscan_content["reverse"] = False
                multiscan_content["offset"] = 0
                multiscan_content["limit"] = 10000
                multiscan_content["stale"] = "false"
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
                        self._verify_items_indexed_for_two_field_index(bucket, id_map["id"],
                                                   scan_content, multiscan_result)

    def test_name_age_all_array_composite_index(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age",
            index_fields=["ALL ARRAY t FOR t in TO_ARRAY(`travel_history`) END", "age"],
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        #Basic Scan
        scan_contents.append([{"Seek": None,
                             "Filter":[{"Low":"India", "High":"US"},
                                       {"Low":30, "High":60}]}])
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                multiscan_content = {}
                projection = {"EntryKeys":[1], "PrimaryKey":False}
                multiscan_content["projection"] = json.dumps(projection)
                multiscan_content["distinct"] = False
                multiscan_content["reverse"] = False
                multiscan_content["offset"] = 0
                multiscan_content["limit"] = 10000
                multiscan_content["stale"] = "false"
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
                        self._verify_items_indexed_for_two_field_index(bucket, id_map["id"],
                                                   scan_content, multiscan_result)

    def test_name_age_premium_customer_composite_index(self):
        query_definition = QueryDefinition(
            index_name="two_field_composite_name_age",
            index_fields=["name", "age", "premium_customer"],
            groups=["two_field_index"], index_where_clause=" name IS NOT NULL ")
        scan_contents = []
        #Basic Scan
        scan_contents.append([{"Seek": None,
                             "Filter":[{"Low":"India", "High":"US"},
                                       {"Low":30, "High":60},
                                       {"Low": False, "High": True}]}])
        for bucket in self.buckets:
            id_map = self.create_index_using_rest(bucket, query_definition)
            for scan_content in scan_contents:
                multiscan_content = {}
                projection = {"EntryKeys":[1], "PrimaryKey":False}
                multiscan_content["projection"] = json.dumps(projection)
                multiscan_content["distinct"] = False
                multiscan_content["reverse"] = False
                multiscan_content["offset"] = 0
                multiscan_content["limit"] = 10000
                multiscan_content["stale"] = "false"
                for name_inclusion in range(4):
                    for age_inclusion in range(4):
                        for cust_inclustion in range(4):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                cust_inclustion
                            multiscan_content["scans"] = json.dumps(scan_content)
                            multiscan_result = \
                                self.rest.multiscan_for_gsi_index_with_rest(
                                    id_map["id"], json.dumps(multiscan_content))
                        # self._verify_items_indexed_for_two_field_index(bucket, id_map["id"],
                        #                            scan_content, multiscan_result)

    def _verify_items_indexed_for_two_field_index(self, bucket, index_id, scan_content, multiscan_result):
        body = {"stale": "False"}
        doc_list = []
        full_table_scan = self.rest.full_table_scan_gsi_index_with_rest(index_id, body)
        for content in scan_content:
            seek = content["Seek"]
            filters = content["Filter"]
            if seek:
                name = seek[0]
                age = seek[1]
                for doc in full_table_scan:
                    if doc["name"] == name and doc["age"] == age:
                        doc_list.append(doc)
            else:
                name_low = filters[0]["Low"]
                name_high = filters[0]["High"]
                name_inclusion = filters[0]["Inclusion"]
                age_low = filters[1]["Low"]
                age_high = filters[1]["High"]
                age_inclusion = filters[1]["Inclusion"]
                temp_doc_list = []
                for doc in full_table_scan:
                    doc_name = doc["key"][0]
                    if name_inclusion == 0:
                        if doc_name > name_low and doc_name < name_high:
                            temp_doc_list.append(doc)
                    if name_inclusion == 1:
                        if doc_name >= name_low and doc_name < name_high:
                            temp_doc_list.append(doc)
                    if name_inclusion == 2:
                        if doc_name > name_low and doc_name <= name_high:
                            temp_doc_list.append(doc)
                    if name_inclusion == 3:
                        if doc_name >= name_low and doc_name <= name_high:
                            temp_doc_list.append(doc)
                for doc in temp_doc_list:
                    doc_age = doc["key"][1]
                    if age_inclusion == 0:
                        if doc_age > age_low and doc_age < age_high:
                            doc_list.append(doc)
                    if age_inclusion == 1:
                        if doc_age >= age_low and doc_age < age_high:
                            doc_list.append(doc)
                    if age_inclusion == 2:
                        if doc_age > age_low and doc_age <= age_high:
                            doc_list.append(doc)
                    if age_inclusion == 3:
                        if doc_age >= age_low and doc_age <= age_high:
                            doc_list.append(doc)
        self.assertEqual(len(doc_list), len(multiscan_result), "No. of items mismatch")
        msg = "The number of rows match but the results mismatch, please check"
        if sorted(doc_list) != sorted(multiscan_result):
            raise Exception(msg)
