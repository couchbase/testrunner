import json
import logging
import random
from threading import Thread

from couchbase.bucket import Bucket
from couchbase_helper.data import FIRST_NAMES, COUNTRIES
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from .base_2i import BaseSecondaryIndexingTests
from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator

DATATYPES = [str, "scalar", int, dict, "missing", "empty", "null"]
RANGE_SCAN_TEMPLATE = "SELECT {0} FROM %s WHERE {1}"
log = logging.getLogger()
emit_fields = "*"
query_definition = QueryDefinition(
    index_name="two_field_composite_name_age",
    index_fields=["name", "age"],
    query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                              "name > \"Adara\" AND "
                                              "name < \"Winta\" "
                                              "AND age > 0 AND age "
                                              "< 100 ORDER BY _id"),
    groups=["two_field_index"],
    index_where_clause=" name IS NOT NULL ")

query_definition1 = QueryDefinition(
    index_name="two_field_composite_name_age1",
    index_fields=["name", "age"],
    query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                              "name > \"Adara\" AND "
                                              "name < \"Winta\" "
                                              "AND age > 20 AND age "
                                              "< 50 ORDER BY _id"),
    groups=["two_field_index1"],
    index_where_clause=" name IS NOT NULL ")

query_definition2 = QueryDefinition(
    index_name="single_field_index",
    index_fields=["name"],
    query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                              "name > \"Adara\" AND "
                                              "name < \"Winta\" "
                                              "ORDER BY _id"),
    groups=["single_field_index"],
    index_where_clause=" name IS NOT NULL ")

global_projection_value = None


class SecondaryIndexingOffsetTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexingOffsetTests, self).setUp()
        self.restServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.restServer)

    def tearDown(self):
        super(SecondaryIndexingOffsetTests, self).tearDown()

    def test_with_offset_0_and_limit(self):
        """
            Test multiscan with offset 0 and limit with different values
        """
        limits = [1, 10, 25, 67, 99, 100]
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for limit in limits:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                self._multiscan_api_helper(scan_contents, bucket, offset=0, limit=limit, id_map=id_map)

    def test_without_offset_and_with_limit(self):
        """
            Test multiscan with no offset and limit with different values
        """
        limits = [1, 10, 25, 67, 99, 100]
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for limit in limits:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                self._multiscan_api_helper(scan_contents, bucket, offset=None, limit=limit, id_map=id_map)

    def test_with_offset_n_and_with_limit(self):
        """
            Test multiscan with different offset values and limit
        """
        offsets = [1, 10, 25, 67, 99]
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for offset in offsets:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                self._multiscan_api_helper(scan_contents, bucket, offset=offset, limit=1, id_map=id_map)

    def test_with_offset_n_and_without_limit(self):
        """
            Test multiscan with different offset values and no limit
        """
        offsets = [1, 10, 25, 67, 99, 200]
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for offset in offsets:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                multiscan_result = self._multiscan_api_helper(scan_contents, bucket, offset=offset, limit=None,
                                                              id_map=id_map)
                minimum = min(100, self.docs_per_day - offset)
                if minimum < 0:
                    minimum = 0
                self.assertEqual(len(multiscan_result), minimum)

    def test_with_offset_and_with_limit_greater_than_available_docs_after_offset(self):
        """
            Test multiscan where offset+limit is greater than available docs in the bucket
        """
        steps = [1, 2, 5, 10]
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for step in steps:
            offset = self.docs_per_day - step
            limit = self.docs_per_day + step
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                self._multiscan_api_helper(scan_contents, bucket, offset=offset, limit=limit, id_map=id_map)

    def test_without_offset_and_without_limit(self):
        """
            Test multiscan without offset and limit
        """
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition)
            self._multiscan_api_helper(scan_contents, bucket, offset=None, limit=None, id_map=id_map)

    def test_with_offset_greater_than_available_docs_and_with_limit(self):
        """
            Test multiscan with offset greater than available docs
        """
        steps = [100, 200, 500, 1000]
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for step in steps:
            offset = self.docs_per_day + step
            limit = step
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                self._multiscan_api_helper(scan_contents, bucket, offset=offset, limit=limit, id_map=id_map)

    # TODO : reverse is not yet implemented, Run it only after the code is merged
    def test_with_offset_limit_and_with_reverse_true(self):
        """
            Test multiscan with offset , limit and where reverse in true
        """
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition)
            self._multiscan_api_helper(scan_contents, bucket, offset=0, limit=10, id_map=id_map)

    def test_with_offset_limit_and_with_3_indexed_fields(self):
        """
            Test multiscan with offset and limit with more than 2 index fields
        """
        query_definition1 = QueryDefinition(
            index_name="three_field_composite_name_age_question_values",
            index_fields=["name", "age", "question_values"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["three_field_index"],
            index_where_clause=" name IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition1)
                multiscan_result = self._multiscan_api_helper(scan_contents, bucket, offset=10, limit=10, id_map=id_map)
                self.assertEqual(len(multiscan_result), 10)

    def test_with_offset_boundary_values_and_limit(self):
        """
            Test multiscan with offset and limit where offset values are the boundary values of int64
        """
        id_map = 0
        # Boundary values of int64
        boundary_values = [-9223372036854775809, -9223372036854775808, -9223372036854775807, 9223372036854775806,
                           9223372036854775807, 9223372036854775808]
        # Once MB-22229 is fixed change expected_docs to [10, 10, 10, 0 , 0, 10]
        expected_docs = [10, 10, 10, 10, 10, 10]

        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for boundary_value, expected_doc in zip(boundary_values, expected_docs):
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                for scan_content in scan_contents:
                    for name_inclusion in range(3):
                        for age_inclusion in range(3):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            multiscan_content = self._build_multiscan_body(scan_content, global_projection_value,
                                                                           offset=boundary_value,
                                                                           limit=10)
                            try:
                                multiscan_result = \
                                    self.rest.multiscan_for_gsi_index_with_rest(
                                        id_map["id"], json.dumps(multiscan_content))
                                log.info(multiscan_result)
                                '''
                                    Not validating with actual data as this is a special case where if the limit is boundary value of int64,
                                    total docs returned does not comply with definition of offset and limit.
                                '''
                                self.assertEqual(len(multiscan_result), expected_doc)
                            except Exception as ex:
                                log.info(str(ex))

    def test_with_offset_limit_and_with_array_indexes(self):
        """
            Test multiscan with offset and limit with array indexes
        """
        query_definition1 = QueryDefinition(index_name="array_index_travel_details",
                                            index_fields=["DISTINCT ARRAY t FOR t in TO_ARRAY(`travel_details`) END"],
                                            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                      "ANY t IN TO_ARRAY(`travel_details`) SATISFIES t.country = \"India\" END ORDER BY _id"),
                                            groups=["all", "array", "duplicate_array", "range",
                                                    "orderby", "equals"],
                                            index_where_clause=" travel_details IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": None}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition1)
                for scan_content in scan_contents:
                    multiscan_content = self._build_multiscan_body(scan_content, global_projection_value, offset=10,
                                                                   limit=10)
                    multiscan_result = \
                        self.rest.multiscan_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                    log.info(multiscan_result)
                    self.assertEqual(len(multiscan_result), 10)

    def test_with_negative_values_for_offset_and_limit(self):
        """
            Test multiscan where both offset and limit are negative values
        """
        id_map = 0
        offset_limit_values = [0, -0, -10, -1000, -100000]

        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for offset_limit_value in offset_limit_values:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                for scan_content in scan_contents:
                    for name_inclusion in range(3):
                        for age_inclusion in range(3):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            multiscan_content = self._build_multiscan_body(scan_content, global_projection_value,
                                                                           offset=offset_limit_value,
                                                                           limit=offset_limit_value)
                            multiscan_result = \
                                self.rest.multiscan_for_gsi_index_with_rest(
                                    id_map["id"], json.dumps(multiscan_content))
                            log.info(multiscan_result)
                            '''
                            Not validating with actual data as this is a special case where if the limit/offset is negative value,
                            total docs returned is equal to total no of docs in the bucket
                            '''
                            self.assertEqual(len(multiscan_result), self.docs_per_day)

    def test_pagination_with_offset_and_limit(self):
        """
            Test multiscan with offset and limit for pagination use case
        """
        id_map = 0
        scan_contents = []
        array = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for i in range(0, self.docs_per_day // 10 + 1):
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                for scan_content in scan_contents:
                    scan_content[0]["Filter"][0]["Inclusion"] = \
                        0
                    scan_content[0]["Filter"][1]["Inclusion"] = \
                        0
                    offset = i * 10
                    multiscan_content = self._build_multiscan_body(scan_content, global_projection_value, offset=offset,
                                                                   limit=10)
                    multiscan_result = \
                        self.rest.multiscan_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                    log.info(multiscan_result)
                    self._verify_items_indexed(bucket, id_map["id"], scan_content, multiscan_result, offset=offset,
                                               limit=10)
                    array.append(len(multiscan_result))
        sum_of_array = sum(array)
        self.assertEqual(sum_of_array, self.docs_per_day)

    def test_parallel_multiscan_with_offset_and_limit_using_same_index(self):
        """
            Test multiscan with offset and limit with same index
        """
        id_map = 0
        scan_contents = []
        threads = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for i in range(0, 100):
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                for scan_content in scan_contents:
                    for name_inclusion in range(3):
                        for age_inclusion in range(3):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            multiscan_content = self._build_multiscan_body(scan_content, global_projection_value,
                                                                           offset=random.randint(0, self.docs_per_day),
                                                                           limit=random.randint(0, self.docs_per_day))
                            thread = Thread(target=self.rest.multiscan_for_gsi_index_with_rest,
                                            args=[id_map["id"], json.dumps(multiscan_content)])
                            threads.append(thread)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_parallel_multiscan_with_offset_and_limit_using_different_indexes(self):
        """
            Test parallel multiscan with offset and limit with different index
        """
        id_map = 0
        id_map1 = 0
        scan_contents = []
        threads = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for bucket in self.buckets:
            if not id_map and not id_map1:
                id_map = self.create_index_using_rest(bucket, query_definition)
                id_map1 = self.create_index_using_rest(bucket, query_definition1)

        for i in range(0, 100):
            for bucket in self.buckets:
                for scan_content in scan_contents:
                    for name_inclusion in range(3):
                        for age_inclusion in range(3):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            multiscan_content = self._build_multiscan_body(scan_content, global_projection_value,
                                                                           offset=random.randint(0, self.docs_per_day),
                                                                           limit=random.randint(0, self.docs_per_day))
                            if i % 2 == 0:
                                thread = Thread(target=self.rest.multiscan_for_gsi_index_with_rest,
                                                args=[id_map["id"], json.dumps(multiscan_content)])
                            else:
                                thread = Thread(target=self.rest.multiscan_for_gsi_index_with_rest,
                                                args=[id_map1["id"], json.dumps(multiscan_content)])
                            threads.append(thread)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_with_offset_limit_and_with_distinct_true(self):
        """
            Test multiscan with offset and limit with distinct=true
        """
        query_definition1 = QueryDefinition(
            index_name="single_field_name",
            index_fields=["name"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "ORDER BY _id"),
            groups=["single_field_index"],
            index_where_clause=" name IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"}]}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition1)
            for scan_content in scan_contents:
                for name_inclusion in range(3):
                    scan_content[0]["Filter"][0]["Inclusion"] = \
                        name_inclusion
                    self._multiscan_distinct_api_helper(scan_content, offset=50, limit=100,
                                                        id_map=id_map)

    def test_with_offset_limit_and_with_distinct_true_on_primary_index(self):
        """
            Test multiscan with offset and limit with distinct=true on primary index
        """
        query_definition_pi = QueryDefinition(
            index_name="primary_index",
            index_fields=["name"],
            query_template="SELECT * FROM %s",
            groups=["full_data_set", "primary"], index_where_clause="")

        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"}]}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition_pi)
            for scan_content in scan_contents:
                for name_inclusion in range(3):
                    scan_content[0]["Filter"][0]["Inclusion"] = \
                        name_inclusion
                    self._multiscan_distinct_api_helper(scan_content, offset=10, limit=100,
                                                        id_map=id_map)

    def test_with_distinct_true_and_with_distinct_array_indexes(self):
        """
            Test multiscan with offset and limit with distinct=true on distinct array indexes
        """
        query_definition1 = QueryDefinition(index_name="array_index_travel_details_distinct",
                                            index_fields=["DISTINCT ARRAY t FOR t in TO_ARRAY(`travel_details`) END"],
                                            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                      "ANY t IN TO_ARRAY(`travel_details`) SATISFIES t.country = \"India\" END ORDER BY _id"),
                                            groups=["all", "array", "range",
                                                    "orderby", "equals"],
                                            index_where_clause=" travel_details IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": None}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition1)
                for scan_content in scan_contents:
                    self._multiscan_distinct_api_helper(scan_content, offset=0, limit=10,
                                                        id_map=id_map)

    def test_with_distinct_false_and_with_distinct_array_indexes(self):
        """
            Test multiscan with offset and limit with distinct=false on distinct array indexes
        """
        query_definition1 = QueryDefinition(index_name="array_index_travel_details_distinct",
                                            index_fields=["DISTINCT ARRAY t FOR t in TO_ARRAY(`travel_details`) END"],
                                            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                      "ANY t IN TO_ARRAY(`travel_details`) SATISFIES t.country = \"India\" END ORDER BY _id"),
                                            groups=["all", "array", "range",
                                                    "orderby", "equals"],
                                            index_where_clause=" travel_details IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": None}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition1)
                for scan_content in scan_contents:
                    self._multiscan_distinct_api_helper(scan_content, offset=0, limit=10,
                                                        id_map=id_map, distinct=False)

    def test_with_distinct_true_and_with_duplicate_array_indexes(self):
        """
            Test multiscan with offset and limit with distinct=true on duplicate array indexes
        """
        query_definition1 = QueryDefinition(index_name="array_index_travel_details_duplicate",
                                            index_fields=["ALL ARRAY t FOR t in TO_ARRAY(`travel_details`) END"],
                                            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                      "ANY t IN TO_ARRAY(`travel_details`) SATISFIES t.country = \"India\" END ORDER BY _id"),
                                            groups=["all", "array", "duplicate_array", "range",
                                                    "orderby", "equals"],
                                            index_where_clause=" travel_details IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": None}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition1)
                for scan_content in scan_contents:
                    self._multiscan_distinct_api_helper(scan_content, offset=0, limit=10,
                                                        id_map=id_map)

    def test_with_distinct_false_and_with_duplicate_array_indexes(self):
        """
            Test multiscan with offset and limit with distinct=false on duplicate array indexes
        """
        query_definition1 = QueryDefinition(index_name="array_index_travel_details_duplicate",
                                            index_fields=["ALL ARRAY t FOR t in TO_ARRAY(`travel_details`) END"],
                                            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                      "ANY t IN TO_ARRAY(`travel_details`) SATISFIES t.country = \"India\" END ORDER BY _id"),
                                            groups=["all", "array", "duplicate_array", "range",
                                                    "orderby", "equals"],
                                            index_where_clause=" travel_details IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": None}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition1)
                for scan_content in scan_contents:
                    self._multiscan_distinct_api_helper(scan_content, offset=0, limit=10,
                                                        id_map=id_map, distinct=False)

    def test_parallel_multiscan_with_offset_and_limit_distinct_true_using_same_index(self):
        """
            Test parallel multiscan with offset and limit with distinct=true using same index
        """
        id_map = 0
        scan_contents = []
        threads = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for i in range(0, 100):
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition)
                for scan_content in scan_contents:
                    for name_inclusion in range(3):
                        for age_inclusion in range(3):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            multiscan_content = self._build_multiscan_body(scan_content, global_projection_value,
                                                                           offset=random.randint(0, self.docs_per_day),
                                                                           limit=random.randint(0, self.docs_per_day),
                                                                           distinct=True)
                            thread = Thread(target=self.rest.multiscan_for_gsi_index_with_rest,
                                            args=[id_map["id"], json.dumps(multiscan_content)])
                            threads.append(thread)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_parallel_multiscan_with_offset_and_limit_distinct_true_using_different_indexes(self):
        """
            Test parallel multiscan with offset and limit with distinct=true using different index
        """
        id_map = 0
        id_map1 = 0
        scan_contents = []
        threads = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"},
                                          {"Low": 0, "High": 100}]}])
        for bucket in self.buckets:
            if not id_map and not id_map1:
                id_map = self.create_index_using_rest(bucket, query_definition)
                id_map1 = self.create_index_using_rest(bucket, query_definition1)

        for i in range(0, 100):
            for bucket in self.buckets:
                for scan_content in scan_contents:
                    for name_inclusion in range(3):
                        for age_inclusion in range(3):
                            scan_content[0]["Filter"][0]["Inclusion"] = \
                                name_inclusion
                            scan_content[0]["Filter"][1]["Inclusion"] = \
                                age_inclusion
                            multiscan_content = self._build_multiscan_body(scan_content, global_projection_value,
                                                                           offset=random.randint(0, self.docs_per_day),
                                                                           limit=random.randint(0, self.docs_per_day),
                                                                           distinct=True)
                            if i % 2 == 0:
                                thread = Thread(target=self.rest.multiscan_for_gsi_index_with_rest,
                                                args=[id_map["id"], json.dumps(multiscan_content)])
                            else:
                                thread = Thread(target=self.rest.multiscan_for_gsi_index_with_rest,
                                                args=[id_map1["id"], json.dumps(multiscan_content)])
                            threads.append(thread)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_with_offset_limit_and_with_distinct_true_on_composite_index(self):
        """
            Test multiscan with offset and limit with distinct=true on composite index
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"}]}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition2)
            for scan_content in scan_contents:
                self._multiscan_distinct_api_helper(scan_content, offset=0, limit=100, id_map=id_map)

    def test_with_offset_limit_and_with_composite_index_on_large_dataset(self):
        """
            Test parallel multiscan with offset and limit on large dataset
        """
        if self.docs_per_day < 1000000:
            log.error("Atleast 1M docs is required for this test to make sense!!")
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append([{"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"}]}])
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition2)
            for scan_content in scan_contents:
                self._multiscan_distinct_api_helper(scan_content, offset=self.docs_per_day - 1000, limit=1000,
                                                    id_map=id_map, distinct=False)

    def test_with_distinct_true_with_multiple_filters_with_non_overlapping_data(self):
        """
            Test parallel multiscan with offset and limit on non overlapping data
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "C"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "D", "High": "F"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "M", "High": "Q"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "S", "High": "V"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "X", "High": "Z"}]})

        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition2)
            self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map)

    def test_with_distinct_true_with_multiple_filters_with_overlapping_data(self):
        """
            Test parallel multiscan with offset and limit on overlapping data
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "C"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "B", "High": "F"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "C", "High": "Q"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "D", "High": "V"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "E", "High": "Z"}]})

        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition2)
            self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map)

    def test_offset_limit_and_distinct_with_binary_data(self):
        """
            Test offset,limit and distinct with binary data
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "M"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "N", "High": "Z"}]})
        cluster = Cluster()
        gen_load = BlobGenerator('binary', 'binary-', self.value_size, end=100)
        rc = cluster.load_gen_docs(self.servers[0], self.buckets[0].name, gen_load,
                                   self.buckets[0].kvs[1], "create", exp=0, flag=0, batch_size=1000,
                                   compression=self.sdk_compression)
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition2)
            self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map)

    def test_distinct_with_lookup_api(self):
        limits = [1, 10, 25, 67, 99, 100]
        id_map = 0
        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition2)
                # multiscan_content = {"equal": "[\"Ebony\"]", "distinct":True, "limit": 100, "stale": False }
            multiscan_content = {"equal": "[\'Netherlands\']"}
            multiscan_result = \
                self.rest.lookup_gsi_index_with_rest(
                    id_map["id"], multiscan_content)
            log.info(multiscan_result)

    def test_offset_limit_and_distinct_with_projection_with_primarykey(self):
        """
            Test offset,limit , distinct and primary key with different values
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "M"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "N", "High": "Z"}]})

        projections = [{"EntryKeys": [0], "PrimaryKey": True}, {"EntryKeys": [0, 1], "PrimaryKey": True},
                       {"EntryKeys": [0, 1, 2], "PrimaryKey": True}, {"EntryKeys": [0, 1, 2, 3], "PrimaryKey": True},
                       {"EntryKeys": [0], "PrimaryKey": False}, {"EntryKeys": [0, 1], "PrimaryKey": False},
                       {"EntryKeys": [0, 1, 2], "PrimaryKey": False}, {"EntryKeys": [0, 1, 2, 3], "PrimaryKey": False}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map, projection=pr,
                                                    distinct=False, verify=True)

    def test_offset_limit_and_distinct_with_projection_of_all_values(self):
        """
            Test offset,limit and distinct with binary data
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "M"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "N", "High": "Z"}]})

        projection = {"EntryKeys": [0, 1, 2, 3], "PrimaryKey": True}

        for bucket in self.buckets:
            if not id_map:
                id_map = self.create_index_using_rest(bucket, query_definition2)
            self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map,
                                                projection=projection)

    def test_offset_limit_and_projection_with_number_entry_keys_more_than_indexed_fields(self):
        """
            Test offset,limit and distinct with negative scenarios
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "M"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "N", "High": "Z"}]})

        projections = [{"EntryKeys": [0, 1, 2, 3, 4], "PrimaryKey": True},
                       {"EntryKeys": [0, 1, 2, 3, 4, 5, 6], "PrimaryKey": False}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                result = self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map,
                                                             projection=pr, verify=False)
                self.assertTrue("Invalid number of Entry Keys" in json.dumps(result))

    def test_offset_limit_and_projection_with_entry_indices_more_than_indexed_fields(self):
        """
            Test offset,limit and distinct with negative scenarios
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "M"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "N", "High": "Z"}]})

        projections = [{"EntryKeys": [7, 8, 9], "PrimaryKey": True},
                       {"EntryKeys": [1000, 20000, 30000000], "PrimaryKey": False},
                       {"EntryKeys": [140000], "PrimaryKey": True},
                       {"EntryKeys": [-1], "PrimaryKey": True},
                       {"EntryKeys": [-10, -20], "PrimaryKey": True}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                result = self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map,
                                                             projection=pr, verify=False)
                self.assertTrue("Invalid Entry Key" in json.dumps(result))

    def test_offset_limit_and_projection_with_duplicate_entry_keys(self):
        """
            Test offset,limit and distinct with duplicate entry keys
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "M"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "N", "High": "Z"}]})

        projections = [{"EntryKeys": [0, 1, 0], "PrimaryKey": True},
                       {"EntryKeys": [1, 1, 2], "PrimaryKey": True},
                       {"EntryKeys": [1, 2, 3, 3], "PrimaryKey": False},
                       {"EntryKeys": [1, 1, 1, 1], "PrimaryKey": False}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                # Validations could fail
                # See MB-22724 for more details
                self._multiscan_distinct_api_helper(scan_contents, offset=0, limit=100, id_map=id_map,
                                                    projection=pr, verify=False)

    def test_offset_limit_and_projection_with_indexed_values_of_large_size(self):
        """
            Test offset,limit and distinct with large indexed values
        """
        query_definition2 = QueryDefinition(
            index_name="multiple_field_index",
            index_fields=["name", "age", "email", "premium_customer", "travel_history_code", "countries_visited",
                          "mutated", "credit_cards", "secret_combination", "address", "question_values",
                          "travel_details", "booking", "travel_history"],
            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                      "name > \"Adara\" AND "
                                                      "name < \"Winta\" "
                                                      "AND age > 0 AND age "
                                                      "< 100 ORDER BY _id"),
            groups=["multiple_field_index"],
            index_where_clause=" name IS NOT NULL ")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "A", "High": "M"}]})
        scan_contents.append({"Seek": None,
                              "Filter": [{"Low": "N", "High": "Z"}]})

        projections = [{"EntryKeys": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], "PrimaryKey": True},
                       # See MB-22724 for more details
                       # {"EntryKeys": [1, 2, 0, 4, 5, 3, 13, 11, 12, 9, 7, 8, 6, 10], "PrimaryKey": False}
                      ]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                self._multiscan_distinct_api_helper(scan_contents, offset=0, distinct=False, limit=100, id_map=id_map,
                                                    projection=pr)

    def test_with_offset_limit_projection_and_with_distinct_array_indexes(self):
        """
            Test multiscan with offset and limit with distinct array indexes
        """
        query_definition2 = QueryDefinition(index_name="array_index_travel_details",
                                            index_fields=["DISTINCT ARRAY t FOR t in TO_ARRAY(`travel_details`) END"],
                                            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                      "ANY t IN TO_ARRAY(`travel_details`) SATISFIES t.country = \"India\" END ORDER BY _id"),
                                            groups=["all", "array", "duplicate_array", "range",
                                                    "orderby", "equals"],
                                            index_where_clause=" travel_details IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                               "Filter": None})
        projections = [{"EntryKeys": [], "PrimaryKey": True}, {"EntryKeys": [0], "PrimaryKey": True},
                       {"EntryKeys": [0], "PrimaryKey": False}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                self._multiscan_distinct_api_helper(scan_contents, offset=0, distinct=False, limit=100, id_map=id_map,
                                                    projection=pr)

    def test_with_offset_limit_projection_and_with_duplicate_array_indexes(self):
        """
            Test multiscan with offset and limit with duplicate array indexes
        """
        query_definition2 = QueryDefinition(index_name="array_index_travel_details",
                                            index_fields=["ALL ARRAY t FOR t in TO_ARRAY(`travel_details`) END"],
                                            query_template=RANGE_SCAN_TEMPLATE.format(emit_fields, " %s " %
                                                                                      "ANY t IN TO_ARRAY(`travel_details`) SATISFIES t.country = \"India\" END ORDER BY _id"),
                                            groups=["all", "array", "duplicate_array", "range",
                                                    "orderby", "equals"],
                                            index_where_clause=" travel_details IS NOT NULL ")
        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                               "Filter": None})
        projections = [{"EntryKeys": [], "PrimaryKey": True}, {"EntryKeys": [0], "PrimaryKey": True},
                       {"EntryKeys": [0], "PrimaryKey": False}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                self._multiscan_distinct_api_helper(scan_contents, offset=0, distinct=False, limit=100, id_map=id_map,
                                                    projection=pr)

    def test_with_offset_limit_projection_and_with_primary_index(self):
        """
            Test multiscan with offset and limit with primary index
        """
        query_definition2 = QueryDefinition(
            index_name="primary_index",
            index_fields=["name"],
            query_template="SELECT * FROM %s",
            groups=["full_data_set", "primary"], index_where_clause="")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"}]})
        projections = [{"EntryKeys": [], "PrimaryKey": True},
                       {"EntryKeys": [], "PrimaryKey": False},
                       {"EntryKeys": [0], "PrimaryKey": True},
                       {"EntryKeys": [0], "PrimaryKey": False},
                       {"PrimaryKey": True},
                       {"PrimaryKey": False}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                self._multiscan_distinct_api_helper(scan_contents, offset=0, distinct=False, limit=100,
                                                    id_map=id_map, projection=pr)

    def test_with_offset_limit_projection_and_with_primary_index_with_distinct_true(self):
        """
            Test multiscan with offset and limit with primary index and distinct=True
        """
        query_definition2 = QueryDefinition(
            index_name="primary_index",
            index_fields=["name"],
            query_template="SELECT * FROM %s",
            groups=["full_data_set", "primary"], index_where_clause="")

        id_map = 0
        scan_contents = []
        scan_contents.append({"Seek": None,
                               "Filter": [{"Low": "A", "High": "Z"}]})
        projections = [{"EntryKeys": [], "PrimaryKey": True},
                       {"EntryKeys": [], "PrimaryKey": False},
                       {"PrimaryKey": True},
                       {"PrimaryKey": False}]

        for pr in projections:
            for bucket in self.buckets:
                if not id_map:
                    id_map = self.create_index_using_rest(bucket, query_definition2)
                self._multiscan_distinct_api_helper(scan_contents, offset=0, distinct=True, limit=100,
                                                    id_map=id_map, projection=pr)

    def _create_composite_index_query_definitions(self):
        definitions_list = []
        emit_fields = "*"
        self.assertEqual(self.dataset, "array", "Use Array as dataset")
        index_name_prefix = "airlines_" + str(random.randint(100000, 999999))
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + "_name_age",
                            index_fields=["name", "age"],
                            query_template=RANGE_SCAN_TEMPLATE.format(
                                emit_fields, " %s " %
                                             "name > \"Adara\" AND name < \"Winta\" "
                                             "AND age > 40 AND age < 50 ORDER BY _id"),
                            groups=["two_field_index"],
                            index_where_clause=" travel_history IS NOT NULL "))
        return definitions_list

    def _build_multiscan_body(self, scan_content, projection_val, distinct=False, reverse=False, offset=0, limit=100,
                              stale="false"):
        multiscan_content = {}
        multiscan_content["scans"] = json.dumps(scan_content)
        if projection_val is not None:
            multiscan_content["projection"] = json.dumps(projection_val)
        multiscan_content["distinct"] = distinct
        multiscan_content["reverse"] = reverse
        multiscan_content["offset"] = offset
        multiscan_content["limit"] = limit
        multiscan_content["stale"] = stale
        if offset is None:
            try:
                del multiscan_content["offset"]
            except KeyError:
                pass
        if limit is None:
            try:
                del multiscan_content["limit"]
            except KeyError:
                pass
        return multiscan_content

    def _verify_items_indexed(self, bucket, index_id, scan_content, multiscan_result, offset=0, limit=100,
                              distinct=False):
        body = {"stale": "False"}
        doc_list = []
        multiscan_doc_list = []
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
                temp_doc_list = []
                for doc in full_table_scan:
                    doc_name = doc["key"][0]
                    if name_inclusion == 0:
                        if doc_name > name_low and doc_name < name_high:
                            temp_doc_list.append(doc_name)
                    if name_inclusion == 1:
                        if doc_name >= name_low and doc_name < name_high:
                            temp_doc_list.append(doc_name)
                    if name_inclusion == 2:
                        if doc_name > name_low and doc_name <= name_high:
                            temp_doc_list.append(doc_name)
                    if name_inclusion == 3:
                        if doc_name >= name_low and doc_name <= name_high:
                            temp_doc_list.append(doc_name)
        if distinct:
            temp_doc_list_distinct = sorted(list(set(temp_doc_list)))
        else:
            temp_doc_list_distinct = sorted(temp_doc_list)
        if offset is None:
            offset = 0
        if limit is None:
            limit = 100
        final_index = limit + offset
        array_limit_offset = temp_doc_list_distinct[offset:final_index]
        log.info(len(array_limit_offset))
        log.info(len(multiscan_result))
        self.assertEqual(len(array_limit_offset), len(multiscan_result), "No. of items mismatch")
        msg = "The number of rows match but the results mismatch, please check"
        for ms in multiscan_result:
            multiscan_doc_list.append(ms["key"][0])
        sort = sorted(array_limit_offset)
        log.info("****")
        log.info(sort)
        log.info(multiscan_doc_list)
        if sort != multiscan_doc_list:
            raise Exception(msg)

    def _verify_distinct_items(self, total_result, multiscan_result, offset=0, limit=100, distinct=False,
                               projection=global_projection_value):
        temp_doc_list = []
        ms_doc_list = []
        for tr in total_result:
            if projection is not None and projection["PrimaryKey"] is False and tr["docid"] != "":
                assert False
            if projection is not None and projection["PrimaryKey"] is True and tr["docid"] == "":
                assert False
            temp_doc_val = str(tr["key"])
            " ".join(temp_doc_val)
            temp_doc_list.append(temp_doc_val)

        for ms in multiscan_result:
            if projection is not None and projection["PrimaryKey"] is False and ms["docid"] != "":
                assert False
            if projection is not None and projection["PrimaryKey"] is True and ms["docid"] == "":
                assert False
            ms_doc_val = str(ms["key"])
            " ".join(ms_doc_val)
            ms_doc_list.append(ms_doc_val)

        if distinct:
            temp_doc_list_distinct = sorted(list(set(temp_doc_list)))
        else:
            temp_doc_list_distinct = sorted(temp_doc_list)
        if limit is not None:
            final_index = limit + offset
        else:
            final_index = None
        array_limit_offset = temp_doc_list_distinct[offset:final_index]
        log.info(array_limit_offset)
        log.info(sorted(ms_doc_list))
        self.assertEqual(len(array_limit_offset), len(ms_doc_list), "No. of items mismatch")
        msg = "The number of rows match but the results mismatch, please check"
        if sorted(array_limit_offset) != sorted(ms_doc_list):
            raise Exception(msg)

    def _multiscan_api_helper(self, scan_contents, bucket, offset, limit, id_map):
        for scan_content in scan_contents:
            for name_inclusion in range(3):
                for age_inclusion in range(3):
                    scan_content[0]["Filter"][0]["Inclusion"] = \
                        name_inclusion
                    scan_content[0]["Filter"][1]["Inclusion"] = \
                        age_inclusion
                    multiscan_content = self._build_multiscan_body(scan_content, global_projection_value, offset=offset,
                                                                   limit=limit)
                    multiscan_result = \
                        self.rest.multiscan_for_gsi_index_with_rest(
                            id_map["id"], json.dumps(multiscan_content))
                    log.info(multiscan_result)
                    self._verify_items_indexed(bucket, id_map["id"], scan_content, multiscan_result, offset=offset,
                                               limit=limit)
                    return multiscan_result

    def _multiscan_distinct_api_helper(self, scan_contents, offset=0, limit=100, id_map=0, distinct=True,
                                       projection=global_projection_value, verify=True):
        multiscan_content = self._build_multiscan_body(scan_contents, projection, offset=0, limit=0,
                                                       distinct=False)
        multiscan_result = \
            self.rest.multiscan_for_gsi_index_with_rest(
                id_map["id"], json.dumps(multiscan_content))

        multiscan_content1 = self._build_multiscan_body(scan_contents, projection, offset=offset,
                                                        limit=limit, distinct=distinct)
        multiscan_result1 = \
            self.rest.multiscan_for_gsi_index_with_rest(
                id_map["id"], json.dumps(multiscan_content1))
        log.info(multiscan_result1)

        if verify:
            self._verify_distinct_items(multiscan_result, multiscan_result1, offset=offset, limit=limit,
                                        distinct=distinct, projection=projection)
        else:
            return multiscan_result1
