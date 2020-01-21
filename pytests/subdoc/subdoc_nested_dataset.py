from .subdoc_base import SubdocBaseTest
import copy, json
import yaml
import random
import traceback

import couchbase.subdocument as SD
from sdk_client import SDKClient
from threading import Thread

class SubdocNestedDataset(SubdocBaseTest):
    def setUp(self):
        super(SubdocNestedDataset, self).setUp()
        self.nesting_level = self.input.param("nesting_level", 0)
        self.client = self.direct_client(self.master, self.buckets[0])
        self.is_sdk_client = True if self.client.__class__.__name__ == 'SDKClient' else False

    def tearDown(self):
        super(SubdocNestedDataset, self).tearDown()

# SANITY TESTS
    def test_sanity(self):
        result = {}
        self.run_testcase(self.test_counter, "test_counter", result)
        self.run_testcase(self.test_get_array_strings, "test_get_array_strings", result)
        self.run_testcase(self.test_add_numbers, "test_add_numbers", result)
        self.run_testcase(self.test_delete_json_array, "test_delete_json_array", result)
        self.run_testcase(self.test_upsert_numbers, "test_upsert_numbers", result)
        self.run_testcase(self.test_replace_boolean, "test_replace_boolean", result)
        self.run_testcase(self.test_add_insert_array, "test_add_insert_array", result)
        self.run_testcase(self.test_add_last_array, "test_add_last_array", result)
        self.run_testcase(self.test_add_first_array, "test_add_first_array", result)
        self.run_testcase(self.test_add_unique_array, "test_add_unique_array", result)
        self.run_testcase(self.test_replace_numbers, "test_replace_numbers", result)
        self.assertTrue(len(result) == 0, result)

    #SD_COUNTER
    def test_counter(self):
        result = True
        dict = {}
        self.key = "test_counter"
        array = {
            'i_add': 0,
            'i_sub': 1,
            'a_i_a': [0, 1],
            'ai_sub': [0, 1]
        }
        expected_array = {
            "i_add": 1,
            "i_sub": 0,
            "a_i_a": [1, 1],
            "ai_sub": [0, 0]
        }
        try:
          base_json = self.generate_json_for_nesting()
          nested_json = self.generate_nested(base_json, array, self.nesting_level)
          if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
          self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
          self.counter(self.client, key=self.key, path='i_add', value="1",
                     xattr=self.xattr, create_parents=self.create_parents)
          self.counter(self.client, key=self.key, path='i_sub', value="-1",
                     xattr=self.xattr, create_parents=self.create_parents)
          self.counter(self.client, key=self.key, path='a_i_a[0]', value="1",
                     xattr=self.xattr, create_parents=self.create_parents)
          self.counter(self.client, key=self.key, path='ai_sub[1]', value="-1",
                     xattr=self.xattr, create_parents=self.create_parents)
          self.json = expected_array
          for key in expected_array.keys():
            logic, data_return = self.get_string_and_verify_return(
                self.client, key=self.key, path = key, expected_value = expected_array[key], xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        except Exception as e:
          traceback.print_exc()

        self.assertTrue(result, dict)

# SD_GET
    def test_get_null(self):
        self.json = self.generate_simple_data_null()
        self.get_verify(self.json, "simple_dataset_null")

    def test_get_array_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.get_verify(self.json, "test_get_array_numbers")

    def test_get_array_strings(self):
        self.json = self.generate_simple_data_strings()
        self.get_verify(self.json, "test_get_array_strings")

    def test_get_mix_arrays(self):
        self.json = self.generate_simple_data_mix_arrays()
        self.get_verify(self.json, "test_get_mix_arrays")

    def test_get_numbers_boundary(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.get_verify(self.json, "test_get_numbers_boundary")

    def test_get_element_arrays(self):
        result = True
        dict = {}
        self.key = "element_arrays"
        data_set = self.generate_simple_arrays()
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        self.log.info(nested_json)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        key_single_dimension_path = "1_d_a[0]"
        logic, msg = self.get_and_verify_with_value(
            self.client, key = self.key, path = key_single_dimension_path, expected_value = str(data_set["1_d_a"][0]), xattr=self.xattr)
        if not logic:
            dict[key_single_dimension_path] = msg
        result = result and logic
        key_two_dimension_path = "2_d_a[0][0]"
        logic, msg = self.get_and_verify_with_value(self.client, key = self.key, path = key_two_dimension_path, expected_value = str(data_set["2_d_a"][0][0]), xattr=self.xattr)
        result = result and logic
        if not logic:
            dict[key_two_dimension_path] = msg
        self.assertTrue(result, dict)

    def test_get_json_and_arrays(self):
        result = True
        dict = {}
        self.key = "element_array"
        expected_value = "value"
        data_set = {
                        "1_d_a": [ { "field": [expected_value] }],
                        "2_d_a": [ [{ "field": [expected_value] }]],
                        "3_d_a": [ [[{ "field": [expected_value] }]]],
                    }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        key_single_dimension_path = "1_d_a[0].field[0]"
        logic, msg = self.get_and_verify_with_value(
            self.client, key = self.key, path = key_single_dimension_path, expected_value = expected_value, xattr=self.xattr)
        if not logic:
            dict[key_single_dimension_path] = msg
        result = result and logic
        key_two_dimension_path = "2_d_a[0][0].field[0]"
        logic, msg = self.get_and_verify_with_value(
            self.client, key = self.key, path = key_two_dimension_path, expected_value = expected_value, xattr=self.xattr)
        result = result and logic
        if not logic:
            dict[key_two_dimension_path] = msg
        self.assertTrue(result, dict)
        key_three_dimension_path = "3_d_a[0][0][0].field[0]"
        logic, msg = self.get_and_verify_with_value(
            self.client, key = self.key, path = key_three_dimension_path, expected_value = expected_value, xattr=self.xattr)
        result = result and logic
        if not logic:
            dict[key_three_dimension_path] = msg
        self.assertTrue(result, dict)

# SD_EXISTS
    def test_exists_json_fields(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.exists(self.json, "test_exists_json_fields")

    def test_exists_element_array(self):
        result = True
        dict = {}
        self.key = "test_exists_element_array"
        data_set = self.generate_simple_arrays()
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        key_single_dimension_path = "1_d_a[0]"
        logic, msg = self.verify_exists(self.client, key = self.key, path = key_single_dimension_path)
        if not logic:
            dict[key_single_dimension_path] = msg
        result = result and logic
        key_two_dimension_path = "2_d_a[0][0]"
        logic, msg = self.verify_exists(self.client, key = self.key, path = key_two_dimension_path)
        result = result and logic
        if not logic:
            dict[key_two_dimension_path] = msg
        self.assertTrue(result, dict)

    def test_exists_json_and_arrays(self):
        result = True
        dict = {}
        self.key = "element_array"
        expected_value = "value"
        data_set =  {
                        "1_a": [ { "field": [expected_value] }],
                        "2_a": [ [{ "field": [expected_value] }]],
                        "3_a": [ [[{ "field": [expected_value] }]]],
                    }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        key_single_dimension_path = "1_a[0].field[0]"
        logic, msg = self.verify_exists(self.client, key = self.key, path = key_single_dimension_path)
        if not logic:
            dict[key_single_dimension_path] = msg
        result = result and logic
        key_two_dimension_path = "2_a[0][0].field[0]"
        logic, msg = self.verify_exists(self.client, key = self.key, path = key_two_dimension_path)
        result = result and logic
        if not logic:
            dict[key_two_dimension_path] = msg
        self.assertTrue(result, dict)
        key_three_dimension_path = "3_a[0][0][0].field[0]"
        logic, msg = self.verify_exists(self.client, key = self.key, path = key_three_dimension_path)
        result = result and logic
        if not logic:
            dict['key_three_dimension_path'] = msg
        self.assertTrue(result, dict)


# SD_ARRAY_ADD
    def test_add_last_array(self):
        result = True
        dict = {}
        self.key = "test_add_last_array"
        array = {
                    "s_e":[],
                    "1d_a":["0"],
                    "2_d_a":[["0"]],
                    "3d_a":[[["0"]]],
                    "4d_a":[[[{"field":["0"]}]]]
                }
        expected_array = {
                    "s_e":["1"],
                    "1d_a":["0", "1"],
                    "2_d_a":[["0", "1"]],
                    "3d_a":[[["0", "1"]]],
                    "4d_a":[[[{"field":["0", "1"]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        self.array_add_last(self.client, key=self.key, path='s_e', value="1",
                            xattr=self.xattr, create_parents=True)
        self.array_add_last(self.client, key=self.key, path='1d_a', value="1",
                            xattr=self.xattr, create_parents=self.create_parents)
        self.array_add_last(self.client, key=self.key, path='2_d_a[0]', value="1",
                            xattr=self.xattr, create_parents=self.create_parents)
        self.array_add_last(self.client, key=self.key, path='3d_a[0][0]', value="1",
                            xattr=self.xattr, create_parents=self.create_parents)
        self.array_add_last(self.client, key=self.key, path='4d_a[0][0][0].field',
                            value="1", xattr=self.xattr, create_parents=self.create_parents)
        self.json = expected_array
        for key in list(expected_array.keys()):
            value = expected_array[key]
            logic, data_return = self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = value, xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_first_array(self):
        result = True
        dict = {}
        self.key = "test_add_first_array"
        array = {
                    "s_e": [],
                    "1_d_a": ["1"],
                    "2_d_a": [["1"]],
                    "3_d_a": [[["1"]]],
                    "4_d_a": [[[{"field":["1"]}]]]
                }
        expected_array = {
                    "s_e": ["0"],
                    "1_d_a": ["0", "1"],
                    "2_d_a": [["0", "1"]],
                    "3_d_a": [[["0", "1"]]],
                    "4_d_a": [[[{"field":["0", "1"]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        self.array_add_first(self.client, key = self.key, path = 's_e', value = "0", xattr=self.xattr, create_parents=True)
        self.array_add_first(self.client, key = self.key, path = '1_d_a', value = "0", xattr=self.xattr, create_parents=True)
        self.array_add_first(self.client, key = self.key, path = '2_d_a[0]', value = "0", xattr=self.xattr, create_parents=True)
        self.array_add_first(self.client, key = self.key, path = '3_d_a[0][0]', value = "0", xattr=self.xattr, create_parents=True)
        self.array_add_first(self.client, key = self.key, path = '4_d_a[0][0][0].field', value =  "0", xattr=self.xattr, create_parents=True)
        for key in list(expected_array.keys()):
            value = expected_array[key]
            logic, data_return = self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = value, xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_unique_array(self):
        result = True
        dict = {}
        self.key = "test_add_unique_array"
        array = {
                    "s_e":[],
                    "1_d_a": ["0", 2],
                    "2_d_a": [["0", 2]],
                    "3_d_a": [[["0", 2]]],
                    "4_d_a": [[[{"field":["0", 2]}]]]
                }
        expected_array = {
                    "s_e":["1"],
                    "1_d_a": ["0", 2, "1"],
                    "2_d_a": [["0", 2, "1"]],
                    "3_d_a": [[["0", 2, "1"]]],
                    "4_d_a": [[[{"field":["0", 2, "1"]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 's_e', value = "1", xattr=self.xattr, create_parents=True)
        self.array_add_unique(self.client, key = self.key, path = '1_d_a', value =  "1", xattr=self.xattr, create_parents=True)
        self.array_add_unique(self.client, key = self.key, path = '2_d_a[0]', value =  "1", xattr=self.xattr, create_parents=True)
        self.array_add_unique(self.client, key = self.key, path = '3_d_a[0][0]', value =  "1", xattr=self.xattr, create_parents=True)
        self.array_add_unique(self.client, key = self.key, path = '4_d_a[0][0][0].field', value = "1", xattr=self.xattr, create_parents=True)
        for key in list(expected_array.keys()):
            expected_value = expected_array[key]
            logic, data_return = self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = expected_value, xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": expected_value, "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_insert_array(self):
        result = True
        dict = {}
        self.key = "test_add_insert_array"
        array = {
                    "1_d_a":[],
                    "2_d_a":[[]],
                    "3_d_a":[[[]]],
                    "3_d_j":[[[{"field":[]}]]]
                }
        expected_array = {
                    "1_d_a":[0, 1, 2, 3],
                    "2_d_a":[[0, 1, 2, 3], [0, 1, 2, 3]],
                    "3_d_a":[[[0, 1, 2, 3], [0, 1, 2, 3]], [0, 1, 2, 3]],
                    "3_d_j":[[[{"field":[0, 1, 2, 3]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="1_d_a[0]",
                              value=1, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="1_d_a[0]",
                              value=0, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="1_d_a[2]",
                              value=3, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="1_d_a[2]",
                              value=2, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="2_d_a[0][0]",
                              value=1, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="2_d_a[0][0]",
                              value=0, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="2_d_a[0][2]",
                              value=3, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="2_d_a[0][2]",
                              value=2, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="2_d_a[1]",
                              value=[0, 1, 2, 3], xattr=self.xattr)
        self.array_add_insert(self.client, self.key,
                              path="3_d_a[0][0][0]", value=1, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key,
                              path="3_d_a[0][0][0]", value=0, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key,
                              path="3_d_a[0][0][2]", value=3, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key,
                              path="3_d_a[0][0][2]", value=2, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="3_d_a[0][1]",
                              value=[0, 1, 2, 3], xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key, path="3_d_a[1]",
                              value=[0, 1, 2, 3], xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key,
                              path="3_d_j[0][0][0].field[0]", value=0, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key,
                              path="3_d_j[0][0][0].field[1]", value=1, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key,
                              path="3_d_j[0][0][0].field[2]", value=2, xattr=self.xattr)
        self.array_add_insert(self.client, key=self.key,
                              path="3_d_j[0][0][0].field[3]", value=3, xattr=self.xattr)
        for key in list(expected_array.keys()):
            expected_value = expected_array[key]
            logic, actual_value = self.get_string_and_verify_return(self.client, key=self.key, path=key,
                                                                   expected_value=expected_value, xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": expected_value, "actual": actual_value}
            result = result and logic
        self.assertTrue(result, dict)

# Test some concurrent operations as seen in MB-21597
    def test_add_concurrent(self):
        DOCID = 'subdoc_doc_id'
        SERVER_IP = self.servers[0].ip
        ITERATIONS = 200
        THREADS = 20

        main_bucket = SDKClient(scheme="couchbase", hosts=[self.servers[0].ip],
                  bucket='default').cb
        main_bucket.upsert(DOCID, {'recs':[]})

        class Runner(Thread):
            def run(self, *args, **kw):
                cb = SDKClient(scheme="couchbase", hosts=[SERVER_IP],
                  bucket='default').cb
                for x in range(ITERATIONS):
                    cb.mutate_in(DOCID, SD.array_append('recs', 1))

        thrs = [Runner() for x in range(THREADS)]
        [t.start() for t in thrs]
        [t.join() for t in thrs]

        obj = main_bucket.get(DOCID)

        array_entry_count = len(obj.value['recs'])

        self.assertTrue(array_entry_count == ITERATIONS * THREADS,
                         'Incorrect number of array entries. Expected {0} actual {1}'.format(ITERATIONS * THREADS,
                                                                           array_entry_count))

# SD_ADD
    def test_add_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_add_verify(self.json, "test_add_numbers")

    def test_add_array_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_add_verify(self.json, "test_add_array_of_numbers")

    def test_add_numbers_boundary(self):
        self.json = self.generate_simple_data_numbers()
        self.dict_add_verify(self.json, "test_add_numbers_boundary")

    def test_add_strings(self):
        self.json = self.generate_simple_data_strings()
        self.dict_add_verify(self.json, "test_add_string")

    def test_add_array_strings(self):
        self.json = self.generate_simple_data_array_strings()
        self.dict_add_verify(self.json, "test_add_array_strings")

    def test_add_null(self):
        self.json = self.generate_simple_data_null()
        self.dict_add_verify(self.json, "test_add_null")

    def test_add_boolean(self):
        self.json = self.generate_simple_data_boolean()
        self.dict_add_verify(self.json, "test_add_boolean")

    def test_add_array_mix(self):
        self.json = self.generate_simple_data_mix_arrays()
        self.dict_add_verify(self.json, "test_add_array_mix")

# SD_UPSERT - Add Operations
    def test_upsert_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_numbers")

    def test_upsert_array_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_array_of_numbers")

    def test_upsert_numbers_boundary(self):
        self.json = self.generate_simple_data_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_numbers_boundary")

    def test_upsert_strings(self):
        self.json = self.generate_simple_data_strings()
        self.dict_upsert_verify(self.json, "test_upsert_string")

    def test_upsert_array_strings(self):
        self.json = self.generate_simple_data_array_strings()
        self.dict_upsert_verify(self.json, "test_upsert_array_strings")

    def test_upsert_null(self):
        self.json = self.generate_simple_data_null()
        self.dict_upsert_verify(self.json, "test_upsert_null")

    def test_upsert_boolean(self):
        self.json = self.generate_simple_data_boolean()
        self.dict_upsert_verify(self.json, "test_upsert_boolean")

    def test_upsert_array_mix(self):
        self.json = self.generate_simple_data_mix_arrays()
        self.dict_upsert_verify(self.json, "test_upsert_array_mix")

# SD_UPSERT - Replace Operations
    def test_upsert_replace_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_numbers")

    def test_upsert_replace_array_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_of_numbers")

    def test_upsert_replace_numbers_boundary(self):
        self.json = self.generate_simple_data_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_numbers_boundary")

    def test_upsert_replace_strings(self):
        self.json = self.generate_simple_data_strings()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_string")

    def test_upsert_replace_array_strings(self):
        self.json = self.generate_simple_data_array_strings()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_strings")

    def test_upsert_replace_null(self):
        self.json = self.generate_simple_data_null()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_null")

    def test_upsert_replace_boolean(self):
        self.json = self.generate_simple_data_boolean()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_boolean")

    def test_upsert_replace_array_mix(self):
        self.json = self.generate_simple_data_mix_arrays()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_mix")

# SD_REPLACE - Replace Operations
    def test_replace_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_replace_verify(self.json, "test_replace_numbers")

    def test_replace_array_numbers(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_replace_verify(self.json, "test_replace_array_of_numbers")

    def test_replace_numbers_boundary(self):
        self.json = self.generate_simple_data_numbers()
        self.dict_replace_verify(self.json, "test_replace_numbers_boundary")

    def test_replace_strings(self):
        self.json = self.generate_simple_data_strings()
        self.dict_replace_verify(self.json, "test_replace_string")

    def test_replace_array_strings(self):
        self.json = self.generate_simple_data_array_strings()
        self.dict_replace_verify(self.json, "test_replace_array_strings")

    def test_replace_null(self):
        self.json = self.generate_simple_data_null()
        self.dict_replace_verify(self.json, "test_replace_null")

    def test_replace_boolean(self):
        self.json = self.generate_simple_data_boolean()
        self.dict_replace_verify(self.json, "test_replace_boolean")

    def test_replace_array_mix(self):
        self.json = self.generate_simple_data_mix_arrays()
        self.dict_replace_verify(self.json, "test_replace_array_mix")

# SD_DELETE
    def test_delete_dict(self):
        self.json = self.generate_simple_data_array_of_numbers()
        self.dict_delete_verify(self.json, "test_delete_array")

    def test_delete_array(self):
        result = True
        self.key = "test_delete_array"
        array = {
                    "nums":[1, 2, 3],
                    "strs":["absde", "dddl", "dkdkd"],
                    "2_d_a":[["0", "1"]],
                    "3_d_a":[[["0", "1"]]]
                }
        expected_array = {
                    "nums":[2, 3],
                    "strs":["absde", "dddl"],
                    "2_d_a":[["1"]],
                    "3_d_a":[[["1"]]]
                }
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, array, self.nesting_level)
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        self.delete(self.client, key = self.key, path = 'nums[0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = 'strs[2]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '2_d_a[0][0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '3_d_a[0][0][0]', xattr=self.xattr)
        for key in list(expected_array.keys()):
            value = expected_array[key]
            new_path = self.generate_path(self.nesting_level, key)
            logic, data_return = self.get_string_and_verify_return( self.client, key = self.key, path = new_path, expected_value = value, xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_delete_json_array(self):
        result = True
        self.key = "test_delete_array"
        array = {
                    "2ja":[{"field":[0, 1, 2]}, [0, 1, 2]],
                    "3ja":[[{"field":[0, 1, 2]}, [0, 1, 2]]],
                    "4ja":[[[{"field":[0, 1, 2]}, [0, 1, 2]]]]
                }
        expected_array = {
                    "2ja":[{"field":[1]}, [1]],
                    "3ja":[[{"field":[1]}, [1]]],
                    "4ja":[[[{"field":[1]}, [1]]]]
                }
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, array, self.nesting_level)
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '2ja[0].field[0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '2ja[0].field[1]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '2ja[1][0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '2ja[1][1]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '3ja[0][0].field[0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '3ja[0][0].field[1]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '3ja[0][1][0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '3ja[0][1][1]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '4ja[0][0][0].field[0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '4ja[0][0][0].field[1]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '4ja[0][0][1][0]', xattr=self.xattr)
        self.delete(self.client, key = self.key, path = '4ja[0][0][1][1]', xattr=self.xattr)
        for key in list(expected_array.keys()):
            value = expected_array[key]
            new_path = self.generate_path(self.nesting_level, key)
            logic, data_return = self.get_string_and_verify_return(
                self.client, key = self.key, path = new_path, expected_value = value, xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

# Helper Methods

    def get_verify(self, dataset, data_key="default"):
        dict = {}
        result = True
        self.key = data_key
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, self.json, self.nesting_level)
        if not self.is_sdk_client:
            nested_json = json.dumps(nested_json)
        self.set_doc(self.client, self.key, nested_json, 0, 0, xattr=self.xattr)
        for key in list(dataset.keys()):
            logic, data_return = self.get_string_and_verify_return(
                self.client, key=self.key, path = key, expected_value = dataset[key], xattr=self.xattr)
            if not logic:
                dict[key] = {"expected": dataset[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def dict_add_verify(self, dataset, data_key="default"):
        result_dict = {}
        result = True
        self.key = data_key
        self.json = dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, {}, self.nesting_level)
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        for key in list(dataset.keys()):
            value = dataset[key]
            self.dict_add(self.client, self.key, key, value, xattr=self.xattr, create_parents=self.create_parents)
        for key in list(dataset.keys()):
            logic, data_return = self.get_string_and_verify_return(
                self.client, key = self.key, path = key, expected_value = dataset[key], xattr=self.xattr)
            if not logic:
                result_dict[key] = {"expected":dataset[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_upsert_verify(self, dataset, data_key = "default"):
        result_dict = {}
        result = True
        self.key = data_key
        self.json = dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, {}, self.nesting_level)
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        for key in list(dataset.keys()):
            value = dataset[key]
            self.dict_upsert(self.client, self.key, key, value, xattr=self.xattr, create_parents=self.create_parents)
        for key in list(dataset.keys()):
            logic, data_return = self.get_string_and_verify_return(
                self.client, key = self.key, path = key, expected_value = dataset[key], xattr=self.xattr)
            if not logic:
                result_dict[key] = {"expected":dataset[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_upsert_replace_verify(self, dataset, data_key="default"):
        result_dict = {}
        result = True
        self.key = data_key
        self.json = dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        new_json = self.shuffle_json(dataset)
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        for key in list(new_json.keys()):
            value = new_json[key]
            self.dict_upsert(self.client, self.key, key, value, xattr=self.xattr, create_parents=self.create_parents)
        for key in list(new_json.keys()):
            logic, data_return = self.get_string_and_verify_return(
                self.client, key = self.key, path = key, expected_value = new_json[key], xattr=self.xattr)
            if not logic:
                result_dict[key] = {"expected":new_json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_replace_verify(self, dataset, data_key = "default"):
        result_dict = {}
        result = True
        self.key = data_key
        self.json = dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        new_json = self.shuffle_json(copy.deepcopy(dataset))
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        for key in list(new_json.keys()):
            value = new_json[key]
            self.dict_replace(self.client, self.key, key, value, xattr=self.xattr)
        for key in list(new_json.keys()):
            logic, data_return = self.get_string_and_verify_return(
                self.client, key = self.key, path = key, expected_value = new_json[key], xattr=self.xattr)
            if not logic:
                result_dict[key] = {"expected":new_json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_delete_verify(self, dataset, data_key = "default"):
        result_dict = {}
        result = True
        self.key = data_key
        self.json = dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        for key in list(dataset.keys()):
            self.delete(self.client, self.key, key, xattr=self.xattr)
            dataset.pop(key)
            for key_1 in list(dataset.keys()):
                # value = dataset[key_1]
                logic, data_return = self.get_string_and_verify_return(
                    self.client, key = self.key, path = key_1, expected_value = dataset[key_1], xattr=self.xattr)
                if not logic:
                    result_dict[key_1] = {"expected":dataset[key_1], "actual":data_return}
                    result = result and logic
            self.assertTrue(result, result_dict)

    def exists(self, dataset, data_key = "default"):
        result_dict = {}
        result = True
        self.key = data_key
        self.json = dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        if not self.is_sdk_client:
            self.json = json.dumps(self.json)
        self.set_doc(self.client, self.key, self.json, 0, 0, xattr=self.xattr)
        print(self.json)
        for key in list(dataset.keys()):
            logic, data_return = self.verify_exists(self.client, key=self.key, path=key)
            if not logic:
                self.log.info(data_return)
                result_dict[key] = {"expected": True, "actual": logic}
                result = result and logic
        self.assertTrue(result, result_dict)

    def delete(self, client, key = '', path = '', xattr=None):
        try:
            if self.is_sdk_client:
                #  xattr not supported?
                client.mutate_in(key, SD.remove(path, xattr=xattr))
            else:
                client.delete_sd(key, path)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to delete in key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_add(self, client, key='', path='', value = None, xattr=None, create_parents=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            if self.is_sdk_client:
                print(path, ":", value)
                rv = client.mutate_in(key, SD.insert(path, value, xattr=xattr, create_parents=create_parents))
                self.log.info("xattr '%s' inserted successfully" % path)
            else:
                client.dict_add_sd(key, new_path, json.dumps(value))
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_replace(self, client, key = '', path = '', value = None, xattr=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            if self.is_sdk_client:
                #  xattr not supported?
                client.mutate_in(key, SD.replace(path, value, xattr=xattr))
            else:
                client.replace_sd(key, new_path, json.dumps(value))
        except Exception as e:
            self.log.error(e)
            self.fail("Unable to replace key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_upsert(self, client, key='', path='', value=None, xattr=None, create_parents=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            if self.is_sdk_client:
                client.mutate_in(key, SD.upsert(path, value, xattr=xattr, create_parents=create_parents))
            else:
                client.dict_upsert_sd(key, new_path, json.dumps(value))
        except Exception as e:
            self.log.error(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def counter(self, client, key = '', path = '', value = None, xattr=None, create_parents=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.log.info(new_path)
            if self.is_sdk_client:
                client.cb.mutate_in(key, SD.counter(new_path, int(value), xattr=xattr, create_parents=create_parents))
            else:
                client.counter_sd(key, new_path, value)
        except Exception as e:
            self.log.error(e)
            msg = "Unable to add key {0} for path {1} after {2} tries".format(key, path, 1)
            return msg

    def array_add_last(self, client, key = '', path = '', value = None, xattr=None, create_parents=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            if self.is_sdk_client:
                client.mutate_in(key, SD.array_append(new_path, value, xattr=xattr, create_parents=create_parents))
            else:
                client.array_push_last_sd(key, new_path, json.dumps(value))
        except Exception as e:
            self.log.error(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_first(self, client, key = '', path = '', value = None, xattr=None, create_parents=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            if self.is_sdk_client:
                client.mutate_in(key, SD.array_prepend(new_path, value, xattr=xattr, create_parents=create_parents))
            else:
                client.array_push_first_sd(key, new_path, json.dumps(value))
        except Exception as e:
            self.log.error(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_unique(self, client, key = '', path = '', value = None, xattr=None, create_parents=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            if self.is_sdk_client:
                client.mutate_in(key, SD.array_addunique(new_path, value, xattr=xattr, create_parents=create_parents))
            else:
                client.array_add_unique_sd(key, new_path, json.dumps(value))
        except Exception as e:
            self.log.error(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_insert(self, client, key = '', path = '', value = None, xattr=None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            if self.is_sdk_client:
                client.mutate_in(key, SD.array_insert(new_path, value, xattr=xattr))
            else:
                client.array_add_insert_sd(key, new_path, json.dumps(value))
        except Exception as e:
            self.log.error(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def get_and_verify(self, client, key = '', path = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            if self.is_sdk_client:
                data = client.cb.retrieve_in(key, new_path).get(0)[1]
            else:
                opaque, cas, data = client.get_sd(key, new_path)
        except Exception as e:
            self.log.error(e)
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        msg = "expected {0}, actual {1}".format(str(data), expected_value)
        self.assertTrue(str(data) == str(expected_value), msg=msg + " .data not returned correctly")

    def get_and_verify_with_value(self, client, key = '', path = '', expected_value = None, xattr=None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            if self.is_sdk_client:
                if xattr:
                    data = self._fix_unicode(client.cb.lookup_in(key, SD.get(path, xattr=xattr))[path])
                else:
                    data = client.cb.retrieve_in(key, new_path).get(0)[1]
            else:
                opaque, cas, data = client.get_sd(key, new_path)
                data = json.loads(data)
        except Exception as e:
            self.log.error(e)
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        msg = "expected {0}, actual {1}".format(str(data), expected_value)
        return str(data) == expected_value, msg

    def get_string_and_verify_return(self, client, key = '', path = '', expected_value = None, xattr=None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            if self.is_sdk_client:
                if xattr:
                    data = self._fix_unicode(client.cb.lookup_in(key, SD.get(path, xattr=xattr))[path])
                else:
                    data = self._fix_unicode(client.cb.retrieve_in(key, new_path).get(0)[1])
            else:
                opaque, cas, data = client.get_sd(key, new_path)
                data = yaml.safe_load(data)
        except Exception as e:
            self.log.error(e)
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        return (str(data) == str(expected_value)), str(data).encode('utf-8')

    def _fix_unicode(self, data):
        if isinstance(data, str):
            return data.encode('utf-8')
        elif isinstance(data, dict):
            data = dict((self._fix_unicode(k), self._fix_unicode(data[k])) for k in data)
        elif isinstance(data, list):
            for i in range(0, len(data)):
                data[i] = self._fix_unicode(data[i])
        return data

    def set_doc(self, client, key, value, exp, flags, xattr=None):
        if self.is_sdk_client:
            if xattr:
                # for verification xattr in subdoc
                client.cb.set(key, {}, exp, flags, )
                if isinstance(value, dict):
                    for k, v in value.items():
                        print(k, ":", v)
                        rv = client.cb.mutate_in(key, SD.upsert(k, v, xattr=xattr))
                        self.log.info("xattr '%s' added successfully?: %s" % (k, rv.success))
                        self.assertTrue(rv.success)

                        rv = client.cb.lookup_in(key, SD.exists(k, xattr=xattr))
                        self.log.info("xattr '%s' exists?: %s" % (k, rv.success))
                        try:
                            self.assertTrue(rv.success)
                        except Exception as e:
                            raise e
                else:
                    self.fail("Unable to handle non-json docs. Please review behavior")
            else:
                client.set(key, value, exp, flags,)
        else:
            client.set(key, exp, flags, value)

    def verify_exists(self, client, key='', path='', xattr=None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            if self.is_sdk_client:
                #  xattr not supported?----------
                client.lookup_in(key, SD.exists(new_path, xattr=xattr))
            else:
                client.exists_sd(key, new_path)
        except Exception as e:
            self.log.error(e)
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        return True, ""

    def shuffle_json(self, json_value):
        dict = {}
        keys = list(json_value.keys())
        for key in keys:
            index = random.randint(0, len(keys)-1)
            dict[key] =json_value[keys[index]]
        return dict

    def run_testcase(self, test_case, test_case_name, result={}):
        try:
            self.log.info("run test case {0}".format(test_case_name))
            test_case()
        except Exception as ex:
            result[test_case_name] = str(ex)


