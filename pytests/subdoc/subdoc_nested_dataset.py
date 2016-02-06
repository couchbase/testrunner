from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *
from subdoc_base import SubdocBaseTest
import copy, json
import yaml
import random

class SubdocNestedDataset(SubdocBaseTest):
    def setUp(self):
        super(SubdocNestedDataset, self).setUp()
        self.nesting_level =  self.input.param("nesting_level",0)
        self.client = self.direct_client(self.master, self.buckets[0])

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
                    "add_integer":0,
                    "sub_integer":1,
                    "array_add_integer":[0,1],
                    "array_sub_integer":[0,1]
                }
        expected_array = {
                    "add_integer":1,
                    "sub_integer":0,
                    "array_add_integer":[1,1],
                    "array_sub_integer":[0,0]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        self.counter(self.client, key = "test_counter", path = 'add_integer', value = "1")
        self.counter(self.client, key = "test_counter", path = 'sub_integer', value = "-1")
        self.counter(self.client, key = "test_counter", path = 'array_add_integer[0]', value = "1")
        self.counter(self.client, key = "test_counter", path = 'array_sub_integer[1]', value = "-1")
        self.json  = expected_array
        for key in expected_array.keys():
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = expected_array[key])
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

# SD_GET

    def test_get_null(self):
        self.json =  self.generate_simple_data_null()
        self.get_verify(self.json, "simple_dataset_null")

    def test_get_array_numbers(self):
    	self.json =  self.generate_simple_data_array_of_numbers()
        self.get_verify(self.json, "test_get_array_numbers")

    def test_get_array_strings(self):
    	self.json =  self.generate_simple_data_strings()
        self.get_verify(self.json, "test_get_array_strings")

    def test_get_mix_arrays(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.get_verify(self.json, "test_get_mix_arrays")

    def test_get_numbers_boundary(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.get_verify(self.json, "test_get_numbers_boundary")

    def test_get_element_arrays(self):
        result = True
        dict = {}
        self.key = "element_arrays"
        data_set =  self.generate_simple_arrays()
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        self.log.info(nested_json)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        key_single_dimension_path = "single_dimension_array[0]"
        logic, msg =  self.get_and_verify_with_value(self.client, key = self.key, path = key_single_dimension_path, expected_value = str(data_set["single_dimension_array"][0]))
        if not logic:
            dict[single_dimension_array] = msg
        result = result and logic
        key_two_dimension_path = "two_dimension_array[0][0]"
        logic, msg = self.get_and_verify_with_value(self.client, key = self.key, path = key_two_dimension_path, expected_value = str(data_set["two_dimension_array"][0][0]))
        result = result and logic
        if not logic:
            dict[key_two_dimension_path] = msg
        self.assertTrue(result, dict)

    def test_get_json_and_arrays(self):
        result = True
        dict = {}
        self.key = "element_array"
        expected_value = "value"
        data_set =  {
                        "1_array":[ { "field": [expected_value] }],
                        "2_array":[ [{ "field": [expected_value] }]],
                        "3_array":[ [[{ "field": [expected_value] }]]],
                    }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        key_single_dimension_path = "1_array[0].field[0]"
        logic, msg =  self.get_and_verify_with_value(self.client, key = self.key, path = key_single_dimension_path, expected_value = expected_value)
        if not logic:
            dict[single_dimension_array] = msg
        result = result and logic
        key_two_dimension_path = "2_array[0][0].field[0]"
        logic, msg = self.get_and_verify_with_value(self.client, key = self.key, path = key_two_dimension_path, expected_value = expected_value)
        result = result and logic
        if not logic:
            dict[key_two_dimension_path] = msg
        self.assertTrue(result, dict)
        key_three_dimension_path = "3_array[0][0][0].field[0]"
        logic, msg = self.get_and_verify_with_value(self.client, key = self.key, path = key_three_dimension_path, expected_value = expected_value)
        result = result and logic
        if not logic:
            dict[key_three_dimension_path] = msg
        self.assertTrue(result, dict)

# SD_EXISTS
    def test_exists_json_fields(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.exists(self.json, "test_exists_json_fields")

    def test_exists_element_array(self):
        result = True
        dict = {}
        self.key = "test_exists_element_array"
        data_set =  self.generate_simple_arrays()
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        key_single_dimension_path = "single_dimension_array[0]"
        logic, msg =  self.verify_exists(self.client, key = self.key, path = key_single_dimension_path)
        if not logic:
            dict[key_single_dimension_path] = msg
        result = result and logic
        key_two_dimension_path = "two_dimension_array[0][0]"
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
                        "1_array":[ { "field": [expected_value] }],
                        "2_array":[ [{ "field": [expected_value] }]],
                        "3_array":[ [[{ "field": [expected_value] }]]],
                    }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, data_set, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        key_single_dimension_path = "1_array[0].field[0]"
        logic, msg =  self.verify_exists(self.client, key = self.key, path = key_single_dimension_path)
        if not logic:
            dict[single_dimension_array] = msg
        result = result and logic
        key_two_dimension_path = "2_array[0][0].field[0]"
        logic, msg = self.verify_exists(self.client, key = self.key, path = key_two_dimension_path)
        result = result and logic
        if not logic:
            dict[key_two_dimension_path] = msg
        self.assertTrue(result, dict)
        key_three_dimension_path = "3_array[0][0][0].field[0]"
        logic, msg = self.verify_exists(self.client, key = self.key, path = key_three_dimension_path)
        result = result and logic
        if not logic:
            dict[key_three_dimension_path] = msg
        self.assertTrue(result, dict)


# SD_ARRAY_ADD

    def test_add_last_array(self):
        result = True
        dict = {}
        self.key = "test_add_last_array"
        array = {
                    "empty":[],
                    "single_dimension_array":["0"],
                    "two_dimension_array":[["0"]],
                    "three_dimension_array":[[["0"]]],
                    "four_json_dimension_array":[[[{"field":["0"]}]]]
                }
        expected_array = {
                    "empty":["1"],
                    "single_dimension_array":["0","1"],
                    "two_dimension_array":[["0", "1"]],
                    "three_dimension_array":[[["0", "1"]]],
                    "four_json_dimension_array":[[[{"field":["0", "1"]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_last(self.client, key = "test_add_last_array", path = 'empty', value = json.dumps("1"))
        self.array_add_last(self.client, key = "test_add_last_array", path = 'single_dimension_array', value =  json.dumps("1"))
        self.array_add_last(self.client, key = "test_add_last_array", path = 'two_dimension_array[0]', value =  json.dumps("1"))
        self.array_add_last(self.client, key = "test_add_last_array", path = 'three_dimension_array[0][0]', value =  json.dumps("1"))
        self.array_add_last(self.client, key = "test_add_last_array", path = 'four_json_dimension_array[0][0][0].field', value =  json.dumps("1"))
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = value)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_first_array(self):
        result = True
        dict = {}
        self.key = "test_add_first_array"
        array = {
                    "empty":[],
                    "single_dimension_array":["1"],
                    "two_dimension_array":[["1"]],
                    "three_dimension_array":[[["1"]]],
                    "four_dimension_json_array":[[[{"field":["1"]}]]]
                }
        expected_array = {
                    "empty":["0"],
                    "single_dimension_array":["0","1"],
                    "two_dimension_array":[["0", "1"]],
                    "three_dimension_array":[[["0", "1"]]],
                    "four_dimension_json_array":[[[{"field":["0","1"]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_first(self.client, key = "test_add_first_array", path = 'empty', value = json.dumps("0"))
        self.array_add_first(self.client, key = "test_add_first_array", path = 'single_dimension_array', value =  json.dumps("0"))
        self.array_add_first(self.client, key = "test_add_first_array", path = 'two_dimension_array[0]', value =  json.dumps("0"))
        self.array_add_first(self.client, key = "test_add_first_array", path = 'three_dimension_array[0][0]', value =  json.dumps("0"))
        self.array_add_first(self.client, key = "test_add_first_array", path = 'four_dimension_json_array[0][0][0].field', value =  json.dumps("0"))
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = value)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_unique_array(self):
        result = True
        dict = {}
        self.key = "test_add_unique_array"
        array = {
                    "empty":[],
                    "single_dimension_array":["0",2],
                    "two_dimension_array":[["0",2]],
                    "three_dimension_array":[[["0", 2]]],
                    "four_dimension_json_array":[[[{"field":["0", 2]}]]]
                }
        expected_array = {
                    "empty":["1"],
                    "single_dimension_array":["0",2, "1"],
                    "two_dimension_array":[["0",2, "1"]],
                    "three_dimension_array":[[["0",2,"1"]]],
                    "four_dimension_json_array":[[[{"field":["0", 2, "1"]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'empty', value = json.dumps("1"))
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'single_dimension_array', value =  json.dumps("1"))
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'two_dimension_array[0]', value =  json.dumps("1"))
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'three_dimension_array[0][0]', value =  json.dumps("1"))
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'four_dimension_json_array[0][0][0].field', value =  json.dumps("1"))
        self.json  = expected_array
        for key in expected_array.keys():
            expected_value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = expected_value)
            if not logic:
                dict[key] = {"expected": expected_value, "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_insert_array(self):
        result = True
        dict = {}
        self.key = "test_add_insert_array"
        array = {
                    "single_dimension_array_no_element":[],
                    "two_dimension_array_no_element":[[]],
                    "three_dimension_array_no_element":[[[]]],
                    "three_dimension_json_array_no_element":[[[{"field":[]}]]]
                }
        expected_array = {
                    "single_dimension_array_no_element":[0, 1 , 2, 3],
                    "two_dimension_array_no_element":[[0, 1, 2, 3],[0, 1, 2, 3]],
                    "three_dimension_array_no_element":[[[0, 1, 2, 3],[0, 1, 2, 3]],[0, 1, 2, 3]],
                    "three_dimension_json_array_no_element":[[[{"field":[0, 1, 2, 3]}]]]
                }
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, array, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[0]", value = json.dumps(1))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[0]", value = json.dumps(0))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[2]", value = json.dumps(3))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[2]", value = json.dumps(2))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][0]", value = json.dumps(1))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][0]", value = json.dumps(0))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][2]", value = json.dumps(3))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][2]", value = json.dumps(2))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[1]", value = json.dumps([0, 1, 2, 3]))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][0]", value = json.dumps(1))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][0]", value = json.dumps(0))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][2]", value = json.dumps(3))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][2]", value = json.dumps(2))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][1]", value = json.dumps([0, 1, 2, 3]))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[1]", value = json.dumps([0, 1, 2, 3]))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_json_array_no_element[0][0][0].field[0]", value = json.dumps(0))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_json_array_no_element[0][0][0].field[1]", value = json.dumps(1))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_json_array_no_element[0][0][0].field[2]", value = json.dumps(2))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_json_array_no_element[0][0][0].field[3]", value = json.dumps(3))
        self.json  = expected_array
        for key in expected_array.keys():
            expected_value = expected_array[key]
            logic, actua_value  =  self.get_string_and_verify_return( self.client, key = self.key, path = key, expected_value = expected_value)
            if not logic:
                dict[key] = {"expected": expected_value, "actual": actua_value}
            result = result and logic
        self.assertTrue(result, dict)

# SD_ADD

    def test_add_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_add_verify(self.json, "test_add_numbers")

    def test_add_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_add_verify(self.json, "test_add_array_of_numbers")

    def test_add_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_add_verify(self.json, "test_add_numbers_boundary")

    def test_add_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_add_verify(self.json, "test_add_string")

    def test_add_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_add_verify(self.json, "test_add_array_strings")

    def test_add_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_add_verify(self.json, "test_add_null")

    def test_add_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_add_verify(self.json, "test_add_boolean")

    def test_add_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_add_verify(self.json, "test_add_array_mix")

# SD_UPSERT - Add Operations

    def test_upsert_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_numbers")

    def test_upsert_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_array_of_numbers")

    def test_upsert_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_numbers_boundary")

    def test_upsert_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_upsert_verify(self.json, "test_upsert_string")

    def test_upsert_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_upsert_verify(self.json, "test_upsert_array_strings")

    def test_upsert_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_upsert_verify(self.json, "test_upsert_null")

    def test_upsert_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_upsert_verify(self.json, "test_upsert_boolean")

    def test_upsert_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_upsert_verify(self.json, "test_upsert_array_mix")

# SD_UPERT - Replace Operations

    def test_upsert_replace_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_numbers")

    def test_upsert_replace_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_of_numbers")

    def test_upsert_replace_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_numbers_boundary")

    def test_upsert_replace_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_string")

    def test_upsert_replace_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_strings")

    def test_upsert_replace_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_null")

    def test_upsert_replace_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_boolean")

    def test_upsert_replace_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_mix")

# SD_REPLACE - Replace Operations

    def test_replace_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_replace_verify(self.json, "test_replace_numbers")

    def test_replace_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_replace_verify(self.json, "test_replace_array_of_numbers")

    def test_replace_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_replace_verify(self.json, "test_replace_numbers_boundary")

    def test_replace_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_replace_verify(self.json, "test_replace_string")

    def test_replace_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_replace_verify(self.json, "test_replace_array_strings")

    def test_replace_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_replace_verify(self.json, "test_replace_null")

    def test_replace_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_replace_verify(self.json, "test_replace_boolean")

    def test_replace_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_replace_verify(self.json, "test_replace_array_mix")

# SD_DELETE

    def test_delete_dict(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_delete_verify(self.json, "test_delete_array")

    def test_delete_array(self):
        result = True
        self.key = "test_delete_array"
        array = {
                    "numbers":[1,2,3],
                    "strings":["absde", "dddl", "dkdkd"],
                    "two_dimension_array":[["0","1"]],
                    "three_dimension_array":[[["0","1"]]]
                }
        expected_array = {
                    "numbers":[2,3],
                    "strings":["absde", "dddl"],
                    "two_dimension_array":[["1"]],
                    "three_dimension_array":[[["1"]]]
                }
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, array, self.nesting_level)
        jsonDump = json.dumps(self.json)
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.delete(self.client, key = "test_delete_array", path = 'numbers[0]')
        self.delete(self.client, key = "test_delete_array", path = 'strings[2]')
        self.delete(self.client, key = "test_delete_array", path = 'two_dimension_array[0][0]')
        self.delete(self.client, key = "test_delete_array", path = 'three_dimension_array[0][0][0]')
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            new_path = self.generate_path(self.nesting_level, key)
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = new_path, expected_value = value)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)


    def test_delete_json_array(self):
        result = True
        self.key = "test_delete_array"
        array = {
                    "2_json_array":[{"field":[0, 1, 2]},[0, 1, 2]],
                    "3_json_array":[[{"field":[0, 1, 2]},[0, 1, 2]]],
                    "4_json_array":[[[{"field":[0, 1, 2]},[0, 1, 2]]]]
                }
        expected_array = {
                    "2_json_array":[{"field":[1]},[1]],
                    "3_json_array":[[{"field":[1]},[1]]],
                    "4_json_array":[[[{"field":[1]},[1]]]]
                }
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, array, self.nesting_level)
        jsonDump = json.dumps(self.json)
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.delete(self.client, key = "test_delete_array", path = '2_json_array[0].field[0]')
        self.delete(self.client, key = "test_delete_array", path = '2_json_array[0].field[1]')
        self.delete(self.client, key = "test_delete_array", path = '2_json_array[1][0]')
        self.delete(self.client, key = "test_delete_array", path = '2_json_array[1][1]')
        self.delete(self.client, key = "test_delete_array", path = '3_json_array[0][0].field[0]')
        self.delete(self.client, key = "test_delete_array", path = '3_json_array[0][0].field[1]')
        self.delete(self.client, key = "test_delete_array", path = '3_json_array[0][1][0]')
        self.delete(self.client, key = "test_delete_array", path = '3_json_array[0][1][1]')
        self.delete(self.client, key = "test_delete_array", path = '4_json_array[0][0][0].field[0]')
        self.delete(self.client, key = "test_delete_array", path = '4_json_array[0][0][0].field[1]')
        self.delete(self.client, key = "test_delete_array", path = '4_json_array[0][0][1][0]')
        self.delete(self.client, key = "test_delete_array", path = '4_json_array[0][0][1][1]')
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            new_path = self.generate_path(self.nesting_level, key)
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = new_path, expected_value = value)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

# Helper Methods

    def get_verify(self, dataset, data_key = "default"):
        dict = {}
        result = True
        self.key = data_key
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, self.json, self.nesting_level)
        jsonDump = json.dumps(nested_json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in dataset.keys():
            logic, data_return  =  self.get_string_and_verify_return(self.client, key = self.key, path = key, expected_value = dataset[key])
            if not logic:
                dict[key] = {"expected": dataset[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def dict_add_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, {}, self.nesting_level)
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in dataset.keys():
            value = dataset[key]
            value = json.dumps(value)
            self.dict_add(self.client, self.key, key, value)
        for key in dataset.keys():
            logic, data_return  =  self.get_string_and_verify_return(self.client, key = self.key, path = key, expected_value = dataset[key])
            if not logic:
                result_dict[key] = {"expected":dataset[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_upsert_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, {}, self.nesting_level)
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in dataset.keys():
            value = dataset[key]
            value = json.dumps(value)
            self.dict_upsert(self.client, self.key, key, value)
        for key in dataset.keys():
            logic, data_return  =  self.get_string_and_verify_return(self.client, key = self.key, path = key, expected_value = dataset[key])
            if not logic:
                result_dict[key] = {"expected":dataset[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_upsert_replace_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(self.json)
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        new_json = self.shuffle_json(dataset)
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in new_json.keys():
            value = new_json[key]
            value = json.dumps(value)
            self.dict_upsert(self.client, self.key, key, value)
        for key in new_json.keys():
            logic, data_return  =  self.get_string_and_verify_return(self.client, key = self.key, path = key, expected_value = new_json[key])
            if not logic:
                result_dict[key] = {"expected":new_json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_replace_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(self.json)
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        new_json = self.shuffle_json(copy.deepcopy(dataset))
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in new_json.keys():
            value = new_json[key]
            value = json.dumps(value)
            self.dict_replace(self.client, self.key, key, value)
        for key in new_json.keys():
            logic, data_return  =  self.get_string_and_verify_return(self.client, key = self.key, path = key, expected_value = new_json[key])
            if not logic:
                result_dict[key] = {"expected":new_json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_delete_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in dataset.keys():
            self.delete(self.client, self.key, key)
            dataset.pop(key)
            for key_1 in dataset.keys():
                value = dataset[key_1]
                logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key_1, expected_value = dataset[key_1])
                if not logic:
                    result_dict[key_1] = {"expected":dataset[key_1], "actual":data_return}
                    result = result and logic
            self.assertTrue(result, result_dict)

    def exists(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        base_json = self.generate_json_for_nesting()
        self.json = self.generate_nested(base_json, dataset, self.nesting_level)
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in dataset.keys():
            logic, data_return  =  self.verify_exists( self.client, key = self.key, path = key)
            if not logic:
                self.log.info(data_return)
                result_dict[key] = {"expected":True, "actual":logic}
                result = result and logic
        self.assertTrue(result, result_dict)

    def delete(self, client, key = '', path = ''):
        try:
            self.client.delete_sd(key, path)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_add(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.client.dict_add_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_replace(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.client.replace_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to replace key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_upsert(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.client.dict_upsert_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def counter(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.log.info(new_path)
            self.client.counter_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            msg = "Unable to add key {0} for path {1} after {2} tries".format(key, path, 1)
            return msg

    def array_add_last(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.client.array_push_last_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_first(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.client.array_push_first_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_unique(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.client.array_add_unique_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_insert(self, client, key = '', path = '', value = None):
        try:
            new_path = self.generate_path(self.nesting_level, path)
            self.client.array_add_insert_sd(key, new_path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def get_and_verify(self, client, key = '', path = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.get_sd(key, new_path)
        except Exception as e:
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        msg = "expected {0}, actual {1}".format(str(data), expected_value)
        self.assertTrue(str(data) == str(expected_value), msg="data not returned correctly")

    def get_and_verify_with_value(self, client, key = '', path = '', value = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.get_sd(key, new_path)
            data = json.loads(data)
        except Exception as e:
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        msg = "expected {0}, actual {1}".format(str(data), expected_value)
        return str(data) == expected_value, msg

    def get_string_and_verify_return(self, client, key = '', path = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.get_sd(key, new_path)
            data = yaml.safe_load(data)
        except Exception as e:
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        return (str(data).encode('utf-8') == str(expected_value)), str(data).encode('utf-8')

    def verify_exists(self, client, key = '', path = ''):
        new_path = self.generate_path(self.nesting_level, path)
        check = True
        try:
            self.client.exists_sd(key, new_path)
        except Exception as e:
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        return True, ""

    def get_and_verify_return(self, client, key = '', path = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.get_sd(key, new_path)
        except Exception as e:
            msg = "Unable to get key {0} for path {1} after {2} tries".format(key, path, 1)
            return False, msg
        return str(data) == str(expected_value), data

    def shuffle_json(self, json_value):
        dict = {}
        keys = json_value.keys()
        for key in keys:
            index = random.randint(0,len(keys)-1)
            dict[key] =json_value[keys[index]]
        return dict

    def run_testcase(self , test_case, test_case_name, result = {}):
        try:
            self.log.info("run test case {0}".format(test_case_name))
            test_case()
        except Exception, ex:
            result[test_case_name] = str(ex)


