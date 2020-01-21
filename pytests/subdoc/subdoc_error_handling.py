from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *
from .subdoc_base import SubdocBaseTest
import copy, json
import sys
import random

class SubdocErrorHandling(SubdocBaseTest):
    def setUp(self):
        super(SubdocErrorHandling, self).setUp()
        self.nesting_level =  self.input.param("nesting_level", 0)
        self.client = self.direct_client(self.master, self.buckets[0])

    def tearDown(self):
        super(SubdocErrorHandling, self).tearDown()

    def test_error_get_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        #self.client.get_sd("simple_data","crap")
        self.log.info("simple_data :: path does not exist")
        self.error_gets("simple_data", "does_not_exist", error = "Memcached error #192 'Path not exists'", field = "simple_data : path does not exist - dictionary", result = result)
        self.log.info("simple_data :: malformed path")
        self.error_gets("simple_data", "{][]}", error = "Memcached error #194 'Invalid path'", field = "simple_data : malformed path", result = result)
        self.log.info("simple_data :: path does not exist - array, out of bounds index")
        self.error_gets("simple_data", "array[200]", error = "Memcached error #192 'Path not exists'", field = "simple_data : path does not exist - array, out of bounds index", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_gets("does_not_exist", "does_not_exist", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_get_nested_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Nested Data
        self.log.info("nested_data :: path does not exist")
        new_path = self.generate_path(20, "does_not_exist")
        self.error_gets("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist - dictionary", result = result)
        self.log.info("nested_data ::path does not exist - array, out of bounds index")
        new_path = self.generate_path(20, "array[200]")
        self.error_gets("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist - array, out of bounds index", result = result)
        self.log.info("nested_data ::malformed path")
        new_path = self.generate_path(20, "{[]}")
        self.error_gets("normal_nested_data", new_path, error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_gets("nested_data", new_path, error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_exists_nested_data(self):
        result = {}
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Nested Data Set
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "{][]}")
        self.error_exists("normal_nested_data", new_path, error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        self.log.info("nested_data :: path does not exist")
        new_path = self.generate_path(20, "does_not_exist")
        self.error_exists("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist malformed path", result = result)
        self.log.info("nested_data ::path does not exist - array, out of bounds index")
        new_path = self.generate_path(20, "array[200]")
        self.error_exists("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist - array, out of bounds index", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_exists("nested_data", new_path, error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_exists_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: path does not exist")
        self.error_exists("simple_data", "does_not_exist", error = "Memcached error #192 'Path not exists'", field = "simple_data : path does not exist ", result = result)
        self.log.info("simple_data :: path does not exist - array, out of bounds index")
        self.error_exists("simple_data", "array[200]", error = "Memcached error #192 'Path not exists'", field = "simple_data : path does not exist - array, out of bounds index", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_exists("does_not_exist", "does_not_exist", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.log.info("simple_data :: malformed path")
        self.error_exists("simple_data", "[]{}]", error = "Memcached error #194 'Invalid path'", field = "simple_data : malformed path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_add_dict_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: path exists")
        self.error_add_dict("simple_data", "field", value = "value_value", error = "Memcached error #197 'Cant insert'", field = "simple_data :: path exists", result = result)
        self.log.info("simple_data :: inserting into an array")
        self.error_add_dict("simple_data", "array[0]", value = "value_value", error = "Memcached error #197 'Cant insert'", field = "simple_data :: inserting into an array", result = result)
        self.log.info("simple_data :: empty path does not exist")
        self.error_add_dict("simple_data", "{][]}", value = "value_value", error = "Memcached error #194 'Invalid path'", field = "simple_data : malformed path", result = result)
        self.assertTrue(len(result) == 0, result)
        self.error_add_dict("simple_data", "", value = "value_value", error = "Memcached error #4 'Invalid'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.log.info("simple_data :: malformed path")

    def test_error_add_dict_nested_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("nested_data :: malformed json")
        new_path = self.generate_path(20, "field_1")
        self.error_add_dict("normal_nested_data", new_path, value = {"data"}, error = "Memcached error #197 'Cant insert'", field = "nested_data : malformed json", result = result)
        self.log.info("nested_data :: path exists")
        new_path = self.generate_path(20, "field")
        self.error_add_dict("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : path exists", result = result)
        self.log.info("nested_data :: inserting into an array")
        new_path = self.generate_path(20, "array[0]")
        self.error_add_dict("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : inserting into an array", result = result)
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_add_dict("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "{}][")
        self.error_add_dict("normal_nested_data", new_path, value = "value_value", error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_add_dict("nested_data", new_path, value = "value_value", error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_upsert_dict_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: insertion into array")
        self.error_upsert_dict("simple_data", "array[0]", value = "value_value", error = "Memcached error #197 'Cant insert'", field = "simple_data : insertion into array", result = result)
        self.log.info("simple_data :: empty path does not exist")
        self.error_upsert_dict("simple_data", "", value = "value_value", error = "Memcached error #4 'Invalid'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_upsert_dict_nested_data(self):
        result = {}
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Nested Data Set
        self.log.info("nested_data :: malformed json")
        new_path = self.generate_path(20, "field_1")
        self.error_upsert_dict("normal_nested_data", new_path, value = {10}, error = "Memcached error #197 'Cant insert'", field = "nested_data : malformed json", result = result)
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_upsert_dict("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: inserting into array")
        new_path = self.generate_path(20, "array[0]")
        self.error_upsert_dict("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : inserting into array", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "{}}[0]")
        self.error_upsert_dict("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : malformed path", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_upsert_dict("nested_data", new_path, value = "value_value", error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_replace_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: document does not exist")
        self.error_replace("does_not_exist", "does_not_exist", value = "value_value", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.log.info("simple_data :: path does not exist - array, negavtie index")
        self.error_replace("simple_data", "array[-1]", value = "value_value", error = "Memcached error #197 'Cant insert'", field = "simple_data : path does not exist - array, negavtie index", result = result)
        self.log.info("simple_data :: path does not exist - array, out of bounds index")
        self.error_replace("simple_data", "array[200]", value = "value_value", error = "Memcached error #197 'Cant insert'", field = "simple_data : path does not exist - array, out of bounds index", result = result)
        self.log.info("simple_data :: empty path does not exist")
        self.error_replace("simple_data", "", value = "value_value", error = "Memcached error #4 'Invalid'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_replace_nested_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_replace("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: path does not exist - array, negavtie index")
        new_path = self.generate_path(20, "array[-1]")
        self.error_replace("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : path does not exist - array, negavtie index", result = result)
        self.log.info("nested_data :: path does not exist - array, out of bounds index")
        new_path = self.generate_path(20, "array[200]")
        self.error_replace("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : path does not exist - array, out of bounds index", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "{][]}")
        self.error_replace("normal_nested_data", new_path, value = "value_value", error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        self.log.info("nested_data :: malformed json")
        new_path = self.generate_path(20, "field")
        self.error_replace("normal_nested_data", new_path, value = {10}, error = "Memcached error #197 'Cant insert'", field = "nested_data : malformed json", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_replace("nested_data", new_path, value = "value_value", error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_delete_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: empty path does not exist")
        self.error_delete("simple_data", "", value = "value_value", error = "Memcached error #4 'Invalid'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_delete("does_not_exist", "does_not_exist", value = "value_value", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.log.info("simple_data :: path does not exist - array, negavtie index")
        self.error_delete("simple_data", "array[-1]", value = "value_value", error = "Memcached error #197 'Cant insert'", field = "simple_data : path does not exist - array, negavtie index", result = result)
        self.log.info("simple_data :: path does not exist - array, out of bounds index")
        self.error_delete("simple_data", "array[200]", value = "value_value", error = "Memcached error #197 'Cant insert'", field = "simple_data : path does not exist - array, out of bounds index", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_delete_nested_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_delete("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: path does not exist - array, negavtie index")
        new_path = self.generate_path(20, "array[-1]")
        self.error_delete("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : path does not exist - array, negavtie index", result = result)
        self.log.info("nested_data :: path does not exist - array, out of bounds index")
        new_path = self.generate_path(20, "array[200]")
        self.error_delete("normal_nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : path does not exist - array, out of bounds index", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "{][]}")
        self.error_delete("normal_nested_data", new_path, value = "value_value", error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "array")
        self.error_delete("nested_data", new_path, value = "value_value", error = "Memcached error #197 'Cant insert'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_push_last_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: empty path does not exist")
        self.error_array_push_last("simple_data", "", error = "Memcached error #193 'Path mismatch'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.log.info("simple_data :: not an array path does not exist")
        self.error_array_push_last("simple_data", "field", error = "Memcached error #193 'Path mismatch'", field = "simple_data : not an array path does not exist - dictionary", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_array_push_last("does_not_exist", "does_not_exist", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "array")
        self.error_array_push_last("nested_data", new_path, error = "Memcached error #1 'Not found'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_push_last_nested_data(self):
        result = {}
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Nested Data Set
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_array_push_last("normal_nested_data", new_path, value = 10, error = "Memcached error #193 'Path mismatch'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "[][\|}{")
        self.error_array_push_last("normal_nested_data", new_path, value = 10, error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        self.log.info("nested_data :: malformed json")
        new_path = self.generate_path(20, "array")
        self.error_array_push_last("normal_nested_data", new_path, value = {10}, error = "Memcached error #197 'Cant insert'", field = "nested_data : malformed json", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "array")
        self.error_array_push_last("nested_data", new_path, error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_push_first_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: empty path does not exist")
        self.error_array_push_first("simple_data", "",  value =1, error = "Memcached error #193 'Path mismatch'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.log.info("simple_data :: not an array path does not exist")
        self.error_array_push_first("simple_data", "field",  value =1, error = "Memcached error #193 'Path mismatch'", field = "simple_data : not an array path does not exist - dictionary", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_array_push_first("does_not_exist", "does_not_exist", value =1, error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_push_first_nested_data(self):
        result = {}
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Nested Data Set
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_array_push_first("normal_nested_data", new_path, value = 10, error = "Memcached error #193 'Path mismatch'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "{{]\{}[")
        self.error_array_push_first("normal_nested_data", new_path, value =10, error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        self.log.info("nested_data :: malformed json")
        new_path = self.generate_path(20, "array")
        self.error_array_push_first("normal_nested_data", new_path, value = {10}, error = "Memcached error #197 'Cant insert'", field = "nested_data : malformed json", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "array")
        self.error_array_push_first("nested_data", new_path, error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_push_unique_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2, {}]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: empty path does not exist")
        self.error_array_add_unique("simple_data", "",  value=2, error = "Memcached error #193 'Path mismatch'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.log.info("simple_data :: not an array path does not exist")
        self.error_array_add_unique("simple_data", "field",  value=2, error = "Memcached error #193 'Path mismatch'", field = "simple_data : not an array path does not exist - dictionary", result = result)
        self.log.info("simple_data :: unique value exists")
        self.error_array_add_unique("simple_data", "array", value=2, error = "Memcached error #193 'Path mismatch'", field = "simple_data : unique value exists - dictionary", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_array_add_unique("does_not_exist", "does_not_exist", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_push_unique_nested_data(self):
        result = {}
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("nested_data :: unique value exists")
        new_path = self.generate_path(20, "array")
        self.error_array_add_unique("normal_nested_data", new_path, value=2, error = "Memcached error #193 'Path mismatch'", field = "simple_data : unique value exists - dictionary", result = result)
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_array_add_unique("normal_nested_data", new_path, value=2, error = "Memcached error #193 'Path mismatch'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "{}][\P")
        self.error_array_add_unique("normal_nested_data", new_path, value=2, error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        self.log.info("nested_data :: malformed json")
        new_path = self.generate_path(20, "array")
        self.error_array_add_unique("normal_nested_data", new_path, value= {10}, error = "Memcached error #197 'Cant insert'", field = "nested_data : malformed json", result = result)
        self.log.info("nested_data :: collision - already present json structure")
        new_path = self.generate_path(20, "array")
        self.error_array_add_unique("normal_nested_data", new_path, value= {}, error = "Memcached error #197 'Cant insert'", field = "nested_data : collision - already present json structure", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_array_add_unique("nested_data", new_path, error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_add_insert_simple_data(self):
        result = {}
        simple_data = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: not an array path does not exist")
        self.error_array_add_insert("simple_data", "field", value=2, error = "Memcached error #194 'Invalid path'", field = "simple_data : not an array path does not exist - dictionary", result = result)
        self.log.info("simple_data :: negative index value")
        self.error_array_add_insert("simple_data", "array[-1]", value=2, error = "Memcached error #194 'Invalid path'", field = "simple_data : negative value - dictionary", result = result)
        self.log.info("simple_data :: out of bounds index value")
        self.error_array_add_insert("simple_data", "array[200]", value=2, error = "Memcached error #192 'Path not exists'", field = "simple_data : out of bounds index  value - dictionary", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_array_add_insert("does_not_exist", "does_not_exist", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.log.info("simple_data :: empty path does not exist")
        self.error_array_add_insert("simple_data", "", value=2, error = "Memcached error #4 'Invalid'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_array_add_insert_nested_data(self):
        result = {}
        nested_simple = {
                    "field":"simple",
                    "array":[{"field":"exists"}, 1, 2]
                }
        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Nested Data Set
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_array_add_insert("normal_nested_data", new_path, value=2, error = "Memcached error #194 'Invalid path'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("simple_data :: out of bounds index value")
        new_path = self.generate_path(20, "array[200]")
        self.error_array_add_insert("normal_nested_data", new_path, value=2, error = "Memcached error #192 'Path not exists'", field = "simple_data : out of bounds index  value - dictionary", result = result)
        self.log.info("simple_data :: malformed path")
        new_path = self.generate_path(20, "{][[e]]}")
        self.error_array_add_insert("normal_nested_data", new_path, value=2, error = "Memcached error #194 'Invalid path'", field = "simple_data : malformed path", result = result)
        self.log.info("simple_data :: malformed json")
        new_path = self.generate_path(20, "array[0]")
        self.error_array_add_insert("normal_nested_data", new_path, value={10}, error = "Memcached error #197 'Cant insert'", field = "simple_data : malformed json", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_array_add_insert("nested_data", new_path, error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def test_error_counter_simple_data(self):
        result = {}
        simple_data = {
                    "integer":1,
                    "double":1.0,
                    "array":[1, 2]
                }
        # Add Simple Data
        jsonDump = json.dumps(simple_data)
        self.client.set("simple_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("simple_data :: document does not exist")
        self.error_counter("does_not_exist", "does_not_exist", value = 1, error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        self.log.info("simple_data :: empty path does not exist")
        self.error_counter("simple_data", "", value = 1, error = "Memcached error #4 'Invalid'", field = "simple_data : empty path does not exist - dictionary", result = result)

        self.assertTrue(len(result) == 0, result)

    def test_error_counter_nested_data(self):
        result = {}

        nested_simple = {
                    "integer":1,
                    "double":1.0,
                    "array":[{"field":"exists"}, 1, 2]
                }

        # Add Normal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 40)
        jsonDump = json.dumps(nested_json)
        self.client.set("nested_data", 0, 0, jsonDump)
        # Add Abnormal Nested Data
        base_json = self.generate_json_for_nesting()
        nested_json = self.generate_nested(base_json, nested_simple, 20)
        jsonDump = json.dumps(nested_json)
        self.client.set("normal_nested_data", 0, 0, jsonDump)
        # Tests for Simple Data Set
        self.log.info("nested_data :: counter to a double")
        new_path = self.generate_path(20, "double")
        self.error_counter("normal_nested_data", new_path, 1.0, error = "Memcached error #200 'Delta out of range'", field = "nested_data : counter to a double - dictionary", result = result)
        self.log.info("nested_data :: integer overflow")
        new_path = self.generate_path(20, "integer")
        self.error_counter("normal_nested_data", new_path, sys.maxsize, error = "Memcached error #197 'Cant insert'", field = "nested_data : integer overflow - dictionary", result = result)
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_counter("normal_nested_data", new_path, error = "Memcached error #193 'Path mismatch'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: malformed path")
        new_path = self.generate_path(20, "[]{}\][")
        self.error_counter("normal_nested_data", new_path, error = "Memcached error #194 'Invalid path'", field = "nested_data : malformed path", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_counter("nested_data", new_path, error = "Memcached error #195 'Path too big'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def error_exists(self, in_key, path, error = "error", field  = "field", result = {}):
        try:
        	self.client.exists_sd(in_key, path)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])

    def error_gets(self, in_key, path, error = "error", field  = "field", result = {}):
        try:
            self.client.get_sd(in_key, path)
            result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])

    def error_add_dict(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.dict_add_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])

    def error_upsert_dict(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.dict_upsert_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])


    def error_array_push_last(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.array_push_last_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])

    def error_array_push_first(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.array_push_first_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])

    def error_array_add_unique(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.array_add_unique_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])


    def error_array_add_insert(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.array_add_insert_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])

    def error_replace(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.replace_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])


    def error_delete(self, in_key, path, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.delete_sd(in_key, path)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])

    def error_counter(self, in_key, path, value = 10, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.counter_sd(in_key, path, value)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
            if (str(ex).find(error) == -1):
                self.log.info(str(ex))
                result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
                self.client = self.direct_client(self.master, self.buckets[0])