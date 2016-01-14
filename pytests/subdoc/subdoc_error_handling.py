from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *
from subdoc_base import SubdocBaseTest
import copy, json
import random

class SubdocErrorHandling(SubdocBaseTest):
    def setUp(self):
        super(SubdocErrorHandling, self).setUp()
        self.nesting_level =  self.input.param("nesting_level",0)
        self.client = self.direct_client(self.master, self.buckets[0])

    def tearDown(self):
        super(SubdocErrorHandling, self).tearDown()

    def test_error_get(self):
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
        self.log.info("simple_data :: empty path does not exist")
        self.error_gets("simple_data", "", error = "Memcached error #192 'Path not exists'", field = "simple_data : empty path does not exist - dictionary", result = result)
        self.log.info("simple_data :: path does not exist")
        self.error_gets("simple_data", "does_not_exist", error = "Memcached error #192 'Path not exists'", field = "simple_data : path does not exist - dictionary", result = result)
        self.log.info("simple_data :: path does not exist - array, negavtie index")
        self.error_gets("simple_data", "array[-1]", error = "Memcached error #192 'Path not exists'", field = "simple_data : path does not exist - array, negavtie index", result = result)
        self.log.info("simple_data :: path does not exist - array, out of bounds index")
        self.error_gets("simple_data", "array[200]", error = "Memcached error #192 'Path not exists'", field = "simple_data : path does not exist - array, out of bounds index", result = result)
        self.log.info("simple_data :: document does not exist")
        self.error_gets("does_not_exist", "does_not_exist", error = "Memcached error #1 'Not found'", field = "simple_data : document does not exist", result = result)
        # Tests for Simple Data Set
        self.log.info("nested_data :: empty path does not exist")
        new_path = self.generate_path(20, "")
        self.error_gets("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : empty path does not exist - dictionary", result = result)
        self.log.info("nested_data :: path does not exist")
        new_path = self.generate_path(20, "does_not_exist")
        self.error_gets("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist - dictionary", result = result)
        self.log.info("nested_data :: path does not exist - array, negavtie index")
        new_path = self.generate_path(20, "array[-1]")
        self.error_gets("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist - array, negavtie index", result = result)
        self.log.info("nested_data ::path does not exist - array, out of bounds index")
        new_path = self.generate_path(20, "array[200]")
        self.error_gets("normal_nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist - array, out of bounds index", result = result)
        # Tests for Nested Data with long path
        self.log.info("long_nested_data ::nested_data : path does not exist - too big path")
        new_path = self.generate_path(40, "field")
        self.error_gets("nested_data", new_path, error = "Memcached error #192 'Path not exists'", field = "nested_data : path does not exist - too big path", result = result)
        self.assertTrue(len(result) == 0, result)

    def error_gets(self, in_key, path, error = "error", field  = "field", result = {}):
        try:
        	opaque, cas, data = self.client.get_sd(in_key, path)
        	result[field]  = "There were no errors. Error expected: %s" % error
        except Exception as ex:
        	if (str(ex).find(error) == -1):
        		self.log.info(str(ex))
        		result[field]  = "Error is incorrect.Actual %s.Expected: %s." %(str(ex), error)
