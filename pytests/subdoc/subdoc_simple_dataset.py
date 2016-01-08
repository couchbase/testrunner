from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *
from subdoc_base import SubdocBaseTest
import copy, json

class SubdocSimpleDataset(SubdocBaseTest):
    def setUp(self):
        super(SubdocSimpleDataset, self).setUp()
        self.client = self.direct_client(self.master, self.buckets[0])

    def tearDown(self):
        super(SubdocSimpleDataset, self).tearDown()

    def test_get_numbers(self):
    	dict = {}
    	result = True
    	self.key = "simple_dataset_numbers"
    	self.json =  self.generate_simple_data_numbers()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
    	for key in self.json.keys():
    		value = self.json[key]
    		logic, data_return  =  self.get_and_verify_return( self.client, key = self.key, path = key)
    		if not logic:
    			dict[key] = {"expected":self.json[key], "actual":data_return}
    		result = result and logic
    	self.assertTrue(result, dict)

    def test_get_boolean(self):
        dict = {}
        result = True
        self.key = "simple_dataset_boolean"
        self.json =  self.generate_simple_data_boolean()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            value = self.json[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": self.json[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_get_array_numbers(self):
    	dict = {}
    	result = True
    	self.key = "simple_dataset_array_numbers"
    	self.json =  self.generate_simple_data_array_of_numbers()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
    	for key in self.json.keys():
    		value = self.json[key]
    		logic, data_return  =  self.get_and_verify_return( self.client, key = self.key, path = key)
    		if not logic:
    			dict[key] = {"expected":self.json[key], "actual":data_return}
    		result = result and logic
    	self.assertTrue(result, dict)

    def test_get_array_strings(self):
    	dict = {}
    	result = True
    	self.key = "generate_simple_data_array_strings"
    	self.json =  self.generate_simple_data_strings()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
    	for key in self.json.keys():
    		value = self.json[key]
    		logic, data_return  =  self.get_string_and_verify_return(self.client, key = self.key, path = key)
    		if not logic:
    			dict[key] = {"expected":self.json[key], "actual":data_return}
    		result = result and logic
    	self.assertTrue(result, dict)

    def test_get_mix_arrays(self):
        dict = {}
        result = True
        self.key = "generate_simple_data_mix_arrays"
        self.json =  self.generate_simple_data_mix_arrays()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            value = self.json[key]
            logic, data_return  =  self.get_string_and_verify_return(self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected":self.json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_get_element_arrays(self):
        dict = {}
        result = True
        self.key = "element_arrays"
        self.json =  self.generate_simple_arrays()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        key_single_dimension_path = "single_dimension_array[0]"
        self.get_and_verify_with_value(self.client, self.key, key_single_dimension_path, str(self.json["single_dimension_array"][0]))
        key_two_dimension_path = "two_dimension_array[0][0]"
        self.get_and_verify_with_value(self.client, self.key, key_two_dimension_path, str(self.json["two_dimension_array"][0][0]))
        self.assertTrue(result, dict)

    def test_get_numbers_boundary(self):
    	dict = {}
    	result = True
    	self.key = "generate_simple_data_numbers_boundary"
    	self.json =  self.generate_simple_data_array_of_numbers()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
    	for key in self.json.keys():
    		value = self.json[key]
    		logic, data_return  =  self.get_and_verify_return( self.client, key = self.key, path = key)
    		if not logic:
    			dict[key] = {"expected":self.json[key], "actual":data_return}
    		result = result and logic
    	self.assertTrue(result, dict)

    def get_and_verify(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        self.assertTrue(str(data) == str(self.json[path]), msg="data not returned correctly")

    def get_and_verify_with_value(self, client, key = '', path = '', value = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
            data = json.loads(data)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        self.assertTrue(str(data) == value, msg="data not returned correctly")

    def get_string_and_verify_return(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
            data = json.loads(data)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        return data == self.json[path], data

    def get_and_verify_return(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        return str(data) == str(self.json[path]), data
