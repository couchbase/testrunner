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

    def test_get_strings(self):
    	dict = {}
    	result = True
    	self.key = "generate_simple_data_strings"
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

    def get_string_and_verify_return(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        return data == self.json[path], data

    def get_and_verify_return(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        return str(data) == str(self.json[path]), data
