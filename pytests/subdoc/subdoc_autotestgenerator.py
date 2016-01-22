from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *
from lib.couchbase_helper.subdoc_helper import SubdocHelper
from subdoc_base import SubdocBaseTest
import copy, json
import random

class SubdocAutoTestGenerator(SubdocBaseTest):
    def setUp(self):
        super(SubdocAutoTestGenerator, self).setUp()
        self.nesting_level =  self.input.param("nesting_level",0)
        self.client = self.direct_client(self.master, self.buckets[0])
        self.subdoc_gen_helper = SubdocHelper()

    def tearDown(self):
        super(SubdocAutoTestGenerator, self).tearDown()

    def test_readonly(self):
    	error_result ={}
        data_set =  self.generate_json_for_nesting()
        base_json = self.generate_json_for_nesting()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_readonly"
        jsonDump = json.dumps(json_document)
        self.client.set(data_key, 0, 0, jsonDump)
        pairs = {}
        self.subdoc_gen_helper.find_pairs(json_document,"", pairs)
        for path in pairs.keys():
            self.log.info(" Analyzing path {0}".format(path))
            opaque, cas, data = self.client.get_sd(data_key, path)
            data = json.loads(data)
            self.log.info(data)
            self.log.info(pairs[path])
            if data != pairs[path]:
            	error_result[path] = "expected {0}, actual = {1}".format(pairs[path], data)
        self.assertTrue(len(error_result) == 0, error_result)

    def test_exists(self):
    	error_result ={}
        data_set =  self.generate_json_for_nesting()
        base_json = self.generate_json_for_nesting()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_readonly"
        jsonDump = json.dumps(json_document)
        self.client.set(data_key, 0, 0, jsonDump)
        pairs = {}
        self.subdoc_gen_helper.find_pairs(json_document,"", pairs)
        for path in pairs.keys():
            self.log.info(" Analyzing path {0}".format(path))
            try:
            	self.client.exists_sd(data_key, path)
            except Exception, ex:
            	error_result[path] = str(ex)
        self.assertTrue(len(error_result) == 0, error_result)

    def test_mutation_dict_operations(self):
        error_result = {}
        self.number_of_operations =  self.input.param("number_of_operations",10)
        data_set =  self.generate_json_for_nesting()
        base_json = self.generate_json_for_nesting()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_mutation_dict_operations"
        jsonDump = json.dumps(json_document)
        self.client.set(data_key, 0, 0, jsonDump)
        operations = self.subdoc_gen_helper.run_operations_dict_slow(json_document, self.number_of_operations)
        for operation in operations:
            function = getattr(self, operation["subdoc_api_function_applied"])
            try:
                function(self.client, data_key, operation["new_path_impacted_after_mutation_operation"], json.dumps(operation["data_value"]))
            except Exception, ex:
                self.log.info(ex)
                for key in operation:
                    self.log.info(" {0} : {1}".format(key, operation[key]))
                raise
        json_document  = copy.deepcopy(operations[len(operations)-1]["mutated_data_set"])
        pairs = {}
        self.subdoc_gen_helper.find_pairs(json_document,"", pairs)
        for path in pairs.keys():
            self.log.info(" Analyzing path {0}".format(path))
            opaque, cas, data = self.client.get_sd(data_key, path)
            data = json.loads(data)
            if data != pairs[path]:
                error_result[path] = "expected {0}, actual = {1}".format(pairs[path], data)
        self.assertTrue(len(error_result) == 0, error_result)

    def test_mutation_array_operations(self):
        error_result = {}
        self.number_of_operations =  self.input.param("number_of_operations",10)
        data_set =  self.generate_json_for_nesting()
        base_json = self.generate_json_for_nesting()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_mutation_dict_operations"
        jsonDump = json.dumps(json_document)
        self.client.set(data_key, 0, 0, jsonDump)
        operations = self.subdoc_gen_helper.run_operations_array_slow(json_document, self.number_of_operations)
        for operation in operations:
            function = getattr(self, operation["subdoc_api_function_applied"])
            try:
                function(self.client, data_key, operation["new_path_impacted_after_mutation_operation"], json.dumps(operation["data_value"]))
            except Exception, ex:
                self.log.info(ex)
                for key in operation:
                    self.log.info(" {0} : {1}".format(key, operation[key]))
                raise
        json_document  = copy.deepcopy(operations[len(operations)-1]["mutated_data_set"])
        pairs = {}
        self.subdoc_gen_helper.find_pairs(json_document,"", pairs)
        for path in pairs.keys():
            self.log.info(" Analyzing path {0}".format(path))
            opaque, cas, data = self.client.get_sd(data_key, path)
            data = json.loads(data)
            if data != pairs[path]:
                error_result[path] = "expected {0}, actual = {1}".format(pairs[path], data)
        self.assertTrue(len(error_result) == 0, error_result)

    def test_mutation_operations(self):
        error_result = {}
        self.number_of_operations =  self.input.param("number_of_operations",10)
        data_set =  self.generate_json_for_nesting()
        base_json = self.generate_json_for_nesting()
        json_document = self.generate_nested(base_json, data_set, self.nesting_level)
        data_key = "test_mutation_operations"
        jsonDump = json.dumps(json_document)
        self.client.set(data_key, 0, 0, jsonDump)
        operations = self.subdoc_gen_helper.run_operations_slow(json_document, self.number_of_operations)
        for operation in operations:
            function = getattr(self, operation["subdoc_api_function_applied"])
            try:
                function(self.client, data_key, operation["new_path_impacted_after_mutation_operation"], json.dumps(operation["data_value"]))
            except Exception, ex:
                self.log.info(ex)
                self.log.info(operation)
                for key in operation:
                    self.log.info(" {0} : {1}".format(key, operation[key]))
                raise
        pairs = {}
        self.subdoc_gen_helper.find_pairs(json_document,"", pairs)
        for path in pairs.keys():
            self.log.info(" Analyzing path {0}".format(path))
            opaque, cas, data = self.client.get_sd(data_key, path)
            data = json.loads(data)
            if data != pairs[path]:
                error_result[path] = "expected {0}, actual = {1}".format(pairs[path], data)
        self.assertTrue(len(error_result) == 0, error_result)


# SUB DOC COMMANDS

# GENERIC COMMANDS
    def delete(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" delete ----> {0} ".format(path))
            self.client.delete_sd(key, path)
        except Exception as e:
            raise

    def replace(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" replace ----> {0} :: {1}".format(path, value))
            self.client.replace_sd(key, path, value)
        except Exception as e:
            raise

    def get(self, client, key = '', path = '', value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.get_sd(key, new_path)
        except Exception as e:
            raise

    def exists(self, client, key = '', path = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.exists_sd(key, new_path)
        except Exception as e:
            raise
    def counter(self, client, key = '', path = '', value = None):
        try:
            self.client.counter_sd(key, path, value)
        except Exception as e:
            raise

# DICTIONARY SPECIFIC COMMANDS
    def dict_add(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" dict_add ----> {0} :: {1}".format(path, value))
            self.client.dict_add_sd(key, path, value)
        except Exception as e:
            raise

    def dict_upsert(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" dict_upsert ----> {0} :: {1}".format(path, value))
            self.client.dict_upsert_sd(key, path, value)
        except Exception as e:
            raise


# ARRAY SPECIFIC COMMANDS
    def array_add_last(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" array_add_last ----> {0} :: {1}".format(path, value))
            self.client.array_push_last_sd(key, path, value)
        except Exception as e:
            raise

    def array_add_first(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" array_add_first ----> {0} :: {1}".format(path, value))
            self.client.array_push_first_sd(key, path, value)
        except Exception as e:
            raise

    def array_add_unique(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" array_add_unique ----> {0} :: {1}".format(path, value))
            self.client.array_add_unique_sd(key, path, value)
        except Exception as e:
            raise

    def array_add_insert(self, client, key = '', path = '', value = None):
        try:
            self.log.info(" array_add_insert ----> {0} :: {1}".format(path, value))
            self.client.array_add_insert_sd(key, path, value)
        except Exception as e:
            raise