#!/usr/bin/env python
import copy
import random
import json
from random_gen import RandomDataGenerator

class SubdocHelper():
    def __init__(self):
      self.randomDataGenerator = RandomDataGenerator()

    def find_json_paths(self, json = {}, path  = "", pairs = {}):
    	for key in json.keys():
    		prefix = ""
    		if path != "":
    			prefix = path+"."
    		if isinstance(json[key], dict):
    			self.find_json_paths(json[key], prefix + key, pairs)
    		elif isinstance(json[key], list):
    			self.find_array_paths(json[key], prefix + key, pairs)
    		pairs[prefix+key] = json[key]

    def find_array_paths(self, array = [], path  = "", pairs = {}):
    	index = 0
    	for element in array:
    		if isinstance(element, dict):
    			self.find_json_paths(element, path + "["+str(index)+"]", pairs)
    		elif isinstance(element, list):
    			self.find_array_paths(element, path + "["+str(index)+"]", pairs)
    		pairs[path+"["+str(index)+"]"] =element
    		index += 1

    def find_pairs(self, data_set, path  = "", pairs = {}):
    	if isinstance(data_set, dict):
    		for key in data_set.keys():
    			prefix = ""
    			if path != "":
    				prefix = path+"."
    			if isinstance(data_set[key], dict):
    				self.find_pairs(data_set[key], prefix + key, pairs)
    			elif isinstance(data_set[key], list):
    				self.find_pairs(data_set[key], prefix + key, pairs)
    			pairs[prefix+key] = data_set[key]
    	elif  isinstance(data_set, list):
    		index = 0
    		for element in data_set:
    			if isinstance(element, dict):
    				self.find_pairs(element, path + "["+str(index)+"]", pairs)
    			elif isinstance(element, list):
    				self.find_pairs(element, path + "["+str(index)+"]", pairs)
    			pairs[path+"["+str(index)+"]"] =element
    			index += 1

    def find_operations(self, data_set, pairs, max_number_operations, filter_paths = []):
      trials = 0
      for i in range(10000):
        if len(pairs.keys()) == 0:
          find_pairs(data_set,"", pairs)
        key = random.choice(pairs.keys())
        if self.isPathPresent(key, filter_paths):
          pairs.pop(key)
          filter_paths.append(key)
        else:
          operation_type, operation  = self.pick_operations(pair[key])
          if isMutationOperation(operation):
            pairs.pop(key)
            filter_paths.append(key)
            # Mutation operation
            data_set  = self.doMutationOperation(operation_type, operation, data_set)
          else:
            self.doNoMutationOperation(operation, data_set, pair[key])
        trial += 1
        if trial == max_number_operations:
          return

    def parse_and_get_data(self, data_set, path):
      print "parse_and_get_data"
      for key in path.split("."):
        if "[" not in key:
          data_set = data_set[key]
        else:
          if key.split("[")[0] != '':
            data_set = data_set[key.split("[")[0]]
          for k in key.split("[")[1:]:
            index = int(k.replace("]",""))
            data_set = data_set[index]
      return data_set


    def gen_data(self):
        return self.randomDataGenerator.gen_data()

    def doNoMutationOperation(self,  operation, data_set, value):
      print "doNoMutationOperation"

    def doMutationOperation(self, operation_type, operation, data_set):
      print "doMutationOperation"

    def isMutationOperation(self, operation):
      return False

    def python_based_dict_add(self, path = "", original_dataset = {}):
      field_name, data_set = self.gen_data()
      if path == "":
        modify_dataset = original_dataset
        modify_dataset[field_name] = data_set
      else:
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
        modify_dataset[field_name] = data_set
      if path != "":
        path = path + "." + field_name
      else:
        path = field_name
      return path, data_set, original_dataset

    def python_based_get(self, path = "", original_dataset = {}):
      return path, self.parse_and_get_data(original_dataset, path), None

    def python_based_exists(self, path = "", original_dataset = {}):
      return path, None, None

    def python_based_array_replace(self, path = "", original_dataset = {}):
      field_name, data_set = self.gen_data()
      modify_dataset  = self.parse_and_get_data(original_dataset, path)
      index = self.randomDataGenerator.random_int(max_int = len(modify_dataset) - 1)
      modify_dataset[index] = data_set
      return path+"["+str(index)+"]", data_set, original_dataset

    def python_based_array_add_insert(self, path = "", original_dataset = None):
      self.python_based_array_add_insert(path, original_dataset = original_dataset, type = "insert")

    def python_based_array_add_first(self, path = "", original_dataset = None):
      self.python_based_array_add_insert(path, original_dataset = original_dataset, type = "first")

    def python_based_array_add_last(self, path = "", original_dataset = None):
      self.python_based_array_add_insert(path, original_dataset = original_dataset, type = "last")

    def python_based_array_add_unique(self, path = "", original_dataset = None):
      self.python_based_array_add_insert(path, original_dataset = original_dataset, type = "unique")

    def python_based_array_add(self, path = "", original_dataset = {}, type = "insert"):
      field_name, data_set = self.gen_data()
      modify_dataset  = self.parse_and_get_data(original_dataset, path)
      if type == "first":
        index = 0
      elif type == "last" or type == "unique":
        index = len(original_dataset)
      else:
        index = self.randomDataGenerator.random_int(max_int = len(modify_dataset) - 1)
        path = path+"["+str(index)+"]"
      modify_dataset.insert(index,data_set)
      return path, data_set, original_dataset

    def python_based_dict_delete(self, path = "", original_dataset = None):
      modify_dataset = original_dataset
      if path != "":
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
      key_to_remove = random.choice(modify_dataset.keys())
      modify_dataset.pop(key_to_remove)
      if path == "":
        path  = key_to_remove
      else:
        path = path+"."+key_to_remove
      return path, None, original_dataset

    def python_based_array_delete(self, path = "", original_dataset = None):
      modify_dataset  = original_dataset
      if path != "":
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
      index = random.choice(range(len(modify_dataset)))
      modify_dataset.pop(index)
      if path == "":
        path  = "["+str(index)+"]"
      else:
        path = path+"["+str(index)+"]"
      return path, None,  original_dataset

    def python_based_dict_upsert_replace(self, path = "", original_dataset = None):
      field_name, data_set = self.gen_data()
      if path == "":
        field_name = random.choice(original_dataset.keys())
        modify_dataset = original_dataset
        modify_dataset[field_name] = data_set
      else:
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
        field_name = random.choice(original_dataset.keys())
        modify_dataset[field_name] = data_set
      if path != "":
        path = path + "." + field_name
      else:
        path = field_name
      return path, data_set, original_dataset

    def python_based_dict_replace(self, path = "", original_dataset = {}):
      return self.python_based_dict_upsert_replace(path = path, original_dataset = original_dataset)

    def python_based_dict_upsert_add(self, path = "", original_dataset = {}):
      return self.python_based_dict_add(path = path, original_dataset = original_dataset)

    def pick_operations(self, path = "", data = None):
      array_ops ={
      "array_add_first" : {"python":"python_based_array_add_first", "subdoc_api":"array_add_first"},
      "array_add_last": {"python":"python_based_array_add_last", "subdoc_api":"array_add_last"},
      "array_add_unique": {"python":"python_based_array_add_unique", "subdoc_api":"array_add_unqiue"},
      "array_add_insert": {"python":"python_based_array_add_insert", "subdoc_api":"array_add_insert"},
      "array_get": {"python":"python_based_get", "subdoc_api":"get"},
      "array_exists": {"python":"python_based_exists", "subdoc_api":"exists"},
      "array_delete": {"python":"python_based_array_delete", "subdoc_api":"delete"},
      "array_replace": {"python":"python_based_array_replace", "subdoc_api":"replace"}
      }
      dict_ops = {
      "dict_add": {"python":"python_based_dict_add", "subdoc_api":"dict_add"},
      "dict_upsert_add": {"python":"python_based_dict_upsert_add", "subdoc_api":"dict_upsert"},
      "dict_upsert_replace": {"python":"python_based_dict_upsert_replace", "subdoc_api":"dict_upsert"},
      "dict_get": {"python":"python_based_python_based_dict_get", "subdoc_api":"get"},
      "dict_exists": {"python":"python_based_dict_exists", "subdoc_api":"exists"},
      "dict_delete": {"python":"python_based_dict_delete", "subdoc_api":"delete"},
      "dict_replace": {"python":"python_based_dict_replace", "subdoc_api":"replace"}
      }
      if "[" in path or isinstance(data, list):
        key = random.choice(array_ops.keys())
        return key, array_ops[key]
      else:
        key = random.choice(dict_ops.keys())
        return key, dict_ops[key]

    def isPathPresent(self, path, filter_paths = []):
      for path_parent in filter_paths:
        if path == path_parent[:len(path)]:
          return True
      return False

    def show_all_paths(self, pairs, data_set):
      for path in pairs.keys():
        parse_path_data = self.parse_and_get_data(data_set, path)
        print "PATH = {0} || VALUE = {1} || PARSE PATH = {2} ".format(path, pairs[path], parse_path_data)
        key, ops_info = self.pick_operations(path, parse_path_data)
        print key
        print "Run python operation {0}".format(ops_info["python"])
        print "Run equiavlent subdoc api operation {0}".format(ops_info["subdoc_api"])

# SUB DOC COMMANDS

# GENERIC COMMANDS
    def delete(self, client, key = '', path = ''):
        try:
            self.client.delete_sd(key, path)
        except Exception as e:
            raise

    def replace(self, client, key = '', path = '', value = None):
        try:
            self.client.replace_sd(key, path, value)
        except Exception as e:
            raise

    def get(self, client, key = '', path = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.get_sd(key, new_path)
            return json.loads(data)
        except Exception as e:
            raise

    def exists(self, client, key = '', path = '', expected_value = None):
        new_path = self.generate_path(self.nesting_level, path)
        try:
            opaque, cas, data = self.client.exists_sd(key, new_path)
            return json.loads(data)
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
            self.client.dict_add_sd(key, path, value)
        except Exception as e:
            raise

    def dict_upsert(self, client, key = '', path = '', value = None):
        try:
            self.client.dict_upsert_sd(key, path, value)
        except Exception as e:
            raise


# ARRAY SPECIFIC COMMANDS
    def array_add_last(self, client, key = '', path = '', value = None):
        try:
            self.client.array_push_last_sd(key, path, value)
        except Exception as e:
            raise

    def array_add_first(self, client, key = '', path = '', value = None):
        try:
            self.client.array_push_first_sd(key, path, value)
        except Exception as e:
            raise

    def array_add_unique(self, client, key = '', path = '', value = None):
        try:
            self.client.array_add_unique_sd(key, path, value)
        except Exception as e:
            raise

    def array_add_insert(self, client, key = '', path = '', value = None):
        try:
            self.client.array_add_insert_sd(key, path, value)
        except Exception as e:
            raise

if __name__=="__main__":
    helper = SubdocHelper()
    pairs= {}
    nest_json = {"string":"data", "integer":3, "simple_json":{"string":"data", "integer":3, "array":[0,1,2, {"field":"value", "array":[0,1,2,3]}]}, "array":[0,1,2]}
    json_document = {"string":"data", "integer":3,"array":[1,2,3, [0, 1, 2, [3]]], "json":nest_json}
    helper.find_pairs(json_document,"", pairs)
    helper.show_all_paths(pairs, json_document)
    pairs= {}
    print  "++++++++++++++++++++++++++"
    nest_json = [{"field_1":1}, {"field_2":2}, 3]
    helper.find_pairs(nest_json,"", pairs)
    helper.show_all_paths(pairs, nest_json)