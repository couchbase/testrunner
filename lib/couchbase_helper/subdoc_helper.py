#!/usr/bin/env python
import copy
import random
import json
import sys
from .random_gen import RandomDataGenerator

class SubdocHelper():
    def __init__(self):
      self.randomDataGenerator = RandomDataGenerator()

    def _find_data_type(self, data, data_type = "any"):
      if data_type != "any" and data_type == "dict" and isinstance(data, dict):
        return True
      elif data_type != "any" and data_type == "array" and isinstance(data, list):
        return True
      elif data_type == "any" and (isinstance(data, dict) or isinstance(data, list)):
        return True
      return False

    def find_pairs_data_type(self, data_set, path  = "", pairs = {}, data_type = "any"):
      if isinstance(data_set, dict):
        for key in list(data_set.keys()):
          prefix = ""
          if path != "":
            prefix = path+"."
          if isinstance(data_set[key], dict):
            self.find_pairs_data_type(data_set[key], prefix + key, pairs)
          elif isinstance(data_set[key], list):
            self.find_pairs_data_type(data_set[key], prefix + key, pairs)
          if self._find_data_type(data_set[key], data_type = data_type):
            pairs[prefix+key] = data_set[key]
      elif  isinstance(data_set, list):
        index = 0
        for element in data_set:
          if isinstance(element, dict):
            self.find_pairs_data_type(element, path + "["+str(index)+"]", pairs)
          elif isinstance(element, list):
            self.find_pairs_data_type(element, path + "["+str(index)+"]", pairs)
          if self._find_data_type(element, data_type = data_type):
            pairs[path+"["+str(index)+"]"] =element
          index += 1

    def find_pairs(self, data_set, path  = "", pairs = {}):
    	if isinstance(data_set, dict):
    		for key in list(data_set.keys()):
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

    '''  Find out combination of operations to be executed for a given JSON document '''
    def build_concurrent_operations(self, data_set = None, max_number_operations = 10, seed = None, mutation_operation_type = "any", force_operation_type = None):
      # FIX THE SEED
      if seed != None:
        random.seed(seed)
        self.randomDataGenerator.set_seed(seed)
      filter_paths = []
      pairs = {}
      operation_definition = []
      operation_index = 1
      self.find_pairs_data_type(data_set,"", pairs = pairs, data_type = mutation_operation_type)
      for i in range(10000):
        if len(list(pairs.keys())) == 0:
          return operation_definition
        key = random.choice(list(pairs.keys()))
        if self.isPathPresent(key, filter_paths):
          pairs.pop(key)
          filter_paths.append(key)
        else:
          if mutation_operation_type == "any":
            operation = self.pick_operations(pairs[key], operation = force_operation_type)
          elif mutation_operation_type == "dict":
            operation = self.pick_dict_operations(pairs[key], operation = force_operation_type)
          else:
            operation = self.pick_array_operations(pairs[key], operation = force_operation_type)
          copy_of_original_dataset = copy.deepcopy(data_set)
          if operation["mutate"] == True:
            function = getattr(self, operation["python"])
            new_path, data = function(key, data_set)
            if new_path != None:
              pairs.pop(key)
              filter_path = new_path
              if "[" in filter_path:
                filter_path = self.trim_path(filter_path, "[")
              filter_paths.append(filter_path)
              operation_definition.append({
                  "data_value":data,
                  "path_impacted_by_mutation_operation":key,
                  "new_path_impacted_after_mutation_operation":new_path,
                  "original_dataset":copy_of_original_dataset,
                  "mutated_data_set": copy.deepcopy(data_set),
                  "python_based_function_applied":operation["python"],
                  "subdoc_api_function_applied":operation["subdoc_api"]
                  })
              operation_index += 1
            elif operation["mutate"] and new_path == None:
              print("mutation failed")
        if operation_index == max_number_operations:
          return operation_definition
      return operation_definition

    '''  Find out combination of operations to be executed for a given JSON document '''
    def build_sequence_operations(self, data_set = None, max_number_operations = 10, seed = None, mutation_operation_type = "any", force_operation_type = None):
      # FIX THE SEED
      if seed != None:
        random.seed(seed)
        self.randomDataGenerator.set_seed(seed)
      operation_definition = []
      operation_index = 0
      while True:
        pairs = {}
        self.find_pairs_data_type(data_set,"", pairs = pairs, data_type = mutation_operation_type)
        if len(pairs) == 0:
          return operation_definition
        key = random.choice(list(pairs.keys()))
        if mutation_operation_type  == "any":
          operation = self.pick_operations(pairs[key], operation = force_operation_type)
        elif mutation_operation_type  == "dict":
          operation = self.pick_dict_operations(pairs[key], operation = force_operation_type)
        elif mutation_operation_type  == "array":
          operation = self.pick_array_operations(pairs[key], operation = force_operation_type)
        if operation["mutate"] == True:
          copy_of_original_dataset = copy.deepcopy(data_set)
          function = getattr(self, operation["python"])
          new_path, data = function(key, data_set)
          if new_path != None:
            pairs.pop(key)
            operation_definition.append({
                  "data_value":data,
                  "path_impacted_by_mutation_operation":key,
                  "new_path_impacted_after_mutation_operation":new_path,
                  "original_dataset":copy_of_original_dataset,
                  "mutated_data_set": copy.deepcopy(data_set),
                  "python_based_function_applied":operation["python"],
                  "subdoc_api_function_applied":operation["subdoc_api"]
                  })
            operation_index += 1
          if operation_index == max_number_operations:
            return operation_definition
        else:
          pairs.pop(key)
      return operation_definition

    def parse_and_get_data(self, data_set, path):
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

    def python_based_dict_add(self, path = "", original_dataset = {}):
      field_name, data_set = self.gen_data()
      if path == "":
        modify_dataset = original_dataset
        modify_dataset[field_name] = copy.deepcopy(data_set)
      else:
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
        modify_dataset[field_name] = copy.deepcopy(data_set)
      if path != "":
        path = path + "." + field_name
      else:
        path = field_name
      return path, data_set

    def python_based_get(self, path = "", original_dataset = {}):
      return path, self.parse_and_get_data(original_dataset, path)

    def python_based_exists(self, path = "", original_dataset = {}):
      return path, None

    def python_based_array_replace(self, path = "", original_dataset = {}):
      field_name, data_set = self.gen_data()
      modify_dataset  = self.parse_and_get_data(original_dataset, path)
      if len(modify_dataset) == 0:
        return None, None
      index = random.choice(list(range(len(modify_dataset))))
      modify_dataset[index] = copy.deepcopy(data_set)
      return path+"["+str(index)+"]", data_set

    def python_based_array_add_insert(self, path = "", original_dataset = None):
      return self.python_based_array_add(path, original_dataset = original_dataset, type = "insert")

    def python_based_array_add_first(self, path = "", original_dataset = None):
      return self.python_based_array_add(path, original_dataset = original_dataset, type = "first")

    def python_based_array_add_last(self, path = "", original_dataset = None):
      return self.python_based_array_add(path, original_dataset = original_dataset, type = "last")

    def python_based_array_add_unique(self, path = "", original_dataset = None):
      return self.python_based_array_add(path, original_dataset = original_dataset, type = "unique")

    def python_based_array_add(self, path = "", original_dataset = {}, type = "insert"):
      field_name, data_set = self.randomDataGenerator.gen_data_no_json()
      modify_dataset  = self.parse_and_get_data(original_dataset, path)
      if type == "first":
        index = 0
      elif type == "last":
        modify_dataset.append(copy.deepcopy(data_set))
        return path, data_set
      elif type == "unique":
        for element in modify_dataset:
          if isinstance(element, dict) or isinstance(element, list) or element == data_set:
            return None, None
        modify_dataset.append(copy.deepcopy(data_set))
        return path, data_set
      else:
        if len(modify_dataset) == 0:
          return None, None
        index = random.choice(list(range(len(modify_dataset))))
        path = path+"["+str(index)+"]"
      modify_dataset.insert(index,copy.deepcopy(data_set))
      return path, data_set

    def python_based_dict_delete(self, path = "", original_dataset = None):
      modify_dataset = original_dataset
      if path != "":
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
      if(len(list(modify_dataset.keys()))  == 0):
        return None, None
      key_to_remove = random.choice(list(modify_dataset.keys()))
      modify_dataset.pop(key_to_remove)
      if path == "":
        path  = key_to_remove
      else:
        path = path+"."+key_to_remove
      return path, None

    def python_based_array_delete(self, path = "", original_dataset = None):
      modify_dataset  = original_dataset
      if path != "":
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
      if len(modify_dataset) == 0:
        return None, None
      index = random.choice(list(range(len(modify_dataset))))
      modify_dataset.pop(index)
      if path == "":
        path  = "["+str(index)+"]"
      else:
        path = path+"["+str(index)+"]"
      return path, None

    def python_based_dict_upsert_replace(self, path = "", original_dataset = None):
      field_name, data_set = self.gen_data()
      if path == "":
        if(len(list(original_dataset.keys())) == 0):
          return None, None
        field_name = random.choice(list(original_dataset.keys()))
        modify_dataset = original_dataset
        modify_dataset[field_name] = copy.deepcopy(data_set)
      else:
        modify_dataset  = self.parse_and_get_data(original_dataset, path)
        if(len(list(modify_dataset.keys())) == 0):
          return None, None
        field_name = random.choice(list(modify_dataset.keys()))
        modify_dataset[field_name] = copy.deepcopy(data_set)
      if path != "":
        path = path + "." + field_name
      else:
        path = field_name
      return path, data_set

    def python_based_dict_replace(self, path = "", original_dataset = {}):
      return self.python_based_dict_upsert_replace(path = path, original_dataset = original_dataset)

    def python_based_dict_upsert_add(self, path = "", original_dataset = {}):
      return self.python_based_dict_add(path = path, original_dataset = original_dataset)

    def pick_operations(self, data = None, operation = None):
      if isinstance(data, list):
        return self.pick_array_operations(data = data, operation = operation)
      elif isinstance(data, dict):
        return self.pick_dict_operations(data = data, operation = operation)
      else:
        return {"mutate":False}

    def pick_array_operations(self, data = None, operation = None):
      array_ops ={
      "array_add_first" : {"python":"python_based_array_add_first", "subdoc_api":"array_add_first", "mutate":True},
      "array_add_last": {"python":"python_based_array_add_last", "subdoc_api":"array_add_last", "mutate":True},
      "array_add_unique": {"python":"python_based_array_add_unique", "subdoc_api":"array_add_unique", "mutate":True},
      "array_add_insert": {"python":"python_based_array_add_insert", "subdoc_api":"array_add_insert", "mutate":True},
      "array_delete": {"python":"python_based_array_delete", "subdoc_api":"delete", "mutate":True},
      "array_replace": {"python":"python_based_array_replace", "subdoc_api":"replace", "mutate":True}
      }
      if isinstance(data, list):
        if operation == None:
          operation = random.choice(list(array_ops.keys()))
        return array_ops[operation]
      else:
        return {"mutate":False}

    def pick_dict_operations(self, data = None, operation = None):
      dict_ops = {
      "dict_add": {"python":"python_based_dict_add", "subdoc_api":"dict_add", "mutate":True},
      "dict_upsert_add": {"python":"python_based_dict_upsert_add", "subdoc_api":"dict_upsert", "mutate":True},
      "dict_upsert_replace": {"python":"python_based_dict_upsert_replace", "subdoc_api":"dict_upsert", "mutate":True},
      "dict_delete": {"python":"python_based_dict_delete", "subdoc_api":"delete", "mutate":True},
      "dict_replace": {"python":"python_based_dict_replace", "subdoc_api":"replace", "mutate":True}
      }
      if isinstance(data, dict):
        if operation == None:
          operation = random.choice(list(dict_ops.keys()))
        return dict_ops[operation]
      else:
        return {"mutate":False}

    def isPathPresent(self, path, filter_paths = []):
      for path_parent in filter_paths:
        if path[:len(path_parent)] == path_parent:
          return True
      return False

    def trim_path(self, path, search_string):
      return path[:path.rfind(search_string)]


    def show_all_operations(self, ops = []):
      operation_index = 0
      for operation in ops:
        print("++++++++++++++++++ OPERATION {0} ++++++++++++++++++++++".format(operation_index))
        operation_index += 1
        for field in list(operation.keys()):
          print("{0} :: {1}".format(field, operation[field]))

    def show_all_paths(self, pairs, data_set):
      for path in list(pairs.keys()):
        parse_path_data = self.parse_and_get_data(data_set, path)
        print("PATH = {0} || VALUE = {1} || PARSE PATH = {2} ".format(path, pairs[path], parse_path_data))
        key, ops_info = self.pick_operations(parse_path_data)
        print(key)
        print("Run python operation {0}".format(ops_info["python"]))
        print("Run equivalent subdoc api operation {0}".format(ops_info["subdoc_api"]))

    def gen_input_file(self, file_name = "sample_json.txt", number_of_test_cases = 100):
      dump_file = open(file_name, 'wb')
      for x in range(number_of_test_cases):
        json_document = {}
        json_document["seed"] = x
        json_document["json_document"] = self.randomDataGenerator.random_json()
        dump_file.write(json.dumps(json_document))
        dump_file.write('\n')
      dump_file.close()

if __name__=="__main__":
    helper = SubdocHelper()
    json_document = {"json":{"field":{"json":{"json":{"field_name":1}}}, "json":{"array":[0]}}, "array":[0,1,2]}
    ops = helper.build_concurrent_operations(json_document, max_number_operations = 10, seed=10, mutation_operation_type = "dict", force_operation_type = "dict_add")
    helper.show_all_operations(ops)
    json_document = {"json":{"field":{"json":{"json":{"field_name":1}}}, "json":{"array":[0]}}, "array":[0,1,2]}
    ops = helper.build_concurrent_operations(json_document, max_number_operations = 10, seed=10, mutation_operation_type = "array", force_operation_type = "array_add_insert")
    helper.show_all_operations(ops)
    json_document = {"json":{"field":{"json":{"json":{"field_name":1}}}, "json":{"array":[0]}}, "array":[0,1,2]}
    ops = helper.build_concurrent_operations(json_document, max_number_operations = 10, seed=10, mutation_operation_type = "all")
    helper.show_all_operations(ops)
    json_document = {"json":{"field":{"json":{"json":{"field_name":1}}}, "json":{"array":[0]}}, "array":[0,1,2]}
    ops = helper.build_sequence_operations(json_document, max_number_operations = 10, seed=10, mutation_operation_type = "dict", force_operation_type = "dict_add")
    helper.show_all_operations(ops)
    json_document = {"json":{"field":{"json":{"json":{"field_name":1}}}, "json":{"array":[0]}}, "array":[0,1,2]}
    ops = helper.build_sequence_operations(json_document, max_number_operations = 10, seed=10, mutation_operation_type = "array", force_operation_type = "array_add_insert")
    helper.show_all_operations(ops)
    json_document = {"json":{"field":{"json":{"json":{"field_name":1}}}, "json":{"array":[0]}}, "array":[0,1,2]}
    ops = helper.build_sequence_operations(json_document, max_number_operations = 10, seed=10, mutation_operation_type = "all")
    helper.show_all_operations(ops)
    helper.gen_input_file()