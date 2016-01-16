#!/usr/bin/env python
import copy

class SubdocHelper():
    def __init__(self):
    	print "Initialize SubdocHelper"

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

    def doNoMutationOperation(self,  operation, data_set, value):
      print "doNoMutationOperation"

    def doMutationOperation(self, operation_type, operation, data_set):
      print "doMutationOperation"

    def isMutationOperation(self, operation):
      return False

    def pick_operations(self, data):
      array_ops = ["array_op1", "array_op2", "array_op3"]
      field_ops = ["field_op1", "field_op2", "field_op3"]
      if isinstance(element, list):
        return "array", random.choice(array_ops)
      else:
        return "other", random.choice(field_ops)

    def isPathPresent(self, path, filter_paths = []):
      for path_parent in filter_paths:
        if path == path_parent[:len(path)]:
          return True
      return False

    def show_all_paths(self, pairs, data_set):
      for path in pairs.keys():
        parse_path_data = self.parse_and_get_data(data_set, path)
        print "PATH = {0} || VALUE = {1} || PARSE PATH = {2} ".format(path, pairs[path], parse_path_data)

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
    '''data_set = {"field":{"field":1},"array":[0,1,2,3]}
    print helper.parse_and_get_data(data_set, "field")
    print helper.parse_and_get_data(data_set, "field.field")
    print helper.parse_and_get_data(data_set, "array")
    print helper.parse_and_get_data(data_set, "array[2]")
    print helper.parse_and_get_data(data_set, "array[1]")
    print helper.parse_and_get_data(data_set, "array[0]")
    data_set = {"array":[{"field":1},1,{"field":[{"inside_field":{"another_field":1000}}]}]}
    print helper.parse_and_get_data(data_set, "array[0].field")
    print helper.parse_and_get_data(data_set, "array[1]")
    print helper.parse_and_get_data(data_set, "array[2].field[0].inside_field.another_field")
    data_set = [{"field":[0,1,{"field":"value"}]},1]
    print helper.parse_and_get_data(data_set, "[0].field[2]")
    print helper.parse_and_get_data(data_set, "[0].field[2].field")'''
