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

    def find_pairs(self, data_set = {}, path  = "", pairs = {}):
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


    def show_all_paths(self, pairs):
    	for path in pairs.keys():
    		print "PATH = {0} || VALUE = {1} ".format(path, pairs[path])

if __name__=="__main__":
    helper = SubdocHelper()
    pairs= {}
    nest_json = {"string":"data", "integer":3, "simple_json":{"string":"data", "integer":3, "array":[0,1,2, {"field":"value", "array":[0,1,2,3]}]}, "array":[0,1,2]}
    json_document = {"string":"data", "integer":3,"array":[1,2,3, [0, 1, 2, [3]]], "json":nest_json}
    helper.find_pairs(json_document,"", pairs)
    helper.show_all_paths(pairs)
    pairs= {}
    print  "++++++++++++++++++++++++++"
    nest_json = [{"field_1":1}, {"field_2":2}, 3]
    helper.find_pairs(nest_json,"", pairs)
    helper.show_all_paths(pairs)

