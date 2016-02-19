from basetestcase import BaseTestCase
from lib.mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached
import copy

class SubdocBaseTest(BaseTestCase):
    def setUp(self):
        super(SubdocBaseTest, self).setUp()
        self.server = self.input.servers[0]

    def tearDown(self):
        super(SubdocBaseTest, self).tearDown()

    def generate_json_for_nesting(self):
        json = {
            "not_tested_integer_zero":0,
            "not_tested_integer_big":1038383839293939383938393,
            "not_tested_double_zero":0.0,
            "not_tested_integer":1,
            "not_tested_integer_negative":-1,
            "not_tested_double":1.1,
            "not_tested_double_negative":-1.1,
            "not_tested_float":2.99792458e8,
            "not_tested_float_negative":-2.99792458e8,
            "not_tested_array_numbers_integer" : [1,2,3,4,5],
            "not_tested_array_numbers_double" : [1.1,2.2,3.3,4.4,5.5],
            "not_tested_array_numbers_float" : [2.99792458e8,2.99792458e8,2.99792458e8],
            "not_tested_array_numbers_mix" : [0,2.99792458e8,1.1],
            "not_tested_array_array_mix" : [[2.99792458e8,2.99792458e8,2.99792458e8],[0,2.99792458e8,1.1],[],[0, 0, 0]],
            "not_tested_simple_string_lower_case":"abcdefghijklmnoprestuvxyz",
            "not_tested_simple_string_upper_case":"ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            "not_tested_simple_string_empty":"",
            "not_tested_simple_string_datetime":"2012-10-03 15:35:46.461491",
            "not_tested_simple_string_special_chars":"_-+!#@$%&*(){}\][;.,<>?/",
            "not_test_json" : { "not_to_bes_tested_string_field1": "not_to_bes_tested_string"}
        }
        return json


    def generate_simple_data_null(self):
    	json = {
            "null":None,
            "null_array":[None, None]
        }
    	return json

    def generate_simple_data_boolean(self):
    	json = {
    		"true":True,
    		"false":False,
    		"array":[True, False, True, False]
    	}
    	return json

    def generate_nested_json(self):
        json_data = self.generate_json_for_nesting()
        json = {
            "json_1": { "json_2": {"json_3":json_data}}
        }
        return json

    def generate_simple_data_numbers(self):
    	json = {
    		"integer_zero":0,
    		"integer_big":1038383839293939383938393,
    		"double_zero":0.0,
    		"integer":1,
    		"integer_negative":-1,
    		"double":1.1,
    		"double_negative":-1.1,
    		"float":2.99792458e8,
    		"float_negative":-2.99792458e8,
    	}
    	return json

    def generate_simple_data_numbers_boundary(self):
    	json = {
    		"integer_max":sys.maxint,
    		"integer_min":sys.minint
    	}
    	return json

    def generate_simple_data_array_of_numbers(self):
    	json = {
    		"array_numbers_integer" : [1,2,3,4,5],
    		"array_numbers_double" : [1.1,2.2,3.3,4.4,5.5],
    		"array_numbers_float" : [2.99792458e8,2.99792458e8,2.99792458e8],
    		"array_numbers_mix" : [0,2.99792458e8,1.1],
    		"array_array_mix" : [[2.99792458e8,2.99792458e8,2.99792458e8],[0,2.99792458e8,1.1],[],[0, 0, 0]]
    	}
    	return json

    def generate_simple_data_strings(self):
    	json = {
    		"simple_string_lower_case":"abcdefghijklmnoprestuvxyz",
    		"simple_string_upper_case":"ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
    		"simple_string_empty":"",
            "simple_string_datetime":"2012-10-03 15:35:46.461491",
    		"simple_string_special_chars":"_-+!#@$%&*(){}\][;.,<>?/"
    	}
    	return json

    def generate_simple_data_array_strings(self):
    	json = {
    		"simple_string_chars_array":['a','b',''],
    		"simple_string_string_array":['aa','11','&#^#',''],
    		"simple_string_string_array_arrays":[['aa','11','&#^#',''],['a','b','']]
    	}
    	return json

    def generate_simple_data_mix_arrays(self):
    	json = {
    		"array_mix" : ["abcdefghijklmnoprestuvxyz",1,1.1,""],
    		"array_arrays_numbers" : [[1,2,3],[4,5,6]],
    		"array_arrays_string" : [["abcdef","ghijklmo","prririr"],["xcvf","ffjfjf","pointer"]]
    	}
    	return json

    def generate_simple_arrays(self):
    	json = {
    		"single_dimension_array" : ["abcdefghijklmnoprestuvxyz",1,1.1,""],
    		"two_dimension_array" : [[1,2,3],["",-1,1,1.1,-1.1]]
    	}
    	return json

    def generate_path(self, level, key):
        path = key
        list = range(level)
        list.reverse()
        for i in list:
            path = "level_"+str(i)+"."+path
        return path

    def generate_nested(self, base_nested_level, nested_data, level_counter):
        json_data = copy.deepcopy(base_nested_level)
        original_json  = json_data
        for i in range(level_counter):
            level = "level_"+str(i)
            json_data[level] = copy.deepcopy(base_nested_level)
            json_data = json_data[level]
        json_data.update(nested_data)
        return original_json

    def direct_mc_bin_client(self, server, bucket, timeout=30):
        # USE MC BIN CLIENT WHEN NOT USING SDK CLIENT
        return VBucketAwareMemcached( RestConnection(server), bucket)
        '''rest = RestConnection(server)
        node = None
        try:
            node = rest.get_nodes_self()
        except ValueError as e:
            self.log.info("could not connect to server {0}, will try scanning all nodes".format(server))
        if not node:
            nodes = rest.get_nodes()
            for n in nodes:
                if n.ip == server.ip and n.port == server.port:
                    node = n

        if isinstance(server, dict):
            self.log.info("dict:{0}".format(server))
            self.log.info("creating direct client {0}:{1} {2}".format(server["ip"], node.memcached, bucket))
        else:
            self.log.info("creating direct client {0}:{1} {2}".format(server.ip, node.memcached, bucket))
        RestHelper(rest).vbucket_map_ready(bucket, 60)
        vBuckets = RestConnection(server).get_vbuckets(bucket)
        if isinstance(server, dict):
            client = MemcachedClient(server["ip"], node.memcached, timeout=timeout)
        else:
            client = MemcachedClient(server.ip, node.memcached, timeout=timeout)
        if vBuckets != None:
            client.vbucket_count = len(vBuckets)
        else:
            client.vbucket_count = 0
        bucket_info = rest.get_bucket(bucket)
        return client'''

    def direct_client(self, server, bucket, timeout=30):
        # CREATE SDK CLIENT
        if self.use_sdk_client:
            try:
                from sdk_client import SDKClient
                scheme = "couchbase"
                host=self.master.ip
                if self.master.ip == "127.0.0.1":
                    scheme = "http"
                    host="{0}:{1}".format(self.master.ip,self.master.port)
                return SDKClient(scheme=scheme,hosts = [host], bucket = bucket.name)
            except Exception, ex:
                self.log.info("cannot load sdk client due to error {0}".format(str(ex)))
        # USE MC BIN CLIENT WHEN NOT USING SDK CLIENT
        return self.direct_mc_bin_client(server, bucket, timeout= timeout)