from basetestcase import BaseTestCase
from lib.mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection, RestHelper

class SubdocBaseTest(BaseTestCase):
    def setUp(self):
        super(SubdocBaseTest, self).setUp()
        self.server = self.input.servers[0]

    def tearDown(self):
        super(SubdocBaseTest, self).tearDown()


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
    		"float_negative":2.99792458e8,
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
    		"array_array_mix" : [[2.99792458e8,2.99792458e8,2.99792458e8],[0,2.99792458e8,1.1],[],[0, 0, 0]],
    	}
    	return json

    def generate_simple_data_strings(self):
    	json = {
    		"simple_string_lower_case":"abcdefghijklmnoprestuvxyz",
    		"simple_string_upper_case":"ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
    		"simple_string_empty":"",
    		"simple_string_special_chars":"_-+!#@$%&*(){}\][;.,<>?/"
    	}
    	return json

    def generate_simple_data_mix_arrays(self):
    	json = {
    		"array_mix" : ["abcdefghijklmnoprestuvxyz",1,1.1,""],
    		"array_arrays_numbers" : [[1,2,3],[4,5,6]],
    		"array_arrays_string" : [["abcdef","ghijklmo","prririr"],["xcvf","ffjfjf","pointer"]]
    	}
    	return json

	def generate_nested(self, nested_level):
   		json = self.generate_simple_data()
    	original_json = json
    	for i in range(10):
    		level = "level_"+str(i)
    		new_json = self.generate_simple_data()
    		json[level] = new_json
    		json = new_json
    	return json

    def direct_client(self, server, bucket, timeout=30):
        rest = RestConnection(server)
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
        # todo raise exception for not bucket_info
        client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                               bucket_info.saslPassword.encode('ascii'))
        return client