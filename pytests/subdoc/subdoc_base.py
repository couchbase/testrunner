from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached
import copy


class SubdocBaseTest(BaseTestCase):
    def setUp(self):
        super(SubdocBaseTest, self).setUp()
        self.server = self.input.servers[0]
        self.xattr = self.input.param("xattr", None)
        self.create_parents = self.input.param("create_parents", None)
        for bucket in self.buckets:
            testuser = [{'id': bucket.name, 'name': bucket.name, 'password': 'password'}]
            rolelist = [{'id': bucket.name, 'name': bucket.name, 'roles': 'admin'}]
            self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)

    def tearDown(self):
        super(SubdocBaseTest, self).tearDown()

    def generate_json_for_nesting(self):
        json = {
            "i_0": 0,
            "i_b": 1038383839293939383938393,
            "d_z": 0.0,
            "i_p": 1,
            "i_n": -1,
            "d_p": 1.1,
            "d_n": -1.1,
            "f": 2.99792458e8,
            "f_n": -2.99792458e8,
            "a_i": [1, 2, 3, 4, 5],
            "a_d": [1.1, 2.2, 3.3, 4.4, 5.5],
            "a_f": [2.99792458e8, 2.99792458e8, 2.99792458e8],
            "a_m": [0, 2.99792458e8, 1.1],
            "a_a": [[2.99792458e8, 2.99792458e8, 2.99792458e8], [0, 2.99792458e8, 1.1], [], [0, 0, 0]],
            "l_c": "abcdefghijklmnoprestuvxyz",
            "u_c": "ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            "s_e": "",
            "d_t": "2012-10-03 15:35:46.461491",
            "s_c": "_-+!#@$%&*(){}\][;.,<>?/",
            "js": {"not_to_bes_tested_string_field1": "not_to_bes_tested_string"}
        }
        return json

    def generate_simple_data_null(self):
        json = {
            "null": None,
            "n_a": [None, None]
        }
        return json

    def generate_simple_data_boolean(self):
        json = {
            "1": True,
            "2": False,
            "3": [True, False, True, False]
        }
        return json

    def generate_nested_json(self):
        json_data = self.generate_json_for_nesting()
        json = {
            "json_1": {"json_2": {"json_3": json_data}}
        }
        return json

    def generate_simple_data_numbers(self):
        json = {
            "1": 0,
            "2": 1038383839293939383938393,
            "3": 0.0,
            "4": 1,
            "5": -1,
            "6": 1.1,
            "7": -1.1,
            "8": 2.99792458e8,
            "9": -2.99792458e8,
        }
        return json

    # def generate_simple_data_numbers_boundary(self):
    #     json = {
    #         "int_max": sys.maxint,
    #         "int_min": sys.minint
    #     }
    #     return json

    def generate_simple_data_array_of_numbers(self):
        json = {
            "ai": [1, 2, 3, 4, 5],
            "ad": [1.1, 2.2, 3.3, 4.4, 5.5],
            "af": [2.99792458e8, 2.99792458e8, 2.99792458e8],
            "am": [0, 2.99792458e8, 1.1],
            "aa": [[2.99792458e8, 2.99792458e8, 2.99792458e8], [0, 2.99792458e8, 1.1], [], [0, 0, 0]]
        }
        return json

    def generate_simple_data_strings(self):
        json = {
            "lc": "abcdefghijklmnoprestuvxyz",
            "uc": "ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            "se": "",
            "dt": "2012-10-03 15:35:46.461491",
            "sc": "_-+!#@$%&*(){}\][;.,<>?/"
        }
        return json

    def generate_simple_data_array_strings(self):
        json = {
            "ac": ['a', 'b', ''],
            "as": ['aa', '11', '&#^#', ''],
            "aas": [['aa', '11', '&#^#', ''], ['a', 'b', '']]
        }
        return json

    def generate_simple_data_mix_arrays(self):
        json = {
            "am": ["abcdefghijklmnoprestuvxyz", 1, 1.1, ""],
            "aai": [[1, 2, 3], [4, 5, 6]],
            "aas": [["abcdef", "ghijklmo", "prririr"], ["xcvf", "ffjfjf", "pointer"]]
        }
        return json

    def generate_simple_arrays(self):
        json = {
            "1_d_a": ["abcdefghijklmnoprestuvxyz", 1, 1.1, ""],
            "2_d_a": [[1, 2, 3], ["", -1, 1, 1.1, -1.1]]
        }
        return json

    def generate_path(self, level, key):
        path = key
        list1 = list(range(level))
        list1.reverse()
        for i in list1:
            path = "level_"+str(i)+"."+path
        return path

    def generate_nested(self, base_nested_level, nested_data, level_counter):
        json_data = copy.deepcopy(base_nested_level)
        original_json = json_data
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
                host = self.master.ip
                if self.master.ip == "127.0.0.1":
                    scheme = "http"
                    host="{0}:{1}".format(self.master.ip, self.master.port)
                return SDKClient(scheme=scheme, hosts = [host], bucket = bucket.name)
            except Exception as ex:
                self.log.error("cannot load sdk client due to error {0}".format(str(ex)))
        # USE MC BIN CLIENT WHEN NOT USING SDK CLIENT
        return self.direct_mc_bin_client(server, bucket, timeout= timeout)
