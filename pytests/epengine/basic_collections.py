from basetestcase import BaseTestCase

from couchbase_helper.documentgenerator import BlobGenerator
from ep_mc_bin_client import MemcachedError, MemcachedClient
from collection.collections_rest_client import Collections_Rest
from couchbase_cli import CouchbaseCLI
from lib.couchbase_helper.tuq_generators import JsonGenerator
from TestInput import TestInputSingleton, TestInputServer
from membase.api.rest_client import RestConnection
import logger

class basic_collections(BaseTestCase):

    def setUp(self):
        #super(basic_collections, self).setUp()
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.default_bucket_name = self.input.param("default_bucket_name", "default")
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.use_rest = self.input.param("use_rest", True)
        self.use_cli = self.input.param("use_cli", False)
        self.rest = Collections_Rest(self.master)
        self.cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password)
        self.cli.enable_dp()
        RestConnection(self.master).delete_all_buckets()
        RestConnection(self.master).create_bucket(bucket=self.default_bucket_name,
                               ramQuotaMB=256,
                               proxyPort=11220)

    def tearDown(self):
        RestConnection(self.master).delete_all_buckets()

    def test_valid_scope_name(self):
        # epengine.basic_collections.basic_collections.test_valid_scope_name
        # bucket is created, create scope with valid and invalid names
        valid_scope_name = ["MYSCPOE", "MY_SCOPE", "MY-Scope_27%", "Scope_With-Largename_81%Scope%", "8", "A", "a",
                            "aaa9999%"]
        for name in valid_scope_name:
            if self.use_rest:
                status = self.rest.create_scope(self.default_bucket_name, scope=name)
            elif self.use_cli:
                status = self.cli.create_scope(self.default_bucket_name, scope=name)

            if status is True:
                self.log.info("Scope creation passed, name={}".format(name))
            else:
                self.fail("Scope creation failed, name={}".format(name))

        invalid_scope_name = ["$scope", "%scope", "_myscope", "{scope", "s{[]/,.", "scope!@#^&*()", "!SCOPE"]
        for name in invalid_scope_name:
            if self.use_rest:
                status = self.rest.create_scope(scope=name, bucket=self.default_bucket_name)
            elif self.use_cli:
                status = self.cli.create_scope(scope=name, bucket=self.default_bucket_name)
            if status is True:
                self.fail("Scope creation passed for invalid name={}".format(name))
            else:
                self.log.info("Scope creation failed as expected for name={}".format(name))

    def test_valid_collection_name(self):
        # epengine.basic_collections.basic_collections.test_valid_collection_name
        # bucket is created, create scope and then chec for valid collection name.
        scope_name = "myscope"
        self.rest.create_scope(scope=scope_name, bucket=self.default_bucket_name)

        valid_collection_name = ["MYCOLLECTION", "MY_COLLECTION", "MY-Collection_27%", "CollectionsWithLargeNamechecki",
                                 "Colle_With-Largename_%Scope81%", "8", "A", "a", "aaa9999%"]
        for name in valid_collection_name:
            if self.use_rest:
                status = self.rest.create_collection(scope=scope_name, collection=name, bucket=self.default_bucket_name)
            elif self.use_cli:
                status = self.cli.create_collection(scope=scope_name, collection=name, bucket=self.default_bucket_name)
            if status is True:
                self.log.info("Collection creation passed, name={}".format(name))
            else:
                self.fail("Collection creation failed, name={}".format(name))

        invalid_collection_name = ["$collection", "%collection", "_mycollection", "{collection", "s{[]/,.",
                                   "collection!@#^&*()", "!COLLECTIONS"]
        for name in invalid_collection_name:
            try:
                if self.use_rest:
                    status = self.rest.create_collection(scope=scope_name, collection=name,
                                                         bucket=self.default_bucket_name)
                elif self.use_cli:
                    status = self.cli.create_collection(scope=scope_name, collection=name,
                                                        bucket=self.default_bucket_name)
                if status is True:
                    self.fail("Collection creation passed for invalid name={}".format(name))
                else:
                    self.log.info("Collection creation failed as expected for name={}".format(name))
            except:
                self.log.info("Collection creation failed as expected for name={}".format(name))

    def test_memecached_basic_api(self):
        # epengine.basic_collections.basic_collections.test_memecached_basic_api
        scope_name = "ScopeWith30CharactersinName123"
        Collection_name = "CollectionsWithLargeNamechecki"
        self.rest.create_scope(scope=scope_name)
        self.rest.create_collection(scope=scope_name, collection=Collection_name, bucket=self.default_bucket_name)

        collection = scope_name + "." + Collection_name
        self.log.info("collection name is {}".format(collection))

        self.sleep(10)

        # create memcached client
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)

        # enable collection and get collections
        mc.enable_collections()
        mc.bucket_select('default')
        # mc.hello(memcacheConstants.FEATURE_COLLECTIONS)
        mc.hello("set_collection")

        mc.get_collections(True)
        self.log.info("get collections completed")

        try:
            mc.set("key", 0, 0, "value", collection=collection)
            flag, keyx, value = mc.get(key="key", collection=collection)
            print("flag:{} keyx:{} value:{}".format(flag, keyx, value))

        except MemcachedError as exp:
            self.fail("Exception with setting and getting the key in collections {0}".format(exp))

    def generate_docs_bigdata(self, docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=docs_per_day, start=start, value_size=document_size)

    def test_load_collection(self):
        #epengine.basic_collections.basic_collections.test_load_collection,value_size=200,num_items=100,collection=True
        self.value_size = 200
        self.enable_bloom_filter = False
        self.buckets = RestConnection(self.master).get_buckets()
        self.active_resident_threshold = float(self.input.param("active_resident_threshold", 100))


        name = self.default_bucket_name + '0'
        self.collection_name[name] = []

        gen_create = BlobGenerator('eviction', 'eviction-', self.value_size, end=100)

        self._load_all_buckets(self.master, gen_create, "create", 0)

        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])

        self._verify_all_buckets(self.master)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

        # Add a node, rebalance and verify data
        self._load_all_buckets(self.master, gen_create, "delete", 0)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

        self._load_all_buckets(self.master, gen_create, "create", 0)
        # Add a node, rebalance and verify data
        self._load_all_buckets(self.master, gen_create, "read", 0)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

        self._load_all_buckets(self.master, gen_create, "update", 0)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        self._verify_all_buckets(self.master)

    def test_delete_default_collection(self):
        # epengine.basic_collections.basic_collections.test_delete_default_collection
        if self.use_rest:
            status = self.rest.delete_collection(self.default_bucket_name, "_default", "_default")
        elif self.use_cli:
            status = self.cli.delete_collection(self.default_bucket_name, "_default", "_default")
        if status is True:
            self.log.info("default collection deleted")
        else:
            self.log.info("default collection delete failed")

