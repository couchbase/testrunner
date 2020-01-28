import time
import logger

from basetestcase import BaseTestCase

from ep_mc_bin_client import MemcachedError, MemcachedClient
from couchbase_helper.documentgenerator import BlobGenerator

from lib.couchbase_helper.tuq_generators import JsonGenerator
import memcacheConstants



class basic_collections(BaseTestCase):

    def setUp(self):
        super(basic_collections, self).setUp()


    def tearDown(self):
        super(basic_collections, self).tearDown()


    def test_valid_scope_name(self):
        #epengine.basic_collections.basic_collections.test_valid_scope_name
        # bucket is created, create scope with valid and invalid names
        valid_scope_name=["MYSCPOE", "MY_SCOPE", "MY-Scope_27%", "Scope_With-Largename_81%Scope%", "8", "A", "a", "aaa9999%"]
        for name in valid_scope_name:
            self.create_scope(scope=name)

        invalid_scope_name=["$scope", "%scope", "_myscope", "{scope", "s{[]/,.", "scope!@#^&*()", "!SCOPE"]
        for name in invalid_scope_name:
            self.create_scope(scope=name, result=False)


    def test_valid_collection_name(self):
        #epengine.basic_collections.basic_collections.test_valid_collection_name
        # bucket is created, create scope and then chec for valid collection name.
        scope_name="myscope"
        self.create_scope(scope=scope_name)

        valid_collection_name=["MYCOLLECTION", "MY_COLLECTION", "MY-Collection_27%", "CollectionsWithLargeNamechecki", "Colle_With-Largename_%Scope81%", "8", "A", "a", "aaa9999%"]
        for name in valid_collection_name:
            self.create_collection(scope=scope_name, collection=name)

        invalid_collection_name=["$collection", "%collection", "_mycollection", "{collection", "s{[]/,.", "collection!@#^&*()", "!COLLECTIONS"]
        for name in invalid_collection_name:
            self.create_collection(scope=scope_name, collection=name, result=False)


    def test_memecached_basic_api(self):
        # epengine.basic_collections.basic_collections.test_memecached_basic_api
        scope_name="ScopeWith30CharactersinName123"
        Collection_name="CollectionsWithLargeNamechecki"
        self.create_scope(scope=scope_name)
        self.create_collection(scope=scope_name, collection=Collection_name)

        collection= scope_name + "." + Collection_name
        self.log.info("colelction name is {}".format(collection))

        self.sleep(10)

        # create memcached client
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)


        # enable collection and get collections
        mc.enable_collections()
        mc.bucket_select('default')
        #mc.hello(memcacheConstants.FEATURE_COLLECTIONS)
        mc.hello("set_collection")

        ret=mc.get_collections(True)
        self.log.info("get collections completed")

        try:

            mc.set("key", 0, 0, "value", collection=collection)
            flag, keyx, value = mc.get(key="key", collection=collection)
            print("flag:{} keyx:{} value:{}".format(flag, keyx, value))

        except MemcachedError as exp:
            self.fail("Exception with setting and getting the key in collections {0}".format(exp) )

    def generate_docs_bigdata(self, docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=docs_per_day, start=start, value_size=document_size)

    def test_load_collection(self):
        # epengine.basic_collections.basic_collections.test_load_collection,value_size=200,num_items=100,collection=True
        self.sleep(10)

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
        self.delete_collection()



