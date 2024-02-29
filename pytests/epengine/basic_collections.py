import time

import logger
from gsi.newtuq import QueryTests
from basetestcase import BaseTestCase
from collection.collections_cli_client import CollectionsCLI
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from couchbase_helper.cluster import Cluster
from lib.couchbase_helper.documentgenerator import SDKDataLoader
from mc_bin_client import MemcachedError, MemcachedClient
from membase.api.rest_client import RestConnection

from TestInput import TestInputSingleton


class BasicCollections(QueryTests):

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.default_bucket_name = self.input.param("default_bucket_name", "default")
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.use_rest = self.input.param("use_rest", True)
        self.use_cli = self.input.param("use_cli", False)
        self.num_items = self.input.param("items", 100)
        self.value_size = self.input.param("value_size", 512)
        self.rest = CollectionsRest(self.master)
        self.cli = CollectionsCLI(self.master)
        # self.cli.enable_dp()
        self.conn = RestConnection(self.master)
        self.stat = CollectionsStats(self.master)
        self.conn.delete_all_buckets()
        time.sleep(5)
        try:
            self.conn.create_bucket(bucket=self.default_bucket_name,
                                    ramQuotaMB=256,
                                    proxyPort=11220)
        except Exception as e:
            pass
        # time.sleep(5)
        self.skip_load = True
        super(BasicCollections, self).setUp()

    def tearDown(self):
         self.conn.delete_all_buckets()

    def test_valid_scope_name(self):
        # epengine.basic_collections.BasicCollections.test_valid_scope_name
        # bucket is created, create scope with valid and invalid names
        valid_scope_names = ["MY_SCOPE", "MYSCOPE", "Scope__With--Exactly__30--Char", "Scope_With-Less_Than-30_Chars",
                             "8a", "A", "a", "aaa9999%"]
        for name in valid_scope_names:
            if self.use_rest:
                self.rest.create_scope(self.default_bucket_name, scope=name)
            elif self.use_cli:
                self.cli.create_scope(self.default_bucket_name, scope=name)

        bucket_scopes = self.rest.get_bucket_scopes(self.default_bucket_name)
        for name in valid_scope_names:
            if name in bucket_scopes:
                self.log.info("Scope creation passed, name={}".format(name))
            else:
                self.fail("Scope creation failed, name={}".format(name))

    def test_invalid_scope_name(self):
        invalid_scope_names = ["$scope", "%scope", "_myscope", "{scope", "s{[]/,.", "scope!@#^&*()", "!SCOPE", "Scope_With-Largename_81%Scope%abcdef1234"]
        for name in invalid_scope_names:
            try:
                if self.use_rest:
                    self.rest.create_scope(scope=name, bucket=self.default_bucket_name)
                elif self.use_cli:
                    self.cli.create_scope(scope=name, bucket=self.default_bucket_name)
            except:
                pass
        bucket_scopes = self.rest.get_bucket_scopes(self.default_bucket_name)
        for name in invalid_scope_names:
            if name in bucket_scopes:
                self.fail("Scope creation passed for invalid name={}".format(name))
            else:
                self.log.info("Scope creation failed as expected for name={}".format(name))

    def test_valid_collection_name(self):
        scope_name = "myscope"
        self.rest.create_scope(scope=scope_name, bucket=self.default_bucket_name)

        valid_collection_names = ["MYCOLLECTION", "MY_COLLECTION", "MY-Collection_27%",
                                  "CollectionsWithLargeNamechecki",
                                  "Colle_With-Largename_%Scope81%",
                                  "8", "A", "a", "aaa9999%"]
        for name in valid_collection_names:
            if self.use_rest:
                self.rest.create_collection(scope=scope_name, collection=name, bucket=self.default_bucket_name)
            elif self.use_cli:
                self.cli.create_collection(scope=scope_name, collection=name, bucket=self.default_bucket_name)

        scope_collections = self.rest.get_scope_collections(self.default_bucket_name, scope_name)
        for name in valid_collection_names:
            if name in scope_collections:
                self.log.info("Collection creation passed, name={}".format(name))
            else:
                self.fail("Collection creation failed, name={}".format(name))

    def test_invalid_collection_name(self):
        scope_name = "myscope"
        self.rest.create_scope(scope=scope_name, bucket=self.default_bucket_name)

        invalid_collection_names = ["$collection", "%collection", "_mycollection",
                                    "{collection", "s{[]/,.",
                                    "collection!@#^&*()", "!COLLECTIONS"]
        for name in invalid_collection_names:
            try:
                if self.use_rest:
                    self.rest.create_collection(scope=scope_name, collection=name,
                                                bucket=self.default_bucket_name)
                elif self.use_cli:
                    self.cli.create_collection(scope=scope_name, collection=name,
                                               bucket=self.default_bucket_name)
            except:
                pass

            scope_collections = self.rest.get_scope_collections(self.default_bucket_name, scope_name)
            for name in invalid_collection_names:
                if name in scope_collections:
                    self.fail("Collection creation passed for invalid name={}".format(name))
                else:
                    self.log.info("Collection creation failed as expected for name={}".format(name))

    def test_bulk_create_from_counts(self):
        import time
        start = time.time()
        self.scope_num = self.input.param("num_scopes", 10)
        self.collection_num = self.input.param("num_collections", 10)
        self.bucket_name = self.input.param("bucket", self.default_bucket_name)
        try:
            self.rest.async_create_scope_collection(self.scope_num, self.collection_num, self.bucket_name)
        except:
            pass
        create = time.time()
        self.log.info("{} scopes with {} collections each created in {} s"
                      .format(self.scope_num, self.collection_num, round(create - start)))
        time.sleep(5)

    def test_bulk_create_from_map(self):
        import time
        start = time.time()
        #TODO
        self.log.info("{} scopes with {} collections each created in {} s"
                      .format(self.scope_num, self.collection_num, round(time.time() - start)))

    def test_delete_default_collection(self):
        # epengine.basic_collections.BasicCollections.test_delete_default_collection
        if self.use_rest:
            status = self.rest.delete_collection(self.default_bucket_name, "_default", "default")
        elif self.use_cli:
            status = self.cli.delete_collection(self.default_bucket_name, "_default", "default")
        else:
            raise Exception("Choose either REST or CLI")
        if status is False:
            self.log.fail("Failed to delete default collection!")
        else:
            self.log.info("Default collection delete passed")

    def test_load_collections_in_bucket(self):
        import time
        start = time.time()
        self.scope_num = self.input.param("num_scopes", 2)
        self.collection_num = self.input.param("num_collections", 2)
        self.bucket_name = self.input.param("bucket", self.default_bucket_name)
        try:
            self.rest.async_create_scope_collection(self.scope_num, self.collection_num, self.bucket_name)
        except:
            pass
        create = time.time()
        self.log.info("{} scopes with {} collections each created in {} s"
                      .format(self.scope_num, self.collection_num, round(create - start)))
        time.sleep(5)

        self.enable_bloom_filter = self.input.param("enable_bloom_filter", False)
        self.buckets = self.conn.get_buckets()
        self.cluster = Cluster()
        self.active_resident_threshold = 100

        self.gen_create = SDKDataLoader(num_ops=self.num_items, percent_create=80, percent_update=20,
                                        percent_delete=20)
        self._load_all_buckets(self.master, self.gen_create)
        load = time.time()
        self.log.info("Done loading {} collections in bucket {} in {}s"
                      .format(self.collection_num, self.bucket_name, round(load - create)))
        for bkt in self.buckets:
            print(self.stat.get_collection_stats(bkt))

    def test_load_collections_in_scope(self):
        pass

    def test_load_collection(self):
        pass

    def test_memecached_basic_api(self):
        # epengine.basic_collections.BasicCollections.test_memecached_basic_api
        scope_name = "ScopeWith30CharactersinName123"
        collection_name = "CollectionsWithLargeNamechecki"
        self.rest.create_scope(scope=scope_name)
        self.rest.create_collection(scope=scope_name, collection=collection_name, bucket=self.default_bucket_name)

        self.log.info(f"scope name is {scope_name} and collection name is {collection_name}")

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
            mc.set("key", 0, 0, "value", scope=scope_name, collection=collection_name)
            flag, keyx, value = mc.get(key="key", scope=scope_name, collection=collection_name)
            print("flag:{} keyx:{} value:{}".format(flag, keyx, value))

        except MemcachedError as exp:
            self.fail("Exception with setting and getting the key in collections {0}".format(exp))

    def test_collection_doc_count(self):
        self.test_load_collections_in_bucket()
        self.buckets = self.conn.get_buckets()
        for bkt in self.buckets:
            bkt_scopes = self.rest.get_bucket_scopes(bkt)
            print(bkt_scopes)
            for scope in bkt_scopes:
                print("Items in {}->{} = {}".format(bkt, scope,
                                                    self.stat.get_scope_item_count(bkt, scope)))
                bkt_collections = self.rest.get_scope_collections(bkt, scope)
                for collection in bkt_collections:
                    print("Items in {}->{}->{} = {}"
                          .format(bkt, scope, collection,
                                  self.stat.get_collection_item_count(bkt, scope, collection)))

    def test_load_data_to_collection_using_python_sdk(self):
        scope = 'test_scope'
        collection = 'test_collection'
        for bucket in self.buckets:
            self.rest.create_scope_collection(bucket=bucket.name, scope=scope, collection=collection)
            gens_load = self.generate_docs(self.docs_per_day)
            self.load(gens_load, flag=self.item_flag, verify_data=False, batch_size=1000, scope=scope,
                      collection=collection)

            count = 0
            stat = 0
            while count < 5:
                stat = self.stat.get_collection_item_count(bucket, scope, collection)
                if stat == 98784:
                    break
                self.sleep(2, "Stat not matching with expected value. Retrying")
                count += 1
            else:
                self.fail(f"Actual docs {stat}, Expected docs 98784")
