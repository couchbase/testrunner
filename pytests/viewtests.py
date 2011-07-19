import json
import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import time
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached

class ViewTests(unittest.TestCase):
    #if we create a bucket and a view let's delete them in the end
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.servers = TestInputSingleton.input.servers
        ClusterOperationHelper.cleanup_cluster(self.servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self._create_default_bucket()
        self.created_views = {}


    #create a bucket if it doesn't exist

    def _create_default_bucket(self):
        name = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
            info = rest.get_nodes_self()
            available_ram = info.mcdMemoryReserved * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram))
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))


    def test_map_multiple_keys(self):
        pass

    def test_create_multiple_development_view(self):
        self.log.info("description : create multiple views without running any view query")
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())
        bucket = "default"

        view_names = ["dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, 5)]
        for view in view_names:
            map_fn = "function (doc) {emit(doc._id, doc);}"
            function = self._create_function(rest, bucket, view, map_fn)
            rest.create_view(bucket, view, function)
            self.created_views[view] = bucket
            response = rest.get_view(bucket, view)
            self.assertTrue(response)
            self.assertEquals(response["_id"], "_design/{0}".format(view))
            self.log.info(response)

    def test_view_on_10k_docs(self):
        num_of_docs = 10000
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "$dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        function = self._create_function(rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        vbaware = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        for i in range(0, num_of_docs):
            key, doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            vbaware.memcached(key).set(key, 0, 0, json.dumps(value))
            vbaware.memcached(key).get(key)
        self.log.info("inserted 10k json documents")
        results = self._get_view_results(rest, bucket, view_name, num_of_docs)
        keys = self._get_keys(results)
        not_found = []
        for name in doc_names:
            if not name in keys:
                not_found.append(key)
                #verify this key actually exists
                a, b, c = vbaware.memcached(name).get(name)
                print a, b, c
                self.log.error("did not find key {0} in the view results".format(key))
        if not_found:
            self.fail("map function did not return docs for {0} keys".format(len(not_found)))

    def test_update_multiple_development_view(self):
        self.log.info("description : create multiple views without running any view query")
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())
        bucket = "default"

        view_names = ["$dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, 5)]
        for view in view_names:
            map_fn = "function (doc) {emit(doc._id, doc);}"
            function = self._create_function(rest, bucket, view, map_fn)
            rest.create_view(bucket, view, function)
            self.created_views[view] = bucket
            response = rest.get_view(bucket, view)
            self.assertTrue(response)
            self.assertEquals(response["_id"], "_design/{0}".format(view))
            self.assertEquals(response["views"][view]["map"].encode("ascii", "ignore"), map_fn)
            #now let's update all those views ?

        view_names = ["$dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, 5)]
        for view in view_names:
            map_fn = "function (doc) {emit(doc._id, null);}"
            function = self._create_function(rest, bucket, view, map_fn)
            rest.create_view(bucket, view, function)
            self.created_views[view] = bucket
            response = rest.get_view(bucket, view)
            self.assertTrue(response)
            self.assertEquals(response["_id"], "_design/{0}".format(view))
            self.assertEquals(response["views"][view]["map"].encode("ascii", "ignore"), map_fn)
            self.log.info(response)


    def test_create_view(self):
        master = self.servers[0]
        rest = RestConnection(master)
        #find the first bucket
        bucket = "default"
        view_name = "$dev_" + str(uuid.uuid4())[:5]
        key = str(uuid.uuid4())
        doc_name = "test_create_view_{0}".format(key)
        mc = MemcachedClientHelper.direct_client(master, bucket)
        value = {"name": doc_name, "age": 1000}
        mc.set(key, 0, 0, json.dumps(value))
        mc.get(key)
        map_fn = "function (doc) { if (doc.name == \"" + doc_name + "\") { emit(doc.name, doc.age);}}"
        function = self._create_function(rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        results = self._get_view_results(rest, bucket, view_name)
        keys = self._get_keys(results)
        actual_key = keys[0]
        self.assertEquals(len(keys), 1)
        self.assertEquals(actual_key, doc_name)


    def _get_keys(self, results):
        self.assertTrue(results, "unable to get view results after multiple attempts")
        #        self.log.info("view_results json_response : {0}".format(results))
        rows = results["rows"]
        key_set = []
        for row in rows:
            key_set.append(row["key"].encode("ascii", "ignore"))
        self.log.info("key_set has {0} elements".format(len(key_set)))
        return key_set


    def _create_function(self, rest, bucket, view, function):
        #if this view already exist then get the rev and embed it here ?
        view_response = rest.get_view(bucket, view)
        function = {"language": "javascript",
                    "_id": "_design/{0}".format(view),
                    "views": {view: {"map": function}}}
        if view_response:
            function["_rev"] = view_response["_rev"]
        return json.dumps(function)

    def _get_view_results(self, rest, bucket, view, limit=20):
        for i in range(0, 20):
            try:
                results = rest.view_results(bucket, view, limit)
                if results:
                    self.log.info("was able to get view results after trying {0} times".format((i + 1)))
                    return results
            except Exception as ex:
                self.log.error("view_results not ready yet , try again in 5 seconds... , error {0}".format(ex))
                time.sleep(5)

    def tearDown(self):
        master = self.servers[0]
        rest = RestConnection(master)
        for view in self.created_views:
            bucket = self.created_views[view]
            rest.delete_view(bucket, view)
            self.log.info("deleted view {0} from bucket {1}".format(view, bucket))

