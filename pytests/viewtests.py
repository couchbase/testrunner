import json
import random
import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import time
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
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
            self._verify_views_replicated(bucket, view, map_fn)

    def test_view_on_100_docs(self):
        self._test_view_on_multiple_docs(100)

    def test_view_on_10k_docs(self):
        self._test_view_on_multiple_docs(10000)

    def test_view_on_100k_docs(self):
        self._test_view_on_multiple_docs(100000)

    def test_count_reduce_100_docs(self):
        self._test_count_reduce_multiple_docs(100)

    def test_count_reduce_10k_docs(self):
        self._test_count_reduce_multiple_docs(10000)

    def test_count_reduce_100k_docs(self):
        self._test_count_reduce_multiple_docs(100000)

    def _test_count_reduce_multiple_docs(self, num_of_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        reduce_fn = "_count"
        function = self._create_function(rest, bucket, view_name, map_fn, reduce_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        vbaware = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_of_docs))
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            vbaware.memcached(key).set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_of_docs))
        self._verify_views_replicated(bucket, view_name, map_fn)
        results = self._get_view_results(rest, bucket, view_name, num_of_docs)
        value = self._get_built_in_reduce_results(results)
        #TODO: we should extend this function to wait for disk_write_queue for all nodes
        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)
        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        while attempt < 6 and value != num_of_docs:
            msg = "view returned {0} items , expected to return {1} items"
            self.log.info(msg.format(value, len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            results = self._get_view_results(rest, bucket, view_name, num_of_docs)
            value = self._get_built_in_reduce_results(results)
        self.assertEquals(value, num_of_docs)

    def _test_sum_reduce_multiple_docs(self, num_of_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, age);}}"
        reduce_fn = "_sum"
        function = self._create_function(rest, bucket, view_name, map_fn, reduce_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        vbaware = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        sum = 0
        self.log.info("inserting {0} json objects".format(num_of_docs))
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            rand = random.randint(0, 20000)
            value = {"name": doc_name, "age": rand}
            sum += rand
            vbaware.memcached(key).set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_of_docs))
        self._verify_views_replicated(bucket, view_name, map_fn)
        results = self._get_view_results(rest, bucket, view_name, num_of_docs)
        value = self._get_built_in_reduce_results(results)
        #TODO: we should extend this function to wait for disk_write_queue for all nodes
        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)
        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        while attempt < 6 and value != num_of_docs:
            msg = "view returned {0} items , expected to return {1} items"
            self.log.info(msg.format(value, len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            results = self._get_view_results(rest, bucket, view_name, num_of_docs)
            value = self._get_built_in_reduce_results(results)
        self.assertEquals(value, num_of_docs)


    def _test_view_on_multiple_docs(self, num_of_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        function = self._create_function(rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        vbaware = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_of_docs))
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            vbaware.memcached(key).set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_of_docs))
        results = self._get_view_results(rest, bucket, view_name, num_of_docs)
        self._verify_views_replicated(bucket, view_name, map_fn)
        keys = self._get_keys(results)
        #TODO: we should extend this function to wait for disk_write_queue for all nodes
        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)
        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        while attempt < 6 and len(keys) != len(doc_names):
            msg = "view returned {0} items , expected to return {1} items"
            self.log.info(msg.format(len(keys), len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            results = self._get_view_results(rest, bucket, view_name, num_of_docs)
            if results and num_of_docs < 10000:
                for i in range(0, 60):
                    results = self._get_view_results(rest, bucket, view_name, num_of_docs)
                    self.log.info("repeat #{0}".format(i, len(results)))
            keys = self._get_keys(results)
        keys = self._get_keys(results)
        not_found = []
        for name in doc_names:
            if not name in keys:
                not_found.append(name)
        if not_found:
            self._print_keys_not_found(not_found, 10)
            self.fail("map function did not return docs for {0} keys".format(len(not_found)))

    def _test_update_view_on_multiple_docs(self, num_of_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        function = self._create_function(rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        vbaware = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        updated_doc_names = []
        updated_num_of_docs = num_of_docs + 100
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_of_docs))
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            vbaware.memcached(key).set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_of_docs))
        self._verify_views_replicated(bucket, view_name, map_fn)
        updated_view_prefix = "{0}-{1}-updated".format(view_name, prefix)
        for i in range(0, updated_num_of_docs):
            key = updated_doc_name = "{0}-{1}-updated-{2}".format(view_name, prefix, i)
            updated_doc_names.append(updated_doc_name)
            value = {"name": updated_doc_name, "age": 1000}
            vbaware.memcached(key).set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(updated_num_of_docs))

        map_fn = "function (doc) {if(doc.name.indexOf(\"" + updated_view_prefix + "\") != -1) { emit(doc.name, doc);}}"
        function = self._create_function(rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self._verify_views_replicated(bucket, view_name, map_fn)
        results = self._get_view_results(rest, bucket, view_name, updated_num_of_docs)
        keys = self._get_keys(results)
        #TODO: we should extend this function to wait for disk_write_queue for all nodes
        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)
        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        while attempt < 6 and len(keys) != len(updated_doc_names):
            msg = "view returned {0} items , expected to return {1} items"
            self.log.info(msg.format(len(keys), len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            results = self._get_view_results(rest, bucket, view_name, updated_num_of_docs)
            keys = self._get_keys(results)
        keys = self._get_keys(results)
        not_found = []
        for name in updated_doc_names:
            if not name in keys:
                not_found.append(name)
        if not_found:
            self._print_keys_not_found(not_found, 10)
            self.fail("map function did not return docs for {0} keys".format(len(not_found)))


    def _print_keys_not_found(self, keys_not_found, how_many):
        #lets just print the first 10 keys
        i = 0
        if how_many > len(keys_not_found):
            how_many = len(keys_not_found)
        while i < how_many:
            i += 1
            self.log.error("did not find key {0} in the view results".format(keys_not_found[i]))

    def test_update_multiple_development_view(self):
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
            self.assertEquals(response["views"][view]["map"].encode("ascii", "ignore"), map_fn)
            #now let's update all those views ?

        view_names = ["dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, 5)]
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
        view_name = "dev_" + str(uuid.uuid4())[:5]
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
        self.assertTrue(results, "results are null")
        keys = self._get_keys(results)
        actual_key = keys[0]
        self.assertEquals(len(keys), 1)
        self.assertEquals(actual_key, doc_name)


    def _get_keys(self, results):
        key_set = []
        if results:
            rows = results["rows"]
            for row in rows:
                key_set.append(row["key"].encode("ascii", "ignore"))
            self.log.info("key_set has {0} elements".format(len(key_set)))
        return key_set

    def _get_built_in_reduce_results(self, results):
        self.assertTrue(results, "results are null")
        rows = results["rows"]
        return rows[0]["value"]


    def _create_function(self, rest, bucket, view, function, reduce=''):
        #if this view already exist then get the rev and embed it here ?
        dict = {"language": "javascript",
                "_id": "_design/{0}".format(view)}
        try:
            view_response = rest.get_view(bucket, view)
            if view_response:
                dict["_rev"] = view_response["_rev"]
        except:
            pass
        if reduce:
            dict["views"] = {view: {"map": function, "reduce": reduce}}
        else:
            dict["views"] = {view: {"map": function}}
        print dict
        return json.dumps(dict)

    def _verify_views_replicated(self, bucket, view_name, map_fn):
        # max 1 minutes
        rest = RestConnection(self.servers[0])
        views_per_vbucket = rest.get_views_per_vbucket(bucket, view_name)
        start = time.time()
        while time.time() - start < 120:
            views_per_vbucket = rest.get_views_per_vbucket(bucket, view_name)
            if len(views_per_vbucket) != len(rest.get_vbuckets(bucket)):
                self.log.info("view is not replicated to all vbuckets yet. try again in 5 seconds")
                time.sleep(5)
            else:
                all_valid = True
                for vb in views_per_vbucket:
                    view_definition = views_per_vbucket[vb]
                    print view_definition["views"][view_name]["map"].encode("ascii", "ignore"), map_fn
                    if view_definition["views"][view_name]["map"].encode("ascii", "ignore") != map_fn:
                        self.log.info("view map_function is not replicated to all vbuckets yet. try again in 5 seconds")
                        all_valid = False
                        time.sleep(5)
                        break

                if all_valid:
                    delta = time.time() - start
                    self.log.info("view was replicated to all vbuckets after {0} seconds".format(delta))
                    break
        msg = "len(views_per_vbucket) : {0} , len(rest.get_vbuckets(bucket)) : {1}"
        self.log.info(msg.format(len(views_per_vbucket), len(rest.get_vbuckets(bucket))))
        self.assertEquals(len(views_per_vbucket), len(rest.get_vbuckets(bucket))
                          , msg="view is not replicated to all vbuckets")
        for vb in views_per_vbucket:
            view_definition = views_per_vbucket[vb]
            self.assertEquals(view_definition["_id"], "_design/{0}".format(view_name))
            self.assertEquals(view_definition["views"][view_name]["map"].encode("ascii", "ignore"), map_fn)


    def _get_view_results(self, rest, bucket, view, limit=20, full_set=True):
        #if view name starts with "dev" then we should append the full_set
        for i in range(0, 20):
            try:
                start = time.time()
                #full_set=true&connection_timeout=60000&limit=10&skip=0
                params = {"connection_timeout": "60000"}
                if view.find("dev_") == 0:
                    params["full_set"] = "true"

                results = rest.view_results(bucket, view, params, limit)
                delta = time.time() - start
                if results:
                    self.log.info("view returned in {0} seconds".format(delta))
                    self.log.info("was able to get view results after trying {0} times".format((i + 1)))
                    return results
            except Exception as ex:
                self.log.error("view_results not ready yet , try again in 5 seconds... , error {0}".format(ex))
                time.sleep(5)

    def tearDown(self):
        master = self.servers[0]
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            rest = RestConnection(master)
            for view in self.created_views:
                bucket = self.created_views[view]
                rest.delete_view(bucket, view)
                self.log.info("deleted view {0} from bucket {1}".format(view, bucket))
            if len(rest.node_statuses()) > 1:
                ClusterOperationHelper.cleanup_cluster(self.servers)

    def _setup_cluster(self):
        master = self.servers[0]
        rebalanced = ClusterOperationHelper.add_and_rebalance(self.servers, master.rest_password)
        self.assertTrue(rebalanced, msg="cluster is not rebalanced")

    def test_view_on_5k_docs_multiple_nodes(self):
        self._setup_cluster()
        self._test_view_on_multiple_docs(5000)

    def test_update_view_on_5k_docs_multiple_nodes(self):
        self._setup_cluster()
        self._test_update_view_on_multiple_docs(5000)


    def test_view_on_10k_docs_multiple_nodes(self):
        self._setup_cluster()
        self._test_view_on_multiple_docs(10000)



        #more tests to run view after each incremental rebalance , add x items
        #rebalance and get the view
