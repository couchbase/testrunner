import json
import random
from threading import Thread
import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import time
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.failover_helper import FailoverHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached, DocumentGenerator
from memcached.helper.data_helper import MemcachedError
from remote.remote_util import RemoteMachineShellConnection


class ViewBaseTests(unittest.TestCase):

    #if we create a bucket and a view let's delete them in the end
    @staticmethod
    def common_setUp(self):
        self.log = logger.Logger.get_logger()
        self.servers = TestInputSingleton.input.servers
        self.params = TestInputSingleton.input.test_params
        self.created_views = {}
        self.replica  = ViewBaseTests.parami("replica", 1)
        self.failover_factor = ViewBaseTests.parami("failover-factor", 1)
        self.num_docs = ViewBaseTests.parami("num-docs", 10000)
        self.num_design_docs = ViewBaseTests.parami("num-design-docs", 20)
        self.expiry_ratio = ViewBaseTests.paramf("expiry-ratio", 0.1)

        # Clear the state from Previous invalid run
        ViewBaseTests.common_tearDown(self)
        master = self.servers[0]
        rest = RestConnection(master)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        mem_quota = int(rest.get_nodes_self().mcdMemoryReserved * node_ram_ratio)
        rest.init_cluster(master.rest_username, master.rest_password)
        rest.init_cluster_memoryQuota(master.rest_username, master.rest_password, memoryQuota=mem_quota)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([master], self)
        ViewBaseTests._create_default_bucket(self, replica=self.replica)
        ViewBaseTests._log_start(self)
        db_compaction  = ViewBaseTests.parami("db_compaction", 30)
        view_compaction  = ViewBaseTests.parami("view_compaction", 30)
        rest.reset_auto_compaction(dbFragmentThreshold = db_compaction,
                              viewFragmntThreshold = view_compaction)

    @staticmethod
    def param(name, default_value):
        input = getattr(ViewBaseTests, "input", TestInputSingleton.input)
        return input.test_params.get(name, default_value)

    @staticmethod
    def parami(name, default_int):
        return int(ViewBaseTests.param(name, default_int))

    @staticmethod
    def paramf(name, default_float):
        return float(ViewBaseTests.param(name, default_float))

    @staticmethod
    def common_tearDown(self):
        master = self.servers[0]
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            try:
                RestConnection(master).stop_rebalance()
            except:
                pass
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                if shell.is_membase_installed():
                    shell.start_membase()
                else:
                    shell.start_couchbase()
                shell.disconnect()

            rest = RestConnection(master)
            for view in self.created_views:
                bucket = self.created_views[view]
                rest.delete_view(bucket, view)
                self.log.info("deleted view {0} from bucket {1}".format(view, bucket))
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
            ClusterOperationHelper.cleanup_cluster(self.servers)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        ViewBaseTests._log_finish(self)

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    #create a bucket if it doesn't exist
    @staticmethod
    def _create_default_bucket(self, replica=1):
        name = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram), replicaNumber=replica)
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))

    @staticmethod
    def _test_view_on_multiple_docs_multiple_design_docs(self, num_docs, num_of_design_docs):
        self._view_test_threads = []
        for i in range(0, num_of_design_docs):
            thread_result = []
            t = Thread(target=ViewBaseTests._test_view_on_multiple_docs_thread_wrapper,
                       name="test_view_on_multiple_docs_multiple_design_docs", args=(self, num_docs, thread_result))
            t.start()
            self._view_test_threads.append((t, thread_result))
        for (t, r) in self._view_test_threads:
            t.join()
        asserts = []
        for (t, r) in self._view_test_threads:
            #get the r
            if len(r) > 0:
                asserts.append(r[0])
                self.log.error("view thread failed : {0}".format(r[0]))
        self.fail("one of multiple threads failed. look at the logs for more specific errors")

    @staticmethod
    def _test_view_on_multiple_docs(self, num_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_docs))
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            moxi.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(len(doc_names)))
        time.sleep(5)
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, len(doc_names),extra_params={"stale":"update_after"})
        #        self._verify_views_replicated(bucket, view_name, map_fn)
        keys = ViewBaseTests._get_keys(self, results)
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
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, len(doc_names),extra_params={"stale":"update_after"})
            keys = ViewBaseTests._get_keys(self, results)
        keys = ViewBaseTests._get_keys(self, results)
        not_found = []
        for name in doc_names:
            if not name in keys:
                not_found.append(name)
        if not_found:
            ViewBaseTests._print_keys_not_found(self, not_found, 10)
            self.fail("map function did not return docs for {0} keys".format(len(not_found)))

    @staticmethod
    def _test_count_reduce_multiple_docs(self, num_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        reduce_fn = "_count"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn, reduce_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_docs))
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            moxi.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_docs))
        #        self._verify_views_replicated(bucket, view_name, map_fn)
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs)
        value = ViewBaseTests._get_built_in_reduce_results(self, results)
        results_without_reduce = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs,
                                                        extra_params={"reduce": False})
        #TODO: we should extend this function to wait for disk_write_queue for all nodes
        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)
        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        num_of_results_without_reduce = len(ViewBaseTests._get_keys(self, results_without_reduce))
        while attempt < 6 and value != num_docs:
            msg = "reduce returned {0}, expected to return {1}"
            self.log.info(msg.format(value, len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            #get the results without reduce ?
            results_without_reduce = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs,
                                                            extra_params={"reduce": False})
            keys = ViewBaseTests._get_keys(self, results_without_reduce)
            num_of_results_without_reduce = len(keys)
            msg = "view with reduce=false returned {0} items, expected to return {1} items"
            self.log.info(msg.format(len(keys), len(doc_names)))
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs)
            value = ViewBaseTests._get_built_in_reduce_results(self, results)
            msg = "reduce returned {0}, expected to return {1}"
            self.log.info(msg.format(value, len(doc_names)))
        self.assertEquals(value, num_docs)
        self.assertEquals(num_of_results_without_reduce, num_docs)
        #now try to get the view without reduce

    @staticmethod
    def _test_sum_reduce_multiple_docs(self, num_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, age);}}"
        reduce_fn = "_sum"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn, reduce_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        sum = 0
        self.log.info("inserting {0} json objects".format(num_docs))
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            rand = random.randint(0, 20000)
            value = {"name": doc_name, "age": rand}
            sum += rand
            moxi.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_docs))
        #        self._verify_views_replicated(bucket, view_name, map_fn)
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs)
        value = ViewBaseTests._get_built_in_reduce_results(self, results)
        #TODO: we should extend this function to wait for disk_write_queue for all nodes
        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)
        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        while attempt < 6 and value != num_docs:
            msg = "view returned {0} items , expected to return {1} items"
            self.log.info(msg.format(value, len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs)
            value = ViewBaseTests._get_built_in_reduce_results(self, results)
        self.assertEquals(value, num_docs)

    @staticmethod
    def _test_view_on_multiple_docs_thread_wrapper(self, num_docs, failures):
        try:
            ViewBaseTests._test_view_on_multiple_docs(self, num_docs)
        except Exception as ex:
            failures.append(ex)

    @staticmethod
    def _test_update_view_on_multiple_docs(self, num_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        updated_doc_names = []
        updated_num_docs = num_docs + 100
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_docs))
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            moxi.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_docs))
        #        self._verify_views_replicated(bucket, view_name, map_fn)
        updated_view_prefix = "{0}-{1}-updated".format(view_name, prefix)
        for i in range(0, updated_num_docs):
            key = updated_doc_name = "{0}-{1}-updated-{2}".format(view_name, prefix, i)
            updated_doc_names.append(updated_doc_name)
            value = {"name": updated_doc_name, "age": 1000}
            moxi.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(updated_num_docs))

        map_fn = "function (doc) {if(doc.name.indexOf(\"" + updated_view_prefix + "\") != -1) { emit(doc.name, doc);}}"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        #        self._verify_views_replicated(bucket, view_name, map_fn)
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, updated_num_docs)
        keys = ViewBaseTests._get_keys(self, results)
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
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, updated_num_docs)
            keys = ViewBaseTests._get_keys(self, results)
        keys = ViewBaseTests._get_keys(self, results)
        not_found = []
        for name in updated_doc_names:
            if not name in keys:
                not_found.append(name)
        if not_found:
            ViewBaseTests._print_keys_not_found(self, not_found, 10)
            self.fail("map function did not return docs for {0} keys".format(len(not_found)))

    @staticmethod
    def _print_keys_not_found(self, keys_not_found, how_many):
        #lets just print the first 10 keys
        i = 0
        if how_many > len(keys_not_found):
            how_many = len(keys_not_found)
        while i < how_many:
            i += 1
            self.log.error("did not find key {0} in the view results".format(keys_not_found[i]))

    @staticmethod
    def _get_keys(self, results):
        key_set = []
        if results:
            rows = results["rows"]
            for row in rows:
                key_set.append(row["key"].encode("ascii", "ignore"))
            self.log.info("key_set has {0} elements".format(len(key_set)))
        return key_set

    @staticmethod
    def _get_doc_names(self, results):
        doc_names = []
        if results:
            rows = results["rows"]
            for row in rows:
                doc_names.append(row["value"]["name"].encode("ascii", "ignore"))
            self.log.info("doc_names has {0} elements".format(len(doc_names)))
        return doc_names

    @staticmethod
    def _get_built_in_reduce_results(self, results):
        self.assertTrue(results, "results are null")
        rows = results["rows"]
        try:
            value = rows[0]["value"]
        except IndexError:
            value = None
        return value

    @staticmethod
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
        self.log.info("dict {0}".format(dict))
        return json.dumps(dict)

    @staticmethod
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
                    self.log.info(
                        "{0} {1}".format(view_definition["views"][view_name]["map"].encode("ascii", "ignore"), map_fn))
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

    @staticmethod
    def _get_view_results(self, rest, bucket, view, limit=20, full_set=True, extra_params={}):
        #if view name starts with "dev" then we should append the full_set
        for i in range(0, 4):
            try:
                start = time.time()
                #full_set=true&connection_timeout=60000&limit=10&skip=0
                params = {"connection_timeout": 60000}
                params.update(extra_params)
                if view.find("dev_") == 0:
                    params["full_set"] = True
                self.log.info("Params {0}".format(params))
                results = rest.view_results(bucket, view, params, limit)
                if results.get(u'errors', []):
                    self.fail("unable to get view_results for {0} due to error {1}".format(view, results.get(u'errors')))

                delta = time.time() - start
                if results.get(u'total_rows', 0) > 0:
                    self.log.info("view returned in {0} seconds".format(delta))
                    self.log.info("was able to get view results after trying {0} times".format((i + 1)))
                    return results
                else:
                    self.log.info("view returned empty results in {0} seconds".format(delta))
                    time.sleep(5)
            except Exception as ex:
                self.log.error("view_results not ready yet , try again in 5 seconds... , error {0}".format(ex))
                time.sleep(5)
        self.fail("unable to get view_results for {0} after 4 tries".format(view))

    @staticmethod
    def _setup_cluster(self):
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        rebalanced = ClusterOperationHelper.add_and_rebalance(self.servers)
        self.assertTrue(rebalanced, msg="cluster is not rebalanced")

    @staticmethod
    def _insert_n_items(self, bucket, view_name, prefix, number_of_docs):
        doc_names = []
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        for i in range(0, number_of_docs):
            key = doc_name = "{0}-{1}-{2}-{3}".format(view_name, prefix, i, str(uuid.uuid4()))
            value = {"name": doc_name, "age": 1100}
            try:
                moxi.set(key, 0, 0, json.dumps(value))
                doc_names.append(doc_name)
            except:
                self.log.error("unable to set item , reinitialize the vbucket map")
        self.log.info("inserted {0} json documents".format(len(doc_names)))
        return doc_names

    @staticmethod
    def _test_insert_json_during_rebalance(self, num_docs):
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        #load num_docs items into single node
        self.log.info("inserting {0} json objects".format(num_docs))
        doc_names.extend(ViewBaseTests._insert_n_items(self, bucket, view_name, prefix, num_docs))

        #grab view results while we still have only single node
        ViewBaseTests._get_view_results(self, rest, bucket, view_name, len(doc_names))

        #now let's add a node and rebalance
        added_nodes = [master]
        for server in self.servers[1:]:
            otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
            added_nodes.append(server)
            msg = "unable to add node {0}:{1} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip, server.port))
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
            #mutate num_docs that we added before
            #vbucket map is constantly changing so we need to catch the exception and
            self.log.info("inserting {0} json objects".format(num_docs))
            doc_names.extend(ViewBaseTests._insert_n_items(self, bucket, view_name, prefix, num_docs))
            #then monitor rebalance
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding nodes")

        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, len(doc_names))
        keys = ViewBaseTests._get_keys(self, results)

        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)

        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        while attempt < 30 and len(keys) != len(doc_names):
            msg = "view returned {0} items , expected to return {1} items"
            self.log.info(msg.format(len(keys), len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, len(doc_names))
            keys = ViewBaseTests._get_keys(self, results)
        keys = ViewBaseTests._get_keys(self, results)
        not_found = []
        for name in doc_names:
            if not name in keys:
                not_found.append(name)
        if not_found:
            ViewBaseTests._print_keys_not_found(self, not_found, 10)
            self.fail("map function did not return docs for {0} keys".format(len(not_found)))

            #add node
            #create view
            #load data
            #get view

            #more tests to run view after each incremental rebalance , add x items
            #rebalance and get the view

    @staticmethod
    def _test_load_data_get_view_x_mins_multiple_design_docs(self,
         load_data_duration, number_of_items,
         run_view_duration,num_of_design_docs):
        ViewBaseTests._setup_cluster(self)
        self.log.info("_setup_cluster returned")
        self._view_test_threads = []
        for i in range(0, num_of_design_docs):
            thread_result = []
            t = Thread(target=ViewBaseTests._test_load_data_get_view_x_mins_thread_wrapper,
                       name="test_view_on_multiple_docs_multiple_design_docs",
                       args=(self, load_data_duration, number_of_items, run_view_duration, thread_result))
            t.start()
            self._view_test_threads.append((t, thread_result))
        for (t, r) in self._view_test_threads:
            t.join()
        asserts = []
        for (t, r) in self._view_test_threads:
            #get the r
            if len(r) > 0:
                asserts.append(r[0])
                self.log.error("view thread failed : {0}".format(r[0]))
        self.fail("one of multiple threads failed. look at the logs for more specific errors")

    @staticmethod
    def _test_load_data_get_view_x_mins_thread_wrapper(self,
       load_data_duration, number_of_items, run_view_duration,failures):
        try:
            ViewBaseTests._test_load_data_get_view_x_mins(self, load_data_duration, number_of_items, run_view_duration)
        except Exception as ex:
            failures.append(ex)

    #need to split this into two tests
    @staticmethod
    def _test_load_data_get_view_x_mins(self, load_data_duration, number_of_items, run_view_duration, get_results=True):
        desc = "description : this test will continuously load data and gets the view results for {0} minutes"
        self.log.info(desc.format(load_data_duration))
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        #the wrapper thread might have already built the cluster
        if len(rest.node_statuses()) < 2:
            ViewBaseTests._setup_cluster(self)
        self.log.info("sleeping for 5 seconds")
        time.sleep(5)
        view_name = "dev_test_view_on_10k_docs-{0}".format(str(uuid.uuid4())[:7])
        prefix = str(uuid.uuid4())[:7]
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        self.shutdown_load_data = False
        self.docs_inserted = []
        load_thread = Thread(target=ViewBaseTests._insert_data_till_stopped, args=(self, bucket, view_name, prefix, number_of_items))
        load_thread.start()
        start = time.time()
        delay = 5
        while (time.time() - start) < load_data_duration * 60:
            #limit can be a random number between 0,1000
            limit = random.randint(0, 1000)
            self.log.info("{0} seconds has passed ....".format((time.time() - start)))
            if get_results:
                results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, limit)
                keys = ViewBaseTests._get_keys(self, results)
                self.log.info("view returned {0} rows".format(len(keys)))
            time.sleep(delay)
        self.shutdown_load_data = True
        load_thread.join()
        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        limit = 10000
        if len(self.docs_inserted) < limit:
            limit = len(self.docs_inserted)
        if get_results:
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, limit)
            keys = ViewBaseTests._get_keys(self, results)
            while attempt < 6 and len(keys) != limit:
                msg = "view returned {0} items , expected to return {1} items"
                self.log.info(msg.format(len(keys), limit))
                self.log.info("trying again in {0} seconds".format(delay))
                time.sleep(delay)
                attempt += 1
                results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, limit)
                keys = ViewBaseTests._get_keys(self, results)
                #if user wants to keep getting view results
            delay = 0.1
            if run_view_duration > 0:
                start = time.time()
                while (time.time() - start) < run_view_duration * 60:
                    results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, limit)
                    keys = ViewBaseTests._get_keys(self, results)
                    if len(keys) != limit:
                        msg = "view returned {0} items , limit was set to {1}"
                        self.log.info(msg.format(len(keys), limit))
                        time.sleep(delay)
                        self.log.info("retrieving view results again in {0} seconds".format(delay))
                    self.log.info(".")

    @staticmethod
    def _insert_data_till_stopped(self, bucket, view_name, prefix, number_of_items, timeout=300):
        # timeout is the length of time that this function will accept failures
        # if no new items are set for timeout seconds, we abort
        self.docs_inserted = []
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        MemcachedClientHelper.direct_client(self.servers[0], bucket).flush()
        inserted_index = []
        time_to_timeout = 0
        while not self.shutdown_load_data:
            for i in range(0, number_of_items):
                key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
                value = {"name": doc_name, "age": 1100}
                try:
                    moxi.set(key, 0, 0, json.dumps(value))
                    time_to_timeout = 0
                    if i not in inserted_index:
                        inserted_index.append(i)
                        self.docs_inserted.append(doc_name)
                except Exception as ex:
                    self.log.error("unable to set item to bucket {0}, error {1}".format(bucket, ex))
                    if time_to_timeout == 0:
                        time_to_timeout = time.time() + timeout
                    if time_to_timeout < time.time():
                        self.fail("inserts timeout after {0} seconds".format(ex))
        self.log.info("inserted {0} json documents".format(len(self.docs_inserted)))

    @staticmethod
    def _create_view_doc_name(self, prefix):
        self.log.info("description : create a view")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view-{0}".format(prefix)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + prefix + "-\") != -1) { emit(doc.name, doc);}}"
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket

    @staticmethod
    def _load_docs(self, num_docs, prefix, verify=True):
        rest = RestConnection(self.servers[0])
        command = "[rpc:multicall(ns_port_sup, restart_port_by_name, [moxi], 20000)]."
        moxis_restarted = rest.diag_eval(command)
        #wait until memcached starts
        self.assertTrue(moxis_restarted, "unable to restart moxi process through diag/eval")
        bucket = "default"
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            # loop till value is set
            fail_count = 0
            while True:
                try:
                    moxi.set(key, 0, 0, json.dumps(value))
                    break
                except MemcachedError as e:
                    fail_count += 1
                    if (e.status == 133 or e.status == 132) and fail_count < 60:
                        if i == 0:
                            self.log.error("moxi not fully restarted, waiting 5 seconds. error {0}".format(e))
                            time.sleep(5)
                        else:
                            self.log.error(e)
                            time.sleep(1)
                    else:
                        raise e
        self.log.info("inserted {0} json documents".format(num_docs))
        if verify:
            ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    def _update_docs(self, num_docs, num_of_updated_docs, prefix):
        bucket = "default"
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = "{0}-{1}".format(prefix, i)
            if i < num_of_updated_docs:
                doc_name = key + "updated"
            else:
                doc_name = key
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            if i < num_of_updated_docs:
                moxi.set(key, 0, 0, json.dumps(value))
        self.log.info("updated {0} json documents".format(num_of_updated_docs))
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    def _delete_docs(self, num_docs, num_of_deleted_docs, prefix):
        bucket = "default"
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            if i < num_of_deleted_docs:
                moxi.delete(key)
            else:
                doc_names.append(doc_name)
        self.log.info("deleted {0} json documents".format(num_of_deleted_docs))
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    def _verify_docs_doc_name(self, doc_names, prefix):
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view-{0}".format(prefix)

        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, limit=2 * len(doc_names))
        doc_names_view = ViewBaseTests._get_doc_names(self, results)
        RebalanceHelper.wait_for_stats(master, bucket, 'ep_queue_size', 0)
        # try this for maximum 1 minute
        attempt = 0
        delay = 5
        # first verify all doc_names get reported in the view
        while attempt < 12 and sorted(doc_names_view) != sorted(doc_names):
            msg = "view returned {0} items, expected to return {1} items"
            self.log.info(msg.format(len(doc_names_view), len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(delay)
            attempt += 1
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, limit=2 * len(doc_names))
            doc_names_view = ViewBaseTests._get_doc_names(self, results)
        if sorted(doc_names_view) != sorted(doc_names):
            self.fail("returned doc names have different values than expected")

    @staticmethod
    def _begin_rebalance_in(self):
        master = self.servers[0]
        rest = RestConnection(master)
        for server in self.servers[1:]:
            self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
            msg = "unable to add node {0}:{1} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip, server.port))
        self.log.info("beginning rebalance in")
        try:
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        except:
            self.log.error("rebalance failed, trying again after 5 seconds")
            time.sleep(5)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

    @staticmethod
    def _begin_rebalance_out(self):
        master = self.servers[0]
        rest = RestConnection(master)

        allNodes = []
        ejectedNodes = []
        nodes = rest.node_statuses()
        #        for node in nodes:
        #            allNodes.append(node.id)
        for server in self.servers[1:]:
            self.log.info("removing node {0}:{1} from cluster".format(server.ip, server.port))
            for node in nodes:
                if "{0}:{1}".format(node.ip, node.port) == "{0}:{1}".format(server.ip, server.port):
                    ejectedNodes.append(node.id)
        self.log.info("beginning rebalance out")
        try:
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)
        except:
            self.log.error("rebalance failed, trying again after 5 seconds")
            time.sleep(5)
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)

    @staticmethod
    def _end_rebalance(self):
        master = self.servers[0]
        rest = RestConnection(master)
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding nodes")
        self.log.info("rebalance finished")

    @staticmethod
    def _test_compare_views_all_nodes(self,num_docs):
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        prefix = "{0}-{1}".format("_test_compare_views_all_nodes", str(uuid.uuid4()))
        doc_names = ViewBaseTests._load_complex_docs(self, num_docs, prefix, False)
        ViewBaseTests._begin_rebalance_in(self)
        view_name = "dev_test_compare_views_all_nodes"
        ViewBaseTests._create_view_for_gen_docs(self, rest, bucket, prefix, view_name)

        RebalanceHelper.wait_for_mc_stats_all_nodes(master, bucket, 'ep_queue_size', 0)
        RebalanceHelper.wait_for_mc_stats_all_nodes(master, bucket, 'ep_flusher_todo', 0)
        RebalanceHelper.wait_for_mc_stats_all_nodes(master, bucket, 'ep_uncommitted_items', 0)

        ViewBaseTests._end_rebalance(self)
        nodes = rest.get_nodes()
        for n in nodes:
            n_rest = RestConnection({"ip": n.ip, "port": n.port,
                                     "username": self.servers[0].rest_username,
                                     "password": self.servers[0].rest_password})
            results = ViewBaseTests._get_view_results(self, n_rest, bucket, view_name, num_docs,
                                             extra_params={"stale": False})
            time.sleep(5)
            doc_names_view = ViewBaseTests._get_doc_names(self, results)
            if sorted(doc_names_view) != sorted(doc_names):
                self.fail("returned doc names have different values than expected")


    ### TODO:  _delete_docs() method cannot be used if this method is called to load docs
    # For now call _delete_complex_docs()
    # possible fix is to the store prefix-id format in documentGenerator for retrieval by delete fn
    ###
    @staticmethod
    def _load_complex_docs(self, num_docs, prefix, verify=True):
        rest = RestConnection(self.servers[0])
        doc_names = []
        bucket = "default"
        smart = VBucketAwareMemcached(rest, bucket)
        kv_template = {"name": "user-${prefix}-${seed}-", "mail": "memcached-json-${prefix}-${padding}"}
        options = {"size": 256, "seed": prefix}
        values = DocumentGenerator.make_docs(num_docs, kv_template, options)
        for value in values:
            value = value.encode("ascii", "ignore")
            _json = json.loads(value, encoding="utf-8")
            _id = _json["_id"].encode("ascii", "ignore")
            _name = _json["name"].encode("ascii", "ignore")
            del _json["_id"]
            smart.memcached(_id).set(_id, 0, 0, json.dumps(_json))
#            print "id:", _id, "j:", json.dumps(_json)
            doc_names.append(_name)
        self.log.info("inserted {0} json documents".format(num_docs))
        if verify:
            ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    # used to delete documents created by the DocumentGenerator class
    def _delete_complex_docs(self, num_docs, num_of_deleted_docs, prefix):
        bucket = "default"
        moxi = MemcachedClientHelper.proxy_client(self.servers[0], bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}".format(i, prefix)
            if i < num_of_deleted_docs:
                moxi.delete(key)
            else:
                doc_names.append(doc_name)
        self.log.info("deleted {0} json documents".format(num_of_deleted_docs))
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    def _create_view_for_gen_docs(self, rest, bucket, prefix, view_name):
        self.log.info("description : create a view")
        _view_name = view_name or "dev_test_view-{0}".format(prefix)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + prefix + "\") != -1) { emit(doc.name, doc);}}"
        function = ViewBaseTests._create_function(self, rest, bucket, _view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket


class ViewBasicTests(unittest.TestCase):
    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_view_on_100_docs(self):
        ViewBaseTests._test_view_on_multiple_docs(self, 100)

    def test_view_on_10k_docs(self):
        ViewBaseTests._test_view_on_multiple_docs(self, 10000)

    def test_view_on_100k_docs(self):
        ViewBaseTests._test_view_on_multiple_docs(self, 100000)

    def test_delete_10k_docs(self):
        prefix = str(uuid.uuid4())[:7]
        num_docs = 100000
        num_of_deleted_docs = 10000
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, num_docs, prefix)
        doc_names = ViewBaseTests._delete_docs(self, num_docs, num_of_deleted_docs, prefix)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_readd_10k_docs(self):
        prefix = str(uuid.uuid4())[:7]
        num_docs = 100000
        num_of_readd_docs = 10000
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, num_docs, prefix)
        doc_names = ViewBaseTests._delete_docs(self, num_docs, num_of_readd_docs, prefix)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        doc_names = ViewBaseTests._update_docs(self, num_docs, num_of_readd_docs, prefix)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_update_10k_docs(self):
        prefix = str(uuid.uuid4())[:7]
        num_docs = 100000
        num_of_update_docs = 10000
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, num_docs, prefix)
        self.log.info("loaded {0} docs".format(num_docs))
        doc_names = ViewBaseTests._update_docs(self, num_docs, num_of_update_docs, prefix)
        self.log.info("updated {0} docs out of {1} docs".format(num_of_update_docs, num_docs))
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_invalid_view(self):
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())[:7]
        view = "invalid_view-{0}".format(prefix)
        bucket = "default"
        function = '{"views":[]}'

        api = rest.baseUrl + 'couchBase/{0}/_design/{1}'.format(bucket, view)
        status, content = rest._http_request(api, 'PUT', function, headers=rest._create_capi_headers())
#        self.created_views[view] = bucket

        # try to load some documents to see if memcached is running
        ViewBaseTests._load_docs(self, self.num_docs, prefix, False)

    def test_create_multiple_development_view(self):
        self.log.info("description : create multiple views without running any view query")
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())
        bucket = "default"

        view_names = ["dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, 5)]
        for view in view_names:
            map_fn = "function (doc) {emit(doc._id, doc);}"
            function = ViewBaseTests._create_function(self, rest, bucket, view, map_fn)
            rest.create_view(bucket, view, function)
            self.created_views[view] = bucket
            response = rest.get_view(bucket, view)
            self.assertTrue(response)
            self.assertEquals(response["_id"], "_design/{0}".format(view))
            self.log.info(response)
            #            self._verify_views_replicated(bucket, view, map_fn)

    def test_view_on_20k_docs_20_design_docs(self):
        ViewBaseTests._test_view_on_multiple_docs_multiple_design_docs(self, 20000, 20)

    def test_update_multiple_development_view(self):
        self.log.info("description : create multiple views without running any view query")
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())
        bucket = "default"

        view_names = ["dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, 5)]
        for view in view_names:
            map_fn = "function (doc) {emit(doc._id, doc);}"
            function = ViewBaseTests._create_function(self, rest, bucket, view, map_fn)
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
            function = ViewBaseTests._create_function(self, rest, bucket, view, map_fn)
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
        function = ViewBaseTests._create_function(self, rest, bucket, view_name, map_fn)
        rest.create_view(bucket, view_name, function)
        self.created_views[view_name] = bucket
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name)
        self.assertTrue(results, "results are null")
        keys = ViewBaseTests._get_keys(self, results)
        actual_key = keys[0]
        self.assertEquals(len(keys), 1)
        self.assertEquals(actual_key, doc_name)


class ViewQueryTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_get_view_during_1_min_load_10k_working_set(self):
        ViewBaseTests._test_load_data_get_view_x_mins(self, 1, 10000, 0)

    def test_get_view_during_1_min_load_100k_working_set(self):
        ViewBaseTests._test_load_data_get_view_x_mins(self, 1, 100000, 0)

    def test_get_view_during_5_min_load_10k_working_set(self):
        ViewBaseTests._test_load_data_get_view_x_mins(self, 5, 1000, 0)

    def test_get_view_during_5_min_load_100k_working_set(self):
        ViewBaseTests._test_load_data_get_view_x_mins(self, 5, 100000, 0)

    def test_get_view_during_30_min_load_100k_working_set(self):
        ViewBaseTests._test_load_data_get_view_x_mins(self, 30, 100000, 0)

    def test_get_view_during_10_min_load_10k_working_set_1_min_after_load(self):
        ViewBaseTests._test_load_data_get_view_x_mins(self, 10, 1000, 1)

    def test_count_sum_10k_docs(self):
        ViewBaseTests._test_sum_reduce_multiple_docs(self, 10000)

    def test_count_reduce_x_docs(self):
        ViewBaseTests._test_count_reduce_multiple_docs(self, self.num_docs)

    def test_load_data_get_view_2_mins_20_design_docs(self):
        ViewBaseTests._test_load_data_get_view_x_mins_multiple_design_docs(self, 2, self.num_docs, 2, 20)


class ViewRebalanceTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_delete_10k_docs_rebalance_in(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_deleted_docs = 10000
        # verify we are fully de-clustered
        ViewBaseTests._begin_rebalance_out(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, self.num_docs, prefix)
        ViewBaseTests._begin_rebalance_in(self)
        doc_names = ViewBaseTests._delete_docs(self, self.num_docs, num_of_deleted_docs, prefix)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_delete_10k_docs_rebalance_out(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_deleted_docs = 10000
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, self.num_docs, prefix)
        ViewBaseTests._begin_rebalance_out(self)
        doc_names = ViewBaseTests._delete_docs(self, self.num_docs, num_of_deleted_docs, prefix)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_load_x_during_rebalance(self):
        ViewBaseTests._test_insert_json_during_rebalance(self, self.num_docs)

    def test_view_stop_start_incremental_rebalance(self):
        prefix = str(uuid.uuid4())[:7]
        ViewBaseTests._create_view_doc_name(self, prefix)
        doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix)

        master = self.servers[0]
        rest = RestConnection(master)

        nodes = rest.node_statuses()
        while len(nodes) < len(self.servers):

            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))

            rebalanced_in, which_servers = RebalanceHelper.rebalance_in(self.servers, 1, monitor=False)
            # Just doing 2 iterations
            for i in [1, 2]:
                expected_progress = 20*i
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
                time.sleep(20)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node")

            self.assertTrue(rebalanced_in, msg="unable to add and rebalance more nodes")
            nodes = rest.node_statuses()

        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)


class ViewFailoverTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_view_failover_multiple_design_docs_x_node_replica_y(self):
            failover_helper = FailoverHelper(self.servers, self)
            master = self.servers[0]
            rest = RestConnection(master)

            # verify we are fully clustered
            ViewBaseTests._begin_rebalance_in(self)
            ViewBaseTests._end_rebalance(self)
            nodes = rest.node_statuses()
            self._view_test_threads = []
            view_names = {}
            for i in range(0, self.num_design_docs):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix)
                view_names[prefix] = doc_names

            id = failover_helper.failover(self.failover_factor)
            self.log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in nodes],
                               ejectedNodes=[id])
            msg = "rebalance failed while removing failover nodes {0}".format(id)
            self.assertTrue(rest.monitorRebalance(), msg=msg)

            for key, value in view_names.items():
                ViewBaseTests._verify_docs_doc_name(self, value, key)


class ViewMultipleNodeTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_compare_views_all_nodes_100k(self):
        ViewBaseTests._test_compare_views_all_nodes(self, 10000)

    def test_view_on_5k_docs_multiple_nodes(self):
        ViewBaseTests._setup_cluster(self)
        ViewBaseTests._test_view_on_multiple_docs(self, 5000)

    def test_update_view_on_5k_docs_multiple_nodes(self):
        ViewBaseTests._setup_cluster(self)
        ViewBaseTests._test_update_view_on_multiple_docs(self, 5000)

    def test_view_on_10k_docs_multiple_nodes(self):
        ViewBaseTests._setup_cluster(self)
        ViewBaseTests._test_view_on_multiple_docs(self, 10000)
