import json
import random
from copy import deepcopy
from threading import Thread, Event
import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import time
import datetime
from couchbase.document import DesignDocument, View
from couchbase.cluster import Cluster
from membase.api.exception import ServerUnavailableException
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
        self.input = TestInputSingleton.input
        self.created_views = {}
        self.servers = self.input.servers
        self.replica = self.input.param("replica", 1)
        self.failover_factor = self.input.param("failover-factor", 1)
        self.num_docs = self.input.param("num-docs", 10000)
        self.num_design_docs = self.input.param("num-design-docs", 20)
        self.expiry_ratio = self.input.param("expiry-ratio", 0.1)
        self.num_buckets = self.input.param("num-buckets", 1)
        self.case_number = self.input.param("case_number", 0)
        self.dgm_run = self.input.param("dgm_run", False)

        #avoid clean up if the previous test has been tear down
        if not self.input.param("skip_cleanup", True) or self.case_number == 1:
            ViewBaseTests._common_clenup(self)
        master = self.servers[0]
        rest = RestConnection(master)
        rest.set_reb_cons_view(disable=False)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        mem_quota = int(rest.get_nodes_self().mcdMemoryReserved * node_ram_ratio)
        if self.dgm_run:
            mem_quota = 256
        rest.init_cluster(master.rest_username, master.rest_password)
        rest.init_cluster_memoryQuota(master.rest_username, master.rest_password, memoryQuota=mem_quota)
        if self.num_buckets == 1:
            ViewBaseTests._create_default_bucket(self, replica=self.replica)
        else:
            ViewBaseTests._create_multiple_buckets(self, replica=self.replica)
        ViewBaseTests._log_start(self)
        db_compaction = self.input.param("db_compaction", 30)
        view_compaction = self.input.param("view_compaction", 30)
        rest.set_auto_compaction(dbFragmentThresholdPercentage=db_compaction,
                                 viewFragmntThresholdPercentage=view_compaction)

    @staticmethod
    def common_tearDown(self):
        if not self.input.param("skip_cleanup", False):
            ViewBaseTests._common_clenup(self)
        ViewBaseTests._log_finish(self)

    @staticmethod
    def _common_clenup(self):
        rest = RestConnection(self.servers[0])
        if rest._rebalance_progress_status() == 'running':
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        ClusterOperationHelper.cleanup_cluster(self.servers)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)

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
            if(available_ram < 256):
                available_ram = 256
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram), replicaNumber=replica)
            msg = 'This not_my_vbucket error is part of wait_for_memcached method, not an issue'
            ready = BucketOperationHelper.wait_for_memcached(master, name, log_msg=msg)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))

    @staticmethod
    def _create_multiple_buckets(self, replica=1):
        master = self.servers[0]
        bucket_type = self.input.param('bucket-type', None)
        if bucket_type == 'sasl':
            created = BucketOperationHelper.create_multiple_buckets(master, replica,
                                                                    howmany=self.num_buckets,
                                                                    saslPassword="password")
        else:
            created = BucketOperationHelper.create_multiple_buckets(master, replica,
                                                                    howmany=self.num_buckets,
                                                                    saslPassword="")
        self.assertTrue(created, "unable to create multiple buckets")

        rest = RestConnection(master)
        buckets = rest.get_buckets()
        for bucket in buckets:
            ready = BucketOperationHelper.wait_for_memcached(master, bucket.name)
            self.assertTrue(ready, msg="wait_for_memcached failed")

    @staticmethod
    def _test_view_on_multiple_docs_multiple_design_docs(self, num_docs, num_of_design_docs, params={}, delay=10):
        self._view_test_threads = []
        for i in range(0, num_of_design_docs):
            thread_result = []
            t = Thread(target=ViewBaseTests._test_view_on_multiple_docs_thread_wrapper,
                       name="test_view_on_multiple_docs_multiple_design_docs", args=(self, num_docs, thread_result, \
                                                                                     params, delay))
            t.start()
            self._view_test_threads.append((t, thread_result))
        for (t, r) in self._view_test_threads:
            t.join()
        asserts = []
        for (t, r) in self._view_test_threads:
            #get the r
            if len(r) > 0:
                asserts.append(r[0])
                self.fail("view thread failed : {0}".format(r[0]))


    @staticmethod
    def _test_view_on_multiple_docs(self, num_docs, params={"stale":"update_after"}, delay=10):
        self.log.info("description : create a view on {0} documents".format(num_docs))
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_{1}_docs-{0}".format(str(uuid.uuid4())[:7], self.num_docs)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, dev_view=False)])
        self.created_views[view_name] = bucket
        rest = RestConnection(self.servers[0])
        smart = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        total_time = 0
        self.log.info("inserting {0} json objects".format(num_docs))
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            smart.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(len(doc_names)))
        time.sleep(10)
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, len(doc_names), extra_params=params)
        view_time = results['view_time']

        keys = ViewBaseTests._get_keys(self, results)

        RebalanceHelper.wait_for_persistence(master, bucket, 0)

        total_time = view_time
        # Keep trying this for maximum 5 minutes
        start_time = time.time()
        # increase timeout to 600 seconds for windows testing
        while (len(keys) != len(doc_names)) and (time.time() - start_time < 900):
            msg = "view returned {0} items , expected to return {1} items"
            self.log.info(msg.format(len(keys), len(doc_names)))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(delay)
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, len(doc_names), extra_params=params)
            view_time = results['view_time']
            total_time += view_time
            keys = ViewBaseTests._get_keys(self, results)

        self.log.info("View time: {0} secs".format(total_time))

        # Only if the lengths are not equal, look for missing keys
        if len(keys) != len(doc_names):
            not_found = list(set(doc_names) - set(keys))
            ViewBaseTests._print_keys_not_found(self, not_found, 10)
            self.fail("map function did not return docs for {0} keys".format(len(not_found)))

    @staticmethod
    def _test_count_reduce_multiple_docs(self, num_docs):
        self.log.info("description : create a view on 10 thousand documents")
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_{1}_docs-{0}".format(str(uuid.uuid4())[:7], self.num_docs)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc.age);}}"
        reduce_fn = "_count"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, reduce_fn, dev_view=False)])
        self.created_views[view_name] = bucket
        smart = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_docs))
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": i}
            smart.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_docs))
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs)
        value = ViewBaseTests._get_built_in_reduce_results(self, results)
        results_without_reduce = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs,
                                                        extra_params={"reduce": False})

        RebalanceHelper.wait_for_persistence(master, bucket, 0)

        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        num_of_results_without_reduce = len(ViewBaseTests._get_keys(self, results_without_reduce))
        msg = "reduce returned {0}, expected to return {1}"
        self.log.info(msg.format(value, len(doc_names)))
        while attempt < 6 and (value != num_docs or num_of_results_without_reduce != num_docs):
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(delay)
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
        view_name = "dev_test_view_on_{1}_docs-{0}".format(str(uuid.uuid4())[:7], self.num_docs)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc.age);}}"
        reduce_fn = "_sum"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, reduce_fn, dev_view=False)])
        self.created_views[view_name] = bucket
        smart = VBucketAwareMemcached(rest, bucket)
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
            smart.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_docs))
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs)
        value = ViewBaseTests._get_built_in_reduce_results(self, results)

        RebalanceHelper.wait_for_persistence(master, bucket, 0)

        # try this for maximum 3 minutes
        attempt = 0
        delay = 10
        while attempt < 6 and value != num_docs:
            msg = "view returned {0} value , expected to return sum {1}"
            self.log.info(msg.format(value, sum))
            self.log.info("trying again in {0} seconds".format(delay))
            time.sleep(10)
            attempt += 1
            results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, num_docs)
            value = ViewBaseTests._get_built_in_reduce_results(self, results)
        self.assertEquals(value, sum)

    @staticmethod
    def _test_view_on_multiple_docs_thread_wrapper(self, num_docs, failures, params={}, delay=10):
        try:
            ViewBaseTests._test_view_on_multiple_docs(self, num_docs, params, delay)
        except Exception as ex:
            failures.append(ex)

    @staticmethod
    def _test_update_view_on_multiple_docs(self, num_docs):
        self.log.info("description : create a view on {0} documents".format(self.num_docs))
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        view_name = "dev_test_view_on_{1}_docs-{0}".format(str(uuid.uuid4())[:7], self.num_docs)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, dev_view=False)])
        self.created_views[view_name] = bucket
        smart = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        updated_doc_names = []
        updated_num_docs = num_docs + 100
        prefix = str(uuid.uuid4())[:7]
        self.log.info("inserting {0} json objects".format(num_docs))
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": 1000}
            smart.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(num_docs))
        updated_view_prefix = "{0}-{1}-updated".format(view_name, prefix)
        for i in range(0, updated_num_docs):
            key = updated_doc_name = "{0}-{1}-updated-{2}".format(view_name, prefix, i)
            updated_doc_names.append(updated_doc_name)
            value = {"name": updated_doc_name, "age": 1000}
            smart.set(key, 0, 0, json.dumps(value))
        self.log.info("inserted {0} json documents".format(updated_num_docs))
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + updated_view_prefix + "\") != -1) { emit(doc.name, doc);}}"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, dev_view=False)])
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, updated_num_docs)
        keys = ViewBaseTests._get_keys(self, results)

        RebalanceHelper.wait_for_persistence(master, bucket, 0)

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
                if(isinstance(row["key"], str)):
                    key_set.append(row["key"].encode("ascii", "ignore"))
                else:
                    key_set.append(row["key"])
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
    def _get_view_results(self, rest, bucket, view, limit=20, extra_params={}, type_="view", invalid_results=False):
        # increase number of try to 40 to test on windows
        num_tries = self.input.param('num-tries', 40)
        timeout = self.input.param('timeout', 10)
        results = json.loads("{}")
        #if view name starts with "dev" then we should append the full_set
        for i in range(0, num_tries):
            try:
                start = time.time()
                #full_set=true&connection_timeout=60000&limit=10&skip=0
                params = {"connection_timeout": 60000}
                params.update(extra_params)
                if view.find("dev_") == 0:
                    params["full_set"] = "true"
                self.log.info("Params {0}".format(params))
                if type_ == "all_docs":
                    results = rest.all_docs(bucket, params, limit)
                else:
                    results = rest.query_view(view, view, bucket, params, invalid_query=invalid_results)

                    if u'error' in results and results[u'error'].find('not_found') < 0:
                        raise Exception("{0}: {1}".format(results[u'error'], results[u'reason']))

                if results.get(u'errors', []):
                    self.fail("unable to get view_results for {0} due to error {1}".format(view, results.get(u'errors')))

                delta = time.time() - start
                if results.get(u'rows', []) or results.get(u'total_rows', 0) > 0:
                    self.log.info("view returned in {0} seconds".format(delta))
                    results['view_time'] = delta
                    self.log.info("was able to get view results after trying {0} times".format((i + 1)))
                    return results
                else:
                    self.log.info("view returned empty results in {0} seconds, sleeping for {1}".format(delta, timeout))
                    ViewBaseTests._wait_for_indexer_ddoc(self, rest, view)
                    time.sleep(timeout)
            except ServerUnavailableException:
                self.log.error(
                            "view_results not ready yet , try again in {0} seconds... , unable to reach host"
                            .format(timeout))
                if i == num_tries:
                    self.fail("unable to get view_results for {0} after {1} tries due to error {2}"
                                  .format(view, num_tries, ex))
                time.sleep(timeout)
            except Exception as ex:
                if invalid_results and ex.message.find('view_undefined') != -1:
                        raise ex
                if ex.message.find('view_undefined') != -1 or ex.message.find('missing') != -1 or \
                 ex.message.find('unable to reach') != -1 or ex.message.find('timeout') != -1 or \
                 ex.message.find('socket error') != -1 or ex.message.find('econnrefused') != -1 or \
                 ex.message.find("doesn't exist") != -1:
                    self.log.error(
                            "view_results not ready yet , try again in {1} seconds... , error {0}"
                            .format(ex, timeout))
                    ViewBaseTests._wait_for_indexer_ddoc(self, rest, view)
                    if i == num_tries:
                        self.fail("unable to get view_results for {0} after {1} tries due to error {2}"
                                  .format(view, num_tries, ex))
                    time.sleep(timeout)
                else:
                    if u'rows' in results:
                        self.fail("Results are returned partially, but also error appears: {0}".format(ex))
                    else:
                        self.fail("Error appears during querying view {0} : {1}".format(view, ex))
        if results and results.get(u'errors', []):
            self.fail("unable to get view_results for {0} after {1} tries due to error {2}".format(view, num_tries, results.get(u'errors')))
        if results.get(u'error', ''):
            self.fail("unable to get view_results for {0} after {1} tries due to error {2}-{3}".format(view, num_tries, results.get(u'error'), results.get(u'reason')))
        self.fail("unable to get view_results for {0} after {1} tries".format(view, num_tries))

    @staticmethod
    def _get_indexer_task_pid(self, rest, ddoc):
        active_tasks = rest.active_tasks()
        if u'error' in active_tasks:
            return None
        if active_tasks:
            for task in active_tasks:
                if task['type'] == 'indexer' and task['indexer_type'] == 'main':
                    for ddoc in task['design_documents']:
                        if ddoc == ('_design/%s' % ddoc):
                            return task['pid'], False
                if task['type'] == 'blocked_indexer' and task['indexer_type'] == 'main':
                    for ddoc in task['design_documents']:
                        if ddoc == ('_design/%s' % ddoc):
                            return task['pid'], True
        return None, None

    @staticmethod
    def _wait_for_task_pid(self, pid, end_time):
        while (time.time() < end_time):
            new_pid, _ = ViewBaseTests._get_indexer_task_pid(self, rest, ddoc)
            if pid == new_pid:
                time.sleep(5)
                continue
            else:
                if new_pid:
                    self.log.info('New index for ddoc %s is started' % ddoc)
                else:
                    self.log.info('Index for ddoc %s is not going on' % ddoc)
                return

    @staticmethod
    def _wait_for_indexer_ddoc(self, rest, ddoc, timeout=300):
        end_time = time.time() + timeout
        old_pid, is_pid_blocked = ViewBaseTests._get_indexer_task_pid(self, rest, ddoc)
        if not old_pid:
            self.log.info('Index for ddoc %s is not going on' % ddoc)
            return
        while is_pid_blocked:
            ViewBaseTests._wait_for_task_pid(old_pid, end_time)
            old_pid, is_pid_blocked = ViewBaseTests._get_indexer_task_pid(self, rest, ddoc)
        if old_pid:
            ViewBaseTests._wait_for_task_pid(old_pid, end_time)

    @staticmethod
    def _setup_cluster(self):
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        rebalanced = ClusterOperationHelper.add_and_rebalance(self.servers)
        self.assertTrue(rebalanced, msg="cluster is not rebalanced")

    @staticmethod
    def _insert_n_items(self, bucket, view_name, prefix, number_of_docs):
        doc_names = []
        rest = RestConnection(self.servers[0])
        smart = VBucketAwareMemcached(rest, bucket)
        for i in range(0, number_of_docs):
            key = doc_name = "{0}-{1}-{2}-{3}".format(view_name, prefix, i, str(uuid.uuid4()))
            value = {"name": doc_name, "age": 1100}
            try:
                smart.set(key, 0, 0, json.dumps(value))
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
        view_name = "dev_test_view_on_{1}_docs-{0}".format(str(uuid.uuid4())[:7], self.num_docs)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, dev_view=False)])
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

        RebalanceHelper.wait_for_persistence(master, bucket, 0)

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
        not_found = list(set(doc_names) - set(keys))
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
         run_view_duration, num_of_design_docs):
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
       load_data_duration, number_of_items, run_view_duration, failures):
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
        num_tries = self.input.param('num-tries', 40)
        #the wrapper thread might have already built the cluster
        if len(rest.node_statuses()) < 2:
            ViewBaseTests._setup_cluster(self)
        self.log.info("sleeping for 5 seconds")
        time.sleep(5)
        view_name = "dev_test_view_on_{1}_docs-{0}".format(str(uuid.uuid4())[:7], self.num_docs)
        prefix = str(uuid.uuid4())[:7]
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + view_name + "\") != -1) { emit(doc.name, doc);}}"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, dev_view=False)])
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
            while attempt < num_tries and len(keys) != limit:
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
        rest = RestConnection(self.servers[0])
        smart = VBucketAwareMemcached(rest, bucket)
        inserted_index = []
        time_to_timeout = 0
        while not self.shutdown_load_data:
            for i in range(0, number_of_items):
                key = doc_name = "{0}-{1}-{2}".format(view_name, prefix, i)
                value = {"name": doc_name, "age": 1100}
                try:
                    smart.set(key, 0, 0, json.dumps(value))
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
    def _create_view_doc_name(self, prefix, bucket='default'):
        self.log.info("description : create a view")
        master = self.servers[0]
        rest = RestConnection(master)
        view_name = "dev_test_view-{0}".format(prefix)
        map_fn = "function (doc) {if(doc.name.indexOf(\"" + prefix + "-\") != -1) { emit(doc.name, doc);}}"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, dev_view=False)])
        self.created_views[view_name] = bucket
        return view_name

    @staticmethod
    def _load_docs(self, num_docs, prefix, verify=True, bucket='default', expire=0):
        master = self.servers[0]
        rest = RestConnection(master)
        smart = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": i}
            smart.set(key, expire, 0, json.dumps(value))
            # loop till value is set
        RebalanceHelper.wait_for_persistence(master, bucket)
        self.log.info("inserted {0} json documents".format(num_docs))
        if verify:
            ViewBaseTests._verify_keys(self, doc_names, prefix)

        return doc_names

    @staticmethod
    def _update_docs(self, num_docs, num_of_updated_docs, prefix):
        bucket = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        smart = VBucketAwareMemcached(rest, bucket)
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
                smart.set(key, 0, 0, json.dumps(value))
        self.log.info("updated {0} json documents".format(num_of_updated_docs))
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    def _delete_docs(self, num_docs, num_of_deleted_docs, prefix):
        bucket = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        smart = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            if i < num_of_deleted_docs:
                smart.delete(key)
            else:
                doc_names.append(doc_name)
        self.log.info("deleted {0} json documents".format(num_of_deleted_docs))
        if not self.__class__.__name__ == 'ViewQueryTests':
            ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    def _verify_keys(self, doc_names, prefix):
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"

        results = rest.all_docs(bucket)
        result_keys = ViewBaseTests._get_keys(self, results)

        # result_keys also contains the design document, but this doesn't
        # matter if we compare like this
        if set(doc_names) - set(result_keys):
            self.fail("returned doc names have different values than expected")

    @staticmethod
    def _verify_docs_doc_name(self, doc_names, prefix, bucket='default'):
        master = self.servers[0]
        rest = RestConnection(master)
        view_name = "dev_test_view-{0}".format(prefix)

        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name, limit=2 * len(doc_names))
        doc_names_view = ViewBaseTests._get_doc_names(self, results)

        RebalanceHelper.wait_for_persistence(master, bucket, 0)

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
            self.log.error("view returned %s items, expected to return %s items" % (
                            len(doc_names_view), len(doc_names)))
            if len(doc_names) > len(doc_names_view):
                self.log.error("There is a dataloss")
                self.log.error("%s are lost" % (set(doc_names) - set(doc_names_view)))
            else:
                self.log.error("There are extra docs")
                self.log.error("%s are extra" % (set(doc_names_view) - set(doc_names)))
            self.fail("returned doc names have different values than expected(see details above)")


    @staticmethod
    def _delete_doc_range(self, prefix, start_index, end_index):
        self.assertTrue(end_index >= start_index, "Requested to delete documents over an invalid range")
        bucket = "default"
        master = self.servers[0]
        rest = RestConnection(self.servers[0])
        smart = VBucketAwareMemcached(rest, bucket)
        delete_count = 0
        for i in range(start_index, end_index):
            key = "{0}-{1}".format(prefix, i)
            smart.delete(key)
            delete_count += 1
        self.log.info("deleted {0} json documents".format(delete_count))
        expected_delete_count = end_index - start_index
        self.assertTrue(delete_count == (end_index - start_index),
            "{0} docs deleted, expected {1}".format(delete_count, expected_delete_count))
        return delete_count

    @staticmethod
    def _begin_rebalance_in(self, timeout=5, howmany=0):
        master = self.servers[0]
        rest = RestConnection(master)
        if not howmany:
            howmany = len(self.servers)
        for server in self.servers[1:howmany]:
            self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
            msg = "unable to add node {0}:{1} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip, server.port))
        self.log.info("beginning rebalance in")
        try:
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        except:
            self.log.error("rebalance failed, trying again after 5 seconds")
            if hasattr(self, 'thread_crashed'):
                self.thread_crashed.set()
            time.sleep(timeout)
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

    @staticmethod
    def _begin_rebalance_out(self, howmany=0):
        master = self.servers[0]
        rest = RestConnection(master)

        ejectedNodes = []
        nodes = rest.node_statuses()
        #        for node in nodes:
        #            allNodes.append(node.id)
        if not howmany:
            howmany = len(self.servers)
        for server in self.servers[1:howmany]:
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
    def _test_compare_views_all_nodes(self, num_docs):
        master = self.servers[0]
        rest = RestConnection(master)
        bucket = "default"
        prefix = "{0}-{1}".format("_test_compare_views_all_nodes", str(uuid.uuid4()))
        doc_names = ViewBaseTests._load_complex_docs(self, num_docs, prefix, False)
        ViewBaseTests._begin_rebalance_in(self)
        view_name = "dev_test_compare_views_all_nodes"
        ViewBaseTests._create_view_for_gen_docs(self, rest, bucket, prefix, view_name)

        RebalanceHelper.wait_for_persistence(master, bucket, 0)

        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._verify_views_from_all(self, master, bucket, view_name, num_docs, doc_names)

    @staticmethod
    def _verify_views_from_all(self, master, bucket, view_name, num_docs, doc_names):
        rest = RestConnection(master)
        nodes = rest.get_nodes()
        for n in nodes:
            n_rest = RestConnection({"ip": n.ip, "port": n.port,
                                     "username": master.rest_username,
                                     "password": master.rest_password})
            results = ViewBaseTests._get_view_results(self, n_rest, bucket, view_name, num_docs,
                                             extra_params={"stale": "false"})
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
            _id = _json["meta"]["id"].encode("ascii", "ignore")
            _name = _json["json"]["name"].encode("ascii", "ignore")
            smart.memcached(_id).set(_id, 0, 0, json.dumps(_json["json"]))
            doc_names.append(_name)
        self.log.info("inserted {0} json documents".format(num_docs))
        if verify:
            ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
        return doc_names

    @staticmethod
    # used to delete documents created by the DocumentGenerator class
    def _delete_complex_docs(self, num_docs, num_of_deleted_docs, prefix):
        bucket = "default"
        rest = RestConnection(self.servers[0])
        smart = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}".format(i, prefix)
            if i < num_of_deleted_docs:
                smart.delete(key)
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
        rest.create_view(_view_name, bucket, [View(_view_name, map_fn, dev_view=False)])
        self.created_views[_view_name] = bucket


class ViewBasicTests(unittest.TestCase):
    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_view_on_x_docs(self):
        ViewBaseTests._test_view_on_multiple_docs(self, self.num_docs)

    def test_delete_x_docs(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_deleted_docs = self.input.param('num-deleted-docs', self.num_docs - 1)
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, self.num_docs, prefix)
        ViewBaseTests._delete_docs(self, self.num_docs, num_of_deleted_docs, prefix)

    def test_readd_x_docs(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_readd_docs = self.input.param('num-readd-docs', self.num_docs - 1)
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, self.num_docs, prefix)
        ViewBaseTests._delete_docs(self, self.num_docs, num_of_readd_docs, prefix)
        ViewBaseTests._update_docs(self, self.num_docs, num_of_readd_docs, prefix)

    def test_update_x_docs(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_update_docs = self.input.param('num-update-docs', self.num_docs - 1)
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, self.num_docs, prefix)
        self.log.info("loaded {0} docs".format(self.num_docs))
        ViewBaseTests._update_docs(self, self.num_docs, num_of_update_docs, prefix)
        self.log.info("updated {0} docs out of {1} docs".format(num_of_update_docs, self.num_docs))

    def test_invalid_view(self):
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())[:7]
        view = "invalid_view-{0}".format(prefix)
        bucket = "default"
        function = '{"views":[]}'

        api = rest.capiBaseUrl + '/{0}/_design/{1}'.format(bucket, view)
        status, content, header = rest._http_request(api, 'PUT', function, headers=rest._create_capi_headers())
        if not status:
            json_parsed = json.loads(content)
            self.assertEquals(u"The field `views' is not a json object." , json_parsed["reason"])
        else:
            self.fail("should be error during creation view with the function : {0}".format(function))
        # try to load some documents to see if memcached is running
        ViewBaseTests._load_docs(self, self.num_docs, prefix, False)

    def test_create_multiple_development_view(self):
        self.log.info("description : create multiple views without running any view query")
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())
        bucket = "default"
        num_views = self.input.param('num-views', 5)
        map_fn = "function (doc) {emit(doc._id, doc);}"
        view_names = ["dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, num_views)]

        for view in view_names:
            rest.create_view(view, bucket, [View(view, map_fn, dev_view=False)])
            self.created_views[view] = bucket
            response, meta = rest.get_view(bucket, view)
            self.assertTrue(response)
            self.assertEquals(meta["id"], "_design/{0}".format(view))
            self.log.info(response)

    def test_view_on_x_docs_y_design_docs(self):
        ViewBaseTests._test_view_on_multiple_docs_multiple_design_docs(self, self.num_docs, self.num_design_docs)

    def test_update_multiple_development_view(self):
        self.log.info("description : create multiple views without running any view query")
        master = self.servers[0]
        rest = RestConnection(master)
        prefix = str(uuid.uuid4())
        bucket = "default"
        num_views = self.input.param('num-views', 5)
        view_names = ["dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, num_views)]
        for view in view_names:
            map_fn = "function (doc) {emit(doc._id, doc);}"
            rest.create_view(view, bucket, [View(view, map_fn, dev_view=False)])
            self.created_views[view] = bucket
            response, meta = rest.get_view(bucket, view)
            self.assertTrue(response)
            self.assertEquals(meta["id"], "_design/{0}".format(view))
            self.assertEquals(response["views"][view]["map"].encode("ascii", "ignore"), map_fn)
            #now let's update all those views ?

        view_names = ["dev_test_map_multiple_keys-{0}-{1}".format(i, prefix) for i in range(0, num_views)]
        for view in view_names:
            map_fn = "function (doc) {emit(doc._id, null);}"
            rest.create_view(view, bucket, [View(view, map_fn, dev_view=False)])
            self.created_views[view] = bucket
            response, meta = rest.get_view(bucket, view)
            self.assertTrue(response)
            self.assertEquals(meta["id"], "_design/{0}".format(view))
            self.assertEquals(response["views"][view]["map"].encode("ascii", "ignore"), map_fn)
            self.log.info(response)

    def test_create_view(self):
        #find the first bucket
        bucket = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        view_name = "dev_" + str(uuid.uuid4())[:5]
        key = str(uuid.uuid4())
        doc_name = "test_create_view_{0}".format(key)
        mc = MemcachedClientHelper.direct_client(master, bucket)
        value = {"name": doc_name, "age": 1000}
        mc.set(key, 0, 0, json.dumps(value))
        mc.get(key)
        map_fn = "function (doc) { if (doc.name == \"" + doc_name + "\") { emit(doc.name, doc.age);}}"
        rest.create_view(view_name, bucket, [View(view_name, map_fn, dev_view=False)])
        self.created_views[view_name] = bucket
        results = ViewBaseTests._get_view_results(self, rest, bucket, view_name)
        self.assertTrue(results, "results are null")
        keys = ViewBaseTests._get_keys(self, results)
        actual_key = keys[0]
        self.assertEquals(len(keys), 1)
        self.assertEquals(actual_key, doc_name)

    def test_get_view_during_x_min_load_y_working_set(self):
        load_time = self.input.param('load-time', 1)
        run_view_time = self.input.param('run-view-time', 0)
        ViewBaseTests._test_load_data_get_view_x_mins(self, load_time, self.num_docs, run_view_time)

    def test_count_sum_x_docs(self):
        ViewBaseTests._test_sum_reduce_multiple_docs(self, self.num_docs)

    def test_count_reduce_x_docs(self):
        ViewBaseTests._test_count_reduce_multiple_docs(self, self.num_docs)

    def test_load_data_get_view_x_mins_y_design_docs(self):
        load_time = self.input.param('load-time', 1)
        run_view_time = self.input.param('run-view-time', 0)
        ViewBaseTests._test_load_data_get_view_x_mins_multiple_design_docs(self, load_time, self.num_docs, run_view_time, self.num_design_docs)

    def test_all_docs_startkey_endkey_validation(self):
        """Regression test for MB-6591

        This tests makes sure that the validation of startkey/endkey
        works with _all_docs which uses raw collation. Return results
        when startkey is smaller than endkey. When endkey is smaller than
        the startkey and exception should be raised. With raw collation
        "Foo" < "foo", with Unicode collation "foo" < "Foo".
        """
        bucket = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        ViewBaseTests._setup_cluster(self)

        startkey = '"Foo"'
        endkey = '"foo"'

        params = {"startkey": startkey, "endkey": endkey}
        results = rest.all_docs(bucket, params)
        self.assertTrue('rows' in results, "Results were returned")

        # Flip startkey and endkey
        params = {"startkey": endkey, "endkey": startkey}
        self.assertRaises(Exception, rest.all_docs, bucket, params)

    def test_view_startkey_endkey_validation(self):
        """Regression test for MB-6591

        This tests makes sure that the validation of startkey/endkey works
        with view, which uses Unicode collation. Return results
        when startkey is smaller than endkey. When endkey is smaller than
        the startkey and exception should be raised. With Unicode collation
        "foo" < "Foo", with raw collation "Foo" < "foo".
        """
        bucket = "default"
        master = self.servers[0]
        rest = RestConnection(master)
        ViewBaseTests._setup_cluster(self)
        ViewBaseTests._create_view_doc_name(self, "startkey-endkey-validation")

        view_name = "dev_test_view-startkey-endkey-validation"
        startkey = '"foo"'
        endkey = '"Foo"'

        params = {"startkey": startkey, "endkey": endkey}
        results = rest.query_view(view_name, view_name, bucket, params)
        self.assertTrue('rows' in results, "Results were returned")

        # Flip startkey and endkey
        params = {"startkey": endkey, "endkey": startkey}
        self.assertRaises(Exception, rest.query_view, view_name, view_name,
                          bucket, params)


class ViewRebalanceTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)
        self.thread_crashed = Event()

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_delete_x_docs_rebalance_in(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_deleted_docs = self.input.param('num-deleted-docs', self.num_docs - 1)
        # verify we are fully de-clustered
        ViewBaseTests._begin_rebalance_out(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, self.num_docs, prefix)
        ViewBaseTests._begin_rebalance_in(self)
        doc_names = ViewBaseTests._delete_docs(self, self.num_docs, num_of_deleted_docs, prefix)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_delete_x_docs_incremental_rebalance_in(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_deleted_docs = self.input.param('num-deleted-docs', self.num_docs - 1)
        ViewBaseTests._create_view_doc_name(self, prefix)

        master = self.servers[0]
        rest = RestConnection(master)
        nodes = rest.node_statuses()

        while len(nodes) < len(self.servers):
            ViewBaseTests._load_docs(self, self.num_docs, prefix)

            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            t = Thread(target=RebalanceHelper.rebalance_in,
                       name="Starting incremental rebalance in",
                       args=(self.servers, 1, False))
            t.start()
            doc_names = ViewBaseTests._delete_docs(self, self.num_docs, num_of_deleted_docs, prefix)
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node")

            t.join()
            ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
            nodes = rest.node_statuses()

    def test_delete_x_docs_rebalance_out(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_deleted_docs = self.input.param('num-deleted-docs', self.num_docs - 1)
        # verify we are fully clustered
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._create_view_doc_name(self, prefix)
        ViewBaseTests._load_docs(self, self.num_docs, prefix)
        ViewBaseTests._begin_rebalance_out(self)
        doc_names = ViewBaseTests._delete_docs(self, self.num_docs, num_of_deleted_docs, prefix)
        ViewBaseTests._end_rebalance(self)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_delete_x_docs_incremental_rebalance_out(self):
        prefix = str(uuid.uuid4())[:7]
        num_of_deleted_docs = self.input.param('num-deleted-docs', self.num_docs - 1)
        ViewBaseTests._create_view_doc_name(self, prefix)

        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        master = self.servers[0]
        rest = RestConnection(master)

        nodes = rest.node_statuses()

        while len(nodes) > 1:

            ViewBaseTests._load_docs(self, self.num_docs, prefix)
            #pick a node that is not the master node
            toBeEjectedNode = RebalanceHelper.pick_node(master)

            self.log.info("current nodes : {0}".format(RebalanceHelper.getOtpNodeIds(master)))
            self.log.info("removing node {0} and rebalance afterwards".format(toBeEjectedNode.id))
            t = Thread(target=rest.rebalance,
                       name="Starting rebalance out",
                       args=([node.id for node in rest.node_statuses()], [toBeEjectedNode.id]))
            t.start()
            doc_names = ViewBaseTests._delete_docs(self, self.num_docs, num_of_deleted_docs, prefix)
            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node {0}".format(toBeEjectedNode.id))

            t.join()

            ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)
            nodes = rest.node_statuses()


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
                expected_progress = 20 * i
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
                time.sleep(20)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])

            self.assertTrue(rest.monitorRebalance(),
                            msg="rebalance operation failed after adding node")

            self.assertTrue(len(rest.node_statuses()) - len(nodes) == 1, msg="unable to add and rebalance more nodes")
            nodes = rest.node_statuses()

        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

    def test_create_view_during_rebalance(self):
        master = self.servers[0]

        prefix = str(uuid.uuid4())[:7]
        doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix, verify=False)

        rebalance_thread = Thread(target=ViewBaseTests._begin_rebalance_in,
                       name="Start rebalance in",
                       args=(self, 5))
        rebalance_thread.start()
        view_name = ViewBaseTests._create_view_doc_name(self, prefix)
        rebalance_thread.join()
        if self.thread_crashed.is_set():
            self.fail("Rebalance thread was crashed")
        ViewBaseTests._end_rebalance(self)

        ViewBaseTests._verify_views_from_all(self, master, 'default', view_name, self.num_docs, doc_names)


class ViewFailoverTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_view_failover_multiple_design_docs_x_node_replica_y(self):
        failover_nodes = []
        try:
            nodes_init = self.input.param("nodes_init", 0)
            failover_helper = FailoverHelper(self.servers, self)
            master = self.servers[0]
            rest = RestConnection(master)

            # verify we are fully clustered
            ViewBaseTests._begin_rebalance_in(self, howmany=nodes_init)
            ViewBaseTests._end_rebalance(self)
            nodes = rest.node_statuses()
            self._view_test_threads = []
            view_names = {}
            #TODO: Parallelize loading and querying views
            for i in range(0, self.num_design_docs):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix)
                view_names[prefix] = doc_names

            failover_nodes = failover_helper.failover(self.failover_factor)
            self.log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=[node.id for node in failover_nodes])
            msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
            self.assertTrue(rest.monitorRebalance(), msg=msg)

            for key, value in view_names.items():
                ViewBaseTests._verify_docs_doc_name(self, value, key)

        finally:
            stopped_servers = []
            for node in failover_nodes:
                for server in self.servers:
                    if node.ip == server.ip and str(node.port) == server.port:
                        stopped_servers.append(server)
                        break
            for server in stopped_servers:
                shell = RemoteMachineShellConnection(server)
                shell.start_couchbase()
                #Wait 10 seconds for server to start back up
                time.sleep(10)
                shell.disconnect()

    def test_view_with_failed_over_node(self):
        failed = []
        try:
            master = self.servers[0]
            rest = RestConnection(master)
            master_node = rest.get_nodes_self()
            # verify we are fully clustered
            ViewBaseTests._begin_rebalance_in(self)
            ViewBaseTests._end_rebalance(self)
            nodes = rest.node_statuses()

            self._view_test_threads = []
            view_names = {}

            # Create half of the design docs before failover
            for i in range(0, self.num_design_docs / 2):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix)
                view_names[prefix] = doc_names

            # Failover total-replica-1 (master) nodes
            howmany = self.replica
            selection = []
            for n in nodes:
                if n.id != master_node.id:
                    selection.append(n)

            failed = selection[0:howmany]
            for f in failed:
                self.log.info("Failing over {0}".format(f.id))
                rest.fail_over(f.id)
            self.log.info("20 seconds sleep after failover, for nodes to go pending....")
            time.sleep(20)

            # Create half of the design docs after failover
            for i in range(self.num_design_docs / 2, self.num_design_docs):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix)
                view_names[prefix] = doc_names

            # Verify views after rebalaning out failed nodes
            for key, value in view_names.items():
                ViewBaseTests._verify_docs_doc_name(self, value, key)
        finally:
            stopped_servers = []
            for node in failed:
                for server in self.servers:
                    if node.ip == server.ip and str(node.port) == server.port:
                        stopped_servers.append(server)
                        break
            for server in stopped_servers:
                shell = RemoteMachineShellConnection(server)
                shell.start_couchbase()
                shell.disconnect()


class ViewCreationDeletionTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    '''
    Test scenario:
    1) upload the dev and production design docs
    2) query both with stale=false
    3) delete the development design doc
    4) query the production one with stale=ok
    5) Verify that the results you got in step 4 are exactly the same as the ones you got in step 2
    '''
    def test_create_delete_similar_views(self):
        ddoc_name_prefix = self.input.param("ddoc_name_prefix", "ddoc")
        view_name = self.input.param("view_name", "test_view")
        map_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}'
        rest = RestConnection(self.servers[0])
        ddocs = [DesignDocument(ddoc_name_prefix + "1", [View(view_name, map_fn,
                                                             dev_view=False)],
                                options={"updateMinChanges":0, "replicaUpdateMinChanges":0}),
                DesignDocument(ddoc_name_prefix + "2", [View(view_name, map_fn,
                                                            dev_view=True)],
                               options={"updateMinChanges":0, "replicaUpdateMinChanges":0})]

        ViewBaseTests._load_docs(self, self.num_docs, "test_")
        for ddoc in ddocs:
            results = self.create_ddoc(rest, 'default', ddoc)

        Cluster().delete_view(self.servers[0], ddocs[1].name, ddocs[1].views[0])
        results_new = rest.query_view(ddocs[0].name, ddocs[0].views[0].name, 'default',
                                  {"stale" : "ok", "full_set" : "true"})
        self.assertEquals(results.get(u'rows', []), results_new.get(u'rows', []),
                          "Results returned previosly %s don't match with current %s" % (
                          results.get(u'rows', []), results_new.get(u'rows', [])))

    def test_view_multiple_buckets(self):
        master = self.servers[0]
        rest = RestConnection(master)
        buckets = rest.get_buckets()
        # Cluster all nodes
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        view_names = {}
        view_bucket = {}
        for bucket in buckets:
            for i in range(0, self.num_design_docs):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix, bucket.name)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix, bucket=bucket.name, \
                    verify=False)
                view_names[prefix] = doc_names
                view_bucket[prefix] = bucket.name

        for key, value in view_names.items():
            view_name = "dev_test_view-{0}".format(key)
            ViewBaseTests._verify_views_from_all(self, master, view_bucket[key], view_name, self.num_docs, value)

    def test_view_node_not_rebalanced(self):
        master = self.servers[0]
        rest = RestConnection(master)
        buckets = rest.get_buckets()

        # Cluster total - 1 nodes
        ViewBaseTests._begin_rebalance_in(self, howmany=len(self.servers) - 1)
        ViewBaseTests._end_rebalance(self)

        view_names = {}
        view_bucket = {}
        for bucket in buckets:
            for i in range(0, self.num_design_docs):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix, bucket.name)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix, bucket=bucket.name, \
                    verify=False)
                view_names[prefix] = doc_names
                view_bucket[prefix] = bucket.name

        # Add the last server remaininggt
        server = self.servers[-1]

        # Add a node, but not rebalance in
        self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
        otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
        msg = "unable to add node {0}:{1} to the cluster"
        self.assertTrue(otpNode, msg.format(server.ip, server.port))

        # Verify before rebalance
        for key, value in view_names.items():
            view_name = "dev_test_view-{0}".format(key)
            ViewBaseTests._verify_views_from_all(self, master, view_bucket[key], view_name, self.num_docs, value)

        # Rebalance the node in
        rest.rebalance(otpNodes=[node.id for node in rest.get_nodes()], ejectedNodes=[])

        # Verify after rebalance
        for key, value in view_names.items():
            view_name = "dev_test_view-{0}".format(key)
            ViewBaseTests._verify_views_from_all(self, master, view_bucket[key], view_name, self.num_docs, value)

    def test_view_node_warmup(self):
        master = self.servers[0]
        rest = RestConnection(master)

        buckets = rest.get_buckets()

        # Cluster total - 1 nodes
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        view_names = {}
        view_bucket = {}
        for bucket in buckets:
            for i in range(0, self.num_design_docs):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix, bucket.name)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix, bucket=bucket.name, \
                    verify=False)
                view_names[prefix] = doc_names
                view_bucket[prefix] = bucket.name

        # Pick a node to warmup
        server = self.servers[-1]
        shell = RemoteMachineShellConnection(server)
        self.log.info("Node {0} is being stopped".format(server.ip))
        shell.stop_couchbase()
        time.sleep(20)
        shell.start_couchbase()

        self.log.info("Node {0} should be warming up".format(server.ip))

        for bucket in buckets:
            for i in range(0, self.num_design_docs):
                prefix = str(uuid.uuid4())[:7]
                ViewBaseTests._create_view_doc_name(self, prefix, bucket.name)
                doc_names = ViewBaseTests._load_docs(self, self.num_docs, prefix, bucket=bucket.name, \
                    verify=False)
                view_names[prefix] = doc_names
                view_bucket[prefix] = bucket.name

        # Verify views
        for key, value in view_names.items():
            view_name = "dev_test_view-{0}".format(key)
            ViewBaseTests._verify_views_from_all(self, master, view_bucket[key], view_name, \
                self.num_docs, value)

    def create_ddoc(self, rest, bucket, ddoc):
        ddoc_to_create = deepcopy(ddoc)
        ddoc_to_create.set_name(("", "dev_")[ddoc.views[0].dev_view] + ddoc_to_create.name)
        rest.create_design_document(bucket, ddoc_to_create)
        return rest.query_view(ddoc_to_create.name, ddoc_to_create.views[0].name, bucket,
                                      {"stale" : "false", "full_set" : "true"})

class ViewMultipleNodeTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_compare_views_all_nodes(self):
        ViewBaseTests._test_compare_views_all_nodes(self, self.num_docs)

    def test_view_on_x_docs_multiple_nodes(self):
        ViewBaseTests._setup_cluster(self)
        ViewBaseTests._test_view_on_multiple_docs(self, self.num_docs)

    def test_update_view_on_x_docs_multiple_nodes(self):
        ViewBaseTests._setup_cluster(self)
        ViewBaseTests._test_update_view_on_multiple_docs(self, self.num_docs)

class ViewPerformanceTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_latency(self):
        ViewBaseTests._test_view_on_multiple_docs_multiple_design_docs(self, self.num_docs, self.num_design_docs, \
                                                                       params={"stale":"false", "debug" : "true" }, delay=1)
