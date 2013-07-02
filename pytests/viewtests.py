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
    def _test_view_on_multiple_docs_thread_wrapper(self, num_docs, failures, params={}, delay=10):
        try:
            ViewBaseTests._test_view_on_multiple_docs(self, num_docs, params, delay)
        except Exception as ex:
            failures.append(ex)

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
    def _get_view_results(self, rest, bucket, view, limit=20, extra_params={}, type_="view", invalid_results=False,
                          num_tries=None):
        # increase number of try to 40 to test on windows
        num_tries = num_tries or self.input.param('num-tries', 40)
        timeout = self.input.param('timeout', 10)
        results = json.loads("{}")
        timeout_for_connection = 60000
        #if view name starts with "dev" then we should append the full_set
        for i in range(0, num_tries):
            try:
                start = time.time()
                #full_set=true&connection_timeout=60000&limit=10&skip=0
                params = {"connection_timeout": timeout_for_connection}
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
                if invalid_results and str(ex).find('view_undefined') != -1:
                        raise ex
                if str(ex).find('view_undefined') != -1 or str(ex).find('not_found') != -1 or \
                   str(ex).find('unable to reach') != -1 or str(ex).find('timeout') != -1 or \
                   str(ex).find('socket error') != -1 or str(ex).find('econnrefused') != -1 or \
                   str(ex).find("doesn't exist") != -1 or str(ex).find('missing') != -1 or \
                   str(ex).find("Undefined set view") != -1:
                    self.log.error(
                            "view_results not ready yet , try again in {1} seconds... , error {0}"
                            .format(ex, timeout))
                    try:
                        ViewBaseTests._wait_for_indexer_ddoc(self, rest, view)
                    except:
                        pass
                    if i == num_tries:
                        self.fail("unable to get view_results for {0} after {1} tries due to error {2}"
                                  .format(view, num_tries, ex))
                    time.sleep(timeout)
                    if str(ex).find('timeout') != -1:
                        timeout_for_connection = 120000
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
    def _setup_cluster(self):
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        rebalanced = ClusterOperationHelper.add_and_rebalance(self.servers)
        self.assertTrue(rebalanced, msg="cluster is not rebalanced")

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
    def _load_docs(self, num_docs, prefix, verify=True, bucket='default', expire=0, flag=0):
        master = self.servers[0]
        rest = RestConnection(master)
        smart = VBucketAwareMemcached(rest, bucket)
        doc_names = []
        for i in range(0, num_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            doc_names.append(doc_name)
            value = {"name": doc_name, "age": i}
            smart.set(key, expire, flag, json.dumps(value))
            # loop till value is set
        RebalanceHelper.wait_for_persistence(master, bucket)
        self.log.info("inserted {0} json documents".format(num_docs))
        if verify:
            ViewBaseTests._verify_keys(self, doc_names, prefix)

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


class ViewBasicTests(unittest.TestCase):
    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)


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

        try:
            cluster = Cluster()
            cluster.delete_view(self.servers[0], ddocs[1].name, ddocs[1].views[0])
        finally:
            cluster.shutdown()

        results_new = rest.query_view(ddocs[0].name, ddocs[0].views[0].name, 'default',
                                  {"stale" : "ok", "full_set" : "true"})
        self.assertEquals(results.get(u'rows', []), results_new.get(u'rows', []),
                          "Results returned previosly %s don't match with current %s" % (
                          results.get(u'rows', []), results_new.get(u'rows', [])))

    def create_ddoc(self, rest, bucket, ddoc):
        ddoc_to_create = deepcopy(ddoc)
        ddoc_to_create.set_name(("", "dev_")[ddoc.views[0].dev_view] + ddoc_to_create.name)
        rest.create_design_document(bucket, ddoc_to_create)
        time.sleep(1)
        return rest.query_view(ddoc_to_create.name, ddoc_to_create.views[0].name, bucket,
                                      {"stale" : "false", "full_set" : "true"})



class ViewPerformanceTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_latency(self):
        ViewBaseTests._test_view_on_multiple_docs_multiple_design_docs(self, self.num_docs, self.num_design_docs, \
                                                                       params={"stale":"false", "debug" : "true" }, delay=1)
