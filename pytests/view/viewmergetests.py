import unittest
import sys
import time
import types
import logger
import json
from collections import defaultdict

from TestInput import TestInputSingleton
from couchbase_helper.document import View
from membase.api.rest_client import RestConnection, Bucket
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from basetestcase import BaseTestCase
from membase.api.exception import QueryViewException
from membase.helper.rebalance_helper import RebalanceHelper


class ViewMergingTests(BaseTestCase):

    def setUp(self):
        try:
            if 'first_case' not in TestInputSingleton.input.test_params:
                TestInputSingleton.input.test_params['default_bucket'] = False
                TestInputSingleton.input.test_params['skip_cleanup'] = True
                TestInputSingleton.input.test_params['skip_buckets_handle'] = True
            self.default_bucket_name = 'default'
            super(ViewMergingTests, self).setUp()
            if 'first_case' in TestInputSingleton.input.test_params \
                    and self.num_servers > 1:
                self.cluster.rebalance(self.servers[:], self.servers[1:], [])
            # We use only one bucket in this test suite
            self.rest = RestConnection(self.master)
            self.bucket = self.rest.get_bucket(Bucket(name=self.default_bucket_name))
            # num_docs must be a multiple of the number of vbuckets
            self.num_docs = self.input.param("num_docs_per_vbucket", 1) * \
                            len(self.bucket.vbuckets)
            self.is_dev_view = self.input.param("is_dev_view", False)
            self.map_view_name = 'mapview1'
            self.red_view_name = 'redview1'
            self.red_view_stats_name = 'redview_stats'
            self.keys_view_name = 'mapviewkeys'
            self.clients = self.init_clients()
            if 'first_case' in TestInputSingleton.input.test_params:
                 self.create_ddocs(False)
                 self.create_ddocs(True)
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        # clean up will only performed on the last run
        if 'last_case' in TestInputSingleton.input.test_params:
            TestInputSingleton.input.test_params['skip_cleanup'] = False
            TestInputSingleton.input.test_params['skip_buckets_handle'] = False
            super(ViewMergingTests, self).tearDown()
        else:
            self.cluster.shutdown(force=True)
            self._log_finish(self)

    def test_empty_vbuckets(self):
        results = self.merged_query(self.map_view_name)
        self.assertEqual(results.get('total_rows', None), 0)
        self.assertEqual(len(results.get('rows', None)), 0)

    def test_nonexisting_views(self):
        view_names = ['mapview2', 'mapview3', 'mapview4', 'mapview5']
        for view_name in view_names:
            try:
                self.merged_query(view_name)
            except QueryViewException:
                self.log.info("QueryViewException is raised as expected")
            except Exception:
                self.assertFail("QueryViewException is expected, but not raised")
            else:
                self.assertFail("QueryViewException is expected, but not raised")

    def test_non_empty_view(self):
        num_vbuckets = len(self.rest.get_vbuckets(self.bucket))
        docs = ViewMergingTests.make_docs(1, self.num_docs + 1)
        self.populate_sequenced(num_vbuckets, docs)
        results = self.merged_query(self.map_view_name)
        self.assertEqual(results.get('total_rows', 0), self.num_docs)
        self.assertEqual(len(results.get('rows', [])), self.num_docs)

    def test_queries(self):
        all_query_params = ['skip', 'limit', 'startkey', 'endkey', 'startkey_docid',
                        'endkey_docid', 'inclusive_end', 'descending', 'key']
        current_params = {}
        for key in self.input.test_params:
            if key in all_query_params:
                current_params[key] = str(self.input.test_params[key])
        results = self.merged_query(self.map_view_name, current_params)
        self.verify_results(results, current_params)

    def test_keys(self):
        keys = [5, 3, 10, 39, 666666, 21]
        results = self.merged_query(self.map_view_name, {"keys": keys})
        self.verify_results(results, {"keys": keys})

        keys = [2, 1, 4]
        results = self.merged_query(self.keys_view_name,
                                    {"keys": keys, "reduce": "false"},
                                    ddoc='test3')
        self.verify_results(results, {"keys": keys})

        keys = [0, 2, 1]
        results = self.merged_query(self.keys_view_name,
                                    {"keys": keys, "group": "true"},
                                    ddoc='test3')
        self.verify_results(results, {"keys": keys})

    def test_include_docs(self):
        results = self.merged_query(self.map_view_name,
                                    {"include_docs": "true"})
        self.verify_results(results, {"include_docs": "true"})
        num = 1
        for row in results.get('rows', []):
            self.assertEqual(row['doc']['json']['integer'], num)
            self.assertEqual(row['doc']['json']['string'], str(num))
            self.assertEqual(row['doc']['meta']['id'], str(num))
            num += 1

    def test_queries_reduce(self):
        all_query_params = ['skip', 'limit', 'startkey', 'endkey', 'startkey_docid',
                        'endkey_docid', 'inclusive_end', 'descending', 'reduce',
                        'group', 'group_level']
        current_params = {}
        for key in self.input.test_params:
            if key in all_query_params:
                current_params[key] = str(self.input.test_params[key])
        results = self.merged_query(self.red_view_name, current_params)
        self.verify_results_reduce(results, current_params)

    def test_stale_ok_alternated_docs(self):
        num_vbuckets = len(self.rest.get_vbuckets(self.bucket))
        docs = ViewMergingTests.make_docs(self.num_docs + 1, self.num_docs + 3)
        self.populate_alternated(num_vbuckets, docs)
        results = self.merged_query(self.map_view_name, {'stale': 'ok'})
        self.assertEqual(results.get('total_rows', None), self.num_docs)
        self.assertEqual(len(results.get('rows', None)), self.num_docs)
        self.verify_keys_are_sorted(results)

    def test_stale_update_after_alternated_docs(self):
        # update_after requests use a stats cache for performance reasons.
        # This means that the sequence number you get might be outdated for
        # up to 0.3 seconds. Hence sleep for 0.4 seconds to make sure we
        # see all changes.
        time.sleep(0.4)
        results = self.merged_query(self.map_view_name, {'stale': 'update_after'})
        self.assertEqual(results.get('total_rows', None), self.num_docs)
        self.assertEqual(len(results.get('rows', None)), self.num_docs)

        retries = 50
        for i in range(retries):
            results = self.merged_query(self.map_view_name, {'stale': 'ok'})
            if results['total_rows'] == self.num_docs + 2:
                break
            else:
                # Poll interval to not overwhelm the server
                time.sleep(0.2)
        else:
            self.fail("Updater didn't finish after {0} retries".format(
                    retries))

        self.assertEqual(results.get('total_rows', None), self.num_docs + 2)
        self.assertEqual(len(results.get('rows', None)), self.num_docs + 2)
        self.verify_keys_are_sorted(results)

    def test_stats_error(self):
        nodes = self.input.param("num_nodes", 0)
        params = {'stale': 'false', 'on_error': 'stop'}
        if nodes == 1:
            try:
                self.merged_query(self.red_view_stats_name, params=params, ddoc='test2')
                self.assertTrue(False, "Expected exception when querying _stats view")
            except QueryViewException as ex:
                # Couchbase version < 3.0
                if "Builtin _stats" in ex:
                    expectedStr = 'Error occured querying view ' + self.red_view_stats_name + \
                        ': {"error":"error","reason":"Builtin _stats function requires map' + \
                        ' values to be numbers"}'
                else:
                    expectedStr = 'Error occured querying view ' + self.red_view_stats_name + \
                            ': {"error":"error","reason":"Reducer: Error building index for view `' + \
                            self.red_view_stats_name + '`, reason: Value is not a number (key \\"1\\")"}'

                self.assertEqual(str(ex).strip("\n"), expectedStr)
        else:
            self.assertTrue(nodes > 1)
            results = self.merged_query(self.red_view_stats_name, params=params, ddoc='test2')
            self.assertEqual(len(results.get('rows', None)), 0)
            self.assertEqual(len(results.get('errors', None)), 1)

    def test_dev_view(self):
        # A lot of additional documents are needed in order to trigger the
        # dev views. If the number is too low, production views will be used
        docs = ViewMergingTests.make_docs(0, self.num_docs)
        num_vbuckets = len(self.rest.get_vbuckets(self.bucket))
        self.populate_alternated(num_vbuckets, docs)
        results = self.merged_query(self.map_view_name, {})
        self.verify_results_dev(results)

    def test_view_startkey_endkey_validation(self):
        """Regression test for MB-6591

        This tests makes sure that the validation of startkey/endkey works
        with view, which uses Unicode collation. Return results
        when startkey is smaller than endkey. When endkey is smaller than
        the startkey and exception should be raised. With Unicode collation
        "foo" < "Foo", with raw collation "Foo" < "foo".
        """
        startkey = '"foo"'
        endkey = '"Foo"'
        params = {"startkey": startkey, "endkey": endkey}
        results = self.merged_query(self.map_view_name, params)
        self.assertTrue('rows' in results, "Results were returned")

        # Flip startkey and endkey
        params = {"startkey": endkey, "endkey": startkey}
        self.assertRaises(Exception, self.merged_query, self.map_view_name,
                          params)

    def calculate_matching_keys(self, params):
        keys = list(range(1, self.num_docs + 1))
        if 'descending' in params:
            keys.reverse()
        if 'startkey' in params:
            if params['startkey'].find('[') > -1:
                key = eval(params['startkey'])[0]
            else:
                key = int(params['startkey'])
            keys = keys[keys.index(key):]
            if 'startkey_docid' in params:
                if params['startkey_docid'] > params['startkey']:
                    keys = keys[1:]
        if 'endkey' in params:
            if params['endkey'].find('[') > -1:
                key = eval(params['endkey'])[0]
            else:
                key = int(params['endkey'])
            keys = keys[:keys.index(key) + 1]
            if 'endkey_docid' in params:
                if params['endkey_docid'] < params['endkey']:
                    keys = keys[:-1]
        if 'inclusive_end' in params and params['inclusive_end'] == 'false':
            keys = keys[:-1]
        if 'skip' in params:
            keys = keys[(int(params['skip'])):]
        if 'limit' in params:
            keys = keys[:(int(params['limit']))]
        if 'key' in params:
            if int(params['key']) <= self.num_docs:
                keys = [int(params['key']), ]
            else:
                keys = []
        return keys

    def verify_results(self, results, params):
        if 'keys' not in params:
            expected = self.calculate_matching_keys(params)
            self.assertEqual(
                results.get('total_rows', 0), self.num_docs,
                "total_rows parameter is wrong, expected %d, actual %d"
                % (self.num_docs, results.get('total_rows', 0)))
            self.assertEqual(
                len(results.get('rows', [])), len(expected),
                "Rows number is wrong, expected %d, actual %d"
                % (len(expected), len(results.get('rows', []))))
            if expected:
                # first
                self.assertEqual(
                    results.get('rows', [])[0]['key'], expected[0],
                    "First row key is wrong, expected %d, actual %d"
                    % (expected[0], results.get('rows', [])[0]['key']))
                # last
                self.assertEqual(
                    results.get('rows', [])[-1]['key'], expected[-1],
                    "Last row key is wrong, expected %d, actual %d"
                    % (expected[-1], results.get('rows', [])[-1]['key']))
                desc = ('descending' in params and
                        params['descending'] == 'true')
                self.verify_keys_are_sorted(results, desc=desc)
        else:
            # For the keys parameter there's a test where there's more than
            # one document per emitted key, hence we can't determine the
            # total count. But the important things to verify is that the
            # result contains at most the keys that were supplied as query
            # parameter and that the keys from the response have the same
            # order as the keys that were supplied as query parameter.
            rows = [row['key'] for row in results.get('rows', [])]
            self.log.info("vmx: viewmergetests: rows {}".format(rows))
            actual = list(rows)
            for key in params['keys']:
                while len(rows) > 0 and rows[0] == key:
                    rows.pop(0)

            self.assertEqual(
                len(rows), 0,
                "Result is wrong, expected the result to be sorted and "
                "a subset of the `keys` query parameter. Supplied keys {}, "
                "actual returned keys {}".format(params['keys'], actual))

    def verify_results_dev(self, results):
        # A development view is always a subset of the production view,
        # hence only check for that (and not the exact items)
        expected = self.calculate_matching_keys({})
        self.assertTrue(len(results.get('rows', [])) < len(expected) and
                        len(results.get('rows', [])) > 0,
                          ("Rows number is wrong, expected to be lower than "
                           "%d and >0, but it was %d"
                          % (len(expected), len(results.get('rows', [])))))
        self.assertTrue(
            results.get('rows', [])[0]['key'] != expected[0] or
            results.get('rows', [])[-1]['key'] != expected[-1],
            "Dev view should be a subset, but returned the same as "
            "the production view")
        self.verify_keys_are_sorted(results)

    def verify_results_reduce(self, results, params):
        if 'reduce' in params and params['reduce'] == 'false' or \
          'group_level' in params or 'group' in params and params['group'] == 'true':
            expected = self.calculate_matching_keys(params)
            self.assertEqual(len(results.get('rows', [])), len(expected),
                          "Rows number is wrong, expected %d, actual %d"
                          % (len(expected), len(results.get('rows', []))))
            if expected:
                #first
                self.assertEqual(results.get('rows', [])[0]['key'][0], expected[0],
                                  "First element is wrong, expected %d, actual %d"
                                  % (expected[0], results.get('rows', [])[0]['key'][0]))
                #last
                self.assertEqual(results.get('rows', [])[-1]['key'][0], expected[-1],
                                  "Last element is wrong, expected %d, actual %d"
                                  % (expected[-1], results.get('rows', [])[-1]['key'][0]))
        else:
            expected = self.calculate_matching_keys(params)
            self.assertEqual(results.get('rows', [])[0]['value'], len(expected),
                              "Value for reduce is incorrect. Expected %s, actual %s"
                              % (len(expected), results.get('rows', [])[0]['value']))

    def create_ddocs(self, is_dev_view):
        mapview = View(self.map_view_name, '''function(doc) {
             emit(doc.integer, doc.string);
          }''', dev_view=is_dev_view)
        self.cluster.create_view(self.master, 'test', mapview)
        redview = View(self.red_view_name, '''function(doc) {
             emit([doc.integer, doc.string], doc.integer);
          }''', '''_count''', dev_view=is_dev_view)
        self.cluster.create_view(self.master, 'test', redview)
        redview_stats = View(self.red_view_stats_name, '''function(doc) {
             emit(doc.string, doc.string);
          }''', '''_stats''', dev_view=is_dev_view)
        self.cluster.create_view(self.master, 'test2', redview_stats)
        # The keys view is there to test the `keys` query parameter. In order
        # to reproduce the ordering bug (MB-16618) there must be more than
        # one document with the same key, hence modulo is used
        modulo = self.num_docs // 3
        keysview = View(self.keys_view_name, '''function(doc) {
             emit(doc.integer % ''' + str(modulo) + ''', doc.string);
          }''', '_count', dev_view=is_dev_view)
        self.cluster.create_view(self.master, 'test3', keysview)
        RebalanceHelper.wait_for_persistence(self.master, self.bucket, 0)

    def _get_server(self, port):
        ''' Return server from cluster matching port '''
        for server in self.servers:
            if server.port == port:
                return server
        return None

    def init_clients(self):
        """Initialise clients for all servers there are vBuckets on

        It returns a dict with 'ip:port' as key (this information is also
        stored this way in every vBucket in the `master` property) and
        the MemcachedClient as the value
        """
        clients = {}
        for vbucket in self.bucket.vbuckets:
            if vbucket.master not in clients:
                ip, port = vbucket.master.split(':')
                sport = str((int(port[-2:]))//2 + int(self.master.port))
                clients[vbucket.master] = MemcachedClientHelper.direct_client(self._get_server(sport), self.default_bucket_name)
        return clients

    def populate_alternated(self, num_vbuckets, docs):
        """Every vBucket gets a doc first

        Populating the vBuckets alternated means that every vBucket gets
        a document first, before it receives the second one and so on.

        For example if we have 6 documents named doc-1 ... doc-6 and 3
        vBuckets the result will be:

            vbucket-1: doc-1, doc-4
            vbucket-2: doc-2, doc-5
            vbucket-3: doc-3, doc-6
        """
        for i, doc in enumerate(docs):
            self.insert_into_vbucket(i % num_vbuckets, doc)
        RebalanceHelper.wait_for_persistence(self.master, self.bucket, 0)

    def populate_sequenced(self, num_vbuckets, docs):
        """vBuckets get filled up one by one

        Populating the vBuckets sequenced means that the vBucket gets
        a certain number of documents, before the next one gets some.

        For example if we have 6 documents named doc-1 ... doc-6 and 3
        vBuckets the result will be:

            vbucket-1: doc-1, doc-2
            vbucket-2: doc-3, doc-4
            vbucket-3: doc-5, doc-5
        """
        docs_per_vbucket = len(docs) // num_vbuckets

        for vbucket in range(num_vbuckets):
            start = vbucket * docs_per_vbucket
            end = start + docs_per_vbucket
            for doc in docs[start:end]:
                    self.insert_into_vbucket(vbucket, doc)
        RebalanceHelper.wait_for_persistence(self.master, self.bucket, 0)

    def insert_into_vbucket(self, vbucket_id, doc):
        """Insert a document into a certain vBucket

        The memcached clients must already been initialised in the
        self.clients property.
        """
        vbucket = self.bucket.vbuckets[vbucket_id]
        client = self.clients[vbucket.master]

        client.set(doc['json']['key'], 0, 0, json.dumps(doc['json']['body']).encode("ascii", "ignore"), vbucket_id)

    @staticmethod
    def make_docs(start, end):
        """Create documents

        `key` will be used as a key and won't end up in the final
        document body.
        `body` will be used as the document body
        """
        docs = []
        for i in range(start, end):
            doc = {
                'key': str(i),
                'body': { 'integer': i, 'string': str(i)}}

            docs.append({"meta":{"id": str(i)}, "json": doc })
        return docs

    def merged_query(self, view_name, params={}, ddoc='test'):
       bucket = self.default_bucket_name
       if not 'stale' in params:
           params['stale'] = 'false'
       ddoc = ("", "dev_")[self.is_dev_view] + ddoc
       return self.rest.query_view(ddoc, view_name, bucket, params)

    def verify_keys_are_sorted(self, results, desc=False):
        current_keys = [row['key'] for row in results['rows']]
        self.assertTrue(ViewMergingTests._verify_list_is_sorted(current_keys, desc=desc), 'keys are not sorted')
        self.log.info('rows are sorted by key')

    @staticmethod
    def _verify_list_is_sorted(keys, key=lambda x: x, desc=False):
        if desc:
            return all([key(keys[i]) >= key(keys[i + 1]) for i in range(len(keys) - 1)])
        else:
            return all([key(keys[i]) <= key(keys[i + 1]) for i in range(len(keys) - 1)])
