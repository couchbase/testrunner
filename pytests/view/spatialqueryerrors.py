from collections import defaultdict
import json
import logger
import sys
import time
import types
import unittest

from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import QueryViewException
from membase.helper.spatial_helper import SpatialHelper
from TestInput import TestInputSingleton


class SpatialQueryErrorsTests(BaseTestCase):
    def setUp(self):
        try:
            if 'first_case' not in TestInputSingleton.input.test_params:
                TestInputSingleton.input.test_params['default_bucket'] = False
                TestInputSingleton.input.test_params['skip_cleanup'] = True
                TestInputSingleton.input.test_params['skip_buckets_handle'] = True
            self.default_bucket_name = 'default'
            super(SpatialQueryErrorsTests, self).setUp()
            if 'first_case' in TestInputSingleton.input.test_params:
                self.cluster.rebalance(self.servers[:], self.servers[1:], [])
            # We use only one bucket in this test suite
            self.rest = RestConnection(self.master)
            self.bucket = self.rest.get_bucket(Bucket(name=self.default_bucket_name))
            # num_docs must be a multiple of the number of vbuckets
            self.num_docs = self.input.param("num_docs", 2000)
            # `testname` is used for the design document name as wel as the
            # spatial function name
            self.testname = 'query-errors'
            self.helper = SpatialHelper(self, "default")
            if 'first_case' in TestInputSingleton.input.test_params:
                self.create_ddoc()
                self.helper.insert_docs(self.num_docs, self.testname)
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        # clean up will only performed on the last run
        if 'last_case' in TestInputSingleton.input.test_params:
            TestInputSingleton.input.test_params['skip_cleanup'] = False
            TestInputSingleton.input.test_params['skip_buckets_handle'] = False
            super(SpatialQueryErrorsTests, self).tearDown()
        else:
            self.cluster.shutdown(force=True)
            self._log_finish(self)

    def test_query_errors(self):
        all_params = ['skip', 'limit', 'stale', 'bbox', 'start_range',
                      'end_range']
        query_params = {}
        for key in self.input.test_params:
            if key in all_params:
                query_params[key] = str(self.input.test_params[key])

        try:
            self.spatial_query(query_params)
        except QueryViewException as ex:
            self.assertEqual(self.input.test_params['error'],
                              json.loads(ex.reason)['error'])
        else:
            self.fail("Query did not fail, but should have. "
                      "Query parameters were: {0}".format(query_params))


    def create_ddoc(self):
        view_fn = '''function (doc) {
    if (doc.age !== undefined || doc.height !== undefined ||
            doc.bloom !== undefined || doc.shed_leaves !== undefined) {
        emit([doc.age, doc.height, [doc.bloom, doc.shed_leaves]], doc.name);
    }}'''
        self.helper.create_index_fun(self.testname, view_fn)

    def spatial_query(self, params={}, ddoc='test'):
       bucket = self.default_bucket_name
       if not 'stale' in params:
           params['stale'] = 'false'
       return self.rest.query_view(self.testname, self.testname, bucket,
                                   params, type="spatial")

