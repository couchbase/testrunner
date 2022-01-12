# -*- coding: utf-8 -*-

import logger
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection
from couchbase_helper.document import View
from memcached.helper.data_helper import MemcachedClientHelper

class DocumentKeysTests(BaseTestCase):

    def setUp(self):
        super(DocumentKeysTests, self).setUp()

    def tearDown(self):
        super(DocumentKeysTests, self).tearDown()

    """Helper function to initialize data generator"""
    def _init_data_gen(self, key="dockey"):
        age = list(range(5))
        first = ['james', 'sharon']
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator(key, template, age, first, start=0, end=self.num_items)
        return gen_load

    """Helper function to wait for persistence and then verify data/stats on all buckets"""
    def _persist_and_verify(self):

        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self._verify_all_buckets(self.master, max_verify=self.max_verify)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

    """Helper function to verify the data using view query"""
    def _verify_with_views(self, expected_rows):

        for bucket in self.buckets:
            default_map_func = 'function (doc, meta) { emit(meta.id, null);}'
            default_view = View("View", default_map_func, None, False)
            ddoc_name = "key_ddoc"

            self.create_views(self.master, ddoc_name, [default_view], bucket.name)
            query = {"stale" : "false", "connection_timeout" : 60000}
            self.cluster.query_view(self.master, ddoc_name, default_view.name, query, expected_rows, bucket=bucket.name)

    """Perform create/update/delete data ops on the input document key and verify"""
    def _dockey_data_ops(self, dockey="dockey"):

        gen_load = self._init_data_gen(dockey)

        for op in ["create", "update", "delete"]:
            self._load_all_buckets(self.master, gen_load, op, 0)
            self._persist_and_verify()

    """Perform verification with views after loading data"""
    def _dockey_views(self, dockey="dockey"):

        gen_load = self._init_data_gen(dockey)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self._persist_and_verify()

        self._verify_with_views(self.num_items)

    """This function loads data in  bucket and waits for persistence. One node is failed over after that
     and it is verified, data can be retrieved"""
    def _dockey_tap(self, dockey="dockey"):

        gen_load = self._init_data_gen(dockey)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self._persist_and_verify()

        #assert if there are not enough nodes to failover
        rest = RestConnection(self.master)
        num_nodes = len(rest.node_statuses())
        self.assertTrue(num_nodes > 1,
                            "ERROR: Not enough nodes to do failover")

        #failover 1 node(we have 1 replica) and verify the keys
        self.cluster.failover(self.servers[:num_nodes],
                                  self.servers[ (num_nodes - 1) : num_nodes])

        self.nodes_init -= 1
        self._persist_and_verify()

    def test_dockey_whitespace_data_ops(self):
        self._dockey_data_ops("d o c k e y")

    def test_dockey_binary_data_ops(self):
        self._dockey_data_ops("d\ro\nckey")

    def test_dockey_unicode_data_ops(self):
        self._dockey_data_ops("\\u00CA")

    def test_dockey_whitespace_views(self):
        self._dockey_views("doc    key  ")

    def test_dockey_binary_views(self):
        self._dockey_views("docke\0y\n")

    def test_dockey_unicode_views(self):
        self._dockey_views("México")

    def test_dockey_whitespace_tap(self):
        self._dockey_tap("d o c k e y")

    def test_dockey_binary_tap(self):
        self._dockey_tap("d\rocke\0y")

    def test_dockey_unicode_tap(self):
        self._dockey_tap("привет")