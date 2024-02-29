# coding=utf-8

from .fts_base import FTSBaseTest
from couchbase_helper.tuq_helper import N1QLHelper
from deepdiff import DeepDiff
from TestInput import TestInputSingleton




class USINGFTS(FTSBaseTest):
    def setUp(self):
        super(USINGFTS, self).setUp()
        self.n1ql_helper = N1QLHelper(use_rest=True, log=self.log)
        self.use_multiindex = TestInputSingleton.input.param("use_multiindex", False)
        self.using_fts_and_gsi = TestInputSingleton.input.param("using_fts_and_gsi", False)
        self.specific_index = TestInputSingleton.input.param("specific_index", False)


    def tearDown(self):
        super(USINGFTS, self).tearDown()

    def test_using_fts_txns(self):
        self.load_data()
        self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.test.collection1", server=self.master)
        if self.use_multiindex:
            self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.test.collection2",
                                           server=self.master)

        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index", collection_index=collection_index, _type=type, analyzer="keyword",
            scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        results = self.n1ql_helper.run_cbq_query(query="START TRANSACTION", txtimeout="2m", server=self.master)
        txn_id = results['results'][0]['txid']

        if self.using_fts_and_gsi:
            query = "select * from default:default.test.collection1 USE INDEX (USING GSI,USING FTS) where dept = 'Sales'"
        elif self.specific_index:
            query = "select * from default:default.test.collection1 USE INDEX (custom_index USING FTS) where dept = 'Sales'"
        else:
            query = "select * from default:default.test.collection1 USE INDEX (USING FTS) where dept = 'Sales'"
        explain_results = self.n1ql_helper.run_cbq_query(query="explain " + query, server=self.master)
        self.assertTrue('IndexFtsSearch' in str(explain_results))
        primary_results = self.n1ql_helper.run_cbq_query(query="select * from default:default.test.collection1 where dept = 'Sales'", server=self.master,txnid=txn_id)
        results = self.n1ql_helper.run_cbq_query(query=query, server=self.master)
        diffs = DeepDiff(set(primary_results), set(results), ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        if self.use_multiindex:
            if self.using_fts_and_gsi:
                query = "select * from default:default.test.collection2 USE INDEX (USING GSI,USING FTS) where dept = 'Sales'"
            elif self.specific_index:
                query = "select * from default:default.test.collection2 USE INDEX (custom_index USING FTS) where dept = 'Sales'"
            else:
                query = "select * from default:default.test.collection2 USE INDEX (USING FTS) where dept = 'Sales'"
            explain_results = self.n1ql_helper.run_cbq_query(
                query="explain " + query,
                server=self.master, txnid=txn_id)
            self.assertTrue('IndexFtsSearch' in str(explain_results))
            primary_results = self.n1ql_helper.run_cbq_query(
                query="select * from default:default.test.collection2 USE INDEX (`#primary`) where dept = 'Sales'",
                server=self.master, txnid=txn_id)
            results = self.n1ql_helper.run_cbq_query(
                query=query,
                server=self.master, txnid=txn_id)
            diffs = DeepDiff(set(primary_results), set(results), ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            results = self.n1ql_helper.run_cbq_query(query="COMMIT TRANSACTION", txnid=txn_id, server=self.master)

    def test_using_fts(self):
        self.load_data()
        self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.test.collection1", server=self.master)
        if self.use_multiindex:
            self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.test.collection2",
                                           server=self.master)

        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index", collection_index=collection_index, _type=type, analyzer="keyword",
            scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        if self.using_fts_and_gsi:
            query = "select * from default:default.test.collection1 USE INDEX (USING GSI,USING FTS) where dept = 'Sales'"
        elif self.specific_index:
            query = "select * from default:default.test.collection1 USE INDEX (custom_index USING FTS) where dept = 'Sales'"
        else:
            query = "select * from default:default.test.collection1 USE INDEX (USING FTS) where dept = 'Sales'"
        explain_results = self.n1ql_helper.run_cbq_query(query="explain " + query, server=self.master)
        self.assertTrue('IndexFtsSearch' in str(explain_results))
        primary_results = self.n1ql_helper.run_cbq_query(query="select * from default:default.test.collection1 where dept = 'Sales'", server=self.master)
        results = self.n1ql_helper.run_cbq_query(query=query, server=self.master)
        diffs = DeepDiff(set(primary_results), set(results), ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        if self.use_multiindex:
            if self.using_fts_and_gsi:
                query = "select * from default:default.test.collection2 USE INDEX (USING GSI,USING FTS) where dept = 'Sales'"
            elif self.specific_index:
                query = "select * from default:default.test.collection2 USE INDEX (custom_index USING FTS) where dept = 'Sales'"
            else:
                query = "select * from default:default.test.collection2 USE INDEX (USING FTS) where dept = 'Sales'"
            explain_results = self.n1ql_helper.run_cbq_query(
                query="explain " + query,
                server=self.master)
            self.assertTrue('IndexFtsSearch' in str(explain_results))
            primary_results = self.n1ql_helper.run_cbq_query(
                query="select * from default:default.test.collection2 USE INDEX (`#primary`) where dept = 'Sales'",
                server=self.master)
            results = self.n1ql_helper.run_cbq_query(
                query=query,
                server=self.master)
            diffs = DeepDiff(set(primary_results), set(results), ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_using_fts_query_context(self):
        self.load_data()
        self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX ON collection1", server=self.master, query_context="default:default.test")
        if self.use_multiindex:
            self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX ON collection2", server=self.master,
                                           query_context="default:default.test")

        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index", collection_index=collection_index, _type=type, analyzer="keyword",
            scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        if self.using_fts_and_gsi:
            query = "select * from collection1 USE INDEX (USING GSI,USING FTS) where dept = 'Sales'"
        elif self.specific_index:
            query = "select * from collection1 USE INDEX (custom_index USING FTS) where dept = 'Sales'"
        else:
            query = "select * from collection1 USE INDEX (USING FTS) where dept = 'Sales'"
        explain_results = self.n1ql_helper.run_cbq_query(query="explain " + query, server=self.master, query_context="default:default.test")
        self.assertTrue('IndexFtsSearch' in str(explain_results))
        primary_results = self.n1ql_helper.run_cbq_query(query="select * from collection1 where dept = 'Sales'",server=self.master, query_context="default:default.test")
        results = self.n1ql_helper.run_cbq_query(query=query, server=self.master, query_context="default:default.test")
        diffs = DeepDiff(set(primary_results), set(results), ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        if self.use_multiindex:
            if self.using_fts_and_gsi:
                query = "select * from collection2 USE INDEX (USING GSI,USING FTS) where dept = 'Sales'"
            elif self.specific_index:
                query = "select * from collection2 USE INDEX (custom_index USING FTS) where dept = 'Sales'"
            else:
                query = "select * from collection2 USE INDEX (USING FTS) where dept = 'Sales'"
            explain_results = self.n1ql_helper.run_cbq_query(
                query="explain " + query,
                server=self.master, query_context="default:default.test")
            self.assertTrue('IndexFtsSearch' in str(explain_results))
            primary_results = self.n1ql_helper.run_cbq_query(
                query="select * from collection2 where dept = 'Sales'", server=self.master,
                query_context="default:default.test")
            results = self.n1ql_helper.run_cbq_query(
                query=query, server=self.master,
                query_context="default:default.test")
            diffs = DeepDiff(set(primary_results), set(results), ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
