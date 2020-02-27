# coding=utf-8

import json
from .fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection


class IndexManagementAPI(FTSBaseTest):

    def setUp(self):
        super(IndexManagementAPI, self).setUp()
        self.rest = RestConnection(self._cb_cluster.get_master_node())
        self.fts_rest = RestConnection(self._cb_cluster.get_random_fts_node())
        self.sample_bucket_name = "travel-sample"
        self.sample_index_name = "idx_travel_sample_fts"
        self.sample_index_name_1 = "idx_travel_sample_fts1"
        self.second_index = self._input.param("second_index", None)
        self.run_in_parallel = self._input.param("run_in_parallel", None)
        self.sample_query = {"match": "United States", "field": "country"}

    def tearDown(self):
        super(IndexManagementAPI, self).tearDown()

    def test_ingest_control(self):
        self.load_sample_buckets(self._cb_cluster.get_master_node(), self.sample_bucket_name)
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name)
        self.sleep(5)
        self.fts_rest.stop_fts_index_update(fts_index.name)

        self.sleep(10)
        index_count_after_pause = fts_index.get_indexed_doc_count()
        self.log.info("index paused and number of docs at present : {0}".format(index_count_after_pause))
        self.sleep(30)

        if self.second_index:
            if self.run_in_parallel:
                self._cb_cluster.create_fts_index(name=self.sample_index_name_1, source_name=self.sample_bucket_name)
            else:
                self._cb_cluster.create_fts_index_wait_for_completion(self.sample_index_name_1, self.sample_bucket_name)

        current_index_count = fts_index.get_indexed_doc_count()

        if index_count_after_pause != current_index_count:
            self.fail("Index not paused and number of docs at present : {0}".format(current_index_count))
        else:
            self.log.info("index still paused and number of docs at present : {0}. Now resuming".format(index_count_after_pause))

        self.fts_rest.resume_fts_index_update(fts_index.name)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def test_planfreeze_control(self):
        self.load_sample_buckets(self._cb_cluster.get_master_node(), self.sample_bucket_name)
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name)
        self.sleep(20)
        self.fts_rest.freeze_fts_index_partitions(fts_index.name)
        self.wait_for_indexing_complete()
        index_count_after_freeze = fts_index.get_indexed_doc_count()

        try:
            fts_index.update_index_partitions(1)
            self.sleep(5)
        except Exception as e:
            if "cannot update partition or replica count for a planFrozen index" in str(e):
                self.log.info("Error expected :   {0}".format(e))
            else:
                self.fail("An unexpected error occured:   {0}".format(e))

        current_index_count = fts_index.get_indexed_doc_count()

        if index_count_after_freeze != current_index_count:
            self.fail("Index changed after freeze and update")
        else:
            self.log.info("index still not changed  number of docs at present : {0}. Now unfreezing".format(current_index_count))

        self.fts_rest.unfreeze_fts_index_partitions(fts_index.name)
        fts_index.update_index_partitions(1)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def test_planfreeze_control_with_2indexes(self):
        self.load_sample_buckets(self._cb_cluster.get_master_node(), self.sample_bucket_name)
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name)
        fts_index2 = self._cb_cluster.create_fts_index(name=self.sample_index_name_1, source_name=self.sample_bucket_name)
        self.wait_for_indexing_complete()
        self.fts_rest.freeze_fts_index_partitions(fts_index.name)
        self.sleep(5)
        fts_index2.update_index_partitions(1)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def test_query_control(self):
        self.load_sample_buckets(self._cb_cluster.get_master_node(), self.sample_bucket_name)
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name)
        self.wait_for_indexing_complete()
        self.fts_rest.disable_querying_on_fts_index(fts_index.name)
        self.sleep(5)
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        hits, matches, _, status= fts_index.execute_query(query,
                                                  zero_results_ok=True,
                                                  expected_hits=None,
                                                  expected_no_of_results=None)
        self.log.info("Hits: %s" % hits)
        self.log.info("Matches: %s" % matches)
        if "pindex not available" in str(status["errors"]):
            self.log.info("Expected error \"pindex not available\" exits in errors of query : {0}"
                          .format(status["errors"]))
        else:
            self.fail("Expected error \"pindex not available\" not found")

        self.log.info("now allowing to run query on this index")

        self.fts_rest.enable_querying_on_fts_index(fts_index.name)
        self.sleep(5)
        hits, matches, _, status= fts_index.execute_query(query,
                                                          zero_results_ok=False,
                                                          expected_hits=None,
                                                          expected_no_of_results=None)
        self.log.info("Hits: %s" % hits)
        if "errors" in status.keys():
            self.fail("Errors found while running query : {0}".format(status["errors"]))

    def test_query_control_n1fty(self):
        self.load_data()
        fts_index_1 = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index_1")
        fts_index_2 = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index_2")
        self.wait_for_indexing_complete()
        self.fts_rest.disable_querying_on_fts_index(fts_index_1.name)
        self.sleep(5)
        self.generate_random_queries(fts_index_2, self.num_queries, self.query_types)
        self.run_query_and_compare(fts_index_2, n1ql_executor=self._cb_cluster)

    def test_index_plan_update_disallow_query(self):
        self.load_sample_buckets(self._cb_cluster.get_master_node(), self.sample_bucket_name)
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name)
        self.sleep(3)
        self.fts_rest.disable_querying_on_fts_index(fts_index.name)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        self.log.info("Updating the plan")

        fts_index.update_index_partitions(1)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)