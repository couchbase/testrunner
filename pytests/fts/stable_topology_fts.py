from fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection
from lib.membase.api.exception import FTSException, ServerUnavailableException


class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()

    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def check_fts_service_started(self):
        try:
            rest = RestConnection(self._cb_cluster.get_random_fts_node())
            rest.get_fts_index_definition("invalid_index")
        except ServerUnavailableException as e:
            raise FTSException("FTS service has not started: %s" %e)

    def create_simple_default_index(self):
        self.load_data()
        plan_params = self.construct_plan_params()
        self.create_default_indexes_all_buckets(plan_params=plan_params)
        if self._update or self._delete:
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)
            self.async_perform_update_delete(self.upd_del_fields)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)

    def run_default_index_query(self):
        self.create_simple_default_index()
        query = eval(self._input.param("query", str(self.sample_query)))
        for index in self._cb_cluster.get_indexes():
            index.execute_query(query, zero_results_ok=False)

    def test_query_type(self):
        self.load_data()
        index = self.create_default_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.run_query_and_compare(index)

    def index_utf16_dataset(self):
        self.load_utf16_data()
        try:
            index = self._cb_cluster.create_fts_index('sample_index',
                                              source_name='default')
            # an exception will most likely be thrown from waiting
            self.wait_for_indexing_complete()
            self.validate_index_count(
                equal_bucket_doc_count=False,
                zero_rows_ok=True,
                must_equal=0)
        except Exception as e:
            raise FTSException("Exception thrown in utf-16 test :{0}".format(e))

    def create_simple_alias(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _ = index.execute_query(self.sample_query,
                                     zero_results_ok=False)
        alias = self.create_alias([index])
        hits2, _ = alias.execute_query(self.sample_query,
                                      zero_results_ok=False)
        if hits != hits2:
            self.fail("Index query yields {0} hits while alias on same index "
                      "yields only {1} hits".format(hits, hits2))
        return index, alias

    def index_wiki(self):
        self.load_wiki(lang=self.lang)
        self._cb_cluster.create_fts_index('wiki_index', source_name='default')
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)

    def delete_index_then_query(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self._cb_cluster.delete_fts_index(index.name)
        try:
            hits2, _ = index.execute_query(self.sample_query)
        except Exception as e:
            # expected, pass test
            self.log.error(" Expected exception: {0}".format(e))

    def drop_bucket_check_index(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self._cb_cluster.delete_bucket("default")
        self.sleep(60, "waiting for bucket deletion to be known by fts")
        status, _ = index.get_index_defn()
        if status:
            self.fail("Able to retrieve index json from index "
                      "built on bucket that was deleted")

    def delete_index_having_alias(self):
        index, alias = self.create_simple_alias()
        self._cb_cluster.delete_fts_index(index.name)
        try:
            hits, _ = index.execute_query(self.sample_query)
            if hits != 0:
                self.fail("Query alias with deleted target returns query results!")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def create_alias_on_deleted_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_default_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        from fts_base import INDEX_DEFAULTS
        alias_def = INDEX_DEFAULTS.ALIAS_DEFINITION
        alias_def['targets'][index.name] = {}
        alias_def['targets'][index.name]['indexUUID'] = index.get_uuid()
        index.delete()
        try:
            self.create_alias([index], alias_def)
            self.fail("Was able to create alias on deleted target")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def edit_index_new_name(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        index.name = "new_index"
        try:
            index.update()
        except Exception as e:
            self.log.info("Expected exception: {0}".format(e))

    def edit_index(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        hits, _ = index.execute_query(self.sample_query)
        new_plan_param = {"maxPartitionsPerPIndex": 30}
        index.index_definition['params'] = \
            index.build_custom_plan_params(new_plan_param)
        index.update()
        hits2, _ = index.execute_query(self.sample_query)
        if hits != hits2:
            self.fail("Changing maxPartitionsPerIndex results in wrong hits for"
                      "same query")


