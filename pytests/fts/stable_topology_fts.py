from fts_base import FTSBaseTest
from couchbase_helper.documentgenerator import *
from lib.membase.api.exception import FTSException

class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()

    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def create_simple_index(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index3',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        docs_indexed = index.get_indexed_doc_count()
        if docs_indexed == 0:
                self.fail("No docs were indexed")
        self.log.info("Docs indexed for index {0}  = {1}".
                      format(index.name, docs_indexed))
        return index

    def simple_index_query(self):
        index = self.create_simple_index()
        query = {"query":{"match": "Safiya Morgan", "field": "name"}}
        query_json = self.construct_query_json(index.name, query)
        try:
            hits, matches = self._cb_cluster.run_fts_query(index.name, query_json)
            self.log.info("%s hits, document matches: %s" %(hits, matches))
            if int(hits) == 0:
                self.fail("No docs returned for query : %s" % query_json)
        except Exception as e:
            self.fail("Error running query: %s" % e)


    def simple_index_query_against_non_cbft_node(self):
        self.load_employee_dataset()
        node = self._cb_cluster.get_random_non_fts_node()
        try:
            index = self._cb_cluster.create_fts_index('sample_index', node=node,
                                                        source_name='default')
            if self._update or self._delete:
                self.async_perform_update_delete(['salary', 'manages.team_size'])
            self.wait_for_indexing_complete()
            docs_indexed = index.get_indexed_doc_count()
            if docs_indexed == 0:
                self.fail("No docs were indexed")
            self.log.info("Docs indexed for index {0}  = {1}".
                      format(index.name, docs_indexed))
            query_json = self.construct_query_json('sample_index','Safiya Morgan')
            hits, _ = self._cb_cluster.run_fts_query(node,
                                                     'sample_index',
                                                     query_json)
            if int(hits) == 0:
                self.fail("No docs returned for query : Safiya Morgan")
        except Exception as e:
            self.fail("Encountered error while indexing/querying on a "
                      "non-cbft node : %s" % e)

    def index_utf16_dataset(self):
        self.load_utf16_data()
        try:
            index = self._cb_cluster.create_fts_index('sample_index',
                                              source_name='default')
            # an exception will most likely be thrown from waiting
            self.wait_for_indexing_complete()
            query_json = self.construct_query_json('sample_index','Safiya Morgan')
            hits, _ = self._cb_cluster.run_fts_query('sample_index', query_json)
        except Exception as e:
            raise FTSException("Error in utf-16 test :{0}".format(e))

    def create_simple_alias(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        docs_indexed = index.get_indexed_doc_count()
        if docs_indexed == 0:
                self.fail("No docs were indexed")
        self.log.info("Docs indexed for index {0}  = {1}".
                      format(index.name, docs_indexed))
        query_json = self.construct_query_json(index.name,'Safiya Morgan')
        hits, _ = self._cb_cluster.run_fts_query( index.name, query_json)
        if int(hits) == 0:
            self.fail("No docs returned for query : {0}".format(query_json))
        alias_def = {
                      "{0}".format(index.name): {
                        "indexUUID": index.get_UUID()
                      }
                    }
        alias = self._cb_cluster.create_fts_index(name='sample_alias',
                                                  source_type='couchbase',
                                                  source_name='',
                                                  index_type='alias',
                                                  index_params=alias_def)
        query_json = self.construct_query_json(alias.name,'Safiya Morgan')
        hits2, _ = self._cb_cluster.run_fts_query(alias.name, query_json)
        if hits != hits2:
            self.fail("Index query yields {0} hits while alias on same index "
                      "yields only {1} hits".format(hits, hits2))

    def index_wiki(self):
        self.load_wiki(lang=self.lang)
        index = self._cb_cluster.create_fts_index('wiki_index',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        docs_indexed = index.get_indexed_doc_count()
        if docs_indexed == 0:
            self.fail("No docs were indexed")
        #self.sleep(300)

    def test_configure_plan_params(self):
        plan_params = {}
        plan_params['numReplicas'] = self.index_replicas
        plan_params['maxPartitionsPerPindex'] = self.partitions_per_pindex
        index = self._cb_cluster.create_fts_index('sample_index',
                                                  source_name='default',
                                                  plan_params=plan_params)
        if self._update or self._delete:
            self.async_perform_update_delete(['salary', 'dept'])
        self.wait_for_indexing_complete()
        #TODO : Add some plan validation method
        docs_indexed = index.get_indexed_doc_count()
        if docs_indexed == 0:
                self.fail("No docs were indexed")
        self.log.info("Docs indexed for index {0}  = {1}".
                      format(index.name, docs_indexed))
        query_json = self.construct_query_json(index.name,'Safiya Morgan')
        hits, _ = self._cb_cluster.run_fts_query('sample_index', query_json)
        if int(hits) == 0:
            self.fail("No docs returned for query : Safiya Morgan")

    def test_parallel_index_building(self):
        self.load_employee_dataset()
        for bucket in self._cb_cluster.get_buckets():
            self._cb_cluster.create_fts_index(
                        "{0}-index".format(bucket.name),
                        source_name=bucket.name)
        if self._update or self._delete:
            self.async_perform_update_delete(['salary', 'dept'])
        self.wait_for_indexing_complete()
        docs_expected_in_index = self._cb_cluster.get_doc_count_in_bucket(
            self._cb_cluster.get_buckets()[0])
        for index in self._cb_cluster.get_indexes():
            index_count = index.get_indexed_doc_count()
            if index_count != docs_expected_in_index:
               raise Exception("No of docs indexed in {0} is {1}, expected: {2}"
                               .format(index_count, docs_expected_in_index))

    def delete_index_then_query(self):
        index = self.create_simple_index()
        self._cb_cluster.delete_fts_index(index.name)
        query_json = self.construct_query_json(index.name, 'Safiya Morgan')
        try:
            hits2, _ = self._cb_cluster.run_fts_query('sample_alias', query_json)
        except Exception as e:
            # expected, pass test
            self.log.error(" Expected exception: {0}".format(e))

    def drop_bucket_check_index(self):
        index = self.create_simple_index()
        self._cb_cluster.delete_bucket("default")
        try:
            index_count = index.get_indexed_doc_count()
            self.log.info("Docs present in index whose source bucket has "
                          "been deleted: %s" % index_count)
            self.fail("Able to retrieve index json and doc count %s from index "
                      "built on bucket that was deleted" % index_count)
        except Exception as e:
            self.log.error("Expected exception: {0}".format(e))

    def delete_index_having_alias(self):
        self.create_simple_alias()
        self._cb_cluster.delete_fts_index('sample_index')
        query_json = self.construct_query_json('sample_alias','Safiya Morgan')
        try:
            hits, _ = self._cb_cluster.run_fts_query('sample_alias', query_json)
            if hits != 0:
                self.fail("Query alias with deleted target returns query results!")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def create_alias_on_deleted_index(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        index_name = index.name
        index_uuid = index.get_uuid()
        index.delete()
        alias_def = {
                  "{0}".format(index_name): {
                      "indexUUID": index_uuid
                    }
            }
        try:
            alias = self._cb_cluster.create_fts_index(name='sample_alias',
                                                  source_type='couchbase',
                                                  source_name='',
                                                  index_type='alias',
                                                  index_params=alias_def)
            self.fail("Was able to create alias on deleted target")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def edit_index_new_name(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        index.name = "new_alias"
        try:
            index.create_or_update()
        except Exception as e:
            self.log.info("Expected exception: {0}".format(e))

    def edit_index(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index',
                                                  source_name='default')
        self.wait_for_indexing_complete()
        query_json = self.construct_query_json('sample_index', 'Safiya Morgan')
        hits, _ = self._cb_cluster.run_fts_query('sample_index', query_json)
        new_plan_param = {"maxPartitionsPerPIndex": 30}
        index.index_definition['params'] = \
            index.build_custom_plan_params(new_plan_param)
        index.create_or_update(self._cb_cluster.get_random_fts_node())
        hits2, _ = self._cb_cluster.run_fts_query('sample_index', query_json)
        if hits != hits2:
            self.fail("Changing maxPartitionsPerIndex results in wrong hits for"
                      "same query")


