from fts_base import FTSBaseTest
from couchbase_helper.documentgenerator import *

class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()

    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def simple_index_query_against_cbft_node(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index')
        if self._update or self._delete:
            self.async_perform_update_delete(['salary', 'dept'])
        self.wait_for_indexing_complete()
        docs_indexed = index.get_indexed_doc_count()
        if docs_indexed == 0:
                self.fail("No docs were indexed")
        self.log.info("Docs indexed for index {0}  = {1}".
                      format(index.name, docs_indexed))
        query_json = self.construct_query_json(index.name,'Safiya Morgan')
        hits, _ = self._cb_cluster.run_fts_query('sample_index', query_json)
        if int(hits) == 0:
            self.fail("No docs returned for query : Safiya Morgan")

    def simple_index_query_against_non_cbft_node(self):
        self.load_employee_dataset()
        node = self._cb_cluster.get_random_non_cbft_node()
        try:
            index = self._cb_cluster.create_fts_index('sample_index', node=node)
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
            self._cb_cluster.create_fts_index('sample_index')
            self.wait_for_indexing_complete()
            query_json = self.construct_query_json('sample_index','Safiya Morgan')
            hits, _ = self._cb_cluster.run_fts_query('sample_index', query_json)
        except Exception as e:
            self.log.error(e)

    def create_simple_alias(self):
        self.load_employee_dataset()
        index = self._cb_cluster.create_fts_index('sample_index')
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

    def index_english_wiki(self):
        self.load_wiki()
        index = self._cb_cluster.create_fts_index('wiki_index')
        self.wait_for_indexing_complete()
        docs_indexed = index.get_indexed_doc_count()
        if docs_indexed == 0:
                self.fail("No docs were indexed")

    def index_english_wiki_with_analyzer(self):
        analyzer = self._input.param("analyzer", "")
        self.load_wiki()
        self.sleep(300)

    def test_parallel_index_building(self):
        pass