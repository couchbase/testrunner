from fts_base import FTSBaseTest

class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()

    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def simple_test(self):
        self.load_cluster()
        node = self._cb_cluster.get_random_cbft_node()
        self._cb_cluster.create_fts_index(node,
                                          'sample_index',
                                          'couchbase',
                                          'default',
                                          'bleve'
                                        )
        self.async_perform_update_delete(['salary','dept'])
        query_json = self.construct_query_json('sample_index','Safiya Morgan')
        hits, _ = self._cb_cluster.run_fts_query(node, 'sample_index', query_json)
        if int(hits) == 0:
            self.fail("No docs returned for query : Safiya Morgan")
