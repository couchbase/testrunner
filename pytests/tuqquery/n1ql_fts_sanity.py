from .tuq import QueryTests
from membase.api.exception import CBQError
from lib.membase.api.rest_client import RestConnection
from pytests.fts.fts_base import CouchbaseCluster
from remote.remote_util import RemoteMachineShellConnection
import json
from pytests.security.rbac_base import RbacBase
from lib.remote.remote_util import RemoteMachineShellConnection
import threading

class N1qlFTSSanityTest(QueryTests):

    def suite_setUp(self):
        super(N1qlFTSSanityTest, self).suite_setUp()


    def setUp(self):
        super(N1qlFTSSanityTest, self).setUp()

        self.log.info("==============  N1qlFTSSanityTest setup has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSSanityTest setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  N1qlFTSSanityTest tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSSanityTest tearDown has completed ==============")
        super(N1qlFTSSanityTest, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  N1qlFTSSanityTest suite_tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSSanityTest suite_tearDown has completed ==============")
        super(N1qlFTSSanityTest, self).suite_tearDown()


    def test_n1ql_syntax_select_from_let(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        self._create_fts_index(index_name="idx_default_fts", doc_count=98784, source_name='default')
        self.drop_index_safe(bucket_name="default", index_name="#primary", is_primary=True)

        self.scan_consistency="NOT_BOUNDED"
        n1ql_query = "select meta().id from default let res=true where search(default, {\"query\": {\"field\": \"email\", \"match\":\"'9'\"}, \"size\":10000})=res"
        fts_request = {"query": {"field": "email", "match": "'9'"}, "size": 10000}
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        total_hits, hits, took, status = rest.run_fts_query(index_name="idx_default_fts",
                                                        query_json=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, hits)
        self.assertEqual(comparison_results, "OK", comparison_results)
        self.log.info("n1ql+fts integration sanity test is passed. Results against n1ql query equal to fts service call results.")
        self.log.info("n1ql results: "+str(n1ql_results))

        explain_result = self.run_cbq_query("explain "+n1ql_query)
        self.assertTrue("idx_default_fts" in str(explain_result), "FTS index is not used!")
        self.log.info("n1ql+fts integration sanity test is passed. FTS index usage is found in execution plan.")
        self._remove_all_fts_indexes()
        self.scan_consistency="REQUEST_PLUS"

    def _create_fts_index(self, index_name='', doc_count=0, source_name=''):
        fts_index_type = self.input.param("fts_index_type", "scorch")

        fts_index = self.cbcluster.create_fts_index(name=index_name, source_name=source_name)
        if fts_index_type == 'upside_down':
            fts_index.update_index_to_upside_down()
        else:
            fts_index.update_index_to_scorch()
        indexed_doc_count = 0
        while indexed_doc_count < doc_count:
            try:
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError as k:
                continue

        return fts_index

    def _compare_n1ql_results_against_fts(self, n1ql_results, fts_results):
        n1ql_doc_ids = []
        for result in n1ql_results:
            n1ql_doc_ids.append(result['id'])
        hits = fts_results
        fts_doc_ids = []
        for hit in hits:
            fts_doc_ids.append(hit['id'])

        if len(n1ql_doc_ids) != len(fts_doc_ids):
            return "Results count does not match for test . FTS - " + str(len(fts_doc_ids)) + ", N1QL - " + str(len(n1ql_doc_ids))
        if sorted(fts_doc_ids) != sorted(n1ql_doc_ids):
            return "Found mismatch in results for test ."
        return "OK"

    def _remove_all_fts_indexes(self):
        indexes = self.cbcluster.get_indexes()
        rest = self.get_rest_client(self.servers[0].rest_username, self.servers[0].rest_password)
        for index in indexes:
            rest.delete_fts_index(index.name)

    def get_rest_client(self, user, password):
        rest = RestConnection(self.cbcluster.get_random_fts_node())
        rest.username = user
        rest.password = password
        return rest
