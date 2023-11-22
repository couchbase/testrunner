import json

from TestInput import TestInputSingleton
from .fts_base import FTSBaseTest


class VectorSearch(FTSBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.run_n1ql_search_function = self.input.param("run_n1ql_search_function", True)
        self.k = self.input.param("k", 3)
        self.vector_dataset = self.input.param("vector_dataset", ["siftsmall"])
        self.dimension = self.input.param("dimension", 128)
        self.similarity = self.input.param("similarity", "l2_norm")
        self.max_threads = 1000
        self.count = 0
        super(VectorSearch, self).setUp()

    def tearDown(self):
        super(VectorSearch, self).tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        super(VectorSearch, self).suite_tearDown()

    def perform_validation_of_results(self, hits):
        print("todo")
        # TODO write logic here for fts expected query hits
        # Validation 1;
        # results should match

        # Validation 2
        # no of hits should match

    def run_vector_query(self, query, index):
        print("*" * 20 + f" Running Query # {self.count} - on index {index.name} " + "*" * 20)
        self.count += 1
        if isinstance(query, str):
            query = json.loads(query)

        # Run fts query via n1ql
        n1ql_hits = -1
        if self.run_n1ql_search_function:
            n1ql_query = f"SELECT COUNT(*) FROM `{index._source_name}`.{index.scope}.{index.collections[0]} AS t1 WHERE SEARCH(t1, {query});"
            print(f" Running n1ql Query - {n1ql_query}")
            n1ql_hits = self._cb_cluster.run_n1ql_query(n1ql_query)['results'][0]['$1']
            if n1ql_hits == 0:
                n1ql_hits = -1
            self.log.info("FTS Hits for N1QL query: %s" % n1ql_hits)

        # Run fts query
        print(f" Running FTS Query - {query}")
        hits, matches, time_taken, status = index.execute_query(query=query['query'], knn=query['knn'],
                                                                explain=query['explain'])
        if hits == 0: hits = -1
        self.log.info("FTS Hits for Search query: %s" % hits)

        # compare fts and n1ql results if required
        if self.run_n1ql_search_function:
            if n1ql_hits == hits:
                self.log.info(
                    f"Validation for N1QL and FTS Passed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
            else:
                self.log.info({"query": query, "reason": f"N1QL hits =  {n1ql_hits}, FTS hits = {hits}"})

        self.perform_validation_of_results(hits)

        return n1ql_hits, hits

    # Indexing
    def test_basic_vector_search(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)
        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                      vector_fields=vector_fields,
                                                      create_vector_index=True)
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                      vector_fields=vector_fields,
                                                      create_vector_index=True)
        indexes.append(index[0])

        query = {"query": {"match_none": {}}, "explain": True, "knn": [{"field": "vector_data", "k": self.k,
                                                                        "vector": []}]}
        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'], False)
            for q in queries:
                query['knn'][0]['vector'] = q.tolist()
                self.run_vector_query(query=query, index=index['index_obj'])

    def test_vector_search_with_wrong_dimensions(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                      vector_fields=vector_fields,
                                                      create_vector_index=True)
        index_obj = next((item for item in indexes if item['name'] == "i1"), None)['index_obj']
        index_doc_count = index_obj.get_indexed_doc_count()
        if index_doc_count != 0:
            self.fail("Able to index documents whose dimensions don't match with dimensions of vector documents")
        indexes.append(index)

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                      vector_fields=vector_fields,
                                                      create_vector_index=True)
        index_obj = next((item for item in indexes if item['name'] == "i2"), None)['index_obj']
        index_doc_count = index_obj.get_indexed_doc_count()
        if index_doc_count != 0:
            self.fail("Able to index documents whose dimensions don't match with dimensions of vector documents")
        indexes.append(index)

        query = {"query": {"match_none": {}}, "explain": True, "knn": [{"field": "vector_data", "k": self.k,
                                                                        "vector": []}]}
        print("indexes - {indexes}")
        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'], False)
            for q in queries[:5]:
                query['knn'][0]['vector'] = q.tolist()
                n1ql_hits, hits = self.run_vector_query(query=query, index=index['index_obj'])
                if n1ql_hits != -1 and hits != -1:
                    self.fail("Able to get query results even though index is created with different dimension")
