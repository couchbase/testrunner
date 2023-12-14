import json
import random
import sys
import threading

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
        self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                      "knn": [{"field": "vector_data", "k": self.k,
                               "vector": []}]}
        super(VectorSearch, self).setUp()

    def tearDown(self):
        super(VectorSearch, self).tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        super(VectorSearch, self).suite_tearDown()

    def perform_validation_of_results(self, matches, neighbours):
        print(f"matches {matches}")
        print(f"neighbours {neighbours}")
        try:
            for i in range(self.k):
                print(
                    f"cb result - fields[sno] = > {matches[i]['fields']['sno']} \n groundtruthresult = {neighbours[i]} ")
        except Exception as e:
            print("Error", str(e))

    def run_vector_query(self, query, index, dataset, neighbours=None):
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
                                                                explain=query['explain'], return_raw_hits=True,
                                                                fields=query['fields'])
        if hits == 0:
            hits = -1
        self.log.info("FTS Hits for Search query: %s" % hits)

        # compare fts and n1ql results if required
        if self.run_n1ql_search_function:
            if n1ql_hits == hits:
                self.log.info(
                    f"Validation for N1QL and FTS Passed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
            else:
                self.log.info({"query": query, "reason": f"N1QL hits =  {n1ql_hits}, FTS hits = {hits}"})

        if neighbours is not None:
            self.perform_validation_of_results(matches, neighbours)

        # validate no of results are k only
        if len(matches) != self.k and n1ql_hits != self.k:
            self.fail(
                f"No of results are not same as k=({self.k} \n k = {self.k} || N1QL hits = {n1ql_hits}  || FTS hits = {hits}")

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
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            neighbours = self.get_groundtruth_file(index['dataset'])
            print(f"neighbours: {neighbours}")
            for count, q in enumerate(queries):
                self.query['knn'][0]['vector'] = q.tolist()
                self.run_vector_query(query=self.query, index=index['index_obj'], dataset=index['dataset'],
                                      neighbours=neighbours[count])

    def test_vector_search_with_wrong_dimensions(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:5]:
                self.query['knn'][0]['vector'] = q.tolist()
                n1ql_hits, hits = self.run_vector_query(query=self.query, index=index['index_obj'],
                                                        dataset=index['dataset'])
                if n1ql_hits != -1 and hits != -1:
                    self.fail("Able to get query results even though index is created with different dimension")

    def create_vector_with_constant_queries_in_background(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)
        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b2.s2.c2")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries:
                self.query['knn'][0]['vector'] = q.tolist()
                thread = threading.Thread(target=self.run_vector_query,
                                          kwargs={'query': self.query, 'index': index['index_obj'],
                                                  'dataset': index['dataset']})
                thread.start()

            for i in range(10):
                idx = [(f"i{i + 10}", "b1.s1.c1")]
                similarity = random.choice(['dot_product', 'l2_norm'])
                vector_fields = {"dims": self.dimension, "similarity": similarity}
                index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector",
                                                             test_indexes=idx,
                                                             vector_fields=vector_fields,
                                                             create_vector_index=True)
                indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:2]:
                self.query['knn'][0]['vector'] = q.tolist()
                self.run_vector_query(query=self.query, index=index['index_obj'], dataset=index['dataset'])

    def generate_random_float_array(self, n):
        min_float_value = sys.float_info.min
        max_float_value = sys.float_info.max
        return [random.uniform(min_float_value, max_float_value) for _ in range(n)]

    def test_vector_search_with_invalid_values(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        # generate invalid vectors
        invalid_vecs = []
        for i in range(4):
            x = self.generate_random_float_array(self.dimension)
            if i == 0:
                x[0] = "A"
            elif i == 1:
                x[0] = "2.0"
            elif i == 2:
                x[0] = '/'
            else:
                x = x[:50]
            invalid_vecs.append(x)
        invalid_vecs.append(set(self.generate_random_float_array(self.dimension)))
        query = f"INSERT INTO `b1`.`s1`.`c1` (KEY, VALUE) VALUES ('vector_data', {invalid_vecs[0]}), ('vector_data', {invalid_vecs[1]}), ('vector_data', {invalid_vecs[2]}), ('vector_data', {invalid_vecs[3]}), ('vector_data', {invalid_vecs[4]});"
        self._cb_cluster.run_n1ql_query(query)
        indexes = []
        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            regular_query = queries[:2]
            regular_query_hits = []
            # run normal queries
            for q in regular_query:
                self.query['knn'][0]['vector'] = q.tolist()
                n1ql_hits, hits = self.run_vector_query(query=self.query, index=index['index_obj'],
                                                        dataset=index['dataset'])
                regular_query_hits.append([n1ql_hits, hits])
            # run invalid queries
            for iv in invalid_vecs:
                if isinstance(iv, set):
                    iv = list(iv)
                self.query['knn'][0]['vector'] = iv
                n1ql_hits, hits = self.run_vector_query(query=self.query, index=index['index_obj'],
                                                        dataset=index['dataset'])
                if n1ql_hits != -1 and hits != -1:
                    self.fail(f"Able to index invalid vector - {iv}")
            # run normal queries
            for count, q in enumerate(regular_query):
                self.query['knn'][0]['vector'] = q.tolist()
                n1ql_hits, hits = self.run_vector_query(query=self.query, index=index['index_obj'],
                                                        dataset=index['dataset'])
                if regular_query_hits[count][0] != n1ql_hits or regular_query_hits[count][1] != hits:
                    self.fail("Hits before running invalid queries are different from hits after running invalid "
                              "queries")

    def delete_vector_with_constant_queries_in_background(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)
        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b2.s2.c2")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector", test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries:
                self.query['knn'][0]['vector'] = q.tolist()
                thread = threading.Thread(target=self.run_vector_query,
                                          kwargs={'query': self.query, 'index': index['index_obj'],
                                                  'dataset': index['dataset']})
                thread.start()

            for i in range(2):
                self._cb_cluster.delete_all_fts_indexes()

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:2]:
                self.query['knn'][0]['vector'] = q.tolist()
                self.run_vector_query(query=self.query, index=index['index_obj'], dataset=index['dataset'])
