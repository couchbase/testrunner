import copy
import json
import logging
import os
import random
import sys
import threading
import struct
import base64
import logger

from TestInput import TestInputSingleton
from lib.membase.api.rest_client import RestConnection
from .fts_backup_restore import FTSIndexBackupClient
from .fts_base import FTSBaseTest


class VectorSearch(FTSBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.log = logger.Logger.get_logger()
        self.__init_logger()
        self.run_n1ql_search_function = self.input.param("run_n1ql_search_function", True)
        self.k = self.input.param("k", 10)
        self.vector_dataset = self.input.param("vector_dataset", "siftsmall")
        if not isinstance(self.vector_dataset, list):
            self.vector_dataset = [self.vector_dataset]
        self.dimension = self.input.param("dimension", 128)
        self.similarity = self.input.param("similarity", "l2_norm")
        self.max_threads = 1000
        self.count = 0
        self.store_in_xattr = self.input.param("store_in_xattr", False)
        self.encode_base64_vector = self.input.param("encode_base64_vector", False)
        if self.store_in_xattr and self.encode_base64_vector:
            self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                          "knn": [{"field": "_$xattrs.vector_encoded", "k": self.k,
                                   "vector_base64": ""}]}
        elif self.store_in_xattr:
            self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                          "knn": [{"field": "_$xattrs.vector_data", "k": self.k,
                                   "vector": []}]}
        elif self.encode_base64_vector:
            self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                          "knn": [{"field": "vector_data_base64", "k": self.k,
                                   "vector_base64": ""}]}
        else:
            self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                          "knn": [{"field": "vector_data", "k": self.k,
                                   "vector": []}]}
        self.expected_accuracy_and_recall = self.input.param("expected_accuracy_and_recall", 85)
        # self.vector_field_type = self.input.param("vector_field_type", "vector")
        self.vector_field_type = "vector_base64" if self.encode_base64_vector else "vector"
        # self.vector_field_name = "vector_data_base64" if self.encode_base64_vector else "vector_data"

        if self.encode_base64_vector:
            if self.store_in_xattr:
                self.vector_field_name = "vector_encoded"
            else:
                self.vector_field_name = "vector_data_base64"
        else:
            self.vector_field_name = "vector_data"

        super(VectorSearch, self).setUp()

    def tearDown(self):
        super(VectorSearch, self).tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        super(VectorSearch, self).suite_tearDown()

    def __init_logger(self):
        if self.input.param("log_level", None):
            self.log.setLevel(level=0)
            for hd in self.log.handlers:
                if str(hd.__class__).find('FileHandler') != -1:
                    hd.setLevel(level=logging.DEBUG)
                else:
                    hd.setLevel(
                        level=getattr(
                            logging,
                            self.input.param(
                                "log_level",
                                None)))

    def get_host_ip(self):
        ip = None

        # could be "linux", "linux2", "linux3", ...
        if sys.platform.startswith("linux"):
            ip = os.popen(
                'ifconfig eth0 | grep "inet " | xargs | cut -d" " -f 2') \
                .read().strip()
        else:
            ip = '127.0.0.1'

        self.log.info("Host_ip = %s" % ip)
        return ip

    def floats_to_little_endian_bytes(self, floats):
        byte_array = bytearray()
        for num in floats:
            float_bytes = struct.pack('<f', num)
            byte_array.extend(float_bytes)

        return byte_array

    def get_base64_encoding(self, array):
        byte_array = self.floats_to_little_endian_bytes(array)
        base64_string = base64.b64encode(byte_array).decode('ascii')
        return base64_string

    def compare_results(self, listA, listB, nameA="listA", nameB="listB"):
        common_elements = set(listA) & set(listB)

        not_common_elements = set(listA) ^ set(listB)
        self.log.info(f"Elements not common in both lists: {not_common_elements}")

        percentage_exist = (len(common_elements) / len(listB)) * 100
        self.log.info(f"Percentage of elements in {nameB} that exist in {nameA}: {percentage_exist:.2f}%")

        accuracy = 0
        if listA[0] == listB[0]:
            accuracy = 1

        return accuracy, percentage_exist

    def perform_validations_from_faiss(self, matches, index, query_vector):
        import faiss
        import numpy as np
        try:
            faiss_index = index.faiss_index
            faiss_query_vector = np.array([query_vector]).astype('float32')
            faiss.normalize_L2(faiss_query_vector)
            distances, ann = faiss_index.search(faiss_query_vector, k=self.k)
            faiss_doc_ids = [i for i in ann[0]]

            fts_doc_ids = [matches[i]['fields']['sno'] - 1 for i in range(self.k)]

            self.log.info(f"Faiss docs sno -----> {faiss_doc_ids}")
            self.log.info(f"FTS docs sno -------> {fts_doc_ids}")

            fts_hits = len(matches)
            if len(faiss_doc_ids) != int(fts_hits):
                msg = "FAIL: FTS hits: %s, while FAISS hits: %s" \
                      % (fts_hits, faiss_doc_ids)
                self.log.error(msg)
                faiss_but_not_fts = list(set(faiss_doc_ids) - set(fts_doc_ids))
                fts_but_not_faiss = list(set(fts_doc_ids) - set(faiss_doc_ids))
                if not (faiss_but_not_fts or fts_but_not_faiss):
                    self.log.info("SUCCESS: Docs returned by FTS = docs"
                                  " returned by FAISS, doc_ids verified")
                else:
                    if fts_but_not_faiss:
                        msg = "FAIL: Following %s doc(s) were not returned" \
                              " by FAISS,but FTS, printing 50: %s" \
                              % (len(fts_but_not_faiss), fts_but_not_faiss[:50])
                    else:
                        msg = "FAIL: Following %s docs were not returned" \
                              " by FTS, but FAISS, printing 50: %s" \
                              % (len(faiss_but_not_fts), faiss_but_not_fts[:50])
                    self.log.error(msg)
                    # self.fail("Validation failed with FAISS index")
            return faiss_doc_ids
        except Exception as e:
            self.log.info(f"Error {str(e)}")

    def get_docs(self, num_items):
        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        split_namespace = bucket.split('.')
        formatted_string = "`.`".join(split_namespace)
        source_bucket = f"`{formatted_string}`"

        n1ql_query = f"SELECT * from {source_bucket} limit {num_items};"
        n1ql_results = self._cb_cluster.run_n1ql_query(n1ql_query)['results']
        coll_name = split_namespace[2]

        docs = [doc[coll_name] for doc in n1ql_results]

        return docs

    def update_doc_vectors(self, docs, new_dimension):

        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        split_namespace = bucket.split('.')
        formatted_string = "`.`".join(split_namespace)
        source_bucket = f"`{formatted_string}`"

        for doc in docs:
            doc_copy = copy.deepcopy(doc)
            vector_data = doc_copy['vector_data']
            current_dimension = len(vector_data)

            dim_to_add = new_dimension - current_dimension
            vectors_to_add = []

            for i in range(dim_to_add):
                random_integer = random.randint(0, 100)
                # Convert the integer to a float
                random_float = float(random_integer)

                vectors_to_add.append(random_float)

            vector_data = vector_data + vectors_to_add

            doc_copy['vector_data'] = vector_data
            doc_key = doc_copy["id"]

            upsert_query = f'''UPSERT INTO {source_bucket} (KEY, VALUE) VALUES ('{doc_key}', {doc_copy});'''
            self.log.info("\nUPSERT QUERY: {}\n".format(upsert_query))
            res = self._cb_cluster.run_n1ql_query(upsert_query)
            self.log.info("\n\nUpsert query result: {}\n\n".format(res))
            status = res['status']
            if not (status == 'success'):
                self.fail("Failed to update doc: {}".format(doc_key))

    def run_vector_query(self, vector, index, neighbours=None,
                         validate_result_count=True, perform_faiss_validation=False,
                         validate_fts_with_faiss=False):
        if isinstance(self.query, str):
            self.query = json.loads(self.query)

        if self.encode_base64_vector:
            self.query['knn'][0]['vector_base64'] = self.get_base64_encoding(vector)
        else:
            self.query['knn'][0]['vector'] = vector
        self.log.info("*" * 20 + f" Running Query # {self.count} - on index {index.name} " + "*" * 20)
        self.count += 1
        # Run fts query via n1ql
        n1ql_hits = -1
        if self.run_n1ql_search_function:
            n1ql_query = f"SELECT COUNT(*) FROM `{index._source_name}`.{index.scope}.{index.collections[0]} AS t1 WHERE SEARCH(t1, {self.query});"
            self.log.info(f" Running n1ql Query - {n1ql_query}")
            n1ql_hits = self._cb_cluster.run_n1ql_query(n1ql_query)['results'][0]['$1']
            if n1ql_hits == 0:
                n1ql_hits = -1
            self.log.info("FTS Hits for N1QL query: %s" % n1ql_hits)

        # Run fts query
        self.log.info(f" Running FTS Query - {self.query}")
        hits, matches, time_taken, status = index.execute_query(query=self.query['query'], knn=self.query['knn'],
                                                                explain=self.query['explain'], return_raw_hits=True,
                                                                fields=self.query['fields'])

        if hits == 0:
            hits = -1

        self.log.info("FTS Hits for Search query: %s" % hits)
        # compare fts and n1ql results if required
        if self.run_n1ql_search_function:
            if n1ql_hits == hits:  #
                self.log.info(
                    f"Validation for N1QL and FTS Passed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
            else:
                self.log.info({"query": self.query, "reason": f"N1QL hits =  {n1ql_hits}, FTS hits = {hits}"})

        recall_and_accuracy = {}

        if validate_fts_with_faiss:
            query_vector = vector
            fts_matches = []
            for i in range(self.k):
                fts_matches.append(matches[i]['fields']['sno'] - 1)

            faiss_results = self.perform_validations_from_faiss(matches, index, query_vector)

            self.log.info("*" * 5 + f"Query RESULT # {self.count - 1}" + "*" * 5)
            self.log.info(f"FTS MATCHES: {fts_matches}")
            self.log.info(f"FAISS MATCHES: {faiss_results}")

            fts_faiss_accuracy, fts_faiss_recall = self.compare_results(faiss_results, fts_matches, "faiss",
                                                                        "fts")
            recall_and_accuracy['fts_faiss_accuracy'] = fts_faiss_accuracy
            recall_and_accuracy['fts_faiss_recall'] = fts_faiss_recall

            self.log.info("*" * 30)

        if neighbours is not None:
            query_vector = vector
            fts_matches = []

            for i in range(self.k):
                fts_matches.append(matches[i]['fields']['sno'] - 1)

            if perform_faiss_validation:
                faiss_results = self.perform_validations_from_faiss(matches, index, query_vector)

            self.log.info("*" * 5 + f"Query RESULT # {self.count}" + "*" * 5)
            self.log.info(f"FTS MATCHES: {fts_matches}")
            if perform_faiss_validation:
                self.log.info(f"FAISS MATCHES: {faiss_results}")
            self.log.info(f"GROUNDTRUTH FILE : {neighbours}")

            if perform_faiss_validation:
                faiss_accuracy, faiss_recall = self.compare_results(neighbours[:100], faiss_results, "groundtruth",
                                                                    "faiss")
                recall_and_accuracy['faiss_accuracy'] = faiss_accuracy
                recall_and_accuracy['faiss_recall'] = faiss_recall
            fts_accuracy, fts_recall = self.compare_results(neighbours[:100], fts_matches, "groundtruth", "fts")

            recall_and_accuracy['fts_accuracy'] = fts_accuracy
            recall_and_accuracy['fts_recall'] = fts_recall

            self.log.info("*" * 30)

        # validate no of results are k only
        if self.run_n1ql_search_function:
            if validate_result_count:
                if len(matches) != self.k and n1ql_hits != self.k:
                    self.fail(
                        f"No of results are not same as k=({self.k} \n k = {self.k} || N1QL hits = {n1ql_hits}  || "
                        f"FTS hits = {hits}")

        return n1ql_hits, hits, matches, recall_and_accuracy

    def validate_similarity(self, index_obj, type_name, new_similarity):
        status, index_def = index_obj.get_index_defn()
        index_definition = index_def["indexDef"]
        if self.store_in_xattr:
            updated_similarity = \
            index_definition['params']['mapping']['types'][type_name]['properties']['_$xattrs']['properties'][
                'vector_data']['fields'][0]['similarity']
        else:
            updated_similarity = \
            index_definition['params']['mapping']['types'][type_name]['properties'][self.vector_field_type]['fields'][
                0]['similarity']

        self.assertTrue(updated_similarity == new_similarity, "Similarity for vector index is not updated, " \
                                                              "Expected: {}, Actual: {}".format(new_similarity,
                                                                                                updated_similarity))

    def validate_dimension(self, index_obj, type_name, new_dimension, index_definition=None):
        if index_definition is None:
            status, index_def = index_obj.get_index_defn()
            index_definition = index_def["indexDef"]
        if self.store_in_xattr:
            updated_dimension = \
            index_definition['params']['mapping']['types'][type_name]['properties']['_$xattrs']['properties'][
                'vector_data'][
                'fields'][0]['dims']
        else:
            updated_dimension = \
            index_definition['params']['mapping']['types'][type_name]['properties'][self.vector_field_type][
                'fields'][0]['dims']

        self.assertTrue(updated_dimension == new_dimension, "Dimensions for vector index are not updated, " \
                                                            "Expected: {}, Actual: {}".format(new_dimension,
                                                                                              updated_dimension))

    # Indexing
    def test_basic_vector_search(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)
        indexes = []

        # create index i1 with l2_norm similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])
        index[0]['dataset'] = bucketvsdataset['bucket_name']
        index_obj = next((item for item in index if item['name'] == "i1"), None)['index_obj']
        index_obj.faiss_index = self.create_faiss_index_from_train_data(index[0]['dataset'])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])
        index[0]['dataset'] = bucketvsdataset['bucket_name']
        index_obj = next((item for item in index if item['name'] == "i2"), None)['index_obj']
        index_obj.faiss_index = self.create_faiss_index_from_train_data(index[0]['dataset'], index_type="IndexFlatIP")

        all_stats = []
        bad_indexes = []
        for index in indexes:
            index_stats = {'index_name': '', 'fts_accuracy': 0, 'fts_recall': 0, 'faiss_accuracy': 0,
                           'faiss_recall': 0}
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            neighbours = self.get_groundtruth_file(index['dataset'])
            fts_accuracy = []
            fts_recall = []
            faiss_accuracy = []
            faiss_recall = []
            perform_faiss_validation = True
            for count, q in enumerate(queries[:self.num_queries]):

                _, _, _, recall_and_accuracy = self.run_vector_query(vector=q.tolist(), index=index['index_obj'],
                                                                     neighbours=neighbours[count],
                                                                     perform_faiss_validation=perform_faiss_validation)
                fts_accuracy.append(recall_and_accuracy['fts_accuracy'])
                fts_recall.append(recall_and_accuracy['fts_recall'])
                if perform_faiss_validation:
                    faiss_accuracy.append(recall_and_accuracy['faiss_accuracy'])
                    faiss_recall.append(recall_and_accuracy['faiss_recall'])

            self.log.info(f"fts_accuracy: {fts_accuracy}")
            self.log.info(f"fts_recall: {fts_recall}")
            if perform_faiss_validation:
                self.log.info(f"faiss_accuracy: {faiss_accuracy}")
                self.log.info(f"faiss_recall: {faiss_recall}")

            index_stats['name'] = index['index_obj'].name
            index_stats['fts_accuracy'] = (sum(fts_accuracy) / len(fts_accuracy)) * 100
            index_stats['fts_recall'] = (sum(fts_recall) / len(fts_recall))

            if perform_faiss_validation:
                index_stats['faiss_accuracy'] = (sum(faiss_accuracy) / len(faiss_accuracy)) * 100
                index_stats['faiss_recall'] = (sum(faiss_recall) / len(faiss_recall))

            if index_stats['fts_accuracy'] < self.expected_accuracy_and_recall or index_stats[
                'fts_recall'] < self.expected_accuracy_and_recall:
                bad_indexes.append(index_stats)
            all_stats.append(index_stats)

        self.log.info(f"Accuracy and recall for queries run on each index : {all_stats}")
        if len(bad_indexes) != 0:
            self.fail(f"Indexes have poor accuracy and recall: {bad_indexes}")

    def test_vector_search_wrong_parameters(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)
        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm", "store": True}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        index_obj = index[0]['index_obj']
        status, index_def = index_obj.get_index_defn()

        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        type_name = bucket[3:]

        if status:
            index_definition = index_def["indexDef"]
            store_value = \
            index_definition['params']['mapping']['types'][type_name]['properties'][self.vector_field_type][
                'fields'][0]['store']
            if store_value:
                self.fail("Index got created with store value of vector field set to True")

    def test_vector_search_with_wrong_dimensions(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:self.num_queries]:

                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'],
                                                                    validate_result_count=False)
                if n1ql_hits != -1 and hits != -1:
                    self.fail("Able to get query results even though index is created with different dimension")

    def create_vector_with_constant_queries_in_background(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)
        indexes = []

        # create index i1 with dot product similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:self.num_queries]:
                thread = threading.Thread(target=self.run_vector_query,
                                          kwargs={'query': self.query, 'index': index['index_obj'],
                                                  })
                thread.start()

        for i in range(3):
            idx = [(f"i{i + 10}", "b1.s1.c1")]
            similarity = random.choice(['dot_product', 'l2_norm'])
            vector_fields = {"dims": self.dimension, "similarity": similarity}
            index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                         field_type=self.vector_field_type,
                                                         test_indexes=idx,
                                                         vector_fields=vector_fields,
                                                         create_vector_index=True)
            indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:2]:
                self.run_vector_query(vector=q.tolist(), index=index['index_obj'])

    def generate_random_float_array(self, n):
        min_float_value = 1.401298464324817e-45  # Minimum representable value for float32 in Go
        max_float_value = 3.4028234663852886e+38  # Maximum representable value for float32 in Go
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
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
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
                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
                regular_query_hits.append([n1ql_hits, hits])
            # run invalid queries
            for iv in invalid_vecs:
                if isinstance(iv, set):
                    iv = list(iv)
                self.run_n1ql_search_function = False
                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=iv, index=index['index_obj'],

                                                                    validate_result_count=False)
                if n1ql_hits != -1 and hits != -1:
                    self.fail(f"Able to index invalid vector - {iv}")
                self.run_n1ql_search_function = True
            # run normal queries
            for count, q in enumerate(regular_query):
                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
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
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        # create index i2 with dot product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        indexes.append(index[0])

        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:self.num_queries]:
                thread = threading.Thread(target=self.run_vector_query,
                                          kwargs={'query': self.query, 'index': index['index_obj']
                                                  })
                thread.start()

        self._cb_cluster.delete_all_fts_indexes()

        # for index in indexes:
        #     index['dataset'] = bucketvsdataset['bucket_name']
        #     queries = self.get_query_vectors(index['dataset'])
        #     for q in queries[:2]:
        #
        #         self.run_vector_query(vector=q.tolist(), index=index['index_obj'], dataset=index['dataset'])

    def test_vector_index_update_dimensions(self):
        new_dimension = self.input.param("new_dim", 130)

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}

        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])

        index_obj = next((item for item in index if item['name'] == "i1"), None)['index_obj']
        index_doc_count = index_obj.get_indexed_doc_count()
        self.log.info("\nIndex doc count before updating: {}\n".format(index_doc_count))
        indexes.append(index[0])

        # update vector index dimension
        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        type_name = bucket[3:]
        index_obj.update_vector_index_dim(new_dimension, type_name, self.vector_field_type)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        self.validate_dimension(index_obj, type_name, new_dimension)

        # create a second index with dot_product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])

        index_obj = next((item for item in index if item['name'] == "i2"), None)['index_obj']
        index_doc_count = index_obj.get_indexed_doc_count()
        self.log.info("\nIndex doc count before updating: {}\n".format(index_doc_count))
        indexes.append(index[0])

        # update vector index dimension
        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        type_name = bucket[3:]
        new_dimension = self.dimension + 5
        index_obj.update_vector_index_dim(new_dimension, type_name, self.vector_field_type)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        self.validate_dimension(index_obj, type_name, new_dimension)

        self.sleep(60, "Wait before querying index")
        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:self.num_queries]:

                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'],

                                                                    validate_result_count=False)
                if n1ql_hits != -1 and hits != -1:
                    self.fail("Able to get query results even though index is created with different dimension")

    def test_vector_search_update_similarity(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # Create index with l2 similarity and change it to dot product
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}

        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])

        index_obj = next((item for item in index if item['name'] == "i1"), None)['index_obj']
        index_doc_count = index_obj.get_indexed_doc_count()
        indexes.append(index[0])

        index = indexes[0]
        index['dataset'] = bucketvsdataset['bucket_name']
        self.wait_for_indexing_complete()
        queries = self.get_query_vectors(index['dataset'])
        self.sleep(60, "Wait before querying index")
        for q in queries[:self.num_queries]:

            n1ql_hits_l2, hits_l2, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
            if n1ql_hits_l2 != self.k or hits_l2 != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_l2, hits_l2, self.k))

        # update vector index similarity
        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        type_name = bucket[3:]
        new_similarity = "dot_product"
        index_obj.update_vector_index_similarity(new_similarity, type_name, self.vector_field_type)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        self.validate_similarity(index_obj, type_name, new_similarity)
        self.sleep(60, "Wait before querying index")
        for q in queries[:self.num_queries]:

            n1ql_hits_dot, hits_dot, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
            if n1ql_hits_dot != self.k or hits_dot != self.k:
                self.fail("Could not get expected hits for index with dot similarity, N1QL hits: {}, Search Hits: {}, " \
                          " Expected: {}".format(n1ql_hits_dot, hits_dot, self.k))

        # Create second index with dot_product similarity and change it to l2_norm
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])

        index_obj = next((item for item in index if item['name'] == "i2"), None)['index_obj']
        index_doc_count = index_obj.get_indexed_doc_count()
        self.log.info("\nIndex doc count before updating: {}\n".format(index_doc_count))
        indexes.append(index[0])

        index = indexes[1]
        index['dataset'] = bucketvsdataset['bucket_name']
        queries = self.get_query_vectors(index['dataset'])
        self.wait_for_indexing_complete()
        self.sleep(60, "Wait before querying index")
        for q in queries[:self.num_queries]:

            n1ql_hits_l2, hits_l2, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
            if n1ql_hits_l2 != self.k or hits_l2 != self.k:
                self.fail("Could not get expected hits for index with dot similarity, N1QL hits: {}, Search Hits: {}," \
                          " Expected: {}".format(n1ql_hits_l2, hits_l2, self.k))

        # update vector index similarity
        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        type_name = bucket[3:]
        new_similarity = "l2_norm"
        index_obj.update_vector_index_similarity(new_similarity, type_name, self.vector_field_type)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        self.validate_similarity(index_obj, type_name, new_similarity)
        self.sleep(60, "Wait before querying index")
        for q in queries[:self.num_queries]:

            n1ql_hits_dot, hits_dot, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
            if n1ql_hits_dot != self.k or hits_dot != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_dot, hits_dot, self.k))

    def test_vector_search_update_partitions(self):
        new_partition_number = self.input.param("update_partitions", 2)

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # creating a vector index with l2_norm similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}

        index_l2_norm = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                             field_type=self.vector_field_type,
                                                             test_indexes=idx,
                                                             vector_fields=vector_fields,
                                                             create_vector_index=True,
                                                             extra_fields=[{"sno": "number"}])

        index_obj_l2_norm = next((item for item in index_l2_norm if item['name'] == "i1"), None)['index_obj']
        index_doc_count = index_obj_l2_norm.get_indexed_doc_count()
        indexes.append(index_l2_norm[0])

        query_index = indexes[0]
        query_index['dataset'] = bucketvsdataset['bucket_name']
        queries = self.get_query_vectors(query_index['dataset'])
        neighbours = self.get_groundtruth_file(query_index['dataset'])
        self.wait_for_indexing_complete()
        self.sleep(60, "Wait before executing queries")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):

            n1ql_hits_l2, hits_l2, matches, _ = self.run_vector_query(vector=q.tolist(), index=query_index['index_obj'],
                                                                      neighbours=neighbours[count])
            if n1ql_hits_l2 != self.k or hits_l2 != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_l2, hits_l2, self.k))

        results_before_update = [matches[i]['fields']['sno'] for i in range(self.k)]

        # update the index partitions
        index_obj_l2_norm.update_index_partitions(new_partition_number)

        status, index_def = index_obj_l2_norm.get_index_defn()
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        index_definition = index_def["indexDef"]
        updated_paritions = index_definition['planParams']['indexPartitions']

        self.assertTrue(new_partition_number == updated_paritions, "Partitions for index i1 did not " \
                                                                   "change, Expected: {}, Actual: {}".format(
            new_partition_number, updated_paritions))

        self.is_index_partitioned_balanced(index=index_obj_l2_norm)
        self.sleep(60, "Wait before querying index")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):

            n1ql_hits_l2, hits_l2, matches, _ = self.run_vector_query(vector=q.tolist(), index=query_index['index_obj'],

                                                                      neighbours=neighbours[count])
            if n1ql_hits_l2 != self.k or hits_l2 != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_l2, hits_l2, self.k))

        results_after_update = [matches[i]['fields']['sno'] for i in range(self.k)]

        # create a second vector index with dot_product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}

        index_dot_product = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                                 field_type=self.vector_field_type,
                                                                 test_indexes=idx,
                                                                 vector_fields=vector_fields,
                                                                 create_vector_index=True,
                                                                 extra_fields=[{"sno": "number"}])

        index_obj_dot_product = next((item for item in index_dot_product if item['name'] == "i2"), None)['index_obj']
        index_doc_count = index_obj_dot_product.get_indexed_doc_count()
        indexes.append(index_dot_product[0])

        query_index = indexes[1]
        query_index['dataset'] = bucketvsdataset['bucket_name']
        queries = self.get_query_vectors(query_index['dataset'])
        self.wait_for_indexing_complete()
        self.sleep(60, "Wait before executing queries")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):

            n1ql_hits_dot, hits_dot, matches, _ = self.run_vector_query(vector=q.tolist(),
                                                                        index=query_index['index_obj'],

                                                                        neighbours=neighbours[count])
            if n1ql_hits_dot != self.k or hits_dot != self.k:
                self.fail(
                    "Could not get expected hits for index with dot product, N1QL hits: {}, Search Hits: {}, Expected: {}".
                    format(n1ql_hits_l2, hits_l2, self.k))

        results_before_update = [matches[i]['fields']['sno'] for i in range(self.k)]

        # update the index partitions
        index_obj_dot_product.update_index_partitions(new_partition_number)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        status, index_def = index_obj_dot_product.get_index_defn()
        index_definition = index_def["indexDef"]
        updated_paritions = index_definition['planParams']['indexPartitions']

        self.assertTrue(new_partition_number == updated_paritions, "Partitions for index i2 did not " \
                                                                   "change, Expected: {}, Actual: {}".format(
            new_partition_number, updated_paritions))

        self.is_index_partitioned_balanced(index=index_obj_dot_product)
        self.sleep(60, "Wait before executing queries")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):

            n1ql_hits_dot, hits_dot, matches, _ = self.run_vector_query(vector=q.tolist(),
                                                                        index=query_index['index_obj'],

                                                                        neighbours=neighbours[count])
            if n1ql_hits_dot != self.k or hits_dot != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_l2, hits_l2, self.k))

        results_after_update = [matches[i]['fields']['sno'] for i in range(self.k)]

    def test_vector_search_update_replicas(self):
        new_replica_number = self.input.param("update_replicas", 2)

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # creating a vector index with l2_norm similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}

        index_l2_norm = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                             field_type=self.vector_field_type,
                                                             test_indexes=idx,
                                                             vector_fields=vector_fields,
                                                             create_vector_index=True,
                                                             extra_fields=[{"sno": "number"}])

        index_obj_l2_norm = next((item for item in index_l2_norm if item['name'] == "i1"), None)['index_obj']
        index_doc_count = index_obj_l2_norm.get_indexed_doc_count()
        indexes.append(index_l2_norm[0])

        query_index = indexes[0]
        query_index['dataset'] = bucketvsdataset['bucket_name']
        queries = self.get_query_vectors(query_index['dataset'])
        neighbours = self.get_groundtruth_file(query_index['dataset'])
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):

            n1ql_hits_l2, hits_l2, matches, _ = self.run_vector_query(vector=q.tolist(), index=query_index['index_obj'],

                                                                      neighbours=neighbours[count])
            if n1ql_hits_l2 != self.k or hits_l2 != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_l2, hits_l2, self.k))

        results_before_update = [matches[i]['fields']['sno'] for i in range(self.k)]

        # update the index partitions
        index_obj_l2_norm.update_num_replicas(new_replica_number)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        status, index_def = index_obj_l2_norm.get_index_defn()
        index_definition = index_def["indexDef"]
        updated_replicas = index_definition['planParams']['numReplicas']

        self.assertTrue(new_replica_number == updated_replicas, "Replicas for index i1 did not " \
                                                                "change, Expected: {}, Actual: {}".format(
            new_replica_number, updated_replicas))

        self.validate_replica_distribution()
        self.sleep(120, "Wait before executing queries")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):

            n1ql_hits_l2, hits_l2, matches, _ = self.run_vector_query(vector=q.tolist(), index=query_index['index_obj'],

                                                                      neighbours=neighbours[count])
            if n1ql_hits_l2 != self.k or hits_l2 != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_l2, hits_l2, self.k))

        results_after_update = [matches[i]['fields']['sno'] for i in range(self.k)]

        # create a second vector index with dot_product similarity
        idx = [("i2", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "dot_product"}

        index_dot_product = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                                 field_type=self.vector_field_type,
                                                                 test_indexes=idx,
                                                                 vector_fields=vector_fields,
                                                                 create_vector_index=True,
                                                                 extra_fields=[{"sno": "number"}])

        index_obj_dot_product = next((item for item in index_dot_product if item['name'] == "i2"), None)['index_obj']
        index_doc_count = index_obj_dot_product.get_indexed_doc_count()
        indexes.append(index_dot_product[0])

        query_index = indexes[1]
        query_index['dataset'] = bucketvsdataset['bucket_name']
        queries = self.get_query_vectors(query_index['dataset'])

        self.sleep(120, "Wait before executing queries")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):
            n1ql_hits_dot, hits_dot, matches, _ = self.run_vector_query(vector=q.tolist(),
                                                                        index=query_index['index_obj'],

                                                                        neighbours=neighbours[count])
            if n1ql_hits_dot != self.k or hits_dot != self.k:
                self.fail(
                    "Could not get expected hits for index with dot product, N1QL hits: {}, Search Hits: {}, Expected: {}".
                    format(n1ql_hits_l2, hits_l2, self.k))

        results_before_update = [matches[i]['fields']['sno'] for i in range(self.k)]

        # update the index partitions
        index_obj_dot_product.update_num_replicas(new_replica_number)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        status, index_def = index_obj_dot_product.get_index_defn()
        index_definition = index_def["indexDef"]
        updated_replicas = index_definition['planParams']['numReplicas']

        self.assertTrue(new_replica_number == updated_replicas, "Partitions for index i2 did not " \
                                                                "change, Expected: {}, Actual: {}".format(
            new_replica_number, updated_replicas))

        self.validate_replica_distribution()

        self.sleep(120, "Wait before executing queries")
        self.log.info("Executing queries on index: {}".format(query_index))
        for count, q in enumerate(queries[:self.num_queries]):

            n1ql_hits_dot, hits_dot, matches, _ = self.run_vector_query(vector=q.tolist(),
                                                                        index=query_index['index_obj'],

                                                                        neighbours=neighbours[count])
            if n1ql_hits_dot != self.k or hits_dot != self.k:
                self.fail("Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                          format(n1ql_hits_l2, hits_l2, self.k))

        results_after_update = [matches[i]['fields']['sno'] for i in range(self.k)]

    def test_vector_search_update_index_concurrently(self):
        create_alias = self.input.param("create_alias", False)
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # creating a vector index with l2_norm similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": "l2_norm"}

        index_l2_norm = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                             field_type=self.vector_field_type,
                                                             test_indexes=idx,
                                                             vector_fields=vector_fields,
                                                             create_vector_index=True,
                                                             extra_fields=[{"sno": "number"}])

        index_obj_l2_norm = next((item for item in index_l2_norm if item['name'] == "i1"), None)['index_obj']
        index_doc_count = index_obj_l2_norm.get_indexed_doc_count()
        indexes.append(index_l2_norm[0])

        if create_alias:
            index_obj_alias = self.create_alias(target_indexes=[index_obj_l2_norm])
            decoded_index = self._decode_index(idx[0])
            collection_index, _type, index_scope, index_collections = self.define_index_params(decoded_index)
            index_obj_alias.scope = index_scope
            index_obj_alias.collections = index_collections
            index_obj_alias._source_name = decoded_index['bucket']
            self.log.info("\n\nIndex Alias object: {}\n\n".format(index_obj_alias.__dict__))

        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        type_name = bucket[3:]
        new_dimension = self.dimension + 5
        new_replica = 1
        new_partitions = 2
        new_similarity = "dot_product"

        threads = []

        thread1 = threading.Thread(target=index_obj_l2_norm.update_vector_index_dim,
                                   args=(new_dimension, type_name, self.vector_field_name))
        threads.append(thread1)
        thread4 = threading.Thread(target=index_obj_l2_norm.update_vector_index_similarity,
                                   args=(new_similarity, type_name, self.vector_field_name, True))
        threads.append(thread4)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        index_obj_l2_norm.update_vector_index_dim(new_dimension, type_name, self.vector_field_name)
        self.sleep(5)
        index_obj_l2_norm.update_num_replicas(new_replica)
        self.sleep(5)
        index_obj_l2_norm.update_index_partitions(new_partitions)
        self.sleep(5)
        index_obj_l2_norm.update_vector_index_similarity(new_similarity, type_name, self.vector_field_name)
        self.sleep(5)

        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        status, index_def = index_obj_l2_norm.get_index_defn()
        index_definition = index_def["indexDef"]

        self.validate_dimension(index_obj_l2_norm, type_name, new_dimension, index_definition)

        updated_replicas = index_definition['planParams']['numReplicas']
        self.assertTrue(new_replica == updated_replicas, "Replicas for index i1 did not " \
                                                         "change, Expected: {}, Actual: {}".format(new_replica,
                                                                                                   updated_replicas))

        updated_paritions = index_definition['planParams']['indexPartitions']
        self.assertTrue(new_partitions == updated_paritions, "Partitions for index i1 did not " \
                                                             "change, Expected: {}, Actual: {}".format(new_partitions,
                                                                                                       updated_paritions))

        self.validate_similarity(index_obj_l2_norm, type_name, new_similarity)
        new_dimension = self.dimension
        index_obj_l2_norm.update_vector_index_dim(new_dimension, type_name, self.vector_field_name)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        self.validate_dimension(index_obj_l2_norm, type_name, new_dimension)
        self.sleep(60, "Wait before executing queries")
        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:self.num_queries]:

                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
                if n1ql_hits != self.k and hits != self.k:
                    self.fail("Could not get expected number of hits for index i1, Expected: {}, N1QL hits: {}, " \
                              " FTS query hits: {}".format(self.k, n1ql_hits, hits))

        if create_alias:
            dataset = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(dataset)
            for q in queries[:self.num_queries]:

                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=q.tolist(), index=index_obj_alias)
                if n1ql_hits != self.k and hits != self.k:
                    self.fail("Could not get expected number of hits for alias index, Expected: {}, N1QL hits: {}, " \
                              " FTS query hits: {}".format(self.k, n1ql_hits, hits))

    def test_vector_search_update_doc(self):

        update_doc_no = self.input.param("update_docs", 10)
        new_dimension = self.input.param("new_dim", 130)
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []

        # creating a vector index with l2_norm similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}

        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])

        index_obj = next((item for item in index if item['name'] == "i1"), None)['index_obj']
        index_doc_count = index_obj.get_indexed_doc_count()
        indexes.append(index[0])

        # update vector index dimension
        buckets = eval(TestInputSingleton.input.param("kv", "{}"))
        bucket = buckets[0]
        type_name = bucket[3:]
        index_obj.update_vector_index_dim(new_dimension, type_name, self.vector_field_type)
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for index to get updated")
        self.validate_dimension(index_obj, type_name, new_dimension)

        docs_to_update = self.get_docs(update_doc_no)
        self.update_doc_vectors(docs_to_update, new_dimension)

        self.sleep(60, "Wait before executing queries")
        for index in indexes:
            index['dataset'] = bucketvsdataset['bucket_name']
            queries = self.get_query_vectors(index['dataset'])
            for q in queries[:self.num_queries]:

                n1ql_hits, hits, matches, _ = self.run_vector_query(vector=q.tolist(), index=index['index_obj'])
                if n1ql_hits != update_doc_no and hits != update_doc_no:
                    self.fail(
                        "Could not get expected hits for index with l2, N1QL hits: {}, Search Hits: {}, Expected: {}".
                        format(n1ql_hits, hits, update_doc_no))

    def test_vector_search_update_doc_dimension(self):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        per_to_resize = eval(TestInputSingleton.input.param("per_to_resize", "[]"))
        dims_to_resize = eval(TestInputSingleton.input.param("dims_to_resize", "[]"))
        faiss_indexes = eval(TestInputSingleton.input.param("faiss_indexes", "[]"))
        perform_faiss_validation = self.input.param("perform_faiss_validation", False)
        faiss_index_node = self.get_host_ip()

        self.log.info(f"Percentage of docs to resize: {per_to_resize}")
        self.log.info(f"Dimensions to resize {dims_to_resize}")
        self.log.info(f"Faiss indexes: {faiss_indexes}")

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        indexes = []
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}

        index_name = "i0"
        idx = [(index_name, "b1.s1.c1")]
        index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                     field_type=self.vector_field_type,
                                                     test_indexes=idx,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     extra_fields=[{"sno": "number"}])
        if len(dims_to_resize) == 0:
            index[0]['perc_docs'] = 1
        else:
            total_per = sum(per_to_resize)
            index[0]['perc_docs'] = 1 - total_per
        indexes.append(index[0])

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset,
                                                percentages_to_resize=per_to_resize,
                                                dims_to_resize=dims_to_resize,
                                                update=True,
                                                faiss_indexes=faiss_indexes,
                                                faiss_index_node=faiss_index_node)

        for i, dim in enumerate(dims_to_resize):
            vector_fields = {"dims": dim, "similarity": self.similarity}

            index_name = "i{}".format(i + 1)
            idx = [(index_name, "b1.s1.c1")]
            index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                         field_type=self.vector_field_type,
                                                         test_indexes=idx,
                                                         vector_fields=vector_fields,
                                                         create_vector_index=True,
                                                         extra_fields=[{"sno": "number"}])

            index[0]['perc_docs'] = per_to_resize[i]
            indexes.append(index[0])
            if perform_faiss_validation:
                index[0]['dataset'] = bucketvsdataset['bucket_name']
                index_obj = index[0]['index_obj']
                index_obj.faiss_index = self.get_faiss_index_from_file(faiss_indexes[i])

        if perform_faiss_validation:
            if len(faiss_indexes) > len(dims_to_resize):
                index = indexes[0]
                index['dataset'] = bucketvsdataset['bucket_name']
                index_obj = index['index_obj']
                index_obj.faiss_index = self.get_faiss_index_from_file(faiss_indexes[-1])

        self.wait_for_indexing_complete()
        self.sleep(60, "Wait before running queries")

        for idx, index in enumerate(indexes):
            index['dataset'] = bucketvsdataset['bucket_name']
            if index['perc_docs'] == 0:
                continue

            self.log.info("Running queries for index: {}".format(index['name']))
            faiss_accuracy = []
            faiss_recall = []

            if idx == 0:
                queries = self.get_query_vectors(index['dataset'])
            else:
                queries = self.get_query_vectors(index['dataset'], dimension=dims_to_resize[idx - 1])

            for q in queries[:self.num_queries]:
                index_stats = {'index_name': '', 'fts_accuracy': 0, 'fts_recall': 0, 'faiss_accuracy': 0,
                               'faiss_recall': 0, 'fts_faiss_accuracy': 0, 'fts_faiss_recall': 0}

                n1ql_hits, hits, matches, recall_and_accuracy = self.run_vector_query(vector=q.tolist(),
                                                                                      index=index['index_obj'],

                                                                                      validate_fts_with_faiss=perform_faiss_validation)
                if n1ql_hits != self.k or hits != self.k:
                    self.sleep(3000, "Wait before failing")
                    self.fail("Could not get expected number of hits, Expected: {}, N1QL hits: {}, " \
                              " FTS query hits: {}".format(self.k, n1ql_hits, hits))

                if perform_faiss_validation:
                    faiss_accuracy.append(recall_and_accuracy['fts_faiss_accuracy'])
                    faiss_recall.append(recall_and_accuracy['fts_faiss_recall'])

            if perform_faiss_validation:
                self.log.info(f"faiss_accuracy: {faiss_accuracy}")
                self.log.info(f"faiss_recall: {faiss_recall}")

            if perform_faiss_validation:
                index_stats['fts_faiss_accuracy'] = (sum(faiss_accuracy) / len(faiss_accuracy)) * 100
                index_stats['fts_faiss_recall'] = (sum(faiss_recall) / len(faiss_recall))

                self.log.info("Index Stats: {}".format(index_stats))

        for faiss_index in faiss_indexes:
            if perform_faiss_validation:
                self.delete_faiss_index_files(faiss_index)

    def test_vector_search_different_dimensions(self):

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)

        per_to_resize = eval(TestInputSingleton.input.param("per_to_resize", "[]"))
        dims_to_resize = eval(TestInputSingleton.input.param("dims_to_resize", "[]"))
        perform_faiss_validation = self.input.param("perform_faiss_validation", False)

        print(f"Percentages to resize {per_to_resize}")
        print(f"Dimensions to resize {dims_to_resize}")
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset,
                                                percentages_to_resize=per_to_resize,
                                                dims_to_resize=dims_to_resize)

        indexes = []
        for i, dim in enumerate(dims_to_resize):
            vector_fields = {"dims": dim, "similarity": self.similarity}

            index_name = "i{}".format(i)
            idx = [(index_name, "b1.s1.c1")]
            index = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                         field_type=self.vector_field_type,
                                                         test_indexes=idx,
                                                         vector_fields=vector_fields,
                                                         create_vector_index=True,
                                                         extra_fields=[{"sno": "number"}])

            indexes.append(index[0])
            if perform_faiss_validation:
                index[0]['dataset'] = bucketvsdataset['bucket_name']
                index_obj = index[0]['index_obj']
                index_obj.faiss_index = self.create_faiss_index_from_train_data(index[0]['dataset'], dimension=dim)

        self.wait_for_indexing_complete()
        self.sleep(60, "Wait before running queries")

        for idx, index in enumerate(indexes):
            index['dataset'] = bucketvsdataset['bucket_name']
            self.log.info("Running queries for index: {}".format(index['name']))
            faiss_accuracy = []
            faiss_recall = []
            queries = self.get_query_vectors(index['dataset'], dimension=dims_to_resize[idx])
            for q in queries[:self.num_queries]:
                index_stats = {'index_name': '', 'fts_accuracy': 0, 'fts_recall': 0, 'faiss_accuracy': 0,
                               'faiss_recall': 0, 'fts_faiss_accuracy': 0, 'fts_faiss_recall': 0}

                n1ql_hits, hits, matches, recall_and_accuracy = self.run_vector_query(vector=q.tolist(),
                                                                                      index=index['index_obj'],

                                                                                      validate_fts_with_faiss=perform_faiss_validation)
                if n1ql_hits != self.k or hits != self.k:
                    self.fail("Could not get expected number of hits, Expected: {}, N1QL hits: {}, " \
                              " FTS query hits: {}".format(self.k, n1ql_hits, hits))

                if perform_faiss_validation:
                    faiss_accuracy.append(recall_and_accuracy['fts_faiss_accuracy'])
                    faiss_recall.append(recall_and_accuracy['fts_faiss_recall'])

            if perform_faiss_validation:
                self.log.info(f"faiss_accuracy: {faiss_accuracy}")
                self.log.info(f"faiss_recall: {faiss_recall}")

            if perform_faiss_validation:
                index_stats['fts_faiss_accuracy'] = (sum(faiss_accuracy) / len(faiss_accuracy)) * 100
                index_stats['fts_faiss_recall'] = (sum(faiss_recall) / len(faiss_recall))

                self.log.info("Index Stats: {}".format(index_stats))

    def test_vector_search_knn_combination_queries(self):
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)

        self.load_data()
        self.wait_for_indexing_complete()
        queries = self.generate_random_queries(index, self.num_queries, self.query_types)

        knn_comb = self.generate_knn_combination_queries(index.fts_queries, index.vector_queries)
        self.log.info("\nKNN comb queries: {}\n".format(len(knn_comb)))

        self.sleep(60, "Wait before executing queries")

        self.run_n1ql_search_function = False
        for query in knn_comb:
            n1ql_hits, hits, matches, _ = self.run_vector_query(vector=query, index=index)
            if hits == -1:
                self.fail("Query returned 0 hits")

    def _check_indexes_definitions(self, index_definitions={}, indexes_for_backup=[]):
        errors = {}

        # check backup filters
        for ix_name in indexes_for_backup:
            if index_definitions[ix_name]['backup_def'] == {} and ix_name in indexes_for_backup:
                error = f"Index {ix_name} is expected to be in backup, but it is not found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {} and ix_name not in indexes_for_backup:
                error = f"Index {ix_name} is not expected to be in backup, but it is found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        # check backup json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                backup_index_defn = index_definitions[ix_name]['backup_def']

                backup_check = self._validate_backup(backup_index_defn, initial_index_defn)
                if not backup_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(
                        f"Backup fts index signature differs from original signature for index {ix_name}.")

        # check restored json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['restored_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                restored_index_defn = index_definitions[ix_name]['restored_def']['indexDef']
                restore_check = self._validate_restored(restored_index_defn, initial_index_defn)
                if not restore_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(
                        f"Restored fts index signature differs from original signature for index {ix_name}")

        return errors

    def _validate_backup(self, backup, initial):
        if 'uuid' in initial.keys():
            del initial['uuid']
        if 'sourceUUID' in initial.keys():
            del initial['sourceUUID']
        if 'uuid' in backup.keys():
            del backup['uuid']
        return backup == initial

    def _validate_restored(self, restored, initial):
        del restored['uuid']
        if 'kvStoreName' in restored['params']['store'].keys():
            del restored['params']['store']['kvStoreName']
        if 'sourceUUID' in restored:
            del restored['sourceUUID']
        if 'sourceUUID' in initial:
            del initial['sourceUUID']
        if restored != initial:
            self.log.info(f"Initial index JSON: {json.dumps(initial)}")
            self.log.info(f"Restored index JSON: {json.dumps(restored)}")
            return False
        return True

    def test_vector_search_backup_restore(self):
        index_definitions = {}

        bucket_name = TestInputSingleton.input.param("bucket", None)

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)
        indexes = []

        # create index i1 with l2_norm similarity
        idx = [("i1", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        indexes = self._create_fts_index_parameterized(field_name=self.vector_field_name,
                                                       field_type=self.vector_field_type,
                                                       test_indexes=idx,
                                                       vector_fields=vector_fields,
                                                       create_vector_index=True,
                                                       extra_fields=[{"sno": "number"}])

        for index in indexes:
            test_index = index['index_obj']
            index_definitions[test_index.name] = {}
            index_definitions[test_index.name]['initial_def'] = {}
            index_definitions[test_index.name]['backup_def'] = {}
            index_definitions[test_index.name]['restored_def'] = {}

            _, index_def = test_index.get_index_defn()
            initial_index_def = index_def['indexDef']
            index_definitions[test_index.name]['initial_def'] = initial_index_def

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        backup_filter = eval(TestInputSingleton.input.param("filter", "None"))
        if bucket_name:
            backup_client = FTSIndexBackupClient(fts_nodes[0], self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            backup_client = FTSIndexBackupClient(fts_nodes[0])

        # perform backup
        if bucket_name:
            status, content = backup_client.backup_bucket_level(_filter=backup_filter,
                                                                bucket=self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            status, content = backup_client.backup(_filter=backup_filter)

        backup_json = json.loads(content)
        backup = backup_json['indexDefs']['indexDefs']

        # store backup index definitions
        # indexes_for_backup = eval(TestInputSingleton.input.param("expected_indexes", "[]"))
        indexes_for_backup = ["i1"]
        for idx in indexes_for_backup:
            backup_index_def = backup[idx]
            index_definitions[idx]['backup_def'] = backup_index_def

        # delete all indexes before restoring from backup
        self._cb_cluster.delete_all_fts_indexes()

        # restoring indexes from backup
        backup_client.restore()

        # getting restored indexes definitions and storing them in indexes definitions dict
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        for ix_name in indexes_for_backup:
            _, restored_index_def = rest.get_fts_index_definition(ix_name)
            index_definitions[ix_name]['restored_def'] = restored_index_def

        # compare all 3 types of index definitions: initial, backed up, and restored from backup
        errors = self._check_indexes_definitions(index_definitions=index_definitions,
                                                 indexes_for_backup=indexes_for_backup)

        # self._cleanup_indexes(index_definitions)

        # errors analysis
        if len(errors.keys()) > 0:
            err_msg = ""
            for err in errors.keys():
                index_errors = errors[err]
                for msg in index_errors:
                    err_msg = err_msg + msg + "\n"
            self.fail(err_msg)

        self.wait_for_indexing_complete()

        self.sleep(60, "Wait before running queries")

        for index in indexes:
            index['dataset'] = self.vector_dataset[0]
            index_obj = index['index_obj']
            index_obj.faiss_index = self.create_faiss_index_from_train_data(index['dataset'])
            queries = self.get_query_vectors(index['dataset'])
            neighbours = self.get_groundtruth_file(index['dataset'])
            for count, q in enumerate(queries[:5]):
                self.run_vector_query(vector=q.tolist(), index=index['index_obj'],
                                      neighbours=neighbours[count])