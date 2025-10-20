from .tuq import QueryTests
import threading
import random
import numpy as np
import ast
from membase.api.exception import CBQError
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
from lib.vector.vector import SiftVector as sift, FAISSVector as faiss
from lib.vector.vector import LoadVector, QueryVector, UtilVector, IndexVector

class VectorSearchTests(QueryTests):
    def setUp(self):
        super(VectorSearchTests, self).setUp()
        self.bucket = "default"
        self.recall_knn = 100
        self.recall_ann = 40 # TBD
        self.accuracy_ann = 2 # TBD
        self.vector = self.input.param("vector", [1,2,3])
        self.use_xattr = self.input.param("use_xattr", False)
        self.use_base64 = self.input.param("use_base64", False)
        self.use_bigendian = self.input.param("use_bigendian", False)
        self.distance = self.input.param("distance", "L2")
        self.description = self.input.param("description", "IVF,SQ8")
        self.nprobes = self.input.param("nprobes", 3)
        self.dimension = self.input.param("dimension", 128)
        self.query_count = self.input.param("query_count", 10)
        self.index_order = self.input.param("index_order", "tail")
        self.prepare_before = self.input.param("prepare_before", False)
        self.use_bhive = self.input.param("use_bhive", False)
        self.train = self.input.param("train", 10000)
        auth = PasswordAuthenticator(self.master.rest_username, self.master.rest_password)
        self.database = Cluster(f'couchbase://{self.master.ip}', ClusterOptions(auth))
        # Get dataset
        sift().download_sift()
        self.xb = sift().read_base()
        self.xq = sift().read_query()
        self.gt = sift().read_groundtruth()
        # Extend dimension beyond 128
        if self.dimension > 128:
            add_dimension = self.dimension - 128
            xq_add = np.ones((len(self.xq), add_dimension), "float32")
            xb_add = np.ones((len(self.xb), add_dimension), "float32")
            self.xb = np.append(self.xb, xb_add, axis=1)
            self.xq = np.append(self.xq, xq_add, axis=1)
        random.seed()

    def suite_setUp(self):
        super(VectorSearchTests, self).suite_setUp()
        threads = []
        self.log.info("Start loading vector data")
        for i in range(0, len(self.xb), 1000): # load in batches of 1000 docs per thread
            thread = threading.Thread(target=LoadVector().load_batch_documents,args=(self.database, self.xb[i:i+1000], i, self.use_xattr, self.use_base64, self.use_bigendian))
            threads.append(thread)
        # Start threads
        for i in range(len(threads)):
            self.sleep(1)
            threads[i].start()
        # Wait for threads to finish
        for i in range(len(threads)):
            threads[i].join()
        # if testing requires custom data (i.e pushdown, unnest, nested, etc)
        if self.input.param("update_data", False):
            self.log.info("Start updating vector data")
            # make it so some docs do not have a price field
            self.run_cbq_query('UPDATE default SET price = [100,150,200,250][FLOOR(RANDOM() * 4)] limit 20000')
            # add nulls into the data 
            self.run_cbq_query('UPDATE default SET null_field = null limit 20000')
            # add nested data into the data
            self.run_cbq_query('UPDATE default SET nested = {"a": 1, "b": [1,2,3]} limit 20000')
            self.run_cbq_query('UPDATE default SET nested = {"a": 2, "b": [3,2,1]} WHERE nested IS MISSING limit 10000')
        self.log.info("Completed loading vector data")

    def tearDown(self):
        super(VectorSearchTests, self).tearDown()

    def suite_tearDown(self):
        super(VectorSearchTests, self).suite_tearDown()

    def test_knn_distances(self):
        begin = random.randint(0, len(self.xq) - self.query_count)
        self.log.info(f"Running KNN query for range [{begin}:{begin+self.query_count}]")
        distances, indices = QueryVector().search(self.database, self.xq[begin:begin+self.query_count], search_function=self.distance, is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(self.query_count):
            self.log.info(f"Check distance for query {i + begin}")
            fail_count = UtilVector().check_distance(self.xq[i + begin], self.xb, indices[i], distances[i], distance=self.distance)
            self.assertEqual(fail_count, 0, "We got some diff! Check log above.")

    def test_knn_distances_faiss(self):
        index = faiss().create_l2_index(self.xb, dim=self.dimension)
        faiss_distances, faiss_indices = faiss().search_index(index, self.xq)

        begin = random.randrange(0, len(self.xq) - self.query_count)
        self.log.info(f"Running KNN query for range [{begin}:{begin+self.query_count}]")
        distances, indices = QueryVector().search(self.database, self.xq[begin:begin+self.query_count], 'L2_SQUARED', is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(self.query_count):
            self.log.info(f"Check distance for query {i + begin} with FAISS")
            self.assertTrue(np.allclose(distances[i], faiss_distances[begin+i]), f"Couchbase distances: {distances[i]} do not match FAISS: {faiss_distances[begin+i]}")

    def test_knn_search(self):
        # we use existing SIFT ground truth for verification for L2/EUCLIDEAN
        begin = random.randint(0, len(self.xq) - self.query_count)
        self.log.info(f"Running KNN query for range [{begin}:{begin+self.query_count}]")
        distances, indices = QueryVector().search(self.database, self.xq[begin:begin+self.query_count], search_function=self.distance, is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(self.query_count):
            self.log.info(f"Check recall rate for query {begin+i} compare to SIFT ({self.distance})")
            recall, accuracy = UtilVector().compare_result(self.gt[begin+i].tolist(), indices[i].tolist())
            self.log.info(f'Recall rate: {recall}% with acccuracy: {accuracy}%')
            if recall < self.recall_knn:
                self.fail(f"Recall rate of {recall} is less than expected {self.recall_knn}")

    def test_knn_search_faiss(self):
        # we build FAISS ground truth for verification for DOT, COSINE and L2
        begin = random.randint(0, len(self.xq) - self.query_count)
        normalize = False
        if self.distance == 'DOT':
            faiss_index = faiss().create_dot_index(self.xb, dim=self.dimension)
        if self.distance == 'COSINE':
            normalize = True
            faiss_index = faiss().create_cosine_index(self.xb, normalize, dim=self.dimension)
        if self.distance == 'L2_SQUARED':
            faiss_index = faiss().create_l2_index(self.xb, dim=self.dimension)
        faiss_distances, faiss_result = faiss().search_index(faiss_index, self.xq, normalize)

        self.log.info(f"Running KNN query for range [{begin}:{begin+self.query_count}]")
        distances, indices = QueryVector().search(self.database, self.xq[begin:begin+self.query_count], search_function=self.distance, is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(self.query_count):
            self.log.info(f"Check recall rate for query {begin+i} compare to FAISS ({self.distance})")
            recall, accuracy = UtilVector().compare_result(faiss_result[begin+i].tolist(), indices[i].tolist())
            self.log.info(f'Recall rate: {recall}% with acccuracy: {accuracy}%')
            if recall < self.recall_knn and accuracy < 100:
                self.fail(f"Recall rate of {recall} is less than expected {self.recall_knn}")

 
    def test_normalize(self):
        vector = ast.literal_eval(self.vector)
        self.log.info(f"Normalize vector: {vector}")
        result = self.run_cbq_query(f'SELECT normalize_vector({vector}) as norm')
        expected = vector / np.linalg.norm(vector)
        self.assertTrue(np.allclose(expected, result['results'][0]['norm']), f"Got wrong result: {result['results']}")

    def test_normalize_invalid(self):
        vector = ast.literal_eval(self.vector)
        self.log.info(f"Normalize vector: {vector}")
        result = self.run_cbq_query(f'SELECT normalize_vector({vector}) as norm')
        self.assertEqual(result['results'][0]['norm'], None, f"Got wrong result: {result['results']}")

    def test_encode(self):
        vector = ast.literal_eval(self.vector)
        self.log.info(f"Encode vector: {vector}")
        result = self.run_cbq_query(f'SELECT encode_vector({vector}, {self.use_bigendian}) as encoded_vector')
        encoded_vector = result['results'][0]['encoded_vector']
        expected_vector = LoadVector().encode_vector(vector, self.use_bigendian)
        self.assertEqual(encoded_vector, expected_vector, f"We expected: {expected_vector} but got: {encoded_vector}")

    def test_decode(self):
        vector = ast.literal_eval(self.vector)
        encoded_vector = LoadVector().encode_vector(vector, self.use_bigendian)
        self.log.info(f"Decode vector: {encoded_vector}")
        result = self.run_cbq_query(f'SELECT decode_vector("{encoded_vector}", {self.use_bigendian}) as decoded_vector')
        decoded_vector = result['results'][0]['decoded_vector']
        self.assertTrue(np.allclose(decoded_vector, vector), f"We expected: {vector} but got: {decoded_vector}")

 
    def test_explain_knn_no_index(self):
        explain_knn = f'EXPLAIN SELECT id, size, brand FROM default WHERE size = 6 AND brand = "Puma" ORDER BY VECTOR_DISTANCE(vec, {self.xq[1].tolist()}, "{self.distance}") LIMIT 100'
        explain = self.run_cbq_query(explain_knn)
        self.log.info(explain['results'])
        index_scan = explain['results'][0]['plan']['~children'][0]['~children'][0]
        index_name = index_scan['index']
        self.assertEqual(index_name, '#sequentialscan')

    def test_advise_knn(self):
        expected_index1 = f"CREATE INDEX adv_brand_size ON `default`(`brand`,`size`)"
        expected_index2 = f"CREATE INDEX adv_size_brand ON `default`(`size`,`brand`)"
        advise_knn_query = f'ADVISE SELECT id, size, brand FROM default WHERE size = 6 AND brand = "Puma" ORDER BY VECTOR_DISTANCE(vec, {self.xq[1].tolist()}, "{self.distance}") LIMIT 100'
        advise = self.run_cbq_query(advise_knn_query)
        self.log.info(advise['results'])
        adviseinfo = advise['results'][0]['advice']['adviseinfo']
        recommended_indexes = adviseinfo['recommended_indexes']['indexes']
        index_statement = recommended_indexes[0]['index_statement']
        self.assertTrue(index_statement == expected_index1 or index_statement == expected_index2, f"We expected {expected_index1} or {expected_index2} but got {index_statement}")

    def test_prepare(self):
        query_num = 1
        prepare_knn_query = f'prepare prepare_knn_vector as SELECT RAW id FROM default WHERE size = 8 AND brand = "adidas" ORDER BY VECTOR_DISTANCE(vec, {self.xq[query_num].tolist()}, "{self.distance}") LIMIT 100'
        prepare_ann_query = f'prepare prepare_ann_vector as SELECT RAW id FROM default WHERE size = 8 AND brand = "adidas" ORDER BY ANN(vec, {self.xq[query_num].tolist()}, "{self.distance}") LIMIT 100'
        if self.prepare_before:
            prepare_knn = self.run_cbq_query(prepare_knn_query)
            self.log.info(prepare_knn['results'])
            prepare_ann = self.run_cbq_query(prepare_ann_query)
            self.log.info(prepare_ann['results'])
        try:
            self.log.info("Create Vector Index")
            IndexVector().create_index(self.database, index_order=self.index_order, similarity=self.distance, is_xattr=self.use_xattr, is_base64=self.use_base64, network_byte_order=self.use_bigendian, description=self.description, dimension=self.dimension, train=self.train, use_bhive=self.use_bhive)
            self.sleep(10)
            if not self.prepare_before:
                prepare_knn= self.run_cbq_query(prepare_knn_query)
                self.log.info(prepare_knn['results'])
                prepare_ann = self.run_cbq_query(prepare_ann_query)
                self.log.info(prepare_ann['results'])
            exec_prepare_knn = self.run_cbq_query('execute prepare_knn_vector')
            recall, accuracy = UtilVector().compare_result(self.gt[query_num].tolist(), exec_prepare_knn['results'])
            self.log.info(f'RecallKNN rate: {round(recall, 2)}% with acccuracy: {round(accuracy,2)}%')
            self.assertEqual(recall, 100.0)

            exec_prepare_ann = self.run_cbq_query('execute prepare_ann_vector')
            recall, accuracy = UtilVector().compare_result(self.gt[query_num].tolist(), exec_prepare_ann['results'])
            self.log.info(f'Recall ANN rate: {round(recall, 2)}% with acccuracy: {round(accuracy,2)}%')
            if self.prepare_before:
                self.assertEqual(recall, 100.0)
            else:
                operator = prepare_ann['results'][0]['operator']
                children = operator['~child']['~children'][0]['~children'][0]['~children'][0]
                index_name = children['index']
                self.assertEqual(index_name, f'vector_index_{self.distance}')
                self.assertTrue('index_vector' in children)
                self.assertTrue(recall > self.recall_ann)
        finally:
            IndexVector().drop_index(self.database, similarity=self.distance, use_bhive=self.use_bhive)

    def test_knn_zero(self):
        vector_zero = np.zeros(self.dimension).tolist()
        query = f'SELECT id, VECTOR_DISTANCE(vec, {vector_zero}, "{self.distance}") distance FROM default WHERE size = 8 AND brand = "adidas" ORDER BY VECTOR_DISTANCE(vec, {vector_zero}, "{self.distance}") NULLS LAST LIMIT 100'
        result = self.run_cbq_query(query)
        for item in result['results']:
            if self.distance in ['L2', 'L2_SQUARED', 'EUCLIDEAN', 'EUCLIDEAN_SQUARED']:
                self.assertTrue(item['distance'] > 0)
            if self.distance == 'DOT':
                self.assertEqual(item['distance'], 0)
            if self.distance == 'COSINE':
                self.assertEqual(item['distance'], None)
            self.assertTrue(item['id'] > 0)

    def test_knn_zero_data(self):
        vector_one = np.ones(self.dimension).tolist()
        vector_scope = 'vector'
        vector_collection = 'zero'
        query = f'SELECT id, VECTOR_DISTANCE(vec, {vector_one}, "{self.distance}") distance FROM default.{vector_scope}.{vector_collection} WHERE size = 8 AND brand = "adidas" ORDER BY VECTOR_DISTANCE(vec, {vector_one}, "{self.distance}") NULLS LAST LIMIT 10'
        self.run_cbq_query(f'create scope default.{vector_scope} if not exists')
        self.sleep(3, "Waiting for scope to be created")
        self.run_cbq_query(f'create collection default.{vector_scope}.{vector_collection} if not exists')
        self.sleep(3, "Waiting for collection to be created")
        data = np.zeros((1000,self.dimension), "float32")
        ones = np.array([np.ones(self.dimension)])
        data = np.append(data,ones,axis=0)
        try:
            self.log.info("Load data with all [0,0,...,0] vectors and one [1,1,...,1] vector")
            LoadVector().load_batch_documents(self.database, data, 0, self.use_xattr, self.use_base64, self.use_bigendian, scope=vector_scope, collection=vector_collection)
            result = self.run_cbq_query(query)
            for i, item in enumerate(result['results']):
                if i == 0:
                    self.log.info("Let's check we got vector one vector first")
                    self.assertEqual(item['id'], 1000, f"We got vector {data[item['id']]}")
                    self.assertAlmostEqual(round(abs(item['distance'])), UtilVector().vector_dist(vector_one, data[item['id']], self.distance))
                else:
                    if self.distance in ['L2', 'EUCLIDEAN']:
                        expected_distance = UtilVector().l2_dist(vector_one, data[item['id']])
                        self.assertAlmostEqual(item['distance'], expected_distance)
                    if self.distance in ['L2_SQUARED', 'EUCLIDEAN_SQUARED']:
                        expected_distance = UtilVector().l2_dist_sq(vector_one, data[item['id']])
                        self.assertAlmostEqual(item['distance'], expected_distance)
                    if self.distance == 'DOT':
                        self.assertAlmostEqual(round(item['distance']), 0)
                    if self.distance == 'COSINE':
                        self.assertAlmostEqual(item['distance'], None)
        finally:
            self.run_cbq_query(f'DROP COLLECTION default.{vector_scope}.{vector_collection} if exists')