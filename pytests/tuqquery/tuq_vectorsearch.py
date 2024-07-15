from .tuq import QueryTests
import threading
import random
import numpy as np
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
from lib.vector.vector import SiftVector as sift, FAISSVector as faiss
from lib.vector.vector import LoadVector, QueryVector, UtilVector

class VectorSearchTests(QueryTests):
    def setUp(self):
        super(VectorSearchTests, self).setUp()
        self.bucket = "default"
        self.recall_knn = 100
        self.use_xattr = self.input.param("use_xattr", False)
        self.use_base64 = self.input.param("use_base64", False)
        self.use_bigendian = self.input.param("use_bigendian", False)
        self.distance = self.input.param("distance", "L2")
        auth = PasswordAuthenticator(self.master.rest_username, self.master.rest_password)
        self.database = Cluster(f'couchbase://{self.master.ip}', ClusterOptions(auth))
        # Get dataset
        sift().download_sift()
        self.xb = sift().read_base()
        self.xq = sift().read_query()
        self.gt = sift().read_groundtruth()
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
            threads[i].start()
        # Wait for threads to finish
        for i in range(len(threads)):
            threads[i].join()
        self.log.info("Completed loading vector data")

    def tearDown(self):
        super(VectorSearchTests, self).tearDown()

    def suite_tearDown(self):
        super(VectorSearchTests, self).suite_tearDown()

    def test_knn_distances(self):
        begin = random.randint(0, len(self.xq) - 5)
        self.log.info(f"Running KNN query for range [{begin}:{begin+5}]")
        distances, indices = QueryVector().search_knn(self.database, self.xq[begin:begin+5], search_function=self.distance, is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(5):
            self.log.info(f"Check distance for query {i + begin}")
            fail_count = UtilVector().check_distance(self.xq[i + begin], self.xb, indices[i], distances[i], distance=self.distance)
            self.assertEqual(fail_count, 0, "We got some diff! Check log above.")

    def test_knn_distances_faiss(self):
        index = faiss().create_l2_index(self.xb)
        faiss_distances, faiss_indices = faiss().search_index(index, self.xq)

        begin = random.randrange(0, len(self.xq) - 5)
        self.log.info(f"Running KNN query for range [{begin}:{begin+5}]")
        distances, indices = QueryVector().search_knn(self.database, self.xq[begin:begin+5], 'L2_SQUARED', is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(5):
            self.log.info(f"Check distance for query {i + begin} with FAISS")
            self.assertTrue(np.allclose(distances[i], faiss_distances[begin+i]), f"Couchbase distances: {distances[i]} do not match FAISS: {faiss_distances[begin+i]}")

    def test_knn_search(self):
        # we use existing SIFT ground truth for verification for L2/EUCLIDEAN
        begin = random.randint(0, len(self.xq) - 5)
        self.log.info(f"Running KNN query for range [{begin}:{begin+5}]")
        distances, indices = QueryVector().search_knn(self.database, self.xq[begin:begin+5], search_function=self.distance, is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(5):
            self.log.info(f"Check recall rate for query {begin+i} compare to sift ({self.distance})")
            recall = UtilVector().compare_result(self.xb, self.gt[begin+i].tolist(), indices[i].tolist(), self.xq[begin+i], self.distance)
            self.log.info(f'Recall rate: {recall}%')
            if recall < self.recall_knn:
                self.fail(f"Recall rate of {recall} is less than expected {self.recall_knn}")

    def test_knn_search_faiss(self):
        # we build FAISS ground truth for verification for DOT, COSINE and L2
        begin = random.randint(0, len(self.xq) - 5)
        normalize = False
        if self.distance == 'DOT':
            faiss_index = faiss().create_dot_index(self.xb)
        if self.distance == 'COSINE':
            normalize = True
            faiss_index = faiss().create_cosine_index(self.xb, normalize)
        if self.distance == 'L2':
            faiss_index = faiss().create_l2_index(self.xb)
        faiss_distances, faiss_result = faiss().search_index(faiss_index, self.xq, normalize)

        self.log.info(f"Running KNN query for range [{begin}:{begin+5}]")
        distances, indices = QueryVector().search_knn(self.database, self.xq[begin:begin+5], search_function=self.distance, is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(5):
            self.log.info(f"Check recall rate for query {begin+i} compare to sift ({self.distance})")
            recall = UtilVector().compare_result(self.xb, faiss_result[begin+i].tolist(), indices[i].tolist(), self.xq[begin+i], self.distance)
            self.log.info(f'Recall rate: {recall}%')
            if recall < self.recall_knn:
                self.fail(f"Recall rate of {recall} is less than expected {self.recall_knn}")