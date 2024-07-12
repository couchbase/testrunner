from .tuq import QueryTests
import threading
import random
import numpy as np
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
from lib.vector.vector import SiftVector as sift, FAISSVector as faiss
from lib.vector.vector import LoadVector, QueryVector, UtilVector

class VectorSearchKNNTests(QueryTests):
    def setUp(self):
        super(VectorSearchKNNTests, self).setUp()
        self.bucket = "default"
        self.use_xattr = self.input.param("use_xattr", False)
        self.use_base64 = self.input.param("use_base64", False)
        self.use_bigendian = self.input.param("use_bigendian", False)
        auth = PasswordAuthenticator(self.master.rest_username, self.master.rest_password)
        self.database = Cluster(f'couchbase://{self.master.ip}', ClusterOptions(auth))
        # Get dataset
        sift().download_sift()
        self.xb = sift().read_base()
        self.xq = sift().read_query()
        self.gt = sift().read_groundtruth()

    def suite_setUp(self):
        super(VectorSearchKNNTests, self).suite_setUp()
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
        super(VectorSearchKNNTests, self).tearDown()

    def suite_tearDown(self):
        super(VectorSearchKNNTests, self).suite_tearDown()

    def test_knn_distances(self):
        begin = random.randrange(0, len(self.xq) - 5)
        self.log.info(f"Running KNN query for range [{begin}:{begin+5}]")
        distances, indices = QueryVector().search_knn(self.database, self.xq[begin:begin+5], 'L2', is_xattr=self.use_xattr, is_base64=self.use_base64, is_bigendian=self.use_bigendian)
        for i in range(5):
            self.log.info(f"Check distance for query {i + begin}")
            fail_count = UtilVector().check_distance(self.xq[i + begin], self.xb, indices[i], distances[i], distance="L2")
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
