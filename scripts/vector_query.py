import shutil
import urllib.request as request
from contextlib import closing
import tarfile
import numpy as np
from numpy import dot
from numpy.linalg import norm
import os
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
import couchbase.subdocument as SD
import base64
import struct
import random
from datetime import timedelta
import faiss

cfg = {
  "sizes":[8,9,10],
  "brands":["nike","adidas"]
}

class UtilVector(object):
    def compare_vector(self, query_vector, xb, actual, expected, dist="L2"):
        fail_count = 0
        for idx, actual_vector in enumerate(actual):
            expected_vector = expected[idx]
            if actual_vector != expected_vector:
                print(f"Warn: Value#{idx} actual: {actual_vector} different than expected {expected_vector}")
                d1 = self.vector_dist(query_vector, xb[expected_vector], dist)
                d2 = self.vector_dist(query_vector, xb[actual_vector], dist)
                print(f"distance of {expected_vector} to query vector is {d1}")
                print(f"distance of {actual_vector} to query vector is {d2}")
                if d1 == d2:
                    continue
                else:
                    fail_count += 1
        if fail_count > 0:
            print (f"Fail rate {fail_count} out of 100")
            return False
        return True
    def compare_result(self, expected, actual, query_vector, dist="L2"):
        if expected == actual:
            print("Success: result matches ground truth")
        else:
            print(f"Warn: result don't match! Let's check each vectors")
            match = self.compare_vector(query_vector, xb, actual, expected, dist)
            if match:
                print(f"Success: difference in some vectors order was due to equidistant vectors")
            else:
                print(f"Fail: result did not match ground truth")
    def vector_dist(self, v1, v2, dist="L2"):
        if dist == "L2" or dist == "EUCLIDEAN":
            return self.l2_dist(v1, v2)
        elif dist == "DOT_PRODUCT":
            return self.dot_product_dist(v1, v2)
        elif dist == "COSINE_SIM":
            return self.cosine_sim_dist(v1, v2)
    def l2_dist(self, v1, v2):
        return float(norm(v1-v2))
    def cosine_sim_dist(self, v1, v2):
        return float(1 - dot(v1, v2)/(norm(v1)*norm(v2)))
    def dot_product_dist(self, v1, v2):
        return float(dot(v1, v2))

class FAISSVector(object):
    def create_dot_index(self, vectors, normalize=False, dim=128):
        data = vectors.copy()
        index = faiss.IndexFlat(dim, faiss.METRIC_INNER_PRODUCT)
        if normalize:
            faiss.normalize_L2(data)
        index.add(data)
        return index
    def create_cosine_index(self, vectors, normalize=False, dim=128):
        data = vectors.copy()
        index = faiss.IndexFlatIP(dim)
        if normalize:
            faiss.normalize_L2(data)
        index.add(data)
        return index
    def create_l2_index(self, vectors, normalize=False, dim=128):
        data = vectors.copy()
        index = faiss.IndexFlatL2(dim)
        if normalize:
            faiss.normalize_L2(data)
        index.add(data)
        return index
    def search_index(self, index, xq, normalize=False, k=100):
        faiss_query_vector = xq.copy()
        if normalize:
            faiss.normalize_L2(faiss_query_vector)
        distances, indices = index.search(faiss_query_vector, k)
        return distances, indices

class SiftVector(object):
    def download_sift(self, vector_dataset='siftsmall'):
        if os.path.exists(f'./{vector_dataset}') != True:
            with closing(request.urlopen(f'ftp://ftp.irisa.fr/local/texmex/corpus/{vector_dataset}.tar.gz')) as r:
                with open(f'{vector_dataset}.tar.gz', 'wb') as f:
                    shutil.copyfileobj(r, f)
            tar = tarfile.open(f'{vector_dataset}.tar.gz', "r:gz")
            tar.extractall()
    def read_fvecs(self, fp):
        a = np.fromfile(fp, dtype='int32')
        d = a[0]
        return a.reshape(-1, d + 1)[:, 1:].copy().view('float32')
    def read_ivecs(self, fp):
        a = np.fromfile(fp, dtype='int32')
        d = a[0]
        return a.reshape(-1, d + 1)[:, 1:].copy()
    def read_base(self, vector_dataset='siftsmall'):
        xb = self.read_fvecs(f'./{vector_dataset}/{vector_dataset}_base.fvecs')
        return xb
    def read_query(self, vector_dataset='siftsmall'):
        xq = self.read_fvecs(f'./{vector_dataset}/{vector_dataset}_query.fvecs')
        return xq
    def read_groundtruth(self, vector_dataset='siftsmall'):
        gt = self.read_ivecs(f'./{vector_dataset}/{vector_dataset}_groundtruth.ivecs')
        return gt

class LoadVector(object):
    def encode_vector(self, vector, is_bigendian=False):
        if is_bigendian:
            endian = '>'
        else:
            endian = '<'
        buf = struct.pack(f'{endian}%sf' % len(vector), *vector)
        return base64.b64encode(buf).decode()
    def load_documents(self, cluster, docs, bucket='siftsmall', scope='_default', collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False):
        cb = cluster.bucket(bucket)
        cb_coll = cb.scope(scope).collection(collection)
        for is1, size in enumerate(cfg["sizes"]):
            for ib, brand in enumerate(cfg["brands"]):
                for idx, x in enumerate(docs):
                    vector = x.tolist()
                    doc = {
                        "id": idx,
                        "size":size,
                        "sizeidx":is1,
                        "brand":brand,
                        "brandidx":ib
                    }
                    self.upsert_document_into_cb(cb_coll, doc, vector, vector_field, is_xattr, is_base64, is_bigendian)
    def upsert_document_into_cb(self, cb_coll, doc, vector, vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False):
        if is_base64:
            vector = self.encode_vector(vector, is_bigendian)
        try:
            key = "vec_" + doc["brand"] + "_" +  str(doc["size"]) + "_" + str(doc["id"])
            cb_coll.upsert(key, doc)
            cb_coll.mutate_in(key, [SD.upsert(vector_field, vector, xattr=is_xattr)])
        except Exception as e:
            print(e)

class QueryVector(object):
    def vector_knn_query(self, vector_field='vec', collection='_default', search_function='L2', is_xattr=False, is_base64=False, network_byte_order=False, direction='ASC', k=100):
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order})"
        if search_function == 'DOT_PRODUCT':
            direction = 'DESC'
        query = f"SELECT RAW id FROM {collection} WHERE size IN $size AND brand IN $brand ORDER BY {search_function}_DIST({vector_field}, $qvec) {direction} LIMIT {k}"
        return query
    def distance_knn_query(self, vector_field='vec', collection='_default', search_function='L2', is_xattr=False, is_base64=False, network_byte_order=False, direction='ASC', k=100):
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order})"
        if search_function == 'DOT_PRODUCT':
            direction = 'DESC'
        query = f"SELECT RAW {search_function}_DIST({vector_field}, $qvec) FROM {collection} WHERE size IN $size AND brand IN $brand ORDER BY {search_function}_DIST({vector_field}, $qvec) {direction} LIMIT {k}"
        return query
    def run_queries(self, cluster, qdocs, gdocs, search_function="L2", bucket='siftsmall', scope='_default', collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        for idx, x in enumerate(qdocs):
            print("-"*30)
            print(f"Running query#{idx} with vector {x.tolist()[:9]} ...")
            qdoc = {"size":[random.choice(cfg["sizes"])], "brand":[random.choice(cfg["brands"])],"qvec": x.tolist(), "sizeidx":0, "brandidx":0}
            self.n1ql_query(cb_scope, qdoc, gdocs[idx], search_function, collection, vector_field, is_xattr, is_base64, is_bigendian)
    def n1ql_query(self, cb_scope, qdoc, gdoc, search_function="L2", collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False):
        vector_query = self.vector_knn_query(vector_field, collection, search_function, is_xattr, is_base64, network_byte_order=is_bigendian)
        actual = []
        expected = gdoc.tolist()
        query_vector = qdoc["qvec"]
        params = {"size": qdoc["size"], "brand":qdoc["brand"],"qvec":query_vector, "sizeidx": qdoc["sizeidx"], "brandidx":qdoc["brandidx"]}
        result = cb_scope.query(
            vector_query,
            QueryOptions(named_parameters=params), metrics=True, timeout=timedelta(seconds=300))
        for row in result.rows():
            actual.append(row)
        print(f"Execution time: {result.metadata().metrics().execution_time()}")
        UtilVector().compare_result(expected, actual, query_vector, dist=search_function)
    def search_knn(self, cluster, xq, search_function="L2", bucket='siftsmall', scope='_default', collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        vectors = []
        distances = []
        for idx, x in enumerate(xq):
            qdoc = {"size":[random.choice(cfg["sizes"])], "brand":[random.choice(cfg["brands"])],"qvec": x.tolist(), "sizeidx":0, "brandidx":0}
            vectors_id = self.n1ql_knn_search(cb_scope, qdoc, search_function, collection, vector_field, is_xattr, is_base64, is_bigendian)
            vectors_distance = self.n1ql_knn_dist(cb_scope, qdoc, search_function, collection, vector_field, is_xattr, is_base64, is_bigendian)
            vectors.append(vectors_id)
            distances.append(vectors_distance)
        return np.array(distances, dtype=np.float32), np.array(vectors, dtype=np.int32)
    def n1ql_knn_search(self, cb_scope, qdoc, search_function, collection, vector_field, is_xattr, is_base64, is_bigendian):
        vector_query = self.vector_knn_query(vector_field, collection, search_function, is_xattr, is_base64, network_byte_order=is_bigendian)
        vectors = []
        query_vector = qdoc["qvec"]
        params = {"size": qdoc["size"], "brand":qdoc["brand"],"qvec":query_vector, "sizeidx": qdoc["sizeidx"], "brandidx":qdoc["brandidx"]}
        result = cb_scope.query(
            vector_query,
            QueryOptions(named_parameters=params), metrics=True, timeout=timedelta(seconds=300))
        for row in result.rows():
            vectors.append(row)
        return vectors
    def n1ql_knn_dist(self, cb_scope, qdoc, search_function, collection, vector_field, is_xattr, is_base64, is_bigendian):
        vector_query = self.distance_knn_query(vector_field, collection, search_function, is_xattr, is_base64, network_byte_order=is_bigendian)
        distances = []
        query_vector = qdoc["qvec"]
        params = {"size": qdoc["size"], "brand":qdoc["brand"],"qvec":query_vector, "sizeidx": qdoc["sizeidx"], "brandidx":qdoc["brandidx"]}
        result = cb_scope.query(
            vector_query,
            QueryOptions(named_parameters=params), metrics=True, timeout=timedelta(seconds=300))
        for row in result.rows():
            distances.append(row)
        return distances

if __name__ == "__main__":
    auth = PasswordAuthenticator('Administrator', 'password')
    cluster = Cluster('couchbase://127.0.0.1', ClusterOptions(auth))

    use_xattr = False
    use_base64 = False
    use_bigendian = False
    load_vector = True
    query_vector = True
    query_vector_dot = False
    query_vector_cosine = False
    query_vector_l2 = False

    print("Download Sift dataset ...")
    SiftVector().download_sift()

    print("Read vector bases, queryies and ground truths ...")
    xb = SiftVector().read_base()
    xq = SiftVector().read_query()
    gt = SiftVector().read_groundtruth()

    if load_vector:
        print("Load sift documents ...")
        LoadVector().load_documents(cluster, xb, is_xattr=use_xattr, is_base64=use_base64, is_bigendian=use_bigendian)
        print

    if query_vector:
        print("Run (l2) queries and compare with SIFT ...")
        QueryVector().run_queries(cluster, xq[:10], gt, is_xattr=use_xattr, is_base64=use_base64, is_bigendian=use_bigendian)
        print

    if query_vector_l2:
        index_l2 = FAISSVector().create_l2_index(xb)
        faiss_l2_distances, faiss_l2_result = FAISSVector().search_index(index_l2, xq)
        print("Run (l2) query and compare with FAISS")
        QueryVector().run_queries(cluster, xq[:5], faiss_l2_result, search_function="L2")
        print

    if query_vector_dot:
        index_dot = FAISSVector().create_dot_index(xb)
        faiss_dot_distances, faiss_dot_result = FAISSVector().search_index(index_dot, xq)
        print("Run (dot product) query and compare with FAISS")
        QueryVector().run_queries(cluster, xq[:5], faiss_dot_result, search_function="DOT_PRODUCT")
        print

    if query_vector_cosine:
        index_cosine = FAISSVector().create_cosine_index(xb, normalize=True)
        faiss_cosine_distances, faiss_cosine_result = FAISSVector().search_index(index_cosine, xq, normalize=True)
        print("Run (cosine sim) query and compare with FAISS")
        QueryVector().run_queries(cluster, xq[:5], faiss_cosine_result, search_function="COSINE_SIM")
        print

    # cb_l2_distances, cb_l2_results = QueryVector().search_knn(cluster, xq[:2], 'L2')