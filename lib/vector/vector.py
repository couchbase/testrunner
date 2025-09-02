import shutil
import urllib.request as request
from contextlib import closing
import tarfile
import numpy as np
from numpy import dot
from numpy.linalg import norm
import os
from couchbase.cluster import QueryOptions
import couchbase.subdocument as SD
import base64
import struct
import random
from datetime import timedelta
import faiss

cfg = {
  "sizes":[8,9],
  "brands":["nike","adidas"]
}

class UtilVector(object):
    def check_distance(self, query_vector, xb, vector_results, vector_distances, distance="L2"):
        fail_count = 0
        for idx, vector in enumerate(vector_results):
            actual_distance = vector_distances[idx]
            if distance == "L2" or distance == "EUCLIDEAN":
                expected_distance = self.l2_dist(query_vector, xb[vector])
            if distance == "L2_SQUARED" or distance == "EUCLIDEAN_SQUARED":
                expected_distance = self.l2_dist_sq(query_vector, xb[vector])
            if distance == "DOT":
                expected_distance = - self.dot_product_dist(query_vector, xb[vector])
            if distance == "COSINE":
                expected_distance = self.cosine_dist(query_vector, xb[vector])
            if not(np.isclose(expected_distance,actual_distance)):
                fail_count += 1
                print(f"FAIL: expected: {expected_distance} actual: {actual_distance}")
                continue
            print(f"PASS: expected: {expected_distance} actual: {actual_distance}")
        return fail_count
    def compare_vector(self, actual, expected):
        recall_count = 0
        accuracy_count = 0
        pct_accuracy = 5
        for idx, actual_vector in enumerate(actual):
            if actual_vector in expected[max(0, idx-pct_accuracy):idx+pct_accuracy+1]:
                recall_count += 1
                accuracy_count += 1
            elif actual_vector in expected:
                recall_count += 1
        if recall_count == 0:
            return 0.0, 0.0
        return recall_count / len(expected) * 100, accuracy_count / recall_count * 100
    def compare_result(self, expected, actual):
        if expected == actual:
            return 100.0, 100.0
        else:
            recall, accuracy = self.compare_vector(actual, expected)
            return recall, accuracy
    def vector_dist(self, v1, v2, dist="L2"):
        if dist == "L2" or dist == "EUCLIDEAN":
            return self.l2_dist(v1, v2)
        if dist == "L2_SQUARED" or dist == "EUCLIDEAN_SQUARED":
            return self.l2_dist_sq(v1, v2)
        elif dist == "DOT":
            return self.dot_product_dist(v1, v2)
        elif dist == "COSINE":
            return self.cosine_dist(v1, v2)
    def l2_dist(self, v1, v2):
        return float(norm(v1-v2))
    def l2_dist_sq(self, v1, v2):
        return self.l2_dist(v1, v2)**2
    def cosine_dist(self, v1, v2):
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
    def __init__(self):
        self.dataset = "siftsmall"
        self.dataset_location = "/data"
        if os.path.exists(self.dataset_location) != True:
            self.dataset_location = "."
    def download_sift(self):
        # need to fix this by uploading the tar somewhere and changing this download link eventually
        if os.path.exists(f'{self.dataset_location}/{self.dataset}') != True:
            with closing(request.urlopen(f'ftp://ftp.irisa.fr/local/texmex/corpus/{self.dataset}.tar.gz')) as r:
                with open(f'{self.dataset}.tar.gz', 'wb') as f:
                    shutil.copyfileobj(r, f)
            tar = tarfile.open(f'{self.dataset}.tar.gz', "r:gz")
            tar.extractall(self.dataset_location)
    def read_fvecs(self, fp):
        a = np.fromfile(fp, dtype='int32')
        d = a[0]
        return a.reshape(-1, d + 1)[:, 1:].copy().view('float32')
    def read_ivecs(self, fp):
        a = np.fromfile(fp, dtype='int32')
        d = a[0]
        return a.reshape(-1, d + 1)[:, 1:].copy()
    def read_base(self, vector_dataset='siftsmall'):
        xb = self.read_fvecs(f'{self.dataset_location}/{vector_dataset}/{vector_dataset}_base.fvecs')
        return xb
    def read_query(self, vector_dataset='siftsmall'):
        xq = self.read_fvecs(f'{self.dataset_location}/{vector_dataset}/{vector_dataset}_query.fvecs')
        return xq
    def read_groundtruth(self, vector_dataset='siftsmall'):
        gt = self.read_ivecs(f'{self.dataset_location}/{vector_dataset}/{vector_dataset}_groundtruth.ivecs')
        return gt

class LoadVector(object):
    def encode_vector(self, vector, is_bigendian=False):
        if is_bigendian:
            endian = '>'
        else:
            endian = '<'
        buf = struct.pack(f'{endian}%sf' % len(vector), *vector)
        return base64.b64encode(buf).decode()
    def load_batch_documents(self, cluster, docs, batch, is_xattr=False, is_base64=False, is_bigendian=False, bucket='default', scope='_default', collection='_default', vector_field='vec'):
        cb = cluster.bucket(bucket)
        cb_coll = cb.scope(scope).collection(collection)
        documents = {}
        for is1, size in enumerate(cfg["sizes"]):
            for ib, brand in enumerate(cfg["brands"]):
                documents = {}
                for idx, x in enumerate(docs):
                    vector = x.tolist()
                    if is_base64:
                        vector = self.encode_vector(vector, is_bigendian)
                    key = f"vec_{brand}_{size}_{idx+batch}"
                    doc = {
                        "id": idx + batch,
                        "size":size,
                        "sizeidx":is1,
                        "brand":brand,
                        "brandidx":ib,
                        vector_field: vector
                    }
                    # if is_xattr:
                    #     del doc[vector_field]
                    documents[key] = doc
                try:
                    upsert = cb_coll.upsert_multi(documents)
                except Exception as e:
                    print(e)
                if is_xattr:
                    for key in documents:
                        cb_coll.mutate_in(key, [SD.upsert(vector_field, documents[key][vector_field], xattr=is_xattr), SD.remove(vector_field)])
    def multi_upsert_document_into_cb(self, cb_coll, documents):
        try:
            cb_coll.upsert_multi(documents)
        except Exception as e:
            print(e)

class IndexVector(object):
    def create_index(self, cluster, bucket='default', scope='_default', collection='_default', index_order='tail', vector_field='vec', is_xattr=False, is_base64=False, network_byte_order=False, dimension=128, train=10000, description='IVF,PQ32x8', similarity='L2_SQUARED', nprobes=3, use_bhive=False, custom_index_fields=None,custom_name=None,use_partition=False):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order})"
        
        vector_definition = {"dimension": dimension, "train_list": train, "description": description, "similarity": similarity, "scan_nprobes": nprobes}
        index_queries = {
            'tail': f'CREATE INDEX vector_index_{similarity} IF NOT EXISTS ON {collection}(size, brand, {vector_field} VECTOR) WITH {vector_definition}',
            'mid': f'CREATE INDEX vector_index_{similarity} IF NOT EXISTS ON {collection}(size, {vector_field} VECTOR, brand) WITH {vector_definition}',
            'lead': f'CREATE INDEX vector_index_{similarity} IF NOT EXISTS ON {collection}({vector_field} VECTOR, size, brand) WITH {vector_definition}',
        }
        if custom_index_fields:
            if custom_name:
                index_query = f'CREATE INDEX {custom_name} IF NOT EXISTS ON {collection}({custom_index_fields}) WITH {vector_definition}'
            else:
                index_query = f'CREATE INDEX vector_index_{similarity}_custom IF NOT EXISTS ON {collection}({custom_index_fields}) WITH {vector_definition}'
        else:
            index_query = index_queries[index_order]
        if use_bhive:
            if custom_index_fields:
                if ",vec VECTOR" in custom_index_fields:
                    custom_index_fields = custom_index_fields.replace(",vec VECTOR", "")
                    if custom_name:
                        index_query = f'CREATE VECTOR INDEX {custom_name} IF NOT EXISTS ON {collection}({vector_field} VECTOR) INCLUDE({custom_index_fields}) WITH {vector_definition}'
                    else:
                        index_query = f'CREATE VECTOR INDEX vector_bhive_index_{similarity}_custom IF NOT EXISTS ON {collection}({vector_field} VECTOR) INCLUDE({custom_index_fields}) WITH {vector_definition}'
                elif "vec VECTOR" in custom_index_fields:
                    custom_index_fields = custom_index_fields.replace("vec VECTOR", "")
                    if custom_name:
                        index_query = f'CREATE VECTOR INDEX {custom_name} IF NOT EXISTS ON {collection}({vector_field} VECTOR) WITH {vector_definition}'
                    else:
                        index_query = f'CREATE VECTOR INDEX vector_bhive_index_{similarity}_custom IF NOT EXISTS ON {collection}({vector_field} VECTOR) WITH {vector_definition}'
            else:
                index_query = f'CREATE VECTOR INDEX vector_bhive_index_{similarity} IF NOT EXISTS ON {collection}({vector_field} VECTOR) INCLUDE(size, brand) WITH {vector_definition}'
        if use_partition:
            index_query = index_query.split("WITH")[0] + f" PARTITION BY HASH(meta().id) WITH " + index_query.split("WITH")[1]
        print(index_query)
        result = cb_scope.query(index_query, metrics=True, timeout=timedelta(seconds=360))
        for row in result:
            print(f"Result: {row}")
        print(f"Execution time: {result.metadata().metrics().execution_time()}")
    def create_cover_index(self, cluster, bucket='default', scope='_default', collection='_default', index_order='tail', vector_field='vec', is_xattr=False, is_base64=False, network_byte_order=False, dimension=128, train=10000, description='IVF,PQ32x8', similarity='L2_SQUARED', nprobes=3, use_bhive=False):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order})"
        vector_definition = {"dimension": dimension, "train_list": train, "description": description, "similarity": similarity, "scan_nprobes": nprobes}
        index_queries = {
            'tail': f'CREATE INDEX vector_index_{similarity} IF NOT EXISTS ON {collection}(size, brand, {vector_field} VECTOR, id) WITH {vector_definition}',
            'mid': f'CREATE INDEX vector_index_{similarity} IF NOT EXISTS ON {collection}(size, {vector_field} VECTOR, brand, id) WITH {vector_definition}',
            'lead': f'CREATE INDEX vector_index_{similarity} IF NOT EXISTS ON {collection}({vector_field} VECTOR, size, brand, id) WITH {vector_definition}',
        }
        index_query = index_queries[index_order]
        if use_bhive:
            index_query = f'CREATE VECTOR INDEX vector_bhive_index_{similarity} IF NOT EXISTS ON {collection}({vector_field} VECTOR) WITH {vector_definition}'
        print(index_query)
        result = cb_scope.query(index_query, metrics=True, timeout=timedelta(seconds=300))
        for row in result:
            print(f"Result: {row}")
        print(f"Execution time: {result.metadata().metrics().execution_time()}")
    def drop_index(self, cluster, bucket='default', scope='_default', collection='_default', similarity='L2_SQUARED', use_bhive=False,custom_fields=False,custom_name=None):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        index_query = f'DROP INDEX vector_index_{similarity} IF EXISTS ON {collection}'
        if use_bhive:
            index_query = f'DROP INDEX vector_bhive_index_{similarity} IF EXISTS ON {collection}'
        if custom_fields:
            if custom_name:
                index_query = f'DROP INDEX {custom_name} IF EXISTS ON {collection}'
            else:
                index_query = f'DROP INDEX vector_index_{similarity}_custom IF EXISTS ON {collection}'
        print(index_query)
        result = cb_scope.query(index_query, metrics=True, timeout=timedelta(seconds=300))
        for row in result:
            print(f"Result: {row}")
        print(f"Execution time: {result.metadata().metrics().execution_time()}")

class QueryVector(object):
    def vector_knn_query(self, vector_field='vec', collection='_default', search_function='L2', is_xattr=False, is_base64=False, network_byte_order=False, direction='ASC', k=100):
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order})"
        query = f'SELECT id, VECTOR_DISTANCE({vector_field}, $qvec, "{search_function}") as distance FROM {collection} WHERE size IN $size AND brand IN $brand ORDER BY KNN_DISTANCE({vector_field}, $qvec, "{search_function}") {direction} LIMIT {k}'
        return query
    def vector_ann_query(self, vector_field='vec', collection='_default', search_function='L2', is_xattr=False, is_base64=False, network_byte_order=False, nprobes=3, direction='ASC', k=100):
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order})"
        size_predicate = ["size in $size", "size = $size[0]", "size < $size[0]+1 AND size > $size[0]-1", "size between $size[0] and $size[0]", "size <= $size[0] AND size > $size[0]-1"]
        query = f'SELECT id, ANN_DISTANCE({vector_field}, $qvec, "{search_function}", {nprobes}) as distance FROM {collection} WHERE {size_predicate[random.randint(0,4)]} AND brand IN $brand ORDER BY ANN_DISTANCE({vector_field}, $qvec, "{search_function}", {nprobes}) {direction} LIMIT {k}'
        return query
    def run_queries(self, cluster, xb, qdocs, gdocs, search_function="L2", bucket='default', scope='_default', collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        for idx, x in enumerate(qdocs):
            print("-"*30)
            print(f"Running query#{idx} with vector {x.tolist()[:9]} ...")
            qdoc = {"size":[random.choice(cfg["sizes"])], "brand":[random.choice(cfg["brands"])],"qvec": x.tolist(), "sizeidx":0, "brandidx":0}
            self.n1ql_query(xb, cb_scope, qdoc, gdocs[idx], search_function, collection, vector_field, is_xattr, is_base64, is_bigendian)
    def n1ql_query(self, xb, cb_scope, qdoc, gdoc, search_function="L2", collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False):
        vector_query = self.vector_knn_query(vector_field, collection, search_function, is_xattr, is_base64, network_byte_order=is_bigendian)
        actual = []
        expected = gdoc.tolist()
        query_vector = qdoc["qvec"]
        params = {"size": qdoc["size"], "brand":qdoc["brand"],"qvec":query_vector, "sizeidx": qdoc["sizeidx"], "brandidx":qdoc["brandidx"]}
        result = cb_scope.query(
            vector_query,
            QueryOptions(named_parameters=params), metrics=True, timeout=timedelta(seconds=300))
        for row in result.rows():
            actual.append(row['id'])
        print(f"Execution time: {result.metadata().metrics().execution_time()}")
        recall, accuracy = UtilVector().compare_result(expected, actual)
        print(f"Recall rate: {recall}% with accuracy: {accuracy}%")
    def search(self, cluster, xq, search_function="L2", type = 'KNN', bucket='default', scope='_default', collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False, nprobes=3):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        vectors = []
        distances = []
        for idx, x in enumerate(xq):
            qdoc = {"size":[random.choice(cfg["sizes"])], "brand":[random.choice(cfg["brands"])],"qvec": x.tolist(), "sizeidx":0, "brandidx":0}
            vectors_distance, vectors_id = self.n1ql_search(cb_scope, qdoc, search_function, type, collection, vector_field, is_xattr, is_base64, is_bigendian, nprobes)
            vectors.append(vectors_id)
            distances.append(vectors_distance)
        return np.array(distances, dtype=np.float32), np.array(vectors, dtype=np.int32)
    def n1ql_search(self, cb_scope, qdoc, search_function, type, collection, vector_field, is_xattr, is_base64, is_bigendian, nprobes):
        if type == 'KNN':
            vector_query = self.vector_knn_query(vector_field, collection, search_function, is_xattr, is_base64, network_byte_order=is_bigendian)
        if type == 'ANN':
            vector_query = self.vector_ann_query(vector_field, collection, search_function, is_xattr, is_base64, network_byte_order=is_bigendian, nprobes=nprobes)
        vectors = []
        distances = []
        print(f"Query: {vector_query}")
        query_vector = qdoc["qvec"]
        params = {"size": qdoc["size"], "brand":qdoc["brand"],"qvec":query_vector, "sizeidx": qdoc["sizeidx"], "brandidx":qdoc["brandidx"]}
        result = cb_scope.query(
            vector_query,
            QueryOptions(named_parameters=params), metrics=True, timeout=timedelta(seconds=300))
        for row in result.rows():
            vectors.append(row['id'])
            distances.append(row['distance'])
        return distances, vectors