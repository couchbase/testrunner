import shutil
import urllib.request as request
from contextlib import closing
import tarfile
import numpy as np
from numpy import dot
from numpy.linalg import norm
import os
try:
    from couchbase.options import QueryOptions
except ImportError:
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
    def check_distance(self, query_vector, xb, vector_results, vector_distances, distance="L2", vector_type='dense'):
        """
        Check that vector search result distances match expected calculation.
        For sparse, only use dot product.
        """
        fail_count = 0
        for idx, vector in enumerate(vector_results):
            actual_distance = vector_distances[idx]
            if vector_type == 'sparse':
                # Only dot product supported for sparse
                query = (query_vector.indices.tolist(), query_vector.data.tolist())
                base = (xb[vector].indices.tolist(), xb[vector].data.tolist())
                expected_distance = -self.sparse_dot_product_dist(query, base)
            else:
                if distance == "L2" or distance == "EUCLIDEAN":
                    expected_distance = self.l2_dist(query_vector, xb[vector])
                elif distance == "L2_SQUARED" or distance == "EUCLIDEAN_SQUARED":
                    expected_distance = self.l2_dist_sq(query_vector, xb[vector])
                elif distance == "DOT":
                    expected_distance = - self.dot_product_dist(query_vector, xb[vector])
                elif distance == "COSINE":
                    expected_distance = self.cosine_dist(query_vector, xb[vector])
                else:
                    expected_distance = None
            if expected_distance is not None and not np.isclose(expected_distance, actual_distance):
                fail_count += 1
                print(f"FAIL: expected: {expected_distance} actual: {actual_distance}")
                continue
            print(f"PASS: expected: {expected_distance} actual: {actual_distance}")
        return fail_count

    def sparse_dot_product_dist(self, v1, v2):
        """
        Compute dot product distance between two sparse vectors.
        v1, v2: each is a tuple (indices, values), where indices is a list of indices and values is a list of corresponding values
        """
        # Create index->value mappings for fast lookup
        v1_dict = dict(zip(v1[0], v1[1]))
        v2_dict = dict(zip(v2[0], v2[1]))
        # Compute sum of (value1 * value2) for all common indices
        dot = 0.0
        for idx in set(v1_dict.keys()) & set(v2_dict.keys()):
            dot += v1_dict[idx] * v2_dict[idx]
        return float(dot)

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

class SparseVector(object):
    def __init__(self, version="small"):
        """
        version: one of {"small", "1M", "full"}
        """
        # Supported sparse dataset versions
        versions = {
            "small": (100000, "base_small.csr.gz", "base_small.dev.gt"),
            "1M": (1000000, "base_1M.csr.gz", "base_1M.dev.gt"),
            "full": (8841823, "base_full.csr.gz", "base_full.dev.gt"),
        }
        assert version in versions, f'version="{version}" is invalid. Please choose one of {list(versions.keys())}.'

        self.version = version
        self.nb = versions[version][0]
        self.ds_fn = versions[version][1]
        self.gt_fn = versions[version][2]
        self.nq = 6980
        self.dataset_location = os.path.abspath("data/sparse")
        self.base_url = "https://storage.googleapis.com/ann-challenge-sparse-vectors/csr/"
        self.qs_fn = "queries.dev.csr.gz"

        # Make sure directory exists
        if not os.path.exists(self.dataset_location):
            os.makedirs(self.dataset_location)

    def download_dataset(self):
        """
        Download and unzip the sparse dataset and queries file, if not present.
        """
        import gzip

        file_list = [self.ds_fn, self.qs_fn, self.gt_fn]
        for fn in file_list:
            # Download gzipped file
            url = os.path.join(self.base_url, fn)
            gz_path = os.path.join(self.dataset_location, fn)
            unzipped_path = gz_path[:-3] if gz_path.endswith(".gz") else gz_path

            if os.path.exists(unzipped_path):
                print(f"Unzipped file already exists: {unzipped_path}")
                continue
            if os.path.exists(gz_path):
                print(f"Gzipped file already exists: {gz_path}")
            else:
                print(f"Downloading {url} -> {gz_path}")
                request.urlretrieve(url, gz_path, quiet=True)
            # Unzip if needed
            if gz_path.endswith(".gz"):
                print(f"Unzipping {gz_path} ...")
                with gzip.open(gz_path, 'rb') as f_in, open(unzipped_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                os.remove(gz_path)
                print(f"Done unzipping: {unzipped_path}")

    def read_vector(self):
        """
        Read the base dataset vectors as csr_matrix.
        """
        from scipy.sparse import csr_matrix

        fname = os.path.join(self.dataset_location, self.ds_fn.replace(".gz", ""))
        if not os.path.exists(fname):
            raise FileNotFoundError(f"Dataset file not found: {fname}")
        return self._read_sparse_matrix(fname)

    def read_query(self):
        """
        Read the queries as csr_matrix.
        """
        fname = os.path.join(self.dataset_location, self.qs_fn.replace(".gz", ""))
        if not os.path.exists(fname):
            raise FileNotFoundError(f"Query file not found: {fname}")
        return self._read_sparse_matrix(fname)

    def read_groundtruth(self):
        """
        Read the groundtruth file and return (I, D) as in knn_result_read.
        """
        fname = os.path.join(self.dataset_location, self.gt_fn)
        if not os.path.exists(fname):
            raise FileNotFoundError(f"Groundtruth file not found: {fname}")
        return self._knn_result_read(fname)

    def _read_sparse_matrix(self, fname):
        """
        Internal: Read a CSR matrix in 'spmat'/sparse format.
        """
        from scipy.sparse import csr_matrix
        with open(fname, "rb") as f:
            sizes = np.fromfile(f, dtype='int64', count=3)
            nrow, ncol, nnz = sizes
            indptr = np.fromfile(f, dtype='int64', count=nrow + 1)
            indices = np.fromfile(f, dtype='int32', count=nnz)
            data = np.fromfile(f, dtype='float32', count=nnz)
        return csr_matrix((data, indices, indptr), shape=(nrow, ncol))

    def _knn_result_read(self, fname):
        """
        Internal: Read the groundtruth as per scripts/sparse.py/knn_result_read.
        Returns (I, D) where I are the indices and D the distances.
        """
        n, d = np.fromfile(fname, dtype="uint32", count=2)
        expected_size = 8 + n * d * (4 + 4)
        file_size = os.stat(fname).st_size
        assert file_size == expected_size, f"File size mismatch: expected {expected_size}, got {file_size}"
        with open(fname, "rb") as f:
            f.seek(8)
            I = np.fromfile(f, dtype="int32", count=n * d).reshape(n, d)
            D = np.fromfile(f, dtype="float32", count=n * d).reshape(n, d)
        return I, D

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
    def encode_vector(self, vector, is_bigendian=False, vector_type='dense'):
        """
        Encodes the vector with endianess, supports both dense (list/ndarray)
        and sparse (tuple/list: (indices, values)) input.
        Returns base64 encoded string for dense or sparse.
        For sparse, encode as: [int32 count][int32 indices...][float32 values...]
        """
        if is_bigendian:
            endian = '>'
        else:
            endian = '<'
        if vector_type == 'dense':
            buf = struct.pack(f'{endian}%sf' % len(vector), *vector)
            return base64.b64encode(buf).decode()
        elif vector_type == 'sparse':
            indices, values = vector
            count = len(indices)
            # First pack count as int32, then indices as int32[count], then values as float32[count]
            fmt = f"{endian}i{count}i{count}f"
            buf = struct.pack(fmt, count, *indices, *values)
            return base64.b64encode(buf).decode()
        else:
            raise ValueError(f"Unknown vector_type: {vector_type}")

    def load_batch_documents(
        self,
        cluster,
        docs,
        batch,
        is_xattr=False,
        is_base64=False,
        is_bigendian=False,
        vector_type='dense',
        bucket='default',
        scope='_default',
        collection='_default',
        vector_field='vec'
    ):
        """
        Loads batch docs into Couchbase, supporting both dense (default) and sparse.
        - If vector_type='dense', `docs` should be a sequence of ndarrays.
        - If vector_type='sparse', `docs` should be a sequence of 2-tuples/lists: (indices, values)
        """
        cb = cluster.bucket(bucket)
        cb_coll = cb.scope(scope).collection(collection)
        documents = {}
        for is1, size in enumerate(cfg["sizes"]):
            for ib, brand in enumerate(cfg["brands"]):
                documents = {}
                for idx, x in enumerate(docs):
                    if vector_type == 'sparse':
                        # Expect x to be (indices, values) or a scipy.sparse row-like object
                        if hasattr(x, "indices") and hasattr(x, "data"):
                            indices = x.indices.tolist()
                            values = x.data.tolist()
                        elif isinstance(x, (tuple, list)) and len(x) == 2:
                            indices, values = x
                        else:
                            raise ValueError("When vector_type='sparse', each doc must be (indices, values)")
                        if is_base64:
                            vector = self.encode_vector([indices, values], is_bigendian, 'sparse')
                        else:
                            vector = [indices, values]
                    else:
                        vector = x.tolist()
                        if is_base64:
                            vector = self.encode_vector(vector, is_bigendian)
                    key = f"vec_{brand}_{size}_{idx+batch}"
                    doc = {
                        "id": idx + batch,
                        "size": size,
                        "sizeidx": is1,
                        "brand": brand,
                        "brandidx": ib,
                        vector_field: vector
                    }
                    # if is_xattr: remove vector_field from doc and store as xattr below
                    documents[key] = doc
                try:
                    cb_coll.upsert_multi(documents)
                except Exception as e:
                    print(e)
                if is_xattr:
                    for key in documents:
                        cb_coll.mutate_in(
                            key,
                            [SD.upsert(vector_field, documents[key][vector_field], xattr=True),
                             SD.remove(vector_field)]
                        )

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
                    index_query = f'DROP INDEX vector_bhive_index_{similarity}_custom IF EXISTS ON {collection}'
        elif custom_fields:
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
    def vector_knn_query(self, vector_field='vec', collection='_default', search_function='L2', is_xattr=False, is_base64=False, network_byte_order=False, vector_type='dense', direction='ASC', k=100):
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            if vector_type == 'dense':
                vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order}, 'dense')"
            elif vector_type == 'sparse':
                vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order}, 'sparse')"
        if vector_type == 'sparse':
            query = (
                f"SELECT id, SPARSE_VECTOR_DISTANCE({vector_field}, $qvec) as distance "
                f"FROM {collection} "
                f"WHERE size IN $size AND brand IN $brand "
                f"ORDER BY SPARSE_VECTOR_DISTANCE({vector_field}, $qvec) {direction} LIMIT {k}"
            )
        elif vector_type == 'dense':
            query = (
                f"SELECT id, VECTOR_DISTANCE({vector_field}, $qvec, \"{search_function}\") as distance "
                f"FROM {collection} "
                f"WHERE size IN $size AND brand IN $brand "
                f"ORDER BY KNN_DISTANCE({vector_field}, $qvec, \"{search_function}\") {direction} LIMIT {k}"
            )
        return query

    def vector_ann_query(self, vector_field='vec', collection='_default', search_function='L2', is_xattr=False, is_base64=False, network_byte_order=False, nprobes=3, direction='ASC', k=100, vector_type='dense'):
        if is_xattr:
            vector_field = f"meta().xattrs.{vector_field}"
        if is_base64:
            if vector_type == 'dense':
                vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order}, 'dense')"
            elif vector_type == 'sparse':
                vector_field = f"DECODE_VECTOR({vector_field}, {network_byte_order}, 'sparse')"
        size_predicate = [
            "size in $size",
            "size = $size[0]",
            "size < $size[0]+1 AND size > $size[0]-1",
            "size between $size[0] and $size[0]",
            "size <= $size[0] AND size > $size[0]-1"
        ]
        if vector_type == 'sparse':
            query = (
                f"SELECT id, SPARSE_ANN_DISTANCE({vector_field}, $qvec, {nprobes}) as distance "
                f"FROM {collection} "
                f"WHERE {size_predicate[random.randint(0,4)]} AND brand IN $brand "
                f"ORDER BY SPARSE_ANN_DISTANCE({vector_field}, $qvec, {nprobes}) {direction} LIMIT {k}"
            )
        else:
            query = (
                f"SELECT id, ANN_DISTANCE({vector_field}, $qvec, \"{search_function}\", {nprobes}) as distance "
                f"FROM {collection} "
                f"WHERE {size_predicate[random.randint(0,4)]} AND brand IN $brand "
                f"ORDER BY ANN_DISTANCE({vector_field}, $qvec, \"{search_function}\", {nprobes}) {direction} LIMIT {k}"
            )
        return query

    def run_queries(self, cluster, xb, qdocs, gdocs, search_function="L2", bucket='default', scope='_default', collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False, vector_type='dense'):
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        for idx, x in enumerate(qdocs):
            print("-"*30)
            print(f"Running query#{idx} with vector {x.tolist()[:9]} ...")
            qdoc = {
                "size": [random.choice(cfg["sizes"])],
                "brand": [random.choice(cfg["brands"])],
                "qvec": x.tolist(),
                "sizeidx": 0,
                "brandidx": 0
            }
            self.n1ql_query(
                xb, cb_scope, qdoc, gdocs[idx], search_function, collection, vector_field,
                is_xattr, is_base64, is_bigendian, vector_type=vector_type
            )

    def n1ql_query(self, xb, cb_scope, qdoc, gdoc, search_function="L2", collection='_default', vector_field='vec', is_xattr=False, is_base64=False, is_bigendian=False, vector_type='dense'):
        vector_query = self.vector_knn_query(
            vector_field=vector_field,
            collection=collection,
            search_function=search_function,
            is_xattr=is_xattr,
            is_base64=is_base64,
            network_byte_order=is_bigendian,
            vector_type=vector_type
        )
        actual = []
        expected = gdoc.tolist()
        query_vector = qdoc["qvec"]
        params = {
            "size": qdoc["size"],
            "brand": qdoc["brand"],
            "qvec": query_vector,
            "sizeidx": qdoc["sizeidx"],
            "brandidx": qdoc["brandidx"]
        }
        result = cb_scope.query(
            vector_query,
            QueryOptions(named_parameters=params), metrics=True, timeout=timedelta(seconds=300))
        for row in result.rows():
            actual.append(row['id'])
        print(f"Execution time: {result.metadata().metrics().execution_time()}")
        recall, accuracy = UtilVector().compare_result(expected, actual)
        print(f"Recall rate: {recall}% with accuracy: {accuracy}%")

    def search(self, cluster, xq, search_function="L2", type='KNN', bucket='default', scope='_default', collection='_default', vector_field='vec',
               is_xattr=False, is_base64=False, is_bigendian=False, nprobes=3, vector_type='dense'):
        """
        Handles both dense and sparse vector search.
        Pass vector_type='sparse' for sparse search, 'dense' for dense search.
        """
        cb = cluster.bucket(bucket)
        cb_scope = cb.scope(scope)
        vectors = []
        distances = []
        for idx, x in enumerate(xq):
            qdoc = {
                "size": [random.choice(cfg["sizes"])],
                "brand": [random.choice(cfg["brands"])],
                "qvec": x.tolist() if hasattr(x, "tolist") else (x.indices.tolist(), x.data.tolist()) if hasattr(x, "indices") and hasattr(x, "data") else x,
                "sizeidx": 0,
                "brandidx": 0
            }
            vectors_distance, vectors_id = self.n1ql_search(
                cb_scope, qdoc, search_function, type, collection,
                vector_field, is_xattr, is_base64, is_bigendian, nprobes, vector_type=vector_type
            )
            vectors.append(vectors_id)
            distances.append(vectors_distance)
        return np.array(distances, dtype=np.float32), np.array(vectors, dtype=np.int32)

    def n1ql_search(self, cb_scope, qdoc, search_function, type, collection, vector_field, is_xattr, is_base64, is_bigendian, nprobes, vector_type='dense'):
        if type == 'KNN':
            vector_query = self.vector_knn_query(
                vector_field, collection, search_function, is_xattr, is_base64,
                network_byte_order=is_bigendian, vector_type=vector_type
            )
        elif type == 'ANN':
            vector_query = self.vector_ann_query(
                vector_field, collection, search_function, is_xattr, is_base64,
                network_byte_order=is_bigendian, nprobes=nprobes, vector_type=vector_type
            )
        else:
            raise ValueError(f"Unknown search type: {type}")
        vectors = []
        distances = []
        print(f"Query: {vector_query}")
        query_vector = qdoc["qvec"]
        params = {
            "size": qdoc["size"],
            "brand": qdoc["brand"],
            "qvec": query_vector,
            "sizeidx": qdoc["sizeidx"],
            "brandidx": qdoc["brandidx"]
        }
        result = cb_scope.query(
            vector_query,
            QueryOptions(named_parameters=params), metrics=True, timeout=timedelta(seconds=300))
        for row in result.rows():
            vectors.append(row['id'])
            distances.append(row['distance'])
        return distances, vectors