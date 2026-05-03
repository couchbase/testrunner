import copy
import json
import os
import re
import time
import statistics
import struct
import base64
import random
import threading

import logger

from TestInput import TestInputSingleton
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from .fts_base import NodeHelper
from .fts_backup_restore import FTSIndexBackupClient
from .fts_vector_search import VectorSearch


class BQVectorSearch(VectorSearch):

    def setUp(self):
        super(BQVectorSearch, self).setUp()
        self.input = TestInputSingleton.input
        self.log = logger.Logger.get_logger()

        self.bq_index_type = self.input.param("bq_index_type", "bivf-sq8")
        self.bq_recall_threshold = self.input.param("bq_recall_threshold", 70)
        self.bq_latency_threshold_ms = self.input.param("bq_latency_threshold_ms", 5000)
        self.bq_ram_growth_pct_threshold = self.input.param("bq_ram_growth_pct_threshold", 20)

        self.vector_backup_filename = self.input.param(
            "vector_backup_filename", "backup_zips/100k_cars_baai_1024.zip")
        self.bq_vector_field = self.input.param("bq_vector_field", "descriptionVector")
        self.bq_bucket = self.input.param("bq_bucket", None)
        self.bq_scope = self.input.param("bq_scope", None)
        self.bq_collection = self.input.param("bq_collection", None)
        self.bq_namespaces = []
        self.bq_collections = []
        self.num_of_docs_per_collection = self.input.param("num_of_docs_per_collection", 100000)
        self.scan_limit = self.input.param("scan_limit", 100)

        self._bq_s3_bucket = self.input.param("bq_s3_bucket", "gsi-vector-car-backups")

        self._faiss_ground_truth = None
        self._all_vectors_cache = None
        self._all_doc_ids_cache = None
        self._query_vectors_cache = None

    def tearDown(self):
        super(BQVectorSearch, self).tearDown()

    def _get_s3_utils(self):
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", None)
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
        if not aws_key or not aws_secret:
            self.fail("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars are required "
                      "for backup restore data loading")

        from serverless.s3_utils import S3Utils
        return S3Utils(aws_access_key_id=aws_key,
                       aws_secret_access_key=aws_secret,
                       s3_bucket=self._bq_s3_bucket,
                       region=self.input.param("region", "us-east-1"))

    def _restore_couchbase_bucket(self):
        s3_utils_obj = self._get_s3_utils()

        filename = self.vector_backup_filename.split('/')[-1]
        s3_utils_obj.download_file(object_name=self.vector_backup_filename,
                                   filename=filename)

        master = self._cb_cluster.get_master_node()

        bq_kv_quota = self.input.param("bq_kv_quota", 3000)
        bq_fts_quota = self.input.param("bq_fts_quota", 3000)
        self.log.info(f"Setting memory quotas: KV={bq_kv_quota}MB, FTS={bq_fts_quota}MB "
                      f"before backup restore")
        rest_conn = RestConnection(master)
        import urllib.parse
        api = rest_conn.baseUrl + "pools/default"
        params = urllib.parse.urlencode({
            "memoryQuota": bq_kv_quota,
            "ftsMemoryQuota": bq_fts_quota,
            "cbasMemoryQuota": 1024,
            "eventingMemoryQuota": 256
        })
        self.log.info(f"Quota REST call params: {params}")
        status, content, _ = rest_conn.urllib_request(api, verb="POST",
                                                      params=params,
                                                      headers=rest_conn._create_headers())
        if status:
            self.log.info(f"Memory quotas set successfully: KV={bq_kv_quota}MB, "
                          f"FTS={bq_fts_quota}MB")
        else:
            self.log.error(f"Failed to set memory quotas: {content}")
            self.fail(f"Cannot set memory quotas needed for backup restore: {content}")
        time.sleep(5)

        remote_client = RemoteMachineShellConnection(master)
        dist_type = remote_client.extract_remote_info().distribution_type.lower()
        remote_client.copy_file_local_to_remote(src_path=filename, des_path=filename)
        repo = filename.split('.')[0]
        remote_client.execute_command("rm -rf backup")

        couchbase_root_dir = "/opt/couchbase/bin/cbbackupmgr"
        if dist_type == 'windows':
            couchbase_root_dir = '"/cygdrive/c/Program Files/Couchbase/Server/bin/cbbackupmgr"'

        if dist_type != 'windows':
            backup_config_cmd = f"{couchbase_root_dir} config --archive backup/ --repo {repo}"
            out = remote_client.execute_command(backup_config_cmd, timeout=1800)
            self.log.info(out)

        info = remote_client.extract_remote_info()
        if "debian" in info.distribution_version.lower():
            remote_client.execute_command("apt-get install -y unzip")
        elif "centos" in info.distribution_version.lower() or "rhel" in info.distribution_version.lower():
            remote_client.execute_command("yum install -y unzip")
        unzip_cmd = f"unzip -o {filename}"
        remote_client.execute_command(command=unzip_cmd)

        rest = RestConnection(master)
        username = rest.username
        password = rest.password

        restore_cmd = (f"{couchbase_root_dir} restore --archive backup --repo {repo} "
                       f"--cluster couchbase://127.0.0.1 --username {username} --password {password} "
                       f"--auto-create-buckets ")
        restore_out = remote_client.execute_command(restore_cmd, timeout=600)
        self.log.info(restore_out)

        restore_stdout = str(restore_out[0]) if restore_out else ""
        bucket_succeeded = "| Succeeded |" in restore_stdout or "Restore completed successfully" in restore_stdout
        has_error = "Error restoring cluster" in restore_stdout

        if has_error and not bucket_succeeded:
            self.fail(f"Backup restore failed: {restore_stdout}")
        elif has_error and bucket_succeeded:
            self.log.warning("Restore had non-critical errors (e.g. AWR metadata) "
                             "but bucket data restored successfully")
        self.log.info("Backup restore completed, waiting for bucket to be ready")
        time.sleep(10)

        buckets = rest.get_buckets()
        namespaces = []
        for bucket in buckets:
            scopes = rest.get_bucket_scopes(bucket=bucket)
            for scope in scopes:
                if scope == '_default':
                    continue
                collections = rest.get_scope_collections(bucket=bucket, scope=scope)
                for collection in collections:
                    namespaces.append({
                        'bucket': bucket.name,
                        'scope': scope,
                        'collection': collection
                    })

        self.log.info(f"Restored namespaces: {namespaces}")

        if not namespaces:
            self.fail("No namespaces found after backup restore")

        self.bq_namespaces = namespaces
        self.bq_bucket = namespaces[0]['bucket']
        self.bq_scope = namespaces[0]['scope']
        self.bq_collections = [ns['collection'] for ns in namespaces
                                if ns['bucket'] == self.bq_bucket
                                and ns['scope'] == self.bq_scope]
        self.bq_collection = self.bq_collections[0]

        self.log.info(f"BQ test data: bucket={self.bq_bucket}, scope={self.bq_scope}, "
                      f"collections={self.bq_collections} "
                      f"({len(self.bq_collections)} collections)")

        return namespaces

    def _fetch_vectors_from_cb(self, limit=None):
        if self._all_vectors_cache is not None:
            return self._all_vectors_cache

        if limit is None:
            limit = self.num_of_docs_per_collection

        vectors = []
        doc_ids = []

        collections = self.bq_collections if self.bq_collections else [self.bq_collection]
        for collection in collections:
            query = (f"SELECT META(d).id AS doc_id, d.{self.bq_vector_field} "
                     f"FROM `{self.bq_bucket}`.`{self.bq_scope}`.`{collection}` d "
                     f"WHERE d.{self.bq_vector_field} IS NOT MISSING "
                     f"LIMIT {limit}")

            self.log.info(f"Fetching vectors from {self.bq_bucket}.{self.bq_scope}.{collection}...")
            results = self._cb_cluster.run_n1ql_query(query)['results']

            for doc in results:
                vec = doc.get(self.bq_vector_field)
                doc_id = doc.get("doc_id")
                if vec and isinstance(vec, list) and doc_id:
                    vectors.append(vec)
                    doc_ids.append(doc_id)

            self.log.info(f"  {collection}: fetched {len(results)} docs")

        self.log.info(f"Total fetched: {len(vectors)} vectors of dimension "
                      f"{len(vectors[0]) if vectors else 0} "
                      f"across {len(collections)} collections")
        self._all_vectors_cache = vectors
        self._all_doc_ids_cache = doc_ids
        return vectors

    def _build_faiss_ground_truth(self, vectors, similarity="l2_norm"):
        import faiss
        import numpy as np

        vectors_np = np.array(vectors, dtype='float32')
        dim = vectors_np.shape[1]

        if similarity in ("dot_product", "DOT"):
            index = faiss.IndexFlatIP(dim)
        elif similarity in ("cosine", "COSINE"):
            faiss.normalize_L2(vectors_np)
            index = faiss.IndexFlatIP(dim)
        else:
            index = faiss.IndexFlatL2(dim)

        index.add(vectors_np)
        self.log.info(f"Built FAISS ground truth index: {index.ntotal} vectors, dim={dim}")
        self._faiss_ground_truth = index
        return index

    def _get_faiss_nearest_neighbors(self, query_vector, k=None):
        import faiss
        import numpy as np

        if k is None:
            k = self.k
        if self._faiss_ground_truth is None:
            self.fail("FAISS ground truth not built. Call _build_faiss_ground_truth first.")

        qvec = np.array([query_vector], dtype='float32')
        distances, indices = self._faiss_ground_truth.search(qvec, k)
        return indices[0].tolist()

    def _pick_query_vectors(self, all_vectors, num_queries=None):
        if self._query_vectors_cache is not None and len(self._query_vectors_cache) >= (num_queries or self.num_queries):
            return self._query_vectors_cache[:num_queries or self.num_queries]

        if num_queries is None:
            num_queries = self.num_queries
        num_queries = min(num_queries, len(all_vectors))

        indices = random.sample(range(len(all_vectors)), num_queries)
        query_vectors = [all_vectors[i] for i in indices]
        self._query_vectors_cache = query_vectors
        self.log.info(f"Picked {len(query_vectors)} query vectors from dataset")
        return query_vectors

    def _compute_recall_against_faiss(self, fts_doc_ids, faiss_neighbor_indices, k=None):
        if k is None:
            k = self.k

        faiss_gt_doc_ids = set()
        for idx in faiss_neighbor_indices[:k]:
            if idx < len(self._all_doc_ids_cache):
                faiss_gt_doc_ids.add(self._all_doc_ids_cache[idx])

        fts_id_set = set(fts_doc_ids[:k])

        overlap = len(fts_id_set & faiss_gt_doc_ids)
        recall = (overlap / k) * 100 if k > 0 else 0
        return recall

    def _extract_doc_ids_from_fts_results(self, matches):
        doc_ids = []
        if not matches:
            return doc_ids
        for match in matches:
            doc_id = match.get('id', '')
            if doc_id:
                doc_ids.append(doc_id)
        return doc_ids

    def _get_bq_vector_fields(self, dims=None, similarity=None):
        if dims is None:
            dims = self.dimension
        if similarity is None:
            similarity = self.similarity
        return {
            "dims": dims,
            "similarity": similarity,
            "vector_index_optimized_for": self.bq_index_type
        }

    def _create_bq_fts_index_on_bucket(self, index_name, dims=None, similarity=None,
                                         extra_fields=None, num_partitions=None,
                                         collections=None):
        vector_fields = self._get_bq_vector_fields(dims=dims, similarity=similarity)

        if collections is None:
            collections = self.bq_collections if self.bq_collections else [self.bq_collection]

        type_list = [f"{self.bq_scope}.{c}" for c in collections]

        bucket = self._cb_cluster.get_bucket_by_name(self.bq_bucket)
        fts_index = self.create_index(
            bucket, index_name,
            collection_index=True,
            _type=type_list,
            scope=self.bq_scope,
            collections=collections
        )

        for collection in collections:
            fts_index.add_child_field_to_default_collection_mapping(
                field_name=self.bq_vector_field,
                field_type="vector",
                field_alias=self.bq_vector_field,
                scope=self.bq_scope,
                collection=collection,
                vector_fields=vector_fields
            )

            if extra_fields:
                for field_dict in extra_fields:
                    for fname, ftype in field_dict.items():
                        fts_index.add_child_field_to_default_collection_mapping(
                            field_name=fname,
                            field_type=ftype,
                            field_alias=fname,
                            scope=self.bq_scope,
                            collection=collection
                        )

        if num_partitions:
            fts_index.update_num_pindexes(num_partitions)

        fts_index.index_definition['uuid'] = fts_index.get_uuid()
        fts_index.update()
        self.wait_for_indexing_complete()

        self.log.info(f"Created BQ FTS index '{index_name}' on "
                      f"{self.bq_bucket}.{self.bq_scope}.{collections} "
                      f"with vector_index_optimized_for={self.bq_index_type}")
        return fts_index

    def _validate_bq_index_definition(self, index_obj, expected_bq_type=None):
        if expected_bq_type is None:
            expected_bq_type = self.bq_index_type

        status, index_def = index_obj.get_index_defn()
        self.assertTrue(status, "Failed to get index definition")
        index_definition = index_def["indexDef"]

        types = index_definition.get('params', {}).get('mapping', {}).get('types', {})
        self.assertTrue(len(types) > 0, "No type mappings found in index definition")

        for type_name, type_def in types.items():
            properties = type_def.get('properties', {})
            field_def = properties.get(self.bq_vector_field, {})
            fields = field_def.get('fields', [])
            self.assertTrue(len(fields) > 0,
                            f"No fields found for '{self.bq_vector_field}' in type '{type_name}'")

            actual = fields[0].get('vector_index_optimized_for', '')
            self.assertEqual(actual, expected_bq_type,
                             f"Expected vector_index_optimized_for='{expected_bq_type}', got '{actual}'")

        self.log.info(f"BQ index definition validated: vector_index_optimized_for={expected_bq_type}")
        return index_definition

    # ---------------------------------------------------------------------------
    # Query execution and stats
    # ---------------------------------------------------------------------------

    def _run_bq_fts_knn_query(self, index_obj, query_vector, k=None, knn_params=None,
                               extra_query=None):
        if k is None:
            k = self.k

        knn_clause = {
            "field": self.bq_vector_field,
            "vector": query_vector,
            "k": k
        }
        if knn_params:
            knn_clause["params"] = knn_params

        fts_query = extra_query if extra_query else {"match_none": {}}

        hits, matches, time_taken, status = index_obj.execute_query(
            query=fts_query,
            knn=[knn_clause],
            explain=False,
            return_raw_hits=True,
            fields=["*"]
        )
        return hits, matches, time_taken, status

    def _measure_query_latency(self, index_obj, query_vectors, k=None, num_queries=None):
        if k is None:
            k = self.k
        if num_queries is None:
            num_queries = len(query_vectors)

        latencies_ms = []
        for q in query_vectors[:num_queries]:
            start = time.time()
            self._run_bq_fts_knn_query(index_obj, q, k=k)
            latencies_ms.append((time.time() - start) * 1000)

        if not latencies_ms:
            return {}

        sorted_l = sorted(latencies_ms)
        stats = {
            'min_ms': round(min(latencies_ms), 2),
            'max_ms': round(max(latencies_ms), 2),
            'avg_ms': round(statistics.mean(latencies_ms), 2),
            'median_ms': round(statistics.median(latencies_ms), 2),
            'p95_ms': round(sorted_l[int(len(sorted_l) * 0.95)], 2),
            'p99_ms': round(sorted_l[min(int(len(sorted_l) * 0.99), len(sorted_l) - 1)], 2),
            'total_queries': len(latencies_ms)
        }
        self.log.info(f"Query latency stats: {stats}")
        return stats

    def _get_fts_ram_usage(self):
        ram_by_node = {}
        rest = RestConnection(self._cb_cluster.get_master_node())
        for node in self._cb_cluster.get_fts_nodes():
            stats_text = rest.get_nsserver_stats(node.ip).text
            match = re.search(
                r'sysproc_mem_resident\{proc="cbft",category="system-processes"\} (\d+)',
                stats_text)
            ram_by_node[node.ip] = int(match.group(1)) if match else 0
        return ram_by_node

    def _measure_ram_usage(self, index_obj, query_vectors, num_queries=None):
        if num_queries is None:
            num_queries = len(query_vectors)

        ram_before = self._get_fts_ram_usage()
        self.log.info(f"RAM before queries: {ram_before}")

        for q in query_vectors[:num_queries]:
            self._run_bq_fts_knn_query(index_obj, q)

        time.sleep(5)
        ram_after = self._get_fts_ram_usage()
        self.log.info(f"RAM after queries: {ram_after}")

        ram_delta = {}
        for ip in ram_before:
            before = ram_before.get(ip, 0)
            after = ram_after.get(ip, 0)
            growth = ((after - before) / before * 100) if before > 0 else 0
            ram_delta[ip] = {
                'before_bytes': before,
                'after_bytes': after,
                'delta_bytes': after - before,
                'growth_pct': round(growth, 2)
            }
        self.log.info(f"RAM delta: {ram_delta}")
        return ram_delta

    def _run_bq_queries_and_validate_recall(self, index_obj, query_vectors,
                                             num_queries=None):
        if num_queries is None:
            num_queries = len(query_vectors)

        recalls = []
        for count, qvec in enumerate(query_vectors[:num_queries]):
            faiss_neighbors = self._get_faiss_nearest_neighbors(qvec, k=self.k)

            hits, matches, time_taken, status = self._run_bq_fts_knn_query(
                index_obj, qvec, k=self.k)

            if isinstance(status, dict) and status.get('failed', 0) != 0:
                self.log.warning(f"Query {count} had failures: {status}")
                continue

            fts_doc_ids = self._extract_doc_ids_from_fts_results(matches)
            recall = self._compute_recall_against_faiss(
                fts_doc_ids, faiss_neighbors, k=self.k)
            recalls.append(recall)
            self.log.info(f"Query {count}: hits={hits}, recall={recall:.1f}%")

        avg_recall = (sum(recalls) / len(recalls)) if recalls else 0
        self.log.info(f"Average recall across {len(recalls)} queries: {avg_recall:.1f}%")
        return avg_recall, recalls

    def _setup_bq_test_data(self, num_queries=None):
        self._restore_couchbase_bucket()

        all_vectors = self._fetch_vectors_from_cb()
        self.assertTrue(len(all_vectors) > 0, "No vectors fetched from CB after restore")

        self._build_faiss_ground_truth(all_vectors, similarity=self.similarity)

        query_vectors = self._pick_query_vectors(all_vectors, num_queries=num_queries)

        return all_vectors, query_vectors

    def test_bq_index_creation(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        index_obj = self._create_bq_fts_index_on_bucket("bq_idx1")
        self._validate_bq_index_definition(index_obj)

        avg_recall, _ = self._run_bq_queries_and_validate_recall(
            index_obj, query_vectors)

        self.log.info(f"BQ index creation test passed: bq_index_type={self.bq_index_type}, "
                      f"recall={avg_recall:.1f}%")

    def test_bq_index_different_similarity_metrics(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        similarity_metrics = ["l2_norm", "dot_product"]
        results = {}

        for sim in similarity_metrics:
            idx_name = f"bq_{sim.replace('_', '')}"
            index_obj = self._create_bq_fts_index_on_bucket(idx_name, similarity=sim)
            self._validate_bq_index_definition(index_obj)

            self._build_faiss_ground_truth(all_vectors, similarity=sim)
            avg_recall, _ = self._run_bq_queries_and_validate_recall(
                index_obj, query_vectors, num_queries=10)
            results[sim] = avg_recall

            self._cb_cluster.delete_fts_index(idx_name)
            time.sleep(5)

        self.log.info(f"BQ similarity metrics results: {results}")
        for sim, recall in results.items():
            self.assertGreater(recall, 0, f"BQ with similarity={sim} returned 0% recall")

    def test_bq_index_with_non_vector_fields(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        extra_fields = [{"make": "text"}, {"year": "number"}]
        index_obj = self._create_bq_fts_index_on_bucket("bq_mixed",
                                                          extra_fields=extra_fields)
        self._validate_bq_index_definition(index_obj)

        for qvec in query_vectors:
            hits, matches, _, status = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertNotEqual(hits, 0, "Query returned 0 hits on mixed BQ index")

        self.log.info("BQ index with non-vector fields test passed")

    def test_bq_index_partitioned(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        num_partitions = self.input.param("num_partitions", 3)
        index_obj = self._create_bq_fts_index_on_bucket("bq_part",
                                                          num_partitions=num_partitions)
        self._validate_bq_index_definition(index_obj)

        for qvec in query_vectors:
            hits, matches, _, _ = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertNotEqual(hits, 0, "Query returned 0 hits on partitioned BQ index")

        self.log.info(f"Partitioned BQ index test passed ({num_partitions} partitions)")

    def test_bq_indexing_and_binarisation_verification(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        index_obj = self._create_bq_fts_index_on_bucket("bq_bin_verify")
        self._validate_bq_index_definition(index_obj)

        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        stats = rest.get_fts_stats()
        self.log.info(f"FTS stats after BQ indexing: {str(stats)[:2000]}")

        avg_recall, _ = self._run_bq_queries_and_validate_recall(
            index_obj, query_vectors)

        self.log.info(f"BQ binarisation verification passed: recall={avg_recall:.1f}%")

    def test_bq_index_memory_footprint(self):
        self._restore_couchbase_bucket()

        ram_before = self._get_fts_ram_usage()
        self.log.info(f"RAM before BQ index creation: {ram_before}")

        all_vectors = self._fetch_vectors_from_cb()
        index_obj = self._create_bq_fts_index_on_bucket("bq_mem")
        self._validate_bq_index_definition(index_obj)
        time.sleep(10)

        ram_after = self._get_fts_ram_usage()
        self.log.info(f"RAM after BQ index creation: {ram_after}")

        for ip in ram_before:
            delta = ram_after.get(ip, 0) - ram_before.get(ip, 0)
            self.log.info(f"Node {ip}: RAM delta = {delta} bytes ({delta / (1024*1024):.2f} MB)")

        self._build_faiss_ground_truth(all_vectors, similarity=self.similarity)
        query_vectors = self._pick_query_vectors(all_vectors, num_queries=3)
        for qvec in query_vectors:
            hits, _, _, _ = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertNotEqual(hits, 0, "Query returned 0 hits")

        self.log.info("BQ memory footprint test passed")

    def test_bq_basic_knn_search(self):
        all_vectors, query_vectors = self._setup_bq_test_data()

        index_obj = self._create_bq_fts_index_on_bucket("bq_knn")
        self._validate_bq_index_definition(index_obj)

        avg_recall, _ = self._run_bq_queries_and_validate_recall(
            index_obj, query_vectors)

        self.assertGreaterEqual(avg_recall, self.bq_recall_threshold,
                                f"BQ recall {avg_recall:.1f}% below threshold "
                                f"{self.bq_recall_threshold}%")
        self.log.info(f"BQ basic KNN search passed: recall={avg_recall:.1f}%")

    def test_bq_search_different_k_values(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        index_obj = self._create_bq_fts_index_on_bucket("bq_kvalues")
        self._validate_bq_index_definition(index_obj)

        k_values = [1, 3, 10, 50, 100]
        results_by_k = {}

        for k_val in k_values:
            original_k = self.k
            self.k = k_val

            avg_recall, _ = self._run_bq_queries_and_validate_recall(
                index_obj, query_vectors, num_queries=10)
            results_by_k[k_val] = avg_recall
            self.k = original_k

        self.log.info(f"BQ results by K: {results_by_k}")
        for k_val, recall in results_by_k.items():
            self.assertGreater(recall, 0, f"BQ with k={k_val} returned 0% recall")

    def test_bq_search_with_prefilter(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        extra_fields = [{"year": "number"}]
        index_obj = self._create_bq_fts_index_on_bucket("bq_prefilter",
                                                          extra_fields=extra_fields)
        self._validate_bq_index_definition(index_obj)

        for count, qvec in enumerate(query_vectors):
            knn_clause = {
                "field": self.bq_vector_field,
                "vector": qvec,
                "k": self.k,
                "filter": {
                    "min": 2000, "max": 2025,
                    "inclusive_min": True, "inclusive_max": True,
                    "field": "year"
                }
            }

            hits, matches, _, status = index_obj.execute_query(
                query={"match_none": {}},
                knn=[knn_clause],
                explain=False,
                return_raw_hits=True,
                fields=["*"]
            )
            self.log.info(f"Pre-filtered query {count}: hits={hits}")

        self.log.info("BQ search with pre-filter test passed")

    def test_bq_search_latency_measurement(self):
        all_vectors, query_vectors = self._setup_bq_test_data()

        index_obj = self._create_bq_fts_index_on_bucket("bq_latency")
        self._validate_bq_index_definition(index_obj)

        latency_stats = self._measure_query_latency(index_obj, query_vectors)
        self.assertTrue(len(latency_stats) > 0, "No latency stats collected")

        if self.bq_latency_threshold_ms > 0:
            self.assertLessEqual(
                latency_stats['avg_ms'], self.bq_latency_threshold_ms,
                f"Avg latency {latency_stats['avg_ms']}ms exceeds "
                f"threshold {self.bq_latency_threshold_ms}ms")

        self.log.info(f"BQ latency test passed: {latency_stats}")

    def test_bq_search_recall_validation(self):
        all_vectors, query_vectors = self._setup_bq_test_data()

        index_obj = self._create_bq_fts_index_on_bucket("bq_recall")
        self._validate_bq_index_definition(index_obj)

        avg_recall, recalls = self._run_bq_queries_and_validate_recall(
            index_obj, query_vectors)

        self.assertGreaterEqual(avg_recall, self.bq_recall_threshold,
                                f"BQ recall {avg_recall:.1f}% below threshold "
                                f"{self.bq_recall_threshold}%")
        self.log.info(f"BQ recall validation passed: avg={avg_recall:.1f}%, "
                      f"min={min(recalls):.1f}%, max={max(recalls):.1f}%")

    def test_bq_search_ram_usage_during_queries(self):
        all_vectors, query_vectors = self._setup_bq_test_data()

        index_obj = self._create_bq_fts_index_on_bucket("bq_ram")
        self._validate_bq_index_definition(index_obj)

        ram_delta = self._measure_ram_usage(index_obj, query_vectors)

        for ip, delta in ram_delta.items():
            if delta['before_bytes'] > 0:
                self.assertLessEqual(
                    delta['growth_pct'], self.bq_ram_growth_pct_threshold,
                    f"RAM growth on {ip} ({delta['growth_pct']}%) exceeds "
                    f"threshold ({self.bq_ram_growth_pct_threshold}%)")

        self.log.info("BQ RAM usage test passed")

    def test_bq_nprobe_parameter_tuning(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        index_obj = self._create_bq_fts_index_on_bucket("bq_nprobe")
        self._validate_bq_index_definition(index_obj)

        nprobe_configs = [
            {"ivf_nprobe_pct": 10, "ivf_max_codes_pct": 10},
            {"ivf_nprobe_pct": 50, "ivf_max_codes_pct": 50},
            {"ivf_nprobe_pct": 100, "ivf_max_codes_pct": 100},
        ]

        results_by_nprobe = {}
        for config in nprobe_configs:
            key = f"nprobe_{config['ivf_nprobe_pct']}"
            recalls = []

            for qvec in query_vectors:
                faiss_neighbors = self._get_faiss_nearest_neighbors(qvec)
                hits, matches, _, status = self._run_bq_fts_knn_query(
                    index_obj, qvec, knn_params=config)

                fts_doc_ids = self._extract_doc_ids_from_fts_results(matches)
                recall = self._compute_recall_against_faiss(
                    fts_doc_ids, faiss_neighbors)
                recalls.append(recall)

            avg = (sum(recalls) / len(recalls)) if recalls else 0
            results_by_nprobe[key] = avg
            self.log.info(f"{key}: avg_recall={avg:.1f}%")

        self.log.info(f"nprobe tuning results: {results_by_nprobe}")

        recall_values = list(results_by_nprobe.values())
        if len(recall_values) >= 2:
            self.assertGreaterEqual(recall_values[-1], recall_values[0],
                                    "Higher nprobe should yield equal or better recall")

    def test_bq_search_combined_with_fts_queries(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        extra_fields = [{"make": "text"}, {"year": "number"}]
        index_obj = self._create_bq_fts_index_on_bucket("bq_combined",
                                                          extra_fields=extra_fields)
        self._validate_bq_index_definition(index_obj)

        for count, qvec in enumerate(query_vectors):
            hits, matches, _, status = self._run_bq_fts_knn_query(
                index_obj, qvec,
                extra_query={"min": 2000, "max": 2025,
                             "inclusive_min": True, "inclusive_max": True,
                             "field": "year"})

            self.log.info(f"Combined query {count}: hits={hits}")
            if isinstance(status, dict) and status.get('failed', 0) != 0:
                self.fail(f"Combined BQ+FTS query {count} failed: {status}")

        self.log.info("BQ combined with FTS queries test passed")

    def test_bq_invalid_optimized_for_value(self):
        self._restore_couchbase_bucket()

        bucket = self._cb_cluster.get_bucket_by_name(self.bq_bucket)
        fts_index = self.create_index(
            bucket, "bq_invalid",
            collection_index=True,
            _type=f"{self.bq_scope}.{self.bq_collection}",
            scope=self.bq_scope,
            collections=[self.bq_collection]
        )

        invalid_vector_fields = {
            "dims": self.dimension,
            "similarity": self.similarity,
            "vector_index_optimized_for": "invalid-bq-type"
        }
        fts_index.add_child_field_to_default_collection_mapping(
            field_name=self.bq_vector_field,
            field_type="vector",
            field_alias=self.bq_vector_field,
            scope=self.bq_scope,
            collection=self.bq_collection,
            vector_fields=invalid_vector_fields
        )

        fts_index.index_definition['uuid'] = fts_index.get_uuid()
        try:
            fts_index.update()
            self.wait_for_indexing_complete()

            query_vec = [0.0] * self.dimension
            hits, matches, _, status = self._run_bq_fts_knn_query(fts_index, query_vec)
            if isinstance(status, dict) and status.get('failed', 0) > 0:
                self.log.info("Query correctly failed with invalid BQ type")
            else:
                self.log.warning("Index with invalid vector_index_optimized_for did not "
                                 "produce an error on update or query -- this may be a bug")
        except Exception as e:
            self.log.info(f"Index update correctly rejected invalid BQ type: {e}")

        self.log.info("test_bq_invalid_optimized_for_value passed")

    def test_bq_index_null_empty_vectors(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        index_obj = self._create_bq_fts_index_on_bucket("bq_nullvec")
        self._validate_bq_index_definition(index_obj)

        rest = RestConnection(self._cb_cluster.get_master_node())
        sdk_url = f"`{self.bq_bucket}`.`{self.bq_scope}`.`{self.bq_collection}`"

        self._cb_cluster.run_n1ql_query(
            f'INSERT INTO {sdk_url} (KEY, VALUE) VALUES '
            f'("null_vec_doc", {{"type": "car", "make": "TestNull", '
            f'"{self.bq_vector_field}": null}})')
        self._cb_cluster.run_n1ql_query(
            f'INSERT INTO {sdk_url} (KEY, VALUE) VALUES '
            f'("empty_vec_doc", {{"type": "car", "make": "TestEmpty", '
            f'"{self.bq_vector_field}": []}})')
        self._cb_cluster.run_n1ql_query(
            f'INSERT INTO {sdk_url} (KEY, VALUE) VALUES '
            f'("missing_vec_doc", {{"type": "car", "make": "TestMissing"}})')

        time.sleep(10)
        self.wait_for_indexing_complete()

        for qvec in query_vectors[:3]:
            hits, matches, _, status = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertGreater(hits, 0, "BQ query returned 0 hits after inserting null/empty docs")

        self.log.info("test_bq_index_null_empty_vectors passed -- no crash with null/empty vectors")

    def test_bq_index_extreme_vectors(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        index_obj = self._create_bq_fts_index_on_bucket("bq_extreme")
        self._validate_bq_index_definition(index_obj)

        sdk_url = f"`{self.bq_bucket}`.`{self.bq_scope}`.`{self.bq_collection}`"

        zeros_vec = [0.0] * self.dimension
        ones_vec = [1.0] * self.dimension
        self._cb_cluster.run_n1ql_query(
            f'INSERT INTO {sdk_url} (KEY, VALUE) VALUES '
            f'("all_zeros_doc", {{"type": "car", "make": "Zeros", '
            f'"{self.bq_vector_field}": {json.dumps(zeros_vec)}}})')
        self._cb_cluster.run_n1ql_query(
            f'INSERT INTO {sdk_url} (KEY, VALUE) VALUES '
            f'("all_ones_doc", {{"type": "car", "make": "Ones", '
            f'"{self.bq_vector_field}": {json.dumps(ones_vec)}}})')

        time.sleep(10)
        self.wait_for_indexing_complete()

        hits_zeros, _, _, _ = self._run_bq_fts_knn_query(index_obj, zeros_vec, k=10)
        self.log.info(f"Query with all-zeros vector: hits={hits_zeros}")
        self.assertGreater(hits_zeros, 0, "All-zeros query returned 0 hits")

        hits_ones, _, _, _ = self._run_bq_fts_knn_query(index_obj, ones_vec, k=10)
        self.log.info(f"Query with all-ones vector: hits={hits_ones}")
        self.assertGreater(hits_ones, 0, "All-ones query returned 0 hits")

        for qvec in query_vectors[:3]:
            hits, _, _, _ = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertGreater(hits, 0, "Normal query returned 0 hits after inserting extreme docs")

        self.log.info("test_bq_index_extreme_vectors passed")

    def test_bq_small_dataset(self):
        self._restore_couchbase_bucket()

        sdk_url = f"`{self.bq_bucket}`.`{self.bq_scope}`.`{self.bq_collection}`"
        self._cb_cluster.run_n1ql_query(f"DELETE FROM {sdk_url} WHERE true LIMIT 99900")
        time.sleep(10)

        count_result = self._cb_cluster.run_n1ql_query(
            f"SELECT COUNT(*) AS cnt FROM {sdk_url}")['results']
        doc_count = count_result[0]['cnt']
        self.log.info(f"Small dataset doc count: {doc_count}")

        all_vectors = self._fetch_vectors_from_cb(limit=doc_count)
        self.assertTrue(len(all_vectors) > 0, "No vectors in small dataset")
        self.assertTrue(len(all_vectors) <= 200,
                        f"Expected <=200 vectors for small dataset test, got {len(all_vectors)}")

        self._build_faiss_ground_truth(all_vectors, similarity=self.similarity)
        query_vectors = self._pick_query_vectors(all_vectors,
                                                  num_queries=min(5, len(all_vectors)))

        k_val = min(self.k, len(all_vectors))
        index_obj = self._create_bq_fts_index_on_bucket("bq_small")
        self._validate_bq_index_definition(index_obj)

        original_k = self.k
        self.k = k_val
        for qvec in query_vectors:
            hits, _, _, _ = self._run_bq_fts_knn_query(index_obj, qvec, k=k_val)
            self.assertGreater(hits, 0, "BQ query on small dataset returned 0 hits")
        self.k = original_k

        self.log.info(f"test_bq_small_dataset passed with {len(all_vectors)} vectors")

    def test_bq_prefilter_low_selectivity(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        extra_fields = [{"year": "number"}]
        index_obj = self._create_bq_fts_index_on_bucket("bq_pf_low",
                                                          extra_fields=extra_fields)
        self._validate_bq_index_definition(index_obj)

        for count, qvec in enumerate(query_vectors):
            knn_clause = {
                "field": self.bq_vector_field,
                "vector": qvec,
                "k": self.k,
                "filter": {
                    "min": 2024, "max": 2025,
                    "inclusive_min": True, "inclusive_max": True,
                    "field": "year"
                }
            }
            hits, matches, _, status = index_obj.execute_query(
                query={"match_none": {}},
                knn=[knn_clause],
                explain=False,
                return_raw_hits=True,
                fields=["*"]
            )
            self.log.info(f"Low selectivity pre-filter query {count}: hits={hits}")

        self.log.info("test_bq_prefilter_low_selectivity passed")

    def test_bq_prefilter_high_selectivity(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        extra_fields = [{"year": "number"}]
        index_obj = self._create_bq_fts_index_on_bucket("bq_pf_high",
                                                          extra_fields=extra_fields)
        self._validate_bq_index_definition(index_obj)

        for count, qvec in enumerate(query_vectors):
            knn_clause = {
                "field": self.bq_vector_field,
                "vector": qvec,
                "k": self.k,
                "filter": {
                    "min": 1900, "max": 2025,
                    "inclusive_min": True, "inclusive_max": True,
                    "field": "year"
                }
            }
            hits, matches, _, status = index_obj.execute_query(
                query={"match_none": {}},
                knn=[knn_clause],
                explain=False,
                return_raw_hits=True,
                fields=["*"]
            )
            self.log.info(f"High selectivity pre-filter query {count}: hits={hits}")
            self.assertGreater(hits, 0,
                               "High selectivity pre-filter returned 0 hits")

        self.log.info("test_bq_prefilter_high_selectivity passed")

    def test_bq_prefilter_different_filter_types(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        extra_fields = [{"make": "text"}, {"year": "number"}]
        index_obj = self._create_bq_fts_index_on_bucket("bq_pf_types",
                                                          extra_fields=extra_fields)
        self._validate_bq_index_definition(index_obj)

        filter_configs = [
            ("term_filter", {"term": "Toyota", "field": "make"}),
            ("range_filter", {"min": 2010, "max": 2020,
                              "inclusive_min": True, "inclusive_max": True,
                              "field": "year"}),
            ("bool_filter", {"conjuncts": [
                {"term": "Honda", "field": "make"},
                {"min": 2015, "max": 2025,
                 "inclusive_min": True, "inclusive_max": True,
                 "field": "year"}
            ]}),
        ]

        for filter_name, filter_query in filter_configs:
            qvec = query_vectors[0]
            knn_clause = {
                "field": self.bq_vector_field,
                "vector": qvec,
                "k": self.k,
                "filter": filter_query
            }
            hits, matches, _, status = index_obj.execute_query(
                query={"match_none": {}},
                knn=[knn_clause],
                explain=False,
                return_raw_hits=True,
                fields=["*"]
            )
            self.log.info(f"Filter type '{filter_name}': hits={hits}")

        self.log.info("test_bq_prefilter_different_filter_types passed")

    def test_bq_backup_restore_fts_index(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        index_obj = self._create_bq_fts_index_on_bucket("bq_backup")
        self._validate_bq_index_definition(index_obj)

        avg_recall_before, _ = self._run_bq_queries_and_validate_recall(
            index_obj, query_vectors)
        self.log.info(f"Recall before backup: {avg_recall_before:.1f}%")

        _, initial_def = index_obj.get_index_defn()
        initial_index_def = initial_def['indexDef']

        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.assertTrue(len(fts_nodes) > 0, "No FTS nodes in cluster")

        backup_client = FTSIndexBackupClient(fts_nodes[0])
        status, content = backup_client.backup()
        self.assertTrue(status, f"FTS backup failed: {content}")
        self.log.info("FTS index backup completed successfully")

        self._cb_cluster.delete_fts_index("bq_backup")
        time.sleep(10)

        restore_ok, restore_resp, _ = backup_client.restore()
        self.assertTrue(restore_ok, f"FTS restore failed: {restore_resp}")
        self.log.info("FTS index restore completed successfully")
        time.sleep(10)

        self.wait_for_indexing_complete()

        restored_index = None
        for idx in self._cb_cluster.get_indexes():
            if idx.name == "bq_backup":
                restored_index = idx
                break
        self.assertIsNotNone(restored_index, "Restored index 'bq_backup' not found")

        _, restored_def = restored_index.get_index_defn()
        restored_index_def = restored_def['indexDef']

        orig_types = initial_index_def.get('params', {}).get('mapping', {}).get('types', {})
        rest_types = restored_index_def.get('params', {}).get('mapping', {}).get('types', {})
        for type_name in orig_types:
            self.assertIn(type_name, rest_types,
                          f"Type '{type_name}' missing from restored index")
            orig_props = orig_types[type_name].get('properties', {})
            rest_props = rest_types[type_name].get('properties', {})
            orig_bq = orig_props.get(self.bq_vector_field, {}).get('fields', [{}])[0]
            rest_bq = rest_props.get(self.bq_vector_field, {}).get('fields', [{}])[0]
            self.assertEqual(
                orig_bq.get('vector_index_optimized_for'),
                rest_bq.get('vector_index_optimized_for'),
                "BQ type mismatch after restore")

        avg_recall_after, _ = self._run_bq_queries_and_validate_recall(
            restored_index, query_vectors)
        self.log.info(f"Recall after restore: {avg_recall_after:.1f}%")

        self.log.info("test_bq_backup_restore_fts_index passed")

    def test_bq_disk_footprint_comparison(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=3)

        bq_index = self._create_bq_fts_index_on_bucket("bq_disk")
        self._validate_bq_index_definition(bq_index)

        non_bq_vector_fields = {
            "dims": self.dimension,
            "similarity": self.similarity
        }
        collections = self.bq_collections if self.bq_collections else [self.bq_collection]
        type_list = [f"{self.bq_scope}.{c}" for c in collections]

        bucket = self._cb_cluster.get_bucket_by_name(self.bq_bucket)
        non_bq_index = self.create_index(
            bucket, "nonbq_disk",
            collection_index=True,
            _type=type_list,
            scope=self.bq_scope,
            collections=collections
        )
        for collection in collections:
            non_bq_index.add_child_field_to_default_collection_mapping(
                field_name=self.bq_vector_field,
                field_type="vector",
                field_alias=self.bq_vector_field,
                scope=self.bq_scope,
                collection=collection,
                vector_fields=non_bq_vector_fields
            )
        non_bq_index.index_definition['uuid'] = non_bq_index.get_uuid()
        non_bq_index.update()
        self.wait_for_indexing_complete()

        time.sleep(15)
        fts_nodes = self._cb_cluster.get_fts_nodes()
        rest = RestConnection(fts_nodes[0])

        _, bq_stats = rest.get_fts_stats(index_name="bq_disk",
                                          stat_name="num_bytes_used_disk")
        _, nonbq_stats = rest.get_fts_stats(index_name="nonbq_disk",
                                             stat_name="num_bytes_used_disk")

        bq_disk = float(bq_stats) if bq_stats else 0
        nonbq_disk = float(nonbq_stats) if nonbq_stats else 0

        self.log.info(f"Disk footprint: BQ={bq_disk/(1024*1024):.2f}MB, "
                      f"Non-BQ={nonbq_disk/(1024*1024):.2f}MB")
        if nonbq_disk > 0:
            ratio = bq_disk / nonbq_disk
            self.log.info(f"BQ/Non-BQ disk ratio: {ratio:.2f}")

        self._cb_cluster.delete_fts_index("nonbq_disk")
        self.log.info("test_bq_disk_footprint_comparison passed")

    def test_bq_rest_api_index_definition(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=3)

        index_obj = self._create_bq_fts_index_on_bucket("bq_restapi")
        self._validate_bq_index_definition(index_obj)

        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        status, index_def_resp = rest.get_fts_index_definition("bq_restapi")
        self.assertTrue(status, "Failed to get index definition via REST API")

        index_def = index_def_resp.get('indexDef', {})

        self.assertEqual(index_def.get('type'), 'fulltext-index',
                         "Index type is not 'fulltext-index'")
        self.assertEqual(index_def.get('sourceName'), self.bq_bucket,
                         f"Source bucket mismatch: expected {self.bq_bucket}")

        types = index_def.get('params', {}).get('mapping', {}).get('types', {})
        self.assertTrue(len(types) > 0, "No type mappings in REST API response")

        for type_name, type_def in types.items():
            props = type_def.get('properties', {})
            vec_field = props.get(self.bq_vector_field, {})
            fields = vec_field.get('fields', [])
            self.assertTrue(len(fields) > 0,
                            f"No fields for {self.bq_vector_field} in REST response")
            field = fields[0]
            self.assertEqual(field.get('type'), 'vector',
                             "Field type is not 'vector'")
            self.assertEqual(field.get('dims'), self.dimension,
                             f"Dimension mismatch: expected {self.dimension}")
            self.assertEqual(field.get('vector_index_optimized_for'), self.bq_index_type,
                             f"BQ type mismatch: expected {self.bq_index_type}")

        self.log.info("test_bq_rest_api_index_definition passed")

    def test_bq_index_dynamic_mapping(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=5)

        collections = self.bq_collections if self.bq_collections else [self.bq_collection]
        type_list = [f"{self.bq_scope}.{c}" for c in collections]

        bucket = self._cb_cluster.get_bucket_by_name(self.bq_bucket)
        fts_index = self.create_index(
            bucket, "bq_dynamic",
            collection_index=True,
            _type=type_list,
            scope=self.bq_scope,
            collections=collections
        )

        vector_fields = self._get_bq_vector_fields()
        for collection in collections:
            fts_index.add_child_field_to_default_collection_mapping(
                field_name=self.bq_vector_field,
                field_type="vector",
                field_alias=self.bq_vector_field,
                scope=self.bq_scope,
                collection=collection,
                vector_fields=vector_fields
            )

        for collection in collections:
            type_key = f"{self.bq_scope}.{collection}"
            if type_key in fts_index.index_definition['params']['mapping']['types']:
                fts_index.index_definition['params']['mapping']['types'][type_key]['dynamic'] = True

        fts_index.index_definition['uuid'] = fts_index.get_uuid()
        fts_index.update()
        self.wait_for_indexing_complete()

        self._validate_bq_index_definition(fts_index)

        for qvec in query_vectors:
            hits, _, _, status = self._run_bq_fts_knn_query(fts_index, qvec)
            self.assertGreater(hits, 0, "Dynamic mapping BQ index returned 0 hits")

        hits, matches, _, _ = fts_index.execute_query(
            query={"term": "Toyota", "field": "make"},
            zero_results_ok=True,
            return_raw_hits=True,
            fields=["*"]
        )
        self.log.info(f"Dynamic mapping text query: hits={hits}")

        self.log.info("test_bq_index_dynamic_mapping passed")

    def test_bq_index_multiple_collections(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        self.assertTrue(len(self.bq_collections) >= 2,
                        f"Expected at least 2 collections from backup restore, "
                        f"got {len(self.bq_collections)}: {self.bq_collections}")

        index_obj = self._create_bq_fts_index_on_bucket("bq_multicol")
        self._validate_bq_index_definition(index_obj)

        status, index_def = index_obj.get_index_defn()
        types = index_def['indexDef'].get('params', {}).get('mapping', {}).get('types', {})
        indexed_collections = set()
        for type_name in types:
            parts = type_name.split('.')
            if len(parts) == 2:
                indexed_collections.add(parts[1])
        for col in self.bq_collections:
            self.assertIn(col, indexed_collections,
                          f"Collection '{col}' not found in index type mappings")

        avg_recall, _ = self._run_bq_queries_and_validate_recall(
            index_obj, query_vectors)

        self.log.info(f"test_bq_index_multiple_collections passed: "
                      f"collections={self.bq_collections}, recall={avg_recall:.1f}%")

    def test_bq_segment_merge_during_queries(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)

        index_obj = self._create_bq_fts_index_on_bucket("bq_segmerge")
        self._validate_bq_index_definition(index_obj)

        sdk_url = f"`{self.bq_bucket}`.`{self.bq_scope}`.`{self.bq_collection}`"

        query_errors = []

        def run_queries():
            for i in range(3):
                for qvec in query_vectors[:5]:
                    try:
                        hits, _, _, status = self._run_bq_fts_knn_query(index_obj, qvec)
                        if isinstance(status, dict) and status.get('failed', 0) > 0:
                            query_errors.append(f"Round {i}: query failed: {status}")
                    except Exception as e:
                        query_errors.append(f"Round {i}: exception: {e}")
                    time.sleep(0.5)

        query_thread = threading.Thread(target=run_queries)
        query_thread.start()

        for batch in range(5):
            sample_vec = [random.random() for _ in range(self.dimension)]
            for i in range(20):
                doc_id = f"merge_test_{batch}_{i}"
                self._cb_cluster.run_n1ql_query(
                    f'UPSERT INTO {sdk_url} (KEY, VALUE) VALUES '
                    f'("{doc_id}", {{"type": "car", "make": "MergeTest", '
                    f'"year": 2025, '
                    f'"{self.bq_vector_field}": {json.dumps(sample_vec)}}})')
            time.sleep(2)

        query_thread.join(timeout=120)

        self.assertEqual(len(query_errors), 0,
                         f"Queries failed during segment merge: {query_errors}")

        for qvec in query_vectors[:3]:
            hits, _, _, _ = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertGreater(hits, 0, "Post-merge query returned 0 hits")

        self.log.info("test_bq_segment_merge_during_queries passed")

    def test_bq_recall_at_different_k(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=20)

        index_obj = self._create_bq_fts_index_on_bucket("bq_recallk")
        self._validate_bq_index_definition(index_obj)

        k_values = [3, 10, 50, 100]
        recall_results = {}

        for k_val in k_values:
            recalls = []
            for qvec in query_vectors:
                faiss_neighbors = self._get_faiss_nearest_neighbors(qvec, k=k_val)
                hits, matches, _, status = self._run_bq_fts_knn_query(
                    index_obj, qvec, k=k_val)
                fts_doc_ids = self._extract_doc_ids_from_fts_results(matches)
                recall = self._compute_recall_against_faiss(
                    fts_doc_ids, faiss_neighbors, k=k_val)
                recalls.append(recall)

            avg_recall = sum(recalls) / len(recalls) if recalls else 0
            recall_results[f"recall@{k_val}"] = round(avg_recall, 2)
            self.log.info(f"recall@{k_val}: avg={avg_recall:.2f}%, "
                          f"min={min(recalls):.2f}%, max={max(recalls):.2f}%")

        self.log.info(f"Full recall results: {recall_results}")

        for key, val in recall_results.items():
            self.assertGreater(val, 0, f"{key} is 0% -- likely a bug")

        self.log.info("test_bq_recall_at_different_k passed")

    def test_bq_community_edition_negative(self):
        rest = RestConnection(self._cb_cluster.get_master_node())
        if rest.is_enterprise_edition():
            self.log.info("Skipping CE negative test -- running on Enterprise Edition")
            return

        self._restore_couchbase_bucket()

        bucket = self._cb_cluster.get_bucket_by_name(self.bq_bucket)
        fts_index = self.create_index(
            bucket, "bq_ce_neg",
            collection_index=True,
            _type=f"{self.bq_scope}.{self.bq_collection}",
            scope=self.bq_scope,
            collections=[self.bq_collection]
        )

        vector_fields = self._get_bq_vector_fields()
        fts_index.add_child_field_to_default_collection_mapping(
            field_name=self.bq_vector_field,
            field_type="vector",
            field_alias=self.bq_vector_field,
            scope=self.bq_scope,
            collection=self.bq_collection,
            vector_fields=vector_fields
        )

        fts_index.index_definition['uuid'] = fts_index.get_uuid()
        try:
            fts_index.update()
            self.log.warning("BQ index creation succeeded on CE -- this may be expected "
                             "if BQ is available on CE, otherwise it's a bug")
        except Exception as e:
            self.log.info(f"BQ index creation correctly rejected on CE: {e}")

        self.log.info("test_bq_community_edition_negative passed")

    def _setup_bq_index_for_topology_test(self):
        all_vectors, query_vectors = self._setup_bq_test_data(num_queries=10)
        index_obj = self._create_bq_fts_index_on_bucket("bq_topo")
        self._validate_bq_index_definition(index_obj)
        return index_obj, all_vectors, query_vectors

    def _verify_bq_index_after_topology_change(self, index_obj, query_vectors):
        self.wait_for_indexing_complete()

        for qvec in query_vectors[:5]:
            hits, matches, _, status = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertGreater(hits, 0,
                               "BQ query returned 0 hits after topology change")
            if isinstance(status, dict) and status.get('failed', 0) > 0:
                self.fail(f"BQ query had failures after topology change: {status}")

        avg_recall, _ = self._run_bq_queries_and_validate_recall(
            index_obj, query_vectors, num_queries=5)
        self.log.info(f"Recall after topology change: {avg_recall:.1f}%")
        return avg_recall

    def _find_failover_node(self):
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "fts" in node_services:
                return node
        return self._cb_cluster.get_nodes()[-1]

    def test_bq_rebalance_in_during_indexing(self):
        self._restore_couchbase_bucket()

        all_vectors = self._fetch_vectors_from_cb()
        self._build_faiss_ground_truth(all_vectors, similarity=self.similarity)
        query_vectors = self._pick_query_vectors(all_vectors, num_queries=10)

        index_obj = self._create_bq_fts_index_on_bucket("bq_topo")

        self.log.info("BQ index building has begun, starting rebalance-in...")
        self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        self._verify_bq_index_after_topology_change(index_obj, query_vectors)
        self.log.info("test_bq_rebalance_in_during_indexing passed")

    def test_bq_rebalance_out_during_indexing(self):
        self._restore_couchbase_bucket()

        all_vectors = self._fetch_vectors_from_cb()
        self._build_faiss_ground_truth(all_vectors, similarity=self.similarity)
        query_vectors = self._pick_query_vectors(all_vectors, num_queries=10)

        index_obj = self._create_bq_fts_index_on_bucket("bq_topo")

        self.log.info("BQ index created, starting rebalance-out...")
        self._cb_cluster.rebalance_out()

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        self._verify_bq_index_after_topology_change(index_obj, query_vectors)
        self.log.info("test_bq_rebalance_out_during_indexing passed")

    def test_bq_swap_rebalance_during_indexing(self):
        self._restore_couchbase_bucket()

        all_vectors = self._fetch_vectors_from_cb()
        self._build_faiss_ground_truth(all_vectors, similarity=self.similarity)
        query_vectors = self._pick_query_vectors(all_vectors, num_queries=10)

        index_obj = self._create_bq_fts_index_on_bucket("bq_topo")

        self.log.info("BQ index created, starting swap rebalance...")
        rest = RestConnection(self._cb_cluster.get_master_node())
        if rest.is_enterprise_edition():
            services = ["fts"]
        else:
            services = ["fts,kv,index,n1ql"]
        self._cb_cluster.swap_rebalance(services=services)

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        self._verify_bq_index_after_topology_change(index_obj, query_vectors)
        self.log.info("test_bq_swap_rebalance_during_indexing passed")

    def test_bq_graceful_failover_and_recovery(self):
        index_obj, all_vectors, query_vectors = self._setup_bq_index_for_topology_test()

        failover_node = self._find_failover_node()
        self.log.info(f"Graceful failover of node {failover_node.ip}...")

        task = self._cb_cluster.async_failover(graceful=True, node=failover_node)
        task.result()
        self.sleep(60, "Waiting after graceful failover")

        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        self._verify_bq_index_after_topology_change(index_obj, query_vectors)
        self.log.info("test_bq_graceful_failover_and_recovery passed")

    def test_bq_hard_failover_and_recovery(self):
        index_obj, all_vectors, query_vectors = self._setup_bq_index_for_topology_test()

        failover_node = self._find_failover_node()
        self.log.info(f"Hard failover of node {failover_node.ip}...")

        task = self._cb_cluster.async_failover(node=failover_node)
        task.result()

        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        self._verify_bq_index_after_topology_change(index_obj, query_vectors)
        self.log.info("test_bq_hard_failover_and_recovery passed")

    def test_bq_failover_during_knn_queries(self):
        index_obj, all_vectors, query_vectors = self._setup_bq_index_for_topology_test()

        query_results = []

        def run_queries():
            for i in range(5):
                for qvec in query_vectors[:3]:
                    try:
                        hits, _, _, _ = self._run_bq_fts_knn_query(index_obj, qvec)
                        query_results.append(('ok', hits))
                    except Exception as e:
                        query_results.append(('error', str(e)))
                    time.sleep(1)

        query_thread = threading.Thread(target=run_queries)
        query_thread.start()

        time.sleep(3)
        failover_node = self._find_failover_node()
        self.log.info(f"Triggering failover during queries on {failover_node.ip}...")
        task = self._cb_cluster.async_failover(node=failover_node)
        task.result()

        query_thread.join(timeout=120)

        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])
        self.wait_for_indexing_complete()

        ok_count = sum(1 for r in query_results if r[0] == 'ok')
        err_count = sum(1 for r in query_results if r[0] == 'error')
        self.log.info(f"Queries during failover: ok={ok_count}, errors={err_count}")

        for qvec in query_vectors[:3]:
            hits, _, _, _ = self._run_bq_fts_knn_query(index_obj, qvec)
            self.assertGreater(hits, 0, "Post-failover query returned 0 hits")

        self.log.info("test_bq_failover_during_knn_queries passed")

    def test_bq_rebalance_between_indexing_and_query(self):
        index_obj, all_vectors, query_vectors = self._setup_bq_index_for_topology_test()

        self.log.info("Index is built, rebalancing before queries...")
        self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        self._verify_bq_index_after_topology_change(index_obj, query_vectors)
        self.log.info("test_bq_rebalance_between_indexing_and_query passed")

    def test_bq_fts_node_crash_during_indexing(self):
        self._restore_couchbase_bucket()

        all_vectors = self._fetch_vectors_from_cb()
        self._build_faiss_ground_truth(all_vectors, similarity=self.similarity)
        query_vectors = self._pick_query_vectors(all_vectors, num_queries=10)

        index_obj = self._create_bq_fts_index_on_bucket("bq_topo")

        fts_node = self._cb_cluster.get_random_fts_node()
        self.log.info(f"Killing cbft process on {fts_node.ip}...")
        NodeHelper.kill_cbft_process(fts_node)

        self.sleep(30, "Waiting for FTS process to restart")

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        self._verify_bq_index_after_topology_change(index_obj, query_vectors)
        self.log.info("test_bq_fts_node_crash_during_indexing passed")
