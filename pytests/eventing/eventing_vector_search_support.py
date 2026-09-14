import json
import re
import time
import logging

from pytests.eventing.eventing_constants import HANDLER_CODE_VECTOR_SEARCH
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.fts.fts_callable import FTSCallable
from pytests.fts.vector_dataset_generator.vector_dataset_loader import VectorLoader
from pytests.fts.vector_dataset_generator.vector_dataset_generator import VectorDataset
from lib.membase.api.rest_client import RestConnection
from lib.sdk_client3 import SDKClient

log = logging.getLogger()


class EventingVectorSearchSupport(EventingBaseTest):

    def setUp(self):
        super(EventingVectorSearchSupport, self).setUp()
        self.vector_index_name = self.input.param("vector_index_name", "vector_test_index")
        self.vector_namespace = self.input.param("vector_namespace", "default.scope0.collection0")
        self.vector_dataset = self.input.param("vector_dataset", "siftsmall")
        self.dimension = self.input.param("dimension", 128)
        self.similarity = self.input.param("similarity", "l2_norm")
        self.k = self.input.param("k", 10)
        self.expected_recall = self.input.param("expected_recall", 85)
        self.vector_field_path = self.input.param("vector_field_path", "vector_data")
        self.vector_index = None

        handler_code = self.input.param('handler_code', 'search_query')
        if handler_code == 'search_query':
            self.handler_code = HANDLER_CODE_VECTOR_SEARCH.VECTOR_SEARCH_KNN_QUERY
            self.dest_query_name = "knnQuery"
        elif handler_code == 'search_query_internal':
            self.handler_code = HANDLER_CODE_VECTOR_SEARCH.VECTOR_SEARCH_KNN_QUERY_INTERNAL
            self.dest_query_name = "knnQueryInternal"
        else:
            raise ValueError(f"Unknown handler_code: {handler_code}")

        self.fts_callable = FTSCallable(nodes=self.servers, es_validate=False)
        fts_memory_quota = self.input.param("fts_memory_quota", 3000)
        log.info("quota for fts service will be %s MB" % fts_memory_quota)
        rest = RestConnection(self.master)
        rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=fts_memory_quota)

    def tearDown(self):
        if getattr(self, "vector_index", None):
            try:
                self.fts_callable.delete_fts_index(self.vector_index_name)
            except Exception:
                log.exception("Cleaning vector index %s failed", self.vector_index_name)
            finally:
                self.vector_index = None
        super(EventingVectorSearchSupport, self).tearDown()

    def _namespace_parts(self):
        parts = self.vector_namespace.split(".")
        return parts[0], parts[1], parts[2]

    def create_vector_index(self, bucket, scope, collection):
        """
        Builds the vector index directly instead of via FTSCallable.create_fts_index,
        which would overwrite the nested vector field mapping added below.
        """
        index = self.fts_callable.generate_FTSIndex_info(
            name=self.vector_index_name,
            source_name=bucket,
            collection_index=True,
            scope=scope,
            collections=[collection]
        )
        index.add_type_mapping_to_index_definition(
            type=f"{scope}.{collection}", analyzer="standard")
        index.index_definition['params']['doc_config'] = {
            "mode": "scope.collection.type_field",
            "type_field": "type"
        }
        index.add_child_field_to_default_collection_mapping(
            field_name=self.vector_field_path,
            field_type="vector",
            scope=scope,
            collection=collection,
            vector_fields={"dims": self.dimension, "similarity": self.similarity}
        )
        index.create()
        return index

    def load_vector_dataset(self, bucket, scope, collection):
        loader = VectorLoader(
            self.master,
            self.master.rest_username,
            self.master.rest_password,
            bucket, scope, collection,
            [self.vector_dataset]
        )
        loader.load_data()

    def get_query_vector_and_groundtruth(self):
        ds = VectorDataset(self.vector_dataset)
        ds.extract_vectors_from_file(use_hdf5_datasets=False, type_of_vec="query")
        query_vector = ds.query_vecs[0]

        ds.extract_vectors_from_file(use_hdf5_datasets=False, type_of_vec="groundtruth")
        groundtruth_ids = ds.neighbors_vecs[0][:self.k]

        return query_vector, groundtruth_ids

    def _doc_id_to_index(self, doc_id):
        """VectorLoader-generated keys look like 'vect2177', not a bare integer --
        pull the trailing digits out regardless of whatever prefix the loader used."""
        match = re.search(r'(\d+)$', str(doc_id))
        if not match:
            raise ValueError(f"Could not extract a numeric index from doc id: {doc_id}")
        return int(match.group(1)) - 1

    def compute_recall(self, result_ids, groundtruth_ids):
        result_set = set(self._doc_id_to_index(i) for i in result_ids)
        truth_set = set(int(i) for i in groundtruth_ids)
        if not truth_set:
            return 0
        return (len(result_set & truth_set) / len(truth_set)) * 100

    def wait_for_query_result(self, dst_namespace, doc_id, timeout=120):
        bucket, scope, collection = dst_namespace.split(".")
        client = SDKClient(
            hosts=[self.master.ip], bucket=bucket,
            username=self.master.rest_username,
            password=self.master.rest_password
        )
        end_time = time.time() + timeout
        last_error = None
        while time.time() < end_time:
            try:
                result = client.get(doc_id, scope=scope, collection=collection)
                if result is not None:
                    if hasattr(result, "content_as"):
                        return result.content_as[dict]
                    if hasattr(result, "value"):
                        return result.value
                    return result
            except Exception as e:
                last_error = e
            self.sleep(5, "Waiting for eventing to write query result")
        self.fail(f"Destination doc {doc_id} was not written within {timeout}s (last error: {last_error})")

    # MB-70470
    def test_eventing_vector_search_knn(self):
        """
        Load vector dataset
        Create vector FTS index
        Deploy Eventing handler bound to a fixed query vector via depcfg.constants
        Trigger handler via mutation
        Read back result doc, compute recall against groundtruth
        """
        bucket, scope, collection = self._namespace_parts()

        self.load_vector_dataset(bucket, scope, collection)
        self.vector_index = self.create_vector_index(bucket, scope, collection)
        self.fts_callable.wait_for_indexing_complete(complete_wait=False, idx=self.vector_index)

        query_vector, groundtruth_ids = self.get_query_vector_and_groundtruth()

        body = self.create_save_function_body(
            self.function_name, self.handler_code)
        body['depcfg']['constants'] = [
            {"value": "QUERY_VECTOR", "literal": json.dumps(list(query_vector))},
            {"value": "K", "literal": str(self.k)},
            {"value": "VECTOR_FIELD", "literal": json.dumps(self.vector_field_path)},
            {"value": "INDEX_NAME", "literal": json.dumps(self.vector_index_name)}
        ]
        self.rest.delete_single_function(body['appname'], self.function_scope)
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)

        client = SDKClient(hosts=[self.master.ip], bucket=bucket,
                           username=self.master.rest_username,
                           password=self.master.rest_password)
        client.upsert("vectorTriggerDoc", {"type": "trigger"}, scope=scope, collection=collection)

        try:
            result = self.wait_for_query_result(
                "default.scope0.collection1", f"{self.dest_query_name}_vectorTriggerDoc")
        except Exception:
            self.print_execution_and_failure_stats(body['appname'])
            self.print_app_logs(body['appname'])
            raise
        self.log.info(f"Handler returned {len(result['ids'])} ids: {result['ids']}")
        self.log.info(f"Groundtruth top-{self.k} indices: {list(int(i) for i in groundtruth_ids)}")
        recall = self.compute_recall(result['ids'], groundtruth_ids)

        self.log.info(f"KNN recall: {recall:.2f}% (threshold {self.expected_recall}%)")
        self.assertTrue(recall >= self.expected_recall,
                        f"Recall {recall:.2f}% below threshold {self.expected_recall}%")

        self.undeploy_and_delete_function(body)
