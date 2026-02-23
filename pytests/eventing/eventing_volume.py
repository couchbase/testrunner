import logging

from pytests.eventing.eventing_constants import HANDLER_CODE_VOLUME
from pytests.eventing.eventing_base import EventingBaseTest

log = logging.getLogger()

class EventingVolume(EventingBaseTest):
    def setUp(self):
        super(EventingVolume, self).setUp()
        # CURL handler requires a reachable hostname for HTTP bindings
        self.hostname = "http://qa.sc.couchbase.com/"
        # Create scopes for volume testing (5 scopes in each bucket)
        self.create_n_scope(self.dst_bucket_name, 5)
        self.create_n_scope(self.src_bucket_name, 5)
        # Create collections under scope_1 for function bindings
        self.create_n_collections(self.dst_bucket_name, "scope_1", 5)
        self.create_n_collections(self.src_bucket_name, "scope_1", 5)
        self.batch_size = 10**4

    def tearDown(self):
        super(EventingVolume, self).tearDown()

    def _create_all_volume_handlers(self):
        """Create all volume eventing handlers and return them as a dict"""
        return {
            'bucket_op': self.create_function_with_collection(
                "bucket_op_vol", HANDLER_CODE_VOLUME.BUCKET_OP_INSERT_REBALANCE,
                collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_0.rw"]
            ),
            'timers': self.create_function_with_collection(
                "timers_vol", HANDLER_CODE_VOLUME.TIMER_OP_INSERT,
                collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_1.rw"]
            ),
            'sbm': self.create_function_with_collection(
                "sbm_vol", HANDLER_CODE_VOLUME.SOURCE_BUCKET_MUTATION_INSERT,
                src_namespace="src_bucket.scope_1.coll_1",
                collection_bindings=["src_bucket.src_bucket.scope_1.coll_1.rw"]
            ),
            'curl': self.create_function_with_collection(
                "curl_vol", HANDLER_CODE_VOLUME.CURL_OP_GET,
                collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_3.rw"],
                is_curl=True
            ),
            'n1ql': self.create_function_with_collection(
                "n1ql_vol", HANDLER_CODE_VOLUME.N1QL_INSERT_REBALANCE,
                collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_4.rw"]
            )
        }

    def _deploy_all_volume_handlers(self, handlers):
        """Deploy all volume handlers from the handlers dict"""
        self.deploy_function(handlers['bucket_op'])
        self.deploy_function(handlers['timers'])
        self.deploy_function(handlers['sbm'])
        self.deploy_function(handlers['curl'])
        self.deploy_function(handlers['n1ql'])

    def _undeploy_and_delete_all_volume_handlers(self, handlers):
        """Undeploy and delete all volume handlers from the handlers dict"""
        self.undeploy_and_delete_function(handlers['bucket_op'])
        self.undeploy_and_delete_function(handlers['timers'])
        self.undeploy_and_delete_function(handlers['sbm'])
        self.undeploy_and_delete_function(handlers['curl'])
        self.undeploy_and_delete_function(handlers['n1ql'])

    def test_eventing_volume_concurrent_handlers_create_delete_mutations(self):
        '''
        Volume test: Multiple eventing handler types processing concurrent CREATE and DELETE mutations
        Tests: bucket operations, timers, source bucket mutations (SBM), CURL, and N1QL at scale
        '''
        # Create all function handlers
        handlers = self._create_all_volume_handlers()

        # Load data to source collections (CREATE mutations)
        src_default_load_tasks = self.load_batch_data_to_collection(
            self.docs_per_day * self.num_docs,
            "src_bucket._default._default",
            wait_for_loading=False
        )
        src_coll1_load_tasks = self.load_batch_data_to_collection(
            self.docs_per_day * self.num_docs,
            "src_bucket.scope_1.coll_1",
            wait_for_loading=False
        )

        self._deploy_all_volume_handlers(handlers)

        for task in src_default_load_tasks + src_coll1_load_tasks:
            task.result()

        # Wait for eventing to catch up with all CREATE mutations and verify results
        expected_docs = self.docs_per_day * self.num_docs

        self.verify_doc_count_collections("dst_bucket.scope_1.coll_0", expected_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_1", expected_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", expected_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", expected_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", expected_docs * 2)

        # DELETE mutations
        src_default_delete_tasks = self.load_batch_data_to_collection(
            self.docs_per_day * self.num_docs,
            "src_bucket._default._default",
            is_delete=True,
            wait_for_loading=False
        )
        src_coll1_delete_tasks = self.load_batch_data_to_collection(
            self.docs_per_day * self.num_docs,
            "src_bucket.scope_1.coll_1",
            is_delete=True,
            wait_for_loading=False
        )

        for task in src_default_delete_tasks + src_coll1_delete_tasks:
            task.result()

        # Wait for eventing to catch up with all DELETE mutations and verify results
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_0", 0)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_1", 0)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", 0)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", 0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", expected_docs)

        self._undeploy_and_delete_all_volume_handlers(handlers)
