from couchbase_helper.documentgenerator import SDKDataLoader
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class EventingCollections(EventingBaseTest):
    def setUp(self):
        super(EventingCollections, self).setUp()

    def tearDown(self):
        super(EventingCollections, self).tearDown()

    def test_source_collection_recreate(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert.js")
        body['depcfg']['buckets'].append(
            {"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "scope_name": self.src_bucket_name,
             "collection_name": self.src_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                     is_delete=True)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        self.pause_function(body)
        self.collection_rest.delete_collection("src_bucket","src_bucket","src_bucket")
        self.wait_for_handler_state(body['appname'], "undeployed")
        self.collection_rest.create_collection(bucket=self.src_bucket_name, scope=self.src_bucket_name,
                                     collection=self.src_bucket_name)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_source_scope_recreate(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert.js")
        body['depcfg']['buckets'].append(
            {"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "scope_name": self.src_bucket_name,
             "collection_name": self.src_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                     is_delete=True)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        self.pause_function(body)
        self.collection_rest.delete_scope("src_bucket","src_bucket")
        self.wait_for_handler_state(body['appname'],"undeployed")
        self.create_scope_collection(self.src_bucket_name,self.src_bucket_name,self.src_bucket_name)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)


    def test_destination_collection_recreate(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert.js")
        body['depcfg']['buckets'].append(
            {"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "scope_name": self.src_bucket_name,
             "collection_name": self.src_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.pause_function(body)
        self.collection_rest.delete_collection("dst_bucket","dst_bucket","dst_bucket")
        self.collection_rest.create_collection(bucket=self.dst_bucket_name, scope=self.dst_bucket_name,
                                               collection=self.dst_bucket_name)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                     is_delete=True)
        self.resume_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        self.undeploy_and_delete_function(body)

    def test_destination_scope_recreate(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert.js")
        body['depcfg']['buckets'].append(
            {"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "scope_name": self.src_bucket_name,
             "collection_name": self.src_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.pause_function(body)
        self.collection_rest.delete_scope("dst_bucket","dst_bucket")
        self.collection_rest.create_scope(bucket=self.dst_bucket_name, scope=self.dst_bucket_name)
        self.collection_rest.create_collection(bucket=self.dst_bucket_name, scope=self.dst_bucket_name,
                                               collection=self.dst_bucket_name)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                     is_delete=True)
        self.resume_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        self.undeploy_and_delete_function(body)

    def test_metadata_collection_recreate(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert.js")
        body['depcfg']['buckets'].append(
            {"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "scope_name": self.src_bucket_name,
             "collection_name": self.src_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                     is_delete=True)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        self.pause_function(body)
        self.collection_rest.delete_collection("metadata","metadata","metadata")
        self.wait_for_handler_state(body['appname'], "undeployed")
        self.collection_rest.create_collection(bucket=self.metadata_bucket_name, scope=self.metadata_bucket_name,
                                     collection=self.metadata_bucket_name)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_metadata_scope_recreate(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert.js")
        body['depcfg']['buckets'].append(
            {"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "scope_name": self.src_bucket_name,
             "collection_name": self.src_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket",
                                     is_delete=True)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", 0)
        self.pause_function(body)
        self.collection_rest.delete_scope("metadata","metadata")
        self.wait_for_handler_state(body['appname'],"undeployed")
        self.create_scope_collection(self.metadata_bucket_name,self.metadata_bucket_name,self.metadata_bucket_name)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    def test_same_scope_multiple_handler(self):
        self.create_n_collections(self.dst_bucket_name,self.dst_bucket_name,3)
        for i in range(3):
            body = self.create_function_with_collection(self.function_name+"_"+str(i), "handler_code/ABO/insert_rand.js",
                                                        src_namespace="src_bucket.src_bucket.src_bucket",
                                                        collection_bindings=["src_bucket.src_bucket.src_bucket.src_bucket.r",
                                                        "dst_bucket.dst_bucket.dst_bucket.coll_"+str(i)+".rw"])
            self.rest.create_function(body['appname'], body, self.function_scope)
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        for i in range(3):
            self.verify_doc_count_collections("dst_bucket.dst_bucket.coll_"+str(i), self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()

    def test_same_scope_source(self):
        self.create_n_collections(self.src_bucket_name,self.src_bucket_name,3)
        for i in range(3):
            body = self.create_function_with_collection(self.function_name+"_"+str(i), "handler_code/ABO/insert_rand.js",
                                                        src_namespace="src_bucket.src_bucket.coll_"+str(i),
                                                        collection_bindings=["src_bucket.src_bucket.src_bucket.coll_"+str(i)+".r",
                                                        "dst_bucket.dst_bucket.dst_bucket.dst_bucket.rw"])
            self.rest.create_function(body['appname'], body, self.function_scope)
            self.deploy_function(body)
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.coll_"+str(i),wait_for_loading=False)
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs*3)
        self.undeploy_delete_all_functions()

    def test_same_metadata_scope(self):
        self.create_n_collections(self.metadata_bucket_name,self.metadata_bucket_name,3)
        for i in range(3):
            body = self.create_function_with_collection(self.function_name+"_"+str(i), "handler_code/ABO/insert_rand.js",
                                                        src_namespace="src_bucket.src_bucket.src_bucket",
                                                        meta_namespace="metadata.metadata.coll_"+str(i),
                                                        collection_bindings=["src_bucket.src_bucket.src_bucket.src_bucket.r",
                                                        "dst_bucket.dst_bucket.dst_bucket.dst_bucket.rw"])
            self.rest.create_function(body['appname'], body, self.function_scope)
            self.deploy_function(body,wait_for_bootstrap=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        self.verify_doc_count_collections("dst_bucket.dst_bucket.dst_bucket", self.docs_per_day * self.num_docs*3)
        self.undeploy_delete_all_functions()

    def test_source_and_metadata_same_keyspace(self):
        try:
            body = self.create_function_with_collection(self.function_name, "handler_code/ABO/insert_rand.js",
                                                 src_namespace="src_bucket.src_bucket.src_bucket",
                                                 meta_namespace="src_bucket.src_bucket.src_bucket")
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_SRC_MB_SAME" in str(e) and "source keyspace same as metadata keyspace" in str(e), True
