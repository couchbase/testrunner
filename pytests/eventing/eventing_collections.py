from couchbase_helper.documentgenerator import SDKDataLoader
from lib.membase.api.rest_client import RestConnection
from lib.membase.helper.rbac_exclusion_helper import verify_rbac_exclusion_syntax
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
        self._verify_rbac_exclusion_eventing()
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

    def _verify_rbac_exclusion_eventing(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        rest = RestConnection(eventing_node)
        bucket_name = self.src_bucket_name
        scope_name = self.src_bucket_name
        allowed_col = self.src_bucket_name
        excluded_col = "rbac_excl_col"
        meta_col = "rbac_excl_meta"
        col_created = False
        meta_created = False
        try:
            self.collection_rest.create_collection(
                bucket=bucket_name, scope=scope_name, collection=excluded_col)
            col_created = True
            self.collection_rest.create_collection(
                bucket=bucket_name, scope=scope_name, collection=meta_col)
            meta_created = True

            def eventing_service_validator(u, p):
                fn_allowed = "rbac_excl_fn_allowed"
                fn_excluded = "rbac_excl_fn_excluded"

                def _make_body(fn_name, src_col):
                    return {
                        "appname": fn_name,
                        "appcode": "function OnUpdate(doc, meta) {}",
                        "depcfg": {
                            "source_bucket": bucket_name,
                            "source_scope": scope_name,
                            "source_collection": src_col,
                            "metadata_bucket": bucket_name,
                            "metadata_scope": scope_name,
                            "metadata_collection": meta_col,
                        },
                        "settings": {
                            "deployment_status": False,
                            "processing_status": False,
                        },
                    }

                # 1. CREATE on allowed collection — should succeed
                fn_allowed_created = False
                try:
                    rest.create_function(
                        fn_allowed, _make_body(fn_allowed, allowed_col),
                        function_scope=None, username=u, password=p)
                    fn_allowed_created = True
                    self.log.info(
                        "Eventing: create function on allowed collection '%s' "
                        "succeeded" % allowed_col)
                except Exception as e:
                    self.fail(
                        "Eventing: create function on allowed collection '%s' "
                        "failed: %s" % (allowed_col, str(e)))

                # 2. DELETE the created function — should succeed
                if fn_allowed_created:
                    try:
                        rest.delete_single_function(
                            fn_allowed, function_scope=None, username=u, password=p)
                        self.log.info(
                            "Eventing: delete function on allowed collection '%s' "
                            "succeeded" % allowed_col)
                    except Exception as e:
                        self.fail(
                            "Eventing: delete function on allowed collection '%s' "
                            "failed: %s" % (allowed_col, str(e)))

                # 3. Try CREATE on excluded collection — should be denied
                fn_excluded_created = False
                try:
                    rest.create_function(
                        fn_excluded, _make_body(fn_excluded, excluded_col),
                        function_scope=None, username=u, password=p)
                    fn_excluded_created = True
                except Exception as e:
                    self.log.info(
                        "Eventing service validation passed: create function on "
                        "excluded collection denied: %s" % str(e))
                if fn_excluded_created:
                    try:
                        rest.delete_single_function(fn_excluded)
                    except Exception:
                        pass
                    self.fail(
                        "Eventing: create function on excluded collection '%s' "
                        "should be denied but succeeded" % excluded_col)

            verify_rbac_exclusion_syntax(
                self, rest, bucket_name, scope_name, allowed_col, excluded_col,
                "eventing", runtype=self.input.param("runtype", "default"),
                service_validator=eventing_service_validator)
        finally:
            if meta_created:
                try:
                    self.collection_rest.delete_collection(
                        bucket_name, scope_name, meta_col)
                except Exception:
                    pass
            if col_created:
                try:
                    self.collection_rest.delete_collection(
                        bucket_name, scope_name, excluded_col)
                except Exception:
                    pass

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
