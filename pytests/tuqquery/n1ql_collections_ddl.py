from collection.collections_cli_client import CollectionsCLI
from collection.collections_n1ql_client import CollectionsN1QL
from collection.collections_rest_client import CollectionsRest
from membase.api.exception import CBQError
from membase.helper.bucket_helper import BucketOperationHelper

from .tuq import QueryTests


class QueryCollectionsDDLTests(QueryTests):
    """
    Tests descriptors.
    Format:
        "test_name":{
            objects hierarchy to be created
            "buckets": [
                {collections_with_same_names_same_scope
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections":[]}
                    ]
                }
            ],
            objects to be tested for successful creation
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1"
                }
            ]
        },
    """
    tests_objects = {
        "single_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1"
                }
            ]
        },
        "multiple_scopes_different_names_same_bucket": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []},
                        {"name": "scope2", "collections": []}
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1"
                },
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope2",
                    "object_container": "bucket1"
                }
            ]
        },
        "multiple_scopes_same_name_different_buckets": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket1"
                },
                {
                    "expected_result": "positive",
                    "object_type": "scope",
                    "object_name": "scope1",
                    "object_container": "bucket2"
                }
            ]
        },
        "single_collection_not_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                }
            ]
        },
        "two_collections_not_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [
                             {"name": "collection1"},
                             {"name": "collection2"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection2",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                }
            ]
        },
        "two_collections_same_name_different_scopes": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         },
                        {"name": "scope2",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope1"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "scope2"
                }
            ]
        },
        "single_collection_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                }
            ]
        },
        "multiple_collections_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"},
                             {"name": "collection2"}
                         ]
                         }
                    ]
                }
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection2",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                },
            ]
        },
        "multiple_collections_same_name_default_scope_different_buckets": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "_default",
                         "collections": [
                             {"name": "collection1"}
                         ]
                         }
                    ]
                },
            ],
            "tests": [
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket1",
                    "object_scope": "_default"
                },
                {
                    "expected_result": "positive",
                    "object_type": "collection",
                    "object_name": "collection1",
                    "object_container": "bucket2",
                    "object_scope": "_default"
                },
            ]
        },
    }

    negative_tests_objects = {
        "same_name_scopes_same_bucket": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create scope bucket1.scope1",
                    "expected_error": "Scope with this name already exists"
                }
            ]
        },
        "scope_in_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1", "collections": []}
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create scope bucket1.scope1.scope2",
                    "expected_error": "syntax error - at ."
                }
            ]
        },
        "scope_in_collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create scope bucket1.scope1.collection1.scope2",
                    "expected_error": "syntax error - at ."
                }
            ]
        },
        "scope_in_missed_keyspace": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": []
                }
            ],
            "test_queries": [
                {
                    "text": "create scope mykeyspace:bucket1.scope1",
                    "expected_error": "syntax error - at :"
                }
            ]
        },
        "collection_in_missed_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": []
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.scope1.collection1",
                    "expected_error": "Scope not found in CB datastore"
                }
            ]
        },
        "collections_with_same_names_same_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.scope1.collection1",
                    "expected_error": "Collection with this name already exists"
                }
            ]
        },
        "collection_missed_bucket_default_scope": {
            "buckets": [
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.collection1",
                    "expected_error": "syntax error - at end of input"
                }
            ]
        },
        "collections_with_same_names_same_default_scope": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.collection1",
                    "expected_error": "syntax error - at end of input"
                }
            ]
        },
        "collection_in_collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection bucket1.scope1.collection1.collection2",
                    "expected_error": "syntax error - at ."
                }
            ]
        },
        "collection_in_missed_keyspace": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "create collection mykeyspace.bucket1.scope1.collection2",
                    "expected_error": "syntax error - at ."
                }
            ]
        },
        "incorrect_collection_drop": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "drop collection collection1",
                    "expected_error": "Invalid path specified: path has wrong number of parts"
                }
            ]
        },
        "incorrect_collection_drop_ver2": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "drop collection scope1.collection1",
                    "expected_error": "syntax error - at end of input"
                }
            ]
        },
        "incorrect_scope_drop": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": []
                         }
                    ]
                }
            ],
            "test_queries": [
                {
                    "text": "drop scope scope1",
                    "expected_error": "syntax error - at end of input"
                }
            ]
        },
    }

    bucket_params = {}

    def setUp(self):
        super(QueryCollectionsDDLTests, self).setUp()
        self.log.info("==============  QueryCollectionsDDLTests setup has started ==============")
        self.log_config_info()
        bucket_type = self.input.param("bucket_type", self.bucket_type)
        self.collections_helper = CollectionsN1QL(self.master)
        eviction_policy = "noEviction" if bucket_type == "ephemeral" else self.eviction_policy
        self.bucket_params = self._create_bucket_params(server=self.master, size=100,
                                                        replicas=self.num_replicas, bucket_type=bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=eviction_policy, lww=self.lww)
        self.rest_client = CollectionsRest(self.master)
        self.cli_client = CollectionsCLI(self.master)
        self.log.info("==============  QueryCollectionsDDLTests setup has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  QueryCollectionsDDLTests suite_setup has started ==============")
        super(QueryCollectionsDDLTests, self).suite_setUp()
        self.log.info("==============  QueryCollectionsDDLTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryCollectionsDDLTests tearDown has started ==============")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        super(QueryCollectionsDDLTests, self).tearDown()
        self.log.info("==============  QueryCollectionsDDLTests tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  QueryCollectionsDDLTests suite_tearDown has started ==============")
        super(QueryCollectionsDDLTests, self).suite_tearDown()
        self.log.info("==============  QueryCollectionsDDLTests suite_tearDown has completed ==============")

    def test_create(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            self.fail("Test name cannot be empty, please fix .conf file")

        test_data = self.tests_objects[test_name]

        test_objects_created, error = \
            self.collections_helper.create_bucket_scope_collection_multi_structure(cluster=self.cluster,
                                                                                   existing_buckets=self.buckets,
                                                                                   bucket_params=self.bucket_params,
                                                                                   data_structure=test_data)
        if not test_objects_created:
            self.assertEquals(True, False, f"Test objects load is failed: {error}")
        result, message = self._perform_test(test_data)
        self.assertEquals(result, True, message)

    def test_scope_name_special_chars(self):
        special_chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '-', '%']
        tick_chars = ['-', '%']
        bucket_name = 'bucket1'
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            scope_name = f"scope{special_char}"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char in tick_chars:
                continue
            scope_name = f"scope{special_char}"
            query = f"create scope {bucket_name}.{scope_name}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char in tick_chars:
                continue
            scope_name = f"scope{special_char}test"
            query = f"create scope {bucket_name}.{scope_name}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char not in tick_chars:
                continue
            tick_char = '`'
            scope_name = f"scope{special_char}test"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name not in bucket_scopes:
                errors.append(f"Cannot create scope {scope_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of scope with special chars is failed.")

    def test_collection_name_special_chars(self):
        special_chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '-', '%']
        tick_chars = ['-', '%']
        bucket_name = 'bucket1'
        scope_name = "_default"
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            collection_name = f"collection{special_char}"
            query = f"create collection {bucket_name}.{scope_name}.{tick_char}{collection_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name not in scope_collections:
                errors.append(f"Cannot create collection {collection_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            if special_char in tick_chars:
                continue
            collection_name = f"collection{special_char}"
            query = f"create collection {bucket_name}.{scope_name}.{collection_name}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name not in scope_collections:
                errors.append(f"Cannot create collection {collection_name} in bucket {bucket_name}")

        self.cluster.bucket_delete(self.master, bucket_name)
        self.wait_for_bucket_delete(bucket_name, 3, 100)
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        for special_char in special_chars:
            tick_char = '`'
            collection_name = f"collection{special_char}test"
            query = f"create collection {bucket_name}.{scope_name}.{tick_char}{collection_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                errors.append(f"Cannot run query: {query} \nError is:{str(err)}")
                continue
            scope_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name not in scope_collections:
                errors.append(f"Cannot create collection {collection_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of collections with special chars is failed.")

    def test_create_negative(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            self.fail("Test name cannot be empty, please fix .conf file")

        test_data = self.negative_tests_objects[test_name]
        test_objects_created, error = \
            self.collections_helper.create_bucket_scope_collection_multi_structure(cluster=self.cluster,
                                                                                   existing_buckets=self.buckets,
                                                                                   bucket_params=self.bucket_params,
                                                                                   data_structure=test_data)
        if not test_objects_created:
            self.assertEquals(True, False, f"Test objects load is failed: {error}")

        test_fails = []

        for test_query in test_data["test_queries"]:
            wrong_object_created = False
            query = test_query['text']
            expected_error = test_query["expected_error"]
            try:
                self.run_cbq_query(test_query["text"])
                wrong_object_created = True
            except CBQError as err:
                err_msg = str(err)
                if expected_error not in err_msg:
                    test_fails.append(f"Unexpected error message found while executing query: {query}"
                                      f"\nFound:{str(err)}"
                                      f"\nExpected message fragment is: {expected_error}")
            if wrong_object_created:
                test_fails.append(f"Unexpected success while executing query: {query}")

        for fail in test_fails:
            self.log.info(fail)
        self.assertEquals(len(test_fails), 0, "See logs for test fails information")

    def test_incorrect_scope_naming_negative(self):
        special_chars = ['_', '%']
        bucket_name = 'bucket1'
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            scope_name = f"{special_char}scope"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name in bucket_scopes:
                errors.append(f"Can create scope {scope_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of scope with incorrect name is successful.")

    def test_create_default_scope(self):
        bucket_name = 'bucket1'
        scope_name = "_default"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        query = f"create scope {bucket_name}.{scope_name}"
        try:
            self.run_cbq_query(query)
        except CBQError as err:
            self.assertEquals(True, True, "Cannot create scope named _default")
            return

        self.assertEquals(True, False, "Creation of scope with name _default is successful.")

    def test_incorrect_scope_naming_not_allowed_symbols_negative(self):
        special_chars = ["~", "!", "#", "$", "^", "&", "*", "(", ")", "-", "+", "=", "{", "[", "}", "]", "|", "\\", ":",
                         ";", "\"", "'", "<", ",", ">", ".", "?", "/"]
        bucket_name = 'bucket1'
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            scope_name = f"scope{special_char}test"
            query = f"create scope {bucket_name}.{tick_char}{scope_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                continue
            bucket_scopes = self.rest.get_bucket_scopes(bucket_name)
            if scope_name in bucket_scopes:
                errors.append(f"Can create scope {scope_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of scope with incorrect name is successful.")

    def test_incorrect_collection_naming_not_allowed_symbols_negative(self):
        special_chars = ["~", "!", "#", "$", "^", "&", "*", "(", ")", "-", "+", "=", "{", "[", "}", "]", "|", "\\", ":",
                         ";", "\"", "'", "<", ",", ">", ".", "?", "/"]
        bucket_name = 'bucket1'
        scope_name = "_default"
        errors = []

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        for special_char in special_chars:
            tick_char = '`'
            collection_name = f"collection{special_char}test"
            query = f"create collection {bucket_name}.{tick_char}{collection_name}{tick_char}"
            try:
                self.run_cbq_query(query)
            except CBQError as err:
                continue
            bucket_collections = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name in bucket_collections:
                errors.append(f"Can create collection {collection_name} in bucket {bucket_name}")

        for error in errors:
            self.log.info(error)
        self.assertEquals(len(errors), 0, "Creation of collection with incorrect name is successful.")

    def test_drop_cli_collection(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        collection_created = self.cli_client.create_scope_collection(bucket=bucket_name, scope=scope_name,
                                                                     collection=collection_name)
        if collection_created:
            self.collections_helper.delete_collection(keyspace=keyspace_name, bucket_name=bucket_name,
                                                      scope_name=scope_name, collection_name=collection_name)

            objects = self.rest.get_scope_collections(bucket_name, scope_name)
            if collection_name in objects:
                self.assertEquals(True, False, "Collection still exists after collection drop.")
        else:
            self.assertEquals(True, False, "Failed to create collection using CLI. Test is failed.")

    def test_drop_rest_collection(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        self.rest_client.create_scope_collection(bucket=bucket_name, scope=scope_name, collection=collection_name)
        self.collections_helper.delete_collection(keyspace=keyspace_name, bucket_name=bucket_name,
                                                  scope_name=scope_name, collection_name=collection_name)

        objects = self.rest.get_scope_collections(bucket_name, scope_name)
        if collection_name in objects:
            self.assertEquals(True, False, "Collection still exists after collection drop.")

    def test_drop_collection(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        # creating all DB objects
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        self.collections_helper.create_scope(bucket_name=bucket_name, scope_name=scope_name)
        self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                  collection_name=collection_name)

        # load document into collection
        try:
            self.run_cbq_query(
                "INSERT INTO " + bucket_name + "." + scope_name + "." + collection_name + "(KEY, VALUE) VALUES ("
                                                                                          "'id1', { '"
                                                                                          "name' : 'name1' })")
            result = self.run_cbq_query(f"select name from {keyspace_name}:{bucket_name}.{scope_name}.{collection_name}"
                                        f" use keys 'id1'")['results'][0]['name']
            self.assertEquals(result, "name1", "Insert and select results do not match!")
        except CBQError as e:
            self.assertEquals(True, False, "Failed to perform insert into collection")
        except KeyError as err:
            self.assertEquals(True, False, "Failed to perform insert into collection")

        # dropping collection
        self.collections_helper.delete_collection(keyspace=keyspace_name, bucket_name=bucket_name,
                                                  scope_name=scope_name, collection_name=collection_name)

        # test that collection is dropped
        objects = self.rest.get_scope_collections(bucket_name, scope_name)
        if collection_name in objects:
            self.assertEquals(True, False, "Collection still exists after collection drop.")

        # test that collection document is deleted
        result = self.run_cbq_query(f"select count(*) as cnt from {bucket_name}")['results'][0]['cnt']
        self.assertEquals(result, 0, "Collection document was not deleted after collection drop")

    def test_drop_cli_scope(self):
        keyspace_name = "default"
        bucket_name = "bucket1"
        scope_name = "scope1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        scope_created = self.cli_client.create_scope(bucket=bucket_name, scope=scope_name)
        if scope_created:
            self.collections_helper.delete_scope(keyspace=keyspace_name, bucket_name=bucket_name, scope_name=scope_name)

            objects = self.rest.get_bucket_scopes(bucket_name)
            if scope_name in objects:
                self.assertEquals(True, False, "Scope still exists after scope drop.")
        else:
            self.assertEquals(True, False, "Failed to create scope using CLI. Test is failed.")

    def test_drop_rest_scope(self):
        keyspace_name = "default"
        bucket_name = "bucket1"
        scope_name = "scope1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        self.rest_client.create_scope(bucket=bucket_name, scope=scope_name)

        self.collections_helper.delete_scope(keyspace=keyspace_name, bucket_name=bucket_name, scope_name=scope_name)

        objects = self.rest.get_bucket_scopes(bucket_name)
        if scope_name in objects:
            self.assertEquals(True, False, "Scope still exists after scope drop.")

    def test_drop_scope(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        keyspace_name = "default"

        # creating all DB objects
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        self.collections_helper.create_scope(bucket_name=bucket_name, scope_name=scope_name)
        self.collections_helper.create_collection(bucket_name=bucket_name, scope_name=scope_name,
                                                  collection_name=collection_name)

        # load document into collection
        try:
            self.run_cbq_query(
                "INSERT INTO " + bucket_name + "." + scope_name + "." + collection_name +
                " (KEY, VALUE) VALUES ('id1', { 'name' : 'name1' })")
            result = self.run_cbq_query(
                f"select name from {keyspace_name}:{bucket_name}.{scope_name}.{collection_name} use keys 'id1'")[
                'results'][0]['name']
            self.assertEquals(result, "name1", "Insert and select results do not match!")
        except CBQError as e:
            self.assertEquals(True, False, "Failed to perform insert into collection")
        except KeyError as err:
            self.assertEquals(True, False, "Failed to perform insert into collection")

        # dropping scope
        self.collections_helper.delete_scope(keyspace=keyspace_name, bucket_name=bucket_name, scope_name=scope_name)

        # check that collection is dropped
        objects = self.rest.get_scope_collections(bucket_name, scope_name)
        if collection_name in objects:
            self.assertEquals(True, False, "Collection still exists after scope drop.")

        # check that scope is dropped
        objects = self.rest.get_bucket_scopes(bucket_name)
        if scope_name in objects:
            self.assertEquals(True, False, "Scope still exists after scope drop.")

        # check that collection document is dropped
        result = self.run_cbq_query(f"select count(*) as cnt from {bucket_name}")['results'][0]['cnt']
        self.assertEquals(result, 0, "Collection document was not deleted after scope drop")

    def _perform_test(self, data_structure=None):
        if data_structure is None:
            raise Exception("Empty value for data_structure parameter")
        tests = data_structure["tests"]
        for test in tests:
            object_type = test["object_type"]
            object_name = test["object_name"]
            object_bucket = test["object_container"]
            if test["object_type"] == "scope":
                objects = self.rest.get_bucket_scopes(object_bucket)
            else:
                objects = self.rest.get_scope_collections(object_bucket, test["object_scope"])

            if not test["object_name"] in objects:
                return False, f"{object_type} {object_name} is not found in bucket {object_bucket}. Test is failed"
        return True, ""

    def _create_bucket(self, bucket_name):
        bucket_params = self._create_bucket_params(server=self.master, size=100,
                                                   replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                   enable_replica_index=self.enable_replica_index,
                                                   eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(bucket_name, 11222, bucket_params)
