from .tuq import QueryTests
from membase.api.exception import CBQError
from membase.helper.bucket_helper import BucketOperationHelper
from collection.collections_n1ql_client import CollectionsN1QL


class QueryCollectionsEnd2EndTests(QueryTests):

    tests_objects = {
        "e2e_1bucket_default_scope_1collection": {
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
            "tests": ["bucket1._default.collection1"]
        },
        "e2e_1bucket_1scope_1collection": {
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
            "tests": ["bucket1.scope1.collection1"]
        },
        "e2e_1bucket_1scope_2collections": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}, {"name": "collection2"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1.scope1.collection1", "bucket1.scope1.collection2"]
        },
        "e2e_1bucket_2scopes_1collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         },
                        {"name": "scope2",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1.scope1.collection1", "bucket1.scope2.collection1"]
        },
        "e2e_1bucket_2scopes_2collections": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         },
                        {"name": "scope2",
                         "collections": [{"name": "collection2"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1.scope1.collection1", "bucket1.scope2.collection2"]
        },
        "e2e_2buckets_default_scopes_1collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "_default",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1._default.collection1", "bucket2._default.collection1"]
        },
        "e2e_2buckets_default_scopes_2collections": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "_default",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "_default",
                         "collections": [{"name": "collection2"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1._default.collection1", "bucket2._default.collection2"]
        },
        "e2e_2buckets_1scope_1collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1.scope1.collection1", "bucket2.scope1.collection1"]
        },
        "e2e_2buckets_1scope_2collections": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection2"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1.scope1.collection1", "bucket2.scope1.collection2"]
        },
        "e2e_2buckets_2scopes_1collection": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "scope2",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1.scope1.collection1", "bucket2.scope2.collection1"]
        },
        "e2e_2buckets_2scopes_2collections": {
            "buckets": [
                {
                    "name": "bucket1",
                    "scopes": [
                        {"name": "scope1",
                         "collections": [{"name": "collection1"}]
                         }
                    ]
                },
                {
                    "name": "bucket2",
                    "scopes": [
                        {"name": "scope2",
                         "collections": [{"name": "collection2"}]
                         }
                    ]
                }
            ],
            "tests": ["bucket1.scope1.collection1", "bucket2.scope2.collection2"]
        },
    }

    bucket_params = {}

    def setUp(self):
        super(QueryCollectionsEnd2EndTests, self).setUp()
        self.log.info("==============  QueryCollectionsEnd2EndTests setup has started ==============")
        self.log_config_info()
        self.collections_helper = CollectionsN1QL(self.master)
        self.bucket_params = self._create_bucket_params(server=self.master, size=100,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.log.info("==============  QueryCollectionsEnd2EndTests setup has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  QueryCollectionsEnd2EndTests suite_setup has started ==============")
        super(QueryCollectionsEnd2EndTests, self).suite_setUp()
        self.log.info("==============  QueryCollectionsEnd2EndTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryCollectionsEnd2EndTests tearDown has started ==============")
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        super(QueryCollectionsEnd2EndTests, self).tearDown()
        self.log.info("==============  QueryCollectionsEnd2EndTests tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  QueryCollectionsEnd2EndTests suite_tearDown has started ==============")
        super(QueryCollectionsEnd2EndTests, self).suite_tearDown()
        self.log.info("==============  QueryCollectionsEnd2EndTests suite_tearDown has completed ==============")

    def test_end_to_end(self):
        test_name = self.input.param("test_name", '')
        if not test_name:
            self.fail("Test name cannot be empty, please fix .conf file")
        test_data = self.tests_objects[test_name]
        if not test_data or test_data == {}:
            raise ValueError(f"Test name {test_name} is incorrect, please check .conf file.")
        test_objects_created, error = \
            self.collections_helper.create_bucket_scope_collection_multi_structure_cli(cluster=self.cluster,
                                                                                   existing_buckets=self.buckets,
                                                                                   bucket_params=self.bucket_params,
                                                                                   data_structure=test_data)
        self.assertTrue(test_objects_created, f"Test objects load is failed: {error}")
        result, errors = self._perform_end_to_end_test(test_data=test_data)

        if len(errors) > 0:
            self.log.info("=" * 20 + " Overall results " + "=" * 20)
            for error in errors:
                self.log.info(error["message"])
        self.assertTrue(result)

    def _perform_end_to_end_test(self, test_data=None):
        sanity_test = self.input.param("sanity_test", False)

        if not test_data:
            test_data = {}
        namespace = "default"
        errors = []
        tests = test_data["tests"]

        # Allow time for scope/collection metadata changes for scopes/collections to propagate before using the new scope/collections
        self.sleep(5)

        for keyspace in tests:
            keyspace = namespace+':'+keyspace
            _, _, collection_name = self._extract_object_names(full_keyspace_name=keyspace)

            # initial insert. Expected result - success
            try:
                query = "insert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id', {'val':1, 'name' : '"+keyspace+"_name' })"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "insert_no_index",
                                   "message": "Initial insert is failed, cannot continue tests, aborting test suite1."})
                    return False, errors
                query = "select * from "+keyspace+" use keys ['"+keyspace+"_id']"
                result = self.run_cbq_query(query)
                self.assertEquals(result['results'][0][collection_name]['name'], keyspace+'_name', "Wrong insert results!")

                query = "insert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id100', {'val':2, 'name' : '"+keyspace+"_name' })"
                self.run_cbq_query(query)
                query = "insert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id200', {'val':3, 'name' : '"+keyspace+"_name' })"
                self.run_cbq_query(query)

            except CBQError as e:
                errors.append({"reason": "insert_no_index",
                               "message": "Initial insert is failed, cannot continue tests, aborting test suite2."})
                return False, errors

        if not sanity_test:
            for keyspace in tests:
                keyspace = namespace+':'+keyspace
                _, _, collection_name = self._extract_object_names(full_keyspace_name=keyspace)

                # select upon not indexed collection. Expected result - fail
                try:
                    query = "select name from "+keyspace+" where val=1"
                    result = self.run_cbq_query(query)
                    if result['status'] == 'success':
                        errors.append({"reason": "select_no_index", "message": "Select upon unindexed collection was unexpectedly successful."})
                except CBQError as e:
                    pass

                # update upon not indexed collection. Expected result - fail
                try:
                    query = "update "+keyspace+" set name=name||'_updated' where val=1"
                    result = self.run_cbq_query(query)['status']
                    if result == 'success':
                        errors.append({"reason": "update_no_index", "message": "Update upon unindexed collection was unexpectedly successful."})
                except CBQError as e:
                    pass

                # upsert as update upon not indexed collection. Expected result - success
                try:
                    query = "upsert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id', {'val':1, 'name' : '"+keyspace+"_name_updated' })"
                    result = self.run_cbq_query(query)['status']
                    if result != 'success':
                        errors.append({"reason": "upsert(update)_no_index", "message": "Upsert(update) upon unindexed collection was unsuccessful."})
                    query = "select * from "+keyspace+" use keys ['"+keyspace+"_id']"
                    result = self.run_cbq_query(query)
                    if result['results'][0][collection_name]['name'] != keyspace+'_name_updated':
                        errors.append({"reason": "upsert(update)_no_index",
                                       "message": "Upsert(update) upon unindexed collection produced incorrect result."})
                except CBQError as e:
                    pass

                # upsert as insert upon not indexed collection. Expected result - success - remove
                try:
                    query = "upsert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id1', {'val':10, 'name' : '"+keyspace+"_name2' })"
                    result = self.run_cbq_query(query)['status']
                    if result != 'success':
                        errors.append({"reason": "upsert(insert)_no_index", "message": "Upsert(insert) upon unindexed collection was unexpectedly successful."})
                    query = "select * from "+keyspace+" use keys ['"+keyspace+"_id1']"
                    result = self.run_cbq_query(query)
                    if result['results'][0][collection_name]['name'] != keyspace+'_name2':
                        errors.append({"reason": "upsert(update)_no_index",
                                       "message": "Upsert(update) upon unindexed collection produced incorrect result."})
                except CBQError as e:
                    pass

                # merge as insert upon not indexed collection. Expected result - fail 0 remove
                try:
                    query = "MERGE INTO "+keyspace+" AS target " \
                                           "USING [ {'name':'"+keyspace+"_name2', 'val': 10} ] AS source " \
                                           "ON target.val = source.val " \
                                           "WHEN MATCHED THEN " \
                                           "UPDATE SET target.name = source.name " \
                                           "WHEN NOT MATCHED THEN " \
                                           "INSERT (KEY UUID(), VALUE {'name': source.name, 'val': source.val})"
                    result = self.run_cbq_query(query)['status']
                    if result == 'success':
                        errors.append({"reason": "merge(insert)_no_index", "message": "Merge(insert) upon unindexed collection was unexpectedly successful."})
                except CBQError as e:
                    pass

                # merge as update upon not indexed collection. Expected result - fail - remobe
                try:
                    query = "MERGE INTO "+keyspace+" AS target " \
                                           "USING [ {'name':'"+keyspace+"_name2', 'val': 1} ] AS source " \
                                           "ON target.val = source.val " \
                                           "WHEN MATCHED THEN " \
                                           "UPDATE SET target.name = source.name " \
                                           "WHEN NOT MATCHED THEN " \
                                           "INSERT (KEY UUID(), VALUE {'name': source.name, 'val': source.val})"
                    result = self.run_cbq_query(query)['status']
                    if result == 'success':
                        errors.append({"reason": "merge(update)_no_index", "message": "Merge(update) upon unindexed collection was unexpectedly successful."})
                except CBQError as e:
                    pass

                # delete upon not indexed collection. Expected result - fail
                try:
                    query = "delete from "+keyspace+" where val='1'"
                    result = self.run_cbq_query(query)['status']
                    if result == 'success':
                        errors.append({"reason": "delete_no_index", "message": "Delete upon unindexed collection was unexpectedly successful."})
                except CBQError as e:
                    pass

        for keyspace in tests:
            keyspace = namespace+':'+keyspace
            bucket_name, scope_name, collection_name = self._extract_object_names(full_keyspace_name=keyspace)

            # Create primary index on collection. Expected result - success
            try:
                query = "create primary index on "+keyspace
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "create_primary_index", "message": "Creation of primary index is failed."})
                query = "select count(*) from system:all_indexes where bucket_id='"+bucket_name+"' and scope_id='"+scope_name+"' and keyspace_id='"+collection_name+"'"
                result = self.run_cbq_query(query)
                if result['results'][0]['$1'] != 1:
                    errors.append({"reason": "create_primary_index", "message": "Primary index for collection is not presented in system:all_indexes."})
            except CBQError as e:
                errors.append({"reason": "create_primary_index", "message": "Creation of primary index is failed."})
                pass

        for keyspace in tests:
            keyspace = namespace+':'+keyspace
            _, _, collection_name = self._extract_object_names(full_keyspace_name=keyspace)

            # select upon collection with primary index. Expected result - success.
            try:
                query = "select name from "+keyspace+" where val=2"
                result = self.run_cbq_query(query)
                if result['status'] != 'success':
                    errors.append({"reason": "select_primary_index", "message": "Select upon primary indexed collection is failed."})
                if result['results'][0]['name'] != keyspace+'_name':
                    errors.append({"reason": "select_primary_index",
                                    "message": "Select upon primary indexed collection returned wrong result."})
            except CBQError as e:
                errors.append(
                    {"reason": "select_primary_index", "message": "Select upon primary indexed collection is failed."})
                pass

            # update upon collection with primary index. Expected result - success.
            try:
                query = "update "+keyspace+" set name=name||'_updated' where val=2"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "update_primary_index", "message": "Update upon primary indexed collection is failed."})
                query = "select name from "+keyspace+" where val=2"
                result = self.run_cbq_query(query)
                if result['results'][0]['name'] != keyspace+'_name_updated':
                    errors.append({"reason": "select_primary_index",
                                    "message": "Update upon primary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append(
                    {"reason": "update_primary_index", "message": "Update upon primary indexed collection is failed."})
            pass

        # upsert as update upon collection with primary index. Expected result - success.
            try:
                query = "upsert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id', {'val':2, 'name' : '"+keyspace+"_name' })"
                result = self.run_cbq_query(query)
                if result['status'] != 'success':
                    errors.append({"reason": "upsert(update)_primary_index", "message": "Upsert(update) upon primary indexed collection is failed."})
                query = "select * from " + keyspace + " where meta().id='"+keyspace+"_id'"
                result = self.run_cbq_query(query)
                if result['results'][0][collection_name]['name'] != keyspace+"_name":
                    errors.append({"reason": "upsert(update)_primary_index",
                                   "message": "Upsert(update) upon primary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "upsert(update)_primary_index",
                               "message": "Upsert(update) upon primary indexed collection is failed."})
                pass

            # upsert as insert upon collection with primary index. Expected result - success.
            try:
                query = "upsert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id2', {'val':2, 'name' : '"+keyspace+"_name2' })"
                result = self.run_cbq_query(query)
                if result['status'] != 'success':
                    errors.append({"reason": "upsert(insert)_primary_index", "message": "Upsert(insert) upon primary indexed collection is failed."})
                query = "select * from " + keyspace + " where meta().id='"+keyspace+"_id2'"
                result = self.run_cbq_query(query)
                if result['results'][0][collection_name]['name'] != keyspace+"_name2":
                    errors.append({"reason": "upsert(insert)_primary_index",
                                   "message": "Upsert(insert) upon primary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "upsert(insert)_primary_index",
                               "message": "Upsert(insert) upon primary indexed collection is failed."})
                pass

        for keyspace in tests:
            keyspace = namespace+':'+keyspace
            bucket_name, scope_name, collection_name = self._extract_object_names(full_keyspace_name=keyspace)

            # drop collection primary index. Expected result - success.
            try:
                query = "drop primary index on "+keyspace
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "drop_primary_index", "message": "Drop of primary index for collection is failed."})
                query = "select count(*) from system:all_indexes where bucket_id='"+bucket_name+"' and scope_id='"+scope_name+"' and keyspace_id='"+collection_name+"'"
                result = self.run_cbq_query(query)
                if result['results'][0]['$1'] != 0:
                    errors.append({"reason": "drop_primary_index", "message": "Primary index for collection is presented in system:all_indexes after drop."})
            except CBQError as e:
                errors.append(
                    {"reason": "drop_primary_index", "message": "Drop of primary index for collection is failed."})
                pass

        for keyspace in tests:
            keyspace = namespace+':'+keyspace
            bucket_name, scope_name, collection_name = self._extract_object_names(full_keyspace_name=keyspace)

            # create collection secondary index. Expected result - success.
            try:
                query = "create index idx_val on "+keyspace+"(val)"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "create_secondary_index", "message": "Create of secondary index for collection is failed."})
                query = "select count(*) from system:all_indexes where bucket_id='"+bucket_name+"' and scope_id='"+scope_name+"' and keyspace_id='"+collection_name+"' and name='idx_val'"
                result = self.run_cbq_query(query)
                if result['results'][0]['$1'] != 1:
                    errors.append({"reason": "create_secondary_index",
                                   "message": "Secondary index for collection is created but not presented in system:indexes."})
            except CBQError as e:
                errors.append({"reason": "create_secondary_index",
                               "message": "Create of secondary index for collection is failed."})
                pass

        for keyspace in tests:
            keyspace = namespace+':'+keyspace

            # select upon collection with secondary index. Expected result - success.
            try:
                query = "select name from "+keyspace+" where val=3"
                result = self.run_cbq_query(query)
                if result['status'] != 'success':
                    errors.append({"reason": "select_secondary_index", "message": "Select upon secondary indexed collection is failed."})
                if result['results'][0]['name'] != keyspace+'_name':
                    errors.append({"reason": "select_primary_index",
                                   "message": "Select upon primary indexed collection returned wrong result."})
            except CBQError as e:
                errors.append({"reason": "select_secondary_index",
                               "message": "Select upon secondary indexed collection is failed."})
                pass

            # update upon collection with secondary index. Expected result - success.
            try:
                query = "update "+keyspace+" set name=name||'_updated' where val=3"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "update_secondary_index", "message": "Update upon secondary indexed collection is failed."})
                query = "select name from "+keyspace+" where val=3"
                result = self.run_cbq_query(query)
                if result['results'][0]['name'] != keyspace+'_name_updated':
                    errors.append({"reason": "update_secondary_index",
                                   "message": "Update upon secondary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "update_secondary_index",
                               "message": "Update upon secondary indexed collection is failed."})
                pass

            # upsert as update upon collection with secondary index. Expected result - success.
            try:
                query = "upsert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id', {'val':3, 'name' : '"+keyspace+"_name_updated_x3' })"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "upsert(update)_secondary_index", "message": "Upsert(update) upon secondary indexed collection is failed."})
                query = "select name from " + keyspace + " where val=3"
                result = self.run_cbq_query(query)
                if result['results'][0]['name'] != keyspace + '_name_updated_x3':
                    errors.append({"reason": "upsert(update)_secondary_index",
                                   "message": "Upsert(update) upon secondary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "upsert(update)_secondary_index",
                               "message": "Upsert(update) upon secondary indexed collection is failed."})
                pass

            # upsert as insert upon collection with secondary index. Expected result - success.
            try:
                query = "upsert into "+keyspace+" (KEY, VALUE) VALUES ('"+keyspace+"_id3', {'val':33, 'name' : '"+keyspace+"_name3' })"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "upsert(insert)_secondary_index", "message": "Upsert(insert) upon secondary indexed collection is failed."})
                query = "select name from " + keyspace + " where val=33"
                result = self.run_cbq_query(query)
                if result['results'][0]['name'] != keyspace + '_name3':
                    errors.append({"reason": "upsert(insert)_secondary_index",
                                   "message": "Upsert(insert) upon secondary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "upsert(insert)_secondary_index",
                               "message": "Upsert(insert) upon secondary indexed collection is failed."})
                pass

            # merge as insert upon collection with secondary index. Expected result - success.
            try:
                query = "MERGE INTO "+keyspace+" AS target " \
                                       "USING [ {'name':'"+keyspace+"_name4', 'val': 4} ] AS source " \
                                       "ON target.val = source.val " \
                                       "WHEN MATCHED THEN " \
                                       "UPDATE SET target.name = source.name " \
                                       "WHEN NOT MATCHED THEN " \
                                       "INSERT (KEY UUID(), VALUE {'name': source.name, 'val': source.val})"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "merge(insert)_secondary_index", "message": "Merge(insert) upon secondary indexed collection is failed."})
                query = "select name from " + keyspace + " where val=4"
                result = self.run_cbq_query(query)
                if result['results'][0]['name'] != keyspace + '_name4':
                    errors.append({"reason": "merge(insert)_secondary_index",
                                   "message": "Merge(insert) upon secondary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "merge(insert)_secondary_index",
                               "message": "Merge(insert) upon secondary indexed collection is failed."})
                pass

            # merge as update upon collection with secondary index. Expected result - success.
            try:
                query = "MERGE INTO "+keyspace+" AS target " \
                                       "USING [ {'name':'"+keyspace+"_merged_name', 'val': 3} ] AS source " \
                                       "ON target.val = source.val " \
                                       "WHEN MATCHED THEN " \
                                       "UPDATE SET target.name = source.name " \
                                       "WHEN NOT MATCHED THEN " \
                                       "INSERT (KEY UUID(), VALUE {'name': source.name, 'val': source.val})"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "merge(update)_secondary_index", "message": "Merge(update) upon secondary indexed collection is failed."})
                query = "select name from " + keyspace + " where val=3"
                result = self.run_cbq_query(query)
                if result['results'][0]['name'] != keyspace + '_merged_name':
                    errors.append({"reason": "merge(update)_secondary_index",
                                   "message": "Merge(update) upon secondary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "merge(update)_secondary_index",
                               "message": "Merge(update) upon secondary indexed collection is failed."})
                pass

            # delete upon collection with secondary index. Expected result - success.
            try:
                query = "delete from "+keyspace+" where val=3"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "delete_secondary_index", "message": "Delete upon secondary indexed collection is failed."})
                query = "select count(*) from " + keyspace + " where val=3"
                result = self.run_cbq_query(query)
                if result['results'][0]['$1'] != 0:
                    errors.append({"reason": "delete_secondary_index",
                                   "message": "Delete upon secondary indexed collection produced wrong result."})
            except CBQError as e:
                errors.append({"reason": "delete_secondary_index",
                               "message": "Delete upon secondary indexed collection is failed."})
                pass

        for keyspace in tests:
            keyspace = namespace+':'+keyspace
            bucket_name, scope_name, collection_name = self._extract_object_names(full_keyspace_name=keyspace)

            # alter secondary index. Expected result - success.
            try:
                query = "ALTER INDEX "+keyspace+".idx_val WITH {'action': 'replica_count', 'num_replica': 1}"

                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "alter_secondary_index", "message": "Alter of secondary index for collection is failed."})
            except CBQError as e:
                errors.append({"reason": "alter_secondary_index",
                               "message": "Alter of secondary index for collection is failed."})
                pass

            # build secondary index. Expected result - success.
            try:
                query = "BUILD INDEX ON "+keyspace+"(idx_val)"
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "build_secondary_index", "message": "Build of secondary index for collection is failed."})
            except CBQError as e:
                errors.append({"reason": "build_secondary_index",
                               "message": "Build of secondary index for collection is failed."})
                pass

            # infer collection. Expected result - success.
            if not sanity_test:
                try:
                    query = "infer "+keyspace
                    result = self.run_cbq_query(query)['status']
                    if result != 'success':
                        errors.append({"reason": "infer", "message": "Infer of collection is failed."})
                except CBQError as e:
                    errors.append({"reason": "infer", "message": "Infer of collection is failed."})
                    pass

                # update statistics for collection. Expected result - success.
                try:
                    query = "UPDATE STATISTICS for "+keyspace+"(val)"
                    result = self.run_cbq_query(query)['status']
                    if result != 'success':
                        errors.append({"reason": "update_statistics", "message": "Update statistics for collection is failed."})
                except CBQError as e:
                    errors.append({"reason": "update_statistics", "message": "Update statistics for collection is failed."})
                    pass

            # drop secondary index for collection. Expected result - success.
            try:
                query = "drop index idx_val on "+keyspace
                result = self.run_cbq_query(query)['status']
                if result != 'success':
                    errors.append({"reason": "drop_secondary_index", "message": "Drop secondary index for collection is failed."})
                query = "select count(*) from system:all_indexes where bucket_id='"+bucket_name+"' and scope_id='"+scope_name+"' and keyspace_id='"+collection_name+"' and name='idx_val'"
                result = self.run_cbq_query(query)
                if result['results'][0]['$1'] != 0:
                    errors.append(
                        {"reason": "drop_secondary_index", "message": "Secondary index for collection is presented in system:all_indexes after drop."})
            except CBQError as e:
                errors.append(
                    {"reason": "drop_secondary_index", "message": "Drop secondary index for collection is failed."})
                pass
        return len(errors) == 0, errors

    def _extract_object_names(self, full_keyspace_name=None):
        bucket_name = full_keyspace_name[full_keyspace_name.find(":") + 1:full_keyspace_name.find(".")]
        scope_name = full_keyspace_name[full_keyspace_name.find(".") + 1:full_keyspace_name.rfind(".")]
        collection_name = full_keyspace_name[full_keyspace_name.rfind(".") + 1:]
        return bucket_name, scope_name, collection_name
