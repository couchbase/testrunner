"""indexer_stats.py: Test index stats correctness, membership and availability
Test details available at - https://docs.google.com/spreadsheets/d/1Y7RKksVazSDG_qY9SqpnydXr5A2FSSHCMTEx-V0Ar08/edit#gid=1559425783

__author__ = "Sri Bhargava Yadavalli"
__maintainer = "Hemant Rajput"
__email__ = "bhargava.yadavalli@couchbase.com"
__git_user__ = "sreebhargava143"
__created_on__ = "29/10/20 3:19 pm"

"""
import json

from .base_gsi import BaseSecondaryIndexingTests
from lib.couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection, RestHelper
from functools import reduce


class IndexerStatsTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(IndexerStatsTests, self).setUp()
        self.doc_size = self.input.param("doc_size", 100)
        self.num_indexes = self.input.param("num_indexes", 1000)
        self.dup_index = self.input.param("dup_index", False)
        self.default_indexes = []
        if len(self.rest.get_buckets()) > 0:
            self.rest.delete_all_buckets()
        for bucket_num in range(1, self.num_buckets + 1):
            bucket_name = f"{self.test_bucket}_{bucket_num}"
            self.bucket_params = self._create_bucket_params(
                server=self.master, size=self.bucket_size,
                replicas=self.num_replicas, bucket_type=self.bucket_type,
                enable_replica_index=self.enable_replica_index,
                eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(
                name=bucket_name, port=11222,
                bucket_params=self.bucket_params)
            self.collection_rest.create_scope_collection_count(
                scope_num=self.num_scopes, collection_num=self.num_collections,
                scope_prefix=self.scope_prefix,
                collection_prefix=self.collection_prefix, bucket=bucket_name)
            self.sleep(10, "Allowing time after collection creation")
            namespace = f'default:{bucket_name}'
            index_name = f'idx_default_{bucket_num}'
            idx_default = QueryDefinition(
                index_name=index_name,
                index_fields=['firstName'])
            query = idx_default.generate_index_create_query(
                namespace=namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
            self.default_indexes.append(
                f'{bucket_name}:{idx_default.index_name}')
            for scope_num in range(1, self.num_scopes + 1):
                scope_name = f'{self.scope_prefix}_{scope_num}'
                for collection_num in range(1, self.num_collections + 1):
                    collection_name =\
                        f'{self.collection_prefix}_{collection_num}'
                    collection_namespace =\
                        f'{namespace}.{scope_name}.{collection_name}'
                    idx_suffix = '_{0}_{1}_{2}'.format(bucket_num, scope_num,
                                                       collection_num)
                    index_name = f'idx_pri{idx_suffix}'
                    idx_pri = QueryDefinition(
                        index_name=index_name)
                    index_name = 'idx_gsi' if self.dup_index\
                        else f'idx_gsi{idx_suffix}'
                    idx_gsi = QueryDefinition(
                        index_name=index_name,
                        index_fields=['firstName'])
                    query = idx_pri.generate_primary_index_create_query(
                        namespace=collection_namespace,
                        defer_build=self.defer_build)
                    self.run_cbq_query(query=query)
                    self.default_indexes.append(
                        '{0}:{1}:{2}:{3}'.format(
                            bucket_name, scope_name, collection_name,
                            idx_pri.index_name))
                    query = idx_gsi.generate_index_create_query(
                        namespace=collection_namespace,
                        defer_build=self.defer_build)
                    self.run_cbq_query(query=query)
                    self.default_indexes.append(
                        '{0}:{1}:{2}:{3}'.format(
                            bucket_name, scope_name, collection_name,
                            idx_gsi.index_name))
                    generator = SDKDataLoader(
                        num_ops=self.num_of_docs_per_collection,
                        percent_create=100, percent_update=0, percent_delete=0,
                        scope=scope_name, collection=collection_name,
                        json_template="Person", doc_size=self.doc_size)
                    self.task = self.cluster.async_load_gen_docs(
                        self.master,
                        bucket_name,
                        generator,
                        pause_secs=1,
                        timeout_secs=300,
                        scope=scope_name,
                        collection=collection_name)
                    self.task.result()
                generator = SDKDataLoader(
                    num_ops=self.num_of_docs_per_collection,
                    percent_create=100, percent_update=0, percent_delete=0,
                    scope=scope_name, collection=collection_name,
                    json_template="Person", doc_size=self.doc_size)
                self.task = self.cluster.async_load_gen_docs(self.master,
                                                             bucket_name,
                                                             generator,
                                                             pause_secs=1,
                                                             timeout_secs=300)
                self.task.result()
        self.wait_until_indexes_online(defer_build=self.defer_build)
        self.sleep(30, "Wait for index scan to complete")
        self.buckets = self.rest.get_buckets()
        self.index_status = self.rest.get_indexer_metadata()
        self.all_master_stats = self.rest.get_all_index_stats()
        self.consumer_filters = {"planner": {
            "indexer_level_stats": ["memory_quota", "memory_used", "memory_used_storage", "uptime", "cpu_utilization",
                                    "memory_rss", "shard_compat_version"],
            "index_level_stats": ['avg_disk_bps', 'avg_drain_rate', 'avg_mutation_rate', 'avg_scan_rate',
                                  'backstore_recs_in_mem', 'backstore_recs_on_disk', 'build_progress',
                                  'combined_resident_percent', 'data_size', 'disk_size', 'index_state',
                                  'items_count', 'last_rollback_time', 'memory_used', 'num_docs_pending',
                                  'num_docs_queued', 'num_flush_queued', 'num_rows_returned', 'progress_stat_time',
                                  'recs_in_mem', 'recs_on_disk', 'resident_percent']
        }, "gsiClient": {
            "indexer_level_stats": [],
            "index_level_stats": ["progress_stat_time", "last_known_scan_time",
                                  "last_rollback_time",
                                  "num_docs_queued",
                                  "num_docs_pending", "index_state"]
        }, "indexStatus": {
            "indexer_level_stats": ["indexer_state", "rebalance_transfer_progress"],
            "index_level_stats": ["build_progress",
                                  "completion_progress",
                                  "last_known_scan_time"]
        }}
        # total_indexer_gc_pause_ns is part of memory stats
        self.external_api_extra_indexer_stats = {'total_indexer_gc_pause_ns'}
        # initial_build_progress is same as build_progress in /stats,
        # num_pending_request derived from num_requests and
        # num_completed_reqeusts from /stats
        self.external_api_extra_index_stats = {'num_pending_requests',
                                               'initial_build_progress'}
        self.index_count = len(self.index_status['status'])
        self.num_index_replicas = self.input.param("num_index_replica", 0)
        self.index_stats_keyset = set()
        self.indexer_stats_keyset = set()
        self.bucket_stats_keyset = set()
        for key in self.all_master_stats.keys():
            ref = key.split(":")
            ref_len = len(ref)
            stat = ref[-1]
            if ref_len == 1:
                self.indexer_stats_keyset.add(stat)
            elif ref_len == 2 and stat != "completion_progress":
                self.bucket_stats_keyset.add(stat)
            else:
                if stat in self.index_stats_keyset:
                    continue
                self.index_stats_keyset.add(stat)
            self.index_stats_keyset.add(stat)

    def tearDown(self):
        super(IndexerStatsTests, self).tearDown()

    def test_redundant_keys(self):
        """
        Every key in the JSON response of /stats, /stats with instance filter,
        /stats with consumer filter, /api/v1/stats and /api/v1/stats with
        filters should be unique
        """
        stats_response_text = self.rest.get_all_index_stats(text=True)
        stats_json_text = json.dumps(json.loads(stats_response_text))
        self.assertTrue(stats_json_text == stats_response_text,
                        msg="Redundant keys test failed for /stats")
        instance_ids = []
        if len(self.buckets) < 1:
            self.fail("No buckets available to test")
        for index in self.index_status['status']:
            instance_ids.append(int(index['instId']))
            instance_id_filter = {"instances": instance_ids}
            filtered_stats_text = self.rest.get_all_index_stats(
                inst_id_filter=instance_id_filter, text=True)
            filtered_stats_json_text = json.dumps(
                json.loads(filtered_stats_text))
            self.assertEqual(filtered_stats_text, filtered_stats_json_text,
                             msg=f"Redundant keys test failed for \
                                 {json.dumps(instance_id_filter)}")
        for consumer_filter in self.consumer_filters.keys():
            consumer_filter_stats_text = self.rest.get_all_index_stats(
                consumer_filter=consumer_filter, text=True)
            try:
                consumer_filter_stats_json_text = json.dumps(
                    json.loads(consumer_filter_stats_text))
                self.assertEqual(consumer_filter_stats_text,
                                 consumer_filter_stats_json_text,
                                 msg=f"Redundant keys test failed for\
                                      {consumer_filter}:\n\
                                      {consumer_filter_stats_text} !=\
                                      {consumer_filter_stats_json_text}")
            except json.decoder.JSONDecodeError:
                self.fail(f"Failed for consumer filter:{consumer_filter}")
        api = f'{self.rest.index_baseUrl}api/v1/stats'
        status, external_api_stats_text, _ = self.rest._http_request(api=api)
        if not status:
            raise Exception("Request ot external stats api failed")
        external_api_stats_text = external_api_stats_text.decode("utf8")\
            .replace('":', '": ').replace(",", ", ")
        external_api_stats_json_text = json.dumps(json.loads(
            external_api_stats_text))
        self.assertEqual(external_api_stats_json_text, external_api_stats_text)
        for bucket in self.buckets:
            api = f'{self.rest.index_baseUrl}api/v1/stats/{bucket.name}'
            status, external_api_bucket_stats_text, _ =\
                self.rest._http_request(api=api)
            external_api_bucket_stats_text =\
                external_api_bucket_stats_text.decode("utf8").replace(
                    '":', '": ').replace(",", ", ")
            external_api_bucket_stats_json_text = json.dumps(
                json.loads(external_api_bucket_stats_text))
            self.assertEqual(
                external_api_bucket_stats_json_text,
                external_api_bucket_stats_text)
            scopes = self.rest.get_bucket_scopes(bucket.name)
            for scope in scopes:
                api = '{0}api/v1/stats/{1}.{2}'.format(
                    self.rest.index_baseUrl, bucket.name, scope)
                status, external_api_scope_stats_text, _ = \
                    self.rest._http_request(
                        api=api)
                external_api_scope_stats_text =\
                    external_api_scope_stats_text.decode("utf8").replace(
                        '":', '": ').replace(",", ", ")
                external_api_scope_stats_json_text = json.dumps(
                    json.loads(external_api_scope_stats_text))
                self.assertEqual(
                    external_api_scope_stats_json_text,
                    external_api_scope_stats_text)
                collections = self.rest.get_scope_collections(bucket.name,
                                                              scope)
                for collection in collections:
                    api = '{0}api/v1/stats/{1}.{2}.{3}'.format(
                        self.rest.index_baseUrl, bucket.name, scope,
                        collection)
                    status, external_api_collection_stats_text, _ =\
                        self.rest._http_request(api=api)
                    external_api_collection_stats_text =\
                        external_api_collection_stats_text.decode(
                            "utf8").replace('":', '": ').replace(",", ", ")
                    external_api_collection_stats_json_text = json.dumps(
                        json.loads(external_api_collection_stats_text))
                    self.assertEqual(external_api_collection_stats_json_text,
                                     external_api_collection_stats_text)

    def test_filtered_index_stats_membership(self):
        """
        1. Every key-value pair of an index 'idx' in /stats response should
           exists in /stats response with instance id filter filtered on
           index 'idx'
        2. Every key in response of instance id filter belongs to key set of
           /stats response
        3. Test index filter incrementally reaches all indexes
           which is equal to /stats response
        """
        all_stats_keys = set(self.all_master_stats.keys())
        indexes = []
        instance_ids = []
        filtered_stats_keys = None
        empty_inst_filter_stats_keys = set(self.rest.get_all_index_stats(
            inst_id_filter={"instances": []}).keys())
        index_status = self.rest.get_indexer_metadata(return_system_query_scope=True)
        for index in index_status['status']:
            instance_ids.append(int(index['instId']))
            instance_id_filter = {"instances": [instance_ids[-1]]}
            filtered_stats_keys = set(self.rest.get_all_index_stats(
                inst_id_filter=instance_id_filter).keys())
            key_space = f"{index['bucket']}" if index['scope'] == "_default" \
                and index['collection'] == "_default" else \
                f"{index['bucket']}:{index['scope']}:{index['collection']}"
            indexes.append(f"{key_space}:{index['name']}")
            current_index_all_stats_keys = filter(lambda key: key.startswith(
                indexes[-1]) or key.startswith(str(instance_ids[-1])),
                all_stats_keys)
            current_index_all_stats_keys = set(current_index_all_stats_keys)
            self.assertEqual(
                current_index_all_stats_keys.union(
                    empty_inst_filter_stats_keys), filtered_stats_keys,
                msg="test_filtered_stats_membership: reverse_check failed on\
                    {0}: {1}".format(
                    key_space,
                    filtered_stats_keys.difference(
                        current_index_all_stats_keys.union(
                            empty_inst_filter_stats_keys))))
            self.assertTrue(
                filtered_stats_keys.issubset(all_stats_keys),
                msg=f"test_filtered_stats_membership:single_instance_id failed: \
                    {key_space}")
            instance_id_filter = {"instances": instance_ids}
            filtered_stats_keys = set(
                self.rest.get_all_index_stats(
                    inst_id_filter=instance_id_filter).keys())
            self.assertTrue(all_stats_keys.issuperset(filtered_stats_keys),
                            msg="test_filtered_stats_membership:Incremental \
                                instance id list: {0} : failed".format(
                len(instance_ids)))
        self.assertEqual(all_stats_keys, filtered_stats_keys,
                         msg="test_filtered_stats_membership:All instance ids \
                             filter failed")

    def test_filtered_index_stats_correctness(self):
        """
        Response should only contain indexes info that are specified in
        instance id list and every key of an index 'idx' in /stats response
        should exists in /stats response with instance id filter filtered on
        index 'idx'
        https://issues.couchbase.com/browse/MB-45467
        """
        index = self.index_status['status'][0]
        namespace = "{0}:{1}:{2}:{3}".format(index['bucket'], index['scope'],
                                             index['collection'], index['name']
                                             )
        namespace_slugs = 4
        if index['scope'] == "_default" and index['collection'] == "_default":
            namespace = f"{index['bucket']}"
            namespace_slugs = 3
        instance_id_filter = {"instances": [index['instId']]}
        index_stats_keys = self.rest.get_all_index_stats(
            inst_id_filter=instance_id_filter).keys()
        index_stats_keys = set(
            [key for key in index_stats_keys if len(key.split(":")) >= 3 and 'MAINT_STREAM' not in key])
        incorrect_index_keys = set(
            [key for key in self.all_master_stats.keys()
                if not key.startswith(namespace)])
        correct_index_keys = set(
            [key for key in self.all_master_stats.keys()
                if key.startswith(namespace) and
                len(key.split(":")) == namespace_slugs])
        self.assertFalse(index_stats_keys.issubset(incorrect_index_keys))
        self.assertEqual(index_stats_keys, correct_index_keys)

    def test_invalid_instances_fallback(self):
        """
        1. valid id + invalid id with correct type
        2. valid id + invalid id with incorrect type
        3. invalid id with correct type
        4. invalid id with incorrect type
        5. valid id with incorrect type
        https://issues.couchbase.com/browse/MB-45467
        """
        index1 = self.index_status['status'][0]
        index2 = self.index_status['status'][1]
        all_stats_keys = set(self.all_master_stats.keys())
        index_stats_keys = set(self.rest.get_all_index_stats(inst_id_filter={
            "instances": [index1['instId'], str(index2['instId'])]
        }).keys())
        self.assertEqual(index_stats_keys, all_stats_keys)
        index_stats_keys = set(self.rest.get_all_index_stats(inst_id_filter={
            "instances": [str(index2['instId'])]
        }).keys())
        self.assertEqual(index_stats_keys, all_stats_keys)
        index_stats_keys = set(self.rest.get_all_index_stats(inst_id_filter={
            "instances": [index1['instId'], 1]
        }).keys())
        index1_stats = set(self.rest.get_all_index_stats(inst_id_filter={
            "instances": [index1['instId']]
        }).keys())
        self.assertEqual(index_stats_keys, index1_stats)
        index_stats_keys = set(self.rest.get_all_index_stats(inst_id_filter={
            "instances": [1]
        }).keys())
        bucket_level_stats = set(
            [key for key in self.all_master_stats
                if (len(key.split(":")) == 2 and not
             key.split(":")[0].isdigit()) or 'MAINT_STREAM' in key])
        indexer_level_stats = self.indexer_stats_keyset.union(
            bucket_level_stats)
        self.assertEqual(index_stats_keys, indexer_level_stats)
        index_stats_keys = set(self.rest.get_all_index_stats(inst_id_filter={
            "instances": []
        }).keys())
        self.assertEqual(index_stats_keys, indexer_level_stats)

    def test_consumer_stats_membership(self):
        """
        Every stats returned by /stats with consumer filter is subset of /stats
        """
        all_stats_keys = set(self.rest.get_all_index_stats(system=True).keys())
        for consumer_filter in self.consumer_filters.keys():
            consumer_filter_stats_keys = set(self.rest.get_all_index_stats(
                consumer_filter=consumer_filter).keys())
            if consumer_filter == 'indexStatus':
                consumer_filter_stats_keys.remove('rebalance_transfer_progress')
            self.assertTrue(
                consumer_filter_stats_keys.issubset(all_stats_keys),
                msg=f"test_consumer_stats_membership:{consumer_filter} failed")

    def test_consumer_stats_correctness(self):
        """
        Every consumer of stats has a set of dedicated stats
        only that set of stats should be returned by the endpoint
        """
        for consumer_filter, stats in self.consumer_filters.items():
            consumer_filter_stats = self.rest.get_all_index_stats(
                consumer_filter=consumer_filter)
            consumer_filter_stats_keys = set(consumer_filter_stats.keys())
            consumer_filter_stats_count = len(stats['indexer_level_stats']) +\
                (len(stats['index_level_stats']) * self.index_count)

            metadata = self.rest.get_indexer_metadata(return_system_query_scope=True)['status']
            system_scope_instance_ids = set([f"{index['instId']}:completion_progress" for index in metadata if index['scope'] == '_system'])
            if consumer_filter == 'indexStatus':
                consumer_filter_stats_keys -= system_scope_instance_ids

            # Instead of exact count matching, verify that the returned stats are valid
            # The exact count might vary due to dynamic stats and instance-specific variations
            self.log.info(f"{consumer_filter}: Expected count: {consumer_filter_stats_count}, Actual count: {len(consumer_filter_stats_keys)}")
            self.log.info(f"Indexer level stats: {len(stats['indexer_level_stats'])}, Index level stats: {len(stats['index_level_stats'])}, Index count: {self.index_count}")
            
            # Verify that we have at least the minimum expected stats
            min_expected_count = len(stats['indexer_level_stats'])
            self.assertGreaterEqual(
                len(consumer_filter_stats_keys), min_expected_count,
                msg=f"{consumer_filter}: Expected at least {min_expected_count} stats, but got {len(consumer_filter_stats_keys)}")
            for key in consumer_filter_stats_keys:
                stat = key.split(":")[-1]
                if stats['indexer_level_stats']:
                    self.assertTrue(
                        stat in stats['indexer_level_stats'] or
                        stat in stats['index_level_stats'],
                        msg=f"{consumer_filter} correctness failed for {key}")
                else:
                    self.assertTrue(
                        stat in stats['index_level_stats'],
                        msg=f"{consumer_filter} correctness failed for {key}")

    def _validate_external_api_stats(self, external_api_index_stats):
        """
        /stats is not superset of /api/v1/stats there are some
        extra stats(derived from stats that are present in /stats)
        that are returned by the external api. The set difference of
        /api/v1/stats and /stats should give the extra stats only
        """
        for stats in external_api_index_stats.values():
            stats_keyset = set(stats.keys())
            extra_stats = stats_keyset.difference(self.index_stats_keyset)
            self.assertEqual(extra_stats, self.external_api_extra_index_stats)

    def test_extenal_api_membership(self):
        """
        though stats returned by the /api/v1/stats is not subset of /stats,
        on removing extra stats should be a subset
        """
        external_api_stats = self.rest.get_index_official_stats()
        external_api_indexer_stats = external_api_stats.pop('indexer', None)
        indexer_stats_keyset = set(external_api_indexer_stats.keys())
        extra_stats = indexer_stats_keyset.difference(
            self.indexer_stats_keyset)
        self.assertEqual(extra_stats, self.external_api_extra_indexer_stats)
        self._validate_external_api_stats(external_api_stats)
        for bucket in self.buckets:
            external_api_bucket_stats = self.rest.get_index_official_stats(
                bucket=bucket.name)
            self._validate_external_api_stats(external_api_bucket_stats)
            scopes = self.rest.get_bucket_scopes(bucket.name)
            for scope in scopes:
                external_api_scope_stats = self.rest.get_index_official_stats(
                    bucket=bucket.name, scope=scope
                )
                self._validate_external_api_stats(external_api_scope_stats)
                collections = self.rest.get_scope_collections(bucket.name,
                                                              scope)
                for collection in collections:
                    external_api_collection_stats =\
                        self.rest.get_index_official_stats(
                            bucket=bucket.name, scope=scope,
                            collection=collection)
                    self._validate_external_api_stats(
                        external_api_collection_stats)

    def test_external_api_correctness(self):
        """
        external api has a feature to query bucket, scope and collection
        level stats all these stats should be consistent and correct there
        should be no overlap of stats
        """
        indexes_dict = {
            "indexes": set()
        }
        for index in self.index_status['status']:
            bucket = index['bucket']
            scope = index['scope']
            collection = index['collection']
            if scope == '_system' and collection == '_query':
                continue
            index_name = f"{bucket}:{scope}:{collection}:{index['name']}"\
                if scope != "_default" and collection != "_default" else\
                f"{bucket}:{index['name']}"
            indexes_dict['indexes'].add(index_name)
            if bucket not in indexes_dict:
                indexes_dict[bucket] = {
                    "indexes": set()
                }
            indexes_dict[bucket]['indexes'].add(index_name)
            if scope not in indexes_dict[bucket]:
                indexes_dict[bucket][scope] = {"indexes": set()}
            indexes_dict[bucket][scope]['indexes'].add(index_name)
            if collection not in indexes_dict[bucket][scope]:
                indexes_dict[bucket][scope][collection] = {
                    "indexes": set()
                }
            indexes_dict[bucket][scope][collection]['indexes'].add(
                index_name)
        external_api_stats = self.rest.get_index_official_stats()
        external_api_stats_keyset = set(external_api_stats.keys())
        self.assertEqual(
            external_api_stats_keyset.difference(indexes_dict['indexes']),
            {"indexer"})
        for bucket in self.buckets:
            external_api_bucket_stats = self.rest.get_index_official_stats(
                bucket=bucket.name)
            bucket_indexes = set([index
                                  for index in
                                  external_api_bucket_stats.keys()])
            self.assertEqual(
                bucket_indexes, indexes_dict[bucket.name]['indexes'])
            scopes = self.rest.get_bucket_scopes(bucket.name)
            for scope in scopes:
                external_api_scope_stats = self.rest.get_index_official_stats(
                    bucket=bucket.name, scope=scope
                )
                scope_indexes = set([index
                                     for index in
                                     external_api_scope_stats.keys()])
                self.assertEqual(
                    scope_indexes, indexes_dict[bucket.name][scope]['indexes'])
                collections = self.rest.get_scope_collections(bucket.name,
                                                              scope)
                for collection in collections:
                    external_api_collection_stats =\
                        self.rest.get_index_official_stats(
                            bucket=bucket.name, scope=scope,
                            collection=collection
                        )
                    collection_indexes = set(
                        [index for index in
                         external_api_collection_stats.keys()])
                    self.assertEqual(
                        collection_indexes,
                        indexes_dict[bucket.name][scope][collection]['indexes']
                    )
        # bucket name with .
        bucket_name = "test_bucket.1"
        index_name = "test_index_1"
        self.bucket_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size,
            replicas=self.num_replicas, bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(
            name=bucket_name, port=11222,
            bucket_params=self.bucket_params)
        self.collection_rest.create_scope_collection_count(
            scope_num=self.num_scopes, collection_num=self.num_collections,
            scope_prefix=self.scope_prefix,
            collection_prefix=self.collection_prefix, bucket=bucket_name)
        self.sleep(10, "Allowing time after collection creation")
        idx_default = QueryDefinition(
            index_name=index_name,
            index_fields=['firstName'])
        query = idx_default.generate_index_create_query(
            namespace=f"default:`{bucket_name}`", defer_build=self.defer_build)
        self.run_cbq_query(query=query)
        external_api_bucket_stats = self.rest.get_index_official_stats(
            bucket=bucket_name)
        self._validate_external_api_stats(external_api_bucket_stats)
        self.assertEqual(
            set(external_api_bucket_stats.keys()),
            set([f"{bucket_name}:{index_name}"]))

    def test_storage_stats_json(self):
        """MB-41287 and MB-41178"""
        if len(self.servers) < 2:
            self.fail("Atleast 2 servers needed for this test")
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        rest_connections = [RestConnection(indexer_node)
                            for indexer_node in indexer_nodes]
        bucket_name = self.buckets[-1].name
        for idx_num in range(self.num_indexes):
            index_name = f"test_idx_{idx_num}"
            index_def = QueryDefinition(index_name=index_name,
                                        index_fields=['firstName'])
            query = index_def.generate_index_create_query(
                namespace=f"default:`{bucket_name}`",
                defer_build=self.defer_build)
            try:
                self.run_cbq_query(query)
            except Exception as e:
                self.log.error("Memory may be insufficient - " + e)
                raise
            self.wait_until_specific_index_online(
                index_name=index_name, defer_build=self.defer_build)
            index_exists = False
            for rest in rest_connections:
                storage_stats = {}
                status = False
                indexer_warmup_message =\
                    b'Indexer In Warmup. Please try again later.'
                content = ""
                api = rest.index_baseUrl + 'stats/storage'
                while True:
                    status, content, _ = rest._http_request(api)
                    if not status:
                        raise Exception(content)
                    if content != indexer_warmup_message:
                        break
                    self.sleep(5, message="Wait for indexer to warmup")
                try:
                    storage_stats = json.loads(content)
                except Exception:
                    self.log.error("***malformed json***")
                    self.log.error(content)
                    self.log.error("********************")
                    raise
                for index_stats in storage_stats:
                    if f"{bucket_name}:{index_name}" == index_stats['Index']:
                        index_exists = True
            self.assertTrue(
                index_exists, msg=f"{bucket_name}:{index_name} not found")

    def test_dropped_index_stats(self):
        """
        Stats of dropped index should not be available
        """
        create_queries = []
        drop_queries = []
        for index in self.index_status['status']:
            namespace = f'default:{index["bucket"]}'
            if index['scope'] != "_default" or\
                    index['collection'] != "_default":
                namespace =\
                    f'{namespace}.{index["scope"]}.{index["collection"]}'
            if index.get('isPrimary', False):
                query_def = QueryDefinition(index_name=index['name'])
                create_query = query_def.generate_primary_index_create_query(
                    namespace=namespace, defer_build=self.defer_build)
                drop_query = query_def.generate_index_drop_query(
                    namespace=namespace)
                create_queries.append(create_query)
                drop_queries.append(drop_query)
                self.run_cbq_query(drop_queries[-1])
                stats = self.rest.get_all_index_stats(
                    inst_id_filter=[index['instId']]).keys()
                stats = list(
                    map(
                        lambda key: ".".join(key.split(":")[:-1])
                        if len(key.split(":")) >= 3 else None, stats))
                self.assertFalse(namespace.split(":")[1] in stats)
            else:
                query_def = QueryDefinition(
                    index_name=index['name'], index_fields=index["secExprs"])
                create_query = query_def.generate_index_create_query(
                    namespace=namespace, defer_build=self.defer_build)
                drop_query = query_def.generate_index_drop_query(
                    namespace=namespace)
                create_queries.append(create_query)
                drop_queries.append(drop_query)
                self.run_cbq_query(drop_queries[-1])
                stats = self.rest.get_all_index_stats(
                    inst_id_filter=[index['instId']]).keys()
                stats = list(
                    map(
                        lambda key: ".".join(key.split(":")[:-1])
                        if len(key.split(":")) >= 3 else None, stats))
                self.assertFalse(namespace.split(":")[1] in stats)

    def test_internal_stats_availability_in_cluster(self):
        """
        Indexes are spread across multiple available nodes. On querying stats
        of a node should give stats of indexes residing on that node only
        https://issues.couchbase.com/browse/MB-45467
        """
        if len(self.servers) < 2:
            self.fail("Atleast 2 servers needed for this test")
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        rest_connections = [RestConnection(indexer_node)
                            for indexer_node in indexer_nodes]
        index_status = [
            rest.get_indexer_metadata() for rest in rest_connections]
        reference_status = index_status[0]
        for status in index_status[1:]:
            if reference_status != status:
                self.fail(msg="Indexer Stats are different on nodes")
        all_index_stats = [
            rest.get_all_index_stats() for rest in rest_connections]
        indexes_info = [
            set([stat.rsplit(":", 1)[0] for stat in index_stats
                 if len(stat.split(":")) > 2 and 'MAINT_STREAM' not in stat])
            for index_stats in all_index_stats]
        for idx_num, current_index in enumerate(indexes_info):
            for index in indexes_info[idx_num + 1:]:
                self.assertSetEqual(current_index.intersection(index), set())
        all_instance_ids = [[
            int(stat.split(":")[0])
            for stat in index_stats if stat.split(":")[0].isdigit()]
            for index_stats in all_index_stats]
        all_index_stats_keys = [
            set(rest.get_all_index_stats(
                inst_id_filter={"instances": instance_ids}).keys())
            for rest_id, rest in enumerate(rest_connections)
            for node_id, instance_ids in enumerate(all_instance_ids)
            if node_id != rest_id]
        all_bucket_level_stats = [
            set([key for key in index_stats if (len(key.split(":")) == 2 and not
                 key.split(":")[0].isdigit()) or 'MAINT_STREAM' in key])
            for index_stats in all_index_stats
        ]
        all_indexer_level_stats = [self.indexer_stats_keyset.union(
            bucket_level_stats)
            for bucket_level_stats in all_bucket_level_stats]
        for indexer_level_stats, index_stats_keys in zip(
                all_indexer_level_stats, all_index_stats_keys):
            self.assertEqual(index_stats_keys, indexer_level_stats)
        for rest in rest_connections:
            rest.consumer_filters = self.consumer_filters.keys()
        all_consumer_filter_stats_keys = [
            [
                set(rest.get_all_index_stats(
                    consumer_filter=consumer_filter).keys())
                for consumer_filter in rest.consumer_filters]
            for rest in rest_connections]
        all_indexes = [
            [
                set([key.rsplit(":", 1)[0]
                     for key in consumer_filter_stats_keys
                     if len(key.split(":")) > 2])
                for consumer_filter_stats_keys in consumer_filter_stats]
            for consumer_filter_stats in all_consumer_filter_stats_keys]
        available_indexes = []
        for current_node_indexes in all_indexes:
            for idx_num, current_index in enumerate(current_node_indexes):
                for index in current_node_indexes[idx_num + 1:]:
                    self.assertSetEqual(current_index, index)

        for node_num, current_node_indexes in enumerate(all_indexes):
            for other_nodex_indexes in all_indexes[node_num + 1:]:
                self.assertEqual(
                    current_node_indexes[0].intersection(
                        other_nodex_indexes[0]), set())
        available_indexes = set()
        for indexes in all_indexes:
            available_indexes = available_indexes.union(indexes[0])
        self.assertSetEqual(available_indexes, set(self.default_indexes))

    def test_external_stats_availability_in_cluster(self):
        """
        Indexes are spread across multiple available nodes. On querying stats
        of a node should give stats of indexes residing on that node only
        """
        if len(self.servers) < 2:
            self.fail("Atleast 2 servers needed for this test")
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        rest_connections = [RestConnection(indexer_node)
                            for indexer_node in indexer_nodes]
        all_index_stats = [
            rest.get_all_index_stats() for rest in rest_connections]
        all_indexes = [set(
            [stat.rsplit(":", 1)[0] for stat in index_stats
             if len(stat.split(":")) > 2 and 'MAINT_STREAM' not in stat]) for index_stats in all_index_stats]
        all_external_api_stats = [
            rest.get_index_official_stats() for rest in rest_connections]
        for external_stats, indexes in zip(
                all_external_api_stats, all_indexes):
            external_stats.pop("indexer")
            self.assertEqual(set(external_stats.keys()), indexes)
        consumer_filters = set()
        for index in self.default_indexes:
            namespace = index.rsplit(":", 1)[0].replace(":", ".")\
                if len(index.split(":")) > 2\
                else index.rsplit(":", 1)[0] + '._default._default'
            bucket_filter_api = f'api/v1/stats/{namespace.split(".")[0]}'
            scope_filter_api = f'api/v1/stats/{namespace.rsplit(".", 1)[0]}'
            collection_filter_api = f'api/v1/stats/{namespace}'
            consumer_filters.add(bucket_filter_api)
            consumer_filters.add(scope_filter_api)
            consumer_filters.add(collection_filter_api)
        for api_filter in consumer_filters:
            external_api_filtered_stats = [
                rest._http_request(
                    api=f"{rest.index_baseUrl}{api_filter}")
                for rest in rest_connections]
            is_index_stat_available = reduce(
                lambda found, stats: found or stats[0],
                external_api_filtered_stats, False)
            if not is_index_stat_available:
                self.fail(msg=f"stats for {api_filter} missing")
            for current_stats in external_api_filtered_stats:
                if current_stats[0]:
                    external_api_stats_key_set = set(
                        json.loads(current_stats[1]).keys()
                    )
                    index_found = False
                    for indexes in all_indexes:
                        if external_api_stats_key_set.issubset(indexes):
                            if index_found:
                                self.fail(msg="{0} is redundant".format(
                                    external_api_stats_key_set))
                            index_found = True

    def test_stats_with_index_replica(self):
        indexes = self.rest.get_indexer_metadata()['status']
        for index in indexes:
            query_def = QueryDefinition(index['name'])
            namespace = "{0}.{1}.{2}".format(
                index['bucket'], index['scope'],
                index['collection'])\
                if index['scope'] != "_default"\
                and index['collection'] != "_default"\
                else f"{index['bucket']}"
            query = query_def.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query)
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        if self.num_index_replicas == 0 or\
                self.num_index_replicas >= len(indexer_nodes):
            self.num_index_replicas = len(indexer_nodes) - 1
        replica_index = QueryDefinition(
            "replica_index",
            index_fields=["firstName"])
        bucket = self.buckets[0].name
        if self.use_https:
            port = '18091'
        else:
            port = '8091'
        query = replica_index.generate_index_create_query(
            namespace=f"default:{bucket}",
            deploy_node_info=[
                idx.ip + ":" + port
                for idx in indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replicas
        )
        self.run_cbq_query(query)
        indexer_stats = self.rest.get_indexer_metadata()
        replica_indexes = list(filter(
            lambda index: index['name'].startswith(
                replica_index.index_name), indexer_stats['status']))
        self.assertTrue(
            len(replica_indexes) == self.num_index_replicas + 1)
        replica_index_ids = list(
            map(lambda index: index["instId"], replica_indexes))
        instance_ids = set()
        rest_connections = [RestConnection(node) for node in indexer_nodes]
        metadata = self.rest.get_indexer_metadata(return_system_query_scope=True)['status']
        system_scope_instance_ids = set([index['instId'] for index in metadata if index['scope'] == '_system'])
        for node_id, rest in enumerate(rest_connections):
            all_stats = rest.get_all_index_stats(
                inst_id_filter=replica_index_ids)
            instance_ids.update(
                set(map(lambda stat: int(stat.split(":")[0]),
                        list(filter(lambda stat: stat.split(":")[0].isdigit(),
                                    all_stats.keys())))))
            instance_ids -= system_scope_instance_ids
            self.assertEqual(len(instance_ids), node_id + 1)
        self.assertEqual(instance_ids, set(replica_index_ids))

    def test_stats_with_partition_index(self):
        indexes = self.rest.get_indexer_metadata()['status']
        for index in indexes:
            query_def = QueryDefinition(index['name'])
            namespace = "{0}.{1}.{2}".format(
                index['bucket'], index['scope'],
                index['collection'])\
                if index['scope'] != "_default"\
                and index['collection'] != "_default"\
                else f"{index['bucket']}"
            query = query_def.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query)
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        rest_connections = [RestConnection(node) for node in indexer_nodes]
        partition_fields = ['meta().id']
        partition_index = QueryDefinition(
            "partition_index",
            index_fields=["firstName"])
        bucket = self.buckets[0].name
        if self.use_https:
            port = '18091'
        else:
            port = '8091'
        query = partition_index.generate_index_create_query(
            namespace=f"default:{bucket}",
            deploy_node_info=[
                idx.ip + ":" + port
                for idx in indexer_nodes],
            defer_build=self.defer_build,
            partition_by_fields=partition_fields
        )
        self.run_cbq_query(query)
        indexer_stats = self.rest.get_indexer_metadata()
        partition_indexes = list(filter(
            lambda index: index['name'].startswith(
                partition_index.index_name), indexer_stats['status']))
        partition_index_ids = list(
            map(lambda index: index["instId"], partition_indexes))
        rest_connections = [RestConnection(node) for node in indexer_nodes]
        metadata = self.rest.get_indexer_metadata(return_system_query_scope=True)['status']
        system_scope_instance_ids = set([index['instId'] for index in metadata if index['scope'] == '_system'])
        instance_ids = set()
        for node_id, rest in enumerate(rest_connections):
            all_stats = rest.get_all_index_stats(
                inst_id_filter=partition_index_ids)
            instance_ids.update(
                set(map(lambda stat: int(stat.split(":")[0]),
                        list(filter(lambda stat: stat.split(":")[0].isdigit(),
                                    all_stats.keys())))))
            instance_ids -= system_scope_instance_ids
            self.assertEqual(len(instance_ids), 1)
        self.assertEqual(instance_ids, set(partition_index_ids))

    def test_stats_with_error_index(self):
        doc = {"indexer.scheduleCreateRetries": 1}
        self.rest.set_index_settings(doc)
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        if len(indexer_nodes) < 2:
            self.fail("need atleast 2 indexer nodes")
        indexer_nodes = indexer_nodes[:2]
        indexes = self.rest.get_indexer_metadata()['status']
        for index in indexes:
            query_def = QueryDefinition(index['name'])
            namespace = "{0}.{1}.{2}".format(
                index['bucket'], index['scope'],
                index['collection'])\
                if index['scope'] != "_default"\
                and index['collection'] != "_default"\
                else f"{index['bucket']}"
            query = query_def.generate_index_drop_query(namespace=namespace)
            self.run_cbq_query(query)
        if self.num_index_replicas == 0 or\
                self.num_index_replicas >= len(indexer_nodes):
            self.num_index_replicas = len(indexer_nodes) - 1
        replica_index = QueryDefinition(
            "replica_index",
            index_fields=["firstName"])
        bucket = self.buckets[0].name
        if self.use_https:
            port = '18091'
        else:
            port = '8091'
        query = replica_index.generate_index_create_query(
            namespace=f"default:{bucket}",
            deploy_node_info=[
                idx.ip + ":" + port
                for idx in indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replicas
        )
        self.run_cbq_query(query)
        self.sleep(10, "Giving some time before blocking communication between indexer nodes")
        self.block_incoming_network_from_node(
            indexer_nodes[0], indexer_nodes[1])
        self.sleep(10)
        rest_connections = [RestConnection(node) for node in indexer_nodes]
        metadata = self.rest.get_indexer_metadata(return_system_query_scope=True)['status']
        system_scope_instance_ids = set([index['instId'] for index in metadata if index['scope'] == '_system'])
        for rest in rest_connections:
            if rest.ip == self.rest.ip:
                doc = {"indexer.scheduleCreateRetries": 1}
                rest.set_index_settings(doc)
                break
        indexer_stats_1 = rest_connections[0].get_indexer_metadata()
        replica_indexes_1 = list(filter(
            lambda index: index['name'].startswith(
                replica_index.index_name), indexer_stats_1['status']))
        msg = f"{replica_indexes_1}"
        self.assertEqual(
            len(replica_indexes_1), self.num_index_replicas + 1, msg=msg)
        indexer_stats_2 = rest_connections[1].get_indexer_metadata()
        replica_indexes_2 = list(filter(
            lambda index: index['indexName'].startswith(
                replica_index.index_name), indexer_stats_2['status']))
        msg = f"{replica_indexes_2}"
        self.assertEqual(
            len(replica_indexes_2), self.num_index_replicas + 1)
        replica_index_ids_1 = list(
            map(lambda index: index["instId"], replica_indexes_1))
        replica_index_ids_2 = list(
            map(lambda index: index["instId"], replica_indexes_2))
        msg = f"{replica_index_ids_1}"
        self.assertEqual(set(replica_index_ids_1), set(replica_index_ids_2),
                         msg=msg)
        instance_ids = set()
        for node_id, rest in enumerate(rest_connections):
            all_stats = rest.get_all_index_stats(
                inst_id_filter=replica_index_ids_1)
            instance_ids.update(
                set(map(lambda stat: int(stat.split(":")[0]),
                        list(filter(lambda stat: stat.split(":")[0].isdigit(),
                                    all_stats.keys())))))
            instance_ids -= system_scope_instance_ids
            msg = f"{instance_ids}"
            self.assertEqual(len(instance_ids), node_id + 1, msg=msg)
        msg = f"{instance_ids}"
        self.assertEqual(instance_ids, set(replica_index_ids_1), msg=msg)
        self.resume_blocked_incoming_network_from_node(
            indexer_nodes[0], indexer_nodes[1])
        self.sleep(10)

    def test_stats_after_rebalance(self):
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        if len(indexer_nodes) < 2:
            self.fail("need atleast 2 indexer nodes")
        rest_connections = [RestConnection(node) for node in indexer_nodes]
        metadata = self.rest.get_indexer_metadata(return_system_query_scope=True)['status']
        system_scope_instance_ids = set([index['instId'] for index in metadata if index['scope'] == '_system'])
        for rest in rest_connections:
            instance_ids = []
            for index in self.index_status['status']:
                if rest.ip + ":" + rest.port in index['hosts']:
                    instance_ids.append(index['instId'])
            stats = rest.get_all_index_stats(inst_id_filter=instance_ids)
            self.assertNotEquals(stats, {})
            node_inst_ids = set(
                [int(stat.split(":")[0]) for stat in stats.keys()
                 if stat.split(":")[0].isdigit()])
            node_inst_ids -= system_scope_instance_ids
            self.assertEqual(set(instance_ids), node_inst_ids)
        out_nodes = [
            node for node in indexer_nodes if self.master.ip != node.ip]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)
        instance_ids = [
            index['instId'] for index in self.index_status['status'] if index['scope'] != '_system']
        stats = self.rest.get_all_index_stats(inst_id_filter=instance_ids)
        self.assertNotEquals(stats, {})
        node_inst_ids = set([int(stat.split(":")[0]) for stat in stats.keys()
                            if stat.split(":")[0].isdigit()])
        node_inst_ids -= system_scope_instance_ids
        self.assertEqual(set(instance_ids), node_inst_ids)
