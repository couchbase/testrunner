"""indexer_stats.py: Test index stats correctness, membership and availability
Test details available at - https://docs.google.com/spreadsheets/d/1Y7RKksVazSDG_qY9SqpnydXr5A2FSSHCMTEx-V0Ar08/edit#gid=739826981&range=A21:G22
# heading=h.b7iqwqesg1v2
Backup and Restore Docs - https://docs.google.com/document/d/1NVvSFC_enKQU0C-eQ6NXoYaNQ90VXY5jN8RqMahvnr8/edit
__author__ = "Sri Bhargava Yadavalli"
__maintainer = "Hemant Rajput"
__email__ = "bhargava.yadavalli@couchbase.com"
__git_user__ = "sreebhargava143"
__created_on__ = "17/11/20 10:00 am"
"""
import json
import random

from .base_gsi import BaseSecondaryIndexingTests
from lib.couchbase_helper.query_definitions import QueryDefinition
from lib.collection.collections_rest_client import CollectionsRest
from membase.api.rest_client import RestConnection
from itertools import combinations, chain


class IndexBackupClient(object):
    """
    A class to represent an Indexer Backup.

    Attributes
    ----------
    rest : membase.api.rest_client.RestConnection
        Object that establishes the rest connection to the indexer node
    node : TestInput.TestInputServer
        Object that represents the indexer node
    bucket : membase.api.rest_client.Bucket
        Object that represents the bucket on which the back is to be taken
    is_backup_exists : boolean
        variable to check whether backup exists
    backup : dict
        result of the backup api

    Methods
    -------
    backup(namespaces=[], include=True):
        Takes the backup of the current indexes.

    restore(namespaces=[], include=True):
        Restores the stored backup to the node.
    """

    def __init__(self, node, bucket):
        if "index" not in node.services.split(","):
            raise Exception("Index service is not available on the node")
        self.rest = RestConnection(node)
        self.node = node
        self.bucket = bucket
        self.backup_api =\
            self.rest.index_baseUrl + "api/v1/bucket/{0}/backup{1}"
        self.is_backup_exists = False
        self.backup_data = {}

    def backup(self, namespaces=[], include=True):
        """Takes the backup of the current indexes.
        Parameters:
            namespaces -- list of namespaces that are to be included in or
            excluded from backup. Namespaces should follow the syntax
            described in docs (default [])
            include -- :boolean: if False then exclude (default True)
        Returns (status, content) tuple
        """
        query_params = ""
        if namespaces:
            query_params = "?include=" if include else "?exclude="
            query_params += ",".join(namespaces)
        api = self.backup_api.format(self.bucket, query_params)
        status, content, _ = self.rest._http_request(api=api)
        if status:
            self.backup_data = json.loads(content)['result']
            self.is_backup_exists = True
        return status, content

    def restore(self, mappings=[]):
        """Restores the stored backup to the node
        Parameters:
            mappings -- list of mappings should follow the syntax
            described in docs (default [])
        Returns (status, content) tuple
        """
        if not self.is_backup_exists:
            raise Exception("No backup found")
        query_params = ""
        if mappings:
            query_params = "?remap={0}".format(",".join(mappings))
        headers = self.rest._create_capi_headers()
        body = json.dumps(self.backup_data)
        api = self.backup_api.format(self.bucket, query_params)
        status, content, _ = self.rest._http_request(
            api=api, method="POST", params=body, headers=headers)
        json_response = json.loads(content)
        if json_response['code'] == "success":
            return True, json_response
        return False, json_response

    def set_bucket(self, bucket):
        self.bucket = bucket
        if self.backup_data:
            for metadata in self.backup_data['metadata']:
                for topology in metadata['topologies']:
                    topology['bucket'] = bucket
                    for definition in topology['definitions']:
                        definition['bucket'] = bucket
                for definition in metadata['definitions']:
                    definition['bucket'] = bucket


class BackupRestoreTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(BackupRestoreTests, self).setUp()
        self.doc_size = self.input.param("doc_size", 100)
        self.bucket_remap = self.input.param("bucket_remap", False)
        self.num_partitions = self.input.param("num_partitions", 8)
        self.collection_rest = CollectionsRest(self.master)
        self.collection_namespaces = []
        self.checkpoint = []
        if not self.default_bucket:
            self._create_bucket_structure()
            self.buckets = self.rest.get_buckets()
            for bucket_num, bucket in enumerate(self.buckets):
                bucket_id = bucket_num + 1
                index_name = f"default_idx_{bucket_id}"
                namespace = f'default:{bucket.name}'
                bucket_idx = QueryDefinition(
                    index_name, index_fields=["firstName", "lastName"])
                query = bucket_idx.generate_index_create_query(
                    namespace=namespace, defer_build=self.defer_build)
                self.run_cbq_query(query=query)
                index_name = f"primary_idx_{bucket_id}"
                primary_idx = QueryDefinition(index_name)
                query = primary_idx.generate_primary_index_create_query(
                    namespace, defer_build=self.defer_build)
                self.run_cbq_query(query=query)
                scopes = self.rest.get_bucket_scopes(bucket.name)
                self.collection_namespaces = [
                    f"default:{bucket.name}.{scope}.{collection}"
                    for scope in scopes
                    for collection in
                    self.rest.get_scope_collections(bucket.name, scope)]
                for idx_id, namespace in enumerate(self.collection_namespaces):
                    index_name = f"idx_{bucket_id}_{idx_id}"
                    query_def = QueryDefinition(
                        index_name, index_fields=["firstName", "lastName"])
                    query = query_def.generate_index_create_query(
                        namespace=namespace, defer_build=self.defer_build)
                    self.run_cbq_query(query=query)

    def tearDown(self):
        super(BackupRestoreTests, self).tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def _create_bucket_structure(self):
        self.collection_rest.create_buckets_scopes_collections(
            self.num_buckets, self.num_scopes, self.num_collections,
            self.bucket_size, self.test_bucket, self.scope_prefix,
            self.collection_prefix)

    def _recreate_bucket_structure(self, bucket=""):
        if self.default_bucket:
            self.rest.create_bucket(self.buckets[0].name, ramQuotaMB=200)
        elif bucket:
            self.rest.create_bucket(bucket, ramQuotaMB=self.bucket_size)
            self.collection_rest.create_scope_collection_count(
                self.num_scopes, self.num_collections, self.scope_prefix,
                self.collection_prefix, bucket)
        else:
            self._create_bucket_structure()
        self.sleep(
            5, message="Wait for buckets-scopes-collections to come online")

    def _verify_indexes(self, indexer_stats_before_backup,
                        indexer_stats_after_restore):
        for idx_before_backup, idx_after_restore in zip(
                indexer_stats_before_backup, indexer_stats_after_restore):
            self.assertEqual(
                idx_before_backup['name'], idx_after_restore['name'])
            self.assertEqual(idx_before_backup.get('isPrimary', None),
                             idx_after_restore.get('isPrimary', None))
            self.assertEqual(idx_before_backup.get('secExprs', None),
                             idx_after_restore.get('secExprs', None))
            self.assertEqual(
                idx_before_backup['indexType'],
                idx_after_restore['indexType'])
            self.assertEqual(idx_after_restore['status'], "Created")
            self.assertEqual(idx_before_backup['numReplica'],
                             idx_after_restore['numReplica'])
            self.assertEqual(idx_before_backup['numPartition'],
                             idx_after_restore['numPartition'])
        self.log.info("Indexes verified successfully")

    def _powerset(self, iter_item):
        """powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"""
        s = list(iter_item)
        return list(
            chain.from_iterable(
                combinations(s, r)
                for r in range(len(s) + 1)))[1:]

    def test_steady_state_basic_backup_restore(self):
        """Test for basic functionality of the backup api without any query
        params"""
        indexer_stats_before_backup = self.rest.get_indexer_metadata()
        index_backup_clients = [
            IndexBackupClient(self.master, bucket.name)
            for bucket in self.buckets]
        for backup_client in index_backup_clients:
            self.assertTrue(
                backup_client.backup()[0],
                msg=f"Backup failed for {backup_client.bucket}")
        self.rest.delete_all_buckets()
        self._recreate_bucket_structure()
        for backup_client in index_backup_clients:
            self.assertTrue(
                backup_client.restore()[0],
                msg=f"Restore failed for {backup_client.bucket}")
            self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_stats_after_restore = self.rest.get_indexer_metadata()
        self._verify_indexes(indexer_stats_before_backup['status'],
                             indexer_stats_after_restore['status'])

    def _drop_indexes(self, indexes):
        for index in indexes:
            query_def = QueryDefinition(index['name'])
            namespace = "default:{0}.{1}.{2}".format(
                index['bucket'], index['scope'], index['collection'])\
                if index['scope'] != "_default"\
                and index['collection'] != "_default"\
                else f"default:{index['bucket']}"
            query = query_def.generate_index_drop_query(
                namespace=namespace)
            self.run_cbq_query(query)

    def _build_indexes(self, indexes):
        indexes_dict = {}
        for index in indexes:
            namespace = index['bucket']
            if index['scope'] != "_default"\
                    and index['collection'] != "_default":
                namespace += ".{0}.{1}".format(
                    index['scope'],
                    index['collection'])
            if namespace in indexes_dict:
                indexes_dict[namespace].append(index['name'])
            else:
                indexes_dict[namespace] = [index['name']]
        for namespace, indexes in indexes_dict.items():
            build_task = self.async_build_index(namespace, indexes)
            build_task.result()

    def test_steady_state_backup_with_include_scopes(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master, bucket.name)
            scopes = self.rest.get_bucket_scopes(bucket.name)
            random.shuffle(scopes)
            scopes_combinations = self._powerset(scopes)
            for scopes in scopes_combinations:
                indexer_stats_before_backup = self.rest.get_indexer_metadata()
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if index['bucket'] == bucket.name
                    and index['scope'] in scopes]
                self.assertTrue(
                    backup_client.backup(scopes)[0],
                    f"backup failed for {scopes}")
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                self.assertTrue(
                    backup_client.restore()[0], f"restore failed for {scopes}")
                self.wait_until_indexes_online(defer_build=self.defer_build)
                indexer_stats_after_restore = self.rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if index['bucket'] == bucket.name
                    and index['scope'] in scopes]
                self._verify_indexes(indexes_before_backup,
                                     indexes_after_restore)
                if not self.defer_build:
                    self._build_indexes(indexes_after_restore)

    def test_steady_state_backup_with_include_collections(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master, bucket.name)
            bucket_collection_namespaces = [
                namespace.split(".", 1)[1] for namespace
                in self.collection_namespaces
                if namespace.split(':')[1].split(".")[0] == bucket.name]
            random.shuffle(bucket_collection_namespaces)
            collection_namespaces_list = self._powerset(
                bucket_collection_namespaces)
            for collection_namespaces in collection_namespaces_list:
                indexer_stats_before_backup = self.rest.get_indexer_metadata()
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                    and f"{index['scope']}.{index['collection']}"
                    in collection_namespaces]
                self.assertTrue(
                    backup_client.backup(collection_namespaces)[0],
                    f"backup failed for {collection_namespaces}")
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                self.assertTrue(
                    backup_client.restore()[0],
                    f"restore failed for {collection_namespaces}")
                self.wait_until_indexes_online(defer_build=self.defer_build)
                indexer_stats_after_restore = self.rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                    and f"{index['scope']}.{index['collection']}"
                    in collection_namespaces]
                self._verify_indexes(indexes_before_backup,
                                     indexes_after_restore)
                if not self.defer_build:
                    self._build_indexes(indexes_after_restore)

    def test_steady_state_backup_with_exclude_scopes(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master, bucket.name)
            scopes = self.rest.get_bucket_scopes(bucket.name)
            random.shuffle(scopes)
            scopes_combinations = self._powerset(scopes)
            for excluded_scopes in scopes_combinations:
                indexer_stats_before_backup = self.rest.get_indexer_metadata()
                self.assertTrue(
                    backup_client.backup(excluded_scopes, include=False)[0],
                    f"restore failed for {scopes}")
                backedup_scopes = [scope for scope in scopes
                                   if scope not in excluded_scopes]
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                    and index['scope'] in backedup_scopes]
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                self.assertTrue(
                    backup_client.restore()[0], f"restore failed for {scopes}")
                self.wait_until_indexes_online(defer_build=self.defer_build)
                indexer_stats_after_restore = self.rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                    and index['scope'] in backedup_scopes]
                self._verify_indexes(indexes_before_backup,
                                     indexes_after_restore)
                if not self.defer_build:
                    self._build_indexes(indexes_after_restore)

    def test_steady_state_backup_with_exclude_collections(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master, bucket.name)
            bucket_collection_namespaces = [
                namespace.split(".", 1)[1] for namespace
                in self.collection_namespaces
                if namespace.split(':')[1].split(".")[0] == bucket.name]
            random.shuffle(bucket_collection_namespaces)
            collection_namespaces_list = self._powerset(
                bucket_collection_namespaces)
            for collection_namespaces in collection_namespaces_list:
                indexer_stats_before_backup = self.rest.get_indexer_metadata()
                self.assertTrue(
                    backup_client.backup(
                        collection_namespaces, include=False)[0],
                    f"backup failed for {collection_namespaces}")
                backedup_collections = [
                    collection_namespace for collection_namespace
                    in bucket_collection_namespaces
                    if collection_namespace not in collection_namespaces]
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                    and f"{index['scope']}.{index['collection']}"
                    in backedup_collections]
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                self.assertTrue(
                    backup_client.restore()[0],
                    f"restore failed for {collection_namespaces}")
                self.wait_until_indexes_online(defer_build=self.defer_build)
                indexer_stats_after_restore = self.rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                    and f"{index['scope']}.{index['collection']}"
                    in backedup_collections]
                self._verify_indexes(
                    indexes_before_backup, indexes_after_restore)
                if not self.defer_build:
                    self._build_indexes(indexes_after_restore)

    def test_steady_state_backup_with_bucket_remap(self):
        original_bucket = self.buckets[0].name
        indexer_metadata = self.rest.get_indexer_metadata()
        indexes_bucket_1 = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == original_bucket]
        backup_client = IndexBackupClient(self.master, original_bucket)
        self.assertTrue(backup_client.backup())
        remap_bucket = "remap_bucket"
        self._recreate_bucket_structure(remap_bucket)
        backup_client.set_bucket(remap_bucket)
        self.assertTrue(backup_client.restore())
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_metadata = self.rest.get_indexer_metadata()
        indexes_bucket_2 = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket]
        self._verify_indexes(indexes_bucket_1, indexes_bucket_2)

    def test_steady_state_backup_with_scope_remap(self):
        original_bucket = self.buckets[0].name
        scopes = self.rest.get_bucket_scopes(original_bucket)
        original_scope = scopes[0]
        backup_client = IndexBackupClient(self.master, original_bucket)
        indexer_metadata = self.rest.get_indexer_metadata()
        orignal_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == original_bucket
            and index['scope'] == original_scope]
        self.assertTrue(backup_client.backup(namespaces=[original_scope]))
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else original_bucket
        if remap_bucket != original_bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_bucket(remap_bucket)
        remap_scope = "remap_scope"
        self.rest.create_scope(remap_bucket, remap_scope)
        for collection_id in range(1, self.num_collections + 1):
            remap_collection = f"{self.collection_prefix}_{collection_id}"
            self.rest.create_collection(
                remap_bucket, remap_scope, remap_collection)
        self.sleep(5, "Allow scope-collection to be online")
        mappings = [f"{scopes[0]}:{remap_scope}"]
        self.assertTrue(backup_client.restore(mappings))
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_metadata = self.rest.get_indexer_metadata()
        remapped_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket
            and index['scope'] == remap_scope]
        self._verify_indexes(orignal_indexes, remapped_indexes)

    def test_steady_state_backup_with_collection_remap(self):
        original_bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.master, original_bucket)
        scopes = self.rest.get_bucket_scopes(original_bucket)
        original_scope = scopes[0]
        indexer_metadata = self.rest.get_indexer_metadata()
        orignal_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == original_bucket
            and index['scope'] == original_scope]
        self.assertTrue(backup_client.backup(namespaces=[original_scope]))
        collections = self.rest.get_scope_collections(
            original_bucket, original_scope)
        original_namespaces = [
            f"{original_scope}.{collection}"
            for collection in collections]
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else original_bucket
        if remap_bucket != original_bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_bucket(remap_bucket)
        remap_scope = "remap_scope"
        remap_namespaces = []
        self.rest.create_scope(remap_bucket, remap_scope)
        for collection_id in range(1, self.num_collections + 1):
            remap_collection = f"remap_collection_{collection_id}"
            self.rest.create_collection(
                remap_bucket, remap_scope, remap_collection)
            remap_namespaces.append(f"{remap_scope}.{remap_collection}")
        self.sleep(5, "Allow scope-collection to be online")
        mappings = [
            f"{original_namespace}:{remap_namespace}"
            for original_namespace, remap_namespace
            in zip(original_namespaces, remap_namespaces)]
        self.assertTrue(backup_client.restore(mappings))
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_metadata = self.rest.get_indexer_metadata()
        remapped_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket
            and index['scope'] == remap_scope]
        self._verify_indexes(orignal_indexes, remapped_indexes)

    def test_steady_state_backup_with_scopes_and_collections_remap(self):
        if self.num_scopes < 2:
            self.fail("Atleast 2 scopes needed")
        original_bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.master, original_bucket)
        scopes = self.rest.get_bucket_scopes(original_bucket)
        include_namespaces = [scopes[0], scopes[1]]
        indexer_metadata = self.rest.get_indexer_metadata()
        original_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == original_bucket
            and index['scope'] in scopes[:2]]
        collections = self.rest.get_scope_collections(
            original_bucket, scopes[1])
        original_namespace = f"{scopes[1]}.{collections[0]}"
        self.assertTrue(backup_client.backup(namespaces=include_namespaces))
        self.rest.delete_scope(original_bucket, scopes[1])
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else original_bucket
        if remap_bucket != original_bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_bucket(remap_bucket)
        self.rest.create_scope(remap_bucket, scopes[1])
        self.rest.create_collection(remap_bucket, scopes[1], collections[1])
        remap_scope_1 = 'remap_scope_1'
        self.rest.create_scope(remap_bucket, remap_scope_1)
        for collection_id in range(1, self.num_collections + 1):
            collection = f"{self.collection_prefix}_{collection_id}"
            self.rest.create_collection(
                remap_bucket, remap_scope_1, collection)
        self.sleep(5, "Allow scope-collection to be online")
        mappings = [f'{scopes[0]}:{remap_scope_1}']
        remap_scope_2 = 'remap_scope_2'
        remap_collection = "remap_collection"
        remap_namespace = f"{remap_scope_2}.{remap_collection}"
        self.rest.create_scope(remap_bucket, remap_scope_2)
        self.rest.create_collection(
            remap_bucket, remap_scope_2, remap_collection)
        self.sleep(5, "Allow scope-collection to be online")
        mappings.append(f'{original_namespace}:{remap_namespace}')
        self.assertTrue(backup_client.restore(mappings))
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_metadata = self.rest.get_indexer_metadata()
        remap_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket
            and index['scope'] == remap_scope_1]
        remap_indexes.extend([
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket
            and index['scope'] == remap_scope_2
            and index['collection'] == remap_collection])
        remap_indexes.extend(
            [
                index
                for index in indexer_metadata['status']
                if index['bucket'] == remap_bucket
                and index['scope'] == scopes[1]
                and index['collection'] == collections[1]
            ]
        )
        self._verify_indexes(original_indexes, remap_indexes)

    def test_backup_restore_with_replicas(self):
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        num_replicas = len(indexer_nodes) - 1
        if num_replicas < 1:
            self.fail("At least 2 indexer nodes required for this test")
        bucket = self.buckets[0].name
        scope = "replica_scope"
        collection = "replica_collection"
        self.rest.create_scope(bucket, scope)
        self.rest.create_collection(bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        index_name = "replica_index"
        namespace = f"default:{bucket}.{scope}.{collection}"
        index_fields = ["firstName", "lastName"]
        index_def = QueryDefinition(
            index_name, index_fields=index_fields)
        query = index_def.generate_index_create_query(
            namespace=namespace,
            defer_build=self.defer_build, num_replica=num_replicas)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        backup_client = IndexBackupClient(self.master, bucket)
        include_namespaces = [scope]
        indexer_stats = self.rest.get_indexer_metadata()
        replica_indexes_before_backup = [
            index for index in indexer_stats['status']
            if index['bucket'] == bucket and index['scope'] == scope
        ]
        backup_client.backup(include_namespaces)
        self.rest.delete_scope(bucket, scope)
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else bucket
        if remap_bucket != bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_bucket(remap_bucket)
        self.rest.create_scope(remap_bucket, scope)
        self.rest.create_collection(remap_bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        backup_client.restore()
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_stats = self.rest.get_indexer_metadata()
        replica_indexes_after_restore = [
            index for index in indexer_stats['status']
            if index['bucket'] == remap_bucket and index['scope'] == scope
        ]
        self._verify_indexes(replica_indexes_before_backup,
                             replica_indexes_after_restore)

    def test_backup_restore_with_partition_index(self):
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        if len(indexer_nodes) < 2:
            self.fail("Atleast 2 indexer nodes required")
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.master, bucket)
        scope = "partition_scope"
        collection = "partition_collection"
        self.rest.create_scope(bucket, scope)
        self.rest.create_collection(bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        index_name = "partition_index"
        namespace = f"default:{bucket}.{scope}.{collection}"
        index_fields = ["firstName", "lastName"]
        query = "CREATE INDEX {0} on {1}({2}) partition by hash(meta().id) \
            with {{'num_partition':{3}}}".format(
                index_name, namespace, ",".join(index_fields),
                self.num_partitions)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_stats = self.rest.get_indexer_metadata()
        partition_indexes_before_backup = [
            index for index in indexer_stats['status']
            if index['bucket'] == bucket and index['scope'] == scope
        ]
        include_namespaces = [scope]
        backup_client.backup(include_namespaces)
        self.rest.delete_scope(bucket, scope)
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else bucket
        if remap_bucket != bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_bucket(remap_bucket)
        self.rest.create_scope(remap_bucket, scope)
        self.rest.create_collection(remap_bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        backup_client.restore()
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexer_stats = self.rest.get_indexer_metadata()
        partition_indexes_after_restore = [
            index for index in indexer_stats['status']
            if index['bucket'] == remap_bucket and index['scope'] == scope
        ]
        self._verify_indexes(partition_indexes_before_backup,
                             partition_indexes_after_restore)
