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

import random

from .base_gsi import BaseSecondaryIndexingTests
from lib.couchbase_helper.query_definitions import QueryDefinition
from lib.collection.collections_rest_client import CollectionsRest
from itertools import combinations, chain
from lib.remote.remote_util import RemoteMachineShellConnection
from lib import testconstants
from lib.collection.gsi.backup_restore_utils import IndexBackupClient
from membase.api.rest_client import RestHelper


class BackupRestoreTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(BackupRestoreTests, self).setUp()
        self.doc_size = self.input.param("doc_size", 100)
        self.bucket_remap = self.input.param("bucket_remap", False)
        self.num_partitions = self.input.param("num_partitions", 8)
        self.indexes_per_collection = self.input.param(
            "indexes_per_collection", 1)
        # use_cbbackupmgr False uses rest
        self.use_cbbackupmgr = self.input.param("use_cbbackupmgr", False)
        self.indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        if self.use_cbbackupmgr:
            for node in self.indexer_nodes:
                remote_client = RemoteMachineShellConnection(node)
                info = remote_client.extract_remote_info().type.lower()
                if info == 'linux':
                    self.backup_path = testconstants.LINUX_BACKUP_PATH
                elif info == 'windows':
                    self.backup_path = testconstants.WIN_BACKUP_C_PATH
                elif info == 'mac':
                    self.backup_path = testconstants.LINUX_BACKUP_PATH
                command = "rm -rf {0}".format(self.backup_path)
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
        self.collection_rest = CollectionsRest(self.master)
        self.collection_namespaces = []
        if not self.default_bucket:
            self._create_bucket_structure()
            self.buckets = self.rest.get_buckets()
            for bucket_num, bucket in enumerate(self.buckets):
                namespace = f'default:{bucket.name}'
                bucket_id = str(bucket_num + 1)
                index_name = "primary_idx_" + bucket_id
                primary_idx = QueryDefinition(index_name)
                query = primary_idx.generate_primary_index_create_query(
                    namespace, defer_build=self.defer_build)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=self.defer_build)
                scopes = self.rest.get_bucket_scopes(bucket.name)
                self.collection_namespaces = [
                    "default:{0}.{1}.{2}".format(
                        bucket.name, scope, collection)
                    for scope in scopes
                    for collection in
                    self.rest.get_scope_collections(bucket.name, scope)]
                for namespace_id, namespace in enumerate(
                        self.collection_namespaces):
                    for idx_id in range(1, self.indexes_per_collection + 1):
                        index_name =\
                            "idx_{0}_{1}_{2}".format(
                                bucket_id, namespace_id + 1, idx_id)
                        query_def = QueryDefinition(
                            index_name, index_fields=["firstName", "lastName"])
                        query = query_def.generate_index_create_query(
                            namespace=namespace, defer_build=self.defer_build)
                        self.run_cbq_query(query=query)
                        self.wait_until_indexes_online(
                            defer_build=self.defer_build)

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
        self.log.info(
            "indexes before backup:" + str(indexer_stats_before_backup))
        self.log.info(
            "indexes after restore:" + str(indexer_stats_after_restore))
        for idx_before_backup, idx_after_restore in zip(
                indexer_stats_before_backup, indexer_stats_after_restore):
            msg = "{0} != {1}".format(idx_before_backup, idx_after_restore)
            self.assertEqual(
                idx_before_backup['name'], idx_after_restore['name'],
                msg=msg)
            self.assertEqual(idx_before_backup.get('isPrimary', None),
                             idx_after_restore.get('isPrimary', None),
                             msg=msg)
            self.assertEqual(idx_before_backup.get('secExprs', None),
                             idx_after_restore.get('secExprs', None),
                             msg=msg)
            self.assertEqual(
                idx_before_backup['indexType'],
                idx_after_restore['indexType'], msg=msg)
            self.assertEqual(
                idx_after_restore['status'], "Created", msg=msg)
            self.assertEqual(idx_before_backup['numReplica'],
                             idx_after_restore['numReplica'], msg=msg)
            self.assertEqual(idx_before_backup['numPartition'],
                             idx_after_restore['numPartition'], msg=msg)
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
            IndexBackupClient(self.master, self.use_cbbackupmgr, bucket.name)
            for bucket in self.buckets]
        for backup_client in index_backup_clients:
            backup_result = backup_client.backup()
            self.assertTrue(
                backup_result[0],
                msg="Backup failed for {0} with {1}".format(
                    backup_client.backup_bucket, backup_result[1]))
        self.rest.delete_all_buckets()
        self._recreate_bucket_structure()
        for backup_client in index_backup_clients:
            restore_result = backup_client.restore()
            self.assertTrue(
                restore_result[0],
                msg="Restore failed for {0} with {1}".format(
                    backup_client.restore_bucket, restore_result[1]))

            if self.use_cbbackupmgr:
                backup_client.remove_backup()
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
                else "default:" + index['bucket']
            query = query_def.generate_index_drop_query(
                namespace=namespace)
            self.run_cbq_query(query)
            self.sleep(5, "wait for index to drop")

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

    def test_steady_state_backup_restore_with_include_scopes(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master,
                                              self.use_cbbackupmgr,
                                              bucket.name)
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
                backup_result = backup_client.backup(scopes)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        scopes, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                restore_result = backup_client.restore()
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        scopes, restore_result[1]))

                if self.use_cbbackupmgr:
                    backup_client.remove_backup()
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

    def test_steady_state_backup_restore_with_include_collections(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master,
                                              self.use_cbbackupmgr,
                                              bucket.name)
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
                    and "{0}.{1}".format(index['scope'], index['collection'])
                    in collection_namespaces]
                backup_result = backup_client.backup(collection_namespaces)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        collection_namespaces, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                restore_result = backup_client.restore()
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        collection_namespaces, restore_result[1]))

                if self.use_cbbackupmgr:
                    backup_client.remove_backup()
                indexer_stats_after_restore = self.rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                    and "{0}.{1}".format(index['scope'], index['collection'])
                    in collection_namespaces]
                self._verify_indexes(indexes_before_backup,
                                     indexes_after_restore)
                if not self.defer_build:
                    self._build_indexes(indexes_after_restore)

    def test_steady_state_backup_restore_with_exclude_scopes(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master,
                                              self.use_cbbackupmgr,
                                              bucket.name)
            scopes = self.rest.get_bucket_scopes(bucket.name)
            random.shuffle(scopes)
            scopes_combinations = self._powerset(scopes)
            for excluded_scopes in scopes_combinations:
                indexer_stats_before_backup = self.rest.get_indexer_metadata()
                backup_result = backup_client.backup(
                    excluded_scopes, include=False)
                self.assertTrue(
                    backup_result[0],
                    "restore failed for {0} with {1}".format(
                        scopes, backup_result[1]))
                backedup_scopes = [scope for scope in scopes
                                   if scope not in excluded_scopes]
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                    and index['scope'] in backedup_scopes]
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                restore_result = backup_client.restore()
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        scopes, restore_result[1]))

                if self.use_cbbackupmgr:
                    backup_client.remove_backup()
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

    def test_steady_state_backup_restore_with_exclude_collections(self):
        for bucket in self.buckets:
            backup_client = IndexBackupClient(self.master,
                                              self.use_cbbackupmgr,
                                              bucket.name)
            bucket_collection_namespaces = [
                namespace.split(".", 1)[1] for namespace
                in self.collection_namespaces
                if namespace.split(':')[1].split(".")[0] == bucket.name]
            random.shuffle(bucket_collection_namespaces)
            collection_namespaces_list = self._powerset(
                bucket_collection_namespaces)
            for collection_namespaces in collection_namespaces_list:
                indexer_stats_before_backup = self.rest.get_indexer_metadata()
                backup_result = backup_client.backup(
                    collection_namespaces, include=False)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        collection_namespaces, backup_result[1]))
                backedup_collections = [
                    collection_namespace for collection_namespace
                    in bucket_collection_namespaces
                    if collection_namespace not in collection_namespaces]
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                    and "{0}.{1}".format(index['scope'], index['collection'])
                    in backedup_collections]
                self._drop_indexes(indexes_before_backup)
                self.sleep(5, "Wait for indexes to drop")
                restore_result = backup_client.restore()
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        collection_namespaces, restore_result[1]))

                if self.use_cbbackupmgr:
                    backup_client.remove_backup()
                indexer_stats_after_restore = self.rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                    and "{0}.{1}".format(index['scope'], index['collection'])
                    in backedup_collections]
                self._verify_indexes(
                    indexes_before_backup, indexes_after_restore)
                if not self.defer_build:
                    self._build_indexes(indexes_after_restore)

    def test_steady_state_backup_restore_with_bucket_remap(self):
        original_bucket = self.buckets[0].name
        indexer_metadata = self.rest.get_indexer_metadata()
        indexes_bucket_1 = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == original_bucket]
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          original_bucket)
        backup_result = backup_client.backup()
        self.assertTrue(backup_result[0], str(backup_result[1]))
        remap_bucket = "remap_bucket"
        self._recreate_bucket_structure(remap_bucket)
        backup_client.set_restore_bucket(remap_bucket)
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexer_metadata = self.rest.get_indexer_metadata()
        indexes_bucket_2 = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket]
        self._verify_indexes(indexes_bucket_1, indexes_bucket_2)

    def test_steady_state_backup_restore_with_scope_remap(self):
        original_bucket = self.buckets[0].name
        scopes = self.rest.get_bucket_scopes(original_bucket)
        original_scope = scopes[0]
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          original_bucket)
        indexer_metadata = self.rest.get_indexer_metadata()
        original_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == original_bucket
            and index['scope'] == original_scope]
        backup_result = backup_client.backup(namespaces=[original_scope])
        self.assertTrue(backup_result[0], str(backup_result[1]))
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else original_bucket
        if remap_bucket != original_bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_restore_bucket(remap_bucket)
        remap_scope = "remap_scope"
        self.rest.create_scope(remap_bucket, remap_scope)
        for collection_id in range(1, self.num_collections + 1):
            remap_collection = "{0}_{1}".format(
                self.collection_prefix, collection_id)
            self.rest.create_collection(
                remap_bucket, remap_scope, remap_collection)
        self.sleep(5, "Allow scope-collection to be online")
        mappings = ["{0}:{1}".format(original_scope, remap_scope)]
        if self.bucket_remap:
            self.rest.delete_bucket(backup_client.backup_bucket)
        restore_result = backup_client.restore(mappings)
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexer_metadata = self.rest.get_indexer_metadata()
        remapped_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket
            and index['scope'] == remap_scope]
        self._verify_indexes(original_indexes, remapped_indexes)

    def test_steady_state_backup_restore_with_collection_remap(self):
        original_bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          original_bucket)
        scopes = self.rest.get_bucket_scopes(original_bucket)
        original_scope = scopes[0]
        indexer_metadata = self.rest.get_indexer_metadata()
        original_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == original_bucket
            and index['scope'] == original_scope]
        backup_result = backup_client.backup(namespaces=[original_scope])
        self.assertTrue(backup_result[0], str(backup_result[1]))
        collections = self.rest.get_scope_collections(
            original_bucket, original_scope)
        original_namespaces = [
            "{0}.{1}".format(original_scope, collection)
            for collection in collections]
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else original_bucket
        if remap_bucket != original_bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_restore_bucket(remap_bucket)
        remap_scope = "remap_scope"
        remap_namespaces = []
        self.rest.create_scope(remap_bucket, remap_scope)
        for collection_id in range(1, self.num_collections + 1):
            remap_collection = "remap_collection_" + str(collection_id)
            self.rest.create_collection(
                remap_bucket, remap_scope, remap_collection)
            remap_namespaces.append("{0}.{1}".format(
                remap_scope, remap_collection))
        self.sleep(5, "Allow scope-collection to be online")
        mappings = [
            "{0}:{1}".format(original_namespace, remap_namespace)
            for original_namespace, remap_namespace
            in zip(original_namespaces, remap_namespaces)]
        if self.bucket_remap:
            self.rest.delete_bucket(backup_client.backup_bucket)
        restore_result = backup_client.restore(mappings)
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexer_metadata = self.rest.get_indexer_metadata()
        remapped_indexes = [
            index
            for index in indexer_metadata['status']
            if index['bucket'] == remap_bucket
            and index['scope'] == remap_scope]
        self._verify_indexes(original_indexes, remapped_indexes)

    def test_steady_state_backup_restore_with_scopes_and_collections_remap(
            self):
        if self.num_scopes < 2:
            self.fail("Atleast 2 scopes needed")
        original_bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          original_bucket)
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
        original_namespace = "{0}.{1}".format(scopes[1], collections[0])
        backup_result = backup_client.backup(namespaces=include_namespaces)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self.rest.delete_scope(original_bucket, scopes[1])
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else original_bucket
        if remap_bucket != original_bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_restore_bucket(remap_bucket)
        self.rest.create_scope(remap_bucket, scopes[1])
        self.rest.create_collection(remap_bucket, scopes[1], collections[1])
        remap_scope_1 = 'remap_scope_1'
        self.rest.create_scope(remap_bucket, remap_scope_1)
        for collection_id in range(1, self.num_collections + 1):
            collection = "{0}_{1}".format(
                self.collection_prefix, collection_id)
            self.rest.create_collection(
                remap_bucket, remap_scope_1, collection)
        self.sleep(5, "Allow scope-collection to be online")
        mappings = [f'{scopes[0]}:{remap_scope_1}']
        remap_scope_2 = 'remap_scope_2'
        remap_collection = "remap_collection"
        remap_namespace = "{0}.{1}".format(remap_scope_2, remap_collection)
        self.rest.create_scope(remap_bucket, remap_scope_2)
        self.rest.create_collection(
            remap_bucket, remap_scope_2, remap_collection)
        self.sleep(5, "Allow scope-collection to be online")
        mappings.append(f'{original_namespace}:{remap_namespace}')
        if self.bucket_remap:
            self.rest.delete_bucket(backup_client.backup_bucket)
        restore_result = backup_client.restore(mappings)
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
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
        num_replicas = len(self.indexer_nodes) - 1
        if num_replicas < 1:
            self.fail("At least 2 indexer nodes required for this test")
        bucket = self.buckets[0].name
        scope = "replica_scope"
        collection = "replica_collection"
        self.rest.create_scope(bucket, scope)
        self.rest.create_collection(bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        index_name = "replica_index"
        namespace = "default:{0}.{1}.{2}".format(bucket, scope, collection)
        index_fields = ["firstName", "lastName"]
        index_def = QueryDefinition(
            index_name, index_fields=index_fields)
        query = index_def.generate_index_create_query(
            namespace=namespace,
            defer_build=self.defer_build, num_replica=num_replicas)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          bucket)
        include_namespaces = [scope]
        indexer_stats = self.rest.get_indexer_metadata()
        replica_indexes_before_backup = [
            index for index in indexer_stats['status']
            if index['bucket'] == bucket and index['scope'] == scope
        ]
        backup_result = backup_client.backup(include_namespaces)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self.rest.delete_scope(bucket, scope)
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else bucket
        if remap_bucket != bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_restore_bucket(remap_bucket)
        self.rest.create_scope(remap_bucket, scope)
        self.rest.create_collection(remap_bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        if self.bucket_remap:
            self.rest.delete_bucket(backup_client.backup_bucket)
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexer_stats = self.rest.get_indexer_metadata()
        replica_indexes_after_restore = [
            index for index in indexer_stats['status']
            if index['bucket'] == remap_bucket and index['scope'] == scope
        ]
        self._verify_indexes(replica_indexes_before_backup,
                             replica_indexes_after_restore)

    def test_backup_restore_with_partition_index(self):
        if len(self.indexer_nodes) < 2:
            self.fail("Atleast 2 indexer nodes required")
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          bucket)
        scope = "partition_scope"
        collection = "partition_collection"
        self.rest.create_scope(bucket, scope)
        self.rest.create_collection(bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        index_name = "partition_index"
        namespace = "default:{0}.{1}.{2}".format(bucket, scope, collection)
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
        backup_result = backup_client.backup(include_namespaces)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self.rest.delete_scope(bucket, scope)
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else bucket
        if remap_bucket != bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_restore_bucket(remap_bucket)
        self.rest.create_scope(remap_bucket, scope)
        self.rest.create_collection(remap_bucket, scope, collection)
        self.sleep(5, "Allow scope-collection to be online")
        if self.bucket_remap:
            self.rest.delete_bucket(backup_client.backup_bucket)
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexer_stats = self.rest.get_indexer_metadata()
        partition_indexes_after_restore = [
            index for index in indexer_stats['status']
            if index['bucket'] == remap_bucket and index['scope'] == scope
        ]
        self._verify_indexes(partition_indexes_before_backup,
                             partition_indexes_after_restore)

    def test_backup_restore_with_rebalance_and_replica_index(self):
        """
        create a index with replica
        take backup
        remove a node
        restore
        verify indexes
        """
        if len(self.indexer_nodes) < 2:
            self.fail("Atleast 2 indexer required")
        if self.num_index_replicas == 0 or\
                self.num_index_replicas >= len(self.indexer_nodes):
            self.num_index_replicas = len(self.indexer_nodes) - 1
        replica_index = QueryDefinition(
            "replica_index",
            index_fields=["firstName"])
        bucket = self.buckets[0].name
        query = replica_index.generate_index_create_query(
            namespace="default:" + bucket,
            deploy_node_info=[
                node.ip + ":" + node.port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replicas
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        backup_client = IndexBackupClient(self.indexer_nodes[0],
                                          self.use_cbbackupmgr,
                                          bucket)
        indexer_stats_before_backup = self.rest.get_indexer_metadata()
        replica_indexes_before_backup = [
            index for index in indexer_stats_before_backup['status']
            if index['name'].startswith(replica_index.index_name)]
        self.assertEqual(len(replica_indexes_before_backup), 2)
        host = "{0}:{1}".format(self.master.ip, self.master.port)
        indexes_before_backup = [
            index for index in replica_indexes_before_backup
            if host in index['hosts']]
        namespaces = ["_default._default"]
        backup_result = backup_client.backup(namespaces=namespaces)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        query = replica_index.generate_index_drop_query(
            namespace="default:" + bucket)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        out_nodes = [
            node for node in self.indexer_nodes if self.master.ip != node.ip]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexes_after_restore = [
            index for index in self.rest.get_indexer_metadata()['status']
            if index['name'].startswith(replica_index.index_name)]
        self.assertEqual(len(indexes_after_restore), 1)
        self._verify_indexes(indexes_before_backup,
                             indexes_after_restore)
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], out_nodes, [],
            services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        replica_indexes_after_restore = [
            index for index in self.rest.get_indexer_metadata()['status']
            if index['name'].startswith(replica_index.index_name)]
        self._verify_indexes(replica_indexes_before_backup,
                             replica_indexes_after_restore)

    def test_backup_restore_with_rebalance_and_partition_index(self):
        """
        create a index with partitions
        take backup
        remove a node
        restore
        verify indexes
        """
        partition_fields = ['meta().id']
        partition_index = QueryDefinition(
            "partition_index",
            index_fields=["firstName"])
        bucket = self.buckets[0].name
        query = partition_index.generate_index_create_query(
            namespace="default:" + bucket,
            deploy_node_info=[
                node.ip + ":" + node.port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            partition_by_fields=partition_fields
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        backup_client = IndexBackupClient(self.indexer_nodes[0],
                                          self.use_cbbackupmgr,
                                          bucket)
        indexes_before_backup = [
            index for index in self.rest.get_indexer_metadata()['status']
            if index['name'].startswith(partition_index.index_name)]
        namespaces = ["_default._default"]
        backup_result = backup_client.backup(namespaces=namespaces)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        query = partition_index.generate_index_drop_query(
            namespace="default:" + bucket)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        out_nodes = [
            node for node in self.indexer_nodes if self.master.ip != node.ip]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        if not reached:
            self.fail(msg="Rebalance failed")
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexes_after_restore = [
            index for index in self.rest.get_indexer_metadata()['status']
            if index['name'].startswith(partition_index.index_name)]
        self._verify_indexes(indexes_before_backup,
                             indexes_after_restore)

    def test_backup_restore_with_duplicate_partition_index_name(self):
        """
        https://issues.couchbase.com/browse/MB-43169
        """
        bucket = self.buckets[0].name
        scope = self.rest.get_bucket_scopes(bucket)[0]
        collection = self.rest.get_scope_collections(bucket, scope)[0]
        partition_fields = ['meta().id']
        partition_index = QueryDefinition(
            "partition_index",
            index_fields=["firstName"])
        namespace = "default:{0}.{1}.{2}".format(bucket, scope, collection)
        query = partition_index.generate_index_create_query(
            namespace=namespace,
            deploy_node_info=[
                node.ip + ":" + node.port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            partition_by_fields=partition_fields
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        basic_index = QueryDefinition(
            "basic_index",
            index_fields=["firstName"])
        query = basic_index.generate_index_create_query(
            namespace=namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        backup_client = IndexBackupClient(self.indexer_nodes[0],
                                          self.use_cbbackupmgr,
                                          bucket)
        namespaces = ["{0}.{1}".format(scope, collection)]
        index_stats_before_backup = self.rest.get_indexer_metadata()
        partition_indexes_before_backup = list(filter(
            lambda index: index['name'].startswith(partition_index.index_name),
            index_stats_before_backup['status']))
        basic_indexes_before_backup = list(filter(
            lambda index: index['name'].startswith(basic_index.index_name),
            index_stats_before_backup['status']))
        backup_result = backup_client.backup(namespaces=namespaces)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        query = partition_index.generate_index_drop_query(namespace=namespace)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        query = basic_index.generate_index_drop_query(namespace=namespace)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        partition_index.partition_by_fields = []
        query = partition_index.generate_index_create_query(
            namespace=namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        query = basic_index.generate_index_create_query(
            namespace=namespace,
            deploy_node_info=[
                node.ip + ":" + node.port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            partition_by_fields=partition_fields
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        for i in range(10):
            restore_result = backup_client.restore()
            self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        index_stats_after_restore = self.rest.get_indexer_metadata()
        partition_indexes_after_restore = list(filter(
            lambda index: index['name'].startswith(partition_index.index_name),
            index_stats_after_restore['status']))
        basic_indexes_after_restore = list(filter(
            lambda index: index['name'].startswith(basic_index.index_name),
            index_stats_after_restore['status']))
        msg = "indexes before backup: {0}\nindexes after restore: {1}".format(
            partition_indexes_before_backup, partition_indexes_after_restore)
        self.assertEqual(
            len(partition_indexes_before_backup),
            len(partition_indexes_after_restore), msg=msg)
        msg = "indexes before backup: {0}\nindexes after restore: {1}".format(
            partition_indexes_before_backup, partition_indexes_after_restore)
        self.assertEqual(
            len(basic_indexes_before_backup),
            len(basic_indexes_after_restore), msg=msg)

    def test_backup_restore_with_duplicate_replica_index_name(self):
        if len(self.indexer_nodes) < 2:
            self.fail("Atleast 2 indexer required")
        if self.num_index_replicas == 0 or\
                self.num_index_replicas >= len(self.indexer_nodes):
            self.num_index_replicas = len(self.indexer_nodes) - 1
        bucket = self.buckets[0].name
        scope = self.rest.get_bucket_scopes(bucket)[0]
        collection = self.rest.get_scope_collections(bucket, scope)[0]
        replica_index = QueryDefinition(
            "replica_index",
            index_fields=["firstName"])
        basic_index = QueryDefinition(
            "basic_index",
            index_fields=["firstName"])
        namespace = "default:{0}.{1}.{2}".format(bucket, scope, collection)
        query = replica_index.generate_index_create_query(
            namespace=namespace,
            deploy_node_info=[
                node.ip + ":" + node.port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replicas
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        query = basic_index.generate_index_create_query(
            namespace=namespace,
            defer_build=self.defer_build
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        backup_client = IndexBackupClient(self.indexer_nodes[0],
                                          self.use_cbbackupmgr,
                                          bucket)
        namespaces = ["{0}.{1}".format(scope, collection)]
        index_stats_before_backup = self.rest.get_indexer_metadata()
        replica_indexes_before_backup = list(filter(
            lambda index: index['name'].startswith(replica_index.index_name),
            index_stats_before_backup['status']))
        basic_indexes_before_backup = list(filter(
            lambda index: index['name'].startswith(basic_index.index_name),
            index_stats_before_backup['status']))
        backup_result = backup_client.backup(namespaces=namespaces)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        query = replica_index.generate_index_drop_query(namespace=namespace)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        query = basic_index.generate_index_drop_query(namespace=namespace)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        query = replica_index.generate_index_create_query(
            namespace=namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        query = basic_index.generate_index_create_query(
            namespace=namespace,
            deploy_node_info=[
                node.ip + ":" + node.port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replicas
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        for i in range(10):
            restore_result = backup_client.restore()
            self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        index_stats_after_restore = self.rest.get_indexer_metadata()
        replica_indexes_after_restore = list(filter(
            lambda index: index['name'].startswith(replica_index.index_name),
            index_stats_after_restore['status']))
        basic_indexes_after_restore = list(filter(
            lambda index: index['name'].startswith(basic_index.index_name),
            index_stats_after_restore['status']))
        msg = "indexes before backup: {0}\nindexes after restore: {1}".format(
            replica_indexes_before_backup, replica_indexes_after_restore)
        self.assertLess(
            len(replica_indexes_after_restore),
            len(replica_indexes_before_backup), msg=msg)
        msg = "indexes before backup: {0}\nindexes after restore: {1}".format(
            basic_indexes_before_backup, basic_indexes_after_restore)
        self.assertEqual(len(basic_indexes_after_restore),
                         self.num_index_replicas + 1, msg=msg)

    def test_backup_restore_with_duplicate_index_name_different_fields(self):
        """
        https://issues.couchbase.com/browse/MB-43169
        """
        indexer_stats_before_backup = self.rest.get_indexer_metadata()
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.indexer_nodes[0],
                                          self.use_cbbackupmgr,
                                          bucket)
        backup_result = backup_client.backup()
        self.assertTrue(backup_result[0], str(backup_result[1]))
        index_fields = ['`newField1`', '`newField2`']
        self._drop_indexes(indexer_stats_before_backup['status'])
        for index in indexer_stats_before_backup['status']:
            idx_def = QueryDefinition(index['name'],
                                      index_fields=index_fields)
            namespace = index['bucket'] if index['scope'] == "_default"\
                and index['collection'] == "_default"\
                else "{0}.{1}.{2}".format(
                    index['bucket'], index['scope'], index['collection'])
            query = idx_def.generate_index_create_query(
                namespace=namespace, defer_build=self.defer_build)
            self.run_cbq_query(query)
            self.sleep(5, "wait for index to create")
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        indexer_stats_after_restore = self.rest.get_indexer_metadata()
        self.assertEqual(
            len(indexer_stats_after_restore['status']),
            len(indexer_stats_before_backup['status']))
        for idx_after in indexer_stats_after_restore['status']:
            msg = str(idx_after)
            self.assertEqual(set(idx_after['secExprs']), set(index_fields),
                             msg=msg)

    def test_backup_restore_while_rebalancing(self):
        """
        https://issues.couchbase.com/browse/MB-43276
        """
        out_nodes = [
            node for node in self.indexer_nodes if self.master.ip != node.ip]
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.indexer_nodes[0],
                                          self.use_cbbackupmgr,
                                          bucket)
        indexer_stats_before_backup = self.rest.get_indexer_metadata()
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        backup_result = backup_client.backup()
        self.assertTrue(backup_result[0], str(backup_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self._drop_indexes(indexer_stats_before_backup['status'])
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], out_nodes, [],
            services=["index"])
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        indexer_stats_after_restore = self.rest.get_indexer_metadata()
        self._verify_indexes(indexer_stats_before_backup['status'],
                             indexer_stats_after_restore['status'])
        indexer_stats_before_backup = indexer_stats_after_restore
        if self.use_cbbackupmgr:
            backup_client.remove_backup_repo()
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], out_nodes, [],
            services=["index"])
        backup_result = backup_client.backup()
        self.assertTrue(backup_result[0], str(backup_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        indexer_stats_after_restore = self.rest.get_indexer_metadata()
        self._verify_indexes(indexer_stats_before_backup['status'],
                             indexer_stats_after_restore['status'])

    def test_backup_restore_with_alter_index(self):
        num_replicas = len(self.indexer_nodes) - 1
        replica_index = QueryDefinition(
            "replica_index",
            index_fields=["firstName"])
        namespace_id = random.randint(0, len(self.collection_namespaces) - 1)
        namespace = self.collection_namespaces[namespace_id]
        query = replica_index.generate_index_create_query(
            namespace=namespace,
            deploy_node_info=[
                node.ip + ":" + node.port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_replicas
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        self.alter_index_replicas(replica_index.index_name,
                                  namespace=namespace,
                                  num_replicas=num_replicas - 1)
        bucket = namespace.split(":")[1].split(".")[0]
        backup_client = IndexBackupClient(self.master, self.use_cbbackupmgr,
                                          bucket)
        indexer_status_before_backup = self.rest.get_indexer_metadata()
        self.log.info(indexer_status_before_backup['status'])
        backup_result = backup_client.backup()
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self._drop_indexes(indexer_status_before_backup['status'])
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        indexer_status_after_restore = self.rest.get_indexer_metadata()
        self.log.info(indexer_status_after_restore['status'])
        replica_indexes = [
            idx
            for idx in indexer_status_after_restore['status']
            if idx['name'].startswith(replica_index.index_name)]
        self.assertEqual(len(replica_indexes), num_replicas)
        for idx in replica_indexes:
            self.assertEqual(idx['numReplica'], num_replicas - 1)
        self._verify_indexes(indexer_status_before_backup['status'],
                             indexer_status_after_restore['status'])
        for idx in replica_indexes:
            self.alter_index_replicas(replica_index.index_name,
                                      namespace=namespace,
                                      action="drop_replica",
                                      replica_id=idx['replicaId'])
        self.sleep(10, "wait for indexes to drop")
        indexer_status_before_backup = self.rest.get_indexer_metadata()
        backup_result = backup_client.backup()
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self._drop_indexes(indexer_status_before_backup['status'])
        restore_result = backup_client.restore()
        self.assertTrue(restore_result[0], str(restore_result[1]))
        indexer_status_after_restore = self.rest.get_indexer_metadata()
        replica_indexes = [
            idx
            for idx in indexer_status_after_restore['status']
            if idx['name'].startswith(replica_index.index_name)
        ]
        self.assertEqual(replica_indexes, [])
        self._verify_indexes(indexer_status_before_backup['status'],
                             indexer_status_after_restore['status'])

    def test_backup_restore_with_error_index_state(self):
        non_master_nodes = [
            node
            for node in self.indexer_nodes if node.ip != self.master.ip]
        if len(non_master_nodes) != 1:
            self.fail("Exactly 2 nodes required for the test")
        indexer_stats_before_backup = self.rest.get_indexer_metadata()
        doc = {"indexer.scheduleCreateRetries": 1}
        self.rest.set_index_settings(doc)
        backup_clients = []
        try:
            self.block_incoming_network_from_node(
                self.master, non_master_nodes[0])
            self.sleep(30, "wait node to be blocked")
            for bucket in self.buckets:
                backup_client = IndexBackupClient(
                    self.master, self.use_cbbackupmgr, bucket.name)
                backup_result = backup_client.backup()
                self.assertFalse(
                    backup_result[0],
                    "Backup failed {0} with {1}".format(
                        bucket.name, backup_result[1]))
                backup_clients.append(backup_client)
        except Exception:
            raise
        finally:
            self.resume_blocked_incoming_network_from_node(
                self.master, non_master_nodes[0])
            self.sleep(30, "wait node to be unblocked")
        for backup_client in backup_clients:
            backup_result = backup_client.backup(backup_args="--resume")
            self.assertTrue(backup_result[0], str(backup_result[1]))
        self._drop_indexes(indexer_stats_before_backup['status'])
        try:
            self.block_incoming_network_from_node(
                self.master, non_master_nodes[0])
            self.sleep(10, "wait node to be blocked")
            for backup_client in backup_clients:
                restore_result = backup_client.restore()
                self.assertFalse(restore_result[0], str(restore_result[1]))
        except Exception:
            raise
        finally:
            self.resume_blocked_incoming_network_from_node(
                self.master, non_master_nodes[0])
            self.sleep(10, "wait node to be unblocked")
        for backup_client in backup_clients:
            restore_result = backup_client.restore()
            self.assertTrue(restore_result[0], str(restore_result[1]))
            if self.use_cbbackupmgr:
                backup_client.remove_backup()
        indexer_stats_after_restore = self.rest.get_indexer_metadata()
        self._verify_indexes(indexer_stats_before_backup['status'],
                             indexer_stats_after_restore['status'])
