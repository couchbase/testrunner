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
import time
import math
import os

from .base_gsi import BaseSecondaryIndexingTests
from lib.couchbase_helper.query_definitions import QueryDefinition
from lib.collection.collections_rest_client import CollectionsRest
from itertools import combinations, chain
from lib.remote.remote_util import RemoteMachineShellConnection
from lib import testconstants
from lib.collection.gsi.backup_restore_utils import IndexBackupClient
from membase.api.rest_client import RestHelper
from concurrent.futures import ThreadPoolExecutor
from deepdiff import DeepDiff
from membase.api.rest_client import RestConnection

class BackupRestoreTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(BackupRestoreTests, self).setUp()
        self.doc_size = self.input.param("doc_size", 100)
        self.bucket_remap = self.input.param("bucket_remap", False)
        self.num_partitions = self.input.param("num_partitions", 8)
        self.indexes_per_collection = self.input.param(
            "indexes_per_collection", 1)
        self.decrease_node_count = self.input.param("decrease_node_count", 0)
        self.create_index_on_n_nodes = self.input.param("create_index_on_n_nodes", None)
        self.rebalance_in = self.input.param("rebalance_in", False)
        self.rebalance_out = self.input.param("rebalance_out", False)
        self.rebalance_swap = self.input.param("rebalance_swap", False)
        # use_cbbackupmgr False uses rest
        self.use_cbbackupmgr = self.input.param("use_cbbackupmgr", False)
        self.indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        if self.use_shard_based_rebalance:
            self.enable_shard_based_rebalance()
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
                keyspace = f'default:{bucket.name}'
                bucket_id = str(bucket_num + 1)
                index_name = "primary_idx_" + bucket_id
                primary_idx = QueryDefinition(index_name)
                query = primary_idx.generate_primary_index_create_query(
                    keyspace, defer_build=self.defer_build)
                self.run_cbq_query(query=query)
                self.wait_until_indexes_online(defer_build=self.defer_build)
                scopes = self.rest.get_bucket_scopes(bucket.name)
                bucket_namespaces = [
                    "default:{0}.{1}.{2}".format(
                        bucket.name, scope, collection)
                    for scope in scopes
                    for collection in
                    self.rest.get_scope_collections(bucket.name, scope)]
                for namespace_id, keyspace in enumerate(
                        bucket_namespaces):
                    namespace_tokens = keyspace.split(".", 1)
                    if namespace_tokens[1] == "_default._default":
                        keyspace = namespace_tokens[0]
                    for idx_id in range(1, self.indexes_per_collection + 1):
                        index_name = \
                            "idx_{0}_{1}_{2}".format(
                                bucket_id, namespace_id + 1, idx_id)
                        query_def = QueryDefinition(
                            index_name, index_fields=["firstName", "lastName"])
                        query = query_def.generate_index_create_query(
                            namespace=keyspace, defer_build=self.defer_build)
                        self.run_cbq_query(query=query)
                        self.wait_until_indexes_online(
                            defer_build=self.defer_build)
                self.collection_namespaces.extend(bucket_namespaces)

        self.log.info("Enabling debug logs for Indexer")
        debug_setting = {"indexer.settings.log_level": "debug"}
        client_debug_setting = {"queryport.client.log_level": "debug"}
        self.index_rest.set_index_settings(debug_setting)
        self.index_rest.set_index_settings(client_debug_setting)
        self.schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        self.schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}

    def tearDown(self):
        super(BackupRestoreTests, self).tearDown()

    def _create_bucket_structure(self):
        self.collection_rest.create_buckets_scopes_collections(
            self.num_buckets, self.num_scopes, self.num_collections,
            self.bucket_size, self.test_bucket, self.scope_prefix,
            self.collection_prefix)

    def _recreate_bucket_structure(self, bucket=""):
        if self.default_bucket:
            self.rest.create_bucket(self.buckets[0].name, ramQuotaMB=self.bucket_size)
        elif bucket:
            self.rest.create_bucket(bucket, ramQuotaMB=self.bucket_size)
            self.collection_rest.create_scope_collection_count(
                self.num_scopes, self.num_collections, self.scope_prefix,
                self.collection_prefix, bucket)
        else:
            self._create_bucket_structure()
        self.sleep(
            5, message="Wait for buckets-scopes-collections to come online")

    def _verify_indexes(self, indexer_stats_before_backup, indexer_stats_after_restore, new_indexes=False):
        self.log.info(
            "indexes before backup:" + str(indexer_stats_before_backup))
        self.log.info(
            "indexes after restore:" + str(indexer_stats_after_restore))
        if not new_indexes:
            self.assertEqual(
                len(indexer_stats_before_backup), len(indexer_stats_after_restore))
            for idx_before_backup, idx_after_restore in zip(
                    indexer_stats_before_backup, indexer_stats_after_restore):
                msg = "\n{0} \n {1}".format(idx_before_backup, idx_after_restore)
                self.assertEqual(
                    idx_before_backup['indexName'], idx_after_restore['indexName'],
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
        else:
            indexes_dict = {}
            for index in indexer_stats_after_restore:
                name = f"{index['bucket']}.{index['scope']}.{index['collection']}{index['name']}"
                indexes_dict[name] = {}
                indexes_dict[name]['numReplica'] = index['numReplica']
                indexes_dict[name]['indexType'] = index['indexType']
                indexes_dict[name]['numPartition'] = index['numPartition']
                if 'isPrimary' in index:
                    indexes_dict[name]['isPrimary'] = index['isPrimary']
                if 'secExprs' in index:
                    indexes_dict[name]['secExprs'] = index['secExprs']
                indexes_dict[name]['status'] = index['status']
            for index in indexer_stats_before_backup:
                name = f"{index['bucket']}.{index['scope']}.{index['collection']}{index['name']}"
                if name not in indexes_dict:
                    raise Exception(f"Index missing after restore {index['name']}")
                if int(index['numReplica']) != int(indexes_dict[name]['numReplica']):
                    raise Exception("Num replica mismatch after restore")
                if int(index['numPartition']) != int(indexes_dict[name]['numPartition']):
                    raise Exception("Num partition mismatch after restore")
                if index['indexType'] != indexes_dict[name]['indexType']:
                    raise Exception("Index type mismatch after restore")
                if 'isPrimary' in index:
                    if index['isPrimary'] != indexes_dict[name]['isPrimary']:
                        raise Exception("IsPrimary mismatch after restore")
                if 'secExprs' in index:
                    if index['secExprs'] != indexes_dict[name]['secExprs']:
                        raise Exception("secExprs mismatch after restore")
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
        index_node = self.get_nodes_from_services_map(service_type="index")
        index_backup_clients = [
            IndexBackupClient(index_node, self.use_cbbackupmgr, bucket.name)
            for bucket in self.buckets]
        for backup_client in index_backup_clients:
            backup_result = backup_client.backup(use_https=self.use_https)
            self.assertTrue(
                backup_result[0],
                msg="Backup failed for {0} with {1}".format(
                    backup_client.backup_bucket, backup_result[1]))
        self.rest.delete_all_buckets()
        self._recreate_bucket_structure()
        while True:
            result = self.run_cbq_query("select * from system:indexes")['results']
            indexer_metadata = self.rest.get_indexer_metadata()
            if 'status' not in indexer_metadata and not result:
                break
            self.sleep(10, "Waiting for all the indexes to get removed")
        self.log.info(f"System Indexes: {result}")
        self.log.info(f"Indexes Metadata: {indexer_metadata}")
        for backup_client in index_backup_clients:
            restore_result = backup_client.restore(use_https=self.use_https)
            self.assertTrue(
                restore_result[0],
                msg="Restore failed for {0} with {1}".format(
                    backup_client.restore_bucket, restore_result[1]))
        indexer_stats_after_restore = self.rest.get_indexer_metadata()
        self._verify_indexes(indexer_stats_before_backup['status'],
                             indexer_stats_after_restore['status'])
        for backup_client in index_backup_clients:
            if self.use_cbbackupmgr:
                backup_client.remove_backup()

    def _drop_indexes(self, indexes):
        n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        timeout = time.time() + 600
        for index in indexes:
            if index['replicaId'] == 0:
                query_def = QueryDefinition(index['name'])
                namespace = "default:{0}.{1}.{2}".format(
                    index['bucket'], index['scope'], index['collection'])\
                    if index['scope'] != "_default"\
                    and index['collection'] != "_default"\
                    else "default:" + index['bucket']
                query = query_def.generate_index_drop_query(
                    namespace=namespace)
                try:
                    self.run_cbq_query(query=query, server=n1ql_server)
                except Exception as e:
                    error_msg = e.__str__()
                    if not "The index was scheduled for background creation.The cleanup will happen in the background" in error_msg:
                        stats = self.index_rest.get_indexer_metadata()['status']
                while time.time() < timeout:
                    try:
                        index_stats =\
                            self.index_rest.get_indexer_metadata()['status']
                    except KeyError as e:
                        error_msg = e.__str__()
                        self.log.info(error_msg)
                        if error_msg == "'status'":
                            break
                    if not [idx for idx in index_stats
                            if idx['name'] == index['name'] and
                            idx['bucket'] == index['bucket'] and
                            idx['scope'] == index['scope'] and
                            idx['collection'] == index['collection']]:
                        break
                    self.sleep(10, "waiting for index to drop and cleanup...")

    def _build_indexes(self, indexes):
        indexes_dict = {}
        for index in indexes:
            namespace = f"default:`{index['bucket']}`"
            if index['scope'] != "_default"\
                    and index['collection'] != "_default":
                namespace += ".`{0}`.`{1}`".format(
                    index['scope'],
                    index['collection'])
            if "replica" in index['name']:
                index_name = index['name'].split()[0]
            else:
                index_name = index['name']
            if namespace in indexes_dict:
                indexes_dict[namespace].append(f"`{index_name}`")
            else:
                indexes_dict[namespace] = [f"`{index_name}`"]
        for namespace, indexes in indexes_dict.items():
            build_task = self.async_build_index(namespace, indexes)
            try:
                build_task.result()
            except Exception as err:
                if "transient error" in str(err):
                    self.log.info("Build index encountered transient error. Ignoring the error")
                else:
                    raise Exception("Build index failure")

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
                backup_result = backup_client.backup(scopes, use_https=self.use_https)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        scopes, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                restore_result = backup_client.restore(use_https=self.use_https)
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
                backup_result = backup_client.backup(collection_namespaces, use_https=self.use_https)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        collection_namespaces, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                restore_result = backup_client.restore(use_https=self.use_https)
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
                    excluded_scopes, include=False, use_https=self.use_https)
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
                restore_result = backup_client.restore(use_https=self.use_https)
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
                    collection_namespaces, include=False, use_https=self.use_https)
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
                restore_result = backup_client.restore(use_https=self.use_https)
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
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        remap_bucket = "remap_bucket"
        self._recreate_bucket_structure(remap_bucket)
        backup_client.set_restore_bucket(remap_bucket)
        restore_result = backup_client.restore(use_https=self.use_https)
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
        backup_result = backup_client.backup(namespaces=[original_scope], use_https=self.use_https)
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
        restore_result = backup_client.restore(mappings, use_https=self.use_https)
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
        backup_result = backup_client.backup(namespaces=[original_scope], use_https=self.use_https)
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
        restore_result = backup_client.restore(mappings, use_https=self.use_https)
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
        backup_result = backup_client.backup(namespaces=include_namespaces, use_https=self.use_https)
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
        restore_result = backup_client.restore(mappings, use_https=self.use_https)
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
        backup_result = backup_client.backup(include_namespaces, use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self.rest.delete_scope(bucket, scope)
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else bucket
        if remap_bucket != bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_restore_bucket(remap_bucket)
        if self.bucket_remap:
            self.rest.delete_bucket(backup_client.backup_bucket)
        restore_result = backup_client.restore(use_https=self.use_https)
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
        backup_result = backup_client.backup(include_namespaces, use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self.rest.delete_scope(bucket, scope)
        remap_bucket = "remap_bucket" if self.bucket_remap\
                       else bucket
        if remap_bucket != bucket:
            self.rest.create_bucket(remap_bucket, ramQuotaMB=self.bucket_size)
            backup_client.set_restore_bucket(remap_bucket)
        if self.bucket_remap:
            self.rest.delete_bucket(backup_client.backup_bucket)
        restore_result = backup_client.restore(use_https=self.use_https)
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
        if self.num_index_replica == 0 or\
                self.num_index_replica >= len(self.indexer_nodes):
            self.num_index_replica = len(self.indexer_nodes) - 1
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        replica_index = QueryDefinition(
            "replica_index",
            index_fields=["firstName"])
        bucket = self.buckets[0].name
        query = replica_index.generate_index_create_query(
            namespace="default:" + bucket,
            deploy_node_info=[
                node.ip + ":" + self.node_port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replica
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          bucket)
        indexer_stats_before_backup = self.rest.get_indexer_metadata()
        replica_indexes_before_backup = indexer_stats_before_backup['status']
        self.assertEqual(
            len(replica_indexes_before_backup), self.num_index_replica + 1)
        indexes_before_backup = [
            index for index in replica_indexes_before_backup
            if index['indexName'] == replica_index.index_name]
        namespaces = ["_default._default"]
        backup_result = backup_client.backup(namespaces=namespaces, use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        query = replica_index.generate_index_drop_query(
            namespace="default:" + bucket)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        out_nodes = [
           node for node in self.indexer_nodes if node.ip != self.master.ip]
        out_node = out_nodes[0]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], [out_node])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        restore_result = backup_client.restore(use_https=self.use_https)
        self.assertTrue(restore_result[0], str(restore_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        time.sleep(30)
        indexes_after_restore = [index for index in self.rest.get_indexer_metadata()['status']
                                 if index['indexName'] == replica_index.index_name]
        if len(index_nodes) > self.num_index_replica:
            self.assertEqual(len(indexes_after_restore), len(indexes_before_backup))
        else:
            self.assertEqual(len(indexes_after_restore), len(index_nodes))
        # self._verify_indexes(indexes_before_backup,
        #                      indexes_after_restore)
        #services = ["index" for node in out_nodes]
        #rebalance = self.cluster.async_rebalance(
        #    self.servers[:self.nodes_init], [out_node], [],
        #    services=services)
        #reached = RestHelper(self.rest).rebalance_reached()
        #self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        #rebalance.result()
        #self.sleep(10)
        #replica_indexes_after_restore = [
        #    index for index in self.rest.get_indexer_metadata()['status']
        #    if index['name'].startswith(replica_index.index_name)]
        #self._verify_indexes(replica_indexes_before_backup,
        #                     replica_indexes_after_restore)

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
                node.ip + ":" + self.node_port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            partition_by_fields=partition_fields
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        backup_client = IndexBackupClient(self.master,
                                          self.use_cbbackupmgr,
                                          bucket)
        indexes_before_backup = [
            index for index in self.rest.get_indexer_metadata()['status']
            if index['name'].startswith(partition_index.index_name)]
        namespaces = ["_default._default"]
        backup_result = backup_client.backup(namespaces=namespaces, use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        query = partition_index.generate_index_drop_query(
            namespace="default:" + bucket)
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to drop")
        out_node = [node for node in self.indexer_nodes if self.master.ip != node.ip][0]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], [out_node])
        reached = RestHelper(self.rest).rebalance_reached()
        if not reached:
            self.fail(msg="Rebalance failed")
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        restore_result = backup_client.restore(use_https=self.use_https)
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
                node.ip + ":" + self.node_port
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
        backup_result = backup_client.backup(namespaces=namespaces, use_https=self.use_https)
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
                node.ip + ":" + self.node_port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            partition_by_fields=partition_fields
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        for i in range(10):
            restore_result = backup_client.restore(use_https=self.use_https)
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
        if self.num_index_replica == 0 or\
                self.num_index_replica >= len(self.indexer_nodes):
            self.num_index_replica = len(self.indexer_nodes) - 1
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
                node.ip + ":" + self.node_port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replica
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
        backup_result = backup_client.backup(namespaces=namespaces, use_https=self.use_https)
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
                node.ip + ":" + self.node_port
                for node in self.indexer_nodes],
            defer_build=self.defer_build,
            num_replica=self.num_index_replica
        )
        self.run_cbq_query(query)
        self.sleep(5, "wait for index to create")
        for i in range(10):
            restore_result = backup_client.restore(use_https=self.use_https)
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
                         self.num_index_replica + 1, msg=msg)

    def test_backup_restore_with_duplicate_index_name_different_fields(self):
        """
        https://issues.couchbase.com/browse/MB-43169
        """
        indexer_stats_before_backup = self.rest.get_indexer_metadata()
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(self.indexer_nodes[0],
                                          self.use_cbbackupmgr,
                                          bucket)
        backup_result = backup_client.backup(use_https=self.use_https)
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
        restore_result = backup_client.restore(use_https=self.use_https)
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
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        self._drop_indexes(indexer_stats_before_backup['status'])
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], out_nodes, [],
            services=["index"])
        restore_result = backup_client.restore(use_https=self.use_https)
        self.assertTrue(restore_result[0], str(restore_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
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
        self.sleep(10)
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], out_nodes, [],
            services=["index"])
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        restore_result = backup_client.restore(use_https=self.use_https)
        self.assertTrue(restore_result[0], str(restore_result[1]))
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
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
                node.ip + ":" + self.node_port
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
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self._drop_indexes(indexer_status_before_backup['status'])
        restore_result = backup_client.restore(use_https=self.use_https)
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
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        self._drop_indexes(indexer_status_before_backup['status'])
        restore_result = backup_client.restore(use_https=self.use_https)
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
                backup_result = backup_client.backup(use_https=self.use_https)
                self.assertTrue(
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
            backup_result = backup_client.backup(backup_args="--resume", use_https=self.use_https)
            self.assertTrue(backup_result[0], str(backup_result[1]))
        self._drop_indexes(indexer_stats_before_backup['status'])
        try:
            self.block_incoming_network_from_node(
                self.master, non_master_nodes[0])
            self.sleep(10, "wait node to be blocked")
            for backup_client in backup_clients:
                restore_result = backup_client.restore(use_https=self.use_https)
                self.assertFalse(restore_result[0], str(restore_result[1]))
        except Exception:
            raise
        finally:
            self.resume_blocked_incoming_network_from_node(
                self.master, non_master_nodes[0])
            self.sleep(10, "wait node to be unblocked")
        for backup_client in backup_clients:
            restore_result = backup_client.restore(restore_args="--resume", use_https=self.use_https)
            self.assertTrue(restore_result[0], str(restore_result[1]))
            if self.use_cbbackupmgr:
                backup_client.remove_backup()
        indexer_stats_after_restore = self.rest.get_indexer_metadata()
        self._verify_indexes(indexer_stats_before_backup['status'],
                             indexer_stats_after_restore['status'])

    def test_backup_restore_with_both_include_exclude(self):
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        namespaces = []
        for namespace in self.collection_namespaces:
            namespace_tokens = namespace.split(".")
            if namespace_tokens[-3].split(":")[1] == bucket:
                namespaces.append(namespace)
        config_args = ""
        if self.use_cbbackupmgr:
            namespaces = ",".join(namespaces)
            config_args = "--include-data {0} --exclude-data {1}".format(
                namespaces, namespaces)
        else:
            namespaces = list(map(
                lambda namespace: namespace.split(":")[1].split(".", 1)[1],
                namespaces))
            namespaces = ",".join(namespaces)
            config_args = "?include={0}&exclude={1}".format(
                namespaces, namespaces)
        backup_result = backup_client.backup(config_args=config_args, use_https=self.use_https)
        self.assertFalse(backup_result[0], str(backup_result[1]))
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        restore_result = backup_client.restore(restore_args=config_args, use_https=self.use_https)
        self.assertFalse(restore_result[0], str(restore_result[1]))
        restore_result = backup_client.restore(use_https=self.use_https)
        self.assertTrue(restore_result[0], str(restore_result[1]))

    def test_backup_restore_with_overlapping_paths(self):
        '''
        https://issues.couchbase.com/browse/MB-43579
        '''
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        collection_namespaces = set(
            map(lambda namespace: namespace.split(".", 1)[1], filter(
                lambda namespace:
                namespace.split(":")[1].split(".")[0] == bucket,
                self.collection_namespaces)))
        scope_namespaces = set(map(
            lambda namespace: namespace.split(".")[0], collection_namespaces))
        namespaces = []
        namespaces.extend(list(scope_namespaces))
        namespaces.extend(list(collection_namespaces))
        self.log.info(namespaces)
        backup_result = backup_client.backup(namespaces, use_https=self.use_https)
        self.assertFalse(backup_result[0], str(backup_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        backup_result = backup_client.backup(namespaces, include=False, use_https=self.use_https)
        self.assertFalse(backup_result[0], str(backup_result[1]))
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        mappings = list(
            map(lambda namespace: f"{namespace}:{namespace}", namespaces))
        restore_result = backup_client.restore(mappings, use_https=self.use_https)
        self.assertFalse(restore_result[0], str(restore_result[1]))

    def test_backup_with_invalid_keyspaces(self):
        invalid_bucket = "invalid_bucket"
        if self.use_cbbackupmgr:
            invalid_bucket = [invalid_bucket]
            invalid_bucket.extend(
                list(map(lambda bucket: bucket.name, self.buckets)))
        invalid_scope = "invalid_scope"
        invalid_collection = "invalid_collection"
        bucket = self.buckets[0].name
        scope = self.rest.get_bucket_scopes(bucket)[0]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        invalid_backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, invalid_bucket)
        backup_result = invalid_backup_client.backup(use_https=self.use_https)
        if self.use_cbbackupmgr:
            if not [msg for msg in backup_result[1] if f"Could not backup bucket '{invalid_bucket[0]}' because it does not exist on the cluster" in msg]:
                self.fail(str(backup_result[1]))
            invalid_backup_client.remove_backup()
        else:
            self.assertTrue(backup_result[0], str(backup_result[1]))
        backup_result = backup_client.backup([invalid_scope], use_https=self.use_https)
        if self.use_cbbackupmgr:
            namespace = "{0}.{1}".format(bucket, invalid_scope)
            if not [msg for msg in backup_result[1] if f"Could not backup scope '{namespace}' because it does not exist on the cluster" in msg]:
                self.fail(str(backup_result[1]))
            backup_client.remove_backup()
        else:
            self.assertTrue(backup_result[0], str(backup_result[1]))
        backup_result = backup_client.backup([invalid_scope], include=False, use_https=self.use_https)
        if self.use_cbbackupmgr:
            namespace = "{0}.{1}".format(bucket, invalid_scope)
            if not [msg for msg in backup_result[1] if f"Excluded scope '{namespace}' does not exist on the cluster" in msg]:
                self.fail(str(backup_result[1]))
            backup_client.remove_backup()
        else:
            self.assertTrue(backup_result[0], str(backup_result[1]))
        backup_result = backup_client.backup(
            [f"{scope}.{invalid_collection}"], use_https=self.use_https)
        if self.use_cbbackupmgr:
            namespace = "{0}.{1}.{2}".format(bucket, scope, invalid_collection)
            if not [msg for msg in backup_result[1] if f"Could not backup collection '{namespace}' because it does not exist on the cluster" in msg]:
                self.fail(str(backup_result[1]))
            backup_client.remove_backup()
        else:
            self.assertTrue(backup_result[0], str(backup_result[1]))
        backup_result = backup_client.backup(
            [f"{scope}.{invalid_collection}"], include=False, use_https=self.use_https)
        if self.use_cbbackupmgr:
            namespace = "{0}.{1}.{2}".format(bucket, scope, invalid_collection)
            if not [msg for msg in backup_result[1] if f"Excluded collection '{namespace}' does not exist on the cluster" in msg]:
                self.fail(str(backup_result[1]))
            backup_client.remove_backup()
        else:
            self.assertTrue(backup_result[0], str(backup_result[1]))

    def test_restore_with_invalid_keyspaces(self):
        invalid_bucket = "invalid_bucket"
        invalid_scope = "invalid_scope"
        invalid_collection = "invalid_collection"
        bucket = self.buckets[0].name
        scope = self.rest.get_bucket_scopes(bucket)[0]
        collection = self.rest.get_scope_collections(bucket, scope)[0]
        bucket = self.buckets[0].name
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        backup_client.set_restore_bucket(invalid_bucket)
        restore_result = backup_client.restore(use_https=self.use_https)
        self.assertFalse(restore_result[0], restore_result[1])
        backup_client.set_restore_bucket(bucket)
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        namespace = [scope]
        backup_result = backup_client.backup(namespace, use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        mappings = ["{0}:{1}".format(scope, invalid_scope)]
        restore_result = backup_client.restore(mappings, use_https=self.use_https)
        self.assertTrue(restore_result[0], restore_result[1])
        if self.use_cbbackupmgr:
            backup_client.remove_backup()
        namespace = ["{0}.{1}".format(scope, collection)]
        backup_result = backup_client.backup(namespace, use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        map_arg = "{0}.{1}".format(scope, invalid_collection)
        mappings = ["{0}:{1}".format(namespace[0], map_arg)]
        restore_result = backup_client.restore(mappings, use_https=self.use_https)
        self.assertTrue(restore_result[0], restore_result[1])


    def test_backup_restore_with_scheduled_indexes(self):
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        self.rest.set_index_settings(self.schedule_index_disable)
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        create_queries = []
        with ThreadPoolExecutor() as executor:
            for i in range(10):
                idx_def = QueryDefinition(
                    "idx_" + str(i), ["firstName"])
                query = idx_def.generate_index_create_query(
                    namespace, defer_build=self.defer_build)
                create_queries.append(query)

            for query in create_queries:
                task = executor.submit(
                    self.run_cbq_query, query=query, rest_timeout=30)
                tasks.append(task)
            for task in tasks:
                try:
                    task.result()
                except Exception as e:
                    error_msg = e.__str__()
                    self.log.info("Raised Exception:" + error_msg)
                    if "The index is scheduled for background creation" not in error_msg:
                        self.log.error(e)

        self.sleep(1, "Wait till indexes available in stats")
        self.wait_until_indexes_online(defer_build=self.defer_build, schedule_index=True)
        backup_result = backup_client.backup(use_https=self.use_https)
        self.assertTrue(backup_result[0], str(backup_result[1]))
        indexes_before_backup = self.rest.get_indexer_metadata()['status']
        index_names_before_backup = set(map(lambda idx: idx['name'], indexes_before_backup))
        self.wait_until_indexes_online(defer_build=self.defer_build, schedule_index=True)
        self._drop_indexes(indexes_before_backup)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        self.rest.set_index_settings(self.schedule_index_enable)
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        index_names_after_restore = set(map(lambda idx: idx['name'], indexes_after_restore))
        msg = "\nIndex_names_before_backup: " + str(index_names_before_backup)
        msg += "\nIndex_names_after_restore: " + str(index_names_after_restore)
        self.assertEqual(index_names_before_backup, index_names_after_restore, msg)
        self._verify_indexes(indexes_before_backup, indexes_after_restore)

    def test_backup_restore_with_scheduled_replica_indexes(self):
        num_indexer_nodes = len(self.indexer_nodes)
        if num_indexer_nodes < 2:
            self.fail("Number of indexer nodes")
        num_replica = num_indexer_nodes - 1
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace, defer_build=self.defer_build,
                            num_replica=num_replica)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(1, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()['status']
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                if idx not in index_names:
                    self.fail("Indexes created and indexes available in indexer-stats are not same")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
                backup_result = backup_client.backup(use_https=self.use_https)
                self.assertTrue(backup_result[0], str(backup_result[1]))
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.wait_until_indexes_online(defer_build=self.defer_build)
        self._drop_indexes(indexes_before_backup)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        indexes_before_backup = list(
            filter(lambda idx: idx['replicaId'] == 0, indexes_before_backup))
        indexes_after_restore = list(
            filter(lambda idx: idx['replicaId'] == 0, indexes_after_restore))
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        self._verify_indexes(indexes_before_backup, indexes_after_restore)

    def test_backup_restore_with_scheduled_partition_indexes(self):
        num_indexer_nodes = len(self.indexer_nodes)
        if num_indexer_nodes < 2:
            self.fail("Number of indexer nodes")
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        partition_fields = ['meta().id']
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace,
                            deploy_node_info=[
                                node.ip + ":" + self.node_port
                                for node in self.indexer_nodes],
                            defer_build=self.defer_build,
                            partition_by_fields=partition_fields)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(1, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()['status']
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                if idx not in index_names:
                    self.fail("Indexes created and indexes available in indexer-stats are not same")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
                backup_result = backup_client.backup(use_https=self.use_https)
                self.assertTrue(backup_result[0], str(backup_result[1]))
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.wait_until_indexes_online(defer_build=self.defer_build)
        self._drop_indexes(indexes_before_backup)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        self._verify_indexes(indexes_before_backup, indexes_after_restore)

    def test_backup_restore_with_duplicate_scheduled_indexes(self):
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
        self.rest.set_index_settings(schedule_index_disable)
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace, defer_build=self.defer_build)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(60, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()
            self.log.info(f"getIndexStatus result: {indexes}")
            indexes = indexes['status']
            self.log.info(f"Index status: {indexes}")
            system_query = "select * from system:indexes"
            sys_query_result = self.run_cbq_query(query=system_query)['results']
            self.log.info(f"Query result: {sys_query_result}")
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                self.assertTrue(idx in index_names, f"Index {idx} not available in {index_names} ")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
                backup_result = backup_client.backup(use_https=self.use_https)
                self.assertTrue(backup_result[0], str(backup_result[1]))
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.rest.set_index_settings(schedule_index_enable)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        self._drop_indexes(indexes_before_backup)
        for i in range(10):
            idx_def = QueryDefinition(
                "idx_" + str(i), ["firstName"])
            query = idx_def.generate_index_create_query(
                namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        for idx_1, idx_2 in zip(indexes_before_backup, indexes_after_restore):
            self.assertEqual(idx_1['name'], idx_2['name'])
            self.assertEqual(idx_2['status'], "Ready")
            self.assertEqual(idx_1.get('secExprs', None),
                             idx_2.get('secExprs', None))
            self.assertEqual(idx_1['indexType'], idx_2['indexType'])

    def test_backup_restore_with_equivalent_scheduled_indexes(self):
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
        self.rest.set_index_settings(schedule_index_disable)
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace, defer_build=self.defer_build)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(60, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()
            self.log.info(f"getIndexStatus result: {indexes}")
            indexes = indexes['status']
            self.log.info(f"Index status: {indexes}")
            system_query = "select * from system:indexes"
            sys_query_result = self.run_cbq_query(query=system_query)['results']
            self.log.info(f"Query result: {sys_query_result}")
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                self.assertTrue(idx in index_names, f"Index {idx} not available in {index_names}")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
                backup_result = backup_client.backup(use_https=self.use_https)
                self.assertTrue(backup_result[0], str(backup_result[1]))
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.rest.set_index_settings(schedule_index_enable)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        self._drop_indexes(indexes_before_backup)
        for i in range(10):
            idx_def = QueryDefinition(
                "idx_" + str(i), ["lastName"])
            query = idx_def.generate_index_create_query(
                namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        num_indexes_before_backup = len(indexes_before_backup)
        num_indexes_after_restore = len(indexes_after_restore)
        self.assertEqual(
            2 * num_indexes_before_backup, num_indexes_after_restore)

    def test_backup_restore_with_scheduled_indexes_and_include(self):
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
        self.rest.set_index_settings(schedule_index_disable)
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace, defer_build=self.defer_build)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(60, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()
            self.log.info(f"getIndexStatus result: {indexes}")
            indexes = indexes['status']
            self.log.info(f"Index status: {indexes}")
            system_query = "select * from system:indexes"
            sys_query_result = self.run_cbq_query(query=system_query)['results']
            self.log.info(f"Query result: {sys_query_result}")
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                self.assertTrue(idx in index_names, f"Index {idx} not available in {index_names}")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
                scopes = set(map(lambda idx: idx['scope'], scheduled_indexes))
                backup_result = backup_client.backup(namespaces=scopes, use_https=self.use_https)
                self.assertTrue(backup_result[0], str(backup_result[1]))
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.rest.set_index_settings(schedule_index_enable)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        self._drop_indexes(indexes_before_backup)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        self._verify_indexes(indexes_before_backup, indexes_after_restore)

    def test_backup_restore_with_scheduled_indexes_and_exclude(self):
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
        self.rest.set_index_settings(schedule_index_disable)
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace, defer_build=self.defer_build)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(60, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()
            self.log.info(f"getIndexStatus result: {indexes}")
            indexes = indexes['status']
            self.log.info(f"Index status: {indexes}")
            system_query = "select * from system:indexes"
            sys_query_result = self.run_cbq_query(query=system_query)['results']
            self.log.info(f"Query result: {sys_query_result}")
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                self.assertTrue(idx in index_names, f"Index {idx} not available in {index_names} ")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
                scopes = set(map(lambda idx: idx['scope'], scheduled_indexes))
                backup_client.backup(namespaces=scopes, include=False, use_https=self.use_https)
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.rest.set_index_settings(schedule_index_enable)
        self.wait_until_indexes_online(defer_build=self.defer_build)
        self._drop_indexes(indexes_before_backup)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata().get(
            "status", None)
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        self.assertEqual(indexes_after_restore, None, msg=msg)

    def test_backup_restore_with_scheduled_indexes_and_remap(self):
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        schedule_index_disable = {"indexer.debug.enableBackgroundIndexCreation": False}
        schedule_index_enable = {"indexer.debug.enableBackgroundIndexCreation": True}
        self.rest.set_index_settings(schedule_index_disable)
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        scope = self.scope_prefix
        self.rest.create_scope(bucket, scope)
        collection = self.collection_prefix
        self.rest.create_collection(bucket, scope, collection)
        self.sleep(10, "Giving some time for scope and collection to be ready")
        namespace += f".{scope}.{collection}"
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace, defer_build=self.defer_build)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(60, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()
            self.log.info(f"getIndexStatus result: {indexes}")
            indexes = indexes['status']
            self.log.info(f"Index status: {indexes}")
            system_query = "select * from system:indexes"
            sys_query_result = self.run_cbq_query(query=system_query)['results']
            self.log.info(f"Query result: {sys_query_result}")
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                self.assertTrue(idx in index_names, f"Index {idx} not available in {index_names} ")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                scopes = set(map(lambda idx: idx['scope'], scheduled_indexes))
                backup_result = backup_client.backup(namespaces=scopes, use_https=self.use_https)
                self.assertTrue(backup_result[0], str(backup_result[1]))
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.rest.set_index_settings(schedule_index_enable)
        self.wait_until_indexes_online()
        self._drop_indexes(indexes_before_backup)
        self.rest.delete_scope(bucket, scope)
        remap_scope = "remap_scope"
        self.rest.create_scope(bucket, remap_scope)
        self.rest.create_collection(bucket, remap_scope, collection)
        mappings = ["{0}:{1}".format(scope, remap_scope)]
        backup_client.restore(mappings, use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        try:
            self._verify_indexes(indexes_before_backup, indexes_after_restore)
        except Exception as e:
            self.log.error(e.__str__())
            self.fail(msg)

    def test_backup_restore_with_scheduled_replica_indexes_less_nodes(self):
        num_indexer_nodes = len(self.indexer_nodes)
        if num_indexer_nodes < 2:
            self.fail("Number of indexer nodes")
        num_replica = num_indexer_nodes - 1
        self._drop_indexes(self.rest.get_indexer_metadata()['status'])
        namespace = self.collection_namespaces[0].split(".", 1)[0]
        bucket = namespace.split(":")[1]
        backup_client = IndexBackupClient(
            self.master, self.use_cbbackupmgr, bucket)
        tasks = []
        indexes_created = []
        indexes_before_backup = []
        indexes = set()
        timeout = time.time() + 60
        scheduled_indexes_present = False
        scheduled_indexes = []
        while not scheduled_indexes_present and time.time() < timeout:
            with ThreadPoolExecutor() as executor:
                for i in range(10):
                    try:
                        idx_def = QueryDefinition(
                            "idx_" + str(i), ["firstName"])
                        query = idx_def.generate_index_create_query(
                            namespace, defer_build=self.defer_build,
                            num_replica=num_replica)
                        task = executor.submit(
                            self.run_cbq_query, query=query, rest_timeout=30)
                        indexes_created.append(idx_def.index_name)
                        tasks.append(task)
                    except Exception as e:
                        error_msg = e.__str__()
                        self.log.info("Raised Exception:" + error_msg)
                        if "The index is scheduled for background creation"\
                                not in error_msg:
                            raise
            self.sleep(1, "Wait till indexes available in stats")
            indexes = self.rest.get_indexer_metadata()['status']
            index_names = set(map(lambda idx: idx['name'], indexes))
            for idx in indexes_created:
                if idx not in index_names:
                    self.fail("Indexes created and indexes available in indexer-stats are not same")
            scheduled_indexes = list(filter(
                lambda idx:
                idx['status'] == "Scheduled for Creation",
                indexes))
            if scheduled_indexes:
                scheduled_indexes_present = True
                indexes_before_backup =\
                    self.rest.get_indexer_metadata()['status']
                backup_client.backup(use_https=self.use_https)
            else:
                self.wait_until_indexes_online(defer_build=self.defer_build)
                self._drop_indexes(indexes_before_backup)
        if not scheduled_indexes_present:
            self.fail("Couldn't create scheduled indexes")
        for task in tasks:
            try:
                result = task.result()
                self.log.info(result)
            except Exception as e:
                error_msg = e.__str__()
                self.log.info("Raised Exception:" + error_msg)
                if "Index creation will be retried in background"\
                        not in error_msg\
                        and "The index is scheduled for background creation"\
                        not in error_msg\
                        and "The index was scheduled for background creation"\
                        not in error_msg\
                        and "will retry building in the background for reason: Build Already In Progress" not in error_msg:
                    raise
        self.wait_until_indexes_online(defer_build=self.defer_build)
        indexes_before_backup = list(
            filter(lambda idx: idx['replicaId'] == 0, indexes_before_backup))
        self._drop_indexes(indexes_before_backup)
        out_nodes = [
            node for node in self.indexer_nodes if self.master.ip != node.ip]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], [], out_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(10)
        backup_client.restore(use_https=self.use_https)
        self.sleep(10, "wait to complete restore")
        indexes_after_restore = self.rest.get_indexer_metadata()['status']
        msg = "\nIndex_names_before_backup: " + str(indexes_before_backup)
        msg += "\nIndex_names_after_restore: " + str(indexes_after_restore)
        self._verify_indexes(indexes_before_backup, indexes_after_restore)

    def test_backup_restore_with_shard_affinity(self):
        try:
            backup_client = None
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
            self.buckets = self.rest.get_buckets()
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template,
                                                 load_default_coll=True)
            time.sleep(10)
            select_queries = set()
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            for _ in range(self.initial_index_batches):
                replica_count = random.randint(1, 2)
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                for namespace in self.namespaces:

                    select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                               namespace=namespace))
                    queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                      namespace=namespace,
                                                                      num_replica=replica_count,
                                                                      randomise_replica_count=True)
                    self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                         query_node=query_node)
            self.wait_until_indexes_online()
            self.item_count_related_validations()
            self.validate_shard_affinity()
            query_results_before_backup = self.run_scans_and_return_results(select_queries)
            for bucket in self.buckets:
                backup_client = IndexBackupClient(self.master,
                                                  self.use_cbbackupmgr,
                                                  bucket.name)
                bucket_collection_namespaces = [
                    namespace.split(".", 1)[1] for namespace
                    in self.namespaces
                    if namespace.split(':')[1].split(".")[0] == bucket.name]
                indexer_stats_before_backup = self.index_rest.get_indexer_metadata()
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                backup_result = backup_client.backup([], use_https=self.use_https)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        bucket_collection_namespaces, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                new_indexes = False
                if self.decrease_node_count > 0:
                    nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                    nodes_out_list = []
                    for i in range(len(nodes_out)):
                        node = nodes_out[i]
                        if node.ip != self.master.ip:
                            nodes_out_list.append(node)
                            if len(nodes_out_list) == self.decrease_node_count:
                                break
                    rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out_list,
                                                             services=['index'], cluster_config=self.cluster_config)
                    self.log.info(f"Rebalance task triggered. Wait in loop until the rebalance starts")
                    time.sleep(3)
                    status, _ = self.rest._rebalance_status_and_progress()
                    while status != 'running':
                        time.sleep(1)
                        status, _ = self.rest._rebalance_status_and_progress()
                    reached = RestHelper(self.rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                    rebalance.result()
                if self.create_index_on_n_nodes is not None:
                    self.enable_honour_nodes_in_definition()
                    new_indexes = True
                    num_nodes = 0
                    indexer_nodes = self.get_nodes_from_services_map(
                        service_type="index", get_all_nodes=True)
                    if self.create_index_on_n_nodes == 'all':
                        num_nodes = len(indexer_nodes)
                    elif self.create_index_on_n_nodes == 'partial':
                        f = math.floor(len(indexer_nodes) / 2)
                        num_nodes = random.randint(1, f)
                    for i in range(num_nodes):
                        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                        deploy_node_info = indexer_nodes[i].ip + ":" + self.node_port
                        for namespace in self.namespaces:
                            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                              namespace=namespace,
                                                                              deploy_node_info=deploy_node_info)
                            query_node = self.get_nodes_from_services_map(service_type="n1ql")
                            self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                                 query_node=query_node)
                restore_result = backup_client.restore(use_https=self.use_https)
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        bucket_collection_namespaces, restore_result[1]))
                self.log.info("Will copy restore log from backup client node to logs folder")
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'restore.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("restore log copy complete")
                except:
                    pass
                finally:
                    shell.disconnect()
                index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
                self.index_rest = RestConnection(index_node)
                indexer_stats_after_restore = self.index_rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                self._verify_indexes(indexes_before_backup, indexes_after_restore, new_indexes)
                self._build_indexes(indexes_after_restore)
            self.wait_until_indexes_online()
            query_results_after_restore = self.run_scans_and_return_results(select_queries)
            for query in query_results_before_backup:
                diffs = DeepDiff(query_results_before_backup[query], query_results_after_restore[query], ignore_order=True)
                if diffs:
                    self.log.error(f"Mismatch in query result before backup and after restore. Select query {query}\n\n. "
                                   f"Result before \n\n {query_results_before_backup[query]}."
                                   f"Result after \n \n {query_results_after_restore[query]}")
                    raise Exception("Mismatch in query results before backup and after restore")
            self.validate_shard_affinity()
            nodes_out_list, nodes_in_list = [], []
            nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            rebalance_task = False
            if self.rebalance_out:
                nodes_out_list = nodes_out[:1]
                rebalance_task = True
            if self.rebalance_in:
                nodes_in_list = self.servers[self.nodes_init:]
                self.enable_redistribute_indexes()
                rebalance_task = True
            if self.rebalance_swap:
                nodes_out_list = nodes_out_list[:1]
                nodes_in_list = self.servers[self.nodes_init:]
                rebalance_task = True
            if rebalance_task:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in_list, nodes_out_list,
                                                         services=['index'], cluster_config=self.cluster_config)
                self.log.info(f"Rebalance task triggered. Wait in loop until the rebalance starts")
                time.sleep(3)
                status, _ = self.rest._rebalance_status_and_progress()
                while status != 'running':
                    time.sleep(1)
                    status, _ = self.rest._rebalance_status_and_progress()
                reached = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                rebalance.result()
                self.validate_shard_affinity()
                query_results_after_rebalance = self.run_scans_and_return_results(select_queries)
                for query in query_results_before_backup:
                    diffs = DeepDiff(query_results_after_rebalance[query], query_results_after_restore[query], ignore_order=True)
                    if diffs:
                        self.log.error(f"Mismatch in query result after restore and after rebalance. Select query {query}\n\n. "
                                       f"Result after \n\n {query_results_after_rebalance[query]}."
                                       f"Result before \n \n {query_results_after_restore[query]}")
                        raise Exception("Mismatch in query result after restore and after rebalance")

        finally:
            if backup_client:
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'backup.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                self.log.info("Will copy backup log from backup client node to logs folder")
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("backup log copy complete")
                except:
                    pass
                finally:
                    shell.disconnect()
                if self.use_cbbackupmgr:
                    backup_client.remove_backup()

    def test_backup_restore_vector_indexes(self):
        from sentence_transformers import SentenceTransformer
        self.encoder = SentenceTransformer(self.data_model, device="cpu")
        self.encoder.cpu()
        self.gsi_util_obj.set_encoder(encoder=self.encoder)
        try:
            backup_client = None
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
            time.sleep(10)
            select_queries = set()
            query_node = self.get_nodes_from_services_map(service_type="n1ql")

            for namespace in self.namespaces:
                query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                          prefix='test', bhive_index=self.bhive_index,
                                                                          similarity=self.similarity, train_list=None,
                                                                          scan_nprobes=self.scan_nprobes,
                                                                          array_indexes=False,
                                                                          limit=self.scan_limit,
                                                                          quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                          quantization_algo_description_vector=self.quantization_algo_description_vector)
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace, limit=self.scan_limit))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace, bhive_index=self.bhive_index,
                                                                  num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                     query_node=query_node)
            self.wait_until_indexes_online()
            self.item_count_related_validations()

            for bucket in self.buckets:
                backup_client = IndexBackupClient(self.master,
                                                  self.use_cbbackupmgr,
                                                  bucket.name)
                bucket_collection_namespaces = [
                    namespace.split(".", 1)[1] for namespace
                    in self.namespaces
                    if namespace.split(':')[-1].split(".")[0] == bucket.name]
                map_before_rebalance, stats_before_rebalance = self._return_maps(perNode=True,
                                                                                 map_from_index_nodes=True)
                indexer_stats_before_backup = self.index_rest.get_indexer_metadata()
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                self.log.info(f"indexes before backup {indexes_before_backup}")
                backup_result = backup_client.backup(bucket_collection_namespaces, use_https=self.use_https)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        bucket_collection_namespaces, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                self.sleep(120)
                timeout = 0
                while timeout <= 360:
                    indexer_metadata = self.index_rest.get_indexer_metadata()
                    if 'status' not in indexer_metadata:
                        break
                    else:
                        timeout = timeout + 1
                if timeout > 360:
                    self.fail("timeout reached for index drop to happen")
                if self.decrease_node_count > 0:
                    nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                    nodes_out_list = []
                    for i in range(len(nodes_out)):
                        node = nodes_out[i]
                        if node.ip != self.master.ip:
                            nodes_out_list.append(node)
                            if len(nodes_out_list) == self.decrease_node_count:
                                break
                    rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out_list,
                                                             services=['index'], cluster_config=self.cluster_config)
                    self.log.info(f"Rebalance task triggered. Wait in loop until the rebalance starts")
                    time.sleep(3)
                    status, _ = self.rest._rebalance_status_and_progress()
                    while status != 'running':
                        time.sleep(1)
                        status, _ = self.rest._rebalance_status_and_progress()
                    reached = RestHelper(self.rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                    rebalance.result()
                restore_result = backup_client.restore(use_https=self.use_https, restore_args="--auto-create-buckets")
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        bucket_collection_namespaces, restore_result[1]))
                self.log.info("Will copy restore log from backup client node to logs folder")
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'restore.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("restore log copy complete")
                except Exception as e:
                    err_msg = e.__str__()
                    self.log.error(err_msg)
                finally:
                    shell.disconnect()
                index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
                self.index_rest = RestConnection(index_node)
                indexer_stats_after_restore = self.index_rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                self._build_indexes(indexes_after_restore)
                self.sleep(10)
                self.wait_until_indexes_online()
                self.display_recall_and_accuracy_stats(select_queries=select_queries, message="recall and accuracy stats after adding back a node and post replica repair", similarity=self.similarity)


            self.wait_until_indexes_online()
            rebalance_task = False
            if self.rebalance_in:
                #basically to add back the previously removed node
                nodes_in_list = nodes_out_list
                self.enable_redistribute_indexes()
                rebalance_task = True
            if rebalance_task:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in_list, [],
                                                         services=['index'], cluster_config=self.cluster_config)
                self.log.info(f"Rebalance task triggered. Wait in loop until the rebalance starts")
                time.sleep(3)
                status, _ = self.rest._rebalance_status_and_progress()
                while status != 'running':
                    time.sleep(1)
                    status, _ = self.rest._rebalance_status_and_progress()
                reached = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                rebalance.result()
                self.wait_until_indexes_online()
                indexer_stats_after_replica_repair = self.index_rest.get_indexer_metadata()
                indexes_after_replica_repair = [
                    index
                    for index in indexer_stats_after_replica_repair['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                self._verify_indexes(indexes_before_backup, indexes_after_replica_repair, new_indexes=True)
                map_after_rebalance, stats_after_rebalance = self._return_maps(perNode=True, map_from_index_nodes=True)

                self.n1ql_helper.validate_item_count_data_size(map_before_rebalance=map_before_rebalance,
                                                               map_after_rebalance=map_after_rebalance,
                                                               stats_map_before_rebalance=stats_before_rebalance,
                                                               stats_map_after_rebalance=stats_after_rebalance,
                                                               item_count_increase=False,
                                                               per_node=True, skip_array_index_item_count=False)
                self.display_recall_and_accuracy_stats(select_queries=select_queries, message="recall and accuracy stats after adding back a node and post replica repair", similarity=self.similarity)
                self.drop_index_node_resources_utilization_validations()

        finally:
            if backup_client:
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'backup.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                self.log.info("Will copy backup log from backup client node to logs folder")
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("backup log copy complete")
                except Exception as e:
                    err_msg = e.__str__()
                    self.log.error(err_msg)
                finally:
                    shell.disconnect()
                if self.use_cbbackupmgr:
                    backup_client.remove_backup()

    def test_kill_retry_backup_restore(self):
        from sentence_transformers import SentenceTransformer
        self.encoder = SentenceTransformer(self.data_model, device="cpu")
        self.encoder.cpu()
        self.gsi_util_obj.set_encoder(encoder=self.encoder)
        try:
            backup_client = None
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
            time.sleep(10)
            select_queries = set()
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

            for namespace in self.namespaces:
                query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                                prefix='test',
                                                                                similarity=self.similarity,
                                                                                train_list=None,
                                                                                scan_nprobes=self.scan_nprobes,
                                                                                array_indexes=False,
                                                                                limit=self.scan_limit,
                                                                                bhive_index=self.bhive_index,
                                                                                quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                                quantization_algo_description_vector=self.quantization_algo_description_vector)
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace, limit=self.scan_limit))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace, bhive_index=self.bhive_index,
                                                                  num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                     query_node=query_node)
            self.wait_until_indexes_online()

            for bucket in self.buckets:
                backup_client = IndexBackupClient(self.master,
                                                  self.use_cbbackupmgr,
                                                  bucket.name, disabled_services="--disable-ft-indexes --disable-views --disable-ft-alias --disable-eventing --disable-analytics")
                bucket_collection_namespaces = [
                    namespace.split(".", 1)[1] for namespace
                    in self.namespaces
                    if namespace.split(':')[-1].split(".")[0] == bucket.name]
                indexer_stats_before_backup = self.index_rest.get_indexer_metadata()
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                print(f"indexes before backup {indexes_before_backup}")
                with ThreadPoolExecutor() as executor:
                    backup_res = executor.submit(backup_client.backup, namespaces=bucket_collection_namespaces, use_https=self.use_https)
                    self.sleep(1)
                    for node in indexer_nodes:
                        remote = RemoteMachineShellConnection(node)
                        remote.stop_indexer()

                self.log.info(backup_res.result())
                self.assertFalse(backup_res.result()[0], " backing up cluster succeded", )

                #start indexer process
                for node in indexer_nodes:
                    remote = RemoteMachineShellConnection(node)
                    remote.start_indexer()
                self.sleep(10)

                #retrying the backup post failing it
                backup_result = backup_client.backup(bucket_collection_namespaces,backup_args="--resume", use_https=self.use_https)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        bucket_collection_namespaces, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                self.sleep(120)
                self.rest.delete_all_buckets()

                with ThreadPoolExecutor() as executor:
                    restore_res = executor.submit(backup_client.restore, use_https=self.use_https, restore_args="--auto-create-buckets")
                    for node in indexer_nodes:
                        remote = RemoteMachineShellConnection(node)
                        remote.stop_indexer()

                self.log.info(restore_res.result())
                self.assertFalse(restore_res.result()[0], " restoring of cluster succeded", )

                # start indexer process
                for node in indexer_nodes:
                    remote = RemoteMachineShellConnection(node)
                    remote.start_indexer()
                self.sleep(10)

                # retrying the restore post failing it
                restore_result = backup_client.restore(use_https=self.use_https,
                                                       restore_args="--auto-create-buckets --resume")
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        bucket_collection_namespaces, restore_result[1]))
                self.log.info("Will copy restore log from backup client node to logs folder")
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'restore.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("restore log copy complete")
                except Exception as e:
                    err_msg = e.__str__()
                    self.log.error(err_msg)
                finally:
                    shell.disconnect()
                index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
                self.index_rest = RestConnection(index_node)
                indexer_stats_after_restore = self.index_rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                self._verify_indexes(indexes_before_backup, indexes_after_restore)
                self._build_indexes(indexes_after_restore)
                self.wait_until_indexes_online()
                self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                                       message="recall and accuracy stats after adding back a node and post replica repair", similarity=self.similarity)

            self.wait_until_indexes_online()

        finally:
            if backup_client:
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'backup.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                self.log.info("Will copy backup log from backup client node to logs folder")
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("backup log copy complete")
                except Exception as e:
                    err_msg = e.__str__()
                    self.log.error(err_msg)
                finally:
                    shell.disconnect()
                if self.use_cbbackupmgr:
                    backup_client.remove_backup()

    def test_shard_seggregation_backup_restore(self):
        try:
            backup_client = None
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
            time.sleep(10)
            collection_namespace = self.namespaces[0]

            # creating scalar index
            scalar_idx = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace, num_replica=self.num_index_replica)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating vector index
            vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64)
            query = vector_idx.generate_index_create_query(namespace=collection_namespace, num_replica=self.num_index_replica)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating bhive index
            bhive_idx = QueryDefinition(index_name='bhive_description',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED")
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True, num_replica=self.num_index_replica)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.item_count_related_validations()


            for bucket in self.buckets:
                backup_client = IndexBackupClient(self.master,
                                                  self.use_cbbackupmgr,
                                                  bucket.name)
                bucket_collection_namespaces = [
                    namespace.split(".", 1)[1] for namespace
                    in self.namespaces
                    if namespace.split(':')[-1].split(".")[0] == bucket.name]
                indexer_stats_before_backup = self.index_rest.get_indexer_metadata()
                indexes_before_backup = [
                    index
                    for index in indexer_stats_before_backup['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                self.log.info(f"indexes before backup {indexes_before_backup}")
                backup_result = backup_client.backup(bucket_collection_namespaces, use_https=self.use_https)
                self.assertTrue(
                    backup_result[0],
                    "backup failed for {0} with {1}".format(
                        bucket_collection_namespaces, backup_result[1]))
                self._drop_indexes(indexes_before_backup)
                self.sleep(120)
                timeout = 0
                while timeout <= 360:
                    indexer_metadata = self.index_rest.get_indexer_metadata()
                    if 'status' not in indexer_metadata:
                        break
                    else:
                        timeout = timeout + 1
                if timeout > 360:
                    self.fail("timeout reached for index drop to happen")
                restore_result = backup_client.restore(use_https=self.use_https, restore_args="--auto-create-buckets")
                self.assertTrue(
                    restore_result[0],
                    "restore failed for {0} with {1}".format(
                        bucket_collection_namespaces, restore_result[1]))
                self.log.info("Will copy restore log from backup client node to logs folder")
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'restore.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("restore log copy complete")
                except Exception as e:
                    err_msg = e.__str__()
                    self.log.error(err_msg)
                finally:
                    shell.disconnect()
                index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
                self.index_rest = RestConnection(index_node)
                indexer_stats_after_restore = self.index_rest.get_indexer_metadata()
                indexes_after_restore = [
                    index
                    for index in indexer_stats_after_restore['status']
                    if bucket.name == index['bucket']
                       and "{0}.{1}".format(index['scope'], index['collection'])
                       in bucket_collection_namespaces]
                self._build_indexes(indexes_after_restore)
                self.wait_until_indexes_online()
                #doing a swap rebalance operation post restore
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                self.enable_shard_seggregation()
                self.sleep(20)
                counter = 0
                for nodes in index_nodes:
                    nodes_to_remove = [nodes]

                    nodes_to_add = [self.servers[self.nodes_init+counter]]
                    task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=nodes_to_add,
                                                        to_remove=nodes_to_remove, services=['index'])
                    task.result()
                    rebalance_status = RestHelper(self.rest).rebalance_reached()
                    self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
                    self.sleep(10)
                    counter += 1


                shard_index_map = self.get_shards_index_map()
                self.validate_shard_seggregation(shard_index_map=shard_index_map)
                self.drop_index_node_resources_utilization_validations()


        finally:
            if backup_client:
                logs_path = self.input.param("logs_folder", "/tmp")
                logs_path_final = os.path.join(logs_path, 'backup.log')
                backup_log_path_src = os.path.join(backup_client.backup_path, 'logs', 'backup-0.log')
                shell = RemoteMachineShellConnection(backup_client.backup_node)
                self.log.info("Will copy backup log from backup client node to logs folder")
                try:
                    shell.copy_file_remote_to_local(backup_log_path_src, logs_path_final)
                    self.log.info("backup log copy complete")
                except Exception as e:
                    err_msg = e.__str__()
                    self.log.error(err_msg)
                finally:
                    shell.disconnect()
                if self.use_cbbackupmgr:
                    backup_client.remove_backup()
