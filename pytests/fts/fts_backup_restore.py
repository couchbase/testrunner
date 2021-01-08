# coding=utf-8

import copy
import json
import random
import time
from threading import Thread

import Geohash
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputSingleton
from tasks.task import ESRunQueryCompare
from tasks.taskmanager import TaskManager
from lib.testconstants import FUZZY_FTS_SMALL_DATASET, FUZZY_FTS_LARGE_DATASET
from .fts_base import FTSBaseTest, INDEX_DEFAULTS, QUERY, download_from_s3
from lib.membase.api.exception import FTSException, ServerUnavailableException
from lib.membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import SDKDataLoader
import threading


class BackupRestore(FTSBaseTest):

    def setUp(self):
        super(BackupRestore, self).setUp()
        self.bucket_params = {}
        self.bucket_params['server'] = self.master
        self.bucket_params['replicas'] = 1
        self.bucket_params['size'] = 100
        self.bucket_params['port'] = 11211
        self.bucket_params['password'] = "password"
        self.bucket_params['bucket_type'] = "membase"
        self.bucket_params['enable_replica_index'] = 1
        self.bucket_params['eviction_policy'] = "valueOnly"
        self.bucket_params['bucket_priority'] = None
        self.bucket_params['flush_enabled'] = 1
        self.bucket_params['lww'] = False
        self.bucket_params['maxTTL'] = None
        self.bucket_params['compressionMode'] = "passive"
        self.bucket_params['bucket_storage'] = "couchstore"
        self.rest = RestConnection(self._cb_cluster.get_random_fts_node())

    def tearDown(self):
        super(BackupRestore, self).tearDown()

    def test_backup_restore(self):
        index_definitions = {}

        self._create_kv()

        bucket_name = TestInputSingleton.input.param("bucket", None)

        self._create_indexes(index_definitions)

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        backup_filter = eval(TestInputSingleton.input.param("filter", "None"))
        if bucket_name:
            backup_client = FTSIndexBackupClient(fts_nodes[0], self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            backup_client = FTSIndexBackupClient(fts_nodes[0])

        # perform backup
        if bucket_name:
            status, content = backup_client.backup_bucket_level(_filter=backup_filter, bucket=self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            status, content = backup_client.backup(_filter=backup_filter)

        backup_json = json.loads(content)
        backup = backup_json['indexDefs']['indexDefs']

        # store backup index definitions
        indexes_for_backup = eval(TestInputSingleton.input.param("expected_indexes", "[]"))
        for idx in indexes_for_backup:
            backup_index_def = backup[idx]
            index_definitions[idx]['backup_def'] = backup_index_def

        # delete all indexes before restoring from backup
        self._cb_cluster.delete_all_fts_indexes()

        # restoring indexes from backup
        backup_client.restore()

        # getting restored indexes definitions and storing them in indexes definitions dict
        for ix_name in indexes_for_backup:
            _,restored_index_def = self.rest.get_fts_index_definition(ix_name)
            index_definitions[ix_name]['restored_def'] = restored_index_def

        #compare all 3 types of index definitions: initial, backed up, and restored from backup
        errors = self._check_indexes_definitions(index_definitions=index_definitions, indexes_for_backup=indexes_for_backup)

        self._cleanup_indexes(index_definitions)

        #errors analysis
        if len(errors.keys()) > 0:
            err_msg = ""
            for err in errors.keys():
                index_errors = errors[err]
                for msg in index_errors:
                    err_msg = err_msg + msg + "\n"
            self.fail(err_msg)

    def test_remap(self):
        index_definitions = {}
        bucket_name = TestInputSingleton.input.param("bucket", None)

        self._create_kv()

        self._create_indexes(index_definitions)

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        backup_filter = eval(TestInputSingleton.input.param("filter", "None"))
        if bucket_name:
            backup_client = FTSIndexBackupClient(fts_nodes[0], self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            backup_client = FTSIndexBackupClient(fts_nodes[0])

        # perform backup
        if bucket_name:
            status, content = backup_client.backup_bucket_level(_filter=backup_filter, bucket=bucket_name)
        else:
            status, content = backup_client.backup(_filter=backup_filter)

        backup_json = json.loads(content)
        backup = backup_json['indexDefs']['indexDefs']

        # store backup index definitions
        indexes_for_backup = eval(TestInputSingleton.input.param("remapped_idx", "[]"))

        # delete all indexes before restoring from backup
        self._cb_cluster.delete_all_fts_indexes()

        # restoring indexes from backup
        mappings = eval(TestInputSingleton.input.param("remap", "[]"))
        status, response, _ = backup_client.restore(mappings=mappings)
        if not status:
            self.fail(str(response))
        time.sleep(10)

        # getting restored indexes definitions and storing them in indexes definitions dict
        for ix in indexes_for_backup:
            index_name = ix[0]
            _,restored_index_def = self.rest.get_fts_index_definition(index_name)
            index_definitions[index_name]['remapped_def'] = restored_index_def['indexDef']

        # prepare indexes information for tests
        expected_mappings = eval(TestInputSingleton.input.param("remapped_idx", "[]"))
        expected_indexes = []
        expected_index_parameters = []
        for mapping in expected_mappings:
            ix = self._decode_index(mapping)
            expected_indexes.append(ix)

        for ix in expected_indexes:
            ix_params_map = {}
            ix_params_map["name"] = ix["name"]
            ix_params_map["params"] = {}
            collection_index, _type, index_scope, index_collections = self.define_index_params(ix)
            ix_params_map["params"]["collection_index"] = collection_index
            ix_params_map["params"]["_type"] = _type
            ix_params_map["params"]["index_scope"] = index_scope
            ix_params_map["params"]["index_collections"] = index_collections
            expected_index_parameters.append(ix_params_map)

        # check indexes remapping
        errors = self._check_indexes_remap(index_definitions=index_definitions,
                                           expected_index_parameters=expected_index_parameters,
                                           remap_data=expected_mappings)

        self._cleanup_indexes(index_definitions)

        #errors analysis
        if len(errors.keys()) > 0:
            err_msg = ""
            for err in errors.keys():
                index_errors = errors[err]
                for msg in index_errors:
                    err_msg = err_msg + str(msg) + "\n"
            self.fail(err_msg)

    def test_backup_restore_clusterops(self):
        index_definitions = {}
        bucket_name = TestInputSingleton.input.param("bucket_name", None)
        action = TestInputSingleton.input.param("action", "drop_node")
        index_replica = TestInputSingleton.input.param("index_replica", 1)

        self._create_kv()
        self._create_indexes(index_definitions, index_replica=index_replica)

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        if bucket_name:
            backup_client = FTSIndexBackupClient(fts_nodes[0], self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            backup_client = FTSIndexBackupClient(fts_nodes[0])

        # perform backup
        if bucket_name:
            status, content = backup_client.backup_bucket_level(bucket=bucket_name)
        else:
            status, content = backup_client.backup()

        backup_json = json.loads(content)
        backup = backup_json['indexDefs']['indexDefs']
        backup_data = backup_json['indexDefs']

        # store backup index definitions
        indexes_for_backup = eval(TestInputSingleton.input.param("expected_indexes", "[]"))
        for idx in indexes_for_backup:
            backup_index_def = backup[idx]
            index_definitions[idx]['backup_def'] = backup_index_def

        # delete all indexes before restoring from backup
        while len(self._cb_cluster.get_indexes()) > 0:
            self._cb_cluster.delete_all_fts_indexes()

        if action == "drop_node":
            self._cb_cluster.failover(master=False, num_nodes=1, graceful=False)
            self._cb_cluster.rebalance_failover_nodes()
            self.rest = RestConnection(self._cb_cluster.get_random_fts_node())
        elif action == "add_node":
            self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        if bucket_name:
            backup_client = FTSIndexBackupClient(fts_nodes[0], self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            backup_client = FTSIndexBackupClient(fts_nodes[0])

        backup_client.backup_data = backup_data
        backup_client.is_backup_exists = True
        # restoring indexes from backup
        backup_client.restore()

        # getting restored indexes definitions and storing them in indexes definitions dict
        for ix_name in indexes_for_backup:
            _,restored_index_def = self.rest.get_fts_index_definition(ix_name)
            index_definitions[ix_name]['restored_def'] = restored_index_def

        #compare all 3 types of index definitions: initial, backed up, and restored from backup
        errors = self._check_indexes_definitions(index_definitions=index_definitions, indexes_for_backup=indexes_for_backup)

        self._cleanup_indexes(index_definitions)

        #errors analysis
        if len(errors.keys()) > 0:
            err_msg = ""
            for err in errors.keys():
                index_errors = errors[err]
                for msg in index_errors:
                    err_msg = err_msg + msg + "\n"
            self.fail(err_msg)

    def test_backup_restore_alias(self):
        index_definitions = {}
        bucket_name = TestInputSingleton.input.param("bucket_name", None)
        action = TestInputSingleton.input.param("action", "drop_node")
        index_replica = TestInputSingleton.input.param("index_replica", 1)

        self._create_kv()

        index = self.create_index(bucket=self._cb_cluster.get_bucket_by_name("b1"), index_name="bucket_index",
                                  collection_index=False, _type=None, scope=None, collections=None)
        index_alias = self.create_alias(target_indexes=[index], name="index_alias", alias_def=None)

        _, initial_alias_def = index_alias.get_index_defn()

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        if bucket_name:
            backup_client = FTSIndexBackupClient(fts_nodes[0], self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            backup_client = FTSIndexBackupClient(fts_nodes[0])

        # perform backup
        if bucket_name:
            status, content = backup_client.backup_bucket_level(bucket=bucket_name)
        else:
            status, content = backup_client.backup()


        # delete all indexes before restoring from backup
        index_alias.delete()
        index.delete()

        # restoring indexes from backup
        backup_client.restore()

        _, restored_alias_def = self.rest.get_fts_index_definition("index_alias")

        original_alias_defn = initial_alias_def['indexDef']
        restored_alias_defn = restored_alias_def['indexDef']
        del original_alias_defn["uuid"]
        del restored_alias_defn["uuid"]

        self.assertEqual(original_alias_defn, restored_alias_defn, "Restored index alias definition differs from original one.")

    def test_complex_index_backup_restore(self):
        index_definitions = {}
        bucket_name = TestInputSingleton.input.param("bucket_name", None)
        action = TestInputSingleton.input.param("action", "drop_node")
        index_replica = TestInputSingleton.input.param("index_replica", 1)

        self._create_kv()

        index = self.create_index(bucket=self._cb_cluster.get_bucket_by_name("b1"), index_name="bucket_index",
                                  collection_index=False, _type=None, scope=None, collections=None)

        index_params = {
            "name": "bucket_index",
            "type": "fulltext-index",
            "params": {
                "doc_config": {
                    "docid_prefix_delim": "",
                    "docid_regexp": "",
                    "mode": "type_field",
                    "type_field": "type"
                },
                "mapping": {
                    "analysis": {
                        "analyzers": {
                            "analyzer1": {
                                "char_filters": ["html"],
                                "token_filters": ["apostrophe"],
                                "tokenizer": "letter",
                                "type": "custom"
                            },
                            "custom analyzer 2": {
                                "tokenizer": "tokenizer1",
                                "type": "custom"
                            }
                        },
                        "char_filters": {
                            "char_filter1": {
                                "regexp": "foo.?",
                                "replace": "$1+$2",
                                "type": "regexp"
                            }
                        },
                        "date_time_parsers": {
                            "datetimeparser1": {
                                "layouts": ["DD/MM/YYYY"],
                                "type": "flexiblego"
                            }
                        },
                        "token_filters": {
                            "token filter": {
                                "form": "nfd",
                                "type": "normalize_unicode"
                            }
                        },
                        "token_maps": {
                            "wordlist1": {
                                "tokens": ["word1","word2"],
                                "type": "custom"
                            }
                        },
                        "tokenizers": {
                            "tokenizer1": {
                                "exceptions": ["exc. pattern"],
                                "tokenizer": "single",
                                "type": "exception"
                            }
                        }
                    },
                    "default_analyzer": "custom analyzer 2",
                    "default_datetime_parser": "datetimeparser1",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": True,
                        "enabled": True
                    },
                    "default_type": "_default",
                    "docvalues_dynamic": True,
                    "index_dynamic": True,
                    "store_dynamic": True,
                    "type_field": "_type",
                    "types": {
                        "type_mapping": {
                            "default_analyzer": "custom analyzer 2",
                            "dynamic": False,
                            "enabled": True
                        }
                    }
                },
                "store": {
                    "indexType": "scorch",
                    "segmentVersion": 15
                }
            },
            "sourceType": "gocbcore",
            "sourceName": "b1",
            "sourceUUID": "788d7b761de392e6e12228f9b5a22601",
            "sourceParams": {},
            "planParams": {
                "maxPartitionsPerPIndex": 52,
                "indexPartitions": 20,
                "numReplicas": 0
            },
            "uuid": "3d3eb8d5942ba392"
        }

        index.index_definition['params']['mapping'] = index_params['params']['mapping']
        index.index_definition['uuid'] = index.get_uuid()
        index.update()

        index_definitions[index.name] = {}
        index_definitions[index.name]['initial_def'] = {}
        index_definitions[index.name]['backup_def'] = {}
        index_definitions[index.name]['restored_def'] = {}

        _, index_def = index.get_index_defn()
        initial_index_def = index_def['indexDef']
        index_definitions[index.name]['initial_def'] = initial_index_def

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        if bucket_name:
            backup_client = FTSIndexBackupClient(fts_nodes[0], self._cb_cluster.get_bucket_by_name(bucket_name))
        else:
            backup_client = FTSIndexBackupClient(fts_nodes[0])

        # perform backup
        if bucket_name:
            status, content = backup_client.backup_bucket_level(bucket=bucket_name)
        else:
            status, content = backup_client.backup()

        backup_json = json.loads(content)
        backup = backup_json['indexDefs']['indexDefs']

        # store backup index definitions
        indexes_for_backup = eval(TestInputSingleton.input.param("expected_indexes", "[]"))
        for idx in indexes_for_backup:
            backup_index_def = backup[idx]
            index_definitions[idx]['backup_def'] = backup_index_def

        # delete all indexes before restoring from backup
        while len(self._cb_cluster.get_indexes()) > 0:
            self._cb_cluster.delete_all_fts_indexes()

        # restoring indexes from backup
        backup_client.restore()

        # getting restored indexes definitions and storing them in indexes definitions dict
        for ix_name in indexes_for_backup:
            _,restored_index_def = self.rest.get_fts_index_definition(ix_name)
            index_definitions[ix_name]['restored_def'] = restored_index_def

        #compare all 3 types of index definitions: initial, backed up, and restored from backup
        errors = self._check_indexes_definitions(index_definitions=index_definitions, indexes_for_backup=indexes_for_backup)

        self._cleanup_indexes(index_definitions)

        #errors analysis
        if len(errors.keys()) > 0:
            err_msg = ""
            for err in errors.keys():
                index_errors = errors[err]
                for msg in index_errors:
                    err_msg = err_msg + msg + "\n"
            self.fail(err_msg)


    def test_filter_missed_kv_negative(self):
        self._create_kv()
        backup_types = ["cluster", "bucket"]
        kv_types = ["bucket", "scope", "collection"]
        filter_options = ["include", "exclude"]

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        backup_client = FTSIndexBackupClient(fts_nodes[0], bucket='b1')
        for backup_type in backup_types:
            for kv_type in kv_types:
                for option in filter_options:
                    if backup_type == "bucket" and kv_type == "bucket":
                        continue
                    container = ""
                    if kv_type == "bucket":
                        container = "missed_bucket"
                    elif kv_type == "scope":
                        container = "missed_scope" if backup_type == "bucket" else "b1.missed_scope"
                    elif kv_type == "collection":
                        container = "s1.missed_collection" if backup_type == "bucket" else "b1.s1.missed_collection"
                    filter = {"option": option, "containers": [container]}

                    if backup_type == "bucket":
                        status, response = backup_client.backup_bucket_level(_filter=filter, bucket="b1")
                    else:
                        status, response = backup_client.backup(_filter=filter)
                    self.assertTrue(status, "Corresponding filter produces incorrect response: "+str(filter))

    def test_empty_filter_negative(self):
        self._create_kv()
        backup_api_include = self.rest.fts_baseUrl + "api/v1/backup?include="
        backup_api_exclude = self.rest.fts_baseUrl + "api/v1/backup?exclude="
        backup_api_bucket_level_include = self.rest.fts_baseUrl + "api/v1/bucket/b1/backup?include="
        backup_api_bucket_level_exclude = self.rest.fts_baseUrl + "api/v1/bucket/b1/backup?exclude="
        urls = [backup_api_include, backup_api_exclude, backup_api_bucket_level_include, backup_api_bucket_level_exclude]

        for url in urls:
            status, content, _ = self.rest._http_request(api=url)
            self.assertTrue(status, "The following URL produces incorrect response: "+url)

    def test_restore_send_invalid_json_negative(self):
        self._create_kv()
        backup_api = self.rest.fts_baseUrl + "api/v1/backup"
        backup_api_bucket = self.rest.fts_baseUrl + "api/v1/bucket/b1/backup"
        invalid_json = '{"uuid": "50364643d997fef6", "indexDefs": ' \
                       '    {"idx1": ' \
                       '        {"type": "fulltext-index", ' \
                       '         "name": "idx1", ' \
                       '         "uuid": "", ' \
                       '         "sourceType": "gocbcore", ' \
                       '         "sourceName": "b1", "planParams": '
        urls = [backup_api, backup_api_bucket]
        headers = self.rest._create_capi_headers()
        for url in urls:
            status, content, _ = self.rest._http_request(api=url, method="POST", params=invalid_json, headers=headers)
            self.assertFalse(status, f"POST request with incorrect JSON produces OK status. REST API URL: {url}")
            self.assertTrue(str(content).find("json unmarshal err") >= 0, f"Incorrect error message for POST request with incorrect JSON for the following endpoint: {url}")

    def test_restore_send_empty_json_negative(self):
        self._create_kv()
        backup_api = self.rest.fts_baseUrl + "api/v1/backup"
        backup_api_bucket = self.rest.fts_baseUrl + "api/v1/bucket/b1/backup"
        empty_json = '{}'
        urls = [backup_api, backup_api_bucket]
        headers = self.rest._create_capi_headers()
        for url in urls:
            status, content, _ = self.rest._http_request(api=url, method="POST", params=empty_json, headers=headers)
            self.assertFalse(status, f"POST request with incorrect JSON produces OK status. REST API URL: {url}")
            self.assertTrue(str(content).find("no index definitions parsed") >= 0, f"Incorrect error message for POST request with incorrect JSON for the following endpoint: {url}")

    def test_remap_missed_kv_negative(self):
        backup_types = ["cluster", "bucket"]
        kv_types = ["bucket", "scope", "collection"]
        index_definitions = {}

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        self._create_kv()
        backup_client = FTSIndexBackupClient(fts_nodes[0], bucket='b1')
        for backup_type in backup_types:
            for kv_type in kv_types:
                index_definitions = {}
                self._create_indexes(index_definitions=index_definitions)
                if backup_type == "bucket" and kv_type == "bucket":
                    continue
                remap = []
                if kv_type == "bucket":
                    remap.append("b1:missed_bucket")
                elif kv_type == "scope":
                    remap.append("s1:missed_scope") if backup_type == "bucket" else remap.append("b1.s1:b1.missed_scope")
                elif kv_type == "collection":
                    remap.append("s1.c1:s1.missed_collection") if backup_type == "bucket" else remap.append("b1.s1.c1:b1.s1.missed_collection")

                if backup_type == "bucket":
                    _, backup_indexes = backup_client.backup_bucket_level(bucket="b1")
                else:
                    _, backup_indexes = backup_client.backup()
                self._cb_cluster.delete_all_fts_indexes()

                if backup_type == "bucket":
                    status, content, api = backup_client.restore_bucket_level(mappings=remap)
                else:
                    status, content, api = backup_client.restore(mappings=remap)

                self._cleanup_indexes(index_definitions=index_definitions)

                if kv_type == "bucket":
                    if status:
                        print(f"Remapping to missed bucket produces OK status. Backup type: {backup_type}. API: {api}")
                    self.assertFalse(status, f"Remapping to missed bucket produces OK status. Backup type: {backup_type}. API: {api}")
                elif kv_type == "scope":
                    if status:
                        print(f"POST request with incorrect JSON produces OK status. Backup type: {backup_type}. API: {api}")
                    self.assertFalse(status, f"POST request with incorrect JSON produces OK status. Backup type: {backup_type}. API: {api}")
                    self.assertTrue(str(content).find("not found in bucket") >= 0, f"Incorrect error message for remapping to missed scope. Backup type: {backup_type}. API: {api}")
                else:
                    if status:
                        print(f"POST request with incorrect JSON produces OK status. Backup type: {backup_type}")
                    self.assertFalse(status, f"POST request with incorrect JSON produces OK status. Backup type: {backup_type}")
                    self.assertTrue(str(content).find("doesn't belong to scope") >= 0, f"Incorrect error message for remapping to missed collection. Backup type: {backup_type}. API: {api}")

        self._cb_cluster.clean_fts_indexes_array()

    def test_incorrect_remap_negative(self):
        backup_types = ["cluster", "bucket"]
        remaps = [":s2", "s1:", "s1.c1:s1", "s1.c1:", "s1.:s1.c2", ":s2.c2"]
        fts_nodes = self._cb_cluster.get_fts_nodes()
        if not fts_nodes or len(fts_nodes) == 0:
            self.fail("At least 1 fts node must be presented in cluster")

        self._create_kv()
        backup_client = FTSIndexBackupClient(fts_nodes[0], bucket='b1')
        for backup_type in backup_types:
            for remap in remaps:
                remap_array = []
                remap_array.append(remap)
                index_definitions = {}
                self._create_indexes(index_definitions=index_definitions)

                if backup_type == "bucket":
                    backup_client.backup_bucket_level(bucket="b1")
                else:
                    backup_client.backup()
                self._cleanup_indexes(index_definitions=index_definitions)
                self._cb_cluster.delete_all_fts_indexes()

                if backup_type == "bucket":
                    status, content, api = backup_client.restore_bucket_level(mappings=remap_array)
                else:
                    status, content, api = backup_client.restore(mappings=remap_array)

                self._cleanup_indexes(index_definitions=index_definitions)
                self.assertFalse(status, f"The following incorrect remap expression produces positive response: {remap}. Corresponding API: {api}")
        self._cb_cluster.clean_fts_indexes_array()

    def _decode_containers(self, encoded_containers=[]):
        decoded_containers={}
        if len(encoded_containers) > 0:
            decoded_containers["buckets"] = []
            for container in encoded_containers:
                container_path = container.split(".")
                if len(container_path) > 0:
                    bucket = container_path[0]
                    bucket_already_decoded = False
                    for b in decoded_containers["buckets"]:
                        if b["name"] == bucket:
                            bucket_already_decoded = True
                            break
                    if not bucket_already_decoded:
                        decoded_containers["buckets"].append({"name": bucket, "scopes": []})
                if len(container_path) > 1:
                    bucket = container_path[0]
                    scope = container_path[1]
                    scope_already_decoded = False
                    for b in decoded_containers["buckets"]:
                        if b["name"] == bucket:
                            for s in b["scopes"]:
                                if s["name"] == scope:
                                    scope_already_decoded = True
                                    break
                            if not scope_already_decoded:
                                b["scopes"].append({"name": scope, "collections": []})
                if len(container_path) > 2:
                    bucket = container_path[0]
                    scope = container_path[1]
                    collection = container_path[2]
                    collection_already_decoded = False
                    for b in decoded_containers["buckets"]:
                        if b["name"] == bucket:
                            for s in b["scopes"]:
                                if s["name"] == scope:
                                    for c in s["collections"]:
                                        if c["name"] == collection:
                                            collection_already_decoded = True
                                            break
                                    if not collection_already_decoded:
                                        s["collections"].append({"name": collection})
        return decoded_containers

    def _decode_index(self, encoded_index):
        decoded_index = {}

        name = encoded_index[0]
        decoded_index["name"] = name
        paths = encoded_index[1].split(",")
        path = paths[0]
        splitted_path = path.split(".")
        if len(splitted_path) > 0:
            decoded_index["bucket"] = splitted_path[0]
        if len(splitted_path) > 1:
            decoded_index["scope"] = splitted_path[1]
            if len(paths) == 1:
                decoded_index["collection"] = splitted_path[2]
            else:
                decoded_index["collection"] = []
                for p in paths:
                    collection = p.split(".")[2]
                    decoded_index["collection"].append(collection)
        return decoded_index

    def define_index_params(self, idx_dict):
        collection_index = ("scope" in idx_dict.keys() and "collection" in idx_dict.keys())

        if not collection_index:
            _type = None
            index_scope = None
            index_collections = None
        else:
            index_scope = idx_dict["scope"]
            index_collections = []
            if type(idx_dict["collection"]) is list:
                _type = []
                for c in idx_dict["collection"]:
                    _type.append(f"{index_scope}.{c}")
                    index_collections.append(c)
            else:
                _type = f"{index_scope}.{idx_dict['collection']}"
                index_collections.append(idx_dict["collection"])
        return collection_index, _type, index_scope, index_collections

    def _check_indexes_definitions(self, index_definitions={}, indexes_for_backup=[]):
        errors = {}

        #check backup filters
        for ix_name in indexes_for_backup:
            if index_definitions[ix_name]['backup_def'] == {} and ix_name in indexes_for_backup:
                error = f"Index {ix_name} is expected to be in backup, but it is not found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {} and ix_name not in indexes_for_backup:
                error = f"Index {ix_name} is not expected to be in backup, but it is found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        #check backup json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                backup_index_defn = index_definitions[ix_name]['backup_def']

                backup_check = self._validate_backup(backup_index_defn, initial_index_defn)
                if not backup_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(f"Backup fts index signature differs from original signature for index {ix_name}.")

        #check restored json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['restored_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                restored_index_defn = index_definitions[ix_name]['restored_def']['indexDef']
                restore_check = self._validate_restored(restored_index_defn, initial_index_defn)
                if not restore_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(f"Restored fts index signature differs from original signature for index {ix_name}")

        return errors

    def _validate_backup(self, backup, initial):
        if 'uuid' in initial.keys():
            del initial['uuid']
        if 'sourceUUID' in initial.keys():
            del initial['sourceUUID']
        if 'uuid' in backup.keys():
            del backup['uuid']
        return backup == initial

    def _validate_restored(self, restored, initial):
        del restored['uuid']
        if 'kvStoreName' in restored['params']['store'].keys():
            del restored['params']['store']['kvStoreName']
        if restored != initial:
            self.log(f"Initial index JSON: {initial}")
            self.log(f"Restored index JSON: {restored}")
            return False
        return True

    def _check_indexes_remap(self, index_definitions={}, expected_index_parameters={}, remap_data=[]):
        errors = {}
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['remapped_def'] != {}:
                remapped_index_defn = index_definitions[ix_name]['remapped_def']
                expected_index_params = {}
                for params in expected_index_parameters:
                    if params["name"] == ix_name:
                        expected_index_params = params
                        break
                index_remap_data = ()
                for d in remap_data:
                    if d[0] == ix_name:
                        index_remap_data = d
                        break
                remap_check = self._validate_remap(remapped_index_defn, expected_index_params, index_remap_data)
                if len(remap_check) > 0:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(remap_check)
        return errors

    def _validate_remap(self, remapped, expected, remap_data):
        errors = []
        required_source_name = str((remap_data[1].split("."))[0])
        actual_source_name = str(remapped['sourceName'])
        if required_source_name != actual_source_name:
            errors.append(f"sourceName param is not correct for index {remapped['name']}. Expected - {required_source_name}, found - {actual_source_name}")
        if expected['params']['collection_index']:
            index_types = remapped['params']['mapping']['types'].keys()
            required_type = ".".join(remap_data[1].split(".")[1:])
            if required_type not in index_types:
                errors.append(f"Remapped type is not found in index {remapped['name']}. Expected type - {required_type}")
        return errors

    def _create_kv(self):
        containers = eval(TestInputSingleton.input.param("kv", "{}"))
        decoded_containers = self._decode_containers(containers)
        test_objects_created, error = \
            self._cb_cluster.create_bucket_scope_collection_multi_structure(existing_buckets=None,
                                                                            bucket_params=self.bucket_params,
                                                                            data_structure=decoded_containers,
                                                                            cli_client=self.cli_client)
        time.sleep(10)

    def _create_indexes(self, index_definitions, index_replica=1):
        test_indexes = eval(TestInputSingleton.input.param("idx", "[]"))
        for idx in test_indexes:
            decoded_index = self._decode_index(idx)
            collection_index, _type, index_scope, index_collections = self.define_index_params(decoded_index)
            if collection_index:
                test_index = self.create_index(self._cb_cluster.get_bucket_by_name(decoded_index["bucket"]),
                                               decoded_index["name"], collection_index=True, _type=_type,
                                               scope=index_scope, collections=index_collections)
                if index_replica > 1:
                    test_index.update_num_replicas(index_replica)
            else:
                test_index = self.create_index(self._cb_cluster.get_bucket_by_name(decoded_index["bucket"]),
                                               decoded_index["name"], collection_index=False)
                if index_replica > 1:
                    test_index.update_num_replicas(index_replica)

            index_definitions[test_index.name] = {}
            index_definitions[test_index.name]['initial_def'] = {}
            index_definitions[test_index.name]['backup_def'] = {}
            index_definitions[test_index.name]['restored_def'] = {}

            _, index_def = test_index.get_index_defn()
            initial_index_def = index_def['indexDef']
            index_definitions[test_index.name]['initial_def'] = initial_index_def

    def _cleanup_indexes(self, index_definitions):
        for ix_name in index_definitions.keys():
            index_is_synced = False
            for non_server_ix in self._cb_cluster.get_indexes():
                if non_server_ix.name == ix_name:
                    index_is_synced = True
                    break
            if not index_is_synced:
                self.rest.delete_fts_index(ix_name)


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

from lib.collection.collections_rest_client import CollectionsRest
from membase.api.rest_client import RestConnection
from itertools import combinations, chain

class FTSIndexBackupClient(object):

    def __init__(self, node=None, bucket=None):
        self.rest = RestConnection(node)
        self.node = node
        self.bucket = bucket
        self.backup_api = self.rest.fts_baseUrl + "api/v1/backup"
        self.backup_api_bucket_level = self.rest.fts_baseUrl + "api/v1/bucket/{0}/backup"
        self.is_backup_exists = False
        self.backup_data = {}

    def backup(self, _filter=None):
        query_params = ""
        if _filter:
            query_params = "?"+_filter["option"]+"="
            for c in _filter["containers"]:
                query_params = query_params + c +","
        query_params = query_params[:len(query_params)-1]
        api = self.backup_api + query_params
        status, content, _ = self.rest._http_request(api=api)
        if status:
            self.backup_data = json.loads(content)['indexDefs']
            self.is_backup_exists = True
        return status, content

    def restore(self, mappings=[]):
        if not self.is_backup_exists:
            raise Exception("No backup found")
        query_params = ""
        if mappings:
            query_params = "?remap={0}".format(",".join(mappings))
        headers = self.rest._create_capi_headers()
        body = json.dumps(self.backup_data)
        api = self.backup_api + query_params
        status, content, _ = self.rest._http_request(
            api=api, method="POST", params=body, headers=headers)
        json_response = json.loads(content)
        if json_response['status'] == "ok":
            return True, json_response, api
        return False, json_response, api

    def restore_bucket_level(self, mappings=[]):
        if not self.is_backup_exists:
            raise Exception("No backup found")
        query_params = ""
        if mappings:
            query_params = "?remap={0}".format(",".join(mappings))
        headers = self.rest._create_capi_headers()
        body = json.dumps(self.backup_data)
        api = self.backup_api_bucket_level + query_params
        status, content, _ = self.rest._http_request(
            api=api, method="POST", params=body, headers=headers)
        json_response = json.loads(content)
        if json_response['status'] == "ok":
            return True, json_response, api
        return False, json_response, api


    def backup_bucket_level(self, _filter=None, bucket=""):
        query_params = ""
        if _filter:
            query_params = "?"+_filter["option"]+"="
            for c in _filter["containers"]:
                query_params = query_params + c +","
        query_params = query_params[:len(query_params)-1]
        api = self.backup_api_bucket_level.format(bucket) + query_params
        status, content, _ = self.rest._http_request(api=api)
        if status:
            self.backup_data = json.loads(content)['indexDefs']
            self.is_backup_exists = True
        return status, content
