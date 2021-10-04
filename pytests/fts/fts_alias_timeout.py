# coding=utf-8

from .fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection

class FTSIndexAliasTimeout(FTSBaseTest):

    def setUp(self):
        super(FTSIndexAliasTimeout, self).setUp()
        self.fts_query = {"wildcard": "000*", "field": "body"}

    def tearDown(self):
        super(FTSIndexAliasTimeout, self).tearDown()

    def test_alias_timeout(self):
        timeout = self._input.param("timeout", None)
        partitions = self._input.param("partitions", 1)
        num_items = self._input.param("items", 1000000)

        self.load_cbworkloadgen(num_items=num_items)

        alias_content = self._input.param("alias_content", None)
        alias_parts = alias_content.split("-")
        control_objects = []
        alias_components = []
        # Create all required fts index alias parts. Can be slow fts index, fast fts index, fts index alias.
        for part in alias_parts:
            if "slow_index" == part:
                collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
                idx = self.create_index(
                    bucket=self._cb_cluster.get_bucket_by_name('default'),
                    index_name="fts_idx_slow",
                    collection_index=collection_index, _type=_type,
                    scope=index_scope,
                    collections=index_collections)
                idx.update_index_partitions(partitions)
                self.wait_for_indexing_complete()
                control_objects.append(idx)
                alias_components.append(idx)
            elif "fast_index" == part:
                collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
                idx = self.create_index(
                    bucket=self._cb_cluster.get_bucket_by_name('default'),
                    index_name="fts_idx_fast",
                    collection_index=collection_index, _type=_type,
                    scope=index_scope,
                    collections=index_collections)
                idx.update_index_partitions(partitions)

                idx.index_definition['params']['doc_config'] = {}
                doc_config = {}
                doc_config['mode'] = 'type_field'
                doc_config['type_field'] = 'name'
                idx.index_definition['params']['doc_config'] = doc_config

                idx.add_type_mapping_to_index_definition(type="filler",
                                                           analyzer="standard")
                idx.index_definition['params']['mapping'] = {
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": False,
                        "enabled": False
                    },
                    "default_type": "_default",
                    "docvalues_dynamic": True,
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {
                        "pymc100": {
                            "default_analyzer": "standard",
                            "dynamic": True,
                            "enabled": True,
                        }
                    }
                }
                idx.index_definition['uuid'] = idx.get_uuid()
                idx.update()

                self.wait_for_indexing_complete()
                control_objects.append(idx)
                alias_components.append(idx)
            elif "alias" == part:
                if len(control_objects) == 0:
                    collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
                    idx = self.create_index(
                        bucket=self._cb_cluster.get_bucket_by_name('default'),
                        index_name=f"fts_idx_slow",
                        collection_index=collection_index, _type=_type,
                        scope=index_scope,
                        collections=index_collections)
                    idx.update_index_partitions(partitions)
                    self.wait_for_indexing_complete()
                else:
                    idx = control_objects[0]
                control_alias = self.create_alias(target_indexes=[idx], name=f"fts_component_alias")
                control_objects.append(control_alias)
                alias_components.append(control_alias)

        idx_alias = self.create_alias(target_indexes=alias_components, name="fts_idx_alias")

        alias_request_timeout = self.define_timeout(timeout_type=timeout)

        hits, _, _, status = idx_alias.execute_query(self.fts_query, timeout=alias_request_timeout, explain=False)

        alias_parts_hits = 0
        # Getting sum of hits for all alias parts and compare this sum sith alias hits.
        for fts_obj in control_objects:
            control_hits, _, _, control_status = fts_obj.execute_query(self.fts_query, timeout=alias_request_timeout, explain=False)
            if control_hits >= 0:
              alias_parts_hits = alias_parts_hits + control_hits

        self.assertEqual(hits, alias_parts_hits, "Hits count is different for alias and for alias parts")

    def define_timeout(self, timeout_type=None):
        if "small" == timeout_type:
            return self.find_small_timeout()
        elif "medium" == timeout_type:
            return self.find_medium_timeout()
        elif "big" == timeout_type:
            return self.find_big_timeout()

    def find_small_timeout(self):
        found = False
        timeout = 2000
        # Calculate maximum timeout value when timeout exceed exception happens.
        while not found:
            slow_index = self._cb_cluster.get_fts_index_by_name("fts_idx_slow")
            hits, matches, time_taken, status = slow_index.execute_query(self.fts_query, timeout=timeout, explain=True)
            if status == "fail" or "context deadline exceeded" in str(status):
                return int(timeout / 2)
            timeout = int(timeout / 2)
            if timeout == 0:
                self.fail("Unable to simulate request timeout.")

    def find_big_timeout(self):
        found = False
        timeout = 1
        # Calculate minimum timeout value when timeout exceed exception does not happen.
        while not found:
            slow_index = self._cb_cluster.get_fts_index_by_name("fts_idx_slow")
            hits, matches, time_taken, status = slow_index.execute_query(self.fts_query, timeout=timeout, explain=True)
            if status != "fail" and "context deadline exceeded" not in str(status):
                return int(timeout * 2)
            timeout = int(timeout * 2)

    def find_medium_timeout(self):
        small_timeout = self.find_small_timeout()
        big_timeout = self.find_big_timeout()
        return int((small_timeout+big_timeout) / 2)

    def load_cbworkloadgen(self, num_items=1000000, is_insert=False):
        if is_insert:
            command_options = '-j --prefix inserts'
        else:
            command_options = '-j'
        shell = RemoteMachineShellConnection(self.master)
        for bucket in self._cb_cluster.get_buckets():
            shell.execute_cbworkloadgen(self.master.rest_username,
                                        self.master.rest_password,
                                        num_items,
                                        100,
                                        bucket.name,
                                        1024,
                                        command_options)
