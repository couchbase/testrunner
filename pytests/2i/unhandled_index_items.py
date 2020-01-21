import copy
import logging
import random
import pdb

from string import ascii_lowercase
from .base_2i import BaseSecondaryIndexingTests
from couchbase.bucket import Bucket
from couchbase_helper.documentgenerator import  DocumentGenerator
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from deepdiff import DeepDiff

log = logging.getLogger(__name__)

class GSIUnhandledIndexItems(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIUnhandledIndexItems, self).setUp()
        self.num_docs = self.input.param("num_docs", 1)
        self.allow_large_keys = self.input.param("allow_large_keys", False)
        self.max_array_size = self.input.param("max_array_size", 4000)
        self.max_item_size = self.input.param("max_item_size", 4000)
        self.repeat = self.input.param("repeat", False)
        self.indexer_node = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.indexer_node)
        testuser = []
        rolelist = []
        for bucket in self.buckets:
            testuser.append({'id': bucket.name, 'name': bucket.name, 'password': 'password'})
            rolelist.append({'id': bucket.name, 'name': bucket.name, 'roles': 'admin'})
            self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)

    def tearDown(self):
        super(GSIUnhandledIndexItems, self).tearDown()

    def test_set_invalid_limits(self):
        index_settings = self.rest.get_index_settings()
        pre_set_allow_large_keys = index_settings["indexer.settings.allow_large_keys"]
        pre_set_array_size = index_settings["indexer.settings.max_array_seckey_size"]
        pre_set_item_size = index_settings["indexer.settings.max_seckey_size"]
        invalid_values = ["abc123", "4565", [1, 2, 3], -4000, 34.65]
        for val in invalid_values:
            self.set_allow_large_keys(val)
            try:
                self.change_max_item_size(val)
                self.change_max_array_size(val)
            except Exception as ex:
                msg = "Setting should be an integer greater than 0"
                self.assertIn(msg, str(ex), "Exception {0} for value {1}".format(str(ex), val))
            else:
                index_settings = self.rest.get_index_settings()
                post_set_allow_large_keys = index_settings["indexer.settings.allow_large_keys"]
                self.assertEqual(pre_set_allow_large_keys, post_set_allow_large_keys, "allow_large_kays is set to {0}".format(post_set_allow_large_keys))
                post_set_array_size = index_settings["indexer.settings.max_array_seckey_size"]
                post_set_item_size = index_settings["indexer.settings.max_seckey_size"]
                if not isinstance(val, float):
                    post_set_array_size = index_settings["indexer.settings.max_array_seckey_size"]
                    post_set_item_size = index_settings["indexer.settings.max_seckey_size"]
                else:
                    pre_set_item_size = int(val)
                    pre_set_array_size = int(val)
                self.assertEqual(post_set_array_size, pre_set_array_size, "max_array_size is set to {0}".format(post_set_array_size))
                self.assertEqual(pre_set_item_size, pre_set_item_size, "max_item_size is set to {0}".format(post_set_item_size))

    def test_max_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        try:
            self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        except Exception as ex:
            msg_list = ["Too big", "Invalid"]
            for msg in msg_list:
                if msg in str(ex):
                    log.info("Document being loaded is {0}".format(msg))
        else:
            query_definitions = self._create_indexes()
            self.sleep(10)
            rest = RestConnection(self.master)
            index_map = rest.get_index_id_map()
            #full table scan
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #                 msg.format(query_definition.index_name,
                    #                            actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)

    def test_increase_max_item_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        item_size_limit = self.max_item_size
        for i in range(5):
            item_size_limit += self.max_item_size
            self.change_max_item_size(item_size_limit)
            self._upload_documents(
                num_items=self.num_docs, item_size=self.max_item_size,
                array_size=self.max_array_size, update_docs=True)
            self.sleep(10)
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #                 msg.format(query_definition.index_name,
                    #                            actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            if not self.repeat:
                break

    def test_decrease_max_item_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size * 5)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        item_size_limit = self.max_item_size * 5
        for i in range(3):
            item_size_limit -= self.max_item_size
            self.change_max_item_size(item_size_limit)
            self._upload_documents(
                num_items=self.num_docs, item_size=self.max_item_size,
                array_size=self.max_array_size, update_docs=True)
            self.sleep(10)
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(
                        query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #                 msg.format(query_definition.index_name,
                    #                            actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            if not self.repeat:
                break

    def test_max_limits_increase_item_size(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size//4)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size//4,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size,
                                            array_size=self.max_array_size, update_docs=True)
        self.sleep(10)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def test_max_limits_decrease_item_size(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size//4,
                                            array_size=self.max_array_size, update_docs=True)
        self.sleep(10)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def test_increase_max_array_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        array_size_limit = self.max_array_size
        for i in range(5):
            array_size_limit += self.max_array_size
            self.change_max_array_size(self.max_array_size * 3)
            self._upload_documents(
                num_items=self.num_docs, item_size=self.max_item_size,
                array_size=self.max_array_size, update_docs=True)
            self.sleep(10)
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(
                        query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #             msg.format(query_definition.index_name,
                    #                        actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)

    def test_decrease_max_array_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size*5)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        array_size_limit = self.max_array_size * 5
        for i in range(3):
            array_size_limit -= self.max_array_size
            self.change_max_item_size(array_size_limit)
            self._upload_documents(
                num_items=self.num_docs, item_size=self.max_item_size,
                array_size=self.max_array_size, update_docs=True)
            self.sleep(10)
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(
                        query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #             msg.format(query_definition.index_name,
                    #                        actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            if not self.repeat:
                break

    def test_max_limits_increase_array_size(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size//4)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size,
                                            array_size=self.max_array_size//4)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size,
                                            array_size=self.max_array_size, update_docs=True)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def test_max_limits_decrease_array_size(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size,
                                            array_size=self.max_array_size//4, update_docs=True)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def test_change_allow_large_key(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        for i in range(5):
            if (isinstance(self.allow_large_keys, str) and
                        self.allow_large_keys.lower() == "false") or \
                (isinstance(self.allow_large_keys, bool) and
                 self.allow_large_keys == False):
                self.allow_large_keys = True
            else:
                self.allow_large_keys = False
            self.set_allow_large_keys(self.allow_large_keys)
            self._upload_documents(
                num_items=self.num_docs, item_size=self.max_item_size,
                array_size=self.max_array_size, update_docs=True)
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(
                        query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #             msg.format(query_definition.index_name,
                    #                        actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            if not self.repeat:
                break

    def test_max_limits_indexer_restart(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        remote = RemoteMachineShellConnection(self.indexer_node)
        remote.stop_server()
        self.sleep(30)
        remote = RemoteMachineShellConnection(self.indexer_node)
        remote.start_server()
        self.sleep(30)
        msg = "Cluster not in Healthy state"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def test_random_increase_decrease_size_limit(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        choice_list = ["increase", "decrease"]
        for i in range(random.randint(5, 10)):
            option = random.choice(choice_list)
            if option == "increase":
                self.max_item_size += random.randint(1000, 3000)
                self.change_max_item_size(self.max_item_size)
            else:
                self.max_item_size += random.randint(1000, 3000)
                self.change_max_item_size(self.max_item_size)
            option = random.choice(choice_list)
            if option == "increase":
                self.max_array_size += random.randint(1000, 3000)
                self.change_max_item_size(self.max_array_size)
            else:
                self.max_array_size += random.randint(1000, 3000)
                self.change_max_item_size(self.max_array_size)
            self._upload_documents(
                num_items=self.num_docs, item_size=self.max_item_size,
                array_size=self.max_array_size, update_docs=True)
            for bucket in self.buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(
                        query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #             msg.format(query_definition.index_name,
                    #                        actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)

    def test_various_docid_keysize_combinations(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        query_definitions = self._create_indexes()
        self.sleep(30)
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        generators = []
        template = '{{"name":"{0}", "age":{1}, "encoded_array": {2}, "encoded_big_value_array": {3}}}'
        max_item_length = self.max_item_size * 4
        max_array_element_size = (self.max_array_size * 4)// 10
        for i in range(self.num_docs):
            index_id = "".join(random.choice(ascii_lowercase) for k in range(random.randint(1, 255)))
            encoded_array = []
            name = "".join(random.choice(ascii_lowercase) for k in range(random.randint(max_item_length)))
            age = random.choice(list(range(4, 59)))
            big_value_array = [name]
            for j in range(30):
                element = "".join(random.choice(ascii_lowercase) for k in range(random.randint(max_array_element_size)))
                encoded_array.append(element)
            generators.append(DocumentGenerator(
                index_id, template, [name], [age], [encoded_array],
                [big_value_array], start=0, end=1))
        index_id = "".join(random.choice(ascii_lowercase) for k in range(250))
        name = "".join(random.choice(ascii_lowercase) for k in range(random.randint(max_item_length)))
        age = random.choice(list(range(4, 59)))
        big_value_array = [name]
        encoded_array = []
        for j in range(30):
            element = "".join(random.choice(ascii_lowercase) for k in range(random.randint(max_array_element_size)))
            encoded_array.append(element)
        generators.append(DocumentGenerator(
            index_id, template, [name], [age], [encoded_array],
            [big_value_array], start=0, end=1))
        self.load(generators, buckets=self.buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        self.full_docs_list = self.generate_full_docs_list(generators)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition)
                msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                #self.assertEqual(sorted(actual_result), sorted(expected_result),
                #                 msg.format(query_definition.index_name,
                #                            actual_result, expected_result))
                diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def _create_indexes(self):
        query_definitions = []
        query_definitions.append(QueryDefinition(index_name="index_long_name",
                            index_fields=["name"]))
        query_definitions.append(QueryDefinition(index_name="index_array_encoded",
                            index_fields=["ALL ARRAY t FOR t in `encoded_array` END"]))
        query_definitions.append(QueryDefinition(index_name="index_array_encoded_bigValue",
                            index_fields=["ALL ARRAY t FOR t in `encoded_big_value_array` END"]))
        query_definitions.append(QueryDefinition(index_name="index_long_name_age",
                            index_fields=["name", "age"]))
        query_definitions.append(QueryDefinition(
            index_name="index_long_endoded_age",
            index_fields=["ALL ARRAY t FOR t in `encoded_array` END", "age"]))
        query_definitions.append(QueryDefinition(
            index_name="index_long_endoded_name",
            index_fields=["ALL ARRAY t FOR t in `encoded_array` END", "name"]))
        query_definitions.append(QueryDefinition(
            index_name="index_long_name_encoded_age",
            index_fields=["name", "ALL ARRAY t FOR t in `encoded_array` END", "age"]))
        self.multi_create_index(query_definitions=query_definitions)
        return query_definitions

    def _upload_documents(self, num_items, item_size, array_size, update_docs=False, array_elements=3):
        if not update_docs:
            for bucket in self.buckets:
                self.rest.flush_bucket(bucket)
            self.sleep(30)
        generators = []
        template = '{{"name":"{0}", "age":{1}, "encoded_array": {2}, "encoded_big_value_array": {3}}}'
        item_length = item_size * 4
        array_element_size = (array_size * 4)//array_elements
        if update_docs:
            num_items = len(self.full_docs_list)
        for i in range(num_items):
            if update_docs:
                index_id = str(self.full_docs_list[i]["_id"].split("-")[0])
            else:
                index_id = "unhandled_items_" + str(random.random()*100000)
            encoded_array = []
            name = "".join(random.choice(ascii_lowercase) for k in range(item_length))
            age = random.choice(list(range(4, 59)))
            big_value_array = [name]
            for j in range(array_elements):
                element = "".join(random.choice(ascii_lowercase) for k in range(array_element_size))
                encoded_array.append(element)
            generators.append(DocumentGenerator(
                index_id, template, [name], [age], [encoded_array],
                [big_value_array], start=0, end=1))
        self.full_docs_list = self.generate_full_docs_list(generators)
        if not update_docs:
            self.load(generators, buckets=self.buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        else:
            for bucket in self.buckets:
                for doc in self.full_docs_list:
                    self._update_document(bucket.name, doc["_id"], doc)

    def _get_expected_results_for_scan(self, query):
        index_settings = self.rest.get_index_settings()
        allow_large_keys = index_settings["indexer.settings.allow_large_keys"]
        array_size = index_settings["indexer.settings.max_array_seckey_size"] * 3
        item_size = index_settings["indexer.settings.max_seckey_size"] * 3
        index_fields = []
        for index_field in query.index_fields:
            temp = index_field.split("`")
            if len(temp) > 1:
                index_fields.append(temp[1])
            else:
                index_fields.append(temp[0])
        expected_result = []
        for doc in self.full_docs_list:
            doc_list = []
            list_param = False
            for field in index_fields:
                if isinstance(doc[field], list):
                    list_param = True
                    if not doc_list:
                        doc_list = [[arr_item] for arr_item in doc[field]]
                    else:
                        temp_doc_list = []
                        for item in doc_list:
                            for arr_item in doc[field]:
                                temp_list = copy.deepcopy(item)
                                temp_list.append(arr_item)
                                temp_doc_list.append(temp_list)
                        doc_list = temp_doc_list
                else:
                    if not doc_list:
                        doc_list.append([doc[field]])
                    else:
                        for item in doc_list:
                            item.append(doc[field])
            if not allow_large_keys:
                if list_param:
                    actual_array_size = self._get_size_of_array(doc_list)
                    if actual_array_size > array_size:
                        doc_list = []
                for doc_items in doc_list:
                    if self._get_size_of_array(doc_items) > item_size:
                        doc_list = []
                        break
            for doc_items in doc_list:
                entry = {"docid": doc["_id"], "key": doc_items}
                expected_result.append(entry)
        return expected_result

    def _update_document(self, bucket_name, key, document):
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=bucket_name)
        bucket = Bucket(url, username=bucket_name, password="password")
        bucket.upsert(key, document)

    def _get_size_of_array(self, array):
        arr_len = 0
        if isinstance(array[0], list):
            for element in array:
                arr_len += self._get_size_of_array(element)
        else:
            arr_len += sum(len(str(element)) for element in array)
        return arr_len

    def change_max_array_size(self, array_size):
        doc = {"indexer.settings.max_array_seckey_size": array_size}
        self.rest.set_index_settings(doc)
        self.sleep(10)

    def change_max_item_size(self, item_size):
        doc = {"indexer.settings.max_seckey_size": item_size}
        self.rest.set_index_settings(doc)
        self.sleep(10)

    def set_allow_large_keys(self, val):
        doc = {"indexer.settings.allow_large_keys": val}
        self.rest.set_index_settings(doc)
        self.sleep(10)
