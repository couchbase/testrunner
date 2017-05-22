import logging
import random

from string import lowercase
from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.documentgenerator import  DocumentGenerator
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

log = logging.getLogger(__name__)

class GSIUnhandledIndexItems(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIUnhandledIndexItems, self).setUp()
        self.num_docs = self.input.param("num_docs", 10)
        self.allow_large_keys = self.input.param("allow_large_keys", False)
        self.max_array_size = self.input.param("max_array_size", 4000)
        self.max_item_size = self.input.param("max_item_size", 4000)
        self.indexer_node = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.indexer_node)
        self.sleep(30)

    def tearDown(self):
        super(GSIUnhandledIndexItems, self).tearDown()

    def test_set_invalid_limits(self):
        index_settings = self.rest.get_index_settings()
        pre_set_allow_large_keys = index_settings["indexer.settings.allow_large_keys"]
        pre_set_array_size = index_settings["indexer.settings.max_array_seckey_size"]
        pre_set_item_size = index_settings["indexer.settings.max_seckey_size"]
        invalid_values = ["abc123", "4565", [1, 2, 3]]
        for val in invalid_values:
            self.set_allow_large_keys(val)
            self.change_max_item_size(val)
            self.change_max_array_size(val)
            index_settings = self.rest.get_index_settings()
            post_set_allow_large_keys = index_settings["indexer.settings.allow_large_keys"]
            post_set_array_size = index_settings["indexer.settings.max_array_seckey_size"]
            post_set_item_size = index_settings["indexer.settings.max_seckey_size"]
            self.assertEqual(pre_set_allow_large_keys, post_set_allow_large_keys, "allow_large_kays is set to {0}".format(val))
            self.assertEqual(pre_set_array_size, post_set_array_size, "max_array_size is set to {0}".format(val))
            self.assertEqual(pre_set_item_size, post_set_item_size, "max_item_size is set to {0}".format(val))

    def test_max_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        generators = self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        self.full_docs_list = self.generate_full_docs_list(generators)
        query_definitions = self._create_indexes()
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition, self.max_array_size, self.max_item_size)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))

    def test_change_max_item_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        generators = self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        self.full_docs_list = self.generate_full_docs_list(generators)
        query_definitions = self._create_indexes()
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        self.change_max_item_size(self.max_item_size * 3)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition, self.max_array_size, self.max_item_size)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))

    def test_max_limits_change_item_size(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition, self.max_array_size, self.max_item_size)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))
        self._upload_documents(num_items=self.num_docs, item_size=self.max_item_size/4,
                                            array_size=self.max_array_size/4, update_docs=True)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition, self.max_array_size, self.max_item_size)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))

    def test_change_max_array_limits(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        self.change_max_array_size(self.max_array_size * 3)
        self._upload_documents(
            num_items=self.num_docs, item_size=self.max_item_size,
            array_size=self.max_array_size, update_docs=True)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition, self.max_array_size, self.max_item_size)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))

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
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        #full table scan
        if (isinstance(self.allow_large_keys, str) and
                    self.allow_large_keys.lower() == "false") or \
            (isinstance(self.allow_large_keys, bool) and
             self.allow_large_keys == False):
            self.set_allow_large_keys(True)
        else:
            self.set_allow_large_keys(False)
        self._upload_documents(
            num_items=self.num_docs, item_size=self.max_item_size,
            array_size=self.max_array_size, update_docs=True)
        for bucket in self.buckets:
            for query_definition in query_definitions:
                index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                    index_id, body={"stale": "false"})
                expected_result = self._get_expected_results_for_scan(
                    query_definition, 4000, 4000)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))

    def test_max_limits_indexer_restart(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        self._upload_documents(num_items=self.num_docs,
                                            item_size=self.max_item_size,
                                            array_size=self.max_array_size)
        query_definitions = self._create_indexes()
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
                    query_definition, self.max_array_size, self.max_item_size)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))

    def test_various_docid_keysize_combinations(self):
        self.set_allow_large_keys(self.allow_large_keys)
        self.change_max_item_size(self.max_item_size)
        self.change_max_array_size(self.max_array_size)
        query_definitions = self._create_indexes()
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket)
        generators = []
        template = '{{"name":"{0}", "age":{1}, "encoded_array": {2}, "encoded_big_value_array": {3}}}'
        max_item_length = self.max_item_size * 4
        max_array_element_size = (self.max_array_size * 4)/ 10
        for i in range(self.num_docs):
            index_id = "".join(random.choice(lowercase) for k in range(random.randint(1, 255)))
            encoded_array = []
            name = "".join(random.choice(lowercase) for k in range(random.randint(max_item_length)))
            age = random.choice(range(4, 59))
            big_value_array = [name]
            for j in range(30):
                element = "".join(random.choice(lowercase) for k in range(random.randint(max_array_element_size)))
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
                    query_definition, self.max_array_size, self.max_item_size)
                self.assertEqual(sorted(actual_result), sorted(expected_result),
                                 "results don't match for index {0}".format(query_definition.index_name))

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

    def _upload_documents(self, num_items, item_size, array_size, update_docs=False, array_elements=30):
        generators = []
        template = '{{"name":"{0}", "age":{1}, "encoded_array": {2}, "encoded_big_value_array": {3}}}'
        item_length = item_size * 4
        array_element_size = (array_size * 4)/array_elements
        if update_docs:
            num_items = len(self.full_docs_list)
        for i in range(num_items):
            if update_docs:
                index_id = str(self.full_docs_list[i]["_id"])
            else:
                index_id = "unhandled_items_" + str(random.random()*100000)
            encoded_array = []
            name = "".join(random.choice(lowercase) for k in range(item_length))
            age = random.choice(range(4, 59))
            big_value_array = [name]
            for j in range(array_elements):
                element = "".join(random.choice(lowercase) for k in range(array_element_size))
                encoded_array.append(element)
            generators.append(DocumentGenerator(
                index_id, template, [name], [age], [encoded_array],
                [big_value_array], start=0, end=1))
        self.load(generators, buckets=self.buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        self.full_docs_list = self.generate_full_docs_list(generators)

    def _get_expected_results_for_scan(self, query, array_size, item_size):
        index_settings = self.rest.get_index_settings()
        allow_large_keys = index_settings["indexer.settings.allow_large_keys"]
        log.info(allow_large_keys)
        index_fields = []
        for index_field in query.index_fields:
            temp = index_field.split("`")
            if len(temp) > 1:
                index_fields.append(temp[1])
            else:
                index_fields.append(temp[0])
        expected_result = []
        docid_list = []
        for field in index_fields:
            for doc in self.full_docs_list:
                found = 0
                if not field in doc.keys():
                    break
                if isinstance(doc[field], list):
                    actual_array_size = self._get_size_of_array(doc[field])
                    if not allow_large_keys and (actual_array_size > array_size):
                        docid_list.append(doc["_id"])
                        continue
                    for expected_doc in sorted(expected_result):
                        if expected_doc["docid"] == doc["_id"]:
                            found = 1
                            temp_list = []
                            for list_entry in doc[field]:
                                entry = {"docid": doc["_id"], "key": expected_doc["key"] + [list_entry]}
                                temp_list.append(entry)
                            expected_result.remove(expected_doc)
                            expected_result.extend(temp_list)
                    if not found:
                        for list_entry in doc[field]:
                            entry = {"docid": doc["_id"], "key": [list_entry]}
                            expected_result.append(entry)
                else:
                    for expected_doc in sorted(expected_result):
                        if expected_doc["docid"] == doc["_id"]:
                            found = 1
                            expected_doc["key"].append(doc[field])
                    if not found:
                        entry = {"docid": doc["_id"], "key": [doc[field]]}
                        expected_result.append(entry)
        if not allow_large_keys:
            expected_result = [doc for doc in expected_result if doc["docid"] not in docid_list]
            expected_result = [doc for doc in expected_result if self._get_size_of_array(doc["key"]) > item_size]

            # for doc in sorted(expected_result):
            #     if docid and doc["docid"] == docid:
            #         expected_result.remove(doc)
            #     else:
            #         docid = ""
            #         if self._get_size_of_array(doc["key"]) > item_size:
            #             docid = doc["docid"]
            #             expected_result.remove(doc)
        return expected_result

    def _get_size_of_array(self, array):
        return sum(len(str(element)) for element in array)

    def change_max_array_size(self, array_size):
        doc = {"indexer.settings.max_array_seckey_size": array_size}
        self.rest.set_index_settings(doc)

    def change_max_item_size(self, item_size):
        doc = {"indexer.settings.max_seckey_size": item_size}
        self.rest.set_index_settings(doc)

    def set_allow_large_keys(self, val):
        doc = {"indexer.settings.allow_large_keys": val}
        self.rest.set_index_settings(doc)
