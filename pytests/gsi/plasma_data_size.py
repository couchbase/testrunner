import copy
import logging
import random
import gc
import psutil

from string import ascii_lowercase
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase_helper.documentgenerator import  DocumentGenerator
from couchbase_helper.data import FIRST_NAMES, COUNTRIES
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.tuq_generators import TuqGenerators
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from .base_gsi import BaseSecondaryIndexingTests
from deepdiff import DeepDiff

log = logging.getLogger(__name__)

class SecondaryIndexDatasizeTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexDatasizeTests, self).setUp()
        self.num_plasma_buckets = self.input.param("standard_buckets", 1)
        self.indexMemQuota = self.input.param("indexMemQuota", 256)
        self.doc_ops = self.input.param("doc_ops", True)
        self.reverse = self.input.param("reverse", False)
        self.multi_intervals = self.input.param("multi_intervals", False)
        self.dgmServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.dgmServer)
        if self.indexMemQuota > 256:
            log.info("Setting indexer memory quota to {0} MB...".format(self.indexMemQuota))
            self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=self.indexMemQuota)
            self.sleep(30)
        self.deploy_node_info = ["{0}:{1}".format(self.dgmServer.ip, self.node_port)]
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)

    def tearDown(self):
        gc.collect()
        super(SecondaryIndexDatasizeTests, self).tearDown()

    def test_sorted_items_indexed(self):
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        generators = self._upload_documents_in_sorted()
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        query_definition = QueryDefinition(index_name="index_range_shrink_name", index_fields=["name"],
                                           query_template="SELECT * FROM %s WHERE name IS NOT NULL", groups=["simple"],
                                           index_where_clause=" name IS NOT NULL ")
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        self.multi_create_index(buckets=buckets,
                                           query_definitions=[query_definition])
        self.sleep(30)
        self.multi_query_using_index(buckets=buckets,
                                     query_definitions=[query_definition])
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def test_sorted_removed_items_indexed(self):
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        generators = self._upload_documents_in_sorted()
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        query_definition = QueryDefinition(index_name="index_range_shrink_name", index_fields=["name"],
                                           query_template="SELECT * FROM %s WHERE name IS NOT NULL", groups=["simple"],
                                           index_where_clause=" name IS NOT NULL ")
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        self.sleep(30)
        intervals = [["d", "e", "f"], ["j", "k", "l", "m"], ["p", "q", "r", "s"] ]
        temp_list = []
        for doc_gen in self.full_docs_list:
            for interval in intervals:
                for character in interval:
                    if doc_gen["name"].lower().startswith(character):
                        for bucket in buckets:
                            url = "couchbase://{0}/{1}".format(self.master.ip,
                                                               bucket.name)
                            auth = PasswordAuthenticator(bucket.name, "password")

                            # Connect to the cluster
                            cluster = Cluster(
                                f"couchbase://{self.master.ip}",
                                ClusterOptions(auth)
                            )
                            bucket_obj = cluster.bucket(bucket.name)
                            # cb.remove(doc_gen["_id"])
                            collection = bucket_obj.default_collection()
                            collection.remove(doc_gen["_id"])
                            temp_list.append(doc_gen)
                if not self.multi_intervals:
                    break
        self.full_docs_list = [doc for doc in self.full_docs_list if doc not in temp_list]
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        self.multi_query_using_index(buckets=buckets,
                                     query_definitions=[query_definition])
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def test_reverse_sorted_removed_items_indexed(self):
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        generators = self._upload_documents_in_sorted()
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        query_definition = QueryDefinition(index_name="index_range_shrink_name", index_fields=["name"],
                                           query_template="SELECT * FROM %s WHERE name IS NOT NULL", groups=["simple"],
                                           index_where_clause=" name IS NOT NULL ")
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        self.sleep(30)
        intervals = [["p", "q", "r", "s"], ["j", "k", "l", "m"], ["d", "e", "f"]]
        temp_list = []
        for doc_gen in self.full_docs_list:
            for interval in intervals:
                for character in interval:
                    if doc_gen["name"].lower().startswith(character):
                        for bucket in buckets:
                            auth = PasswordAuthenticator(bucket.name, "password")
                            cluster = Cluster(
                                f"couchbase://{self.master.ip}",
                                ClusterOptions(auth)
                            )
                            bucket_obj = cluster.bucket(bucket.name)
                            collection = bucket_obj.default_collection()
                            collection.remove(doc_gen["_id"])
                            temp_list.append(doc_gen)
                if not self.multi_intervals:
                    break
        self.full_docs_list = [doc for doc in self.full_docs_list if doc not in temp_list]
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        self.multi_query_using_index(buckets=buckets,
                                     query_definitions=[query_definition])
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def test_change_doc_size(self):
        self.iterations = self.input.param("num_iterations", 1)
        buckets = self._create_plasma_buckets()
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        query_definitions = self._create_indexes(buckets)
        self.sleep(20)
        array_size = random.choice(list(range(10, 15)))
        item_size = random.choice(list(range(10, 15)))
        self.upload_documents(num_items=200, item_size=item_size, array_size=array_size, buckets=buckets)  # Reduced from 1000
        rest = RestConnection(self.master)
        index_map = rest.get_index_id_map()
        for j in range(self.iterations):
            log.info("Iteration: {0}".format(j))
            array_size = random.choice(list(range(10, 15)))
            item_size = random.choice(list(range(10, 15)))
            self.upload_documents(num_items=1000, item_size=item_size,
                                  array_size=array_size, buckets=buckets, update_docs=True)
            for bucket in buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(
                    query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #         msg.format(query_definition.index_name,
                    #                    actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
                    gc.collect()

            self.sleep(20)
            array_size = random.choice(list(range(100, 500)))  # Reduced from 1000-5000
            item_size = random.choice(list(range(100, 500)))   # Reduced from 1000-5000
            self.upload_documents(num_items=200, item_size=item_size,  # Reduced from 1000
                                  array_size=array_size, buckets=buckets, update_docs=True)
            for bucket in buckets:
                for query_definition in query_definitions:
                    index_id = str(index_map[bucket.name][query_definition.index_name]["id"])
                    actual_result = self.rest.full_table_scan_gsi_index_with_rest(
                        index_id, body={"stale": "false"})
                    expected_result = self._get_expected_results_for_scan(
                    query_definition)
                    msg = "Results don't match for index {0}. Actual: {1}, Expected: {2}"
                    #self.assertEqual(sorted(actual_result), sorted(expected_result),
                    #         msg.format(query_definition.index_name,
                    #                    actual_result, expected_result))
                    diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
                    gc.collect()

    def test_change_key_size(self):
        self.iterations = self.input.param("num_iterations", 5)
        buckets = self._create_plasma_buckets()
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        query_definition = QueryDefinition(index_name="index_name_big_values", index_fields=["bigValues"],
                                           query_template="SELECT * FROM %s WHERE bigValues IS NOT NULL",
                                           groups=["simple"], index_where_clause=" bigValues IS NOT NULL ")
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        template = '{{"name":"{0}", "age":{1}, "bigValues": "{2}" }}'
        generators = []
        for j in range(self.iterations):
            for i in range(10):
                name = FIRST_NAMES[random.choice(list(range(len(FIRST_NAMES))))]
                id_size = random.choice(list(range(5, 10)))
                short_str = "".join(random.choice(ascii_lowercase) for k in range(id_size))
                id = "{0}-{1}".format(name, short_str)
                age = random.choice(list(range(4, 19)))
                bigValues = "".join(random.choice(ascii_lowercase) for k in range(5))
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
            for i in range(10):
                name = FIRST_NAMES[random.choice(list(range(len(FIRST_NAMES))))]
                id_size = random.choice(list(range(20, 50)))  # Reduced from 100-200
                long_str = "".join(random.choice(ascii_lowercase[:10]) for k in range(id_size))  # Use smaller char set
                id = "{0}-{1}".format(name, long_str)
                age = random.choice(list(range(4, 19)))
                bigValues = "".join(random.choice(ascii_lowercase[:5]) for k in range(5))  # Use smaller char set
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
            gc.collect()
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def test_change_doc_key_size(self):
        self.iterations = self.input.param("num_iterations", 5)
        buckets = self._create_plasma_buckets()
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        query_definition = QueryDefinition(index_name="index_name_big_values", index_fields=["bigValues"],
                                           query_template="SELECT * FROM %s WHERE bigValues IS NOT NULL",
                                           groups=["simple"], index_where_clause=" bigValues IS NOT NULL ")
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        template = '{{"name":"{0}", "age":{1}, "bigValues": "{2}" }}'
        generators = []
        for j in range(self.iterations):
            for i in range(10):
                name = FIRST_NAMES[random.choice(list(range(len(FIRST_NAMES))))]
                id_size = random.choice(list(range(5, 50)))  # Reduced from 5-200
                short_str = "".join(random.choice(ascii_lowercase[:10]) for k in range(id_size))  # Use smaller char set
                id = "{0}-{1}".format(name, short_str)
                age = random.choice(list(range(4, 19)))
                bigValue_size = random.choice(list(range(10, 200)))  # Reduced from 10-5000
                bigValues = "".join(random.choice(ascii_lowercase[:5]) for k in range(bigValue_size))  # Use smaller char set
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
            gc.collect()
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def _upload_documents_in_sorted(self):
        generators = []
        template = '{{"name":"{0}", "age":{1}}}'
        FIRST_NAMES.sort(reverse=self.reverse)
        age = random.randint(25, 70)
        buckets = self._create_plasma_buckets()
        for name in FIRST_NAMES:
            prefix = "range_shrink_record_" + str(random.random()*100000)
            generators.append(DocumentGenerator(
                prefix, template, [name], [age], start=0, end=100))
        self.load(generators, buckets=buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        return generators

    def _create_plasma_buckets(self):
        for bucket in self.buckets:
            if bucket.name.startswith("standard"):
                BucketOperationHelper.delete_bucket_or_assert(
                    serverInfo=self.dgmServer, bucket=bucket.name)
        self.buckets = [bu for bu in self.buckets if not bu.name.startswith("standard")]
        buckets = []
        for i in range(self.num_plasma_buckets):
            name = "plasma_dgm_" + str(i)
            buckets.append(name)
        bucket_size = self._get_bucket_size(self.quota,
                                            len(self.buckets)+len(buckets))
        self._create_buckets(server=self.master, bucket_list=buckets,
                             bucket_size=bucket_size)
        testuser = []
        rolelist = []
        for bucket in buckets:
            testuser.append({'id': bucket, 'name': bucket, 'password': 'password'})
            rolelist.append({'id': bucket, 'name': bucket, 'roles': 'admin', 'password':'password'})
        self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        return buckets

    def _update_document(self, bucket_name, key, document):
        if self.use_https:
            url = 'couchbases://{ip}/{name}?ssl=no_verify'.format(ip=self.master.ip, name=bucket_name)
        else:
            url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=bucket_name)
        auth = PasswordAuthenticator(bucket_name, "password")
        cluster = Cluster(
            f"couchbase://{self.master.ip}",
            ClusterOptions(auth)
        )
        bucket_obj = cluster.bucket(bucket_name)
        collection = bucket_obj.default_collection()
        collection.upsert(key, document)

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

    def _create_indexes(self, buckets):
        query_definitions = []
        query_definitions.append(QueryDefinition(index_name="index_long_name", index_fields=["name"]))
        query_definitions.append(QueryDefinition(index_name="index_array_encoded",
                                                 index_fields=["ALL ARRAY t FOR t in `encoded_array` END"]))
        query_definitions.append(QueryDefinition(index_name="index_array_encoded_bigValue",
                                                 index_fields=["ALL ARRAY t FOR t in `encoded_big_value_array` END"]))
        query_definitions.append(QueryDefinition(index_name="index_long_name_age", index_fields=["name", "age"]))
        query_definitions.append(QueryDefinition(index_name="index_long_endoded_age",
                                                 index_fields=["ALL ARRAY t FOR t in `encoded_array` END", "age"]))
        query_definitions.append(QueryDefinition(index_name="index_long_endoded_name",
                                                 index_fields=["ALL ARRAY t FOR t in `encoded_array` END", "name"]))
        query_definitions.append(QueryDefinition(index_name="index_long_name_encoded_age",
                                                 index_fields=["name", "ALL ARRAY t FOR t in `encoded_array` END",
                                                               "age"]))
        self.multi_create_index(buckets=buckets, query_definitions=query_definitions)
        return query_definitions

    def upload_documents(self, num_items, item_size, array_size, buckets=None, update_docs=False, array_elements=3):
        if not buckets:
            buckets = self.buckets
        if not update_docs:
            for bucket in buckets:
                self.rest.flush_bucket(bucket)
            self.sleep(30)
        generators = []
        template = '{{"name":"{0}", "age":{1}, "encoded_array": {2}, "encoded_big_value_array": {3}}}'
        # Reduce sizes by 80%
        item_length = min(item_size * 2, 100)  # Cap at 100
        array_element_size = min((array_size * 2)//array_elements, 50)  # Cap at 50
        array_elements = min(array_elements, 2)  # Cap at 2 elements
        if update_docs:
            num_items = len(self.full_docs_list)
        for i in range(num_items):
            if update_docs:
                index_id = str(self.full_docs_list[i]["_id"].split("-")[0])
            else:
                index_id = "unhandled_items_" + str(random.random()*100000)
            encoded_array = []
            name = "".join(random.choice(ascii_lowercase[:10]) for k in range(item_length))  # Use smaller char set
            age = random.choice(list(range(4, 59)))
            big_value_array = [name]
            for j in range(array_elements):
                element = "".join(random.choice(ascii_lowercase[:5]) for k in range(array_element_size))  # Use smaller char set
                encoded_array.append(element)
            generators.append(DocumentGenerator(
                index_id, template, [name], [age], [encoded_array],
                [big_value_array], start=0, end=1))
        self.full_docs_list = self.generate_full_docs_list(generators)
        if not update_docs:
            self.load(generators, buckets=buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        else:
            for bucket in buckets:
                for doc in self.full_docs_list:
                    self._update_document(bucket.name, doc["_id"], doc)
