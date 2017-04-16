import logging
import random

from string import lowercase
from couchbase.bucket import Bucket
from couchbase_helper.documentgenerator import  DocumentGenerator
from couchbase_helper.data import FIRST_NAMES, COUNTRIES
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.tuq_generators import TuqGenerators
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from base_2i import BaseSecondaryIndexingTests

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
            self.rest.set_indexer_memoryQuota(indexMemoryQuota=self.indexMemQuota)
            self.sleep(30)
        self.deploy_node_info = ["{0}:{1}".format(self.dgmServer.ip, self.dgmServer.port)]
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)

    def tearDown(self):
        super(SecondaryIndexDatasizeTests, self).tearDown()

    def test_sorted_items_indexed(self):
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        generators = self._upload_documents_in_sorted()
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        query_definition =  QueryDefinition(
            index_name="index_range_shrink_name",
            index_fields=["name"],
            query_template="SELECT * FROM %s WHERE name IS NOT NULL",
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
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
        query_definition =  QueryDefinition(
            index_name="index_range_shrink_name",
            index_fields=["name"],
            query_template="SELECT * FROM %s WHERE name IS NOT NULL",
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        self.sleep(30)
        intervals = [["d", "e", "f"], ["j", "k", "l", "m"], ["p", "q", "r", "s"] ]
        for doc_gen in generators:
            for interval in intervals:
                for character in interval:
                    if doc_gen.name.lower().startswith(character):
                        for bucket in buckets:
                            url = "couchbase://{0}/{1}".format(self.master.ip,
                                                               bucket.name)
                            cb = Bucket(url)
                            cb.remove(doc_gen._id)
                            generators.remove(doc_gen)
                if not self.multi_intervals:
                    break
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
        query_definition =  QueryDefinition(
            index_name="index_range_shrink_name",
            index_fields=["name"],
            query_template="SELECT * FROM %s WHERE name IS NOT NULL",
            groups=["simple"], index_where_clause=" name IS NOT NULL ")
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        self.sleep(30)
        intervals = [["p", "q", "r", "s"], ["j", "k", "l", "m"], ["d", "e", "f"]]
        for doc_gen in generators:
            for interval in intervals:
                for character in interval:
                    if doc_gen.name.lower().startswith(character):
                        for bucket in buckets:
                            url = "couchbase://{0}/{1}".format(self.master.ip,
                                                               bucket.name)
                            cb = Bucket(url)
                            cb.remove(doc_gen._id)
                            generators.remove(doc_gen)
                if not self.multi_intervals:
                    break
        self.multi_query_using_index(buckets=buckets,
                                     query_definitions=[query_definition])
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def test_change_doc_size(self):
        self.iterations = self.input.param("num_iterations", 5)
        buckets = self._create_plasma_buckets()
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        query_definition =  QueryDefinition(
            index_name="index_name_big_values",
            index_fields=["bigValues"],
            query_template="SELECT * FROM %s WHERE bigValues IS NOT NULL",
            groups=["simple"], index_where_clause=" bigValues IS NOT NULL ")
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        template = '{{"name":"{0}", "age":{1}, "bigValues": "{2}" }}'
        generators = []
        for j in range(self.iterations):
            for i in range(10):
                name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
                id = "{0}-{1}".format(name, str(i))
                age = random.choice(range(4, 19))
                bigValue_size = random.choice(range(10, 15))
                bigValues = "".join(random.choice(lowercase) for k in range(bigValue_size))
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
            for i in range(10):
                name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
                id = "{0}-{1}".format(name, str(i))
                age = random.choice(range(4, 19))
                bigValue_size = random.choice(range(1000, 5000))
                bigValues = "".join(random.choice(lowercase) for k in range(bigValue_size))
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def test_change_key_size(self):
        self.iterations = self.input.param("num_iterations", 5)
        buckets = self._create_plasma_buckets()
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        query_definition =  QueryDefinition(
            index_name="index_name_big_values",
            index_fields=["bigValues"],
            query_template="SELECT * FROM %s WHERE bigValues IS NOT NULL",
            groups=["simple"], index_where_clause=" bigValues IS NOT NULL ")
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        template = '{{"name":"{0}", "age":{1}, "bigValues": "{2}" }}'
        generators = []
        for j in range(self.iterations):
            for i in range(10):
                name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
                id_size = random.choice(range(5, 10))
                short_str = "".join(random.choice(lowercase) for k in range(id_size))
                id = "{0}-{1}".format(name, short_str)
                age = random.choice(range(4, 19))
                bigValues = "".join(random.choice(lowercase) for k in range(5))
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
            for i in range(10):
                name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
                id_size = random.choice(range(1000, 5000))
                long_str = "".join(random.choice(lowercase) for k in range(id_size))
                id = "{0}-{1}".format(name, long_str)
                age = random.choice(range(4, 19))
                bigValues = "".join(random.choice(lowercase) for k in range(5))
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
        self.sleep(30)
        self.multi_drop_index(buckets=buckets,
                              query_definitions=[query_definition])

    def test_change_doc_key_size(self):
        self.iterations = self.input.param("num_iterations", 5)
        buckets = self._create_plasma_buckets()
        if self.plasma_dgm:
            self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        query_definition =  QueryDefinition(
            index_name="index_name_big_values",
            index_fields=["bigValues"],
            query_template="SELECT * FROM %s WHERE bigValues IS NOT NULL",
            groups=["simple"], index_where_clause=" bigValues IS NOT NULL ")
        self.multi_create_index(buckets=buckets,
                                query_definitions=[query_definition])
        template = '{{"name":"{0}", "age":{1}, "bigValues": "{2}" }}'
        generators = []
        for j in range(self.iterations):
            for i in range(10):
                name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
                id_size = random.choice(range(5, 5000))
                short_str = "".join(random.choice(lowercase) for k in range(id_size))
                id = "{0}-{1}".format(name, short_str)
                age = random.choice(range(4, 19))
                bigValue_size = random.choice(range(10, 5000))
                bigValues = "".join(random.choice(lowercase) for k in range(bigValue_size))
                generators.append(DocumentGenerator(
                    id, template, [name], [age], [bigValues], start=0, end=10))
            self.load(generators, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.full_docs_list = self.generate_full_docs_list(generators)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.multi_query_using_index(buckets=buckets,
                                         query_definitions=[query_definition])
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
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        return buckets
