import copy
import logging
import random

from couchbase.bucket import Bucket
from couchbase_helper.documentgenerator import  DocumentGenerator
from couchbase_helper.data import FIRST_NAMES, COUNTRIES
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.tuq_generators import TuqGenerators
from membase.api.rest_client import RestConnection
from base_2i import BaseSecondaryIndexingTests

log = logging.getLogger(__name__)

class SecondaryIndexDatasizeTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexDatasizeTests, self).setUp()
        self.num_plasma_buckets = self.input.param("num_plasma_buckets", 1)
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
        query_definition =  QueryDefinition(
            index_name="index_range_shrink_name",
            index_fields=["name"],
            query_template="SELECT {0} FROM %s WHERE name IS NOT NULL",
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
        query_definition =  QueryDefinition(
            index_name="index_range_shrink_name",
            index_fields=["name"],
            query_template="SELECT {0} FROM %s WHERE name IS NOT NULL",
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
        buckets = []
        for i in range(self.num_plasma_buckets):
            name = "plasma_dgm_" + str(i)
            buckets.append(name)
        self._create_buckets(self.master, buckets)
        buckets = []
        for bucket in self.buckets:
            if bucket.name.startswith("plasma_dgm"):
                buckets.append(bucket)
        for name in FIRST_NAMES:
            prefix = "range_shrink_record_" + str(random.random()*100000)
            generators.append(DocumentGenerator(
                prefix, template, [name], [age], start=0, end=100))
        self.load(generators, buckets=buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        return generators
