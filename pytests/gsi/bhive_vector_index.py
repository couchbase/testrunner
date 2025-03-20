"""bhive_vector_index.py: "This class test BHIVE Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "13/03/24 02:03 pm"

"""
import datetime
import json

import requests
from concurrent.futures import ThreadPoolExecutor

from docutils.nodes import definition_list
from requests.auth import HTTPBasicAuth
from sentence_transformers import SentenceTransformer

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition, FULL_SCAN_ORDER_BY_TEMPLATE, \
    RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE, RANGE_SCAN_TEMPLATE, RANGE_SCAN_ORDER_BY_TEMPLATE
from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestHelper
from scripts.multilevel_dict import MultilevelDict
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView
from memcached.helper.data_helper import MemcachedClientHelper
from threading import Thread
import random


class BhiveVectorIndex(BaseSecondaryIndexingTests):
    def setUp(self):
        super(BhiveVectorIndex, self).setUp()
        self.log.info("==============  BhiveVectorIndex setup has started ==============")
        self.use_magma_loader = True
        self.log.info("==============  BhiveVectorIndex setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  BhiveVectorIndex tearDown has started ==============")
        super(BhiveVectorIndex, self).tearDown()
        self.log.info("==============  BhiveVectorIndex tearDown has ended ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def test_bhive_workload(self):
        try:
            self.bucket_params = self._create_bucket_params(server=self.master, size=256, bucket_storage="magma",
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name="test_bucket", port=11222,
                                                bucket_params=self.bucket_params)
        except Exception as err:
            pass
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=True)
        # self.sleep(10)

        definitions = []
        for similarity in ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]:
            for namespace in self.namespaces:
                definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                          prefix=f'test_{similarity}',
                                                                          similarity=similarity, train_list=None,
                                                                          scan_nprobes=self.scan_nprobes,
                                                                          array_indexes=False, persist_full_vector=False,
                                                                          xattr_indexes=self.xattr_indexes,
                                                                          limit=self.scan_limit, is_base64=self.base64,
                                                                          quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                          quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                          bhive_index=self.bhive_index)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                         namespace=namespace,
                                                                         bhive_index=self.bhive_index)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                      namespace=namespace, limit=self.scan_limit)
                drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions,
                                                                     namespace=namespace)
                self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace)
                self.wait_until_indexes_online()

          # Run validations on recall, metadata

        # Start the workload

        # Run validations on recall, metadata
        # Run Validation on Item count
