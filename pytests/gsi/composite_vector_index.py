"""composite_vector_index.py: "This class test composite Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "19/06/24 03:27 pm"

"""
import faiss
import numpy as np
import pandas as pd

from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestConnection
from serverless.gsi_utils import GSIUtils


class CompositeVectorIndex(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CompositeVectorIndex, self).setUp()
        self.log.info("==============  CompositeVectorIndex setup has started ==============")
        self.namespaces = []
        self.dimension = self.input.param("dimension", None)
        self.trainlist = self.input.param("trainlist", None)
        self.description = self.input.param("description", None)
        self.similarity = self.input.param("similarity", None)
        self.nprobes = self.input.param("nprobes", None)
        self.log.info("==============  CompositeVectorIndex setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  CompositeVectorIndex tearDown has started ==============")
        super(CompositeVectorIndex, self).tearDown()
        self.log.info("==============  CompositeVectorIndex tearDown has completed ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def test_create_indexes(self):
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
                                             load_default_coll=True, model=self.data_model)

        query_node = self.get_nodes_from_services_map(service_type="n1ql")

        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.generate_mini_car_vector_index_definition(index_name_prefix='test')
            queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                              namespace=namespace, defer_build_mix=True, dimension=self.dimension, trainlist=self.trainlist, description=self.description, similarity=self.similarity, nprobes=self.nprobes)
            self.gsi_util_obj.create_gsi_indexes(create_queries=queries, query_node=query_node)

        sentences = ["This is a Chevrolet car with eCVT (Electronic Continuously Variable Transmission) transmission "
                     "and manufactured in 1902 year. This car is available in baby poop green color. This car belongs "
                     "to Convertible category and has a rating of 3 stars",
                     "This is a Tesla car with Manual transmission and manufactured in 1920 year. This car is "
                     "available in dark khaki color. This car belongs to Sedan category and has a rating of 1 stars",
                     "This is a Ford car with eCVT (Electronic Continuously Variable Transmission) transmission and "
                     "manufactured in 1963 year. This car is available in light seafoam color. This car belongs to "
                     "Sports Car category and has a rating of 2 stars",
                     "This is a Lexus car with eCVT (Electronic Continuously Variable Transmission) transmission and "
                     "manufactured in 1981 year. This car is available in pale sky blue color. This car belongs to "
                     "Van category and has a rating of 0 stars",
                     "This is a Hyundai car with Semi-Automatic transmission and manufactured in 1976 year. This car "
                     "is available in electric blue color. This car belongs to Wagon category and has a rating of 2 "
                     "stars",
                     "This is a Chevrolet car with Semi-Automatic transmission and manufactured in 1995 year. This "
                     "car is available in pale pink color. This car belongs to Convertible category and has a rating "
                     "of 0 stars",
                     "This is a Audi car with Manual transmission and manufactured in 1939 year. This car is "
                     "available in red purple color. This car belongs to Pickup category and has a rating of 1 stars",
                     "This is a Land Rover car with Semi-Automatic transmission and manufactured in 1958 year. This "
                     "car is available in rose pink color. This car belongs to Sports Car category and has a rating "
                     "of 4 stars",
                     "This is a Audi car with IVT (Intelligent Variable Transmission) transmission and manufactured "
                     "in 1997 year. This car is available in chocolate brown color. This car belongs to Luxury Car "
                     "category and has a rating of 4 stars",
                     "This is a Nissan car with CVT (Continuously Variable Transmission) transmission and "
                     "manufactured in 1994 year. This car is available in camouflage green color. This car belongs to "
                     "Minivan category and has a rating of 4 stars"
                     ]
        df = pd.DataFrame(sentences, columns=['text'])
        vectors = self.encoder.encode(df['text'], precision='float32')
        faiss_db = self.load_data_into_faiss(vectors=vectors)

        search_text = "A convertible car with red color made in 1990"
        result = self.search_ann_in_faiss(query=search_text, faiss_db=faiss_db)
        merge_result = pd.merge(result, df, left_on='ann', right_index=True)
        print(merge_result)
