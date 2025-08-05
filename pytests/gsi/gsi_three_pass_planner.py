"""gsi_three_pass_planner.py: "This class test composite Vector indexes  for GSI"

__author__ = "Yash Dodderi"
__maintainer = "Yash Dodderi"
__email__ = "yash.dodderi@couchbase.com"
__git_user__ = "yash-dodderi7"
__created_on__ = "06/11/24 03:27 pm"

"""
'''
Test case link - https://docs.google.com/spreadsheets/d/1WRbJwqOUJ1NPr4Ufjqsz5npV1rNnmk0UXRwoRrjc_CY/edit?gid=164129179#gid=164129179
'''
import random

from sentence_transformers import SentenceTransformer

from couchbase_helper.query_definitions import QueryDefinition
from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestHelper
from remote.remote_util import RestConnection


class ThreePassPlanner(BaseSecondaryIndexingTests):
    def setUp(self):
        super(ThreePassPlanner, self).setUp()
        self.log.info("==============  ThreePassPlanner setup has started ==============")
        self.encoder = SentenceTransformer(self.data_model, device="cpu")
        self.encoder.cpu()
        self.gsi_util_obj.set_encoder(encoder=self.encoder)
        self.same_category_index = self.input.param("same_category_index", True)
        self.build_phase = self.input.param("build_phase", "create")
        self.skip_default = self.input.param("skip_default", True)
        self.index_type = self.input.param("index_type", "scalar")
        self.index_drop = self.input.param("index_drop", False)
        self.toggle_on_off_shard_dealer = self.input.param("toggle_on_off_shard_dealer", False)
        self.post_rebalance_action = self.input.param("post_rebalance_action", "data_load")
        self.enable_shard_based_rebalance()
        self.enable_shard_seggregation()
        # the below setting will be reversed post the resolving of MB-63697
        self.index_rest.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": False})

        self.log.info("==============  ThreePassPlanner setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  ThreePassPlanner tearDown has started ==============")
        super(ThreePassPlanner, self).tearDown()
        self.log.info("==============  ThreePassPlanner tearDown has completed ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def validate_no_of_shards(self, validation_length=6):
        shard_list = self.fetch_shard_id_list()
        self.assertEqual(len(shard_list), validation_length, f"shard list {shard_list}")

    def fetch_shard_partition_map(self):

        shards_partition_map = {}
        index_node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(index_node)
        metadata = rest.get_indexer_metadata()['status']

        # Loop through each index in the status list
        for index in metadata:
            # Extract alternate shard ids for each index
            alternate_shards = index.get("alternateShardIds", {})

            for ip, partitions in alternate_shards.items():
                # Iterate over the partitions (shard keys)
                for shard_key, ids in partitions.items():
                    # Iterate over each ID in the partition
                    for id_str in ids:
                        # If the ID already exists in the reverse map, append the shard key
                        if id_str in shards_partition_map:
                            shards_partition_map[id_str].append(f"{index['instId']}-{shard_key}")
                        else:
                            # Otherwise, create a new entry with the current shard key
                            shards_partition_map[id_str] = [f"{index['instId']}-{shard_key}"]

        return shards_partition_map

    def create_index_post_initial_index_creation(self, collection_namespace):
        if self.index_type == "scalar":
            if self.same_category_index:
                idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'],
                                               partition_by_fields=['meta().id'])
                query = idx.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            else:
                idx = QueryDefinition(index_name='vector_rgb_2', index_fields=['colorRGBVector VECTOR'],
                                             dimension=3,
                                             description="IVF,PQ3x8", similarity="L2_SQUARED",
                                             partition_by_fields=['meta().id'])
                query = idx.generate_index_create_query(namespace=collection_namespace,
                                                               defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)

        elif self.index_type == "vector":
            if self.same_category_index:
                idx = QueryDefinition(index_name='vector_rgb_2', index_fields=['colorRGBVector VECTOR'],
                                      dimension=3,
                                      description="IVF,PQ3x8", similarity="L2_SQUARED",
                                      partition_by_fields=['meta().id'])
                query = idx.generate_index_create_query(namespace=collection_namespace,
                                                        defer_build=self.defer_build)
            else:
                idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'],
                                      partition_by_fields=['meta().id'])
                query = idx.generate_index_create_query(namespace=collection_namespace,
                                                        defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)

        elif self.index_type == "bhive":
            if self.same_category_index:
                idx = QueryDefinition(index_name= 'bhive_description_2',
                                index_fields=['descriptionVector VECTOR'],
                                dimension=384, description=f"IVF,PQ32x8",
                                similarity="L2_SQUARED", partition_by_fields=['meta().id'])
                query = idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True, defer_build=self.defer_build)
            else:
                idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'],
                                      partition_by_fields=['meta().id'])
                query = idx.generate_index_create_query(namespace=collection_namespace,
                                                        defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)

        elif self.index_type == "all":
            scalar_idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'],
                                  partition_by_fields=['meta().id'])
            scalar_query = scalar_idx.generate_index_create_query(namespace=collection_namespace,
                                                    defer_build=self.defer_build)
            vector_idx = QueryDefinition(index_name='vector_rgb_2', index_fields=['colorRGBVector VECTOR'],
                                      dimension=3,
                                      description="IVF,PQ3x8", similarity="L2_SQUARED",
                                      partition_by_fields=['meta().id'])
            vector_query = vector_idx.generate_index_create_query(namespace=collection_namespace,
                                                        defer_build=self.defer_build)
            bhive_idx = QueryDefinition(index_name='bhive_description_2',
                                  index_fields=['descriptionVector VECTOR'],
                                  dimension=384, description=f"IVF,PQ32x8",
                                  similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            bhive_query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True,
                                                    defer_build=self.defer_build)
            for query in [scalar_query, vector_query, bhive_query]:
                self.run_cbq_query(query=query, server=self.n1ql_node)

    def test_create_drop_indexes_under_min_shards(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]
        if self.index_type == "scalar":
            scalar_idx = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()

        elif self.index_type == "vector":
            vector_idx = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED")
            query = vector_idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()

        elif self.index_type == "mixed":
            #creating scalar index
            scalar_idx = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            #creating vector index
            vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64)
            query = vector_idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()

        elif self.index_type == "all":
            # creating scalar index
            scalar_idx = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating vector index
            vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64)
            query = vector_idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating bhive index
            bhive_idx = QueryDefinition(index_name='bhive_description',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED")
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()


        #validating if num_shards are less than the minimum(6)
        shard_list = self.fetch_shard_id_list()
        self.assertLessEqual(len(shard_list), 6, f"shard list {shard_list}")


        if self.index_type == "scalar":
            vector_idx = QueryDefinition(index_name='vector_rgb_2', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED")
            query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = vector_idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
            self.wait_until_indexes_online(defer_build=self.defer_build)

        elif self.index_type == "vector":
            scalar_idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = scalar_idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
            self.wait_until_indexes_online(defer_build=self.defer_build)

        elif self.index_type == "bhive":
            bhive_idx = QueryDefinition(index_name= 'bhive_description',
                            index_fields=['descriptionVector VECTOR'],
                            dimension=384, description=f"IVF,PQ32x8",
                            similarity="L2_SQUARED")
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True, defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = bhive_idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
            self.wait_until_indexes_online(defer_build=self.defer_build)


        #validating that shard seggregation has kicked in
        shard_index_map = self.get_shards_index_map()
        self.validate_shard_seggregation(shard_index_map=shard_index_map)

    def test_create_drop_indexes_shard_limit(self):
        '''
        A brief description of the below test
        This "single" node test ensures that indexes are created from different categories upto the shard limit(18 for node with 512MiB memory for indexing)
        Post creating indexes depending on if the initial index creation had a specific category(scalar, vector or bhive) or all categories, further creation of different categories of indexes should create new shards
        Indexes of same category should ensure that the existing shards should be reused
        '''
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        # self.index_rest.set_index_settings({"indexer.plasma.sharedFlushBufferMultipler": 1})
        collection_namespace = self.namespaces[0]
        if self.index_type == "scalar":
            scalar_idx_1 = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace)
            scalar_idx_2 = QueryDefinition(index_name='scalar_fuel', index_fields=['fuel'], partition_by_fields=['meta().id'])
            scalar_query_2 = scalar_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            scalar_idx_3 = QueryDefinition(index_name='scalar_manufacturer', index_fields=['manufacturer'], partition_by_fields=['meta().id'])
            scalar_query_3 = scalar_idx_3.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            scalar_idx_4 = QueryDefinition(index_name='scalar_manufacturer_1', index_fields=['manufacturer'],
                                           partition_by_fields=['meta().id'])
            scalar_query_4 = scalar_idx_4.generate_index_create_query(namespace=collection_namespace, num_partition=36)
            for query in [scalar_query_1, scalar_query_2, scalar_query_3, scalar_query_4]:
                self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()

        elif self.index_type == "vector":
            vector_idx_1 = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED")
            vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace)
            vector_idx_2 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'], dimension=384,
                                         description="IVF,PQ64x8", similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            vector_query_2 = vector_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            vector_idx_3 = QueryDefinition(index_name='vector_description_1', index_fields=['descriptionVector VECTOR', 'category'], dimension=384,
                                         description="IVF,PQ64x8", similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            vector_query_3 = vector_idx_3.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            vector_idx_4 = QueryDefinition(index_name='vector_description_2',
                                           index_fields=['descriptionVector VECTOR', 'category'], dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_4 = vector_idx_4.generate_index_create_query(namespace=collection_namespace, num_partition=36)
            for query in [vector_query_1, vector_query_2, vector_query_3, vector_query_4]:
                self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()

        elif self.index_type == "mixed":
            vector_idx_1 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                           dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            vector_idx_2 = QueryDefinition(index_name='vector_description_1',
                                           index_fields=['descriptionVector VECTOR', 'category'], dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_2 = vector_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            vector_idx_3 = QueryDefinition(index_name='vector_description_2',
                                           index_fields=['descriptionVector VECTOR', 'category'], dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_3 = vector_idx_3.generate_index_create_query(namespace=collection_namespace, num_partition=36)
            scalar_idx_1 = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace)
            for query in [vector_query_1, vector_query_2, scalar_query_1, vector_query_3]:
                self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()


        elif self.index_type == "all":
            # creating scalar index
            scalar_idx = QueryDefinition(index_name='scalar_rgb', index_fields=['color'], partition_by_fields=['meta().id'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating vector index
            vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64, partition_by_fields=['meta().id'])
            query = vector_idx.generate_index_create_query(namespace=collection_namespace, num_partition=36)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating bhive index
            bhive_idx = QueryDefinition(index_name='bhive_description',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True, num_partition=4)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()

        # validating if num_shards are at shard capacity only when all categories of indexes are not created since when all categories of indexes are created there is an overflow of no of shards
        if not self.index_type == "all":
            max_shards = self.fetch_total_shards_limit()
            self.validate_no_of_shards(validation_length=max_shards)

        if self.index_type == "scalar":
            vector_idx = QueryDefinition(index_name='vector_rgb_2', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED")
            query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = vector_idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
            self.wait_until_indexes_online(defer_build=self.defer_build)

        elif self.index_type == "vector":
            scalar_idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = scalar_idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
            self.wait_until_indexes_online(defer_build=self.defer_build)

        elif self.index_type == "bhive":
            bhive_idx = QueryDefinition(index_name= 'bhive_description',
                            index_fields=['descriptionVector VECTOR'],
                            dimension=384, description=f"IVF,PQ32x8",
                            similarity="L2_SQUARED")
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True, defer_build=self.defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            if self.index_drop:
                self.sleep(10)
                drop_query = bhive_idx.generate_index_drop_query(namespace=collection_namespace)
                self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                self.sleep(10)
                self.run_cbq_query(query=query, server=self.n1ql_node)
                self.sleep(10)
            self.wait_until_indexes_online(defer_build=self.defer_build)

        elif self.index_type == "all":
            # creating scalar index
            scalar_idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'], partition_by_fields=['meta().id'])
            query = scalar_idx.generate_index_create_query(namespace=collection_namespace, num_partition=6)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating vector index
            vector_idx = QueryDefinition(index_name='vector_rgb_2', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64, partition_by_fields=['meta().id'])
            query = vector_idx.generate_index_create_query(namespace=collection_namespace, num_partition=36)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating bhive index
            bhive_idx = QueryDefinition(index_name='bhive_description_2',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True, num_partition=6)
            self.run_cbq_query(query=query, server=self.n1ql_node)

            self.wait_until_indexes_online()

        # if different categories of indexes are created after shard capacity has been reached by only one category of indexes then new shards will be created
        else:
            shard_index_map = self.get_shards_index_map()
            self.validate_shard_seggregation(shard_index_map=shard_index_map)

    def test_multinode_shard_seggregation(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]
        num_replica = random.randint(1, self.num_index_replica)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # Soft limit basically indicated the max no of partitions per shard
        if self.index_load_three_pass == 'soft_limit':
            index_node_1, index_node_2, index_node_3 = index_nodes
            scalar_idx_1 = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace, deploy_node_info=[f"{index_node_2.ip}:{index_node_2.port}", f"{index_node_3.ip}:{index_node_3.port}"])
            scalar_idx_2 = QueryDefinition(index_name='scalar_fuel', index_fields=['fuel'])
            scalar_query_2 = scalar_idx_2.generate_index_create_query(namespace=collection_namespace, deploy_node_info=[f"{index_node_1.ip}:{index_node_1.port}", f"{index_node_3.ip}:{index_node_3.port}"])
            scalar_idx_3 = QueryDefinition(index_name='scalar_manufacturer', index_fields=['manufacturer'])
            scalar_query_3 = scalar_idx_3.generate_index_create_query(namespace=collection_namespace, deploy_node_info=[f"{index_node_1.ip}:{index_node_1.port}", f"{index_node_2.ip}:{index_node_2.port}"])

            #vector index
            vector_idx_1 = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                           description="IVF,PQ3x8", similarity="L2_SQUARED")
            vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace, deploy_node_info=[f"{index_node_2.ip}:{index_node_2.port}", f"{index_node_3.ip}:{index_node_3.port}"])
            vector_idx_2 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                           dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED")
            vector_query_2 = vector_idx_2.generate_index_create_query(namespace=collection_namespace, deploy_node_info=[f"{index_node_1.ip}:{index_node_1.port}", f"{index_node_3.ip}:{index_node_3.port}"])
            vector_idx_3 = QueryDefinition(index_name='vector_description_1',
                                           index_fields=['descriptionVector VECTOR', 'category'], dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED")
            vector_query_3 = vector_idx_3.generate_index_create_query(namespace=collection_namespace, deploy_node_info=[f"{index_node_1.ip}:{index_node_1.port}", f"{index_node_2.ip}:{index_node_2.port}"])
            for query in [scalar_query_1, scalar_query_2, scalar_query_3, vector_query_1, vector_query_2, vector_query_3]:
                self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()
            num_shards_at_soft_limit = self.fetch_shard_partition_map()
            # to verify shards are within soft limit
            shard_partition_map = self.fetch_shard_partition_map()
            for shard in shard_partition_map:
                # the assertion is done for 5 partitions per shard since for index mem quota from 0-1GB the shards are reused until each shard has 5 partitions each
                self.assertLessEqual(len(shard_partition_map[shard]), 5,
                                     f'shard partition map is {shard_partition_map}')
        else:
            #todo to determine the actual index load for reaching shard capacity in a multi node setup below index creations are just a placeholder as of now
            #shard capacity signifies the max no of shards a node can have. It is 18 for all the tests in this file since its calculated for index nodes with 512MiB memory

            # creating scalar index
            scalar_idx_1 = QueryDefinition(index_name='scalar_rgb', index_fields=['color'])
            scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace, num_replica=num_replica)
            scalar_idx_2 = QueryDefinition(index_name='scalar_fuel', index_fields=['fuel'],
                                           partition_by_fields=['meta().id'])
            scalar_query_2 = scalar_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=4, num_replica=num_replica)
            scalar_idx_3 = QueryDefinition(index_name='scalar_manufacturer', index_fields=['manufacturer'],
                                           partition_by_fields=['meta().id'])
            scalar_query_3 = scalar_idx_3.generate_index_create_query(namespace=collection_namespace, num_partition=4, num_replica=num_replica)
            scalar_idx_4 = QueryDefinition(index_name='scalar_manufacturer_1', index_fields=['manufacturer'],
                                           partition_by_fields=['meta().id'])
            scalar_query_4 = scalar_idx_4.generate_index_create_query(namespace=collection_namespace, num_partition=36, num_replica=num_replica)
            for query in [scalar_query_1, scalar_query_2, scalar_query_3, scalar_query_4]:
                self.run_cbq_query(query=query, server=self.n1ql_node)

            # creating vector index
            vector_idx_1 = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                           description="IVF,PQ3x8", similarity="L2_SQUARED")
            vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace)
            vector_idx_2 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                           dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_2 = vector_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            vector_idx_3 = QueryDefinition(index_name='vector_description_1',
                                           index_fields=['descriptionVector VECTOR', 'category'], dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_3 = vector_idx_3.generate_index_create_query(namespace=collection_namespace, num_partition=4)
            vector_idx_4 = QueryDefinition(index_name='vector_description_2',
                                           index_fields=['descriptionVector VECTOR', 'category'], dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_4 = vector_idx_4.generate_index_create_query(namespace=collection_namespace, num_partition=36)
            for query in [vector_query_1, vector_query_2, vector_query_3, vector_query_4]:
                self.run_cbq_query(query=query, server=self.n1ql_node)

            #creating bhive index
            bhive_idx = QueryDefinition(index_name='bhive_description',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED")
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True, num_replica=num_replica)
            self.run_cbq_query(query=query, server=self.n1ql_node)

        if self.index_load_three_pass == "shard_capacity":
            self.wait_until_indexes_online()
            shard_index_map = self.get_shards_index_map()
            self.validate_shard_seggregation(shard_index_map=shard_index_map)

        elif self.index_load_three_pass == "soft_limit":
            scalar_idx = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'])
            scalar_query = scalar_idx.generate_index_create_query(namespace=collection_namespace)
            vector_idx = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                           description="IVF,PQ3x8", similarity="L2_SQUARED")
            vector_query = vector_idx.generate_index_create_query(namespace=collection_namespace)

            for query in [scalar_query, vector_query]:
                self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()
            # within soft limit there were only vector and scalar indexes so if another vector and scalar index is created within the soft limit the shards must be resued
            num_shards_at_post_creating_index_at_soft_limit = self.fetch_shard_partition_map()
            self.assertEqual(len(num_shards_at_soft_limit), len(num_shards_at_post_creating_index_at_soft_limit), f"before {num_shards_at_soft_limit}, after {num_shards_at_post_creating_index_at_soft_limit}")

            # creating bhive index
            # within soft limit there were only vector and scalar indexes so if a bhive index is created within the soft limit new shards will be created irrespective being within soft limit
            bhive_idx = QueryDefinition(index_name='bhive_description_2',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED")
            query = bhive_idx.generate_index_create_query(namespace=collection_namespace, bhive_index=True,
                                                          num_replica=num_replica)
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()
            shard_index_map = self.get_shards_index_map()
            self.validate_shard_seggregation(shard_index_map=shard_index_map)

    def test_create_drop_same_category_indexes_soft_limit_shards(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]
        if self.index_type == "scalar":
            scalar_idx_1 = QueryDefinition(index_name='scalar_rgb', index_fields=['color'], partition_by_fields=['meta().id'])
            scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=3)
            scalar_idx_2 = QueryDefinition(index_name='scalar_fuel', index_fields=['fuel'],
                                           partition_by_fields=['meta().id'])
            scalar_query_2 = scalar_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=10)
            for query in [scalar_query_1, scalar_query_2]:
                self.run_cbq_query(query=query, server=self.n1ql_node)

        elif self.index_type == "vector":
            vector_idx_1 = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                           description="IVF,PQ3x8", similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=3)
            vector_idx_2 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                           dimension=384,
                                           description="IVF,PQ64x8", similarity="L2_SQUARED",
                                           partition_by_fields=['meta().id'])
            vector_query_2 = vector_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=10)
            for query in [vector_query_1, vector_query_2]:
                self.run_cbq_query(query=query, server=self.n1ql_node)

        elif self.index_type == "bhive":
            bhive_idx_1 = QueryDefinition(index_name='bhive_description',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            bhive_query_1 = bhive_idx_1.generate_index_create_query(namespace=collection_namespace, bhive_index=True, num_partition=3)
            bhive_idx_2 = QueryDefinition(index_name='bhive_rgb',
                                        index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,PQ32x8",
                                        similarity="L2_SQUARED", partition_by_fields=['meta().id'])
            bhive_query_2 = bhive_idx_2.generate_index_create_query(namespace=collection_namespace, num_partition=10)
            for query in [bhive_query_1, bhive_query_2]:
                self.run_cbq_query(query=query, server=self.n1ql_node)



        no_of_shards_within_soft_limit = self.fetch_shard_id_list()
        #to verify shards are within soft limit
        shard_partition_map = self.fetch_shard_partition_map()
        for shard in shard_partition_map:
            # the assertion is done for 8 partitions per shard since for index mem quota from 0-1GB the shards are reused until each shard has 8 partitions each
            self.assertLessEqual(len(shard_partition_map[shard]), 8, f'shard partition map is {shard_partition_map}')

        self.create_index_post_initial_index_creation(collection_namespace=collection_namespace)
        self.wait_until_indexes_online(defer_build=self.defer_build)

        #validating that creation of indexes shares existing shards
        if self.same_category_index:
            no_of_shards_post_crossing_soft_limit = self.fetch_shard_id_list()
            self.assertEqual(len(no_of_shards_within_soft_limit), len(no_of_shards_within_soft_limit),
                             f" before {no_of_shards_within_soft_limit}, after {no_of_shards_post_crossing_soft_limit}")
        else:
            shard_index_map = self.get_shards_index_map()
            self.validate_shard_seggregation(shard_index_map=shard_index_map)



    def test_rebalance(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if self.toggle_on_off_shard_dealer:
            self.disable_shard_seggregation()
            self.disable_shard_based_rebalance()
            self.sleep(10)
        scalar_idx_1 = QueryDefinition(index_name='scalar_rgb', index_fields=['color'],
                                       partition_by_fields=['meta().id'])
        scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=3, num_replica=self.num_index_replica)
        vector_idx_1 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                       dimension=384,
                                       description="IVF,PQ64x8", similarity="L2_SQUARED",
                                       partition_by_fields=['meta().id'])
        vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=7, num_replica=self.num_index_replica)
        bhive_idx_1 = QueryDefinition(index_name='bhive_description',
                                      index_fields=['descriptionVector VECTOR'],
                                      dimension=384, description=f"IVF,PQ32x8",
                                      similarity="L2_SQUARED", partition_by_fields=['meta().id'])
        bhive_query_1 = bhive_idx_1.generate_index_create_query(namespace=collection_namespace, bhive_index=True,
                                                                num_partition=3, num_replica=self.num_index_replica)
        for query in [bhive_query_1, vector_query_1, scalar_query_1]:
            self.run_cbq_query(query=query, server=self.n1ql_node)
        self.wait_until_indexes_online()

        if self.toggle_on_off_shard_dealer:
            self.enable_shard_based_rebalance()
            self.enable_shard_seggregation()
            self.sleep(10)

        shard_list_before_rebalance = self.fetch_shard_id_list()
        if not self.toggle_on_off_shard_dealer or len(index_nodes) == 1:
            # to verify shards are within soft limit
            shard_partition_map = self.fetch_shard_partition_map()
            for shard in shard_partition_map:
                # the assertion is done for 8 partitions per shard since for index mem quota from 0-1GB the shards are reused until each shard has 8 partitions each
                self.assertLessEqual(len(shard_partition_map[shard]), 8, f'shard partition map is {shard_partition_map}')

        if self.rebalance_type == "rebalance_in":
            add_nodes = [self.servers[self.nodes_init]]
            task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                to_remove=[], services=['index'])
        elif self.rebalance_type == "swap_rebalance":
            add_nodes = [self.servers[self.nodes_init]]
            task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                to_remove=[index_nodes[0]], services=['index'])
        elif self.rebalance_type == "rebalance_out":
            task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                to_remove=[index_nodes[0]], services=['index'])

        task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
        self.update_master_node()
        self.sleep(10)


        #validating shard segregation post rebalance
        shard_index_map = self.get_shards_index_map()
        self.validate_shard_seggregation(shard_index_map=shard_index_map)

        #ensuring the number shards are the same before and after rebalance
        shard_list_post_rebalance = self.fetch_shard_id_list()
        self.assertEqual(len(shard_list_post_rebalance), len(shard_list_before_rebalance), f"shard list before rebalance {shard_list_before_rebalance}, shard list after rebalance {shard_list_post_rebalance}")

        if self.rebalance_type == "rebalance_out":
            self.create_index_post_initial_index_creation(collection_namespace=collection_namespace)
            self.wait_until_indexes_online(defer_build=self.defer_build)

            # validating shard segregation post creating new indexes post rebalance
            shard_index_map = self.get_shards_index_map()
            self.validate_shard_seggregation(shard_index_map=shard_index_map)

    def test_toggle_on_off_shard_seggregation(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]
        scalar_idx_1 = QueryDefinition(index_name='scalar_rgb', index_fields=['color'],
                                       partition_by_fields=['meta().id'])
        scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=3)
        vector_idx_1 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                       dimension=384,
                                       description="IVF,PQ64x8", similarity="L2_SQUARED",
                                       partition_by_fields=['meta().id'])
        vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=7)
        bhive_idx_1 = QueryDefinition(index_name='bhive_description',
                                      index_fields=['descriptionVector VECTOR'],
                                      dimension=384, description=f"IVF,PQ32x8",
                                      similarity="L2_SQUARED", partition_by_fields=['meta().id'])
        bhive_query_1 = bhive_idx_1.generate_index_create_query(namespace=collection_namespace, bhive_index=True,
                                                                num_partition=3)
        for query in [bhive_query_1, vector_query_1, scalar_query_1]:
            self.run_cbq_query(query=query, server=self.n1ql_node)

        shard_map = self.get_shards_index_map()
        self.validate_shard_seggregation(shard_index_map=shard_map)

        # when shard seggregation is disabled there is a chance where different categories of indexes can share shards
        self.disable_shard_seggregation()
        self.sleep(10)

        scalar_idx_1 = QueryDefinition(index_name='scalar_rgb_2', index_fields=['color'],
                                       partition_by_fields=['meta().id'])
        scalar_query_1 = scalar_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=3)
        vector_idx_1 = QueryDefinition(index_name='vector_description_2', index_fields=['descriptionVector VECTOR'],
                                       dimension=384,
                                       description="IVF,PQ64x8", similarity="L2_SQUARED",
                                       partition_by_fields=['meta().id'])
        vector_query_1 = vector_idx_1.generate_index_create_query(namespace=collection_namespace, num_partition=7)
        bhive_idx_1 = QueryDefinition(index_name='bhive_description_2',
                                      index_fields=['descriptionVector VECTOR'],
                                      dimension=384, description=f"IVF,PQ32x8",
                                      similarity="L2_SQUARED", partition_by_fields=['meta().id'])
        bhive_query_1 = bhive_idx_1.generate_index_create_query(namespace=collection_namespace, bhive_index=True,
                                                                num_partition=3)
        for query in [bhive_query_1, vector_query_1, scalar_query_1]:
            self.run_cbq_query(query=query, server=self.n1ql_node)

        try:
            shard_map = self.get_shards_index_map()
            self.validate_shard_seggregation(shard_index_map=shard_map)
        except Exception as e:
            self.log.info(f" error {str(e)}")