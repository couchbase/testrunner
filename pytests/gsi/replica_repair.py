"""
replica_repair.py: These tests validate the changes introduced in 7.2.1 for replica repair

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 29/06/23 12:05 pm

"""
from membase.api.capella_rest_client import RestConnection as RestConnectionCapella
from membase.api.on_prem_rest_client import RestHelper
from membase.api.rest_client import RestConnection
from .base_gsi import BaseSecondaryIndexingTests


class ReplicaRepair(BaseSecondaryIndexingTests):
    def setUp(self):
        super(ReplicaRepair, self).setUp()
        self.log.info("==============  ReplicaRepair setup has started ==============")
        if self.capella_run:
            buckets = self.rest.get_buckets()
            if buckets:
                for bucket in buckets:
                    RestConnectionCapella.delete_bucket(self, bucket=bucket.name)

        else:
            self.rest.delete_all_buckets()
        self.password = self.input.membase_settings.rest_password
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        if not self.capella_run:
            self._create_server_groups()
            self.cb_version = float(self.cb_version.split('-')[0][0:3])
        self.log.info("==============  ReplicaRepair setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  ReplicaRepair tearDown has started ==============")
        self.log.info("==============  ReplicaRepair tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  ReplicaRepair tearDown has started ==============")
        super(ReplicaRepair, self).tearDown()
        self.log.info("==============  ReplicaRepair tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  ReplicaRepair suite_setup has started ==============")
        self.log.info("==============  ReplicaRepair suite_setup has completed ==============")

    def test_replica_repair_with_rebalance(self):
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        for namespace in self.namespaces:
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, num_replica=self.num_index_replica)
            self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace)
        self.wait_until_indexes_online()

        indexer_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']

        # Marking 2 index nodes for excludeNode=in and check if replica repair is happening on it
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        exclude_list = index_nodes[:-1]
        for node in exclude_list:
            rest = RestConnection(node)
            index_setting = "excludeNode=in"
            rest.set_index_planner_settings(index_setting)
            value = rest.get_exclude_node_value()
            self.log.info(f"Setting planner value on {node} to {value}")
        self.sleep(30, "Adding sleep so that indexer reads the setting changed")

        try:
            node_out = index_nodes[-1]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node_out],
                                                     services=['index'], cluster_config=self.cluster_config)
            self.sleep(30)
            reached = RestHelper(self.rest).rebalance_reached()
            rebalance.result()
            indexer_metadata_after_repair = self.index_rest.get_indexer_metadata()['status']
            self.assertEqual(len(indexer_metadata_after_repair), len(indexer_metadata_before_rebalance))

            # As the nodes are marked for exclude no new index creation should be allowed
            create_query = f'create index idx on {self.namespaces[0]}(age) with {{"num_replica": 1}}'
            self.run_cbq_query(query=create_query)
            self.fail("This query shouldn't run as all the index nodes are marked for exclude")
        except Exception as err:
            err_msg = 'Some indexer nodes may be marked as excluded'
            if err_msg not in str(err):
                self.fail(err)
        finally:
            # resetting the nodes exclude value
            for node in exclude_list:
                rest = RestConnection(node)
                index_setting = "excludeNode="
                rest.set_index_planner_settings(index_setting)
                value = rest.get_exclude_node_value()
                self.log.info(f"Setting planner value on {node} to {value}")
