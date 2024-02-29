from .gsi_index_partitioning import GSIIndexPartitioningTests
import time
from membase.helper.bucket_helper import BucketOperationHelper

class GSIPartialPartitioningTests(GSIIndexPartitioningTests):
    def setUp(self):
        super(GSIPartialPartitioningTests, self).setUp()
        self.partitoned_index = self.input.param("partitoned_index", False)
        self.partial_index = self.input.param("partial_index", False)
        self.secondary = self.input.param("secondary", False)
        self.equivalent_indexes = self.input.param("equivalent_indexes", False)
        self.ddl_type = self.input.param("ddl_type", "UPDATE")
        self.num_index_replicas = self.input.param("num_index_replicas", 0)
        self.alter_index_increase = self.input.param("alter_index_increase", False)
        self.alter_index_decrease = self.input.param("alter_index_decrease", False)
        self.rest.load_sample("travel-sample")
        init_time = time.time()
        while True:
            next_time = time.time()
            query_response = self.n1ql_helper.run_cbq_query("SELECT COUNT(*) FROM `" + self.bucket_name + "`")
            if query_response['results'][0]['$1'] == 31591:
                break
            if next_time - init_time > 600:
                break
            time.sleep(1)

        self.wait_until_indexes_online()
        # Only need to delete the indexes on the main travel-sample bucket, we do not care about the other indexes
        list_of_indexes = self.n1ql_helper.run_cbq_query(query="select raw name from system:indexes where keyspace_id = 'travel-sample'")

        for index in list_of_indexes['results']:
            if index == "#primary":
                continue
            else:
                # Leave the primary index for within comparisons
                if index == "def_primary":
                    continue
                else:
                    self.n1ql_helper.run_cbq_query(query="drop index `travel-sample`.`%s`" % index)

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        # Adding sleep due to MB-37067, once resolved, remove this sleep and delete_all_buckets
        self.sleep(120)
        super(GSIPartialPartitioningTests, self).tearDown()


    # Create an index and verify the replicas
    def _create_index_query(self, index_statement='', index_name=''):
        try:
            self.n1ql_helper.run_cbq_query(query=index_statement, server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.assertTrue(self.verify_index_in_index_map(index_name),
                        "Index did not appear in the index map after 10 minutes")
        self.assertTrue(self.wait_until_specific_index_online(index_name), "Index never finished building")
        index_map = self.get_index_map()
        self.log.info(index_map)
        self.n1ql_helper.verify_replica_indexes([index_name], index_map, self.num_index_replicas)

    # Create a partitioned index and verify the replicas
    def _create_partitioned_index(self, index_statement='', index_name =''):
        try:
            self.n1ql_helper.run_cbq_query(query=index_statement,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.sleep(10)

        self.verify_partitioned_indexes(index_name, self.num_index_replicas)

    # Verify the partioned indexes
    def verify_partitioned_indexes(self, index_name='', expected_replicas=0, dropped_replica=False, replicaId=0):
        index_map = self.get_index_map()
        self.log.info(index_map)

        index_metadata = self.rest.get_indexer_metadata()
        self.log.info("Indexer Metadata Before Build:")
        self.log.info(index_metadata)

        self.assertTrue(
            self.validate_partition_map(index_metadata, index_name, expected_replicas, self.num_index_partitions, dropped_replica, replicaId),
            "Partitioned index created not as expected")

    def test_common_mutations(self):
        num_hotels = 917
        num_airlines = 187
        num_airports = 1968
        metaid_query = "select meta().id from `{0}` where type = {1} LIMIT 1"

        if self.partitoned_index:
            create_index_query = "CREATE INDEX idx1 ON `{0}`(type) PARTITION BY HASH(meta().id)".format(self.bucket_name)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx1')
        elif self.partial_index:
            create_index_query = "CREATE INDEX idx1 ON `{0}`(type) WHERE type = 'hotel'".format(self.bucket_name)
            self._create_index_query(index_statement=create_index_query, index_name='idx1')
            create_index_query = "CREATE INDEX idx2 ON `{0}`(type) WHERE type = 'airline'".format(self.bucket_name)
            self._create_index_query(index_statement=create_index_query, index_name='idx2')
            create_index_query = "CREATE INDEX idx3 ON `{0}`(type) WHERE type = 'airport'".format(self.bucket_name)
            self._create_index_query(index_statement=create_index_query, index_name='idx3')
        elif self.secondary:
            create_index_query = "CREATE INDEX idx1 ON `{0}`(type)".format(self.bucket_name)
            self._create_index_query(index_statement=create_index_query, index_name='idx1')
        elif self.equivalent_indexes:
            create_index_query = "CREATE INDEX idx1 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'hotel' with {{'num_replica':{1}, 'num_partition':{2}}}".format(self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx1')
            create_index_query = "CREATE INDEX idx2 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'airline' with {{'num_replica':{1}, 'num_partition':{2}}}".format(
                self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx2')
            create_index_query = "CREATE INDEX idx3 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'airport' with {{'num_replica':{1}, 'num_partition':{2}}}".format(
                self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx3')
            create_index_query = "CREATE INDEX idx4 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'hotel' with {{'num_replica':{1}, 'num_partition':{2}}}".format(self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx4')
            create_index_query = "CREATE INDEX idx5 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'airline' with {{'num_replica':{1}, 'num_partition':{2}}}".format(
                self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx5')
            create_index_query = "CREATE INDEX idx6 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'airport' with {{'num_replica':{1}, 'num_partition':{2}}}".format(
                self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx6')
        else:
            create_index_query = "CREATE INDEX idx1 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'hotel' with {{'num_replica':{1}, 'num_partition':{2}}}".format(self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx1')
            create_index_query = "CREATE INDEX idx2 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'airline' with {{'num_replica':{1}, 'num_partition':{2}}}".format(
                self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx2')
            create_index_query = "CREATE INDEX idx3 ON `{0}`(type) PARTITION BY HASH(meta().id) WHERE type = 'airport' with {{'num_replica':{1}, 'num_partition':{2}}}".format(
                self.bucket_name, self.num_index_replicas, self.num_index_partitions)
            self._create_partitioned_index(index_statement=create_index_query, index_name='idx3')
        self.wait_until_indexes_online()

        # Just make sure indexes are functioning as expected
        self.verify_type_fields(num_hotels,num_airports,num_airlines)

        hotel_document = self.n1ql_helper.run_cbq_query(query=metaid_query.format(self.bucket_name, "'hotel'"), server=self.n1ql_node)
        hotel_id = hotel_document['results'][0]['id']

        if self.alter_index_increase:
            alter_index_query = 'ALTER INDEX `{0}`.'.format(self.bucket_name) + 'idx1' + \
                                ' WITH {"action":"replica_count","num_replica": 2}'
            self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node)

        elif self.alter_index_decrease:
            alter_index_query = 'ALTER INDEX `{0}`.'.format(self.bucket_name) + 'idx1' + \
                                ' WITH {"action":"replica_count","num_replica": 1}'
            self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node)

        if self.ddl_type == "INSERT":
            insert1 = {'id': '001', 'type': 'airline'}
            insert2 = {'id': '002', 'type': 'hotel'}
            insert3 = {'id': '003', 'type': 'airport'}
            self.n1ql_helper.run_cbq_query(query="INSERT INTO `{0}` ( KEY, VALUE ) VALUES ('k001', {1})".format(self.bucket_name, insert1), server=self.n1ql_node)
            num_airlines = num_airlines + 1

            self.verify_type_fields(num_hotels, num_airports, num_airlines)

            self.n1ql_helper.run_cbq_query(query="INSERT INTO `{0}` ( KEY, VALUE ) VALUES ('k002', {1})".format(self.bucket_name, insert2), server=self.n1ql_node)
            num_hotels = num_hotels + 1

            self.verify_type_fields(num_hotels, num_airports, num_airlines)

            self.n1ql_helper.run_cbq_query(query="INSERT INTO `{0}` ( KEY, VALUE ) VALUES ('k003', {1})".format(self.bucket_name, insert3), server=self.n1ql_node)
            num_airports = num_airports + 1

            self.verify_type_fields(num_hotels, num_airports, num_airlines)
        elif self.ddl_type == "DELETE":
            self.n1ql_helper.run_cbq_query(query="DELETE FROM `{0}` WHERE meta().id = '{1}'".format(self.bucket_name,hotel_id), server=self.n1ql_node)
            num_hotels = num_hotels - 1
            self.verify_type_fields(num_hotels, num_airports, num_airlines)
        else:
            self.n1ql_helper.run_cbq_query(query="UPDATE `{0}` USE KEYS '{1}' SET type = 'airline'".format(self.bucket_name,hotel_id), server=self.n1ql_node)
            num_hotels = num_hotels - 1
            num_airlines = num_airlines + 1

            self.verify_type_fields(num_hotels, num_airports, num_airlines)

            self.n1ql_helper.run_cbq_query(query="UPDATE `{0}` USE KEYS '{1}' SET type = 'hotel'".format(self.bucket_name,hotel_id), server=self.n1ql_node)
            num_hotels = num_hotels + 1
            num_airlines = num_airlines - 1

            self.verify_type_fields(num_hotels, num_airports, num_airlines)
