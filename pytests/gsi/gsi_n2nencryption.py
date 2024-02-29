"""gsi_n2nencryption.py: description

__author__ = "Pavan PB"
__maintainer = "Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"

"""
from security.ntonencryptionBase import ntonencryptionBase
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests

class TLSSecondaryIndexing(BaseSecondaryIndexingTests):
    def setUp(self):
        super(TLSSecondaryIndexing, self).setUp()
        self.log.info("==============  TLSSecondaryIndexing setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.log.info("==============  TLSSecondaryIndexing setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  TLSSecondaryIndexing tearDown has started ==============")
        super(TLSSecondaryIndexing, self).tearDown()
        self.log.info("==============  TLSSecondaryIndexing tearDown has completed ==============")

    def test_create_and_alter_index_with_nodeinfo_on_all_encryption_mode(self):
        '''
        Enable node to node encryption and set encryption level to all.
        Validate that create/alter indexes work with both 8091 and 18091 ports.
        https://issues.couchbase.com/browse/MB-49879
        '''
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="all")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        host1, host2, port1, port2 = servers[0].ip, servers[1].ip, servers[0].port, 18091
        # create index on port 8091 on node1 and 18091 on node 2
        index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
        deploy_node_info = ["{0}:{1}".format(host1, port1)]
        query = index_gen1.generate_index_create_query(namespace=collection_namespace,
                                                       deploy_node_info=deploy_node_info)
        index_gen2 = QueryDefinition(index_name='idx2', index_fields=['city'])
        deploy_node_info2 = ["{0}:{1}".format(host2, port2)]
        query2 = index_gen2.generate_index_create_query(namespace=collection_namespace, deploy_node_info=deploy_node_info2)
        self.run_cbq_query(query=query)
        self.run_cbq_query(query=query2)
        self.wait_until_indexes_online()
        self.sleep(10)
        index_info = self.get_index_map()[self.test_bucket]
        for idx in index_info:
            if idx == 'idx1' and host1 not in index_info[idx]['hosts']:
                self.fail(f"Index idx1 not on expected host. Expected host {host1} Actual host {index_info[idx]['hosts']}")
            elif idx == 'idx2' and host2 not in index_info[idx]['hosts']:
                self.fail(f"Index idx2 not on expected host. Expected host {host2} Actual host {index_info[idx]['hosts']}")
        # Verify if the newly created indexes are being used with an explain query
        query = f'explain select count(*) from {collection_namespace}  where age > 45'
        result = self.run_cbq_query(query=query)['results']
        self.assertEqual(result[0]['plan']['~children'][0]['index'], 'idx1',
                         "idx1 index is not used for age scan.")
        query = f'explain select count(*) from {collection_namespace}  where city like "a%"'
        result = self.run_cbq_query(query=query)['results']
        self.assertEqual(result[0]['plan']['~children'][0]['index'], 'idx2',
                         "idx2 index is not used for city scan.")
        # alter idx1 on 18091 and increase replica count
        nodes_info1 = ["{0}:18091".format(server.ip,server.port) for server in servers[:2]]
        self.alter_index_replicas(index_name='idx1', namespace=collection_namespace, action='replica_count',
                                  nodes=nodes_info1, num_replicas=1)
        # alter idx2 on 8091 and increase replica count
        nodes = ["{0}:{1}".format(server.ip, server.port) for server in servers[:2]]
        self.alter_index_replicas(index_name='idx2', namespace=collection_namespace, action='replica_count',
                                  nodes=nodes, num_replicas=1)
        self.sleep(10)
        self.wait_until_indexes_online()
        index_info = self.get_index_map()[self.test_bucket]
        # verify that index hosts info gets updated for the replica hosts
        for idx in index_info:
            if 'idx1' in idx:
                if 'replica' not in idx:
                    if host1 not in index_info[idx]['hosts']:
                        self.fail(f"Index idx1 not on expected host after alter statement. "
                                  f"Expected host {host1} Actual host {index_info[idx]['hosts']}")
                else:
                    if host2 not in index_info[idx]['hosts']:
                        self.fail(f"Index idx1 (replica) not on expected host after alter statement"
                                  f". Expected host {host2} Actual host {index_info[idx]['hosts']}")
            elif idx == 'idx2':
                if 'replica' not in idx:
                    if host2 not in index_info[idx]['hosts']:
                        self.fail(f"Index idx2 replica not on expected host after alter statement. "
                                  f"Expected host {host2} Actual host {index_info[idx]['hosts']}")
                else:
                    if host1 not in index_info[idx]['hosts']:
                        self.fail(f"Index idx2  not on expected host after alter statement"
                                  f". Expected host {host1} Actual host {index_info[idx]['hosts']}")
