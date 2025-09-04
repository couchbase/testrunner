from .gsi_index_partitioning import GSIIndexPartitioningTests
from lib.remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.helper.bucket_helper import BucketOperationHelper
import random
from threading import Thread


class GSIAlterIndexesTests(GSIIndexPartitioningTests):
    def setUp(self):
        super(GSIAlterIndexesTests, self).setUp()
        self.num_change_replica = self.input.param("num_change_replica", 0)
        self.replica_index = self.input.param("replica_index", False)
        self.build_index = self.input.param("build_index", False)
        self.expect_failure = self.input.param("expect_failure", False)
        self.replicaId = self.input.param("replicaId", 1)
        self.negative_test = self.input.param('negative_test', None)
        self.stop_server = self.input.param('stop_server', None)
        self.check_repair = self.input.param('check_repair', False)
        self.drop_replica = self.input.param('drop_replica', False)
        self.same_index = self.input.param('same_index', False)
        self.change_replica_count = self.input.param('change_replica_count', False)
        self.create_replica_hole = self.input.param('create_replica_hole', False)
        self.server_group_basic = self.input.param('server_group_basic', False)
        self.decrement_from_server_group = self.input.param('decrement_from_server_group', False)
        self.flush_bucket = self.input.param('flush_bucket', False)
        self.alter_index_error = ''
        # No need for this step as data has already been loaded into the bucket
        # self.shell.execute_cbworkloadgen(self.rest.username, self.rest.password, 400000, 100, "default", 1024, '-j')

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        # Adding sleep due to MB-37067, once resolved, remove this sleep and delete_all_buckets
        self.sleep(120)
        super(GSIAlterIndexesTests, self).tearDown()

    # Create an index and verify the replicas
    def _create_index_query(self, index_statement='', index_name='', defer_build=False):
        try:
            self.n1ql_helper.run_cbq_query(query=index_statement, server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.assertTrue(self.verify_index_in_index_map(index_name),
                        "Index did not appear in the index map after 10 minutes")
        self.assertTrue(self.wait_until_specific_index_online(index_name, defer_build=defer_build), "Index never finished building")
        index_map = self.get_index_map()
        self.log.info(index_map)
        self.n1ql_helper.verify_replica_indexes([index_name], index_map, self.num_index_replica)

    # Create a partitioned index and verify the replicas
    def _create_partitioned_index(self, index_statement='', index_name ='', defer_build=False):
        try:
            self.n1ql_helper.run_cbq_query(query=index_statement,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("index creation failed with error : {0}".format(str(ex)))

        self.assertTrue(self.verify_index_in_index_map(index_name),
                        "Index did not appear in the index map after 10 minutes")

        self.assertTrue(self.wait_until_indexes_online(defer_build=defer_build), "Indexes never finished building")

        self.verify_partitioned_indexes(index_name, self.num_index_replica)

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

    # Use alter index to increase/decrease replicas as well as drop replicas
    def _alter_index_replicas(self, index_name, bucket="default", num_replicas=1, set_error=False ,no_num_replica=False, drop_replica=False, replicaId=1):
        error = []
        if drop_replica:
            alter_index_query = 'ALTER INDEX {0}.'.format(bucket) + index_name + \
                                ' WITH {{"action":"drop_replica","replicaId": {0}}}'.format(replicaId)
            self.log.info(f"Executing drop replica query: {alter_index_query}")
            self.log.info(f"Dropping replica {replicaId} for index {index_name}")
        else:
            # Negative case consideration
            if no_num_replica:
                alter_index_query = 'ALTER INDEX {0}.'.format(bucket) + index_name + \
                                    ' WITH {{"action":"replica_count"}}'
            else:
                alter_index_query = 'ALTER INDEX {0}.'.format(bucket) + index_name + \
                                    ' WITH {{"action":"replica_count","num_replica": {0}}}'.format(num_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node)
        except Exception as ex:
            error.append(str(ex))
            self.log.error(str(ex))

        if error:
            if self.expect_failure:
                self.log.info("alter index replica count failed as expected")
                self.log.info("Error : %s", error)
                if set_error:
                    self.alter_index_error = error
            else:
                self.log.info("Error : %s", error)
                self.fail("alter index failed to change the number of replicas")
        else:
            self.log.info("alter index started successfully")
        return error

    '''Execute specific negative test cases for alter index'''
    def test_alter_index_neg(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        if self.negative_test == 'string':
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas='string')
        elif self.negative_test == 'no_num_replica':
            error = self._alter_index_replicas(index_name=index_name_prefix, no_num_replica=True)

        else:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=None)

        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        if self.expected_err_msg not in error[0]:
            self.fail("Move index failed with unexpected error")

    '''Execute alter index tests on indexes with and without replicas'''
    def test_alter_index_with_num_replica(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        if self.stop_server:
            remote = RemoteMachineShellConnection(self.servers[1])
            remote.stop_server()
            # Sleep to wait some time to let the server properly stop, and to let the cluster become aware of an issue
            self.sleep(30)

        try:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
            # Review how to get rid of this sleep, need to check if alter index is for increase/decrease and check accordingly
            self.sleep(10)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

            if self.expected_err_msg:
              if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error")
            else:
              index_map = self.get_index_map()
              definitions = self.rest.get_index_statements()
              if not expected_num_replicas == 0:
                  for definition in definitions:
                      if index_name_prefix in definition:
                          self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
              self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)
        finally:
            if self.stop_server:
                remote.start_server()
                # Sleep sometime to let the cluster catch up to the server being back online
                self.sleep(30)

    '''Create an index with the same names on two different buckets, make sure alter index works on the intended index'''
    def test_alter_index_multiple_buckets(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"
        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON standard_bucket0(age) USING GSI;"
        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)

        # Review how to get rid of this sleep, need to check if alter index is for increase/decrease and check accordingly
        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()
        definitions = self.rest.get_index_statements()
        if not expected_num_replicas == 0:
            for definition in definitions:
                if index_name_prefix in definition and "default" in definition:
                    self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

    '''Test basic paritioned indexes'''
    def test_alter_index_num_partitions(self):
        self._load_emp_dataset(end=self.num_items)
        expected_num_replicas= self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
        else:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{0}}}".format(self.num_index_partitions)

        self._create_partitioned_index(create_index_statement, 'idx1')

        error = self._alter_index_replicas(index_name='idx1', num_replicas=expected_num_replicas)

        # Review how to get rid of this sleep, need to check if alter index is for increase/decrease and check accordingly
        self.sleep(10)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        self.verify_partitioned_indexes('idx1', expected_num_replicas)

    ''' Test drop replicas for partitioned indexes'''
    def test_alter_index_num_partitions_drop_replica(self):
        self._load_emp_dataset(end=self.num_items)
        expected_num_replicas= self.num_index_replica - 1

        if self.replica_index:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}}}".format(
                self.num_index_replica, self.num_index_partitions)
        else:
            create_index_statement = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{0}}}".format(self.num_index_partitions)
        self._create_partitioned_index(create_index_statement, 'idx1')
        error = self._alter_index_replicas(index_name='idx1', drop_replica=True, replicaId=self.replicaId)
        self.sleep(60)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        self.verify_partitioned_indexes('idx1', expected_num_replicas, dropped_replica=True, replicaId=self.replicaId)

    '''This test is designed to see if you can increment a deferred index before it is built or after it is built, 
       replica should behave the same as the index it is a replica of. If the index is deferred the replica should also 
       be deferred, if the index is built the replica should be built'''
    def test_alter_index_with_num_replica_deferred_partitioned(self):
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_replica':{0}, 'num_partition':{1}, 'defer_build':true}}".format(
                self.num_index_replica, self.num_index_partitions)
        else:
            create_index_query = "CREATE INDEX idx1 on default(name,dept,salary) partition by hash(name) with {{'num_partition':{0}, 'defer_build':true}}".format(self.num_index_partitions)

        self._create_partitioned_index(create_index_query, 'idx1', defer_build=True)

        if self.build_index:
            build_index_query = "BUILD INDEX ON default('idx1')"
            self.n1ql_helper.run_cbq_query(query=build_index_query, server=self.n1ql_node)
            self.assertTrue(self.verify_index_in_index_map('idx1'),
                            "Index did not appear in the index map after 10 minutes")
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        error = self._alter_index_replicas(index_name='idx1', num_replicas=expected_num_replicas)

        self.sleep(5)
        if not self.build_index:
            self.assertTrue(self.wait_until_indexes_online(defer_build=True), "Indexes were never created")
        else:
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()
        # Check if the added replicas are in the same state as the index they are replica of
        # (if index is built new replica should be built, if index is still deferred new replica should be deferred
        for index in index_map['default']:
            if self.build_index:
                self.assertEqual(index_map['default'][index]['status'], 'Ready')
            else:
                self.assertEqual(index_map['default'][index]['status'], 'Created')

        self.verify_partitioned_indexes('idx1', expected_num_replicas)


    '''Execute alter index tests on indexes with and without replicas'''
    def test_alter_index_with_num_replica_rebalance(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        num_index_replicas = 2
        expected_num_replicas = num_index_replicas + self.num_change_replica
        nodes_with_replicas = []
        nodes_list = []
        i = 0

        for server in self.servers:
            nodes_list.append((i,'{0}:{1}'.format(server.ip, self.node_port)))
            i += 1

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'nodes': '{0}:{1}'}};".format(self.master.ip,
                                                                                                   self.node_port)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        if self.replica_index:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=num_index_replicas)
            self.sleep(10)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()

        for index in index_map['default']:
            if index_map['default'][index]['hosts'] not in nodes_with_replicas:
                nodes_with_replicas.append(index_map['default'][index]['hosts'])

        if self.drop_replica:
            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
            self.sleep(30)
        else:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
            self.sleep(10)

        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        index_map = self.get_index_map()

        if self.drop_replica:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas,
                                                    dropped_replica=True, replicaId=self.replicaId)
        else:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

        if self.num_change_replica > 0:
            # Rebalance out the node that contains the newly created replica
            for index in index_map['default']:
                if index_map['default'][index]['hosts'] not in nodes_with_replicas:
                    for node in nodes_list:
                        if index_map['default'][index]['hosts'] == node[1]:
                            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[],[self.servers[node[0]]])
                            reached = RestHelper(self.rest).rebalance_reached()
                            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                            rebalance.result()

        else:
            new_nodes_list = []
            for index in index_map['default']:
                if index_map['default'][index]['hosts'] not in new_nodes_list:
                    new_nodes_list.append(index_map['default'][index]['hosts'])

            # Rebalance out the node that contains the decreased replica
            self.log.info(f"nodes_with_replicas: {nodes_with_replicas}")
            self.log.info(f"new_nodes_list: {new_nodes_list}")
            nodes_to_rebalance_out = list(set(nodes_with_replicas) - set(new_nodes_list))
            if not nodes_to_rebalance_out:
                self.log.info("No nodes need to be rebalanced out - all replica nodes are still in use")
                return
            rebalance_out_node = nodes_to_rebalance_out[0]
            rebalance_in_server=''
            for node in nodes_list:
                if node[1] == rebalance_out_node:
                    rebalance_in_server = self.servers[node[0]]
                    rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                             [self.servers[node[0]]])
                    reached = RestHelper(self.rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                    rebalance.result()

            # Replica that was removed should be re-created
            if self.check_repair:
                # Sleep for sometime to allow cluster state to be stabilized after rebalance
                self.sleep(30)
                pre_rebalance_in_map = self.get_index_map()
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [rebalance_in_server], [],services=["index"])
                reached = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                rebalance.result()
                post_rebalance_in_map = self.get_index_map()
                self.log.info(f"Pre rebalance in map: {pre_rebalance_in_map}")
                self.log.info(f"Post rebalance in map: {post_rebalance_in_map}")
                self.assertEqual(pre_rebalance_in_map, post_rebalance_in_map)

    '''Do the same alter index tests on an index created with a node list'''
    def test_alter_index_with_node_list(self):
        i = 0
        nodes_list=[]
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica
        for _ in self.servers:
            if i <= self.num_index_replica:
                nodes_list.append('{0}:{1}'.format(self.servers[i].ip, self.node_port))
                i += 1

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes':{1}}};".format(self.num_index_replica, nodes_list)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        if self.stop_server:
            remote = RemoteMachineShellConnection(self.servers[1])
            remote.stop_server()
            # Sleep to wait some time to let the server properly stop, and to let the cluster become aware of an issue
            self.sleep(30)

        try:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)

            self.sleep(5)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

            if self.expected_err_msg:
              if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error")
            else:
              index_map = self.get_index_map()
              definitions = self.rest.get_index_statements()
              if not expected_num_replicas == 0:
                  for definition in definitions:
                      if index_name_prefix in definition:
                          self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
              self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)
        finally:
            if self.stop_server:
                remote.start_server()
                # Sleep sometime to let the cluster catch up to the server being back online
                self.sleep(30)

    '''This test is designed to see if you can increment a deferred index before it is built or after it is built, 
       replica should behave the same as the index it is a replica of. If the index is deferred the replica should also 
       be deferred, if the index is built the replica should be built'''
    def test_alter_index_with_num_replica_deferred(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0},'defer_build':true}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI WITH {{'defer_build':true}};"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix, defer_build=True)

        if self.build_index:
            build_index_query = "BUILD INDEX ON default(%s)" % index_name_prefix
            self.n1ql_helper.run_cbq_query(query=build_index_query, server=self.n1ql_node)

        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)

        self.sleep(30)
        if not self.build_index:
            self.assertTrue(self.wait_until_indexes_online(defer_build=True), "Indexes were never created")
        else:
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        if self.expected_err_msg:
          if self.expected_err_msg not in error[0]:
            self.fail("Move index failed with unexpected error")
        else:
          index_map = self.get_index_map()
          definitions = self.rest.get_index_statements()
          # Check if the added replicas are in the same state as the index they are replica of
          # (if index is built new replica should be built, if index is still deferred new replica should be deferred
          for index in index_map['default']:
              if self.build_index:
                  self.assertEqual(index_map['default'][index]['status'], 'Ready')
              else:
                  self.assertEqual(index_map['default'][index]['status'], 'Created')

          if not expected_num_replicas == 0:
              for definition in definitions:
                  if index_name_prefix in definition:
                      self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
          self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, self.num_index_replica + self.num_change_replica)

    '''Execute failover tests for alter index'''
    def test_alter_index_with_num_replica_failover(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"
        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)
        try:
            # Get the index map to identify which node contains the index
            index_map = self.get_index_map()
            index_nodes = set()
            # Find all nodes that contain the index
            for index_name, index_info in index_map.get('default', {}).items():
                if index_name_prefix in index_name:
                    if 'hosts' in index_info:
                        for host in index_info['hosts']:
                            node_ip = host.split(':')[0]
                            index_nodes.add(node_ip)
            # Find a node to failover that doesn't contain the index and is not a KV node
            failover_candidate = None
            kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
            kv_node_ips = {node.ip for node in kv_nodes}
            for i in range(self.nodes_init):
                server = self.servers[i]
                if server.ip not in index_nodes and server.ip not in kv_node_ips:
                    failover_candidate = server
                    break
            # If no suitable non-KV, non-index node found, try to find any non-KV node
            if failover_candidate is None:
                for i in range(self.nodes_init):
                    server = self.servers[i]
                    if server.ip not in kv_node_ips:
                        failover_candidate = server
                        break
            # If all nodes are KV nodes, use the last non-KV node but log a warning
            if failover_candidate is None:
                failover_candidate = index_nodes[1]
                self.log.warning(f"All nodes contain the index {index_name_prefix}, using last node for failover")
            else:
                self.log.info(f"Failing over node {failover_candidate.ip} which doesn't contain index {index_name_prefix}")
            # Failover the selected node
            failover_task = self.cluster.async_failover(self.servers[:self.nodes_init], failover_nodes=[failover_candidate])
            failover_task.result()
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
            self.sleep(5)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
            if self.expected_err_msg:
                if not error[0]:
                    self.fail("Move index failed with unexpected error")
        finally:
            # Use the same node that was failed over for rebalancing out
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [failover_candidate])

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()

    '''Execute alter index tests on indexes with and without replicas'''
    def test_alter_index_drop_index(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica - 1


        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        index_map = self.get_index_map()
        definitions = self.rest.get_index_statements()
        indexes = self.rest.get_indexer_metadata()
        self.log.info(indexes)
        self.log.info(definitions)

        if self.stop_server:
            remote = RemoteMachineShellConnection(self.servers[1])
            remote.stop_server()
            # Sleep to wait some time to let the server properly stop, and to let the cluster become aware of an issue
            self.sleep(30)

        try:
            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
            
            # Log the result of the drop operation
            self.log.info(f"Drop replica operation result: {error}")
            self.log.info(f"Attempting to drop replica {self.replicaId}")

            self.sleep(30)
            if not self.replicaId == 0:
                self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

            if self.expected_err_msg:
              if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error")
            else:
              index_map = self.get_index_map()
              definitions = self.rest.get_index_statements()
              indexes = self.rest.get_indexer_metadata()
              self.log.info(indexes)
              self.log.info(definitions)
              if not self.replicaId == 0:
                  for index in indexes['status']:
                      if index_name_prefix in index['name']:
                          self.log.info("index replicaID: %s" % index['replicaId'])
                          self.log.info("dropped replicaID: %s" % self.replicaId)
                          self.assertTrue(self.replicaId != index['replicaId'], '%s' % str(index['replicaId']))
                  self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas, dropped_replica=True, replicaId=self.replicaId)
              else:
                  # When dropping replica 0, the entire index should be removed
                  self.log.info(f"Checking if index was fully removed (replicaId={self.replicaId})")
                  self.log.info(f"Current definitions count: {len(definitions)}")
                  self.log.info(f"Current definitions: {definitions}")
                  self.log.info(f"Current index map keys: {list(index_map.get('default', {}).keys())}")
                  self.assertTrue(definitions == [], "The index was not fully removed %s" % definitions)
        finally:
            if self.stop_server:
                remote.start_server()
                # Sleep sometime to let the cluster catch up to the server being back online
                self.sleep(30)

    '''Execute alter index tests on indexes with and without replicas'''
    def test_alter_index_drop_all_indexes(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        for i in range(0, self.num_index_replica):
            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=i+1)

        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        if self.expected_err_msg:
          if self.expected_err_msg not in error[0]:
            self.fail("Move index failed with unexpected error")
        else:
          index_map = self.get_index_map()
          self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, 0, dropped_replica=True, replicaId=self.replicaId)

    '''This test is designed to see if you can increment a deferred index before it is built or after it is built, 
       replica should behave the same as the index it is a replica of. If the index is deferred the replica should also 
       be deferred, if the index is built the replica should be built'''
    def test_alter_index_with_drop_replica_deferred(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica - 1

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0},'defer_build':true}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI WITH {{'defer_build':true}};"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix, defer_build=True)

        if self.build_index:
            build_index_query = "BUILD INDEX ON default(%s)" % index_name_prefix
            self.n1ql_helper.run_cbq_query(query=build_index_query, server=self.n1ql_node)

        error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
        self.sleep(60)
        if not self.build_index:
            self.assertTrue(self.wait_until_indexes_online(defer_build=True), "Indexes were never created")
        else:
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        if self.expected_err_msg:
          if self.expected_err_msg not in error[0]:
            self.fail("Move index failed with unexpected error")

        else:
            index_map = self.get_index_map()
            definitions = self.rest.get_index_statements()
            self.log.info(f"Definitions: {definitions}")
            indexes = self.rest.get_indexer_metadata()
            self.log.info(f"Indexes: {indexes}")
            if not self.replicaId == 0:
                for index in index_map['default']:
                    if self.build_index:
                        self.assertEqual(index_map['default'][index]['status'], 'Ready')
                    else:
                        self.assertEqual(index_map['default'][index]['status'], 'Created')
                for index in indexes['status']:
                    if index_name_prefix in index['name']:
                        self.assertTrue(self.replicaId != index['replicaId'])
                self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas,
                                                      dropped_replica=True, replicaId=self.replicaId)
            else:
                self.assertTrue(definitions == [], "The index was not fully removed")

    '''Execute failover tests for alter index'''
    def test_alter_index_with_drop_replica_failover(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        try:
            # Failover an indexer node
            failover_task = self.cluster.async_failover(self.servers[:self.nodes_init], failover_nodes=[self.servers[self.nodes_init-1]])
            failover_task.result()

            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)

            self.sleep(30)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

            if self.expected_err_msg:
                if not error[0]:
                    self.fail("Move index failed with unexpected error")
        finally:
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [self.servers[self.nodes_init-1]])

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()


    '''Execute an alter index during a rebalance'''
    def test_alter_index_concurrent_rebalance(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        t1 = Thread(target=self.cluster.async_rebalance, name="rebalance", args=(self.servers[:self.nodes_init],[],
                                                                                [self.servers[self.nodes_init-1]]))
        t1.start()
        self.sleep(2)
        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
        t1.join()
        self.sleep(120)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        if not error and self.expect_failure:
            self.fail("Move did not fail and it should have")

    '''Put the indexer in a DGM/Paused state (depending on plasma/moi), try to execute alter indexes'''
    def test_alter_index_one_node_in_paused_state(self):
        index_server = self.index_servers[0]
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        create_index_query1 = "CREATE PRIMARY INDEX ON default USING GSI"
        create_index_query2 = "CREATE INDEX idx_job_title ON default(_id) USING GSI"
        create_index_query3 = "CREATE INDEX idx_join_yr ON default(mutated) USING GSI"
        create_index_query4 = "CREATE INDEX idx_job_title_join_yr ON default(_id, mutated) USING GSI"

        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query1,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        # Ensure indexer reaches to paused state
        self._saturate_indexer_memory(index_server)

        if self.drop_replica:
            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
            self.sleep(60)
        else:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
            self.sleep(60)

        self.assertTrue(self.wait_until_indexes_online(check_paused_index=True), "Indexes never finished building")

        if self.expected_err_msg:
            if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error")
        else:
            index_map = self.get_index_map()
            if self.drop_replica:
                self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas,
                                                        dropped_replica=True, replicaId=self.replicaId)
            else:
                self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

    '''Test alter index during create index'''
    def test_alter_index_concurrent_create(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica

        if not self.same_index:
            create_index_query = "CREATE INDEX idx1 ON default(_id) USING GSI;"
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            self.sleep(5)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        if self.same_index:
            threads = [
                Thread(target=self.n1ql_helper.run_cbq_query, name="create index",
                       args=(create_index_query,self.n1ql_node)),
                Thread(target=self._alter_index_replicas, name="alter_index", args=(index_name_prefix,"default", expected_num_replicas))]
        else:
            threads = [
                Thread(target=self.n1ql_helper.run_cbq_query, name="create index",
                       args=(create_index_query,self.n1ql_node)),
                Thread(target=self._alter_index_replicas, name="alter_index", args=('idx1',"default", expected_num_replicas))]

        for thread in threads:
            thread.start()
            self.sleep(8)

        for thread in threads:
            thread.join()

        self.sleep(10)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.same_index:
            self.n1ql_helper.verify_replica_indexes(["idx1"], index_map, expected_num_replicas)
        else:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

    '''Test alter index during alter index, the second alter index fails because the first alter index is still using the index'''
    def test_alter_index_concurrent_alter(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        create_index_query = "CREATE INDEX idx1 ON default(_id) USING GSI;"
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")


        threads = [
            Thread(target=self._alter_index_replicas, name="alter_index", args=(index_name_prefix,"default", expected_num_replicas)),
            Thread(target=self._alter_index_replicas, name="alter index",
                   args=(index_name_prefix,"default", expected_num_replicas - 1))]

        for thread in threads:
            thread.start()
            self.sleep(1)

        for thread in threads:
            thread.join()

        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()
        self.log.info(index_map)
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

    '''Test alter index during build index, alter index should fail because the index is still building'''
    def test_alter_index_concurrent_build(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(name) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        create_index_query = "CREATE INDEX idx1 ON default(body) USING GSI with {'defer_build':true};"
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.wait_until_specific_index_online("idx1", 600, defer_build=True)

        build_index_query = "build index on default(idx1)"

        threads = [
            Thread(target=self.n1ql_helper.run_cbq_query, name="build index",
                   args=(build_index_query, self.n1ql_node)),
            Thread(target=self._alter_index_replicas, name="alter index",
                   args=(index_name_prefix,"default", expected_num_replicas,True))]

        for thread in threads:
            thread.start()
            self.sleep(15)

        for thread in threads:
            thread.join()

        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        if self.expected_err_msg:
            if self.expected_err_msg not in self.alter_index_error[0]:
                self.fail("Move index failed with unexpected error: %s" % self.alter_index_error)
            self.alter_index_error = ''
        else:
            index_map = self.get_index_map()
            definitions = self.rest.get_index_statements()
            if not expected_num_replicas == 0:
                for definition in definitions:
                    if index_name_prefix in definition:
                        self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition,
                                        "Number of replicas in the definition is wrong: %s" % definition)
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

    '''Drop the bucket while alter index is ongoing'''
    def test_alter_index_drop_bucket(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica
        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        if self.flush_bucket:
            threads = [
                Thread(target=self._alter_index_replicas, name="alter_index", args=(index_name_prefix,"default",expected_num_replicas,True)),
                Thread(target=self.rest.flush_bucket, name="flush bucket", args=["default"])]
        else:
            threads = [
                Thread(target=self._alter_index_replicas, name="alter_index", args=(index_name_prefix,"default",expected_num_replicas,True)),
                Thread(target=self.cluster.bucket_delete, name="drop bucket", args=(kv_node,"default"))]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        if self.flush_bucket:
            self.sleep(30)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
            index_map = self.get_index_map()
            definitions = self.rest.get_index_statements()
            if not expected_num_replicas == 0:
                for definition in definitions:
                    if index_name_prefix in definition:
                        self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition,
                                        "Number of replicas in the definition is wrong: %s" % definition)
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)
        else:
            self.assertFalse(self.alter_index_error)

    def test_alter_index_bucket_partial_rollback(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA & NodeB")
        mem_client = MemcachedClientHelper.direct_client(self.servers[0],
                                                         "default")
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self.servers[1],
                                                         "default")
        mem_client.stop_persistence()

        self.run_doc_ops()

        self.sleep(30)

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on NodeB")
        mem_client = MemcachedClientHelper.direct_client(
            self.input.servers[1], "default")
        mem_client.start_persistence()

        # Failover Node B
        self.log.info("Failing over NodeB")
        self.sleep(30)
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [self.servers[1]], self.graceful,
            wait_for_pending=30)

        failover_task.result()

        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            [], [self.servers[1]])

        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached,
                        "rebalance failed, stuck or did not complete")
        rebalance.result()

        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
        self.sleep(10)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()
        definitions = self.rest.get_index_statements()
        if not expected_num_replicas == 0:
            for definition in definitions:
                if index_name_prefix in definition:
                    self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition,
                                    "Number of replicas in the definition is wrong: %s" % definition)
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

    '''Failover the node while alter index is happening'''
    def test_alter_index_failover(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas= self.num_index_replica + self.num_change_replica
        failover_node = self.servers[1]

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        try:
            threads = [
                Thread(target=self.cluster.async_failover, name="failover", args=(self.servers[:self.nodes_init],[failover_node], False)),
                Thread(target=self._alter_index_replicas, name="alter_index", args=(index_name_prefix,"default",expected_num_replicas,True))]

            for thread in threads:
                thread.start()
                self.sleep(3)

            for thread in threads:
                thread.join()

            if self.expect_failure:
                if not self.alter_index_error:
                    self.log.error(self.alter_index_error)
                    self.fail("Alter index did not error!")
                self.alter_index_error = ''
            else:
                self.sleep(5)
                self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
                index_map = self.get_index_map()

                definitions = self.rest.get_index_statements()
                if not expected_num_replicas == 0:
                    for definition in definitions:
                        if index_name_prefix in definition:
                            self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition,
                                            "Number of replicas in the definition is wrong: %s" % definition)
                self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)
        finally:
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [failover_node])

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()

    '''Test a backup restore after alter index happened'''
    def test_alter_index_backup_restore_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        expected_num_replicas = self.num_change_replica + self.num_index_replica

        if self.replica_index:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        if self.drop_replica:
            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
            self.sleep(30)
        else:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)

        self.sleep(10)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map_before_backup = self.get_index_map()
        self.log.info(index_map_before_backup)

        if self.drop_replica:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map_before_backup, expected_num_replicas,
                                                    dropped_replica=True, replicaId=self.replicaId)
        else:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_backup,expected_num_replicas)

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)
        self._create_backup(kv_node)

        # Drop and recreate bucket
        self.cluster.bucket_delete(kv_node, bucket="default")
        default_params = self._create_bucket_params(server=self.master,
                                                    size=self.bucket_size,
                                                    replicas=self.num_replicas)

        self.cluster.create_default_bucket(default_params)

        # Restore backup
        self._create_restore(kv_node)

        self.sleep(60)

        # Validate all indexes restored correctly
        index_map = self.get_index_map()
        self.log.info(index_map)
        if self.drop_replica:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map_before_backup, expected_num_replicas,
                                                    dropped_replica=True, replicaId=self.replicaId)
        else:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_backup,expected_num_replicas)

    '''Test backup restore when the index names are already present in the live cluster'''
    def test_backup_restore_same_index_name(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        index_names = []

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {'num_replica':1};"
        create_index_query2 = "CREATE INDEX idx1 ON default(_id) USING GSI  WITH {'num_replica':3};"
        create_index_query3 = "CREATE INDEX idx2 ON default(age,_id) USING GSI;"

        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        self.n1ql_helper.run_cbq_query(query=create_index_query2, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        self.n1ql_helper.run_cbq_query(query=create_index_query3, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map_before_backup = self.get_index_map()
        self.log.info(index_map_before_backup)

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)
        self._create_backup(kv_node)

        # Drop and recreate bucket
        self.cluster.bucket_delete(kv_node, bucket="default")
        default_params = self._create_bucket_params(server=self.master,
                                                    size=self.bucket_size,
                                                    replicas=self.num_replicas)

        self.cluster.create_default_bucket(default_params)

        if self.node_out > 0:
            node_out = self.servers[self.node_out]
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [node_out])

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()

        # Create new indexes with the same names to see what happens
        if self.change_replica_count:
            create_index_query = "CREATE INDEX " + index_name_prefix + \
                                 " ON default(age) USING GSI;"
            create_index_query2 = "CREATE INDEX idx1 ON default(_id) USING GSI  WITH {'num_replica':2};"
            create_index_query3 = "CREATE INDEX idx2 ON default(age,_id) USING GSI WITH {'num_replica':1};"
        else:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(_id) USING GSI;"
            index_names.append((index_name_prefix, 0))
            create_index_query2 = "CREATE INDEX idx1 ON default(age) USING GSI;"
            index_names.append(('idx1', 0))
            create_index_query3 = "CREATE INDEX idx2 ON default(age,name) USING GSI  WITH {'num_replica': 2};"
            index_names.append(('idx2', 2))

        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        self.n1ql_helper.run_cbq_query(query=create_index_query2, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        self.n1ql_helper.run_cbq_query(query=create_index_query3, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        if self.create_replica_hole:
            error = self._alter_index_replicas(index_name='idx1', drop_replica=True, replicaId=self.replicaId)
            self.sleep(30)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        self._create_restore(kv_node)

        self.sleep(60)

        index_map = self.get_index_map()
        self.log.info(index_map)
        # Old indexes should be renamed
        if not self.change_replica_count:
            index_names.append((index_name_prefix + '_0', 1))
            index_names.append(("idx1_0", 2))
            index_names.append(("idx2_0", 0))
        else:
            index_names.append((index_name_prefix , 0))
            index_names.append(("idx1", 2))
            index_names.append(("idx2", 1))

        for index_name in index_names:
            if self.create_replica_hole and index_name[0] == 'idx1':
                self.n1ql_helper.verify_replica_indexes([index_name[0]], index_map, 1, dropped_replica=True, replicaId=self.replicaId)
            else:
                self.n1ql_helper.verify_replica_indexes([index_name[0]], index_map, index_name[1])

    '''Test backup restore when the live cluster has an unhealthy replica'''
    def test_backup_restore_unhealthy_replica(self):
        create_index_query = "CREATE INDEX idx1 ON default(_id) USING GSI  WITH {'num_replica':2};"
        expected_num_replicas=1

        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map_before_backup = self.get_index_map()
        self.log.info(index_map_before_backup)

        kv_node = self.get_nodes_from_services_map(service_type="kv",
                                                   get_all_nodes=False)
        self._create_backup(kv_node)

        # Drop and recreate bucket
        self.cluster.bucket_delete(kv_node, bucket="default")
        default_params = self._create_bucket_params(server=self.master,
                                                    size=self.bucket_size,
                                                    replicas=self.num_replicas)

        self.cluster.create_default_bucket(default_params)

        if self.node_out > 0:
            node_out = self.servers[self.node_out]
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [node_out])

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()

        create_index_query = "CREATE INDEX idx1 ON default(_id) USING GSI  WITH {'num_replica':2};"

        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.sleep(5)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()
        self.log.info(index_map)

        host_name = index_map['default']['idx1 (replica 1)']['hosts']

        for server in self.servers:
            if host_name == (server.ip + ':' + self.node_port) and server != self.master:
                stop_node = server
                break

        try:
            remote = RemoteMachineShellConnection(stop_node)
            remote.stop_server()
            # Sleep to wait some time to let the server properly stop, and to let the cluster become aware of an issue
            self.sleep(30)
            failover_task = self.cluster.async_failover(self.servers[:self.nodes_init],
                                                        failover_nodes=[stop_node], graceful=False)
            failover_task.result()

            self.sleep(30)
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], [stop_node])
            rest = RestConnection(self.master)
            reached = RestHelper(rest).rebalance_reached()
            self.assertTrue(reached,
                            "rebalance failed, stuck or did not complete")
            rebalance.result()

            self.sleep(60)

            self._create_restore(kv_node)

            self.sleep(60)

            index_map = self.get_index_map()
            self.log.info(index_map)
            self.n1ql_helper.verify_replica_indexes(['idx1'], index_map, expected_num_replicas, dropped_replica=True, replicaId=self.replicaId)
        finally:
            remote.start_server()
            # Sleep sometime to let the cluster catch up to the server being back online
            self.sleep(30)

    '''Execute alter index to increase and decrease indexes in a loop'''
    def test_chain_alter_index(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        for i in range(10):
            # Increase number of replicas
            expected_num_replicas = self.num_replicas + 1
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
            self.sleep(5)
            self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
            index_map = self.get_index_map()
            definitions = self.rest.get_index_statements()
            for definition in definitions:
                if index_name_prefix in definition:
                    self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

            if self.drop_replica:
                # Decrease number of replicas
                expected_num_replicas = expected_num_replicas - 1
                error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
                self.sleep(30)
                self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
                index_map = self.get_index_map()
                definitions = self.rest.get_index_statements()
                if not expected_num_replicas == 0:
                    for definition in definitions:
                        if index_name_prefix in definition:
                            self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition,
                                            "Number of replicas in the definition is wrong: %s" % definition)
                self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas, dropped_replica=True, replicaId=self.replicaId)
            else:
                # Decrease number of replicas
                expected_num_replicas = expected_num_replicas - 1
                error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
                self.sleep(10)
                self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
                index_map = self.get_index_map()
                definitions = self.rest.get_index_statements()
                if not expected_num_replicas == 0:
                    for definition in definitions:
                        if index_name_prefix in definition:
                            self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition,
                                            "Number of replicas in the definition is wrong: %s" % definition)
                self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

    '''Execute an invalid drop replica, then increase the number of replicas so the drop index becomes valid, then execute drop index again'''
    def test_alter_index_fail_drop(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        # Fail drop replica
        error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        self.log.info(error)

        # Increase number of replicas
        expected_num_replicas = self.num_replicas + 1
        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        index_map = self.get_index_map()
        definitions = self.rest.get_index_statements()
        for definition in definitions:
            if index_name_prefix in definition:
                self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

        # Decrease number of replicas
        expected_num_replicas = expected_num_replicas - 1
        error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas, dropped_replica=True, replicaId=self.replicaId)

    '''Drop a replica, then rebalance out the node that the replica was dropped from, rebalance in the node and verify that nothing else gets added'''
    def test_alter_index_drop_rebalance_increase(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        nodes_with_replicas = []
        nodes_list = []
        i = 0
        for server in self.servers:
            nodes_list.append((i,'{0}:{1}'.format(server.ip, self.node_port)))
            i += 1

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        index_map = self.get_index_map()

        for index in index_map['default']:
            if index_map['default'][index]['hosts'] not in nodes_with_replicas:
                nodes_with_replicas.append(index_map['default'][index]['hosts'])

        error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)

        self.sleep(60)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()

        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, (self.num_index_replica - 1),
                                                dropped_replica=True, replicaId=self.replicaId)
        for index in index_map['default']:
            if index_map['default'][index]['hosts'] in nodes_with_replicas:
                for node in nodes_list:
                    if index_map['default'][index]['hosts'] == node[1]:
                        nodes_list.remove((node[0],node[1]))
                        for server in nodes_with_replicas:
                            if node[1] == server:
                                nodes_with_replicas.remove(server)
        for node in nodes_list:
            for server in nodes_with_replicas:
                if node[1] == server:
                    rebalance_in_server = node[0]
                    rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[node[0]]])
                    reached = RestHelper(self.rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                    rebalance.result()

        # Replica that was removed should be re-created because it is not a broken replica
        pre_rebalance_in_map = self.get_index_map()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[rebalance_in_server]], [],services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        post_rebalance_in_map = self.get_index_map()
        self.assertNotEqual(pre_rebalance_in_map, post_rebalance_in_map)

    '''Attempt to drop an unhealthy replica'''
    def test_alter_index_drop_unhealthy_replica(self):
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))

        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(self.num_index_replica)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=False)
        rest = RestConnection(index_node)

        self.sleep(10)
        indexes = rest.get_indexer_metadata()

        rebalance_out_node_ip, rebalance_out_node = None, None
        for index in indexes['status']:
            if '(replica ' in index['name'] and index['hosts'][0] != (self.master.ip + ":" + self.node_port):
                rebalance_out_node_ip = index['hosts'][0]
                replica_name = index['name']
                replica_id = index['replicaId']

        for server in self.servers:
            if (server.ip + ":" + self.node_port) == rebalance_out_node_ip and server != self.master:
                rebalance_out_node = server

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [rebalance_out_node])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        if self.drop_replica:
            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=replica_id)
            self.sleep(30)
        else:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=self.num_index_replica - 1)
            self.sleep(10)

        index_map = self.get_index_map()

        if self.expected_err_msg:
            if self.expected_err_msg not in error[0]:
                self.fail("Move index failed with unexpected error, here is the real error %s" % str(error[0]))
        else:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, (self.num_index_replica - 1),
                                                    dropped_replica=True, replicaId=replica_id)

        # Replica that was removed should not be re-created because it is not a broken replica
        if not self.drop_replica:
            pre_rebalance_in_map = self.get_index_map()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [rebalance_out_node], [],services=["index"])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            post_rebalance_in_map = self.get_index_map()
            self.assertEqual(pre_rebalance_in_map, post_rebalance_in_map)

    '''Use alter index to move the node the index is on, then execute new alter index functionality'''
    def test_move_index_replica_alter(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + \
                             " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes': {1}}};".format(self.num_index_replica, nodes)

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        index_map = self.get_index_map()
        self.log.info(index_map)

        self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                index_map,
                                                len(nodes) - 1, nodes)
        dest_nodes = self._get_node_list(self.dest_node)
        self.log.info(dest_nodes)
        expect_failure = False

        output, error = self._cbindex_move(src_node=self.servers[0],
                                       node_list=dest_nodes,
                                       index_list=index_name_prefix,
                                       expect_failure=expect_failure,
                                           alter_index=self.alter_index)
        self.sleep(60)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, len(dest_nodes) - 1, dest_nodes)

        if self.drop_replica:
            expected_num_replica = self.num_index_replica - 1
            error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=self.replicaId)
            self.sleep(30)
        else:
            expected_num_replica = self.num_index_replica + self.num_change_replica
            error = self._alter_index_replicas(index_name=index_name_prefix,  num_replicas=expected_num_replica)
            self.sleep(10)

        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        index_map = self.get_index_map()
        self.log.info(index_map)

        if self.drop_replica:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replica,
                                                    dropped_replica=True, replicaId=self.replicaId)
        else:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replica)

    '''Test basic server group test cases'''
    def test_alter_index_with_server_groups_basic(self):
        self._create_server_groups()
        self.sleep(5)
        server_group_one= False
        server_group_two = False
        server_group_one_count = 0
        server_group_two_count = 0
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)

        self.sleep(10)

        # First test case is with 2 server groups and 2 copies, one copy should be in each server group
        index_map = self.get_index_map()
        self.log.info(index_map)
        definitions = self.rest.get_index_statements()
        for definition in definitions:
            if index_name_prefix in definition:
                self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)
        if self.server_group_basic:
            for index in index_map['default']:
                if index_map['default'][index]['hosts'] in self.server_group_map['ServerGroup_1']:
                    server_group_one = True
                elif index_map['default'][index]['hosts'] in self.server_group_map['ServerGroup_2']:
                    server_group_two = True

            self.assertTrue(server_group_one and server_group_two , "One of the server groups is not in use and it should be")

        # If you have 3 copies on 2 server groups, when you decrease the number of replicas the server group with more copies should lose its replica
        if self.decrement_from_server_group:
            error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas-1)
            index_map = self.get_index_map()
            self.log.info(index_map)
            definitions = self.rest.get_index_statements()
            for definition in definitions:
                if index_name_prefix in definition:
                    self.assertTrue('"num_replica":{0}'.format(expected_num_replicas-1) in definition,
                                    "Number of replicas in the definition is wrong: %s" % definition)
            self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas-1)

            for index in index_map['default']:
                if index_map['default'][index]['hosts'] in self.server_group_map['ServerGroup_1']:
                    server_group_one_count += 1
                elif index_map['default'][index]['hosts'] in self.server_group_map['ServerGroup_2']:
                    server_group_two_count += 1
            self.assertTrue(server_group_one_count == 1 and server_group_two_count == 1, "There should be one copy in each server group")

    '''If you have 2 server groups with six nodes across them, one copy in each server group. Increase the number of replicas so that
       each server group should have 2 copies each, it should not be the case that one server group has 3 copies and the other has 1'''
    def test_alter_index_with_server_groups_six_node(self):
        self._create_server_groups()
        self.sleep(5)
        server_group_one_count = 0
        server_group_two_count = 0
        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)

        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
        self.sleep(10)

        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas+2)
        self.sleep(10)
        index_map = self.get_index_map()
        self.log.info(index_map)
        definitions = self.rest.get_index_statements()
        for definition in definitions:
            if index_name_prefix in definition:
                self.assertTrue('"num_replica":{0}'.format(expected_num_replicas+2) in definition,
                                "Number of replicas in the definition is wrong: %s" % definition)
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, (expected_num_replicas+2))

        for index in index_map['default']:
            if index_map['default'][index]['hosts'] in self.server_group_map['ServerGroup_1']:
                server_group_one_count += 1
            elif index_map['default'][index]['hosts'] in self.server_group_map['ServerGroup_2']:
                server_group_two_count += 1
        self.assertTrue(server_group_one_count == 2 and server_group_two_count == 2, "There should be one copy in each server group")

    '''Failover a node that has a replica on it , rebalance it out, now rebalance in the node. Drop the replica from the node and increase the number of replicas,
       the rebalanced in node should get the new replica'''
    def test_alter_index_with_server_groups_failover(self):
        self._create_server_groups()
        self.sleep(5)

        index_name_prefix = "random_index_" + str(random.randint(100000, 999999))
        expected_num_replicas = self.num_index_replica + self.num_change_replica

        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI;"

        self._create_index_query(index_statement=create_index_query, index_name=index_name_prefix)


        error = self._alter_index_replicas(index_name=index_name_prefix, num_replicas=expected_num_replicas)
        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")


        index_map = self.get_index_map()
        self.log.info(index_map)
        definitions = self.rest.get_index_statements()
        for definition in definitions:
            if index_name_prefix in definition:
                self.assertTrue('"num_replica":{0}'.format(expected_num_replicas) in definition, "Number of replicas in the definition is wrong: %s" % definition)
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas)

        failover_task = self.cluster.async_failover(self.servers[:self.nodes_init],
                                                    failover_nodes=[self.servers[1]], graceful=False)
        failover_task.result()

        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            [], [self.servers[1]])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached,
                        "rebalance failed, stuck or did not complete")
        rebalance.result()

        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            [self.servers[1]], [], services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached,
                        "rebalance failed, stuck or did not complete")
        rebalance.result()

        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")
        index_metadata = self.rest.get_indexer_metadata()
        self.log.info(index_metadata)

        replica_id = None
        for index in index_metadata['status']:
            if index['hosts'][0] == (self.servers[1].ip + ":" + self.node_port):
                replica_id = index['replicaId']

        error = self._alter_index_replicas(index_name=index_name_prefix, drop_replica=True, replicaId=replica_id)
        self.sleep(30)
        self.assertTrue(self.wait_until_indexes_online(), "Indexes never finished building")

        index_map = self.get_index_map()

        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, (expected_num_replicas - 1),
                                                dropped_replica=True, replicaId=replica_id)

        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes([index_name_prefix], index_map, expected_num_replicas )
