"""gsi_autofailover.py: These tests validate autofailover for GSI

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "10/12/21 11:31 pm"

"""
from couchbase_helper.query_definitions import QueryDefinition
from gsi.collections_concurrent_indexes import powerset
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest
from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.rest_client import RestHelper
from membase.api.rest_client import RestConnection
from membase.api.exception import RebalanceFailedException, ServerUnavailableException


class GSIAutofailover(AutoFailoverBaseTest, BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIAutofailover, self).setUp()
        self.log.info("==============  GSIAutofailover setup has started ==============")
        self.rest.delete_all_buckets()
        self.index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                         'suffix', 'filler1', 'phone', 'zipcode'])
        if self.failover_orchestrator:
            self.master = self.servers[1]
            self.rest = RestConnection(self.master)
        self.log.info("==============  GSIAutofailover setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIAutofailover tearDown has started ==============")
        super(GSIAutofailover, self).tearDown()
        self.log.info("==============  GSIAutofailover tearDown has completed ==============")

    def _create_indexes(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                idx = f'idx_{item}'
                index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              num_replica=self.num_index_replicas)
                self.run_cbq_query(query=query, server=n1ql_node)

    def is_failover_expected(self, failure_node_number):
        failover_not_expected = (self.max_count == 1 and failure_node_number > 1 and
                                 self.pause_between_failover_action <
                                 self.timeout or self.num_index_replicas < 1)
        failover_not_expected = failover_not_expected or (1 < self.max_count < failure_node_number and
                                                          self.pause_between_failover_action < self.timeout or
                                                          self.num_index_replicas < failure_node_number)
        return not failover_not_expected

    def gsi_multi_node_failover(self):
        servers_to_fail = self.server_to_fail
        for i in range(self.max_count):
            self.server_to_fail = [servers_to_fail[i]]
            self.failover_expected = self.is_failover_expected(i + 1)
            self.failover_actions[self.failover_action](self)

    def test_gsi_auto_failover(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=10**5)
        self._create_indexes()
        self.enable_autofailover_and_validate()
        self.sleep(5)
        if self.max_count > 1:
            self.gsi_multi_node_failover()
        else:
            self.failover_actions[self.failover_action](self)
        try:
            self.disable_autofailover_and_validate()
        except Exception as err:
            pass

    def test_failed_rebalance_with_gsi_autofailover(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=10 ** 5)
        self._create_indexes()
        # enable auto failover
        self.enable_autofailover_and_validate()
        # Start rebalance in
        rebalance_task = self.cluster.async_rebalance(servers=self.servers,
                                                      to_add=self.servers_to_add,
                                                      to_remove=self.servers_to_remove,
                                                      services=['kv', 'index'])
        self.sleep(20)
        reached = RestHelper(self.rest).rebalance_reached(percentage=20)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(20))
        # Do a fail over action - reboot, hang, kill. This is defined in the conf file. Test sometimes fail
        # because the rebalance action is completed fast and there's no way to induce a failure.
        self.failover_actions[self.failover_action](self)
        try:
            rebalance_task.result()
        except Exception as err:
            self.log.info("Rebalance failed with : {0}".format(str(err)))
            if "Rebalance failed. See logs for detailed reason. You can try again" in str(err):
                self.log.info(
                    "Rebalance failed even before auto-failover had a chance to stop it self.server_to_fail.ip: {0}".format(
                        str(err)))
            elif not RestHelper(self.rest).is_cluster_rebalanced():
                if self._auto_failover_message_present_in_logs(self.server_to_fail[0].ip):
                    self.log.info("Rebalance interrupted due to auto-failover of nodes - message was seen in logs")
                else:
                    self.fail("Rebalance interrupted message was not seen in logs")
            else:
                self.fail("Rebalance was not aborted by auto fail-over")
        self.disable_autofailover_and_validate()

    def test_autofailover_and_addback_of_node(self):
        """
        Test autofailover of nodes and then addback of the node after failover
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Addback node and validate that the addback was successful.
        4. Failover the same node again.
        :return: Nothing
        """
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        self._create_indexes()
        self.enable_autofailover_and_validate()
        self.sleep(5)
        self.failover_actions[self.failover_action](self)
        self.bring_back_failed_nodes_up()
        self.sleep(30)
        self.log.info(self.server_to_fail[0])
        self.nodes = self.rest.node_statuses()
        self.log.info(self.nodes[0].id)
        self.rest.add_back_node("ns_1@{}".format(self.server_to_fail[0].ip))
        self.rest.set_recovery_type("ns_1@{}".format(self.server_to_fail[0].ip),
                                    self.recovery_strategy)
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
        msg = "rebalance failed while recovering failover nodes {0}".format(
            self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)
        self.failover_actions[self.failover_action](self)
        try:
            self.disable_autofailover_and_validate()
        except Exception as err:
            pass
