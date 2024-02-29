import logging
from .base_gsi import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection


log = logging.getLogger(__name__)
class SecondaryIndexingCreateDropTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingCreateDropTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingCreateDropTests, self).tearDown()

    def test_multi_create_drop_index(self):
        if self.run_async:
            tasks = self.async_run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = False)
            for task in tasks:
                task.result()
            tasks = self.async_run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = False, drop_index = True)
            for task in tasks:
                task.result()
        else:
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = True)

    def test_create_index_on_empty_bucket(self):
        """
        Fix for MB-15329
        Create indexes on empty buckets
        :return:
        """
        rest = RestConnection(self.master)
        for bucket in self.buckets:
            log.info("Flushing bucket {0}...".format(bucket.name))
            rest.flush_bucket(bucket)
        self.sleep(30)
        self.multi_create_index(buckets=self.buckets, query_definitions=self.query_definitions)
        self._verify_bucket_count_with_index_count()

    def test_deployment_plan_with_defer_build_plan_create_drop_index(self):
        self.run_async = True
        self.test_multi_create_drop_index()

    def test_deployment_plan_with_nodes_only_plan_create_drop_index_for_secondary_index(self):
        query_definitions = []
        tasks = []
        verification_map ={}
        query_definition_map ={}
        servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        try:
            servers.reverse()
            for bucket in self.buckets:
                query_definition_map[bucket.name] =[]
                for server in servers:
                    index_name = "index_name_ip_{0}_port_{1}_{2}".format(server.ip.replace(".", "_"), self.node_port, bucket.name)
                    query_definition = QueryDefinition(index_name=index_name, index_fields=["join_yr"],
                                                       query_template="", groups=[])
                    query_definition_map[bucket.name].append(query_definition)
                    query_definitions.append(query_definition)
                    node_key = "{0}:{1}".format(server.ip, self.node_port)
                    deploy_node_info = [node_key]
                    if node_key not in list(verification_map.keys()):
                        verification_map[node_key] = {}
                    verification_map[node_key][bucket.name]=index_name
                    tasks.append(self.async_create_index(bucket.name, query_definition, deploy_node_info = deploy_node_info))
                for task in tasks:
                    task.result()
            index_map = self.get_index_stats(perNode=True)
            self.log.info(index_map)
            for bucket in self.buckets:
                for node in list(index_map.keys()):
                    ip, port = node.split(':')
                    verification_node = f'{ip}:{self.node_port}'
                    self.log.info(" verifying node {0}".format(node))
                    self.assertTrue(verification_map[verification_node][bucket.name] in list(index_map[node][bucket.name].keys()), \
                        "for bucket {0} and node {1}, could not find key {2} in {3}".format(bucket.name, verification_node, verification_map[verification_node][bucket.name], index_map))
        except Exception as ex:
            self.log.info(ex)
            raise
        finally:
            for bucket in self.buckets:
                self.log.info("<<<<<<<<<<<< drop index {0} >>>>>>>>>>>".format(bucket.name))
                self.run_multi_operations(buckets = [bucket], query_definitions = query_definition_map[bucket.name], drop_index = True)

    def test_fail_deployment_plan_defer_build_same_name_index(self):
        query_definitions = []
        tasks = []
        index_name = "test_deployment_plan_defer_build_same_name_index"
        servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        try:
            servers.reverse()
            for server in servers:
                self.defer_build=True
                query_definition = QueryDefinition(index_name=index_name, index_fields=["join_yr"], query_template="",
                                                   groups=[])
                query_definitions.append(query_definition)
                deploy_node_info = ["{0}:{1}".format(server.ip, self.node_port)]
                tasks.append(self.async_create_index(self.buckets[0], query_definition, deploy_node_info = deploy_node_info))
            for task in tasks:
                task.result()
        except Exception as ex:
            msg =  "index test_deployment_plan_defer_build_same_name_index already exist"
            self.assertTrue(msg in str(ex), ex)

    def test_concurrent_deployment_plan_defer_build_different_name_index(self):
        query_definitions = []
        tasks = []
        self.defer_build = True
        index_name = "test_concurrent_deployment_plan_defer_build_different_name_index"
        servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        try:
            servers.reverse()
            for server in servers:
                self.defer_build=True
                for index in range(0, 10):
                    query_definition = QueryDefinition(index_name=index_name + "_" + str(index),
                                                       index_fields=["join_yr"], query_template="", groups=[])
                    query_definitions.append(query_definition)
                    deploy_node_info = ["{0}:{1}".format(server.ip, self.node_port)]
                    tasks.append(self.async_create_index(self.buckets[0], query_definition, deploy_node_info = deploy_node_info))
            for task in tasks:
                task.result()
            index_list = [query_definition.index_name for query_definition in query_definitions]
            task = self.async_build_index(bucket = "default", index_list = index_list)
            task.result()
            tasks = []
            for index_name in index_list:
                tasks.append(self.async_monitor_index(bucket = "default", index_name = index_name))
            for task in tasks:
                task.result()
        except Exception as ex:
            msg =  "Index test_deployment_plan_defer_build_same_name_index already exist"
            self.assertTrue(msg in str(ex), ex)

    def test_failure_concurrent_create_index(self):
        try:
            self.run_async = True
            tasks = []
            for query_definition in self.query_definitions:
                tasks.append(self.async_create_index(self.buckets[0].name, query_definition))
            for task in tasks:
                task.result()
            self.assertTrue(False, " Created indexes concurrently, should have failed! ")
        except Exception as ex:
            msg = "Build Already In Progress"
            self.assertTrue(msg in str(ex), ex)

    def test_deployment_plan_with_nodes_only_plan_create_drop_index_for_primary_index(self):
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.query = "DROP PRIMARY INDEX ON {0} using gsi".format(self.buckets[0].name)
        try:
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            self.log.info(ex)
        query_definitions = []
        servers = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
        deploy_node_info = ["{0}:{1}".format(servers[0].ip, self.node_port)]
        self.query = "CREATE PRIMARY INDEX ON {0} using gsi".format(self.buckets[0].name)
        deployment_plan = {}
        if deploy_node_info  != None:
            deployment_plan["nodes"] = deploy_node_info
        if self.defer_build != None:
            deployment_plan["defer_build"] = self.defer_build
        if len(deployment_plan) != 0:
            self.query += " WITH "+ str(deployment_plan)
        try:
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
            if self.defer_build:
                build_index_task = self.async_build_index(self.buckets[0], ["`#primary`"])
                build_index_task.result()
            check = self.n1ql_helper.is_index_online_and_in_list(self.buckets[0], "#primary", server = server)
            self.assertTrue(check, "index primary failed to be created")

            self.query = "DROP PRIMARY INDEX ON {0} using gsi".format(self.buckets[0].name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            self.log.info(ex)
            raise

    def test_create_primary_using_views_with_existing_primary_index_gsi(self):
        query_definition = QueryDefinition(
            index_name="test_failure_create_primary_using_views_with_existing_primary_index_gsi", index_fields="crap",
            query_template="", groups=[])
        check = False
        self.query = "CREATE PRIMARY INDEX ON {0} USING VIEW".format(self.buckets[0].name)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            self.log.info(ex)
            raise

    def test_create_primary_using_gsi_with_existing_primary_index_views(self):
        query_definition = QueryDefinition(
            index_name="test_failure_create_primary_using_gsi_with_existing_primary_index_views", index_fields="crap",
            query_template="", groups=[])
        check = False
        self.query = "CREATE PRIMARY INDEX ON {0} USING GSI".format(self.buckets[0].name)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            self.log.info(ex)
            raise

    def test_create_gsi_index_existing_view_index(self):
        self.indexes= self.input.param("indexes", "").split(":")
        query_definition = QueryDefinition(index_name="test_failure_create_index_existing_index",
                                           index_fields=self.indexes, query_template="", groups=[])
        self.query = query_definition.generate_index_create_query(namespace=self.buckets[0].name,
                                                                  use_gsi_for_secondary=False, gsi_type=self.gsi_type)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
            # create same index again
            self.query = query_definition.generate_index_create_query(namespace=self.buckets[0].name,
                                                                      use_gsi_for_secondary=True,
                                                                      gsi_type=self.gsi_type)
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            self.log.info(ex)
            raise
        finally:
            self.query = query_definition.generate_index_drop_query(namespace=self.buckets[0].name)
            actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)

    def test_failure_create_index_big_fields(self):
        field_name = "_".join([str(a) for a in range(1, 100)])
        query_definition = QueryDefinition(index_name="test_failure_create_index_existing_index",
                                           index_fields=field_name, query_template="", groups=[])
        self.query = query_definition.generate_index_create_query(namespace=self.buckets[0], gsi_type=self.gsi_type)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            msg = "not indexable"
            self.assertTrue(msg in str(ex), " 5000 error not received as expected {0}".format(ex))

    def test_create_gsi_index_without_primary_index(self):
        self.indexes= self.input.param("indexes", "").split(":")
        query_definition = QueryDefinition(index_name="test_failure_create_index_existing_index",
                                           index_fields=self.indexes, query_template="", groups=[])
        self.query = query_definition.generate_index_create_query(namespace=self.buckets[0].name,
                                                                  gsi_type=self.gsi_type)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            msg="Keyspace not_present_bucket name not found - cause: Bucket not_present_bucket not found"
            self.assertTrue(msg in str(ex),
                " 5000 error not received as expected {0}".format(ex))

    def test_failure_create_index_non_existing_bucket(self):
        self.indexes= self.input.param("indexes", "").split(":")
        query_definition = QueryDefinition(index_name="test_failure_create_index_existing_index",
                                           index_fields=self.indexes, query_template="", groups=[])
        self.query = query_definition.generate_index_create_query(namespace="not_present_bucket",
                                                                  gsi_type=self.gsi_type)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            msg="Keyspace not found in CB datastore: default:not_present_bucket - cause: No bucket named not_present_bucket"
            self.assertTrue(msg in str(ex),
                " 12003 error not received as expected {0}".format(ex))

    def test_failure_drop_index_non_existing_bucket(self):
        query_definition = QueryDefinition(index_name="test_failure_create_index_existing_index", index_fields="crap",
                                           query_template="", groups=[])
        self.query = query_definition.generate_index_drop_query(namespace="not_present_bucket")
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            msg="Keyspace not found in CB datastore: default:not_present_bucket - cause: No bucket named not_present_bucket"
            self.assertTrue(msg in str(ex),
                " 12003 error not received as expected {0}".format(ex))

    def test_failure_drop_index_non_existing_index(self):
        query_definition = QueryDefinition(index_name="test_failure_create_index_existing_index", index_fields="crap",
                                           query_template="", groups=[])
        self.query = query_definition.generate_index_drop_query(namespace=self.buckets[0].name)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            msg="GSI index test_failure_create_index_existing_index not found"
            self.assertTrue(msg in str(ex),
                " 5000 error not received as expected {0}".format(ex))

    def test_failure_create_index_existing_index(self):
        self.indexes= self.input.param("indexes", "").split(":")
        query_definition = QueryDefinition(index_name="test_failure_create_index_existing_index",
                                           index_fields=self.indexes, query_template="", groups=[])
        self.query = query_definition.generate_index_create_query(namespace=self.buckets[0].name,
                                                                  gsi_type=self.gsi_type)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
            # create same index again
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception as ex:
            self.assertTrue("index test_failure_create_index_existing_index already exist" in str(ex),
                " 5000 error not received as expected {0}".format(ex))
        finally:
            self.query = query_definition.generate_index_drop_query(namespace=self.buckets[0].name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)

    def test_fail_create_kv_node_down(self):
        servr_out =[]
        servr_out = self.get_nodes_from_services_map(service_type = "kv", get_all_nodes = True)
        node_out = servr_out[1]
        if servr_out[1] == self.servers[0]:
            node_out = servr_out[0]
        remote = RemoteMachineShellConnection(node_out)
        remote.stop_server()
        self.sleep(10)
        index_name = self.query_definitions[0].index_name
        self.query = self.query_definitions[0].generate_index_create_query(namespace=self.buckets[0].name,
                                                                           gsi_type=self.gsi_type)
        try:
            res = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.log.info(res)
        except Exception as ex:
            msg = "cause: Encountered transient error.  Index creation will be retried in background."
            self.log.info(ex)
            self.assertTrue(msg in str(ex), ex)
        finally:
            remote = RemoteMachineShellConnection(node_out)
            remote.start_server()

    def test_fail_drop_index_node_down(self):
        try:
            self.run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions, create_index = True, drop_index = False)
            servr_out = self.get_nodes_from_services_map(service_type = "index", get_all_nodes = True)
            failover_task = self.cluster.async_failover([self.master],
                    failover_nodes = servr_out, graceful=self.graceful)
            failover_task.result()
            self.sleep(10)
            self.query = self.query_definitions[0].generate_index_drop_query(namespace=self.buckets[0].name,
                                                                             use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                             use_gsi_for_primary=self.use_gsi_for_primary)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.log.info(" non-existant indexes cannot be dropped ")
        except Exception as ex:
            self.log.info(ex)
            msg = "GSI index {0} not found".format(self.query_definitions[0].index_name)
            self.assertTrue(msg in str(ex), ex)

    def test_delete_bucket_while_index_build(self):
        create_index_task = []
        index_list = []
        self.defer_build=True
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                create_index_task.append(self.async_create_index(bucket.name, query_definition))
                index_list.append(query_definition.index_name)
        for task in create_index_task:
            task.result()
        try:
            for bucket in self.buckets:
                build_task = self.async_build_index(bucket, index_list)
                log.info("Deleting bucket {0}".format(bucket.name))
                BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.master, bucket=bucket.name)
                build_task.result()
        except Exception as ex:
            msg = "Keyspace not found in CB datastore: default:default - cause: No bucket named default"
            self.assertIn(msg, str(ex), str(ex))
            log.info("Error while building index Expected...")

    def test_ambiguity_in_gsi_indexes_due_to_node_down(self):
        servr_out = self.get_nodes_from_services_map(service_type = "index")
        query_definitions = []
        tasks = []
        try:
            query_definition = QueryDefinition(index_name="test_ambiguity_in_gsi_indexes_due_to_node_down",
                                               index_fields=["join_yr"],
                                               query_template="SELECT * from %s WHERE join_yr > 1999", groups=[])
            query_definitions.append(query_definition)
            deploy_node_info = ["{0}:{1}".format(servr_out.ip, servr_out.port)]
            task = self.async_create_index(self.buckets[0].name, query_definition, deploy_node_info = deploy_node_info)
            task.result()
            remote = RemoteMachineShellConnection(servr_out)
            remote.stop_server()
            self.sleep(10)
            task = self.async_create_index(self.buckets[0].name, query_definition)
            task.result()
            self.assertTrue(False, "Duplicate index should not be allowed when index node is down")
        except Exception as ex:
            self.log.info(ex)
            remote = RemoteMachineShellConnection(servr_out)
            remote.start_server()
            self.assertTrue("Index test_ambiguity_in_gsi_indexes_due_to_node_down already exist" in str(ex), ex)
