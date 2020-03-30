import logging
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection
from random import randint

log = logging.getLogger(__name__)


class QueryHelperTests(BaseTestCase):
    def setUp(self):
        super(QueryHelperTests, self).setUp()
        self.create_primary_index = self.input.param("create_primary_index",
                                                     True)
        self.use_gsi_for_primary = self.input.param("use_gsi_for_primary",
                                                    True)
        self.use_gsi_for_secondary = self.input.param(
            "use_gsi_for_secondary", True)
        self.scan_consistency = self.input.param("scan_consistency", "request_plus")
        self.shell = RemoteMachineShellConnection(self.master)
        if not self.skip_init_check_cbserver:  # for upgrade tests
            self.buckets = RestConnection(self.master).get_buckets()
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.use_rest = self.input.param("use_rest", True)
        self.max_verify = self.input.param("max_verify", None)
        self.item_flag = self.input.param("item_flag", 0)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.dataset = self.input.param("dataset", "default")
        self.groups = self.input.param("groups", "all").split(":")
        self.doc_ops = self.input.param("doc_ops", False)
        self.batch_size = self.input.param("batch_size", 1)
        self.create_ops_per = self.input.param("create_ops_per", 0)
        self.expiry_ops_per = self.input.param("expiry_ops_per", 0)
        self.delete_ops_per = self.input.param("delete_ops_per", 0)
        self.update_ops_per = self.input.param("update_ops_per", 0)
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.full_docs_list = self.generate_full_docs_list(self.gens_load)
        self.gen_results = TuqGenerators(self.log,
                                         full_set=self.full_docs_list)
        if not self.skip_init_check_cbserver:   # for upgrade tests
            self.n1ql_server = self.get_nodes_from_services_map(
                service_type="n1ql")
        query_definition_generator = SQLDefinitionGenerator()
        if self.dataset == "default" or self.dataset == "employee":
            self.query_definitions = query_definition_generator.generate_employee_data_query_definitions()
        if self.dataset == "simple":
            self.query_definitions = query_definition_generator.generate_simple_data_query_definitions()
        if self.dataset == "sabre":
            self.query_definitions = query_definition_generator.generate_sabre_data_query_definitions()
        if self.dataset == "bigdata":
            self.query_definitions = query_definition_generator.generate_big_data_query_definitions()
        if self.dataset == "array":
            self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.num_index_replicas = self.input.param("num_index_replica", 0)

    def tearDown(self):
        super(QueryHelperTests, self).tearDown()

    def generate_docs(self, num_items, start=0):
        try:
            if self.dataset == "simple":
                return self.generate_docs_simple(num_items, start)
            if self.dataset == "array":
                return self.generate_docs_array(num_items, start)
            return getattr(self, 'generate_docs_' + self.dataset)(num_items, start)
        except Exception as ex:
            log.info(str(ex))
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_ops_docs(self, num_items, start=0):
        try:
            json_generator = JsonGenerator()
            if self.dataset == "simple":
                return self.generate_ops(num_items, start, json_generator.generate_docs_simple)
            if self.dataset == "array":
                return self.generate_ops(num_items, start, json_generator.generate_all_type_documents_for_gsi)
        except Exception as ex:
            log.info(ex)
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_docs_default(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day, start)

    def generate_docs_simple(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start=start, docs_per_day=docs_per_day)

    def generate_docs_array(self, num_items=10, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_all_type_documents_for_gsi(start=start, docs_per_day=num_items)

    def generate_ops(self, docs_per_day, start=0, method=None):
        gen_docs_map = {}
        for key in list(self.ops_dist_map.keys()):
            isShuffle = False
            if key == "update":
                isShuffle = True
            if self.dataset != "bigdata":
                gen_docs_map[key] = method(docs_per_day=self.ops_dist_map[key]["end"],
                    start=self.ops_dist_map[key]["start"])
            else:
                gen_docs_map[key] = method(value_size=self.value_size,
                    end=self.ops_dist_map[key]["end"],
                    start=self.ops_dist_map[key]["start"])
        return gen_docs_map

    def generate_full_docs_list_after_ops(self, gen_docs_map):
        docs = []
        for key in list(gen_docs_map.keys()):
            if key != "delete" and key != "expiry":
                update = False
                if key == "update":
                    update = True
                gen_docs = self.generate_full_docs_list(gens_load=gen_docs_map[key], update=update)
                for doc in gen_docs:
                    docs.append(doc)
        return docs

    def async_run_doc_ops(self):
        if self.doc_ops:
            tasks = self.async_ops_all_buckets(self.docs_gen_map, batch_size=self.batch_size)
            self.n1ql_helper.full_docs_list = self.full_docs_list_after_ops
            self.gen_results = TuqGenerators(
                full_set=self.n1ql_helper.full_docs_list)
            return tasks
        return []

    def run_doc_ops(self):
        verify_data = True
        if self.scan_consistency == "request_plus":
            verify_data = False
        if self.doc_ops:
            self.sync_ops_all_buckets(docs_gen_map=self.docs_gen_map,
                                      batch_size=self.batch_size,
                                      verify_data=verify_data)
            self.n1ql_helper.full_docs_list = self.full_docs_list_after_ops
            self.gen_results = TuqGenerators(
                full_set=self.n1ql_helper.full_docs_list)
            log.info("------ KV OPS Done ------")

    def create_index(self, bucket, query_definition, deploy_node_info=None):
        defer_build = True
        query = query_definition.generate_index_create_query(
            bucket=bucket, use_gsi_for_secondary=self.use_gsi_for_secondary,
            deploy_node_info=deploy_node_info, defer_build=defer_build, num_replica=self.num_index_replicas)
        log.info(query)
        # Define Helper Method which will be used for running n1ql queries, create index, drop index
        self.n1ql_helper = N1QLHelper(shell=self.shell,
                                      max_verify=self.max_verify,
                                      buckets=self.buckets,
                                      item_flag=self.item_flag,
                                      n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list,
                                      log=self.log, input=self.input,
                                      master=self.master,
                                      use_rest=True
                                      )
        create_index_task = self.cluster.async_create_index(
            server=self.n1ql_server, bucket=bucket, query=query,
            n1ql_helper=self.n1ql_helper,
            index_name=query_definition.index_name,
            defer_build=defer_build)
        create_index_task.result()
        query = self.n1ql_helper.gen_build_index_query(
            bucket=bucket, index_list=[query_definition.index_name])
        build_index_task = self.cluster.async_build_index(
            server=self.n1ql_server, bucket=bucket, query=query,
            n1ql_helper=self.n1ql_helper)
        build_index_task.result()
        check = self.n1ql_helper.is_index_ready_and_in_list(bucket,
                                                            query_definition.index_name,
                                                            server=self.n1ql_server)
        self.assertTrue(check, "index {0} failed to be created".format(query_definition.index_name))

    def _create_primary_index(self):
        if self.n1ql_server:
            if self.doc_ops:
                self.ops_dist_map = self.calculate_data_change_distribution(
                    create_per=self.create_ops_per, update_per=self.update_ops_per,
                    delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                    start=0, end=self.docs_per_day)
                log.info(self.ops_dist_map)
                self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
                self.full_docs_list_after_ops = self.generate_full_docs_list_after_ops(self.docs_gen_map)
            # Define Helper Method which will be used for running n1ql queries, create index, drop index
            self.n1ql_helper = N1QLHelper(shell=self.shell,
                                          max_verify=self.max_verify,
                                          buckets=self.buckets,
                                          item_flag=self.item_flag,
                                          n1ql_port=self.n1ql_port,
                                          full_docs_list=self.full_docs_list,
                                          log=self.log, input=self.input,
                                          master=self.master,
                                          use_rest=True
                                          )
            log.info(self.n1ql_server)
            if self.create_primary_index:
                self.n1ql_helper.create_primary_index(
                    using_gsi=self.use_gsi_for_primary, server=self.n1ql_server)

    def query_using_index(self, bucket, query_definition, expected_result=None, scan_consistency=None,
                          scan_vector=None, verify_results=True):
        if not scan_consistency:
            scan_consistency = self.scan_consistency
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(
                print_expected_result=False)
        query = self.gen_results.query
        log.info("Query : {0}".format(query))
        msg, check = self.n1ql_helper.run_query_and_verify_result(
                query=query, server=self.n1ql_server,
                expected_result=expected_result, scan_consistency=scan_consistency,
                scan_vector=scan_vector, verify_results=verify_results)
        self.assertTrue(check, msg)

    def drop_index(self, bucket, query_definition, verify_drop=True):
        try:
            query = query_definition.generate_index_drop_query(
                bucket=bucket,
                use_gsi_for_secondary=self.use_gsi_for_secondary,
                use_gsi_for_primary=self.use_gsi_for_primary)
            log.info(query)
            actual_result = self.n1ql_helper.run_cbq_query(query=query,
                                                           server=self.n1ql_server)
            if verify_drop:
                check = self.n1ql_helper._is_index_in_list(bucket,
                                                           query_definition.index_name,
                                                           server=self.n1ql_server)
                self.assertFalse(check, "index {0} failed to be "
                                        "deleted".format(query_definition.index_name))
        except Exception as ex:
                log.info(ex)
                query = "select * from system:indexes"
                actual_result = self.n1ql_helper.run_cbq_query(query=query,
                                                               server=self.n1ql_server)
                log.info(actual_result)

    def run_async_index_operations(self, operation_type):
        if operation_type == "create_index":
            self._create_primary_index()
            for bucket in self.buckets:
                for query_definition in self.query_definitions:
                    self.create_index(bucket=bucket, query_definition=query_definition)
        if operation_type == "query":
            for bucket in self.buckets:
                for query_definition in self.query_definitions:
                    self.query_using_index(bucket=bucket,
                               query_definition=query_definition)
        if operation_type == "drop_index":
            for bucket in self.buckets:
                for query_definition in self.query_definitions:
                    self.drop_index(bucket=bucket,
                               query_definition=query_definition)
        if operation_type == "generate_docs":
            for bucket in self.buckets:
                    self.generate_docs(self.docs_per_day, start=randint(0, 1000000000))