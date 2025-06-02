from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection


class QueryTests(BaseTestCase):
    def setUp(self):
        super(QueryTests, self).setUp()
        self.expiry = self.input.param("expiry", 0)
        self.batch_size = self.input.param("batch_size", 1)
        self.scan_consistency = self.input.param("scan_consistency", "request_plus")
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.run_async = self.input.param("run_async", True)
        self.version = self.input.param("cbq_version", "git_repo")
        for server in self.servers:
            if server.dummy:
                continue
            rest = RestConnection(server)
            temp = rest.cluster_status()
            self.log.info("Initial status of {0} cluster is {1}".format(server.ip, temp['nodes'][0]['status']))
            while temp['nodes'][0]['status'] == 'warmup':
                self.log.info("Waiting for cluster to become healthy")
                self.sleep(5)
                temp = rest.cluster_status()
            self.log.info("current status of {0}  is {1}".format(server.ip, temp['nodes'][0]['status']))

        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # Set indexer storage mode
        indexer_rest = RestConnection(indexer_node[0])
        gsi_type = self.gsi_type
        if not indexer_rest.is_enterprise_edition:
           gsi_type = "forestdb"

        doc = {"indexer.settings.storage_mode": gsi_type}
        indexer_rest.set_index_settings_internal(doc)
        self.log.info("Allowing the indexer to complete restart after setting the internal settings")
        self.sleep(5)
        doc = {"indexer.api.enableTestServer": True}
        indexer_rest.set_index_settings_internal(doc)
        self.indexer_scanTimeout = self.input.param("indexer_scanTimeout", None)
        if self.indexer_scanTimeout is not None:
            for server in indexer_node:
                rest = RestConnection(server)
                rest.set_index_settings({"indexer.settings.scan_timeout": self.indexer_scanTimeout})
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.master)
        self.use_gsi_for_primary = self.input.param("use_gsi_for_primary", True)
        self.use_gsi_for_secondary = self.input.param("use_gsi_for_secondary", True)
        self.create_primary_index = self.input.param("create_primary_index", True)
        self.use_rest = self.input.param("use_rest", True)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = RestConnection(self.master).get_buckets()
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.dataset = self.input.param("dataset", "default")
        self.value_size = self.input.param("value_size", 1024)
        self.doc_ops = self.input.param("doc_ops", False)
        self.create_ops_per = self.input.param("create_ops_per", 0)
        self.expiry_ops_per = self.input.param("expiry_ops_per", 0)
        self.delete_ops_per = self.input.param("delete_ops_per", 0)
        self.update_ops_per = self.input.param("update_ops_per", 0)
        self.num_index_replica = self.input.param("num_index_replica", 0)
        if self.input.param("gomaxprocs", None):
            self.n1ql_helper.configure_gomaxprocs()

        verify_data = False
        if self.scan_consistency != "request_plus":
            verify_data = True
        if not self.skip_load:
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.full_docs_list = self.generate_full_docs_list(self.gens_load)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            # Add sleep to allow cluster to stabilize
            self.sleep(120)
            self.load(self.gens_load, flag=self.item_flag, verify_data=verify_data, batch_size=100)
            if self.doc_ops:
                self.ops_dist_map = self.calculate_data_change_distribution(
                    create_per=self.create_ops_per, update_per=self.update_ops_per,
                    delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                    start=0, end=self.docs_per_day)
                self.log.info(self.ops_dist_map)
                self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
                self.full_docs_list_after_ops = self.generate_full_docs_list_after_ops(self.docs_gen_map)
        else:
            self.full_docs_list = None
        # Define Helper Method which will be used for running n1ql queries, create index, drop index
        self.n1ql_helper = N1QLHelper(version=self.version, shell=self.shell,
                                      use_rest=self.use_rest, max_verify=self.max_verify,
                                      buckets=self.buckets, item_flag=self.item_flag,
                                      n1ql_port=self.n1ql_port, full_docs_list=self.full_docs_list,
                                      log=self.log, input=self.input, master=self.master)
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.log.info(self.n1ql_node)
        # self.n1ql_helper._start_command_line_query(self.n1ql_node)

        # sleep to avoid race condition during bootstrap -- pretty evident in CE
        self.sleep(30)

        if self.create_primary_index:
            try:
                self.n1ql_helper.create_primary_index(using_gsi=self.use_gsi_for_primary,
                                                      server=self.n1ql_node, num_index_replica=self.num_index_replica)
            except Exception as ex:
                self.log.info(ex)
                raise ex

    def tearDown(self):
        if not self.capella_run:
            self.check_gsi_logs_for_panic()
        if hasattr(self, 'n1ql_helper'):
            if hasattr(self, 'skip_cleanup') and not self.skip_cleanup:
                self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
                self.n1ql_helper.drop_primary_index(using_gsi=self.use_gsi_for_primary, server=self.n1ql_node)
        # Do not kill indexer and other processes during teardown.
        #if hasattr(self, 'shell') and self.shell is not None:
            #if not self.skip_cleanup:
                #self.n1ql_helper._restart_indexer()
                #self.n1ql_helper.killall_tuq_process()
        super(QueryTests, self).tearDown()

    def run_query_with_retry(self, query, expected_result=None, is_count_query=False, delay=5, tries=10):
        attempts = 0
        res = None
        while attempts < tries:
            attempts = attempts + 1
            try:
                res = self.n1ql_helper.run_cbq_query(query=query)['results']
                if expected_result is None:
                    return res
                elif is_count_query and res[0]['$1'] == expected_result:
                    return res[0]['$1']
                elif res == expected_result:
                    return res
                else:
                    self.sleep(delay, f'incorrect results, sleeping for {delay}')
            except Exception as ex:
                raise Exception(f'exception returned: {ex}')
        return res

    def generate_docs(self, num_items, start=0):
        try:
            if self.dataset == "simple":
                return self.generate_docs_simple(num_items, start)
            if self.dataset == "sales":
                return self.generate_docs_sales(num_items, start)
            if self.dataset == "bigdata":
                return self.generate_docs_bigdata(num_items, start)
            if self.dataset == "sabre":
                return self.generate_docs_sabre(num_items, start)
            if self.dataset == "array":
                return self.generate_docs_array(num_items, start)
            return getattr(self, 'generate_docs_' + self.dataset)(num_items, start)

        except Exception as ex:
            self.log.info(str(ex))
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_ops_docs(self, num_items, start=0):
        try:
            json_generator = JsonGenerator()
            if self.dataset == "simple":
                return self.generate_ops(num_items, start, json_generator.generate_docs_simple)
            if self.dataset == "sales":
                return self.generate_ops(num_items, start, json_generator.generate_docs_sales)
            if self.dataset == "employee" or self.dataset == "default":
                return self.generate_ops(num_items, start, json_generator.generate_docs_employee)
            if self.dataset == "sabre":
                return self.generate_ops(num_items, start, json_generator.generate_docs_sabre)
            if self.dataset == "bigdata":
                return self.generate_ops(num_items, start, json_generator.generate_docs_bigdata)
            if self.dataset == "array":
                return self.generate_ops(num_items, start, json_generator.generate_all_type_documents_for_gsi)
        except Exception as ex:
            self.log.info(ex)
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_docs_default(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day, start)

    def generate_docs_sabre(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sabre(docs_per_day, start)

    def generate_docs_employee(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day=docs_per_day, start=start)

    def generate_docs_simple(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start=start, docs_per_day=docs_per_day)

    def generate_docs_sales(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sales(docs_per_day=docs_per_day, start=start)

    def generate_docs_bigdata(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(docs_per_day=docs_per_day,
                                                    start=start, value_size=self.value_size)

    def generate_docs_array(self, num_items=10, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_all_type_documents_for_gsi(docs_per_day=num_items,
                                                                  start=start)

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
            self.gen_results = TuqGenerators(self.log, self.n1ql_helper.full_docs_list)
            self.log.info("------ KV OPS Done ------")
            return tasks
        return []

    def run_doc_ops(self):
        verify_data = True
        if self.scan_consistency == "request_plus":
            verify_data = False
        if self.doc_ops:
            self.sync_ops_all_buckets(docs_gen_map=self.docs_gen_map, batch_size=self.batch_size,
                                      verify_data=verify_data)
            self.n1ql_helper.full_docs_list = self.full_docs_list_after_ops
            self.gen_results = TuqGenerators(self.log, self.n1ql_helper.full_docs_list)

    def check_gsi_logs_for_panic(self):
        """ Checks for panics/other errors in indexer and projector logs
        """
        self.generate_map_nodes_out_dist()
        panic_str, fail_test, vbs_out_of_range_str = "panic", False, "NumVbs out of valid range"
        indexers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        strings_to_monitor = [panic_str, vbs_out_of_range_str]
        if not indexers:
            return None
        for server in indexers:
            if server not in self.nodes_out_list:
                shell = RemoteMachineShellConnection(server)
                _, dir = RestConnection(server).diag_eval(
                    'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
                indexer_log = str(dir) + '/indexer.log*'
                for string in strings_to_monitor:
                    count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                                       format(string, indexer_log))
                    if isinstance(count, list):
                        count = int(count[0])
                    else:
                        count = int(count)
                    if count > 0:
                        self.log.info("===== {0} OBSERVED IN INDEXER LOGS ON SERVER {1}=====".format(string, server.ip))
                        fail_test = True
                shell.disconnect()
        projectors = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        if not projectors:
            return None
        for server in projectors:
            if server not in self.nodes_out_list:
                shell = RemoteMachineShellConnection(server)
                _, dir = RestConnection(server).diag_eval(
                    'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
                projector_log = str(dir) + '/projector.log*'
                count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                                   format(panic_str, projector_log))
                if isinstance(count, list):
                    count = int(count[0])
                else:
                    count = int(count)
                shell.disconnect()
                if count > 0:
                    self.log.info("===== PANIC OBSERVED IN PROJECTOR LOGS ON SERVER {0}=====".format(server.ip))
                    fail_test = True
        if fail_test:
            raise Exception("Panic seen in projector/indexer")
