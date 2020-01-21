from logredaction.log_redaction_base import LogRedactionBase
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from couchbase_helper.tuq_generators import JsonGenerator
from couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
import json
import logging

log = logging.getLogger()

class LogRedactionTests(LogRedactionBase):
    def setUp(self):
        super(LogRedactionTests, self).setUp()
        self.doc_per_day = self.input.param("doc-per-day", 100)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.interrupt_replication = self.input.param("interrupt-replication", False)

    def tearDown(self):
        super(LogRedactionTests, self).tearDown()

    def test_enabling_redaction(self):
        self.set_redaction_level()

    def test_cbcollect_with_redaction_enabled(self):
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/')+1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)

    def test_cbcollect_with_redaction_disabled(self):
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        nonredactFileName = logs_path.split('/')[-1]
        remotepath = logs_path[0:logs_path.rfind('/')+1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    nonredactFileName=nonredactFileName)

    def test_ns_server_with_redaction_enabled(self):
        #load bucket and do some ops
        gen_create = BlobGenerator('logredac', 'logredac-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)

        gen_delete = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items // 2, end=self.num_items)
        gen_update = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items + 1,
                                   end=self.num_items * 3 // 2)

        self._load_all_buckets(self.master, gen_delete, "create", 0)
        self._load_all_buckets(self.master, gen_update, "create", 0)

        #set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.debug.log")

    def test_ns_server_with_rebalance_failover_with_redaction_enabled(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        rest = RestConnection(self.master)
        # load bucket and do some ops
        gen_create = BlobGenerator('logredac', 'logredac-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)
        gen_delete = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items // 2,
                                   end=self.num_items)
        gen_update = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items + 1,
                                   end=self.num_items * 3 // 2)
        self._load_all_buckets(self.master, gen_delete, "create", 0)
        self._load_all_buckets(self.master, gen_update, "create", 0)
        # set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        services_in = ["kv"]
        to_add_nodes = [self.servers[self.nodes_init]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        # failover a node
        server_failed_over = self.servers[self.nodes_init]
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[server_failed_over], graceful=True)
        fail_over_task.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [server_failed_over])
        reached = RestHelper(rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        result = self.monitor_logs_collection()
        log.info(result)
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.debug.log")

##############################################################################################
#
#   GSI
##############################################################################################

    def set_indexer_logLevel(self, loglevel="info"):
        """
        :param loglevel:
        Possible Values
            -- info
            -- debug
            -- warn
            -- verbose
            -- Silent
            -- Fatal
            -- Error
            -- Timing
            -- Trace
        """
        self.log.info("Setting indexer log level to {0}".format(loglevel))
        server = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(server)
        status = rest.set_indexer_params("logLevel", loglevel)

    def set_projector_logLevel(self, loglevel="info"):
        """
        :param loglevel:
        Possible Values
            -- info
            -- debug
            -- warn
            -- verbose
            -- Silent
            -- Fatal
            -- Error
            -- Timing
            -- Trace
        """
        self.log.info("Setting indexer log level to {0}".format(loglevel))
        server = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(server)
        proj_settings = {"projector.settings.log_level": loglevel}
        status = rest.set_index_settings(proj_settings)

    def test_gsi_with_crud_with_redaction_enabled(self):
        # load bucket and do some ops
        self.set_indexer_logLevel("trace")
        self.set_projector_logLevel("trace")
        json_generator = JsonGenerator()
        gen_docs = json_generator.generate_all_type_documents_for_gsi(docs_per_day=self.doc_per_day, start=0)
        full_docs_list = self.generate_full_docs_list(gen_docs)
        n1ql_helper = N1QLHelper(use_rest=True, buckets=self.buckets, full_docs_list=full_docs_list,
                                      log=log, input=self.input, master=self.master)
        self.load(gen_docs)
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        query_definition_generator = SQLDefinitionGenerator()
        n1ql_helper.create_primary_index(using_gsi=True, server=n1ql_node)
        query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
        query_definitions = query_definition_generator.filter_by_group(["simple"], query_definitions)
        # set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        # Create partial Index
        for query_definition in query_definitions:
            for bucket in self.buckets:
                create_query = query_definition.generate_index_create_query(bucket.name)
                n1ql_helper.run_cbq_query(query=create_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                scan_query = query_definition.generate_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=scan_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                drop_query = query_definition.generate_index_drop_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=drop_query, server=n1ql_node)
        result = self.monitor_logs_collection()
        log.info(result)
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        log_file = self.input.param("log_file_name", "indexer.log")
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.{0}".format(log_file))

    def test_gsi_with_flush_bucket_redaction_enabled(self):
        # load bucket and do some ops
        self.set_indexer_logLevel("trace")
        self.set_projector_logLevel("trace")
        json_generator = JsonGenerator()
        gen_docs = json_generator.generate_all_type_documents_for_gsi(docs_per_day=self.doc_per_day, start=0)
        full_docs_list = self.generate_full_docs_list(gen_docs)
        n1ql_helper = N1QLHelper(use_rest=True, buckets=self.buckets, full_docs_list=full_docs_list,
                                      log=log, input=self.input, master=self.master)
        self.load(gen_docs)
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        n1ql_helper.create_primary_index(using_gsi=True, server=n1ql_node)
        query_definition_generator = SQLDefinitionGenerator()
        query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
        query_definitions = query_definition_generator.filter_by_group(["simple"], query_definitions)
        # set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        # Create partial Index
        for query_definition in query_definitions:
            for bucket in self.buckets:
                create_query = query_definition.generate_index_create_query(bucket.name)
                n1ql_helper.run_cbq_query(query=create_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                scan_query = query_definition.generate_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=scan_query, server=n1ql_node)

        rest = RestConnection(self.master)
        rest.flush_bucket(self.buckets[0].name)

        self.sleep(100)
        self.load(gen_docs, buckets=[self.buckets[0]])

        for query_definition in query_definitions:
            for bucket in self.buckets:
                scan_query = query_definition.generate_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=scan_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                drop_query = query_definition.generate_index_drop_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=drop_query, server=n1ql_node)
        result = self.monitor_logs_collection()
        log.info(result)
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        log_file = self.input.param("log_file_name", "indexer.log")
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.{0}".format(log_file))

    def test_gsi_with_index_restart_redaction_enabled(self):
        # load bucket and do some ops
        self.set_indexer_logLevel("trace")
        self.set_projector_logLevel("trace")
        json_generator = JsonGenerator()
        gen_docs = json_generator.generate_all_type_documents_for_gsi(docs_per_day=self.doc_per_day, start=0)
        full_docs_list = self.generate_full_docs_list(gen_docs)
        n1ql_helper = N1QLHelper(use_rest=True, buckets=self.buckets, full_docs_list=full_docs_list,
                                      log=log, input=self.input, master=self.master)
        self.load(gen_docs)
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        n1ql_helper.create_primary_index(using_gsi=True, server=n1ql_node)
        query_definition_generator = SQLDefinitionGenerator()
        query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
        query_definitions = query_definition_generator.filter_by_group(["simple"], query_definitions)
        # set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        # Create partial Index
        for query_definition in query_definitions:
            for bucket in self.buckets:
                create_query = query_definition.generate_index_create_query(bucket.name)
                n1ql_helper.run_cbq_query(query=create_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                scan_query = query_definition.generate_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=scan_query, server=n1ql_node)

        index_node = self.get_nodes_from_services_map(service_type="index")
        remote = RemoteMachineShellConnection(index_node)
        remote.stop_server()
        self.sleep(30)
        remote.start_server()
        self.sleep(30)
        for query_definition in query_definitions:
            for bucket in self.buckets:
                scan_query = query_definition.generate_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=scan_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                drop_query = query_definition.generate_index_drop_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=drop_query, server=n1ql_node)
        result = self.monitor_logs_collection()
        log.info(result)
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        log_file = self.input.param("log_file_name", "indexer.log")
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.{0}".format(log_file))

    def test_gsi_with_index_rebalance_redaction_enabled(self):
        # load bucket and do some ops
        self.set_indexer_logLevel("trace")
        self.set_projector_logLevel("trace")
        json_generator = JsonGenerator()
        gen_docs = json_generator.generate_all_type_documents_for_gsi(docs_per_day=self.doc_per_day, start=0)
        full_docs_list = self.generate_full_docs_list(gen_docs)
        n1ql_helper = N1QLHelper(use_rest=True, buckets=self.buckets, full_docs_list=full_docs_list,
                                 log=log, input=self.input, master=self.master)
        self.load(gen_docs)
        self.find_nodes_in_list()
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        n1ql_helper.create_primary_index(using_gsi=True, server=n1ql_node)
        query_definition_generator = SQLDefinitionGenerator()
        query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
        query_definitions = query_definition_generator.filter_by_group(["simple"], query_definitions)
        # set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        # Create partial Index
        for query_definition in query_definitions:
            for bucket in self.buckets:
                create_query = query_definition.generate_index_create_query(bucket.name)
                n1ql_helper.run_cbq_query(query=create_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                scan_query = query_definition.generate_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=scan_query, server=n1ql_node)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list,
                                                 [], services=self.services_in)

        rebalance.result()
        self.sleep(30)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                scan_query = query_definition.generate_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=scan_query, server=n1ql_node)

        for query_definition in query_definitions:
            for bucket in self.buckets:
                drop_query = query_definition.generate_index_drop_query(bucket=bucket.name)
                n1ql_helper.run_cbq_query(query=drop_query, server=n1ql_node)
        result = self.monitor_logs_collection()
        log.info(result)
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        log_file = self.input.param("log_file_name", "indexer.log")
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.{0}".format(log_file))

    def test_cbcollect_with_redaction_enabled_with_views(self):
        self.set_redaction_level()
        self._create_views()
        """ start collect logs """
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/')+1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)

    def test_cbcollect_with_redaction_enabled_with_xdcr(self):
        rest_src = RestConnection(self.master)
        rest_src.remove_all_replications()
        rest_src.remove_all_remote_clusters()

        rest_dest = RestConnection(self.servers[1])
        rest_dest_helper = RestHelper(rest_dest)

        try:
            rest_src.remove_all_replications()
            rest_src.remove_all_remote_clusters()
            self.set_redaction_level()
            rest_src.add_remote_cluster(self.servers[1].ip, self.servers[1].port,
                                        self.servers[1].rest_username,
                                        self.servers[1].rest_password, "C2")

            """ at dest cluster """
            self.add_built_in_server_user(node=self.servers[1])
            rest_dest.create_bucket(bucket='default', ramQuotaMB=512)
            bucket_ready = rest_dest_helper.vbucket_map_ready('default')
            if not bucket_ready:
                self.fail("Bucket default at dest not created after 120 seconds.")
            repl_id = rest_src.start_replication('continuous', 'default', "C2")
            if repl_id is not None:
                self.log.info("Replication created successfully")
            gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
            tasks = self._async_load_all_buckets(self.master, gen, "create", 0)
            for task in tasks:
                task.result()
            self.sleep(10)

            """ enable firewall """
            if self.interrupt_replication:
                RemoteUtilHelper.enable_firewall(self.master, xdcr=True)

            """ start collect logs """
            self.start_logs_collection()
            result = self.monitor_logs_collection()
            """ verify logs """
            try:
                logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
            except KeyError:
                logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
            redactFileName = logs_path.split('/')[-1]
            nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
            remotepath = logs_path[0:logs_path.rfind('/')+1]
            self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
            self.log.info("Verify on log ns_server.goxdcr.log")
            self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.goxdcr.log")
        finally:
            """ clean up xdcr """
            if self.interrupt_replication:
                shell = RemoteMachineShellConnection(self.master)
                shell.disable_firewall()
                shell.disconnect()
            rest_dest.delete_bucket()
            rest_src.remove_all_replications()
            rest_src.remove_all_remote_clusters()

##############################################################################################
#
#   N1QL
##############################################################################################

    def test_n1ql_through_rest_with_redaction_enabled(self):
        gen_create = BlobGenerator('logredac', 'logredac-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        curl_path = "curl"
        if type.lower() == 'windows':
            self.curl_path = "%scurl" % self.path

        shell.execute_command("%s -u Administrator:password http://%s:%s/query/service -d 'statement=create primary index on default'"
                              % (curl_path, self.master.ip, self.n1ql_port))

        shell.execute_command("%s -u Administrator:password http://%s:%s/query/service -d 'statement=create index idx on default(fake)'"
                              % (curl_path, self.master.ip, self.n1ql_port))

        shell.execute_command("%s -u Administr:pasword http://%s:%s/query/service -d 'statement=select * from default'"
                              % (curl_path, self.master.ip, self.n1ql_port))

        shell.execute_command("%s http://Administrator:password@%s:%s/query/service -d 'statement=select * from default'"
                              % (curl_path, self.master.ip, self.n1ql_port))

        shell.execute_command("%s -u Administrator:password http://%s:%s/query/service -d 'statement=select * from default'"
                              % (curl_path, self.master.ip, self.n1ql_port))

        # Get the CAS mismatch error by double inserting a document, second one will throw desired error
        shell.execute_command("%s -u Administrator:password http://%s:%s/query/service -d 'statement=insert into default (KEY,VALUE) VALUES(\"test\",{\"field1\":\"test\"})'"
                              % (curl_path, self.master.ip, self.n1ql_port))

        shell.execute_command("%s -u Administrator:password http://%s:%s/query/service -d 'statement=insert into default (KEY,VALUE) VALUES(\"test\",{\"field1\":\"test\"})'"
                              % (curl_path, self.master.ip, self.n1ql_port))

        # Delete a document that does not exist
        shell.execute_command("%s -u Administrator:password http://%s:%s/query/service -d 'statement=DELETE FROM default USE KEYS \"fakekey\"})'"
                              % (curl_path, self.master.ip, self.n1ql_port))


        #set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/')+1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.query.log")
        shell.disconnect()

    '''Convert output of remote_util.execute_command to json
       (stripping all white space to match execute_command_inside output)'''
    def convert_list_to_json(self, output_of_curl):
        new_list = [string.replace(" ", "") for string in output_of_curl]
        concat_string = ''.join(new_list)
        json_output=json.loads(concat_string)
        return json_output

    def test_fts_log_redaction(self):
        gen_create = BlobGenerator('logredac', 'logredac-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)
        index_definition = {
            "type": "fulltext-index",
            "name": "index1",
            "sourceType": "couchbase",
            "sourceName": "default"
        }
        rest = RestConnection(self.master)
        status = rest.create_fts_index("index1", index_definition)
        if status:
            log.info("Index 'index1' created")
        else:
            log.info("Error creating index, status = {0}".format(status))
        self.sleep(60, "waiting for docs to get indexed")
        query_json = {"query": {"field": "type", "match": "emp"}}
        hits, _, _, _ = rest.run_fts_query(index_name="index1",
                           query_json=query_json)
        log.info("Hits from query {0}: {1}".format(query_json, hits))
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        try:
            logs_path = result["perNode"]["ns_1@" + str(self.master.ip)]["path"]
        except KeyError:
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.fts.log")
