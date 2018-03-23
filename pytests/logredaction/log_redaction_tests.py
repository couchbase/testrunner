from logredaction.log_redaction_base import LogRedactionBase
from couchbase_helper.documentgenerator import BlobGenerator
from lib.membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
import logging

log = logging.getLogger()

class LogRedactionTests(LogRedactionBase):
    def setUp(self):
        super(LogRedactionTests, self).setUp()

    def tearDown(self):
        super(LogRedactionTests, self).tearDown()

    def test_enabling_redaction(self):
        self.set_redaction_level()

    def test_cbcollect_with_redaction_enabled(self):
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
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
        logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        nonredactFileName = logs_path.split('/')[-1]
        remotepath = logs_path[0:logs_path.rfind('/')+1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    nonredactFileName=nonredactFileName)

    def test_ns_server_with_redaction_enabled(self):
        #load bucket and do some ops
        gen_create = BlobGenerator('logredac', 'logredac-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)

        gen_delete = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items / 2, end=self.num_items)
        gen_update = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items + 1,
                                   end=self.num_items * 3 / 2)

        self._load_all_buckets(self.master, gen_delete, "create", 0)
        self._load_all_buckets(self.master, gen_update, "create", 0)

        #set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
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
        gen_delete = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items / 2,
                                   end=self.num_items)
        gen_update = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items + 1,
                                   end=self.num_items * 3 / 2)
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

    def test_cbcollect_with_redaction_enabled_with_views(self):
        self.set_redaction_level()
        self._create_views()
        """ start collect logs """
        self.start_logs_collection()
        result = self.monitor_logs_collection()
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
            logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
            redactFileName = logs_path.split('/')[-1]
            nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
            remotepath = logs_path[0:logs_path.rfind('/')+1]
            self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
            self.log.info("Verify on log {0}".format(xdcr_log))
            self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.goxdcr.log")
        finally:
            """ clean up xdcr """
            rest_dest.delete_bucket()
            rest_src.remove_all_replications()
            rest_src.remove_all_remote_clusters()
            if self.interrupt_replication:
                shell = RemoteMachineShellConnection(self.master)
                shell.disable_firewall()
                shell.disconnect()

