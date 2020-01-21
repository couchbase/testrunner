from lib import testconstants
from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase, Backupset
from upgrade.newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection
import logging
import subprocess, os
from remote.remote_util import RemoteMachineShellConnection
from pytests.eventing.eventing_constants import EXPORTED_FUNCTION
from ent_backup_restore.validation_helpers.backup_restore_validations \
                                                 import BackupRestoreValidations
from testconstants import LINUX_COUCHBASE_BIN_PATH,\
                          COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH_RAW,\
                          WIN_COUCHBASE_BIN_PATH_RAW, WIN_COUCHBASE_BIN_PATH, WIN_TMP_PATH_RAW,\
                          MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH,\
                          WIN_TMP_PATH, STANDARD_BUCKET_PORT
log = logging.getLogger()


class EventingTools(EventingBaseTest, EnterpriseBackupRestoreBase, NewUpgradeBaseTest):
    def setUp(self):
        super(EventingTools, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=500)
        if self.create_functions_buckets:
            self.bucket_size = 100
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMERS
        elif handler_code == 'n1ql_op_with_timers':
            # index is required for delete operation through n1ql
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
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
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        self.backupset = Backupset()
        self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        self.backupset.name = self.input.param("name", "backup")
        self.backupset.backup_host = self.servers[0]
        self.backupset.cluster_host = self.servers[0]
        self.backupset.cluster_host_username = self.servers[0].rest_username
        self.backupset.cluster_host_password = self.servers[0].rest_password
        self.backupset.restore_cluster_host = self.servers[1]
        self.backupset.restore_cluster_host_username = self.servers[1].rest_username
        self.backupset.restore_cluster_host_password = self.servers[1].rest_password
        self.num_shards = self.input.param("num_shards", None)
        self.debug_logs = self.input.param("debug-logs", False)
        cmd = 'curl -g %s:8091/diag/eval -u Administrator:password ' % self.master.ip
        cmd += '-d "path_config:component_path(bin)."'
        bin_path = subprocess.check_output(cmd, shell=True)
        if "bin" not in bin_path:
            self.fail("Check if cb server install on %s" % self.master.ip)
        else:
            self.cli_command_location = bin_path.replace('"', '') + "/"
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        self.root_path = LINUX_ROOT_PATH
        self.wget = "wget"
        self.os_name = "linux"
        self.tmp_path = "/tmp/"
        self.long_help_flag = "--help"
        self.short_help_flag = "-h"
        if info == 'linux':
            if self.nonroot:
                base_path = "/home/%s" % self.master.ssh_username
                self.database_path = "%s%s" % (base_path, COUCHBASE_DATA_PATH)
                self.root_path = "/home/%s/" % self.master.ssh_username
        elif info == 'windows':
            self.os_name = "windows"
            self.cmd_ext = ".exe"
            self.wget = "/cygdrive/c/automation/wget.exe"
            self.database_path = WIN_COUCHBASE_DATA_PATH_RAW
            self.root_path = WIN_ROOT_PATH
            self.tmp_path = WIN_TMP_PATH
            self.long_help_flag = "help"
            self.short_help_flag = "h"
            win_format = "C:/Program Files"
            cygwin_format = "/cygdrive/c/Program\ Files"
            if win_format in self.cli_command_location:
                self.cli_command_location = self.cli_command_location.replace(win_format,
                                                                              cygwin_format)
            self.backupset.directory = self.input.param("dir", WIN_TMP_PATH_RAW + "entbackup")
        elif info == 'mac':
            self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        else:
            raise Exception("OS not supported.")
        self.backup_validation_files_location = "/tmp/backuprestore" + self.master.ip
        self.backups = []
        self.validation_helper = BackupRestoreValidations(self.backupset,
                                                          self.cluster_to_backup,
                                                          self.cluster_to_restore,
                                                          self.buckets,
                                                          self.backup_validation_files_location,
                                                          self.backups,
                                                          self.num_items,
                                                          self.vbuckets)
        self.restore_only = self.input.param("restore-only", False)
        self.same_cluster = self.input.param("same-cluster", False)
        self.reset_restore_cluster = self.input.param("reset-restore-cluster", True)
        self.no_progress_bar = self.input.param("no-progress-bar", True)
        self.multi_threads = self.input.param("multi_threads", False)
        self.threads_count = self.input.param("threads_count", 1)
        self.bucket_delete = self.input.param("bucket_delete", False)
        self.bucket_flush = self.input.param("bucket_flush", False)
        include_buckets = self.input.param("include-buckets", "")
        include_buckets = include_buckets.split(",") if include_buckets else []
        exclude_buckets = self.input.param("exclude-buckets", "")
        exclude_buckets = exclude_buckets.split(",") if exclude_buckets else []
        self.backupset.exclude_buckets = exclude_buckets
        self.backupset.include_buckets = include_buckets
        self.backupset.disable_bucket_config = self.input.param("disable-bucket-config", False)
        self.backupset.disable_views = self.input.param("disable-views", False)
        self.backupset.disable_gsi_indexes = self.input.param("disable-gsi-indexes", False)
        self.backupset.disable_ft_indexes = self.input.param("disable-ft-indexes", False)
        self.backupset.disable_data = self.input.param("disable-data", False)
        self.backupset.disable_conf_res_restriction = self.input.param("disable-conf-res-restriction", None)
        self.backupset.force_updates = self.input.param("force-updates", True)
        self.backupset.resume = self.input.param("resume", False)
        self.backupset.purge = self.input.param("purge", False)
        self.backupset.threads = self.input.param("threads", self.number_of_processors())
        self.backupset.start = self.input.param("start", 1)
        self.backupset.end = self.input.param("stop", 1)
        self.backupset.number_of_backups = self.input.param("number_of_backups", 1)
        self.backupset.number_of_backups_after_upgrade = \
            self.input.param("number_of_backups_after_upgrade", 0)
        self.backupset.filter_keys = self.input.param("filter-keys", "")
        self.backupset.random_keys = self.input.param("random_keys", False)
        self.backupset.filter_values = self.input.param("filter-values", "")
        self.backupset.no_ssl_verify = self.input.param("no-ssl-verify", False)
        self.backupset.secure_conn = self.input.param("secure-conn", False)
        self.backupset.bk_no_cert = self.input.param("bk-no-cert", False)
        self.backupset.rt_no_cert = self.input.param("rt-no-cert", False)
        self.backupset.backup_list_name = self.input.param("list-names", None)
        self.backupset.backup_incr_backup = self.input.param("incr-backup", None)
        self.backupset.bucket_backup = self.input.param("bucket-backup", None)
        self.backupset.backup_to_compact = self.input.param("backup-to-compact", 0)
        self.backupset.map_buckets = self.input.param("map-buckets", None)
        self.add_node_services = self.input.param("add-node-services", "kv")
        self.backupset.backup_compressed = \
            self.input.param("backup-conpressed", False)
        self.number_of_backups_taken = 0
        self.vbucket_seqno = []
        self.expires = self.input.param("expires", 0)
        self.auto_failover = self.input.param("enable-autofailover", False)
        self.auto_failover_timeout = self.input.param("autofailover-timeout", 30)
        self.graceful = self.input.param("graceful", False)
        self.recoveryType = self.input.param("recoveryType", "full")
        self.skip_buckets = self.input.param("skip_buckets", False)
        self.lww_new = self.input.param("lww_new", False)
        self.skip_consistency = self.input.param("skip_consistency", False)
        self.master_services = self.get_services([self.backupset.cluster_host],
                                                 self.services_init, start_node=0)
        if not self.master_services:
            self.master_services = ["kv"]
        self.per_node = self.input.param("per_node", True)
        if not os.path.exists(self.backup_validation_files_location):
            os.mkdir(self.backup_validation_files_location)
        self.total_buckets = len(self.buckets)
        self.replace_ttl = self.input.param("replace-ttl", None)
        self.replace_ttl_with = self.input.param("replace-ttl-with", None)
        self.verify_before_expired = self.input.param("verify-before-expired", False)
        self.vbucket_filter = self.input.param("vbucket-filter", None)
        self.new_replicas = self.input.param("new-replicas", None)
        self.should_fail = self.input.param("should-fail", False)
        self.restore_compression_mode = self.input.param("restore-compression-mode", None)
        self.enable_firewall = False

    def tearDown(self):
        super(EventingTools, self).tearDown()

    @property
    def cluster_to_backup(self):
        return self.get_nodes_in_cluster(self.backupset.cluster_host)

    @property
    def cluster_to_restore(self):
        return self.get_nodes_in_cluster(self.backupset.restore_cluster_host)

    def test_backup_create(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # deploy the function
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        # self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.backup_create()
        self.backup_create_validate()
        self.backup_cluster()
        self.backup_list()
        self.cluster.rebalance([self.servers[1]], [self.servers[2]], [], services=["eventing"])
        try:
            self.backup_restore_validate()
        except Exception as ex:
            if "Extra elements found in the actual metadata Data" not in str(ex):
                self.fail("restore failed : {0}".format(str(ex)))
        self.cluster.rebalance([self.servers[1]], [], [self.servers[2]])

    def test_eventing_lifecycle_with_couchbase_cli(self):
        # load some data in the source bucket
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # This value is hardcoded in the exported function name
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, EXPORTED_FUNCTION.NEW_BUCKET_OP)
        fh = open(abs_file_path, "r")
        lines = fh.read()
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
        elif info == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = testconstants.WIN_COUCHBASE_BIN_PATH_RAW
        elif info == 'mac':
            self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
        else:
            raise Exception("OS not supported.")
        # create the json file need on the node
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        remote_client = RemoteMachineShellConnection(eventing_node)
        remote_client.write_remote_file_single_quote("/root", "Function_396275055_test_export_function.json", lines)
        # import the function
        self._couchbase_cli_eventing(eventing_node, "Function_396275055_test_export_function", "import",
                                     "SUCCESS: Events imported",
                                     file_name="Function_396275055_test_export_function.json")
        # deploy the function
        self._couchbase_cli_eventing(eventing_node, "Function_396275055_test_export_function", "deploy",
                                     "SUCCESS: Request to deploy the function was accepted")
        self.verify_eventing_results("Function_396275055_test_export_function", self.docs_per_day * 2016,
                                     skip_stats_validation=True)
        # list the function
        self._couchbase_cli_eventing(eventing_node, "Function_396275055_test_export_function", "list",
                                     " Status: Deployed")
        # export the function
        self._couchbase_cli_eventing(eventing_node, "Function_396275055_test_export_function", "export",
                                     "SUCCESS: Function exported to: Function_396275055_test_export_function2.json",
                                     file_name="Function_396275055_test_export_function2.json")
        # check if the exported function actually exists
        exists = remote_client.file_exists("/root", "Function_396275055_test_export_function2.json")
        # check if the exported file exists
        if not exists:
            self.fail("file does not exist after export")
        # export-all functions
        self._couchbase_cli_eventing(eventing_node, "Function_396275055_test_export_function", "export-all",
                                     "SUCCESS: All functions exported to: export_all.json",
                                    file_name="export_all.json", name=False)
        # check if the exported function actually exists
        exists = remote_client.file_exists("/root", "export_all.json")
        # check if the exported file exists
        if not exists:
            self.fail("file does not exist after export-all")
        # undeploy the function
        self._couchbase_cli_eventing(eventing_node, "Function_396275055_test_export_function", "undeploy",
                                     "SUCCESS: Request to undeploy the function was accepted")
        self.sleep(120)
        # delete the function
        self._couchbase_cli_eventing(eventing_node, "Function_396275055_test_export_function", "delete",
                                     "SUCCESS: Request to delete the function was accepted")

    def _couchbase_cli_eventing(self, host, function_name, operation, result, file_name=None, name=True):
        remote_client = RemoteMachineShellConnection(host)
        cmd = "couchbase-cli eventing-function-setup -c {0} -u {1} -p {2} --{3} ".format(
            host.ip, host.rest_username, host.rest_password, operation)
        if name:
            cmd += " --name {0}".format(function_name)
        if file_name:
            cmd += " --file {0}".format(file_name)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        log.info(command)
        output, error = remote_client.execute_command(command)
        if error or not [x for x in output if result in x]:
            self.fail("couchbase-cli event-setup function {0} failed: {1}".format(operation, output))
        else:
            log.info("couchbase-cli event-setup function {0} succeeded : {1}".format(operation, output))
