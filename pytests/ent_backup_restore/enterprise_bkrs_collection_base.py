import copy
import os, shutil, ast, re, subprocess
import json
import random
import urllib.request, urllib.parse, urllib.error, datetime
from collection.collections_cli_client import CollectionsCLI
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from collection.collections_n1ql_client import CollectionsN1QL
from lib.couchbase_helper.documentgenerator import SDKDataLoader


from basetestcase import BaseTestCase
from TestInput import TestInputSingleton, TestInputServer
from couchbase_helper.data_analysis_helper import DataCollector
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from ent_backup_restore.validation_helpers.backup_restore_validations \
    import BackupRestoreValidations
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestHelper, RestConnection, \
    Bucket as CBBucket
from couchbase_helper.document import View
from testconstants import LINUX_COUCHBASE_BIN_PATH, \
    COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH_RAW, \
    WIN_COUCHBASE_BIN_PATH_RAW, WIN_COUCHBASE_BIN_PATH, WIN_TMP_PATH_RAW, \
    MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH, \
    WIN_TMP_PATH, STANDARD_BUCKET_PORT, WIN_CYGWIN_BIN_PATH
from testconstants import INDEX_QUOTA, FTS_QUOTA, CLUSTER_QUOTA_RATIO
from security.rbac_base import RbacBase
from couchbase.bucket import Bucket

from lib.couchbase_helper.tuq_generators import JsonGenerator
from lib.memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper

SOURCE_CB_PARAMS = {
    "authUser": "default",
    "authPassword": "",
    "authSaslUser": "",
    "authSaslPassword": "",
    "clusterManagerBackoffFactor": 0,
    "clusterManagerSleepInitMS": 0,
    "clusterManagerSleepMaxMS": 20000,
    "dataManagerBackoffFactor": 0,
    "dataManagerSleepInitMS": 0,
    "dataManagerSleepMaxMS": 20000,
    "feedBufferSizeBytes": 0,
    "feedBufferAckThreshold": 0
}
INDEX_DEFINITION = {
    "type": "fulltext-index",
    "name": "",
    "uuid": "",
    "params": {},
    "sourceType": "couchbase",
    "sourceName": "default",
    "sourceUUID": "",
    "sourceParams": SOURCE_CB_PARAMS,
    "planParams": {}
}


class EnterpriseBackupRestoreCollectionBase(BaseTestCase):
    def setUp(self):
        super().setUp()
        """ from version 4.6.0 and later, --host flag is deprecated """
        self.cluster_flag = "--host"
        self.backupset = Backupset()
        self.cmd_ext = ""
        self.should_fail = self.input.param("should-fail", False)
        self.restore_should_fail = self.input.param("restore_should_fail", False)
        self.merge_should_fail = self.input.param("merge_should_fail", False)
        self.database_path = COUCHBASE_DATA_PATH

        self.master.protocol = "http://"
        if self.enforce_tls:
            self.master.port = '18091'
            self.master.protocol = "https://"
            for server in self.servers:
                server.port = '18091'

        cmd = 'curl -k -g {4}{0}:{3}/diag/eval -u {1}:{2} '.format(self.master.ip,
                                                                   self.master.rest_username,
                                                                   self.master.rest_password,
                                                                   self.master.port,
                                                                   self.master.protocol)
        cmd += '-d "path_config:component_path(bin)."'
        bin_path = subprocess.check_output(cmd, shell=True)
        try:
            bin_path = bin_path.decode()
        except AttributeError:
            pass
        if not self.skip_init_check_cbserver:
            if "bin" not in bin_path:
                self.fail("Check if cb server install on %s" % self.master.ip)
            else:
                self.cli_command_location = bin_path.replace('"', '') + "/"

        self.use_rest = self.input.param("use_rest", True)
        self.use_cli = self.input.param("use_cli", False)
        self.num_items = self.input.param("items", 1000)
        self.value_size = self.input.param("value_size", 512)
        self.non_ascii_name = self.input.param("non_ascii_name", None)
        self.create_scopes = self.input.param("create_scopes", True)
        self.create_collections = self.input.param("create_collections", True)
        self.buckets_only = self.input.param("buckets_only", False)
        self.scopes_only = self.input.param("scopes_only", False)
        self.collections_only = self.input.param("collections_only", False)
        self.drop_scopes = self.input.param("drop_scopes", False)
        self.drop_collections = self.input.param("drop_collections", False)
        self.check_scopes = self.input.param("check_scopes", False)
        self.check_scopes_details = self.input.param("check_scopes_details", False)
        self.check_collections = self.input.param("check_collections", False)
        self.check_collections_details = self.input.param("check_collections_details", False)
        self.check_collection_enable = self.input.param("check_collection_enable", True)
        self.json_output = self.input.param("json_output", False)
        self.load_json_format = self.input.param("load_json_format", False)
        self.load_to_scopes = self.input.param("load_to_scopes", False)
        self.load_to_collections = self.input.param("load_to_collections", False)
        self.load_to_default_scopes = self.input.param("load_to_scopes", False)
        self.cbwg_no_value_in_col_flag = self.input.param("cbwg_no_value_in_col_flag", False)
        self.cbwg_invalid_value_in_col_flag = self.input.param("cbwg_invalid_value_in_col_flag", False)
        self.load_to_default_collections = self.input.param("load_to_collections", False)
        self.backupset.auto_create_buckets = self.input.param("auto_create_buckets", False)
        self.backupset.create_restore_buckets = self.input.param("create_restore_buckets", True)
        self.backupset.exist_scopes_in_restore_buckets = self.input.param("exist_scopes_in_restore_buckets", False)
        self.backupset.exist_collections_in_restore_buckets = self.input.param("exist_collections_in_restore_buckets", False)
        self.backupset.custom_scopes = self.input.param("custom_scopes", False)
        self.backupset.custom_collections = self.input.param("custom_collections", False)

        self.debug_logs = self.input.param("debug-logs", False)
        self.show_bk_list = self.input.param("show_bk_list", True)
        self.vbuckets_filter_no_data = False
        self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        self.backupset.user_env = self.input.param("user-env", False)
        self.backupset.passwd_env = self.input.param("passwd-env", False)
        self.backupset.log_archive_env = self.input.param("log-archive-env", False)
        self.backupset.log_redaction = self.input.param("log-redaction", False)
        self.backupset.redaction_salt = self.input.param("redaction-salt", None)
        self.backupset.no_log_output_flag = self.input.param("no-log-output-flag", False)
        self.backupset.ex_logs_path = self.input.param("ex-logs-path", None)
        self.backupset.overwrite_user_env = self.input.param("overwrite-user-env", False)
        self.backupset.overwrite_passwd_env = self.input.param("overwrite-passwd-env", False)
        self.replace_ttl_with = self.input.param("replace-ttl-with", None)
        self.num_shards = self.input.param("num_shards", None)
        self.multi_num_shards = self.input.param("multi_num_shards", False)
        self.shards_action = self.input.param("shards_action", None)
        self.bkrs_flag = self.input.param("bkrs_flag", None)
        self.force_restart_erlang = self.input.param("force_restart_erlang", False)
        self.force_restart_couchbase_server = \
            self.input.param("force_restart_couchbase_server", False)
        self.bk_with_ttl = self.input.param("bk-with-ttl", None)
        self.backupset.user_env_with_prompt = \
            self.input.param("user-env-with-prompt", False)
        self.backupset.passwd_env_with_prompt = \
            self.input.param("passwd-env-with-prompt", False)
        self.backup_corrupted = False
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        self.root_path = LINUX_ROOT_PATH
        self.wget = "wget"
        self.os_name = "linux"
        self.tmp_path = "/tmp/"
        self.long_help_flag = "--help"
        self.short_help_flag = "-h"
        self.cygwin_bin_path = ""
        self.enable_firewal = False
        self.rfc3339_date = "date +%s --date='{0} seconds' | ".format(self.replace_ttl_with) + \
                            "xargs -I {} date --date='@{}' --rfc-3339=seconds | " \
                            "sed 's/ /T/'"
        self.seconds_with_ttl = "date +%s --date='{0} seconds'".format(self.replace_ttl_with)
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
            self.cygwin_bin_path = WIN_CYGWIN_BIN_PATH
            self.rfc3339_date = "date +%s --date='{0} seconds' | ".format(self.replace_ttl_with) + \
                                "{0}xargs -I {{}} date --date=\"@'{{}}'\" --rfc-3339=seconds | " \
                                    .format(self.cygwin_bin_path) + \
                                "sed 's/ /T/'"
            win_format = "C:/Program Files"
            cygwin_format = "/cygdrive/c/Program\\ Files"
            if win_format in self.cli_command_location:
                self.cli_command_location = self.cli_command_location.replace(win_format,
                                                                              cygwin_format)
            self.backupset.directory = self.input.param("dir", WIN_TMP_PATH_RAW + "entbackup")
        elif info == 'mac':
            self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        else:
            raise Exception("OS not supported.")
        self.backup_validation_files_location = "/tmp/backuprestore" + self.master.ip
        self.backupset.backup_host = self.input.clusters[1][0]
        self.backupset.directory += "_" + self.master.ip
        self.backupset.name = self.input.param("name", "backup")
        self.non_master_host = self.input.param("non-master", False)
        self.compact_backup = self.input.param("compact-backup", False)
        self.merged = self.input.param("merged", None)
        self.expire_time = self.input.param('expire_time', 0)
        self.after_upgrade_merged = self.input.param("after-upgrade-merged", False)
        self.batch_size = self.input.param("batch_size", 1000)
        self.create_gsi = self.input.param("create-gsi", False)
        self.gsi_names = ["num1", "num2"]
        self.enable_firewall = False
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.timer_storage_chan_size = self.input.param('timer_storage_chan_size', 10000)
        self.dcp_gen_chan_size = self.input.param('dcp_gen_chan_size', 10000)
        self.is_sbm = self.input.param('source_bucket_mutation', False)
        self.is_curl = self.input.param('curl', False)
        self.print_eventing_handler_code_in_logs = self.input.param('print_eventing_handler_code_in_logs', True)
        self.do_restore = self.input.param("do-restore", False)
        self.do_verify = self.input.param("do-verify", False)
        self.create_views = self.input.param("create-views", False)
        self.create_fts_index = self.input.param("create-fts-index", False)
        self.test_fts = self.input.param("test_fts", False)
        self.cluster_new_user = self.input.param("new_user", None)
        self.cluster_new_role = self.input.param("new_role", None)
        self.new_replicas = self.input.param("new-replicas", None)
        self.restore_only = self.input.param("restore-only", False)
        self.replace_ttl = self.input.param("replace-ttl", None)
        self.vbucket_filter = self.input.param("vbucket-filter", None)
        self.restore_compression_mode = self.input.param("restore-compression-mode", None)
        self.force_version_upgrade = self.input.param("force-version-upgrade", None)
        if self.non_master_host:
            self.backupset.cluster_host = self.servers[1]
            self.backupset.cluster_host_username = self.servers[1].rest_username
            self.backupset.cluster_host_password = self.servers[1].rest_password
        else:
            self.backupset.cluster_host = self.servers[0]
            self.backupset.cluster_host_username = self.servers[0].rest_username
            self.backupset.cluster_host_password = self.servers[0].rest_password
        self.same_cluster = self.input.param("same-cluster", False)
        self.reset_backup_cluster = self.input.param("reset_backup_cluster", False)
        self.reset_restore_cluster = self.input.param("reset-restore-cluster", True)
        self.backupset.backup_services_init = self.input.param("backup_services_init", None)
        self.no_progress_bar = self.input.param("no-progress-bar", True)
        self.multi_threads = self.input.param("multi_threads", False)
        self.threads_count = self.input.param("threads_count", 1)
        self.bucket_delete = self.input.param("bucket_delete", False)
        self.bucket_flush = self.input.param("bucket_flush", False)
        if self.same_cluster:
            self.backupset.restore_cluster = self.servers
            self.backupset.restore_cluster_host = self.servers[0]
            self.backupset.restore_cluster_host_username = self.servers[0].rest_username
            self.backupset.restore_cluster_host_password = self.servers[0].rest_password
        else:
            self.backupset.restore_cluster = self.input.clusters[0]
            self.backupset.restore_cluster_host = self.input.clusters[0][0]
            self.backupset.restore_cluster_host_username = self.input.clusters[0][0].rest_username
            self.backupset.restore_cluster_host_password = self.input.clusters[0][0].rest_password
        """ new user to test RBAC """
        if self.cluster_new_user:
            self.backupset.cluster_host_username = self.cluster_new_user
            self.backupset.restore_cluster_host_username = self.cluster_new_user
        include_buckets = self.input.param("include-buckets", "")
        #include_buckets = include_buckets.split(",") if include_buckets else []
        exclude_buckets = self.input.param("exclude-buckets", "")
        #exclude_buckets = exclude_buckets.split(",") if exclude_buckets else []
        self.backupset.exclude_buckets = exclude_buckets
        self.backupset.include_buckets = include_buckets
        self.backupset.disable_bucket_config = self.input.param("disable-bucket-config", False)
        self.backupset.disable_views = self.input.param("disable-views", False)
        self.backupset.disable_gsi_indexes = self.input.param("disable-gsi-indexes", False)
        self.backupset.disable_ft_indexes = self.input.param("disable-ft-indexes", False)
        self.backupset.disable_ft_alias = self.input.param("disable-ft-alias", False)
        self.backupset.disable_analytics = self.input.param("disable-analytics", False)
        self.backupset.disable_data = self.input.param("disable-data", False)
        self.backupset.disable_conf_res_restriction = self.input.param("disable-conf-res-restriction", None)
        self.backupset.force_updates = self.input.param("force-updates", False)
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
        self.backupset.map_bucket_names = []
        self.ops_type = self.input.param("ops-type", "update")
        """
        ****** Collection params *******
        """
        self.backupset.map_data_collection = self.input.param("map_data_collection", False)
        self.backupset.col_per_scope = self.input.param("col_per_scope", 1)
        self.backupset.delete_old_bucket = self.input.param("delete-old-bucket", False)
        self.backupset.info_all = self.input.param("info_all", True)
        self.backupset.info_to_json = self.input.param("info_to_json", True)
        self.backupset.load_to_collection = self.input.param("load_to_collection", False)
        self.backupset.collection_string = self.input.param("collection_string", None)
        self.backupset.scopes = None
        self.backupset.collections = None
        self.backupset.load_scope_id = None

        self.add_node_services = self.input.param("add-node-services", "kv")
        self.backupset.backup_compressed = \
            self.input.param("backup-conpressed", False)
        self.backups = []

        self.objstore_provider = None
        provider = self.input.param("objstore_provider", None)
        if provider == "s3":
            self.objstore_provider = S3(self.backupset.objstore_access_key_id, self.backupset.objstore_bucket,
                                        self.backupset.objstore_cacert, self.backupset.objstore_endpoint,
                                        self.backupset.objstore_no_ssl_verify, self.backupset.objstore_region,
                                        self.backupset.objstore_secret_access_key,
                                        self.backupset.objstore_staging_directory)

        # We run in a separate branch so when we add more providers the setup will be run by default
        if provider:
            # Override and use the same archive directory across platforms
            self.backupset.directory = f"archive-{self.master.ip}"
            self.objstore_provider.setup()

        self.validation_helper = BackupRestoreValidations(self.backupset,
                                                          self.cluster_to_backup,
                                                          self.cluster_to_restore,
                                                          self.buckets,
                                                          self.backup_validation_files_location,
                                                          self.backups,
                                                          self.num_items,
                                                          self.vbuckets,
                                                          self.objstore_provider)
        self.number_of_backups_taken = 0
        self.vbucket_seqno = []
        self.expires = self.input.param("expires", 0)
        self.auto_failover = self.input.param("enable-autofailover", False)
        self.auto_failover_timeout = self.input.param("autofailover-timeout", 30)
        self.graceful = self.input.param("graceful", False)
        self.failover_ip = None
        self.done_failover = False
        self.recoveryType = self.input.param("recoveryType", "full")
        self.skip_buckets = self.input.param("skip_buckets", False)
        self.lww_new = self.input.param("lww_new", False)
        self.skip_consistency = self.input.param("skip_consistency", False)
        self.master_services = self.get_services([self.backupset.cluster_host],
                                                 self.services_init, start_node=0)
        if not self.master_services:
            self.master_services = ["kv"]
        self.restore_services = self.get_services([self.backupset.restore_cluster_host],
                                                  self.services_init, start_node=0)
        if not self.restore_services:
            self.restore_services = ["kv"]
        self.per_node = self.input.param("per_node", True)

        self.rest_bk = CollectionsRest(self.backupset.cluster_host)
        self.cli_bk = CollectionsCLI(self.backupset.cluster_host)
        #self.cli.enable_dp()
        self.conn_bk = RestConnection(self.backupset.cluster_host)
        self.stat_bk = CollectionsStats(self.backupset.cluster_host)

        self.rest_rs = CollectionsRest(self.backupset.restore_cluster_host)
        self.cli_rs = CollectionsCLI(self.backupset.restore_cluster_host)
        self.conn_rs = RestConnection(self.backupset.restore_cluster_host)
        self.stat_rs = CollectionsStats(self.backupset.restore_cluster_host)

        if not os.path.exists(self.backup_validation_files_location):
            os.mkdir(self.backup_validation_files_location)
        shell.disconnect()

    def tearDown(self):
        super(EnterpriseBackupRestoreCollectionBase, self).tearDown()
        if self.enforce_tls:
            self.master.port = '8091'
            self.master.protocol = "http://"
            for server in self.servers:
                server.port = '8091'
        if not self.input.param("skip_cleanup", False):
            remote_client = RemoteMachineShellConnection(self.input.clusters[1][0])
            info = remote_client.extract_remote_info().type.lower()
            self.tmp_path = "/tmp/"
            if info == 'linux' or info == 'mac':
                backup_directory = "/tmp/entbackup"
            elif info == 'windows':
                self.tmp_path = WIN_TMP_PATH
                backup_directory = WIN_TMP_PATH_RAW + "entbackup"
            else:
                raise Exception("OS not supported.")
            backup_directory += "_" + self.master.ip
            validation_files_location = "%sbackuprestore" % self.tmp_path + self.master.ip
            if info == 'linux':
                command = "rm -rf {0}".format(backup_directory)
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
            elif info == 'windows':
                remote_client.remove_directory_recursive(backup_directory)
            if info == 'linux':
                command = "rm -rf /cbqe3043/entbackup"
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
            if self.input.clusters:
                for key in list(self.input.clusters.keys()):
                    servers = self.input.clusters[key]
                    try:
                        self.backup_reset_clusters(servers)
                    except:
                        self.log.error("was not able to cleanup cluster the first time")
                        self.backup_reset_clusters(servers)
            if os.path.exists(validation_files_location):
                self.log.info("delete dir %s" % validation_files_location)
                shutil.rmtree(validation_files_location)
            remote_client.disconnect()

    @property
    def cluster_to_backup(self):
        return self.get_nodes_in_cluster(self.backupset.cluster_host)

    @property
    def cluster_to_restore(self):
        return self.get_nodes_in_cluster(self.backupset.restore_cluster_host)

    def number_of_processors(self):
        remote_client = RemoteMachineShellConnection(self.input.clusters[1][0])
        info = remote_client.extract_remote_info().type.lower()
        if info == 'linux' or info == 'mac':
            command = "nproc"
            output, error = remote_client.execute_command(command)
            if output:
                return output[0]
            else:
                return error[0]
        elif info == 'windows':
            sysinfo = remote_client.get_windows_system_info()
            numprocs = sysinfo['Processor(s)'].split(' ')
            return numprocs[0]

    def backup_reset_clusters(self, servers):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, self)
        ClusterOperationHelper.cleanup_cluster(servers, master=servers[0])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers,
                                                             self, debug=False)

    def store_vbucket_seqno(self):
        try:
            vseqno = self.get_vbucket_seqnos(self.cluster_to_backup,
                                             self.buckets,
                                             self.skip_consistency, self.per_node)
            self.vbucket_seqno.append(vseqno)
        except Exception as e:
            if e:
                if "Memcached error #1 'Not found'" in str(e) and self.done_failover:
                    self.log.info("error as expected")
                else:
                    self.fail(e)

    def create_scope_cluster_host(self, num_scope=2, admin_tools_package=False):
        bucket_name = self.buckets[0].name
        for x in range(num_scope):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                self.rest_bk.create_scope(bucket=bucket_name, scope=scope_name,
                                              params=None)
            else:
                self.cli_bk.create_scope(bucket=bucket_name, scope=scope_name,
                                         admin_tools_package=admin_tools_package)

    def delete_scope_cluster_host(self, num_scope=2):
        bucket_name = self.buckets[0].name
        for x in range(num_scope):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                self.rest_bk.delete_scope(bucket_name, scope_name)
            else:
                self.cli_bk.delete_scope(scope_name, bucket=bucket_name)

    def create_collection_cluster_host(self, num_collection=1):
        bucket_name = self.buckets[0].name
        scopes = self.get_bucket_scope_cluster_host()
        if scopes:
            for x in range(num_collection):
                for scope in scopes:
                    if bucket_name in scope:
                        continue
                    if self.use_rest:
                        self.rest_bk.create_collection(bucket=bucket_name, scope=scope,
                                                   collection="mycollection_{0}_{1}".format(scope, x))
                    else:
                        self.cli_bk.create_collection(bucket=bucket_name, scope=scope,
                                                   collection="mycollection_{0}_{1}".format(scope, x))

    def delete_collection_cluster_host(self, num_collection=1):
        bucket_name = self.buckets[0].name
        for x in range(num_collection):
            if self.use_rest:
                self.rest_bk.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                   collection="_{0}".format(bucket_name))
            else:
                self.cli_bk.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                  collection="_{0}".format(bucket_name))

    def create_scope_collection_cluster_host(self, num_scope_collection=2):
        bucket_name = self.buckets[0].name
        for x in range(num_collection):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                self.rest_bk.create_scope_collection(bucket_name, scope_name,
                                                     "mycollection{0}".format(x))
            else:
                self.cli_bk.create_scope_collection(bucket_name, scope_name,
                                                    "mycollection{0}".format(x))

    def get_bucket_scope_cluster_host(self):
        bucket_name = self.buckets[0].name
        if self.use_rest:
            scopes = self.rest_bk.get_bucket_scopes(bucket_name)
        else:
            scopes = self.cli_bk.get_bucket_scopes(bucket_name)[0]
        return scopes

    def create_scope_restore_cluster_host(self, num_scope=2):
        bucket_name = self.buckets[0].name
        for x in range(num_scope):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                self.rest_rs.create_scope(bucket=bucket_name, scope=scope_name,
                                              params=None)
            else:
                self.cli_rs.create_collection(bucket=bucket_name, scope=scope_name)

    def delete_scope_restore_cluster_host(self, num_scope=2):
        bucket_name = self.buckets[0].name
        for x in range(num_scope):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                self.rest_rs.delete_scope(scope_name, bucket=bucket_name)
            else:
                self.cli_rs.delete_scope(scope_name, bucket=bucket_name)

    def create_collection_restore_cluster_host(self, num_collection=1):
        bucket_name = self.buckets[0].name
        scopes = self.get_bucket_scope_cluster_host()
        if not scopes:
            self.sleep(10, "Wait for scopes created completely")
            scopes = self.get_bucket_scope_cluster_host()
        self.create_scope_restore_cluster_host(len(scopes) - 1)
        if scopes:
            for x in range(num_collection):
                for scope in scopes:
                    if bucket_name in scope:
                        continue
                    if self.use_rest:
                        self.rest_rs.create_collection(bucket=bucket_name, scope=scope,
                                                   collection="mycollection_{0}_{1}".format(scope, x))
                    else:
                        self.cli_rs.create_collection(bucket=bucket_name, scope=scope,
                                                   collection="mycollection_{0}_{1}".format(scope, x))

    def delete_collection_restore_cluster_host(self, num_collection=2):
        bucket_name = self.buckets[0].name
        for x in range(num_collection):
            if self.use_rest:
                self.rest_rs.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                   collection="_{0}".format(bucket_name))
            else:
                self.cli_rs.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                  collection="_{0}".format(bucket_name))

    def create_scope_collection_restore_cluster_host(self, num_scope_collection=2):
        bucket_name = self.buckets[0].name
        for x in range(num_collection):
            if self.use_rest:
                self.rest_rs.create_scope_collection(bucket_name, "scope{0}".format(x),
                                                     "mycollection{0}".format(x))
            else:
                self.cli_rs.create_scope_collection(bucket_name, "scope{0}".format(x),
                                                    "mycollection{0}".format(x))

    def get_bucket_scope_restore_cluster_host(self):
        bucket_name = self.buckets[0].name
        if self.backupset.map_buckets:
            bucket_name = self.backupset.map_bucket_name[0]
        scopes = None
        if self.use_rest:
            scopes = self.rest_rs.get_bucket_scopes(bucket_name)
        else:
            scopes = self.cli_rs.get_bucket_scopes(bucket_name)
        return scopes

    def get_bucket_collection_cluster_host(self, bucket_name=None, scope=None):
        if bucket_name is None:
            bucket_name = self.buckets[0].name
        if scope is None:
            raise Exception("Need scope name to get collection")
        collections = None
        if self.use_rest:
            collections = self.rest_bk.get_scope_collections(bucket_name, scope)
        else:
            collections = self.cli_bk.get_scope_collections(bucket_name, scope)
        return collections

    def get_bucket_collection_restore_cluster_host(self, bucket_name=None, scope=None):
        if bucket_name is None:
            bucket_name = self.buckets[0].name
        if scope is None:
            raise Exception("Need scope name to get collection")
        collections = None
        if self.use_rest:
            collections = self.rest_rs.get_scope_collections(bucket_name, scope)
        else:
            collections = self.cli_rs.get_scope_collections(bucket_name, scope)
        return collections

    def verify_scopes_in_restore_cluster_host(self):
        backup_scopes = self.get_bucket_scope_cluster_host()
        if isinstance(backup_scopes, tuple):
            backup_scopes = backup_scopes[0]
        restore_scopes = self.get_bucket_scope_restore_cluster_host()
        if isinstance(restore_scopes, tuple):
            restore_scopes = restore_scopes[0]
        not_found_scopes = []
        for l in [backup_scopes, restore_scopes]:
            if l[0][:4] == "\x1b[6n":
                l[0] = l[0][4:]
        for restore_scope in restore_scopes:
            if isinstance(restore_scope, list):
                for ele in backup_scopes:
                    if ele in restore_scope:
                        self.log.info("Found '{0} in restore scopes {1}".format(ele, restore_scope))
                    else:
                        not_found_scopes.append(ele)
                if not_found_scopes:
                    self.fail("Scope {0} failed to restore".format(not_found_scopes))
                else:
                    break
            else:
                if restore_scope in backup_scopes:
                    self.log.info("Scope {0} restored".format(restore_scope))
                else:
                    self.fail("Scope {0} failed to restore".format(restore_scope))

    def verify_collections_in_restore_cluster_host(self):
        if self.buckets_only or self.scopes_only:
            return
        bucket_name = self.buckets[0].name
        scopes = self.get_bucket_scope_restore_cluster_host()
        if isinstance(scopes, tuple):
            scopes = scopes[0]
        for scope in scopes:
            if "_default" in scope:
                scopes.remove(scope)
        for scope in scopes:
            check_bucket = bucket_name
            if self.backupset.map_buckets:
                check_bucket = RestConnection(self.backupset.cluster_host).get_buckets()[0].name
            backup_collections = self.get_bucket_collection_cluster_host(check_bucket, scope)
            if isinstance(backup_collections, tuple):
                backup_collections = backup_collections[0]
            if self.backupset.map_buckets:
                bucket_name = self.backupset.map_bucket_name[0]
            restore_collections = self.get_bucket_collection_restore_cluster_host(bucket_name, scope)
            if isinstance(restore_collections, tuple):
                restore_collections = restore_collections[0]
            for l in [backup_collections, restore_collections]:
                if l[0][:4] == "\x1b[6n":
                    l[0] = l[0][4:]
            for backup_collection in backup_collections:
                if backup_collection in restore_collections:
                    self.log.info("Collection {0} restored".format(backup_collection))
                else:
                    if not self.drop_collections:
                        self.fail("Collection {0} failed to restore".format(backup_collection))

    def get_collection_stats_cluster_host(self):
        bucket = self.buckets[0]
        return self.stat_bk.get_collection_stats(bucket)

    def get_scopes_id_cluster_host(self, scope):
        bucket = self.buckets[0]
        return self.stat_bk.get_scope_id(bucket, scope)

    def get_collections_id_cluster_host(self, scope, collection):
        bucket = self.buckets[0]
        return self.stat_bk.get_collection_id(bucket, scope, collection)

    def load_all_buckets(self, cluster=None, item_size=125, ratio=0.9, command_options=""):
        return self.__load_all_buckets(cluster, item_size, ratio, command_options)

    def __load_all_buckets(self, cluster=None, item_size=125, ratio=0.9, command_options=""):
        if not cluster:
            cluster = self.master
        shell = RemoteMachineShellConnection(cluster)
        for bucket in self.buckets:
            shell.execute_cbworkloadgen(cluster.rest_username,
                                        cluster.rest_password,
                                        self.num_items,
                                        ratio,
                                        bucket.name,
                                        item_size,
                                        command_options)

    def backup_create(self, del_old_backup=True):
        args = "config --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        if self.backupset.exclude_buckets:
            args += " --exclude-data \"{0}\"".format(self.backupset.exclude_buckets)
        if self.backupset.include_buckets:
            args += " --include-data \"{0}\"".format(self.backupset.include_buckets)
        if self.backupset.disable_bucket_config:
            args += " --disable-bucket-config"
        if self.backupset.disable_views:
            args += " --disable-views"
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes"
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes"
        if self.backupset.disable_ft_alias:
            args += " --disable-ft-alias"
        if self.backupset.disable_analytics:
            args += " --disable-analytics"
        if self.backupset.disable_data:
            args += " --disable-data"
        if self.backupset.log_to_stdout:
            args += " --log-to-stdout"
        if self.vbucket_filter:
            if self.vbucket_filter == "all":
                all_vbuckets = "0"
                for x in range(1, 1024):
                    all_vbuckets += "," + str(x)
                args += " --vbucket-filter {0}".format(all_vbuckets)
            else:
                args += " --vbucket-filter {0}".format(self.vbucket_filter)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        if del_old_backup:
            self.log.info("Remove any old dir before create new one")
            remote_client.execute_command("rm -rf {0}".format(self.backupset.directory))
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        return output, error

    def backup_info(self):
        args = "info -a {0} -r {1}".format(self.backupset.directory, self.backupset.name)
        if self.backupset.info_to_json:
            args += " --json"
        if self.backupset.info_all:
            args += " --all"
        if self.backupset.backup_incr_backup:
            args += " --backup {0}".format(self.backupset.backup_incr_backup)
        if self.backupset.collection_string:  # format 'bucket.scope.collection'
            args += " --collection-string {0}".format(self.backupset.collection_string)

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        if self.debug_logs:
            remote_client.log_command_output(output, error)
        remote_client.disconnect()
        if error:
            return False, error, "Getting backup info failed."
        else:
            return True, output, "Backup info obtained"

    def _verify_backup_directory_count(self, expected):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, _ = shell.execute_command("ls -l {}/{} | wc -l".format(self.backupset.directory,
                                                                       self.backupset.name))
        output = " ".join(output)
        try:
            count = int(output)
            if count != expected + 2:
                self.fail(
                    "Number of backup directories {0} does not match expected {1}".format(count - 2, expected + 2))
        except ValueError as e:
            self.fail("Could not get the number of backups in the archive due to: {0}".format(e))

        shell.disconnect()

    def backup_create_validate(self):
        output, error = self.backup_create()
        if error or "Backup repository `{0}` created successfully".format(self.backupset.name) not in output[0]:
            self.fail("Creating backupset failed.")
        status, msg = self.validation_helper.validate_backup_create()
        if not status:
            self.fail(msg)
        self.log.info(msg)

    def backup_cluster(self, threads_count=1):
        url_format = ""
        secure_port = ""
        if self.backupset.secure_conn:
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.cluster_host)
            secure_port = "1"
            url_format = "s"
        if self.enforce_tls:
            url_format = "s"

        user_input = "--username %s " % self.backupset.cluster_host_username
        password_input = "--password %s " % self.backupset.cluster_host_password
        if self.backupset.user_env and not self.backupset.overwrite_user_env:
            user_input = ""
        elif self.backupset.user_env_with_prompt:
            password_input = "-u "

        if self.backupset.passwd_env and not self.backupset.overwrite_passwd_env:
            password_input = ""
        elif self.backupset.passwd_env_with_prompt:
            password_input = "-p "

        """ Print out of cbbackupmgr from 6.5 is different with older version """
        self.cbbkmgr_version = "6.5"
        self.bk_printout = ["Backup completed successfully", "Backup successfully completed"]
        if RestHelper(RestConnection(self.backupset.backup_host)).is_ns_server_running():
            self.cbbkmgr_version = RestConnection(self.backupset.backup_host).get_nodes_version()

        if "4.6" <= self.cbbkmgr_version[:3]:
            self.cluster_flag = "--cluster"

        args = "backup --archive {0} --repo {1} {6} http{7}://{2}:{8}{3} " \
               "{4} {5}".format(self.backupset.directory, self.backupset.name,
                                self.backupset.cluster_host.cluster_ip,
                                self.backupset.cluster_host.port,
                                user_input,
                                password_input,
                                self.cluster_flag, url_format,
                                secure_port)
        if self.backupset.no_ssl_verify:
            args += " --no-ssl-verify"
        if self.backupset.secure_conn:
            if not self.backupset.bk_no_cert:
                args += " --cacert %s" % cacert
        if self.backupset.resume:
            args += " --resume"
        if self.backupset.purge:
            args += " --purge"
        if self.no_progress_bar:
            args += " --no-progress-bar"
        if self.multi_threads:
            args += " --threads %s " % threads_count
        if self.backupset.backup_compressed:
            args += " --value-compression compressed"
        if self.backupset.log_to_stdout:
            args += " --log-to-stdout"
        if self.backupset.auto_select_threads:
            args += " --auto-select-threads"
        user_env = ""
        password_env = ""
        if self.backupset.user_env:
            self.log.info("set user env to Administrator")
            user_env = "export CB_USERNAME=Administrator; "
        if self.backupset.passwd_env:
            self.log.info("set password env to password")
            password_env = "export CB_PASSWORD=password; "
        if self.backupset.user_env_with_prompt:
            self.log.info("set username env to prompt")
            user_env = "unset CB_USERNAME; export CB_USERNAME;"
        if self.backupset.passwd_env_with_prompt:
            self.log.info("set password env to prompt")
            password_env = "unset CB_PASSWORD; export CB_PASSWORD;"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{3} {2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                       password_env, user_env)

        output, error = remote_client.execute_command(command)
        if self.debug_logs:
            remote_client.log_command_output(output, error)

        for bucket in self.buckets:
            self.bk_printout.append('Backed up bucket "{0}" succeeded'.format(bucket.name))
        for bucket in self.buckets:
            if error or not self._check_output(self.bk_printout, output):
                self.log.error("Failed to backup bucket {0}".format(bucket.name))
                return output, error

        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = remote_client.execute_command(command)
        if o:
            self.backups.append(o[0])
        self.number_of_backups_taken += 1
        self.log.info("Finished taking backup  with args: {0}".format(args))
        remote_client.disconnect()
        return output, error

    def backup_cluster_validate(self, skip_backup=False, repeats=1,
                                validate_directory_structure=True):
        if not skip_backup:
            output, error = self.backup_cluster()
            if error or not self._check_output(self.bk_printout, output):
                if self.vbucket_filter is not None:
                    self.log.info("These vbuckets {0} may not contain data"
                                  .format(self.vbucket_filter))
                    self.vbuckets_filter_no_data = True
                    return
                else:
                    self.fail("Taking cluster backup failed. Check printout below. "
                              "\nErrors: {0} \nOutput: {1}".format(error, output))
        if self.show_bk_list:
            self.backup_list()
        if repeats < 2 and validate_directory_structure:
            status, msg = self.validation_helper.validate_backup()
            if not status:
                self.fail(msg)
            self.log.info(msg)
        if not self.backupset.deleted_buckets:
            if not self.backupset.force_updates:
                if not self.done_failover:
                    self.store_vbucket_seqno()
                else:
                    return
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location,
                                                    self.objstore_provider)

    def backup_restore(self):
        if self.restore_only:
            self.backups.append("2019-11-21T14_54_50.808254198-08_00")
        if self.vbuckets_filter_no_data:
            self.log.info("No data in backup repo as expected.")
            return
        try:
            backup_start = self.backups[int(self.backupset.start) - 1]
        except IndexError:
            backup_start = "{0}{1}".format(self.backups[-1], self.backupset.start)
        try:
            backup_end = self.backups[int(self.backupset.end) - 1]
        except IndexError:
            backup_end = "{0}{1}".format(self.backups[-1], self.backupset.end)
        url_format = ""
        secure_port = ""
        if self.backupset.secure_conn:
            url_format = "s"
            secure_port = "1"
            cacert = self.root_path + "cert.pem"
            if self.os_name == "windows":
                cacert = WIN_TMP_PATH_RAW + "cert.pem"
        if self.enforce_tls:
            url_format = "s"

        user_input = "--username %s " % self.backupset.restore_cluster_host_username
        password_input = "--password %s " % self.backupset.restore_cluster_host_password
        if self.backupset.user_env and not self.backupset.overwrite_user_env:
            user_input = ""
        elif self.backupset.user_env_with_prompt:
            user_input = "-u "
        if self.backupset.passwd_env and not self.backupset.overwrite_passwd_env:
            password_input = ""
        elif self.backupset.passwd_env_with_prompt:
            password_input = "-p "

        version = RestConnection(self.backupset.backup_host).get_nodes_version()
        if "4.6" <= version:
            self.cluster_flag = "--cluster"

        args = "restore --archive {0} --repo {1} {2} http{9}://{3}:{10}{4} " \
               "{5} {6} --start {7} --end {8}" \
            .format(self.backupset.directory,
                    self.backupset.name,
                    self.cluster_flag,
                    self.backupset.restore_cluster_host.cluster_ip,
                    self.backupset.restore_cluster_host.port,
                    user_input,
                    password_input,
                    backup_start, backup_end, url_format, secure_port)
        if version >= "6.5" and self.backupset.auto_select_threads:
            args += " --auto-select-threads"
        if version >= "6.5" and self.backupset.log_to_stdout:
            args += " --log-to-stdout"
        if self.backupset.no_ssl_verify:
            args += " --no-ssl-verify"
        if self.backupset.secure_conn:
            if not self.backupset.rt_no_cert:
                args += " --cacert %s" % cacert
        if self.backupset.exclude_buckets:
            args += " --exclude-data {0}".format(self.backupset.exclude_buckets)
        if self.backupset.include_buckets:
            args += " --include-data {0}".format(self.backupset.include_buckets)
        if self.backupset.disable_bucket_config:
            args += " --disable-bucket-config {0}".format(self.backupset.disable_bucket_config)
        if self.backupset.disable_views:
            args += " --disable-views "
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes {0}".format(self.backupset.disable_gsi_indexes)
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes {0}".format(self.backupset.disable_ft_indexes)
        if self.backupset.disable_data:
            args += " --disable-data {0}".format(self.backupset.disable_data)
        if self.backupset.disable_conf_res_restriction is not None:
            args += " --disable-conf-res-restriction {0}".format(
                self.backupset.disable_conf_res_restriction)
        if self.backupset.auto_create_buckets:
            args += " --auto-create-buckets "
        filter_chars = {"star": "*", "dot": "."}
        if self.backupset.filter_keys:
            for key in filter_chars:
                if key in self.backupset.filter_keys:
                    self.backupset.filter_keys = self.backupset.filter_keys.replace(key,
                                                                                    filter_chars[key])
            args += " --filter-keys '{0}'".format(self.backupset.filter_keys)
        if self.backupset.filter_values:
            for key in filter_chars:
                if key in self.backupset.filter_values:
                    self.backupset.filter_values = self.backupset.filter_values.replace(key,
                                                                                        filter_chars[key])
            args += " --filter-values '{0}'".format(self.backupset.filter_values)
        if self.backupset.force_updates:
            args += " --force-updates"
        if self.no_progress_bar:
            args += " --no-progress-bar"
        bucket_compression_mode = self.compression_mode
        if self.restore_compression_mode is not None:
            bucket_compression_mode = self.restore_compression_mode
        if not self.skip_buckets:
            rest_conn = RestConnection(self.backupset.restore_cluster_host)
            restore_cluster_services = rest_conn.get_nodes_services()
            restore_services = list(restore_cluster_services.values())[0]
            shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
            self.log.info("Services in restore cluster: {0}".format(restore_services))
            rest_helper = RestHelper(rest_conn)
            total_mem = rest_conn.get_nodes_self().mcdMemoryReserved
            ram_size = rest_conn.get_nodes_self().mcdMemoryReserved * CLUSTER_QUOTA_RATIO
            has_index_node = False
            if "index" in restore_services:
                has_index_node = True
                ram_size = int(ram_size) - INDEX_QUOTA
            if "fts" in restore_services:
                ram_size = int(ram_size) - FTS_QUOTA
            all_buckets = self.total_buckets
            if len(self.buckets) > 0:
                all_buckets = len(self.buckets)
            bucket_size = self._get_bucket_size(ram_size, all_buckets)
            if "index" in restore_services and "fts" in restore_services:
                bucket_size = bucket_size * .8
            if self.dgm_run:
                bucket_size = 256

            count = 0
            buckets = []
            replicas = self.num_replicas
            if self.new_replicas:
                replicas = self.new_replicas
            reset_restore_cluster = False
            reset_cluster_count = 0
            for bucket in self.buckets:
                bucket_name = bucket.name
                if not rest_helper.bucket_exists(bucket_name) and \
                    self.backupset.create_restore_buckets:
                    if self.backupset.map_buckets is None:
                        self.log.info("Creating bucket {0} in restore host {1}"
                                      .format(bucket_name,
                                              self.backupset.restore_cluster_host.ip))
                    elif self.backupset.map_buckets:
                        self.log.info("Create new bucket name to restore to this bucket")
                        bucket_maps = ""
                        bucket_name = bucket.name + "_" + str(count)
                        self.backupset.map_bucket_name.append(bucket_name)
                    if self.bucket_type == "ephemeral":
                        self.eviction_policy = "noEviction"
                        self.log.info("ephemeral bucket needs to set restore cluster "
                                      "to memopt for gsi.")
                        self.test_storage_mode = "memory_optimized"
                        kv_quota = self._reset_storage_mode(rest_conn, self.test_storage_mode)
                        if self.test_fts:
                            if reset_cluster_count == 0:
                                self._create_restore_cluster()
                                reset_restore_cluster = True
                        if not self.dgm_run and int(kv_quota) > 0:
                            bucket_size = kv_quota
                    if not reset_restore_cluster and reset_cluster_count == 0:
                        self._create_restore_cluster()
                    self.log.info("replica in bucket {0} is {1}".format(bucket.name, replicas))
                    try:
                        if ram_size - bucket_size < 1:
                            bucket_size = ram_size - 1
                        rest_conn.create_bucket(bucket=bucket_name,
                                                ramQuotaMB=int(bucket_size),
                                                replicaNumber=replicas,
                                                bucketType=self.bucket_type,
                                                proxyPort=bucket.port,
                                                evictionPolicy=self.eviction_policy,
                                                lww=self.lww_new,
                                                compressionMode=bucket_compression_mode)
                    except Exception as e:
                        if "unable to create bucket" in str(e):
                            ready = RestHelper(RestConnection(self.backupset.restore_cluster_host)).is_ns_server_running()
                            if not ready:
                                self.fail("Couchbase Server failed to start")
                            else:
                                rest_conn.create_bucket(bucket=bucket_name,
                                                    ramQuotaMB=int(bucket_size),
                                                    replicaNumber=replicas,
                                                    bucketType=self.bucket_type,
                                                    proxyPort=bucket.port,
                                                    evictionPolicy=self.eviction_policy,
                                                    lww=self.lww_new,
                                                    compressionMode=bucket_compression_mode)
                    bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
                    if not bucket_ready:
                        self.fail("Bucket {0} not created after 120 seconds.".format(bucket_name))
                elif self.backupset.map_buckets and self.same_cluster:
                    bucket_maps = ""
                    bucket_name = bucket.name + "_" + str(count)
                    self.log.info("replica in bucket {0} is {1}".format(bucket_name, replicas))
                    if self.backupset.delete_old_bucket:
                        BucketOperationHelper.delete_bucket_or_assert( \
                            self.backupset.restore_cluster_host, bucket.name, self)
                    if ram_size - bucket_size < 1:
                        bucket_size = ram_size - 1
                    rest_conn.create_bucket(bucket=bucket_name,
                                            ramQuotaMB=int(bucket_size),
                                            replicaNumber=replicas,
                                            bucketType=self.bucket_type,
                                            proxyPort=bucket.port,
                                            evictionPolicy=self.eviction_policy,
                                            lww=self.lww_new,
                                            compressionMode=bucket_compression_mode)
                    bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
                    if not bucket_ready:
                        self.fail("Bucket {0} not created after 120 seconds.".format(bucket_name))
                if self.backupset.create_restore_buckets:
                    count = 0
                    bucket_status = rest_conn.get_bucket_status(bucket_name)
                    while bucket_status == "warmup":
                        self.sleep(5, "wait for bucket is up")
                        bucket_status = rest_conn.get_bucket_status(bucket_name)
                        count += 1
                        if count == 15:
                            raise Exception("Bucket does not ready after 30 seconds")
                    if has_index_node:
                        self.sleep(15, "wait for index service ready")
                    buckets.append("%s=%s" % (bucket.name, bucket_name))
                    count += 1
                    reset_cluster_count += 1
                    if self.backupset.exist_scopes_in_restore_buckets:
                        self.create_scope_restore_cluster_host()
                    if self.backupset.exist_collections_in_restore_buckets:
                        self.create_collection_restore_cluster_host()
            if self.backupset.create_restore_buckets:
                bucket_maps = ",".join(buckets)
        if self.backupset.map_buckets:
            args += " --map-data %s " % bucket_maps
        user_env = ""
        password_env = ""
        if self.backupset.user_env:
            self.log.info("set user env to Administrator")
            user_env = "export CB_USERNAME=Administrator; "
        if self.backupset.passwd_env:
            self.log.info("set password env to password")
            password_env = "export CB_PASSWORD=password; "
        if self.backupset.user_env_with_prompt:
            self.log.info("set username env to prompt")
            user_env = "unset CB_USERNAME; export CB_USERNAME;"
        if self.backupset.passwd_env_with_prompt:
            self.log.info("set password env to prompt")
            password_env = "unset CB_PASSWORD; export CB_PASSWORD;"
        shell = RemoteMachineShellConnection(self.backupset.backup_host)

        self.ttl_value = ""
        if self.replace_ttl is not None:
            if self.replace_ttl == "all" or self.replace_ttl == "expired":
                if self.replace_ttl_with is None:
                    self.fail("Need to include param 'replace-ttl-with' value")
                else:
                    if self.replace_ttl_with == 0:
                        self.ttl_value = "0"
                        args += " --replace-ttl {0} --replace-ttl-with 0" \
                            .format(self.replace_ttl)
                    else:
                        ttl_date, _ = shell.execute_command(self.rfc3339_date)
                        self.seconds_with_ttl, _ = shell.execute_command(self.seconds_with_ttl)
                        if self.seconds_with_ttl:
                            self.ttl_value = self.seconds_with_ttl[0]
                        if ttl_date and ttl_date[0]:
                            args += " --replace-ttl {0} --replace-ttl-with {1}" \
                                .format(self.replace_ttl, ttl_date[0])
                        elif isinstance(self.replace_ttl_with, str):
                            args += " --replace-ttl {0} --replace-ttl-with {1}" \
                                .format(self.replace_ttl, self.replace_ttl_with)
            elif self.replace_ttl == "add-none":
                args += " --replace-ttl none"
            elif self.replace_ttl == "empty-flag":
                args += " --replace-ttl-with {0}".format(self.replace_ttl_with)
            else:
                args += " --replace-ttl {0}".format(self.replace_ttl)
        """ rename restore node to ip """
        RestConnection(self.backupset.restore_cluster_host).rename_node(
                                            self.backupset.restore_cluster_host.cluster_ip)
        if self.backupset.secure_conn:
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.restore_cluster_host)
        if self.backupset.map_data_collection and not self.buckets_only:
            args += " --map-data %s " % self.bucket_map_collection
        command = "{3} {2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                       password_env, user_env)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        if not self.enable_firewall:
            self._verify_bucket_compression_mode(bucket_compression_mode)

        eventing_service_in = False
        errs_check = ["Unable to process value", "Error restoring cluster",
                      "Expected argument for option"]
        rest_rs = RestConnection(self.backupset.restore_cluster_host)
        rs_cluster_services = list(rest_rs.get_nodes_services().values())
        for srv in rs_cluster_services:
            if "eventing" in srv:
                eventing_service_in = True
                errs_check.append("User needs one of the following permissions: cluster.eventing")
        accepted_errs = False
        for err in errs_check:
            if self._check_output(err, output):
                accepted_errs = True
                break

        if accepted_errs:
            if not self.should_fail:
                if not self.restore_should_fail and not eventing_service_in:
                    self.fail("Failed to restore cluster")
            else:
                self.log.info("This test is for negative test")
        res = output
        res.extend(error)
        error_str = "Error restoring cluster: Transfer failed. " \
                    "Check the logs for more information."
        if error_str in res:
            bk_log_file_name = "backup.log"
            if "6.5" <= RestConnection(self.backupset.backup_host).get_nodes_version():
                bk_log_file_name = "backup-*.log"
            command = "cat " + self.backupset.directory + \
                      "/logs/{0} | grep '".format(bk_log_file_name) + \
                      error_str + "' -A 10 -B 100"
            output, error = shell.execute_command(command)
            shell.log_command_output(output, error)
        if 'Required Flags:' in res:
            if not self.should_fail:
                self.fail("Command line failed. Please check test params.")
        shell.disconnect()
        return output, error

    def backup_restore_validate(self, compare_uuid=False,
                                seqno_compare_function="==",
                                replicas=False, mode="memory",
                                expected_error=None):
        if self.vbuckets_filter_no_data:
            self.log.info("No data in backup repo as expected.")
            return
        output, error = self.backup_restore()
        if expected_error:
            output.extend(error)
            error_found = False
            if expected_error:
                for line in output:
                    if self.debug_logs:
                        print("output from cmd: ", line)
                        print("expect error   : ", expected_error)
                    if line.find(expected_error) != -1:
                        error_found = True
                        break
            self.assertTrue(error_found, "Expected error not found: %s" % expected_error)
            return

        self.verify_scopes_in_restore_cluster_host()
        self.verify_collections_in_restore_cluster_host()

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        bk_log_file_name = "backup.log"
        if "6.5" <= RestConnection(self.backupset.backup_host).get_nodes_version():
            bk_log_file_name = "backup-*.log"
        ipv6_raw_format = [":", "[", "]"]
        ipv6_raw_ip = False
        bk_raw_ipv6_dir = ""
        if ":" in self.backupset.directory:
            ipv6_raw_ip = True
            bk_raw_ipv6_dir = self.backupset.directory
            for x in ipv6_raw_format:
                bk_raw_ipv6_dir = bk_raw_ipv6_dir.replace(x, "\\" + x)
        bk_dir = self.backupset.directory
        if ipv6_raw_ip:
            bk_dir = bk_raw_ipv6_dir
        command = "grep 'Restore completed successfully' " + bk_dir + \
                  "/logs/{0}".format(bk_log_file_name)
        output, error = remote_client.execute_command(command)
        if self.debug_logs:
            remote_client.log_command_output(output, error)
        if not output:
            self.fail("Restoring backup failed.")
        command = "grep 'Transfer failed' " + bk_dir + \
                  "/logs/{0}".format(bk_log_file_name)
        output, error = remote_client.execute_command(command)
        if self.debug_logs:
            remote_client.log_command_output(output, error)
        if output:
            self.fail("Restoring backup failed.")

        self.log.info("Finished restoring backup")
        self.log.info("Get current vseqno on node %s " % self.cluster_to_restore[0].ip)

        """ Add built-in user cbadminbucket to second cluster """
        self.add_built_in_server_user(node=self.input.clusters[0][:self.nodes_init][0])
        current_vseqno = {}
        verify_buckets = RestConnection(self.backupset.restore_cluster_host).get_buckets()
        if not self.backupset.force_updates:
            current_vseqno = self.get_vbucket_seqnos(self.cluster_to_restore, verify_buckets,
                                                     self.skip_consistency, self.per_node)
        self.log.info("*** Start to validate the restore ")
        if self.replace_ttl_with:
            if int(self.replace_ttl_with) < 120 and int(self.replace_ttl_with) != 0:
                self.fail("Need more than 3 minutes to run with verify before expired")
            if int(self.replace_ttl_with) > 14400 or int(self.replace_ttl_with) == 0:
                self.log.info("Time is set more than 4 hours.  No need to sleep")
        if self.vbucket_filter and self.vbucket_filter != "empty":
            self._validate_restore_vbucket_filter()
        elif self.replace_ttl == "all" or self.replace_ttl == "expired":
            self._validate_restore_replace_ttl_with(self.ttl_value)
        else:
            status, msg = self.validation_helper.validate_restore(self.backupset.end,
                                                                  self.vbucket_seqno, current_vseqno,
                                                                  compare_uuid=compare_uuid,
                                                                  compare=seqno_compare_function,
                                                                  get_replica=replicas, mode=mode)

            """ limit the length of message printout to 3000 chars """
            info = str(msg)[:3000] + '..' if len(str(msg)) > 3000 else msg
            if not status:
                self.fail(info)
            self.log.info(info)
        remote_client.disconnect()

    def backup_list(self):
        args = "info --all -j --archive {0}".format(self.backupset.directory)
        if self.backupset.backup_list_name:
            args += " --repo {0}".format(self.backupset.backup_list_name)
        if self.backupset.backup_incr_backup:
            args += " --backup {0}".format(self.backupset.backup_incr_backup)
        if self.backupset.bucket_backup:
            args += " --bucket {0}".format(self.backupset.bucket_backup)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        if self.debug_logs:
            remote_client.log_command_output(output, error)
        remote_client.disconnect()
        if error:
            return False, error, "Getting backup list failed."
        else:
            return True, output, "Backup list obtained"

    def backup_compact(self):
        args = "compact --archive {0} --repo {1} --backup {2}".format(self.backupset.directory, self.backupset.name,
                                                                      self.backups[self.backupset.backup_to_compact])
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if "Compaction succeeded," not in output[0]:
            return False, output, "Compacting backup failed."
        else:
            return True, output, "Compaction of backup success"
        remote_client.disconnect()

    def backup_remove(self, backup_range=None):
        args = "remove --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        if backup_range is not None:
            args += " --backups {0}".format(backup_range)

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.verify_cluster_stats()
        if error:
            return False, error, "Removing backup failed."
        elif "failed" in " ".join(output):
            return False, output, "Removing backup failed."
        else:
            return True, output, "Removing of backup success"

    def backup_list_validate(self):
        return self.backup_info_validate()

    def backup_info_validate(self, scopes=None, collections=None):
        status, output, message = self.backup_info()
        if not status:
            self.fail(message)
        status, message = self.validation_helper.validate_backup_info(output,
                                                                      scopes,
                                                                      collections)
        if not status:
            self.fail(message)
        self.log.info(message)


    def backup_compact_validate(self):
        self.log.info("Listing backup details before compact")
        status, output_before_compact, message = self.backup_list()
        if not status:
            self.fail(message)
        status, output, message = self.backup_compact()
        if not status:
            self.fail(message)
        self.log.info("Listing backup details after compact")
        status, output_after_compact, message = self.backup_list()
        if not status:
            self.fail(message)
        status, message = self.validation_helper.validate_compact_lists(output_before_compact, output_after_compact)
        if not status:
            self.fail(message)
        self.log.info(message)



    def backup_merge(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " \
                      + str(self.number_of_backups_taken))
        if self.backupset.date_range == '':
            if self.backupset.deleted_backups:
                self.backupset.end -= len(self.backupset.deleted_backups)
            try:
                backup_start = self.backups[int(self.backupset.start) - 1]
            except IndexError:
                backup_start = "{0}{1}".format(self.backups[-1], self.backupset.start)
            try:
                backup_end = self.backups[int(self.backupset.end) - 1]
            except IndexError:
                backup_end = "{0}{1}".format(self.backups[-1], self.backupset.end)
            args = "merge --archive {0} --repo {1} --start {2} --end {3}" \
                .format(self.backupset.directory,
                        self.backupset.name,
                        backup_start, backup_end)
        else:
            args = "merge --archive {0} --repo {1} --date-range {2}".format(self.backupset.directory,
                                                                            self.backupset.name,
                                                                            self.backupset.date_range)

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            return False, error, "Merging backup failed"
        elif output and not self._check_output("Merge completed successfully", output):
            return False, output, "Merging backup failed"
        elif not output:
            self.log.info("process cbbackupmge may be killed")
            return False, [], "cbbackupmgr may be killed"
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory,
                                                    self.backupset.name)
        o, e = remote_client.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: "
                      + str(self.number_of_backups_taken))
        remote_client.disconnect()
        return True, output, "Merging backup succeeded"

    def backup_merge_validate(self, repeats=1, skip_validation=False):
        status, output, message = self.backup_merge()
        if not status:
            if self.backup_corrupted:
                self.log.info("Merge failed as expected.  This is negative test.")
                return
            else:
                if self._check_output("no such table: partition", output):
                    mesg = "\n**************** "
                    mesg += "\nThis issue was report in MB-36984.  Will fix in 7.x"
                    mesg += "\n**************** "
                    self.log.error(mesg)
                else:
                    self.fail(message)

        if repeats < 2 and not skip_validation:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location, merge=True)

            self.validation_helper.validate_merge(self.backup_validation_files_location)

    def validate_backup_data(self, server_host, server_bucket, master_key,
                             perNode, getReplica, mode, items, key_check,
                             validate_keys=False,
                             regex_pattern=None):
        """
            Compare data in backup file with data in bucket
        """
        data_matched = True
        data_collector = DataCollector()
        bk_file_data, _ = data_collector.get_kv_dump_from_backup_file(server_host, self.cli_command_location,
                                                                      self.cmd_ext, master_key, self.buckets,
                                                                      backupset=self.backupset)
        restore_file_data = bk_file_data
        regex_backup_data = {}
        if regex_pattern is not None:
            pattern = re.compile("%s" % regex_pattern)
            for bucket in self.buckets:
                key_in_file_match_regex = 0
                regex_backup_data[bucket.name] = {}
                self.log.info("Extract keys with regex pattern '%s' either in key or body"
                              % regex_pattern)
                for key in restore_file_data[bucket.name]:
                    if self.debug_logs:
                        print(("key in backup file of bucket %s:  %s" \
                              % (bucket.name, key)))
                    if validate_keys:
                        if pattern.search(key):
                            regex_backup_data[bucket.name][key] = \
                                restore_file_data[bucket.name][key]
                            key_in_file_match_regex += 1
                    else:
                        if self.debug_logs:
                            print(("value of key in backup file  ", \
                                  restore_file_data[bucket.name][key]))
                        if pattern.search(restore_file_data[bucket.name][key]["Value"]):
                            regex_backup_data[bucket.name][key] = \
                                restore_file_data[bucket.name][key]
                            key_in_file_match_regex += 1
                if self.debug_logs:
                    print(("\nKeys and value in backup file of bucket {0} \
                           that matches pattern '{1}'" \
                          .format(bucket.name, regex_pattern)))
                    for x in regex_backup_data[bucket.name]:
                        print(("key: ", x))
                        print(("value: ", regex_backup_data[bucket.name][x]["Value"]))
                self.log.info("Total keys matched in bk file of bucket {0} is {1}"
                              .format(bucket.name, key_in_file_match_regex))
                restore_file_data = regex_backup_data

        buckets_data = {}
        for bucket in self.buckets:
            headerInfo, bucket_data = data_collector.collect_data(server_bucket, [bucket],
                                                                  perNode=False,
                                                                  getReplica=getReplica,
                                                                  mode=mode)
            buckets_data[bucket.name] = bucket_data[bucket.name]
            to_element = 5
            if self.backupset.filter_values:
                to_element = 7
            for key in buckets_data[bucket.name]:
                value = buckets_data[bucket.name][key]
                if self.backupset.random_keys:
                    value = ",".join(value.split(',')[4:to_element])
                    value = value.replace('""', '"')
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                else:
                    value = ",".join(value.split(',')[4:5])
                if value.startswith("b'"):
                    value = value[2:-1]
                buckets_data[bucket.name][key] = value
            self.log.info("*** Compare data in bucket and in backup file of bucket {0} ***"
                          .format(bucket.name))
            failed_persisted_bucket = []
            ready = RebalanceHelper.wait_for_stats_on_all(self.backupset.cluster_host,
                                                          bucket.name, 'ep_queue_size',
                                                          0, timeout_in_seconds=120)
            if not ready:
                failed_persisted_bucket.append(bucket.name)
            if failed_persisted_bucket:
                self.fail("Buckets {0} did not persisted.".format(failed_persisted_bucket))
            count = 0
            key_count = 0
            for key in buckets_data[bucket.name]:
                if restore_file_data[bucket.name]:
                    try:
                        if restore_file_data[bucket.name][key]:
                            if buckets_data[bucket.name][key] \
                                    != restore_file_data[bucket.name][key]["Value"]:
                                if count < 20:
                                    self.log.error("Data does not match at key {0}.\
                                                bucket: {1} != {2} file" \
                                                   .format(key, buckets_data[bucket.name][key],
                                                           restore_file_data[bucket.name][key]["Value"]))
                                    data_matched = False
                                    count += 1
                                else:
                                    raise Exception("Data not match in backup bucket {0}" \
                                                    .format(bucket.name))
                            key_count += 1
                        else:
                            raise Exception("Key {0} has no value: {1}"
                                            .format(key, restore_file_data[bucket.name][key]))
                    except Exception as e:
                        if e:
                            print(e)
                            raise Exception("\nMissing key: {0}".format(key))
                else:
                    raise Exception("Database file of bucket {0} is empty"
                                    .format(bucket.name))
            if len(restore_file_data[bucket.name]) != key_count:
                raise Exception("Total key counts do not match.  Backup {0} != {1} bucket" \
                                .format(restore_file_data[bucket.name], key_count))
            self.log.info("******** Data macth in backup file and bucket {0} ******** " \
                          .format(bucket.name))
            print(("Bucket: ", bucket.name))
            print(("Total items in backup file:   ", len(bk_file_data[bucket.name])))
            if regex_pattern is not None:
                print(("Total items to be restored with regex pattern '{0}' is {1} " \
                      .format(regex_pattern, key_count)))
            print(("Total items in bucket should be:   ", key_count))
            rest = RestConnection(server_bucket[0])
            actual_keys = rest.get_active_key_count(bucket.name)
            print(("Total items actual in bucket:      ", actual_keys))
            if actual_keys != key_count:
                self.fail("Total keys matched: %s != Total keys in bucket %s: %s" \
                          "at node %s " % (key_count, bucket.name, actual_keys,
                                           server_bucket[0]))
            if self.merged:
                if key_check:
                    self.log.info("Check if deleted keys still in backup after merged")
                    for key in restore_file_data[bucket.name]:
                        if key == key_check:
                            raise Exception("There is an old key after delete bucket,"
                                            " backup and merged ")
                    self.log.info("No deleted keys in backup after merged")
        return data_matched

    def get_info_in_database(self, server, bucket, text_search):
        """
            Get info from database file like couch.1
        """
        shell = RemoteMachineShellConnection(server)
        cmd = "%s/couch_dbdump " % self.cli_command_location
        """ since there is no load, it should have only one file per vbucket """
        output, error = shell.execute_command("%s %s/%s/0.couch.* | grep deleted"
                                              % (cmd, self.database_path,
                                                 bucket.name))
        found = False
        if output:
            self.log.info("Search for '%s' in database info" % text_search)
            for x in output:
                if text_search in x:
                    self.log.info("*** Found %s in database %s" % (text_search, x))
                    found = True
                    break
        else:
            self.log.info("output is empty")
        shell.disconnect()
        return found

    def validate_help_content(self, output, source):
        """
            Compare content of of the list with sample output
        """
        for x in range(0, len(source)):
            output[x] = output[x].strip()
            output[x] = " ".join(output[x].split())

            source[x] = source[x].strip()
            source[x] = " ".join(source[x].split())
            if not isinstance(output[x], str):
                continue
            if self.debug_logs:
                print(("\noutput:  ", repr(output[x])))
                print(("source:  ", repr(source[x])))
            if output[x] != source[x]:
                self.log.error("Element %s in output did not match " % x)
                self.log.error("Output => %s != %s <= Source" % (output[x], source[x]))
                raise Exception("Content does not match "
                                "Output => %s != %s <= Source" % (output[x], source[x]))

    def get_cluster_certificate_info(self, server_host, server_cert):
        """
            This will get certificate info from cluster
        """
        cert_file_location = self.root_path + "cert.pem"
        if self.os_name == "windows":
            cert_file_location = WIN_TMP_PATH_RAW + "cert.pem"
        shell = RemoteMachineShellConnection(server_host)
        cmd = "%s/couchbase-cli ssl-manage -c %s:8091 -u Administrator -p password " \
              "  --regenerate-cert  %s" % (self.cli_command_location,
                                             server_cert.ip,
                                             cert_file_location)
        output, _ = shell.execute_command(cmd)
        if output and "Error" in output[0]:
            self.fail("Failed to get CA certificate from cluster.")
        shell.disconnect()
        return cert_file_location

    def _create_views(self):
        self.log.info("Create views")
        default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        default_view_name = "test"
        default_ddoc_name = "ddoc_test"
        prefix = "dev_"
        query = {"full_set": "true", "stale": "false", "connection_timeout": 60000}
        rest = RestConnection(self.backupset.cluster_host)
        bucket_name = "default"
        buckets = rest.get_buckets()
        if len(buckets) > 0:
            for bucket in buckets:
                view = View(default_view_name, default_map_func)
                task = self.cluster.async_create_view(self.backupset.cluster_host,
                                                      default_ddoc_name, view, bucket.name)
                task.result()
        else:
            self.fail("No bucket found")

    def validate_backup_views(self, server_host):
        """
            Verify view is backup
        """
        data_collector = DataCollector()
        bk_views_def = data_collector.get_views_definition_from_backup_file(server_host,
                                                                            self.backupset.directory,
                                                                            self.buckets)
        def_check = ['"id": "_design/dev_ddoc_test"',
                     '"json": { "views": { "test": { "map": "function (doc) {\\n  emit(doc._id, doc);\\n}" } } }']
        if not self.backupset.disable_views:
            if bk_views_def:
                self.log.info("Validate views function")
                for x in def_check:
                    if x not in bk_views_def:
                        return False, "Missing {0} in views definition".format(x)
                return True, "Views function validated"
        else:
            if bk_views_def is None:
                return False, "Views function does not backup as expected"
            else:
                return True, "disable-view flag does not work"

    def get_database_file_info(self):
        """
           Extract database file size and items from backup repo
           :return: Datebase file information
        """
        status, output, message = self.backup_list()
        file_info = {}
        unit_size = ["MB", "GB"]
        if status:
            if output:
                for x in output:
                    if "Shards" in x:
                        if x.strip().split()[0][-2:] in unit_size:
                            file_info["file_size"] = \
                                int(x.strip().split()[0][:-2].split(".")[0])

                            file_info["items"] = int(x.strip().split()[1])
                        print(("output content   ", file_info))
            return file_info
        else:
            print(message)

    def validate_backup_compressed_file(self, no_compression, with_compression):
        """
            Compare before and after using flag --value-compression compressed
        :return: None
        """
        compressed = False
        if no_compression["items"] == with_compression["items"]:
            if no_compression["file_size"] > with_compression["file_size"]:
                compressed = True
                self.log.info("no compressed: %d, with compressed: %d"
                              % (no_compression["file_size"],
                                 with_compression["file_size"]))
            else:
                self.log.info("no compressed: %d, with compressed: %d"
                              % (no_compression["file_size"],
                                 with_compression["file_size"]))
                self.fail("cbbackupmgr failed to compress database")

        else:
            self.fail("Item miss match on 2 backup files")

    def create_indexes(self):
        gsi_type = "memory_optimized"
        rest = RestConnection(self.backupset.cluster_host)
        username = self.master.rest_username
        password = self.master.rest_password
        if "5" <= rest.get_nodes_version()[:1]:
            gsi_type = "plasma"
        gsi_type += " -auth {0}:{1} ".format(username, password)
        buckets = rest.get_buckets()
        shell = RemoteMachineShellConnection(self.backupset.cluster_host)
        for bucket in buckets:
            cmd = "cbindex -type create -bucket {0} -using {1} -index ".format(bucket.name, gsi_type)
            cmd += "{0} -fields=Num1".format(self.gsi_names[0])
            command = "{0}/{1}".format(self.cli_command_location, cmd)
            self.log.info("Create gsi indexes")
            output, error = shell.execute_command(command)
            self.sleep(5)

            if self.debug_logs:
                self.log.info("\noutput gsi: {0}".format(output))
            cmd = "cbindex -type create -bucket {0} -using {1} -index ".format(bucket.name, gsi_type)
            cmd += "{0} -fields=Num2".format(self.gsi_names[1])
            command = "{0}/{1}".format(self.cli_command_location, cmd)
            shell.execute_command(command)
            self.sleep(5)
        shell.disconnect()

    def verify_gsi(self):
        if not self.create_gsi:
            self.log.info("No GSI created.  Skip verify")
            return
        rest = RestConnection(self.backupset.cluster_host)
        buckets = rest.get_buckets()
        if "5" <= rest.get_nodes_version()[:1]:
            self.add_built_in_server_user(node=self.backupset.cluster_host)
        cmd = "cbindex -type list"
        if "5" <= rest.get_nodes_version()[:1]:
            cmd += " -auth %s:%s" % (self.backupset.cluster_host.rest_username,
                                     self.backupset.cluster_host.rest_password)
        shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)

        for bucket in buckets:
            index_found = 0
            if len(output) > 1:
                for name in self.gsi_names:
                    mesg = "GSI index {0} created in restore cluster as expected".format(name)
                    for x in output:
                        if "Index:{0}/{1}".format(bucket.name, name) in x:
                            index_found += 1
                            self.log.info(mesg)
            if index_found < len(self.gsi_names):
                self.fail("GSI indexes are not restored in bucket {0}".format(bucket.name))
        shell.disconnect()

    def _delete_repo(self):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        shell.execute_command("rm -rf {}/{}".format(self.backupset.directory, self.backupset.name))
        shell.disconnect()

    def _check_output(self, word_check, output):
        found = False
        if len(output) >= 1:
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in x.lower():
                            self.log.info("Found '{0} in CLI output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in x.lower():
                        self.log.info("Found '{0}' in CLI output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found

    def _reset_storage_mode(self, rest, storageMode, reset_node=True):
        nodes_in_cluster = rest.get_nodes()
        for node in nodes_in_cluster:
            matched_server = None
            for server in self.servers:
                if node.hostname[:-5] == server.ip:
                    matched_server = server
                    break
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
            if reset_node:
                RestConnection(node).force_eject_node()
                ready = RestHelper(rest).is_ns_server_running()
                if ready:
                    if server is not None:
                        cmd_init = 'node-init'
                        shell = RemoteMachineShellConnection(server)
                        shell.enable_diag_eval_on_non_local_hosts()
                        if self.hostname and server.ip.endswith(".com"):
                            options = '--node-init-hostname ' + server.ip
                            output, _ = shell.execute_couchbase_cli(cli_command=cmd_init,
                                                    options=options,
                                                    cluster_host="localhost",
                                                    user=server.rest_username,
                                                    password=server.rest_password)
                        shell.disconnect()
                        if not self._check_output("SUCCESS: Node initialize", output):
                            raise("Failed to set hostname")
                else:
                    self.fail("NS server is not ready after reset node")
        rest.set_indexer_storage_mode(username='Administrator',
                                      password='password',
                                      storageMode=storageMode)
        self.log.info("Done reset node with storage mode")
        sv_in_rs = self.backupset.restore_cluster_host.services
        if self.backupset.restore_cluster_host.services and \
            "," in self.backupset.restore_cluster_host.services[0]:
            sv_in_rs = self.backupset.restore_cluster_host.services.split(",")
        kv_quota = rest.init_node(sv_in_rs)
        return kv_quota

    def _reset_restore_cluster_with_bk_services(self, bk_services):
        master = self.backupset.restore_cluster_host

        BucketOperationHelper.delete_all_buckets_or_assert(
            self.backupset.restore_cluster, self)
        ClusterOperationHelper.cleanup_cluster(
            self.backupset.restore_cluster, master=master)

        rest = RestConnection(master).force_eject_node()
        rest = RestConnection(master)
        ready = RestHelper(rest).is_ns_server_running()
        if ready:
            shell = RemoteMachineShellConnection(master)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
        else:
            self.fail("NS server is not ready after reset node")
        bk_services = ['kv', 'index', 'n1ql']
        for i in range(len(self.servers)):
            if self.servers[i].ip == master.ip:
                self.backupset.restore_cluster_host.services = ",".join(bk_services)
                break
        rest = RestConnection(self.backupset.restore_cluster_host)
        kv_quota = rest.init_node()
        self.log.info("Done reset restore node")
        if len(self.input.clusters[1]) > 1:
            num_servers = len(self.backupset.restore_cluster) - 1
            self.cluster.rebalance(
                self.backupset.restore_cluster[:num_servers],
                self.backupset.restore_cluster[1:num_servers],
                [],
                services=self.services)
        count = 0
        while count < 15:
            reb_status = rest._rebalance_status_and_progress()
            if reb_status != "none":
                self.sleep(2, "wait rebalance to complete")
                count += 1
            else:
                break
            if count == 15:
                self.fail("Status still shows running after rebalance complete 30 seconds")
        return kv_quota

    def _collect_logs(self):
        """
           CB_ARCHIVE_PATH env: param log-archive-env=False
           syntax: cbbackupmgr collect-logs -a /tmp/backup -o /tmp/envlogs
        """
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        info = shell.extract_remote_info().type.lower()

        args = " collect-logs -a {0}".format(self.backupset.directory)
        if self.backupset.ex_logs_path:
            if info == 'windows':
                if "tmp" in self.backupset.ex_logs_path:
                    self.fail("It must use other name for ex logs dir in windows")
                self.backupset.ex_logs_path = "/cygdrive/c/" + \
                                              self.backupset.ex_logs_path
                if "relativepath" in self.backupset.ex_logs_path:
                    self.backupset.ex_logs_path = \
                        self.backupset.ex_logs_path.replace("/cygdrive/c/", "~/")
            else:
                self.backupset.ex_logs_path = "/tmp/" + self.backupset.ex_logs_path
                if "relativepath" in self.backupset.ex_logs_path:
                    self.backupset.ex_logs_path = \
                        self.backupset.ex_logs_path.replace("/tmp/", "~/")
            self.log.info("remove any old ex logs directory in {0}"
                          .format(self.backupset.ex_logs_path))
            shell.execute_command("rm -rf {0}".format(self.backupset.ex_logs_path))
            if self.backupset.ex_logs_path == "non-exist-dir":
                self.log.info("test on non exist directory.")
            else:
                self.log.info("Create logs dir at {0}".format(self.backupset.ex_logs_path))
                shell.execute_command("mkdir {0}".format(self.backupset.ex_logs_path))
            ex_logs_path = self.backupset.ex_logs_path
            if info == 'windows':
                ex_logs_path = ex_logs_path.replace("/cygdrive/c", "c:")
            args += " -o {0}".format(ex_logs_path)
        log_archive_env = ""
        args_env = ""
        if self.backupset.log_redaction:
            args += " --redact"
            args_env += " --redact"
        if self.backupset.redaction_salt:
            args += " --salt {}".format(self.backupset.redaction_salt)
            args_env += " --salt {}".format(self.backupset.redaction_salt)
        if self.backupset.log_archive_env:
            self.log.info("set log arvhive env to {0}".format(self.backupset.directory))
            log_archive_env = "unset CB_ARCHIVE_PATH; export CB_ARCHIVE_PATH={0}; " \
                .format(self.backupset.directory)
            if self.backupset.ex_logs_path:
                self.log.info("overwrite env log path with flag -o")
                args_env += " -o {0}".format(ex_logs_path)
            command = "{0} {1}/cbbackupmgr collect-logs {2}" \
                .format(log_archive_env,
                        self.cli_command_location,
                        args_env)
        else:
            if "-o" in args and self.backupset.no_log_output_flag:
                args = args.replace("-o", " ")
            command = "{0} {1}/cbbackupmgr {2}" \
                .format(log_archive_env,
                        self.cli_command_location,
                        args)
        collection_time = datetime.datetime.utcnow()
        output, error = shell.execute_command(command)
        shell.disconnect()
        if self._check_output("Collecting logs succeeded", output):
            self._verify_cbbackupmgr_logs(collection_time)
        elif self.backupset.no_log_output_flag and self._check_output("error", output):
            self.log.info("This is negative test")
        elif self.backupset.ex_logs_path:
            if "non-exist_dir" in self.backupset.ex_logs_path:
                self.log.info("This is negative test on non exist output logs dir")
            else:
                self.fail("Failed to collect logs")
        else:
            self.fail("Failed to collect logs. Output: {0}".format(output))

    def _verify_cbbackupmgr_logs(self, log_collect_datetime):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        self.log.info("\n**** start to verify cbbackupmgr logs ****")

        if not self.backupset.ex_logs_path:
            logs_path = self.backupset.directory + "/logs"
        elif self.backupset.ex_logs_path:
            logs_path = self.backupset.ex_logs_path
        if self.backupset.log_archive_env:
            logs_path = self.backupset.directory
            if self.backupset.ex_logs_path:
                logs_path = self.backupset.ex_logs_path

        output, _ = shell.execute_command("ls {0}".format(logs_path))

        if not output:
            if self.backupset.log_archive_env:
                self.fail("Failed to pass CB_ARCHIVE_PATH")
            if self.backupset.ex_logs_path:
                self.fail("Failed to run with ex log path")
            else:
                self.fail("Unexpexted error in _verify_cbbackupmgr_logs with"
                          "logs path of {}".format(logs_path))

        if "C_" in output:
            win_path = "/C_/tmp/entbackup"
            logs_path = logs_path + win_path
            output, _ = shell.execute_command(
                "ls {0}".format(logs_path + win_path))

        if self.debug_logs:
            print(("\nlog path: ", logs_path))
            print(("output : ", output))

        # While we extract logs, record the name of each backup's logs archive
        log_archive_names = []
        for zip_file in output:
            if "collectinfo" in zip_file:
                shell.execute_command("cd {0}; unzip {1}".format(
                    logs_path, zip_file))
                if 'redacted-' in zip_file:
                    # Redacted logs include the backup archive one layer deeper
                    if self.master.ip in zip_file:
                        log_archive_names.append("{}/{}".format(
                            ".".join(zip_file.split(".")[:-1]),
                            ".".join(zip_file.split(".")[:-1]).split('-', 1)[1]))
                    else:
                        log_archive_names.append("{}/{}".format(
                            zip_file.split(".")[0],
                            zip_file.split(".")[0].split('-', 1)[1]))
                else:
                    if self.master.ip in zip_file:
                        log_archive_names.append(".".join(zip_file.split(".")[:-1]))
                    else:
                        log_archive_names.append(zip_file.split(".")[0])

        if self.backupset.log_redaction:
            # Verify logs were redacted correctly
            for log_archive in log_archive_names:
                if not log_archive.startswith("redacted"):
                    # First check redacted directory exists
                    redacted_dir, _ = shell.execute_command(
                        "ls {}/redacted-{}".format(logs_path, log_archive))
                    if not redacted_dir:
                        self.fail(
                            "Log redaction is enabled, "
                            "yet no redacted files exist for {}".format(
                                log_archive))
                    if not self.backupset.redaction_salt:
                        self.log.info(
                            "No salt provided for log redaction, "
                            "so skipping hashing consistency test.")
                    else:
                        redacted_logs_path = "{0}/redacted-{1}/{1}/logs/".format(
                            logs_path, log_archive)
                        unredacted_logs_path = "{}/{}/logs/".format(
                            logs_path, log_archive)
                        redacted_logs, _ = shell.execute_command(
                            "ls " + redacted_logs_path)
                        for log_file in redacted_logs:
                            if not log_file.startswith("backup-"):
                                continue
                            self._validate_log_redaction_hashing(
                                unredacted_logs_path + log_file,
                                redacted_logs_path + log_file,
                                self.backupset.redaction_salt)

        for archive_path in log_archive_names:
            list_for_time_check = []
            archive_top_level, _ = shell.execute_command(
                "ls -l --time-style=long-iso {}/{}".format(logs_path,
                                                           archive_path))

            # Verify that the correct directories/files exist in the backup
            # directory
            dir_file_list = ["backup", "logs", "info.json", "system_info.log"]
            for name in dir_file_list:
                if not any(name in dir_file for dir_file in archive_top_level):
                    self.fail(
                        "Missing dir/file {0} in cbbackupmgr logs".format(
                            name))
            list_for_time_check.extend(archive_top_level[1:])

            # Verify backup-meta.json exists in backup dir. Collect
            # timestamps for timestamp verification
            backup_dir, _ = shell.execute_command(
                "ls -l --time-style=long-iso {}/{}/{}".format(
                    logs_path, archive_path, self.backupset.name))
            if not any(
                    "backup-meta.json" in file_ for file_ in backup_dir):
                self.fail("Missing file 'backup-meta.json' in backup dir")
            list_for_time_check.extend(backup_dir[1:])

            # Verify log files exist in the logs dir. Collect timestamps
            # for timestamp verification
            logs_dir, _ = shell.execute_command(
                "ls -l --time-style=long-iso {}/{}/logs".format(
                    logs_path, archive_path))
            if not any(".log" in file_ for file_ in logs_dir):
                self.fail("Missing log files in logs dir")
            list_for_time_check.extend(logs_dir[1:])

            # Check that collected timestamps for log files have been correctly
            # generated
            version = RestConnection(
                self.backupset.backup_host).get_nodes_version()
            if "6.5" <= version[:3]:
                for log_file in list_for_time_check:

                    # Extract and parse timestamp
                    archive_path = log_file.split()[-1]
                    file_timestamp = ' '.join(log_file.split()[5:7])
                    file_timestamp = datetime.datetime.strptime(
                        file_timestamp, "%Y-%m-%d %H:%M")

                    # Ensure collection times are correct, with a grace period
                    # of 30 minutes
                    if not (file_timestamp - log_collect_datetime
                            < datetime.timedelta(minutes=30)):
                        self.fail(
                            "File '{}' timestamp of '{}' inconsistent "
                            "with collection time of '{}' ".format(
                                archive_path,
                                file_timestamp,
                                log_collect_datetime))
        shell.disconnect()

    def _validate_log_redaction_hashing(self, source_file, target_file, salt):
        """For a given source file, redact it with cblogredaction, and diff its
         output with the target file. The salt provided must be the same as
         that used for the target file."""

        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        shell.execute_command("rm -f /tmp/backup_redacted_files/*")
        stdout, stderr = shell.execute_command(
            "mkdir -p /tmp/backup_redacted_files")
        if stderr:
            print(stdout)
            raise Exception(stderr)
        # Create redacted files in temp location
        stdout, stderr = shell.execute_command(
            "{}cblogredaction {} "
            "-v --salt {} "
            "--output /tmp/backup_redacted_files".format(
                LINUX_COUCHBASE_BIN_PATH, source_file, salt))

        if "ERROR" in stderr:
            shell.execute_command("rm -f /tmp/backup_redacted_files/*")
            raise Exception(stderr)

        cblogredact_output = "/tmp/backup_redacted_files/redacted-{}".format(
            source_file.split('/')[-1])

        stdout, stderr = shell.execute_command(
            "diff {} {}".format(cblogredact_output, target_file))


        if stderr:
            print(stdout)
            raise Exception(stderr)
        if stdout:
            self.fail("Log redaction hashing does not match cblogredaction's "
                      "hashing for the same salt ({}). Diff of file {} and {}"
                      " is: \n{}".format(salt,
                                       cblogredact_output,
                                       target_file,
                                       stdout))
        shell.execute_command("rm -f /tmp/backup_redacted_files/*")
        shell.disconnect()

    def _validate_restore_vbucket_filter(self):
        data_collector = DataCollector()
        if "," in self.vbucket_filter:
            vbucket_filter = [int(x) for x in self.vbucket_filter.split(",")]
        elif "-" in self.vbucket_filter:
            vbucket_filter = []
            vbuckets_range = [int(x) for x in self.vbucket_filter.split("-")]
            for i in range(vbuckets_range[0], vbuckets_range[1] + 1):
                vbucket_filter.append(i)
        else:
            vbucket_filter = self.vbucket_filter.split("")
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        rest = RestConnection(self.backupset.restore_cluster_host)
        restore_buckets = rest.get_buckets()
        for bucket in restore_buckets:
            bucket_ready = RestHelper(rest).vbucket_map_ready(bucket.name)
            if not bucket_ready:
                self.fail("Restore bucket {0} not ready after 120 seconds".format(bucket.name))
        restore_buckets_items = rest.get_buckets_itemCount()

        for bucket in self.buckets:
            output, error = shell.execute_command("ls {0}/backup/*/{1}*/data " \
                                                  .format(self.backupset.directory, bucket.name))
            filter_vbucket_keys = {}
            total_filter_keys = 0
            backup_files = []
            backup_data = []
            """ get vbucket keys pair in data base """
            self.log.info("Collecting data from backup repo ...")
            for vb in vbucket_filter:
                cmd = "{0}cbriftdump{1} --no-meta --no-body " \
                      " -f {2}/backup/*/*/data/shard_{3}.sqlite.0 | grep 'Key:'" \
                    .format(self.cli_command_location, self.cmd_ext, \
                            self.backupset.directory, vb)
                dump_output, error = shell.execute_command(cmd, debug=False)
                if dump_output:
                    dump_output = [x.replace("Key: ", "") for x in dump_output]
                    filter_vbucket_keys[vb] = dump_output
                    total_filter_keys += len(dump_output)
                    if self.debug_logs:
                        print(("dump output: ", dump_output))
            if filter_vbucket_keys:
                if int(restore_buckets_items[bucket.name]) != total_filter_keys:
                    mesg = "** Failed to restore keys from vbucket-filter. "
                    mesg += "\nTotal filter keys in backup file {0}".format(total_filter_keys)
                    mesg += "\nTotal keys in bucket {0}: {1}".format(bucket.name,
                                                                     restore_buckets_items)
                    self.fail(mesg)
                else:
                    self.log.info("Success restore items from vbucket-filter")
                    vbuckets = rest.get_vbuckets(bucket.name)
                    headerInfo, bucket_data = \
                        data_collector.collect_data([self.backupset.restore_cluster_host],
                                                    [bucket], perNode=False,
                                                    getReplica=False, mode="memory")
                    client = VBucketAwareMemcached(rest, bucket.name)
                    self.log.info(" ** vbuckets should be restore: {0}".format(vbucket_filter))
                    for key in bucket_data[bucket.name]:
                        vBucketId = client._get_vBucket_id(key)
                        if self.debug_logs:
                            print(("This key {0} in vbucket {1}".format(key, vBucketId)))
                        if vBucketId not in vbucket_filter:
                            self.fail("vbucketId {0} of key {1} not from vbucket filters {2}" \
                                      .format(vBucketId, key, vbucket_filter))
            else:
                self.log.info("No keys with vbucket filter {0} restored".format(vbucket_filter))
        shell.disconnect()

    def _validate_restore_replace_ttl_with(self, ttl_set):
        data_collector = DataCollector()
        bk_file_data, _ = data_collector.get_kv_dump_from_backup_file(self.backupset.backup_host,
                                                                      self.cli_command_location, self.cmd_ext,
                                                                      "ent-backup", self.buckets,
                                                                      backupset=self.backupset)
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        rest = RestConnection(self.backupset.restore_cluster_host)
        restore_buckets_items = rest.get_buckets_itemCount()
        buckets = rest.get_buckets()
        keys_fail = {}
        max_failed_keys = 10
        for bucket in buckets:
            count = 0
            keys_fail[bucket.name] = {}
            if len(list(bk_file_data[bucket.name].keys())) != \
                    int(restore_buckets_items[bucket.name]):
                self.fail("Total keys do not match")
            items_info = rest.get_items_info(list(bk_file_data[bucket.name].keys()),
                                             bucket.name)
            ttl_matched = True
            for key in list(bk_file_data[bucket.name].keys()):
                if items_info[key]['meta']['expiration'] != int(ttl_set):
                    if self.replace_ttl == "all":
                        ttl_matched = False
                    if self.replace_ttl == "expired" and self.bk_with_ttl is not None:
                        ttl_matched = False
                    if count < max_failed_keys:
                        keys_fail[bucket.name][key] = []
                        keys_fail[bucket.name][key].append(items_info[key]['meta']['expiration'])
                        keys_fail[bucket.name][key].append(int(ttl_set))
                        count += 1
                    if self.debug_logs:
                        print(("ttl time set: ", ttl_set))
                        print(("key {0} failed to set ttl with {1}".format(key,
                                                                           items_info[key]['meta']['expiration'])))

            if not ttl_matched:
                self.log.error("Here are keys not set correcttly {0}".format(keys_fail))
                self.fail("ttl value did not set correctly")
            else:
                self.log.info("all ttl value set matched")
            if int(self.replace_ttl_with) == 0:
                if len(list(bk_file_data[bucket.name].keys())) != \
                        int(restore_buckets_items[bucket.name]):
                    self.fail("Keys do not restore with ttl set to 0")
            elif int(self.replace_ttl_with) > 0:
                output, _ = shell.execute_command("date +%s")
                sleep_time = int(ttl_set) - int(output[0])
                if sleep_time > 0:
                    if self.replace_ttl == "expired" and self.bk_with_ttl is None:
                        self.log.info("No need to wait.  No expired items in backup ")
                    elif sleep_time < 600:
                        self.sleep(sleep_time + 10, "wait for items to expire")
                    else:
                        self.log.info("No need to wait.  Expired time set > 5 mins")
                else:
                    self.log.info("time expired.  No need to wait")

                BucketOperationHelper.verify_data(self.backupset.restore_cluster_host,
                                                  list(bk_file_data[bucket.name].keys()),
                                                  False, False, self, bucket=bucket.name)
                k = 0
                bucket_ready = RestHelper(RestConnection(rest).vbucket_map_ready(bucket.name))
                while not bucket_ready and k < 10:
                    if k == 10:
                        self.fail("Bucket {0} is not ready after 10 seconds".format(bucket.name))
                    bucket_ready = RestHelper(RestConnection(rest).vbucket_map_ready(bucket.name))
                    self.sleep(1, "wait for bucket update new stats")
                    k += 1

                restore_buckets_items = rest.get_buckets_itemCount()
                if int(restore_buckets_items[bucket.name]) > 0:
                    if self.replace_ttl == "expired" and self.bk_with_ttl is not None:
                        if sleep_time < 600:
                            self.fail("Key did not expire after wait more than 10 seconds of ttl")
        shell.disconnect()

    def _verify_bucket_compression_mode(self, restore_bucket_compression_mode):
        if self.enable_firewall and self.should_fail:
            self.log.info("No need to verify.  This is negative test")
            return
        rest = RestConnection(self.backupset.restore_cluster_host)
        cb_version = rest.get_nodes_version()
        if 5.5 > float(cb_version[:3]):
            self.log.info("This version is pre vulcan.  No need to verify compression mode.")
            return
        buckets = rest.get_buckets()
        bucket_compression_mode = []
        for bucket in buckets:
            compressionMode = rest.get_bucket_compressionMode(bucket=bucket.name)
            bucket_compression_mode.append(compressionMode)
        for compressionMode in bucket_compression_mode:
            if compressionMode != restore_bucket_compression_mode:
                self.fail("cbbackupmgr modified bucket pre-config.")

    def generate_docs_simple(self, num_items, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start=start, docs_per_day=self.docs_per_day)

    def ordered(self, obj):
        if isinstance(obj, dict):
            return sorted((k, self.ordered(v)) for k, v in list(obj.items()))
        if isinstance(obj, list):
            return sorted(self.ordered(x) for x in obj)
        else:
            return obj

    def _validate_num_files(self, extension_name, num_shards, bucket_name):
        """
           Validate number of file in backup repo
        """
        num_files_matched = False
        mesg = ""
        now = datetime.datetime.now()
        cmd = "ls {0}/backup/{1}*/{2}*/data/ | ".format(self.backupset.directory, now.year,
                                                        bucket_name)
        cmd += "grep{0} {1} | wc -l ".format(self.cmd_ext, extension_name)
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = shell.execute_command(cmd)
        if num_shards is None:
            num_shards = 1024
        self.log.info("\n**** Verify number of files with extension {0} *****" \
                      .format(extension_name))
        if output and int(output[0]) != int(num_shards):
            mesg = "number of shards do not match.  Pass: {0} vs actual: {1}" \
                .format(num_shards, output[0])
        else:
            num_files_matched = True
            self.log.info("Number of shards with extension {0} is {1}" \
                          .format(extension_name, output[0]))
        return num_files_matched, mesg

    def _shards_modification(self, action):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        for bucket in self.buckets:
            num_shards = 0
            now = datetime.datetime.now()
            backup_date = now.year
            """ handle change in daylight saving time """
            time_diff = "08_00"
            is_dst = datetime.time.localtime().tm_isdst
            if is_dst:
                time_diff = "07_00"

            cmd1 = "cd {0}/backup/; ls  | grep{1} '{2}' " \
                .format(self.backupset.directory, self.cmd_ext, time_diff)
            output, error = shell.execute_command(cmd1)

            if output and len(output) >= 1:
                output = sorted([s.replace(':', '') for s in output])
            if self.backupset.number_of_backups > 1:
                if not self.bk_merged:
                    backup_date = output[1]
                else:
                    backup_date = output[0]
            else:
                backup_date = output[0]

            cmd2 = "ls {0}/backup/{1}/{2}*/data/ | grep{3} .sqlite.0 | wc -l" \
                .format(self.backupset.directory, backup_date,
                        bucket.name, self.cmd_ext)
            output, error = shell.execute_command(cmd2)
            if output and int(output[0]) > 0:
                num_shards = int(output[0])

            if action == "remove":
                self.log.info("Remove a shard in backup repo")
                cmd = "cd {0}/backup/{1}*/{2}*/data/; rm -f shard_1.sqlite.0  " \
                    .format(self.backupset.directory, backup_date, bucket.name)

                output, error = shell.execute_command(cmd)
                cmd = "cd {0}/backup/{1}*/{2}*/data/; ls | grep{3} .sqlite.0 | wc -l" \
                    .format(self.backupset.directory, backup_date, bucket.name,
                            self.cmd_ext)
                output, error = shell.execute_command(cmd)
            if action == "duplicate":
                self.log.info("Duplicate one shard in backup repo")
                cmd = "cd {0}/backup/{1}*/{2}*/data/; cp shard_1.sqlite.0 shard_{3}.sqlite.0" \
                    .format(self.backupset.directory,
                            backup_date, bucket.name, num_shards + 1)
                output, error = shell.execute_command(cmd)
            if action == "corrupted":
                """ This test needs set to 20 shards to make sure all shards have data """
                if num_shards > 20:
                    self.fail("This test needs to set 20 shards to run")
                if num_shards > 0:
                    self.log.info("Corrupted a shard in backup repo")
                    cmd = "cd {0}/backup/{1}*/{2}*/data/; echo 'Hello world' > shard_1.sqlite.0" \
                        .format(self.backupset.directory, backup_date,
                                bucket.name)
                    output, error = shell.execute_command(cmd)
        shell.disconnect()

    def _compare_vbuckets_each_shard(self):
        vbuckets_per_shard = {}
        now = datetime.datetime.now()
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        for bucket in self.buckets:
            vbuckets_per_shard[bucket.name] = {}
            print("---- Collecting vbuckets in each shard in backup repo")
            cmd1 = "ls {0}/backup/{1}*/{2}*/data | grep \\.sqlite | wc -l " \
                .format(self.backupset.directory, now.year, bucket.name)
            num_shards, error = shell.execute_command(cmd1)

            if num_shards and int(num_shards[0]) > 0:
                for i in range(0, int(num_shards[0])):
                    cmd2 = "{0}forestdb_dump{1} --plain-meta " \
                           "{2}/backup/{3}*/{4}*/data/shard_{5}.sqlite.0 | grep partition | wc -l " \
                        .format(self.cli_command_location, self.cmd_ext, \
                                self.backupset.directory, now.year, \
                                bucket.name, i)
                    output, error = shell.execute_command(cmd2, debug=False)
                    vbuckets_per_shard[bucket.name][i] = int(output[0])
        shell.disconnect()

    def _create_backup_cluster(self, node_services=["kv,index"]):
        rest_bk = RestConnection(self.backupset.cluster_host)
        bk_bucket = rest_bk.get_buckets()
        if bk_bucket:
            BucketOperationHelper.delete_all_buckets_or_assert(self.backupset.restore_cluster, self)
        ClusterOperationHelper.cleanup_cluster(self.backupset.restore_cluster,
                                               master=self.backupset.restore_cluster_host)

        rest_bk = RestConnection(self.backupset.cluster_host)
        rest_bk.force_eject_node()
        ready = RestHelper(rest_bk).is_ns_server_running()
        if ready:
            shell = RemoteMachineShellConnection(self.backupset.cluster_host)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
        kv_quota = rest_bk.init_node(node_services)
        rest_bk.set_indexer_storage_mode(username='Administrator',
                                      password='password')
        self.add_built_in_server_user(node=self.backupset.cluster_host)

    def _create_restore_cluster(self, node_services=["kv"]):
        rest_rs = RestConnection(self.backupset.restore_cluster_host)
        rs_bucket = rest_rs.get_buckets()
        if rs_bucket:
            BucketOperationHelper.delete_all_buckets_or_assert(self.backupset.restore_cluster, self)
        ClusterOperationHelper.cleanup_cluster(self.backupset.restore_cluster,
                                               master=self.backupset.restore_cluster_host)

        rest_bk = RestConnection(self.backupset.cluster_host)
        eventing_service_in = False
        index_service_in = False
        fts_service_in = False
        bk_cluster_services = list(rest_bk.get_nodes_services().values())
        print("\nbk service in reset restore: ", bk_cluster_services)
        for srv in bk_cluster_services:
            print("\n service: ", srv)
            if "eventing" in srv:
                eventing_service_in = True
            if "index" in srv:
                index_service_in = True
            if "fts" in srv:
                fts_service_in = True
        if index_service_in:
            bk_storage_mode = rest_bk.get_index_settings()["indexer.settings.storage_mode"]
        else:
            bk_storage_mode = "plasma"
        bk_services = max(bk_cluster_services, key=len)

        rest_rs = RestConnection(self.backupset.restore_cluster_host)
        nodes_all = rest_rs.node_statuses()
        if self.test_fts and "fts" not in bk_services:
            bk_services.append("fts")
        if "eventing" not in bk_services and eventing_service_in:
            bk_services.append("eventing")
        self.backupset.restore_cluster_host.services = ",".join(bk_services)
        rest_rs.force_eject_node()
        ready = RestHelper(rest_rs).is_ns_server_running()
        shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        if ready:
            shell.enable_diag_eval_on_non_local_hosts()
        sv_in_rs = self.backupset.restore_cluster_host.services
        if self.backupset.restore_cluster_host.services and \
            "," in self.backupset.restore_cluster_host.services[0]:
            sv_in_rs = self.backupset.restore_cluster_host.services[0].split(",")
        kv_quota = rest_rs.init_node(sv_in_rs)
        """ set node to hostname if hostname=true """
        if self.hostname and self.backupset.restore_cluster_host.ip.endswith(".com"):
            self.log.info("\n*** Set node with hostname")
            cmd_init = 'node-init'
            options = '--node-init-hostname ' + self.backupset.restore_cluster_host.ip
            output, _ = shell.execute_couchbase_cli(cli_command=cmd_init, options=options,
                                        cluster_host="localhost",
                                        user=self.backupset.restore_cluster_host.rest_username,
                                        password=self.backupset.restore_cluster_host.rest_password)
            if not self._check_output("SUCCESS: Node initialize", output):
                raise("Failed to set hostname")
        shell.disconnect()
        rest_rs.set_indexer_storage_mode(username=self.backupset.restore_cluster_host.rest_username,
                                      password=self.backupset.restore_cluster_host.rest_password,
                                      storageMode=bk_storage_mode)
        self.log.info("Done reset node with storage mode")
        #self._reset_storage_mode(rest_rs, bk_storage_mode, False)
        if len(bk_cluster_services) > 1:
            bk_cluster_services.remove(bk_services)
        if len(self.input.clusters[0]) > 1:
            if not bk_cluster_services:
                bk_cluster_services = bk_cluster_services.append(self.input.clusters[0][1].services)
            self.sleep(12, "time needs for index service is up (MB-39859)")
            rest_rs.add_node(self.input.clusters[0][1].rest_username,
                    self.input.clusters[0][1].rest_password,
                    self.input.clusters[0][1].cluster_ip, services=bk_cluster_services[0])
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [], [])
            rebalance.result()
            count = 0
            while count < 15:
                reb_st, _ = rest_rs._rebalance_status_and_progress()
                if reb_st != "none":
                    self.sleep(2, "wait rebalance to complete")
                    count += 1
                else:
                    break
                if count == 15:
                    self.fail("Status still shows running after rebalance complete 30 seconds")
        else:
            self.log.info("No availabe node to create cluster.")

    def _extract_collection_names(self, collections):
        if collections and collections[0]:
            collections = collections[0]
            collections = collections[1::2]
            collections = [x.replace("-", "") for x in collections]
            collections = [x.strip() for x in collections]
            return collections
        else:
            self.fail("Need to pass collection list")


class Backupset:
    def __init__(self):
        self.backup_host = None
        self.directory = ''
        self.name = ''
        self.cluster_host = None
        self.cluster_host_username = ''
        self.cluster_host_password = ''
        self.cluster_new_user = None
        self.cluster_new_role = None
        self.backup_cluster = None
        self.restore_cluster = None
        self.restore_cluster_host = None
        self.restore_cluster_host_username = ''
        self.restore_cluster_host_password = ''
        self.threads = ''
        self.exclude_buckets = ""
        self.include_buckets = ""
        self.disable_bucket_config = False
        self.disable_views = False
        self.disable_gsi_indexes = False
        self.disable_ft_indexes = False
        self.disable_data = False
        self.disable_conf_res_restriction = False
        self.force_updates = False
        self.resume = False
        self.purge = False
        self.start = 1
        self.end = 1
        self.number_of_backups = 1
        self.number_of_backups_after_upgrade = 1
        self.filter_keys = ''
        self.filter_values = ''
        self.no_ssl_verify = False
        self.random_keys = False
        self.secure_conn = False
        self.bk_no_cert = False
        self.rt_no_cert = False
        self.backup_list_name = ''
        self.backup_incr_backup = ''
        self.bucket_backup = ''
        self.backup_to_compact = ''
        self.map_buckets = None
        self.map_bucket_name = []
        self.delete_old_bucket = False
        self.backup_compressed = False
        self.user_env = False
        self.log_archive_env = False
        self.no_log_output_flag = False
        self.ex_logs_path = None
        self.log_redaction = None
        self.redaction_salt = None
        self.passwd_env = False
        self.overwrite_user_env = False
        self.overwrite_passwd_env = False
        self.user_env_with_prompt = False
        self.passwd_env_with_prompt = False
        self.deleted_buckets = []
        self.new_buckets = []
        self.flushed_buckets = []
        self.deleted_backups = []
        self.log_to_stdout = False
        self.auto_select_threads = False
        self.date_range = ''
        self.scopes = None
        self.collections = None
        self.map_data_collection = False
        self.col_per_scope = 1
        self.info_all = True
        self.info_to_json = True
        self.collection_string = None
        self.load_to_collection = False
        self.load_scope_id = None
        self.disable_ft_alias = False
        self.disable_analytics = False
        self.auto_create_buckets = False
        self.create_restore_buckets = True
        self.exist_scopes_in_restore_buckets = False
        self.exist_collections_in_restore_buckets = False
        self.custom_scopes = False
        self.custom_collections = False
        self.backup_services_init = None

        # Common configuration which is to be shared accross cloud providers
        self.objstore_access_key_id = ""
        self.objstore_bucket = ""
        self.objstore_cacert = ""
        self.objstore_endpoint = ""
        self.objstore_no_ssl_verify = False
        self.objstore_region = ""
        self.objstore_secret_access_key = ""
        self.objstore_staging_directory = ""

        # S3 specific configuration
        self.s3_force_path_style = False
