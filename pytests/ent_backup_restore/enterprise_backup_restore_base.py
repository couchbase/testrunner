import copy
import os, shutil, ast, re, subprocess
import json
import uuid
import random
import queue
import urllib.request, urllib.parse, urllib.error, datetime

from boto3 import s3, resource
from botocore.exceptions import ClientError
from basetestcase import BaseTestCase
from TestInput import TestInputSingleton, TestInputServer
from couchbase_helper.data_analysis_helper import DataCollector
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from ent_backup_restore.validation_helpers.backup_restore_validations \
    import BackupRestoreValidations
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from membase.api.rest_client import RestHelper, RestConnection, \
    Bucket as CBBucket
from couchbase_helper.document import View
from testconstants import LINUX_COUCHBASE_BIN_PATH, \
    COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH_RAW, \
    WIN_COUCHBASE_BIN_PATH_RAW, WIN_COUCHBASE_BIN_PATH, WIN_TMP_PATH_RAW, \
    MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH, \
    WIN_TMP_PATH, STANDARD_BUCKET_PORT, WIN_CYGWIN_BIN_PATH
from testconstants import INDEX_QUOTA, FTS_QUOTA, \
    CLUSTER_QUOTA_RATIO, GCP_AUTH_PATH
from security.rbac_base import RbacBase
from couchbase.bucket import Bucket
from tasks.future import Future

from lib.couchbase_helper.tuq_generators import JsonGenerator
from lib.memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper

from ent_backup_restore.provider.s3 import S3
from ent_backup_restore.provider.gcp import GCP
from ent_backup_restore.provider.azure import AZURE

from pytests.security.x509_multiple_CA_util import x509main

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


class EnterpriseBackupRestoreBase(BaseTestCase):
    def setUp(self):
        super().setUp()
        """ from version 4.6.0 and later, --host flag is deprecated """
        self.cluster_flag = "--host"
        self.backupset = Backupset()
        self.cmd_ext = ""
        self.backup_outputs = []
        self.restore_outputs = []
        self.backup_errors = []
        self.restore_errors = []
        self.create_bucket_count = 0
        self.restore_consistent_metadata = self.input.param("restore_consistent_metadata", False)
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

        cmd = 'curl -g -k {4}{0}:{3}/diag/eval -u {1}:{2} '.format(self.master.ip,
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
        if self.input.param("tools_package", False):
            self.previous_cli = self.cli_command_location
            self.cli_command_location = "/tmp/tools_package/couchbase-server-dev-tools-*/bin/"
        if self.input.param("admin_tools_package", False):
            self.previous_cli = self.cli_command_location
            self.cli_command_location = "/tmp/tools_package/couchbase-server-admin-tools-*/bin/"

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
                                "{0}xargs -I {{}} date --date=@'{{}}' --rfc-3339=seconds | " \
                                    .format(self.cygwin_bin_path) + \
                                "sed 's/ /T/'"
            win_format = "C:/Program Files"
            cygwin_format = "/cygdrive/c/Program\\ Files"
            if win_format in self.cli_command_location:
                self.cli_command_location = self.cli_command_location.replace(win_format,
                                                                              cygwin_format)
            self.backupset.directory = self.input.param("dir", WIN_TMP_PATH_RAW + "entbackup")
            if self.input.param("tools_package", False):
                self.previous_cli = self.cli_command_location
                self.cli_command_location = "/cygdrive/c/tmp/tools_package/couchbase-server-dev-tools-*/bin/"
            if self.input.param("admin_tools_package", False):
                self.previous_cli = self.cli_command_location
                self.cli_command_location = "/cygdrive/c/tmp/tools_package/couchbase-server-admin-tools-*/bin/"
        elif info == 'mac':
            self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        else:
            raise Exception("OS not supported.")
        self.backup_validation_files_location = "/tmp/backuprestore" + self.master.ip
        if self.input.bkrs_client is not None:
            self.backupset.backup_host = self.input.bkrs_client
        else:
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
        self.bkinfo_start = self.input.param("bkinfo_start", 0)
        self.bkinfo_date_start_ago = self.input.param("bkinfo_date_start_ago", 0)
        self.bkinfo_end = self.input.param("bkinfo_end", 0)
        self.bkinfo_start_end_with_bkname = self.input.param("bkinfo_start_end_with_bkname", None)
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
        self.reset_restore_cluster = self.input.param("reset-restore-cluster", True)
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
        """ Get cb version of cbm client from bkrs client server """
        if self.backupset.current_bkrs_client_version is None:
            self.backupset.current_bkrs_client_version = \
                        self._get_current_bkrs_client_version()
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
        self.backupset.disable_ft_alias = self.input.param("disable-ft-alias", False)
        self.backupset.disable_analytics = self.input.param("disable-analytics", False)
        self.backupset.disable_data = self.input.param("disable-data", False)
        self.backupset.enable_users = self.input.param("enable-users", False)
        self.backupset.disable_conf_res_restriction = self.input.param("disable-conf-res-restriction", False)
        self.backupset.force_updates = self.input.param("force-updates", False)
        self.backupset.auto_create_buckets = self.input.param("auto-create-buckets", False)
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
        self.backupset.delete_old_bucket = self.input.param("delete-old-bucket", False)
        self.add_node_services = self.input.param("add-node-services", "kv")
        self.backupset.backup_compressed = \
            self.input.param("backup-conpressed", False)
        self.backups = []
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
        if not os.path.exists(self.backup_validation_files_location):
            os.mkdir(self.backup_validation_files_location)
        shell.disconnect()

        # Common configuration which are shared accross cloud providers
        self.backupset.objstore_access_key_id = self.input.param('access_key_id', '')
        self.backupset.objstore_bucket = self.input.cbbackupmgr_param('bucket', str(uuid.uuid1()))
        self.backupset.objstore_cacert = self.input.cbbackupmgr_param('cacert', '')
        self.backupset.objstore_endpoint = self.input.cbbackupmgr_param('endpoint', '')
        self.backupset.objstore_no_ssl_verify = self.input.cbbackupmgr_param('no_ssl_verify', False)
        self.backupset.objstore_region = self.input.cbbackupmgr_param('region', '')
        self.backupset.objstore_secret_access_key = self.input.param('secret_access_key', '') # Required
        self.backupset.objstore_staging_directory = self.change_root_of_path(info, self.input.cbbackupmgr_param('staging_directory'))

        # S3 specific configuration
        self.backupset.s3_force_path_style = self.input.cbbackupmgr_param('s3_force_path_style', False)

        # The setup/teardown is provider specific, this is hidden behind the 'Provider' abstract class
        self.objstore_provider = None
        provider = self.input.param("objstore_provider", None)
        provider_region = self.input.param("objstore_provider_region", None)
        if provider_region != None:
            self.backupset.objstore_region = provider_region
        if provider == "s3":
            self.objstore_provider = S3(self.backupset.objstore_access_key_id, self.backupset.objstore_bucket,
                                        self.backupset.objstore_cacert, self.backupset.objstore_endpoint,
                                        self.backupset.objstore_no_ssl_verify, self.backupset.objstore_region,
                                        self.backupset.objstore_secret_access_key,
                                        self.backupset.objstore_staging_directory)
        elif provider == "gcp":
            GCP_AUTH_PATH_src = os.getenv("GCP_AUTH_PATH", GCP_AUTH_PATH)
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.execute_command("mkdir -p /root/.config/gcloud")
                shell.copy_file_local_to_remote(GCP_AUTH_PATH_src, GCP_AUTH_PATH)
            self.objstore_provider = GCP(self.backupset.objstore_access_key_id, self.backupset.objstore_bucket,
                                         self.backupset.objstore_cacert, self.backupset.objstore_endpoint,
                                         self.backupset.objstore_no_ssl_verify, self.backupset.objstore_region,
                                         self.backupset.objstore_secret_access_key,
                                         self.backupset.objstore_staging_directory, f"archive-{self.master.ip}")
        elif provider == "azure":
             self.objstore_provider = AZURE(self.backupset.objstore_access_key_id, self.backupset.objstore_bucket,
                                        self.backupset.objstore_cacert, self.backupset.objstore_endpoint,
                                        self.backupset.objstore_no_ssl_verify, self.backupset.objstore_region,
                                        self.backupset.objstore_secret_access_key,
                                        self.backupset.objstore_staging_directory, f"archive-{self.master.ip}")

        # We run in a separate branch so when we add more providers the setup will be run by default
        if provider:
            # Override and use the same archive directory across platforms
            self.backupset.directory = f"archive-{self.master.ip}"
            self.objstore_provider.setup()
            if (provider == "azure"):
                self.backupset.objstore_secret_access_key = self.objstore_provider.access_key_id
                self.backupset.objstore_access_key_id = self.objstore_provider.storage_name
                self.backupset.objstore_bucket = self.objstore_provider.container_name

        self.validation_helper = BackupRestoreValidations(self.backupset,
                                                          self.cluster_to_backup,
                                                          self.cluster_to_restore,
                                                          self.buckets,
                                                          self.backup_validation_files_location,
                                                          self.backups,
                                                          self.num_items,
                                                          self.vbuckets,
                                                          self.objstore_provider)

        self.multi_root_certs = self.input.param("x509", False)
        if self.multi_root_certs:
            self.setup_multi_root_certs()

        self.backupset.read_only = False

    def clean_up(self, server, skip_eject_and_rebalance=False):
        """ Clean up files and directories left by backup testing.

        Params:
            server (TestInputServer): The server to clean up.
        """
        remote_client = RemoteMachineShellConnection(server)
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
        if not skip_eject_and_rebalance and self.input.clusters:
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

        # testrunner runs the teardown before running setup which means the 'objstore_provider' will be undefined
        # we can continue in this case knowing that we haven't yet loaded any data into the cloud.
        try:
            if self.objstore_provider:
                self.objstore_provider.teardown(info, remote_client)
                # Foor Azure we're deleting the remote storage resoruce
                if self.input.param("objstore_provider", None) == "azure":
                    self.objstore_provider.__del__()
        except AttributeError:
            pass

        remote_client.disconnect()

    def tearDown(self):
        super(EnterpriseBackupRestoreBase, self).tearDown()
        if not self.input.param("skip_cleanup", False):
            try:
                self.clean_up(self.input.clusters[1][0])
            except AssertionError as e:
                if "bucket \"default\" was not deleted" in e:
                    self.log.info("Ignore trying to delete the default bucket.")
                else:
                    raise e
        if self.enforce_tls:
            self.master.port = '8091'
            self.master.protocol = "http://"
            for server in self.servers:
                server.port = '8091'
        if self.input.param("x509", False):
            self.x509 = x509main(host=self.master)
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.execute_command(f"rm -rf {self.x509.CACERTFILEPATH}")
                self.x509.delete_inbox_folder_on_server(server=server)
            self.x509.teardown_certs(servers=self.servers)

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

    def change_root_of_path(self, os_info, path):
        """ If the operating system is windows changes the path's root to c:/ """
        if os_info == "windows":
            path = re.sub(r"^/", "c:/", path)
        return path

    def _get_current_bkrs_client_version(self):
        self.backupset.current_bkrs_client_version = \
                        RestConnection(self.backupset.backup_host).get_nodes_version()
        self.backupset.current_bkrs_client_version = \
                   "-".join(self.backupset.current_bkrs_client_version.split('-')[:2])
        return self.backupset.current_bkrs_client_version

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

    def backup_create(self, del_old_backup=True):
        args = (
            f"config --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
            f" --repo {self.backupset.name}"
            f"{self._common_objstore_arguments()}"
        )

        cbbkmgr_version = RestConnection(self.backupset.backup_host).get_nodes_version()
        exclude_bucket = " --exclude-data "
        include_bucket = " --include-data "
        if cbbkmgr_version[:3] == "6.6":
            # in 6.6, flags keep the same name with 6.5
            exclude_bucket = " --exclude-buckets "
            include_bucket = " --include-buckets "
        if self.backupset.exclude_buckets:
            args += " {0} \"{1}\"".format(exclude_bucket,
                                          ",".join(self.backupset.exclude_buckets))
        if self.backupset.include_buckets:
            args += " {0} \"{1}\"".format(include_bucket,
                                          ",".join(self.backupset.include_buckets))
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
        if self.backupset.enable_users:
            args += " --enable-users"
        if self.backupset.current_bkrs_client_version[:3] >= "7.0":
            if self.vbucket_filter is not None:
                args += " --vbucket-filter {0}".format(self.vbucket_filter)
        if type(self.objstore_provider) == GCP:
            args += " --vbucket-filter 0-63"

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
            # Since we are just wiping out the archive here, we can just run the object store teardown
            if self.objstore_provider:
                self.objstore_provider.teardown(remote_client.extract_remote_info().type.lower(), remote_client)

            remote_client.execute_command("rm -rf {0}".format(self.backupset.directory))
            if self.os_info == "windows":
                remote_client.execute_command("rm -rf /cygdrive/c/tmp/cbbackupmgr-staging")

        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        return output, error

    def backup_info(self, json_out=True, start=None, end=None):
        args = (
            f"info --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
            f" --repo {self.backupset.name} "
            f"{self._common_objstore_arguments()}"
            f"{' --json' if json_out else ''}"
        )

        if start is not None:
            args += " --start {}".format(start)
            if end is not None:
                args += " --end {}".format(end)
            else:
                self.fail("start flag needs end flag to work")
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        return output, error

    def _verify_backup_directory_count(self, expected):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        command = (
            f"ls -l {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name} | wc -l"
        )

        output, _ = shell.execute_command(command)
        output = " ".join(output)

        try:
            count = int(output)
            self.assertEqual(count - 3, expected,
                             "Number of backup directories {0} does not match expected {1}".format(count - 3, expected))
        except ValueError as e:
            self.fail("Could not get the number of backups in the archive due to: {0}".format(e))

        shell.disconnect()

        # It's important that we check to ensure the backups were removed in object store as well as in the staging
        # directory.
        if self.objstore_provider:
            count = len(self.objstore_provider.list_backups(self.backupset.directory, self.backupset.name))
            self.assertEqual(count, expected,
                             f"Number of remote backup directories {count} does not match expected {expected}")

    def backup_create_validate(self):
        output, error = self.backup_create()
        if error or "Backup repository `{0}` created successfully".format(self.backupset.name) not in output[0]:
            self.fail("Creating backupset failed.")
        status, msg = self.validation_helper.validate_backup_create()
        if not status:
            self.fail(msg)
        self.log.info(msg)

    def backup_cluster(self, threads_count=None):
        if threads_count is None:
            threads_count = self.threads_count
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

        if self.input.param("x509", False):
            user_input = f" --client-cert {self.backupset.client_certs[2][0]}"
            password_input = f" --client-key {self.backupset.client_certs[2][1]}"

        """ Print out of cbbackupmgr from 6.5 is different with older version """
        self.cbbkmgr_version = "6.5"
        self.bk_printout = ["Backup completed successfully", "Backup successfully completed"]
        if RestHelper(RestConnection(self.backupset.backup_host)).is_ns_server_running():
            self.cbbkmgr_version = RestConnection(self.backupset.backup_host).get_nodes_version()

        if "4.6" <= self.cbbkmgr_version[:3]:
            self.cluster_flag = "--cluster"

        args = (
            f"backup --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
            f" --repo {self.backupset.name}"
            f" {self.cluster_flag} http{url_format}://{self.backupset.cluster_host.cluster_ip}:{secure_port}{self.backupset.cluster_host.port}"
            f" {user_input}"
            f" {password_input}"
            f"{' --full-backup' if self.backupset.full_backup else ''}"
            f"{self._common_objstore_arguments()}"
        )

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
        if self.backupset.sqlite:
            args += " --storage sqlite"
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
        self.backup_outputs.append(output)
        self.backup_errors.append(error)
        if self.debug_logs:
            remote_client.log_command_output(output, error)

        for bucket in self.buckets:
            self.bk_printout.append('Backed up bucket "{0}" succeeded'.format(bucket.name))
            if error or not self._check_output(self.bk_printout, output):
                self.log.error("Failed to backup bucket {0}".format(bucket.name))
                return output, error

        pattern = r"^\d{4}-\d{2}-\d{2}"
        command = (
            f"ls -tr {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name}/ | grep -P '{pattern}' | tail -1"
        )

        o, e = remote_client.execute_command(command)
        if o:
            self.backups.append(o[0])
        self.number_of_backups_taken += 1
        self.log.info("Finished taking backup  with args: {0}".format(args))
        remote_client.disconnect()
        if self.input.param("read_only_archive", False) and not self.backupset.read_only:
            self.make_archive_read_only()
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

    def backup_restore(self):
        if self.restore_only:
            self.backups.append("2020-07-28T16_20_05.720270674-07_00")
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
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.restore_cluster_host)
            url_format = "s"
            secure_port = "1"
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
        if "4.6" <= self.backupset.current_bkrs_client_version[:3]:
            self.cluster_flag = "--cluster"

        args = (
            f"restore --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
            f" --repo {self.backupset.name}"
            f" {self.cluster_flag} http{url_format}://{self.backupset.restore_cluster_host.cluster_ip}:{secure_port}{self.backupset.restore_cluster_host.port}"
            f" {user_input}"
            f" {password_input}"
            f" --start {backup_start}"
            f" --end {backup_end}"
            f"{self._common_objstore_arguments()}"
        )

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
            args += f" {'--exclude-data' if int(version[0]) >= 7 else '--exclude-buckets'} {''.join(self.backupset.exclude_buckets)}"
        if self.backupset.include_buckets:
            args += f" {'--include-data' if int(version[0]) >= 7 else '--include-buckets'} {''.join(self.backupset.include_buckets)}"
        if self.backupset.enable_users:
            args += " --enable-users"
        if self.backupset.disable_views:
            args += " --disable-views "
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes"
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes"
        if self.backupset.disable_data:
            args += " --disable-data"
        if self.backupset.disable_conf_res_restriction:
            args += " --disable-conf-res-restriction"
        if self.backupset.auto_create_buckets:
            RestConnection(self.backupset.cluster_host).delete_all_buckets()
            args += " --auto-create-buckets"
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
        if self.backupset.resume:
            args += " --resume"
        if self.no_progress_bar:
            args += " --no-progress-bar"
        if self.multi_threads:
            args += " --threads %s " % self.threads_count
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
            if self.dgm_run:
                bucket_size = 256
            else:
                bucket_size = int(bucket_size * .7)
            if self.bucket_storage == 'magma':
                rest_conn.set_internalSetting("magmaMinMemoryQuota", 256)
                if bucket_size < 256:
                    bucket_size = 256

            count = 0
            buckets = []
            replicas = self.num_replicas
            if self.new_replicas:
                replicas = self.new_replicas
            reset_restore_cluster = False
            reset_cluster_count = 0
            for bucket in self.buckets:
                if self.create_bucket_count > 0:
                    break
                bucket_name = bucket.name
                if not rest_helper.bucket_exists(bucket_name):
                    if self.backupset.map_buckets is None:
                        self.log.info("Creating bucket {0} in restore host {1}"
                                      .format(bucket_name,
                                              self.backupset.restore_cluster_host.ip))
                    elif self.backupset.map_buckets:
                        self.log.info("Create new bucket name to restore to this bucket")
                        bucket_maps = ""
                        bucket_name = bucket.name + "_" + str(count)
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
                            bucket_size = (kv_quota * 0.7)
                    if not reset_restore_cluster and reset_cluster_count == 0:
                        if self.create_bucket_count == 0:
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
            bucket_maps = ",".join(buckets)
        if self.backupset.map_buckets:
            args += " --map-buckets %s " % bucket_maps
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
                    elif isinstance(self.replace_ttl_with, str):
                        args += " --replace-ttl {0} --replace-ttl-with {1}" \
                            .format(self.replace_ttl, self.replace_ttl_with)
                    else:
                        ttl_date, _ = shell.execute_command(self.rfc3339_date)
                        if ttl_date[0] == '':
                            ttl_date, _ = shell.execute_command("date --date='{0}\
                                    seconds' --rfc-3339=seconds".format(self.replace_ttl_with))
                            ttl_date[0] = ttl_date[0].replace(" ","T")
                        self.seconds_with_ttl, _ = shell.execute_command(self.seconds_with_ttl)
                        if self.seconds_with_ttl:
                            self.ttl_value = self.seconds_with_ttl[0]
                        if ttl_date and ttl_date[0]:
                            args += " --replace-ttl {0} --replace-ttl-with {1}" \
                                .format(self.replace_ttl, ttl_date[0])
            elif self.replace_ttl == "add-none":
                args += " "
            elif self.replace_ttl == "empty-flag":
                args += " --replace-ttl-with {0}".format(self.replace_ttl_with)
            else:
                args += " --replace-ttl {0}".format(self.replace_ttl)
        command = "{3} {2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                       password_env, user_env)
        output, error = shell.execute_command_raw(command)
        self.restore_outputs.append(output)
        self.restore_errors.append(error)
        shell.log_command_output(output, error)
        if (not self.enable_firewall) and ("community" not in getattr(self, 'cb_version', '')):
            self._verify_bucket_compression_mode(bucket_compression_mode)

        eventing_service_in = False
        errs_check = ["Unable to process value", "Error restoring cluster",
                      "Expected argument for option",
                      "Error restoring cluster: failed to lock archive"]
        disable_firewall = False
        if self.enable_firewall:
            rs_conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
            rs_conn.disable_firewall()
            rs_conn.disconnect()
            disable_firewall = True
        rest_rs = RestConnection(self.backupset.restore_cluster_host)
        rs_cluster_services = list(rest_rs.get_nodes_services().values())
        for srv in rs_cluster_services:
            if "eventing" in srv:
                eventing_service_in = True
                errs_check.append("User needs one of the following permissions: cluster.eventing")
        if disable_firewall:
            RemoteUtilHelper.enable_firewall(self.backupset.restore_cluster_host)
        accepted_errs = False
        for err in errs_check:
            if self._check_output(err, output):
                accepted_errs = True
                break

        if accepted_errs and not self.restore_consistent_metadata:
            if not self.should_fail:
                if not self.restore_should_fail and not eventing_service_in:
                    if not self.enable_firewall:
                        self.fail("Failed to restore cluster")
            else:
                self.log.info("This test is for negative test")
        res = output
        res.extend(error)
        error_str = "Error restoring cluster: Transfer failed. " \
                    "Check the logs for more information."
        if error_str in res:
            bk_log_file_name = "backup*log"

            command = (
                f"cat {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
                f"{self.backupset.directory}/logs/{bk_log_file_name} | grep {error_str} -A 10 -B 100"
            )

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
                                expected_error=None,doc_store=None,restore_iterator=0,connector=None,
                                validation_list=None,validate_final_backup=False):

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
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        bk_log_file_name = "backup*log"
        ipv6_raw_format = [":", "[", "]"]
        ipv6_raw_ip = False
        bk_raw_ipv6_dir = ""
        if ":" in self.backupset.directory:
            ipv6_raw_ip = True
            bk_raw_ipv6_dir = self.backupset.directory
            for x in ipv6_raw_format:
                bk_raw_ipv6_dir = bk_raw_ipv6_dir.replace(x, "\\" + x)
        bk_dir = f"{self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}{self.backupset.directory}"
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
        if not self.backupset.force_updates:
            current_vseqno = self.get_vbucket_seqnos(self.cluster_to_restore, self.buckets,
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
            if self.expires:
                self.log.info("Remove expired items by checking them")
                self._verify_all_buckets(self.backupset.restore_cluster_host)

            info = remote_client.extract_remote_info().type.lower()
            check_dir = bk_dir if info != "windows" else bk_dir.replace("C\\:", "/cygdrive/c")
            output = remote_client.list_files(f"{check_dir}/backup")
            if not output:
                self.fail("Could not read backup directory")
            backups_on_disk = len(output) - 3
            if doc_store:
                restore_doc_store = []
                for k in validation_list:
                    status,content = connector.get_doc_by_key(k)
                    if bool(status):
                        restore_doc_store.append(json.loads(content.decode()).get("base64"))
                    else:
                        self.fail(f"Failed to get doc with key {k}")

                if validate_final_backup:
                    for i in range(10):
                        index = len(doc_store)-10+i
                        if doc_store[index] != restore_doc_store[i]:
                            self.fail(f"Backup Doc id:{i} != Restore Doc id:{i}\n")
                    self.log.info(f"SUCCESS. Restore data validated\n")
                else:
                    for i in range(10):
                        ofset = 10*restore_iterator
                        index = ofset + i
                        if doc_store[index] != restore_doc_store[i]:
                            self.fail(f"Backup Doc id:{i} != Restore Doc id:{i}\n")
                    self.log.info(f"SUCCESS. Restore data validated\n")

            else:
                status, msg = self.validation_helper.validate_restore(self.backupset.end,
                                                                    self.vbucket_seqno, current_vseqno,
                                                                    compare_uuid=compare_uuid,
                                                                    compare=seqno_compare_function,
                                                                    get_replica=replicas, mode=mode,
                                                                    backups_on_disk=backups_on_disk)

                """ limit the length of message printout to 3000 chars """
                info = str(msg)[:3000] + '..' if len(str(msg)) > 3000 else msg
                if not status:
                    self.fail(info)
                self.log.info(info)
        remote_client.disconnect()

    def backup_list(self):
        args = (
            f"info --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
            f"{self._common_objstore_arguments()}"
            f" --all"
            f" --json"
        )

        version = RestConnection(self.backupset.backup_host).get_nodes_version()

        if self.backupset.backup_list_name:
            args += " --repo {0}".format(self.backupset.backup_list_name)
        if self.backupset.backup_incr_backup:
            args += " --backup {0}".format(self.backupset.backup_incr_backup)
        if self.backupset.bucket_backup:
            args += f" {'--collection-string' if int(version[0]) >= 7 else '--bucket'} {self.backupset.bucket_backup}"
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

    def get_backup_info(self, repo=None, backup=None, collection_string=None, json=False, all_flag=False):
        """ Get output from cbbackupmgr info

        The parent arguments for each child argument must be provided (e.g. --backup requires --repo to be set).

        Args:
            repo (str): Repository name.
            backup (str): Backup name.
            collection_string (str): The collection_string or bucket name.
            json (bool): If true passes the --json flag.
            all_flag (bool): If true passes the --all flag.

        Returns:
            list: The output lines emitted from 'cbbackupmgr info'.

        """
        args = (
            f"info --archive {self.backupset.directory}"
        )

        version = RestConnection(self.backupset.backup_host).get_nodes_version()

        if repo:
            args += f" --repo {repo}"
        if repo and backup:
            args += f" --backup {backup}"
        if repo and backup and collection_string:
            args += f" {'--collection-string' if int(version[0]) >= 7 else '--bucket'} {collection_string}"
        if json:
            args += " --json"
        if all_flag:
            args += " --all"

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = f"{self.cli_command_location}/cbbackupmgr {args}"
        output, error = remote_client.execute_command(command)

        if self.debug_logs:
            remote_client.log_command_output(output, error)

        remote_client.disconnect()

        if error:
            self.fail("Getting backup info failed.")

        return output

    def backup_compact(self):
        args = "compact --archive {0} --repo {1} --backup {2}".format(self.backupset.directory, self.backupset.name,
                                                                      self.backups[self.backupset.backup_to_compact])
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        for line in output:
            if "Compaction succeeded," in line:
                return True, output, "Compaction of backup success"
        return False, output, "Compacting backup failed."
        remote_client.disconnect()

    def backup_remove(self, backup_range=None, verify_cluster_stats=True):
        args = (
            f"remove --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
            f" --repo {self.backupset.name}"
            f"{self._common_objstore_arguments()}"
        )

        if backup_range is not None:
            args += " --backups {0}".format(backup_range)

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        if verify_cluster_stats:
            self.verify_cluster_stats()
        if error:
            return False, error, "Removing backup failed."
        elif "failed" in " ".join(output):
            return False, output, "Removing backup failed."
        else:
            return True, output, "Removing of backup success"

    def backup_list_validate(self):
        status, output, message = self.backup_list()
        if not status:
            self.log.error(output)
            self.fail(message)
        status, message = self.validation_helper.validate_backup_list(output)
        if not status:
            self.fail(message)
        self.log.info(message)

    def backup_compact_validate(self):
        self.log.info("Listing backup details before compact")
        status, output_before_compact, message = self.backup_list()
        if not status:
            self.log.error(output_before_compact)
            self.fail(message)
        status, output, message = self.backup_compact()
        if not status:
            self.log.error(output)
            self.fail(message)
        self.log.info("Listing backup details after compact")
        status, output_after_compact, message = self.backup_list()
        if not status:
            self.log.error(output_after_compact)
            self.fail(message)
        status, message = self.validation_helper.validate_compact_lists(output_before_compact, output_after_compact)
        if not status:
            self.fail(message)
        self.log.info(message)

    def backup_compact_deleted_keys_validation(self, delete_keys):
        self.log.info("Check deleted keys status in file after compact")
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = conn.execute_command("ls {0}/backup/201*/default*/data " \
                                             .format(self.backupset.directory))
        deleted_key_status = {}
        if "shard_0.sqlite.0" in output:
            cmd = "{0}cbriftdump{1} " \
                  " -f {2}/backup/201*/default*/data/shard_0.sqlite.0 | grep -A 6 ent-backup " \
                  % (self.cli_command_location, self.cmd_ext, \
                     self.backupset.directory)
            dump_output, error = conn.execute_command(cmd)
            if dump_output:
                key_ids = [x.split(":")[1].strip(' ') for x in dump_output[0::8]]
                miss_keys = [x for x in delete_keys if x not in key_ids]
                if miss_keys:
                    raise Exception("Lost some keys {0} ".format(miss_keys))
                partition_ids = [x.split(":")[1].strip(' ') for x in dump_output[1::8]]
                status_ids = [x.split(" ")[-3].strip(' ') for x in dump_output[6::8]]
                for idx, key in enumerate(key_ids):
                    deleted_key_status[key] = \
                        {"KV store name": partition_ids[idx], "Status": status_ids[idx]}
                    if status_ids[idx] != "deleted":
                        raise Exception("key {0} status was not deleted. ".format(key))
            else:
                raise Exception("backup compaction failed to keep delete docs in file")
        else:
            raise Exception("file shard_0.sqlite did not created ")
        conn.disconnect()
        return deleted_key_status

    def bk_with_memcached_crash_and_restart(self):
        num_shards = ""
        backup_result = self.cluster.async_backup_cluster(
            backupset=self.backupset,
            objstore_provider=self.objstore_provider,
            resume=self.backupset.resume,
            purge=self.backupset.purge,
            no_progress_bar=self.no_progress_bar,
            cli_command_location=self.cli_command_location,
            cb_version=self.cb_version,
            num_shards=num_shards)
        conn_bk = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn_bk.pause_memcached(timesleep=8, delay=10)
        conn_bk.unpause_memcached()
        conn_bk.disconnect()
        output = backup_result.result(timeout=600)
        if self.debug_logs:
            if output:
                self.log.info("\nOutput from backup cluster: {0} ".format(output))
            else:
                self.fail("No output printout.")
        self.assertTrue(self._check_output("Backup completed successfully", output),
                        "Backup failed with memcached crash and restart within 180 seconds")
        self.log.info("Backup succeeded with memcached crash and restart within 180 seconds")
        self.sleep(20)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)

        command = (
            f"ls -tr {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name} | tail -1"
        )

        o, e = conn.execute_command(command)
        if o:
            self.backups.append(o[0])
        conn.log_command_output(o, e)
        self.number_of_backups_taken += 1
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets,
                                          self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets,
                                            self.number_of_backups_taken,
                                            self.backup_validation_files_location)
        conn.disconnect()

    def bk_with_erlang_crash_and_restart(self):
        num_shards = ""
        backup_failed = False
        backup_result = self.cluster.async_backup_cluster(
            backupset=self.backupset,
            objstore_provider=self.objstore_provider,
            resume=self.backupset.resume,
            purge=self.backupset.purge,
            no_progress_bar=self.no_progress_bar,
            cli_command_location=self.cli_command_location,
            cb_version=self.cb_version,
            num_shards=num_shards)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang(delay=10)
        conn.start_couchbase()
        output = backup_result.result(timeout=700)
        if self._check_output("Error backing up cluster", output):
            backup_failed = True
        if backup_failed:
            backup_result = self.cluster.async_backup_cluster(
                backupset=self.backupset,
                objstore_provider=self.objstore_provider,
                resume=True,
                purge=self.backupset.purge,
                no_progress_bar=self.no_progress_bar,
                cli_command_location=self.cli_command_location,
                cb_version=self.cb_version,
                num_shards=num_shards)
            output = backup_result.result(timeout=300)
        self.assertTrue(self._check_output("Backup completed successfully", output),
                        "Backup failed with erlang crash and restart within 180 seconds")
        self.log.info("Backup succeeded with erlang crash and restart within 180 seconds")
        self.sleep(30)
        conn.disconnect()
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = (
            f"ls -tr {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name} | tail -1"
        )

        o, e = conn.execute_command(command)
        if o:
            self.backups.append(o[0])
        conn.log_command_output(o, e)
        self.number_of_backups_taken += 1
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets,
                                          self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets,
                                            self.number_of_backups_taken,
                                            self.backup_validation_files_location)
        conn.disconnect()

    def bk_with_cb_server_stop_and_restart(self):
        num_shards = ""
        backup_failed = False
        backup_result = self.cluster.async_backup_cluster(
            backupset=self.backupset,
            objstore_provider=self.objstore_provider,
            resume=self.backupset.resume,
            purge=self.backupset.purge,
            no_progress_bar=self.no_progress_bar,
            cli_command_location=self.cli_command_location,
            cb_version=self.cb_version,
            num_shards=num_shards)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        output = backup_result.result(timeout=500)
        if self._check_output("Error backing up cluster", output):
            backup_failed = True
        conn.disconnect()
        self.sleep(30, "Waiting for couchbase to come back up...")
        if backup_failed:
            backup_result = self.cluster.async_backup_cluster(
                backupset=self.backupset,
                objstore_provider=self.objstore_provider,
                resume=True,
                purge=self.backupset.purge,
                no_progress_bar=self.no_progress_bar,
                cli_command_location=self.cli_command_location,
                cb_version=self.cb_version,
                num_shards=num_shards)
            output = backup_result.result(timeout=300)
        self.assertTrue(self._check_output("Backup completed successfully", output),
                       "Backup failed with couchbase stop and start within 180 seconds")

        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = (
            f"ls -tr {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name} | tail -1"
        )

        o, e = conn.execute_command(command)
        if o:
            self.backups.append(o[0])
        conn.log_command_output(o, e)
        self.number_of_backups_taken += 1
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets,
                                          self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets,
                                            self.number_of_backups_taken,
                                            self.backup_validation_files_location)
        conn.disconnect()

    def bk_with_stop_and_resume(self, iterations=1, remove_staging_directory=False):
        old_backup_name = ""
        new_backup_name = ""
        num_shards = ""
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        started_couchbase = False
        try:
            for _ in range(iterations):
                backup_result = self.cluster.async_backup_cluster(
                    backupset=self.backupset,
                    objstore_provider=self.objstore_provider,
                    resume=False,
                    purge=self.backupset.purge,
                    no_progress_bar=self.no_progress_bar,
                    cli_command_location=self.cli_command_location,
                    cb_version=self.cb_version,
                    num_shards=num_shards)
                self.wait_for_DCP_stream_start(backup_result)
                conn.kill_erlang(self.os_name)
                try:
                    output = backup_result.result(timeout=600)
                    self.log.info(str(output))
                    status, output, message = self.backup_list()
                    if not status:
                        self.log.error(output)
                        self.fail(message)
                    for line in output:
                        if "enterprise" in line:
                            continue
                        if re.search(r"\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                            old_backup_name = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}_\d{2}"
                                                        r"_\d{2}.\d+-\d{2}_\d{2}", line).group()
                            self.log.info("Backup name before resume: " + old_backup_name)
                except Exception as e:
                    conn.start_couchbase()
                    raise(e)
                conn.start_couchbase()
                ready = RestHelper(RestConnection(self.backupset.cluster_host)).is_ns_server_running()
                if not ready:
                    self.fail("Server failed to start")
                else:
                    started_couchbase = True
            if self.objstore_provider and remove_staging_directory:
                remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
                self.objstore_provider._remove_staging_directory(remote_client.extract_remote_info().type.lower(), remote_client)
                output, _ = self.backup_cluster()
                self.assertNotEqual(len(output), 0, "Expected some useful information to have been printed to stdout")
                self.assertIn("it looks like the staging directory was removed you must re-run with '--purge' to ensure previous in-progress uploads are purged",
                              output[0],
                              "Expected an informative error about why backup could not be resumed")
                # We don't need to continue any further, we've validated all the output that we need to
                return
            else:
                output, error = self.backup_cluster()
                if error or not self._check_output("Backup completed successfully", output):
                    self.fail("Taking cluster backup failed.")
            status, output, message = self.backup_list()
            if not status:
                self.log.error(output)
                self.fail(message)
            for line in output:
                if "enterprise" in line:
                    continue
                if re.search(r"\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                    new_backup_name = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}_\d{2}"
                                                r"_\d{2}.\d+-\d{2}_\d{2}", line).group()
                    self.log.info("Backup name after resume: " + new_backup_name)

            # Once the backup has been resumed to completion we shouldn't see any orphaned multipart uploads
            if self.objstore_provider:
                self.assertEqual(
                    self.objstore_provider.num_multipart_uploads(), 0,
                    "Expected all multipart uploads to have been purged (all newly created ones should have also been completed)"
                )

            self.assertEqual(old_backup_name, new_backup_name,
                             "Old backup name and new backup name are not same when resume is used")
            self.log.info("Old backup name and new backup name are same when resume is used")
        except Exception as ex:
            self.fail(str(ex))
        finally:
            if not started_couchbase:
                conn.start_couchbase()
            conn.disconnect()
            ready = RestHelper(RestConnection(self.backupset.cluster_host)).is_ns_server_running()
            if not ready:
                self.fail("Server failed to start")

    def _check_output_in_backup_logs(self, words, at_start = False, lines_before = 0, lines_after = 0):
        """ Checks if a word is present in the backup logs.

        Args:
            words: A word or list words to find in the backup logs.
            at_start: Check for the word at the start of a line only.
            lines_before: The number of lines to display before the match.
            lines_after: The number of lines to display after the match.

        Returns:
            A tuple (True if found, the matches output, remote shell error)
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        words = "|".join(words) if isinstance(words, list) else words
        caret = '^' if at_start else ''

        output, error = remote_client.execute_command(
                    f"cat {self.backupset.directory}/logs/backup-*.log | grep '{caret}{words}' -B {lines_before} -A {lines_after}"
        )

        return self._check_output(words, output), output, error

    def backup_merge(self, check_for_panic = False):
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
        output, error, exit_code = remote_client.execute_command(command,
                get_exit_code=True)
        remote_client.log_command_output(output, error)
        if error or exit_code:
            if not error: error = output
            return False, error, "Merging backup failed"
        elif not output:
            self.log.info("process cbbackupmgr may be killed")
            return False, [], "cbbackupmgr may be killed"
        elif check_for_panic:
            found_panic, output, error = \
                    self._check_output_in_backup_logs("panic", at_start = True, lines_after = 12)

            if found_panic:
                self.log.error("Found word 'panic' in backup logs")
                return False, output, "Panic found in backup logs:\n" + "\n".join(output)

        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = (
            f"ls -tr {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name} | tail -1"
        )

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

    def backup_merge_validate(self, repeats=1, skip_validation=False, **kwargs):
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
                    self.log.error(output)
                    self.fail(message)

        # if repeats < 2 and not skip_validation:
            # self.validate_backup_data(**kwargs)

    def validate_backup_data(self, server_host=None, server_bucket=None, master_key="ent-backup",
                             perNode=False, getReplica=False, mode="memory",
                             items=None, key_check="ent-backup1", validate_keys=False,
                             regex_pattern=None, swap=False, backup_name=None, filter_keys=None,
                             skip_stats_check=False):
        """Compare data in cluster with data in backup file.

           This function checks if key:value pairs in the cluster are present in the backup file i.e.
           it checks if the documents in the backup are a subset of the documents on the cluster.

           params:
               server_host: The server holding cbbackupmgr backup files.
               server_bucket (list): The cluster to retrieve documents from.
               master_key (str): A key prefix e.g. "ent-backup".
               perNode (bool): If set we oraganize collected data per node else we take a union. (Always set this to False)
               mode (str): e.g. "memory"
               backup_name (str): If None retrieves a backup from the filesystem (or cloud) otherwise uses the backup_name provided.
               skip_stats_check (bool): Skip fetching cluster stats
        """
        if server_host is None:
            server_host = self.backupset.backup_host
        if server_bucket is None:
            server_bucket = [self.master]
        if items is None:
            items = self.num_items
        data_matched = True
        data_collector = DataCollector()
        bk_buckets = [b for b in self.buckets if b.name not in self.backupset.exclude_buckets]
        if self.backupset.include_buckets:
            bk_buckets = [b for b in bk_buckets if b.name in self.backupset.include_buckets]
        bk_file_data, _ = data_collector.get_kv_dump_from_backup_file(server_host, self.cli_command_location,
                                                                      self.cmd_ext, master_key, bk_buckets,
                                                                      objstore_provider=self.objstore_provider,
                                                                      backupset=self.backupset, backup_name=backup_name)
        restore_file_data = bk_file_data
        regex_backup_data = {}
        if regex_pattern is not None:
            search_pattern = regex_pattern
            if " " in regex_pattern:
                search_pattern = search_pattern.replace(" ", "")
            pattern = re.compile(search_pattern)
            for bucket in bk_buckets:
                key_in_file_match_regex = 0
                regex_backup_data[bucket.name] = {}
                self.log.info("Extract keys with regex pattern '%s' either in key or body"
                              % regex_pattern)
                max_display = 10
                for key in restore_file_data[bucket.name]:
                    if self.debug_logs:
                        if max_display > 0:
                            print(("key in backup file of bucket %s:  %s" \
                                  % (bucket.name, key)))
                            max_display -= 1
                    if validate_keys:
                        if pattern.search(key):
                            regex_backup_data[bucket.name][key] = \
                                restore_file_data[bucket.name][key]
                            key_in_file_match_regex += 1
                    else:
                        if self.debug_logs:
                            if max_display > 0:
                                print(("value of key in backup file  ", \
                                      restore_file_data[bucket.name][key]))
                                max_display -= 1
                        if " " in restore_file_data[bucket.name][key]["Value"] and " " in regex_pattern:
                            pattern = re.compile(regex_pattern)
                        if pattern.search(str(restore_file_data[bucket.name][key]["Value"])):
                            regex_backup_data[bucket.name][key] = \
                                restore_file_data[bucket.name][key]
                            key_in_file_match_regex += 1
                if self.debug_logs:
                    if max_display > 0:
                        print(("\nKeys and value in backup file of bucket {0} \
                               that matches pattern '{1}'" \
                              .format(bucket.name, regex_pattern)))
                        for x in regex_backup_data[bucket.name]:
                            print(("key: ", x))
                            print(("value: ", regex_backup_data[bucket.name][x]["Value"]))
                        max_display -= 1
                self.log.info("Total keys matched in bk file of bucket {0} is {1}"
                              .format(bucket.name, key_in_file_match_regex))
                restore_file_data = regex_backup_data

        buckets_data = {}
        for bucket in bk_buckets:
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
                    try:
                        json.loads(",".join(value.split(',')[4:8])[3:-2].replace('""','"'))
                        value = ",".join(value.split(',')[4:8])[3:-2].replace('""','"')
                    except:
                        value = ",".join(value.split(',')[4:5])
                if value.startswith("b'"):
                    value = value[2:-1]
                buckets_data[bucket.name][key] = value
            self.log.info("*** Compare data in bucket and in backup file of bucket {0} ***"
                          .format(bucket.name))
            if not skip_stats_check:
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
            version = RestConnection(self.backupset.backup_host).get_nodes_version()

            if filter_keys:
                buckets_data[bucket.name] = {key: buckets_data[bucket.name][key] for key in buckets_data[bucket.name].keys() if key in filter_keys}

            for key in buckets_data[bucket.name]:
                if restore_file_data[bucket.name]:
                    try:
                        if restore_file_data[bucket.name][key]:
                            bucket_data_value = buckets_data[bucket.name][key]
                            restore_data_value = restore_file_data[bucket.name][key]["Value"]
                            if version[:3] <= "6.6":
                                bucket_data_value = str(bucket_data_value.replace(" ", ""))

                            # A hack to adjust the bucket data string in binary merge. For some reason the data there
                            # is stringified to a different (unknown)format. This  workaround removes all the meta characters,
                            # so that only the data itself stays
                            if self.document_type == "binary":
                                 bucket_data_value = bucket_data_value[bucket_data_value.find("ent-backup"):bucket_data_value.rfind('')]
                                 restore_data_value = restore_data_value[bucket_data_value.find("ent-backup"):restore_data_value.rfind('')]
                                 bucket_data_value = bucket_data_value.replace("\\'", "'")
                                 restore_data_value = restore_data_value.replace("b'", "")
                                 bucket_data_value = bucket_data_value.replace("b'", "")
                                 restore_data_value = restore_data_value.replace("b\"", "")
                                 bucket_data_value = bucket_data_value.replace("b\"", "")
                                 bucket_data_value = restore_data_value.replace('\'', '')
                                 restore_data_value = restore_data_value.replace('\'', '')

                            if bucket_data_value != restore_data_value:
                                if count < 20:
                                    self.log.error("Data does not match at key {0}.\
                                                bucket: {1} != {2} file" \
                                                   .format(key, bucket_data_value,
                                                           restore_data_value))
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
                            raise Exception("There was a prolbem with the data in bucket: {0}".format(bucket.name))
                else:
                    raise Exception("Database file of bucket {0} is empty"
                                    .format(bucket.name))
            if len(restore_file_data[bucket.name]) != key_count:
                raise Exception("Total key counts do not match.  Backup {0} != {1} bucket" \
                                .format(len(restore_file_data[bucket.name]), key_count))
            self.log.info("******** Data match in backup file and bucket {0} ******** " \
                          .format(bucket.name))
            print(("Bucket: ", bucket.name))
            print(("Total items in backup file:   ", len(bk_file_data[bucket.name])))
            if regex_pattern is not None:
                print(("Total items to be restored with regex pattern '{0}' is {1} " \
                      .format(regex_pattern, key_count)))
            print(("Total items in bucket should be:   ", key_count))
            if filter_keys:
                actual_keys = len(buckets_data[bucket.name].keys())
            else:
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
        cmd = "%s/couchbase-cli ssl-manage -c %s:%s -u Administrator -p password " \
              " --regenerate-cert %s" % (self.cli_command_location,
                                             server_cert.ip,
                                             server_cert.port,
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
                    index_name_path =  "Index:{0}/{1}".format(bucket.name, name)
                    version = RestConnection(
                              self.backupset.restore_cluster_host).get_nodes_version()
                    if version[:1] >= "7":
                        index_name_path =  "Index:{0}/_{0}/_{0}/{1}".format(bucket.name, name)
                    for x in output:
                        if index_name_path in x:
                            index_found += 1
                            self.log.info(mesg)
            if index_found < len(self.gsi_names):
                self.fail("GSI indexes are not restored in bucket {0}".format(bucket.name))
        shell.disconnect()

    def wait_for_DCP_stream_start(self, backup_result):
        """
        Waiting for an async backup to start streaming DCP.
        backup_result is the future object of an async backup (the output of async_backup_cluster)
        """
        log_directory = self.backupset.objstore_staging_directory + "/*/logs" if self.objstore_provider else self.backupset.directory + "/logs"
        command = "tail -n 1 {}/backup-*.log | grep ' (DCP) '"\
                                                   .format(log_directory)
        backup_client = RemoteMachineShellConnection(self.backupset.backup_host)
        Future.wait_until(
            lambda: (bool(backup_client.execute_command(command)[0]) or backup_result.done()),
            lambda x: x is True,
            200,
            interval_time=0.1,
            exponential_backoff=False)
             # If the backup finished and we never saw a DCP connection something's not right.
        if backup_result.done():
            self.fail("Never found evidence of open DCP stream in backup logs.")

    def _delete_repo(self):
        # We should remove all the remote objects as well as the files in the staging directory
        if self.objstore_provider:
            self.objstore_provider.delete_objects(f"{self.backupset.directory}/{self.backupset.name}")

        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        command = (
            f"rm -rf {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name}"
        )

        shell.execute_command(command)
        shell.disconnect()

    def _check_output(self, word_check, output):
        found = False
        if len(output) >= 1:
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in str(x.lower()):
                            self.log.info("Found '{0} in CLI output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in str(x.lower()):
                        self.log.info("Found '{0}' in CLI output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found

    def _reset_storage_mode(self, rest, storageMode, reset_node=True):
        if not storageMode:
            storageMode = "memory_optimized"
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
                            if not self._check_output("SUCCESS: Node initialize", output):
                                shell.disconnect()
                                raise("Failed to set hostname")
                        shell.disconnect()
                else:
                    self.fail("NS server is not ready after reset node")
        rest.set_indexer_storage_mode(username='Administrator',
                                      password='password',
                                      storageMode=storageMode)
        self.log.info("\n*** Done reset storage mode")
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

        args = (
            f"collect-logs --archive {self.objstore_provider.schema_prefix() + self.backupset.objstore_bucket + '/' if self.objstore_provider else ''}{self.backupset.directory}"
            f"{self._common_objstore_arguments()}"
        )

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
        os_dist = shell.info.distribution_version.replace(" ", "").lower()
        if "debian" in os_dist:
            shell.execute_command("apt update -y && apt install -y unzip")
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
                self.fail("Missing file 'backup-meta.json' in backup dir: " + backup_dir)
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
                                                                      objstore_provider= self.objstore_provider,
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
            if type(self.objstore_provider) == GCP:
                restore_buckets_items[bucket.name] = 63
            if len(list(bk_file_data[bucket.name].keys())) != \
                    int(restore_buckets_items[bucket.name]):
                self.fail(f"Total keys do not match. Expected: {len(bk_file_data[bucket.name])}  Found: {restore_buckets_items[bucket.name]}")
            items_info = rest.get_items_info(list(bk_file_data[bucket.name].keys()), bucket.name)
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
                bucket_ready = RestHelper(rest).vbucket_map_ready(bucket.name)
                while not bucket_ready and k < 10:
                    if k == 10:
                        self.fail("Bucket {0} is not ready after 10 seconds".format(bucket.name))
                    bucket_ready = RestHelper(rest).vbucket_map_ready(bucket.name)
                    self.sleep(1, "wait for bucket update new stats")
                    k += 1

                restore_buckets_items = rest.get_buckets_itemCount()
                if int(restore_buckets_items[bucket.name]) > 0:
                    if self.replace_ttl == "expired" and self.bk_with_ttl is not None:
                        if sleep_time < 600:
                            self.log.info("{0} items still in bucket".format(restore_buckets_items))
                            self.sleep(60, "wait for items to expire")
                            restore_buckets_items = rest.get_buckets_itemCount()
                            self.log.info("{0} items still in bucket".format(restore_buckets_items))
                            if int(restore_buckets_items[bucket.name]) > 0:
                                self.fail("Key did not expire after wait more than 70 seconds of ttl")
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

    def create_save_function_body(self, appname, appcode, description="Sample Description",
                                       checkpoint_interval=10000, cleanup_timers=False,
                                       dcp_stream_boundary="everything", deployment_status=True,
                                       skip_timer_threshold=86400,
                                       sock_batch_size=1, tick_duration=60000, timer_processing_tick_interval=500,
                                       timer_worker_pool_size=3, worker_count=3, processing_status=True,
                                       cpp_worker_thread_count=1, multi_dst_bucket=False, execution_timeout=3,
                                       data_chan_size=10000, worker_queue_cap=100000, deadline_timeout=6
                                       ):
        body = {}
        body['appname'] = appname
        script_dir = "pytests/eventing"
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        if multi_dst_bucket:
            body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        body['depcfg']['metadata_bucket'] = self.metadata_bucket_name
        body['depcfg']['source_bucket'] = self.src_bucket_name
        body['settings'] = {}
        body['settings']['checkpoint_interval'] = checkpoint_interval
        body['settings']['cleanup_timers'] = cleanup_timers
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = deployment_status
        body['settings']['description'] = description
        body['settings']['log_level'] = self.eventing_log_level
        body['settings']['skip_timer_threshold'] = skip_timer_threshold
        body['settings']['sock_batch_size'] = sock_batch_size
        body['settings']['tick_duration'] = tick_duration
        body['settings']['timer_processing_tick_interval'] = timer_processing_tick_interval
        body['settings']['timer_worker_pool_size'] = timer_worker_pool_size
        body['settings']['worker_count'] = worker_count
        body['settings']['processing_status'] = processing_status
        body['settings']['cpp_worker_thread_count'] = cpp_worker_thread_count
        body['settings']['execution_timeout'] = execution_timeout
        body['settings']['data_chan_size'] = data_chan_size
        body['settings']['worker_queue_cap'] = worker_queue_cap
        body['settings']['use_memory_manager'] = self.use_memory_manager
        if execution_timeout != 3:
            deadline_timeout = execution_timeout + 1
        body['settings']['deadline_timeout'] = deadline_timeout
        return body

    def deploy_function(self, body, deployment_fail=False, wait_for_bootstrap=True,pause_resume=False,pause_resume_number=1,
                        deployment_status=True,processing_status=True):
        body['settings']['deployment_status'] = deployment_status
        body['settings']['processing_status'] = processing_status
        if self.print_eventing_handler_code_in_logs:
            self.log.info("Deploying the following handler code : {0} with \nbindings: {1} and \nsettings: {2}".format(body['appname'], body['depcfg'] , body['settings']))
            self.log.info("\n{0}".format(body['appcode']))
        rest = RestConnection(self.backupset.cluster_host)
        content1 = rest.create_function(body['appname'], body)
        self.log.info("deploy Application : {0}".format(content1))
        if deployment_fail:
            res = json.loads(content1)
            if not res["compile_success"]:
                return
            else:
                raise Exception("Deployment is expected to be failed but no message of failure")
        if wait_for_bootstrap:
            # wait for the function to come out of bootstrap state
            self.bkrs_wait_for_handler_state(body['appname'], "deployed", rest)
        if pause_resume and pause_resume_number > 0:
            self.pause_resume_n(body, pause_resume_number, rest)

    def pause_resume_n(self, body, num , rest):
        for i in range(num):
            self.bkrs_pause_function(body, rest)
            self.sleep(30)
            self.bkrs_resume_function(body, rest)

    def bkrs_pause_function(self, body, rest, wait_for_pause=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = False
        self.refresh_rest_server()
        # save the function so that it is visible in UI
        #content = self.rest.save_function(body['appname'], body)
        # undeploy the function
        content1 = rest.set_settings_for_function(body['appname'], body['settings'])
        self.log.info("Pause Application : {0}".format(body['appname']))
        if wait_for_pause:
            self.bkrs_wait_for_handler_state(body['appname'], "paused", rest)

    def bkrs_resume_function(self, body, rest, wait_for_resume=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if "dcp_stream_boundary" in body['settings']:
            body['settings'].pop('dcp_stream_boundary')
        self.log.info("Settings after deleting dcp_stream_boundary : {0}"
                      .format(body['settings']))
        content1 = rest.set_settings_for_function(body['appname'], body['settings'])
        self.log.info("Resume Application : {0}".format(body['appname']))
        if wait_for_resume:
            self.bkrs_wait_for_handler_state(body['appname'], "deployed", rest)

    def bkrs_wait_for_handler_state(self, name, status, rest, iterations=20, server=None):
        self.sleep(20, message="Waiting for {} to {}...".format(name, status))
        result = rest.get_composite_eventing_status()
        count = 0
        composite_status = None
        while composite_status != status and count < iterations:
            self.sleep(20, "Waiting for {} to {}...".format(name, status))
            result = rest.get_composite_eventing_status()
            for i in range(len(result['apps'])):
                if result['apps'][i]['name'] == name:
                    composite_status = result['apps'][i]['composite_status']
            count += 1
            """ ******************* remove these codes below when ticket MB-47236 is fixed. """
            if count == 5 and "undeployed" in status:
                if server is not None:
                    print("\nserver: ", server)
                    BucketOperationHelper.delete_all_buckets_or_assert([server], self)
                else:
                    raise Exception('Need to pass server to delete buckets')
            """ *******************  end remove here """
        if count == iterations:
            raise Exception('Eventing took lot of time for handler {} to {}'.format(name, status))

    def bkrs_undeploy_and_delete_function(self, body, rest, server=None):
        self.bkrs_undeploy_function(body, rest, server)
        self.sleep(5)
        self.bkrs_delete_function(body, rest)

    def bkrs_undeploy_function(self, body, rest, server):
        content = rest.undeploy_function(body['appname'])
        self.log.info("Undeploy Application : {0}".format(body['appname']))
        self.bkrs_wait_for_handler_state(body['appname'], "undeployed", rest, server=server)
        return content

    def bkrs_delete_function(self, body, rest):
        content1 = rest.delete_single_function(body['appname'])
        self.log.info("Delete Application : {0}".format(body['appname']))
        return content1

    def _verify_backup_events_definition(self, bk_fxn, body, backup_index=0):
        backup_path = self.backupset.directory + "/backup/{0}/".format(self.backups[backup_index])
        events_file_name = "events.json"
        bk_file_events_dir = "/tmp/backup_events{0}/".format(self.master.ip)
        bk_file_events_path = bk_file_events_dir + events_file_name

        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        self.log.info("create local dir")
        if os.path.exists(bk_file_events_dir):
            shutil.rmtree(bk_file_events_dir)
        os.makedirs(bk_file_events_dir)
        self.log.info("copy eventing definition from remote to local")
        shell.copy_file_remote_to_local(backup_path + events_file_name,
                                        bk_file_events_path)
        local_bk_def = open(bk_file_events_path)
        bk_file_fxn = json.loads(local_bk_def.read())
        for k, v in list(bk_file_fxn[0]["settings"].items()):
            if k in body['settings'] and v != bk_fxn[0]["settings"][k]:
                self.log.info("key {0} has value not match".format(k))
                self.log.info("{0} : {1}".format(v, bk_fxn[0]["settings"][k]))
        self.log.info("remove tmp file in slave")
        if os.path.exists(bk_file_events_dir):
            shutil.rmtree(bk_file_events_dir)
        shell.disconnect()

    def _verify_restore_events_definition(self, bk_fxn):
        backup_path = self.backupset.directory + "/backup/{0}/".format(self.backups[0])
        events_file_name = "events.json"
        bk_file_events_dir = "/tmp/backup_events{0}/".format(self.master.ip)
        bk_file_events_path = bk_file_events_dir + events_file_name
        rest = RestConnection(self.backupset.restore_cluster_host)
        rs_fxn = rest.get_all_functions()
        rs_fxn_dict = json.loads(rs_fxn)[0]
        bk_fxn_dict = json.loads(bk_fxn)[0]
        not_matching_list = ['version', 'handleruuid', 'function_instance_id']
        for i in not_matching_list:
            rs_fxn_dict.pop(i)
            bk_fxn_dict.pop(i)
        if self.ordered(rs_fxn_dict) != self.ordered(bk_fxn_dict):
            self.fail("Events definition of backup and restore cluster are different")

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

    def add_server_with_custom_services(self, server, services):
        """ Add a server to the cluster_host and provision it with custom services
        """
        # Credentials to the server
        username = server.rest_username
        password = server.rest_password

        # Add server provisioning it with `services`
        RestConnection(self.backupset.cluster_host).add_node(username, password, server.ip, services=services)

        # Rebalance
        self.cluster.rebalance([self.backupset.cluster_host], [], [])

    def _create_restore_cluster(self, node_services=["kv"]):
        rest_rs = RestConnection(self.backupset.restore_cluster_host)
        rs_bucket = rest_rs.get_buckets()
        if rs_bucket:
            BucketOperationHelper.delete_all_buckets_or_assert(self.backupset.restore_cluster, self)
        ClusterOperationHelper.cleanup_cluster(self.backupset.restore_cluster,
                                               master=self.backupset.restore_cluster_host)

        rest_bk = RestConnection(self.backupset.cluster_host)
        try:
            bk_storage_mode = rest_bk.get_index_settings()["indexer.settings.storage_mode"]
        except Exception as e:
            if e:
                print("Exception error: ", str(e))
                raise("Need index service in node {0}".format(self.backupset.cluster_host.ip))
        eventing_service_in = False
        bk_cluster_services = list(rest_bk.get_nodes_services().values())
        for srv in bk_cluster_services:
            if "eventing" in srv:
                eventing_service_in = True
                break
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

        if len(bk_cluster_services) > 1:
            bk_cluster_services.remove(bk_services)
        if len(self.input.clusters[0]) > 1:
            if not bk_cluster_services:
                bk_cluster_services = bk_cluster_services.append(self.input.clusters[0][1].services)
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
        self._reset_storage_mode(rest_rs, bk_storage_mode, False)

    def _common_objstore_arguments(self):
        return self.backupset.common_objstore_arguments(self.objstore_provider)

    def setup_multi_root_certs(self):
        # Generate and upload the certs
        self.x509 = x509main(host=self.master)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)
        self.x509.teardown_certs(servers=self.servers)
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
        for server in self.servers:
            self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.upload_client_cert_settings(server=self.master)

        # Copy the certs onto the test machines
        self.backupset.client_certs = list()
        for ca_name in ("i1_r1", "iclient1_r1", "iclient1_clientroot"):
            self.backupset.client_certs.append(self.x509.get_client_cert(int_ca_name=ca_name))
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for cert_tuple in self.backupset.client_certs:
                for cert in cert_tuple:
                    shell.execute_command(f"mkdir -p {os.path.dirname(cert)}")
                    shell.copy_file_local_to_remote(cert, cert)
            shell.execute_command(f"mkdir -p {self.x509.CACERTFILEPATH}all")
            shell.copy_file_local_to_remote(f"{self.x509.CACERTFILEPATH}all/all_ca.pem", f"{self.x509.CACERTFILEPATH}all/all_ca.pem")

    def make_archive_read_only(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        # Get current .backup file from remote
        conf_file_contents = remote_client.read_remote_file(self.backupset.directory, ".backup")
        self.assertIsNotNone(conf_file_contents, "Could not read .backup file")

        conf_file_contents = conf_file_contents[0][:-1].replace('"read_only":false', '"read_only":true')
        # Write modified .backup back to remote
        remote_client.write_remote_file_single_quote(self.backupset.directory, ".backup", conf_file_contents)

        # Flag that backup has been made read_only
        self.backupset.read_only = True


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
        self.exclude_buckets = []
        self.include_buckets = []
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
        self.bkrs_client_version = None
        self.current_bkrs_client_version = None
        self.bkrs_client_upgrade = False
        self.bwc_version = None
        self.full_backup = False
        self.disable_ft_alias = False
        self.disable_analytics = False
        self.auto_create_buckets = False
        self.sqlite = False
        self.enable_users = False

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

        # Backup Service specific configuration
        self.backup_service = False
        self.objstore_alternative_staging_directory = ""

    def common_objstore_arguments(self, objstore_provider):
        """ Returns a string consisting of the common objstore command line params and args.

        Args:
            objstore_provider (Provider): If None returns an empty string.

        """
        return (
            f"{' --obj-access-key-id ' + self.objstore_access_key_id if objstore_provider and self.objstore_access_key_id else ''}"
            f"{' --obj-cacert ' + self.objstore_cacert if objstore_provider and self.objstore_cacert else ''}"
            f"{' --obj-endpoint ' + self.objstore_endpoint if objstore_provider and self.objstore_endpoint else ''}"
            f"{' --obj-no-ssl-verify' if objstore_provider and self.objstore_no_ssl_verify else ''}"
            f"{' --obj-region ' + self.objstore_region if objstore_provider and self.objstore_region else ''}"
            f"{' --obj-secret-access-key ' + self.objstore_secret_access_key if objstore_provider and self.objstore_secret_access_key else ''}"
            f"{' --obj-staging-dir ' + self.objstore_staging_directory if objstore_provider else ''}"
            f"{' --s3-force-path-style' if objstore_provider and objstore_provider.schema_prefix() == 's3://' else ''}"
        )

class EnterpriseBackupMergeBase(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupMergeBase, self).setUp()
        self.actions = self.input.param("actions", None)
        self.document_type = self.input.param("document_type", "json")
        if self.document_type == "binary":
            self.initial_load_gen = BlobGenerator("ent-backup", "ent-backup-",
                                                  self.value_size,
                                                  start=0,
                                                  end=self.num_items)
            self.create_gen = BlobGenerator("ent-backup", "ent-backup-",
                                            self.value_size, start=self.num_items,
                                            end=self.num_items + self.num_items
                                                * 0.5)
            self.update_gen = BlobGenerator("ent-backup", "ent-backup-",
                                            self.value_size,
                                            end=self.num_items * 0.9)

            self.delete_gen = BlobGenerator("ent-backup", "ent-backup-",
                                            self.value_size,
                                            start=self.num_items // 10,
                                            end=self.num_items)
        elif self.document_type == "json":
            age = list(range(5))
            first = ['james', 'sharon']
            template = '{{ "age": {0}, "first_name": "{1}" }}'
            gen = DocumentGenerator('test_docs', template, age, first, start=0,
                                    end=5)
            self.initial_load_gen = DocumentGenerator('test_docs', template,
                                                      age, first, start=0,
                                                      end=self.num_items)
            self.create_gen = DocumentGenerator('test_docs', template,
                                                age, first,
                                                start=self.num_items,
                                                end=self.num_items +
                                                    self.num_items * 0.5)
            self.update_gen = DocumentGenerator('test_docs', template,
                                                age, first,
                                                end=self.num_items * 0.9)

            self.delete_gen = DocumentGenerator('test_docs', template,
                                                age, first,
                                                start=self.num_items // 10,
                                                end=self.num_items)
        self.new_buckets = 1
        self.bucket_to_flush = 1
        self.ephemeral = False
        self.recreate_bucket = False
        self.graceful = True
        self.recoveryType = 'full'
        self.nodes_in = self.input.param("nodes_in", 0)
        self.nodes_out = self.input.param("nodes_out", 0)
        self.number_of_repeats = self.input.param("repeats", 1)
        self.backup_to_corrupt = 0
        self.backup_to_delete = 0
        self.skip_restore_indexes = self.input.param("skip_restore_indexe", False)
        self.overwrite_indexes = self.input.param("overwrite_indexes", False)
        self.skip_validation = self.input.param("skip_validation", False)

    def tearDown(self):
        super(EnterpriseBackupMergeBase, self).tearDown()

    def _get_python_sdk_client(self, ip, bucket, cluster_host):
        try:
            role_del = [bucket.name]
            RbacBase().remove_user_role(role_del, RestConnection(cluster_host))
        except Exception as ex:
            self.log.info(str(ex))
            self.assertIn("User was not found", str(ex), str(ex))

        testuser = [{'id': bucket.name, 'name': bucket.name, 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', cluster_host)

        role_list = [{'id': bucket.name, 'name': bucket.name, 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(cluster_host), 'builtin')

        try:
            cb = Bucket('couchbase://' + ip + '/' + bucket.name, password='password')
            if cb is not None:
                self.log.info("Established connection to bucket " + bucket.name + " on " + ip + " using python SDK")
            else:
                self.fail("Failed to connect to bucket " + bucket.name + " on " + ip + " using python SDK")
            return cb
        except Exception as ex:
            self.fail(str(ex))

    def async_ops_on_buckets(self):
        """
        Performs async operations on all the buckets in the cluster.
        The operations are: creates, updates and deletes
        :return: List of tasks running the load operations.
        """
        tasks = []
        create_tasks = self._async_load_all_buckets(self.master,
                                                    self.create_gen,
                                                    "create", self.expires)
        update_tasks = self._async_load_all_buckets(self.master,
                                                    self.update_gen,
                                                    "update", self.expires)
        delete_tasks = self._async_load_all_buckets(self.master,
                                                    self.delete_gen,
                                                    "delete", self.expires)
        tasks.extend(create_tasks)
        tasks.extend(update_tasks)
        tasks.extend(delete_tasks)
        return tasks

    def ops_on_buckets(self):
        ops_tasks = self.async_ops_on_buckets()
        for task in ops_tasks:
            task.result()

    def backup(self):
        """
        Backup the cluster and validate the backupset.
        :return: Nothing
        """
        if not self.debug_logs:
            self.show_bk_list = False
        self.backup_cluster_validate(repeats=self.number_of_repeats)

    def backup_with_expiry(self):
        """
        Backup the cluster and validate the backupset with expiry items before they expire.
        :return: Nothing
        """
        for bucket in self.buckets:
            cb = self._get_python_sdk_client(self.master.ip, bucket, self.backupset.cluster_host)
            for i in range(int(self.num_items * 0.7) + 1, self.num_items + 1):
                cb.upsert("doc" + str(i), {"key": "value"}, ttl=self.expires)
        self.backup_cluster_validate()
        self.sleep(self.expires)

    def backup_after_expiry(self):
        """
        Backup the cluster and validate the backupset with expiry items after they expire.
        :return: Nothing
        """
        for bucket in self.buckets:
            cb = self._get_python_sdk_client(self.master.ip, bucket, self.backupset.cluster_host)
            for i in range(int(self.num_items * 0.7) + 1, self.num_items + 1):
                cb.upsert("doc" + str(i), {"key": "value"}, ttl=self.expires)
        self.sleep(self.expires)
        self.backup_cluster_validate()

    def backup_with_ops(self):
        """
        Backup the data while loading the buckets in parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.backup()
        self.log.info(ops_tasks)
        for task in ops_tasks:
            task.result()

    def _check_output(self, word_check, output):
        found = False
        if len(output) >= 1:
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in str(x.lower()):
                            self.log.info("Found '{0}' in CLI output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in str(x.lower()):
                        self.log.info("Found '{0}' in CLI output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found

    def merge(self):
        """
        Merge all the existing backups in the backupset.
        :return: Nothing
        """
        skip_validation = self.skip_validation
        if self.backupset.start != 1: skip_validation = True
        self.backup_merge_validate(repeats=self.number_of_repeats, skip_validation=skip_validation)

    def merge_with_ops(self):
        """
        Merge all the existing backups in the backupset while loading the
        buckets in parallel
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.merge()
        for task in ops_tasks:
            task.result()

    def merge_with_memcached_crash_and_restart(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " + str(self.number_of_backups_taken))
        merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                        backups=self.backups,
                                                        directory=self.backupset.directory, name=self.backupset.name,
                                                        cli_command_location=self.cli_command_location,
                                                        start=int(self.backupset.start),
                                                        end=int(self.backupset.end)
                                                        )
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        conn.pause_memcached()
        conn.unpause_memcached()
        output = merge_result.result(timeout=200)
        self.assertTrue(self._check_output("Merge completed successfully", output),
                        "Merge failed with memcached crash and restart within 180 seconds")
        self.log.info("Merge succeeded with memcached crash and restart within 180 seconds")
        ready = RestHelper(RestConnection(self.backupset.cluster_host)).is_ns_server_running()
        if not ready:
            self.fail("Memcached failed to restart")

        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = (
            f"ls -tr {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name} | tail -1"
        )

        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2 and not self.skip_validation:
            self.validate_backup_data()
        conn.disconnect()

    def merge_with_erlang_crash_and_restart(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " + str(self.number_of_backups_taken))
        merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                        backups=self.backups,
                                                        directory=self.backupset.directory, name=self.backupset.name,
                                                        cli_command_location=self.cli_command_location,
                                                        start=int(self.backupset.start),
                                                        end=int(self.backupset.end)
                                                        )
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = merge_result.result(timeout=200)
        self.assertTrue(self._check_output("Merge completed successfully", output),
                        "Merge failed with erlang crash and restart within 180 seconds")
        self.log.info("Merge succeeded with erlang crash and restart within 180 seconds")
        ready = RestHelper(RestConnection(self.backupset.cluster_host)).is_ns_server_running()
        if not ready:
            self.fail("Erlang failed to restart")

        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2 and not self.skip_validation:
            self.validate_backup_data()
        conn.disconnect()

    def merge_with_cb_server_stop_and_restart(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " + str(self.number_of_backups_taken))
        merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                        backups=self.backups,
                                                        directory=self.backupset.directory, name=self.backupset.name,
                                                        cli_command_location=self.cli_command_location,
                                                        start=int(self.backupset.start),
                                                        end=int(self.backupset.end)
                                                        )
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        output = merge_result.result(timeout=200)
        self.assertTrue(self._check_output("Merge completed successfully", output),
                        "Merge failed with couchbase stop and start within 180 seconds")
        self.log.info("Merge succeeded with couchbase stop and start within 180 seconds")
        ready = RestHelper(RestConnection(self.backupset.cluster_host)).is_ns_server_running()
        if not ready:
            self.fail("cb server failed to restart")

        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = (
            f"ls -tr {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name} | tail -1"
        )

        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2 and not self.skip_validation:
            self.validate_backup_data()
        conn.disconnect()

    def compact_backup(self):
        """
        Compact the given backup and validate that the compaction resulted
        in reduction of backup size.
        :return:
        """
        self.backup_compact_validate()

    def async_rebalance(self):
        """
        Asynchronously rebalance the cluster.
        :return: The task that is rebalancing the nodes.
        """
        rest = RestConnection(self.master)
        # Get the nodes already in the backup cluster
        nodes_in_cluster = rest.node_statuses()
        # Get the nodes available for the backup_custer
        backup_cluster = [server for server in self.servers if (server not in
                                                                self.input.clusters[0] and server not in
                                                                self.input.clusters[1])]
        # Get the potential servers that can be added to the cluster
        serv_in = [server for server in backup_cluster if server.ip not in [
            node.ip for node in nodes_in_cluster]]
        # Get the potential servers that can be removed from the cluster
        serv_out = [server for server in backup_cluster if server.ip in [
            node.ip for node in nodes_in_cluster]]
        serv_in = serv_in[:self.nodes_in]
        serv_out = serv_out[serv_out.__len__() - self.nodes_out:]
        cluster_healthy = RestHelper(rest).is_cluster_healthy()
        if not cluster_healthy:
            ready = RestHelper(rest).is_ns_server_running()
            if not ready:
                self.fail("cluster is not ready")

        rebalance = self.cluster.async_rebalance(self.cluster_to_backup,
                                                 serv_in, serv_out)
        return rebalance

    def rebalance(self):
        """
        Rebalance the cluster
        :return: Nothing
        """
        rebalance = self.async_rebalance()
        rebalance.result()

    def rebalance_with_ops(self):
        """
        Rebalance the cluster while running bucket operations in parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.rebalance()
        for task in ops_tasks:
            task.result()

    def async_failover(self, ip=None):
        """
        Asynchronously failover a node
        :return: Nothing
        """
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        for x in nodes_all:
            if x.ip == self.master.ip:
                nodes_all.remove(x)
                if len(nodes_all) == 0:
                    self.fail("No more node to failover.")
        if ip:
            ip_to_failover = ip
        else:
            ip_to_failover = random.choice(nodes_all).ip
            self.log.info("Node needs to failover: {0}".format(ip_to_failover))
        status = False
        for node in nodes_all:
            if node.ip == ip_to_failover:
                self.failover_ip = node.ip
                status = rest.fail_over(otpNode=node.id, graceful=self.graceful)
                if not status:
                    self.sleep(10)
                    healthy = RestHelper(rest).is_cluster_healthy()
                    if healthy:
                        status = rest.fail_over(otpNode=node.id, graceful=self.graceful)
        return status

    def async_failover_and_recover(self):
        """
            Asynchronously failover a node and add back the node after 30 sec
            :return: Task that is running the rebalance at end.
        """
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                rest.fail_over(otpNode=node.id, graceful=self.graceful)
                self.sleep(90)
                try:
                    rest.set_recovery_type(otpNode=node.id,
                                           recoveryType=self.recoveryType)
                except Exception as e:
                    if "Set RecoveryType failed" in str(e):
                        self.sleep(30, "Wait for node to complete failover")
                        status = rest.set_recovery_type(otpNode=node.id,
                                                        recoveryType=self.recoveryType)
                        if not status:
                            self.fail("Fail to set recovery mode")
                rest.add_back_node(otpNode=node.id)
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        return rebalance

    def async_recover_node(self):
        """
            Asynchronously add back the node after failover.
            :return: Task that is running the rebalance at end.
        """
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()

        recovery_node = self.servers[1].ip
        if self.failover_ip is not None:
            recovery_node = self.failover_ip
        self.log.info("recovery node: {0} ******".format(recovery_node))
        for node in nodes_all:
            if node.ip == recovery_node:
                rest.set_recovery_type(otpNode=node.id,
                                       recoveryType=self.recoveryType)
                rest.add_back_node(otpNode=node.id)
                self.failover_ip = None
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        return rebalance

    def async_remove_failed_node(self):
        """
            Asynchronously remove a failed over node by performing a
            rebalance of cluster
        :return: Task that is running the rebalance.
        """
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        return rebalance

    def failover(self):
        """
        Failover a node and add back the node.
        :return: Nothing
        """
        status = self.async_failover()
        if not status:
            self.fail("Failed to failover a node")
        else:
            self.log.info("Done failover a node")
            self.done_failover = True

    def failover_with_ops(self):
        """
        Failover a node while bucket operations are running in parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.failover()
        for task in ops_tasks:
            task.result()

    def failover_and_recover(self):
        """
        Failover a node and add back the node after 30 sec.
        :return: Nothing
        """
        task = self.async_failover_and_recover()
        task.result()

    def failover_and_recover_with_ops(self):
        """
        Failover a node and add back the node after 30 sec while bucket
        operations are running in parallel
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.failover_and_recover()
        for task in ops_tasks:
            task.result()

    def recover_node(self):
        """
        Recover a failedover node.
        :return: Nothing
        """
        task = self.async_recover_node()
        task.result()

    def recover_node_with_ops(self):
        """
        Recover a failedover node while bucket operations are running in
        parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.recover_node()
        for task in ops_tasks:
            task.result()

    def remove_failed_node(self):
        """
        Remove a failed over node from the cluster
        :return: Nothing
        """
        task = self.async_remove_failed_node()
        task.result()

    def _reconfigure_bucket_memory(self, num_new_buckets):
        """
        Reconfigure bucket memories in the cluster to accommodate adding new
        buckets
        :param num_new_buckets:
        :return: The new bucket size.
        """
        rest = RestConnection(self.master)
        ram_size = rest.get_nodes_self().memoryQuota
        bucket_size = self._get_bucket_size(ram_size, self.bucket_size +
                                            num_new_buckets)
        self.log.info("Changing the existing buckets size to accomodate new "
                      "buckets")
        for bucket in self.buckets:
            rest.change_bucket_props(bucket, ramQuotaMB=bucket_size)
        return bucket_size

    def create_new_bucket_and_populate(self):
        """
        Create new buckets and populate the bucket with items.
        :return: Nothing
        """
        bucket_size = self._reconfigure_bucket_memory(self.new_buckets)
        rest = RestConnection(self.master)
        server_id = rest.get_nodes_self().id
        bucket_tasks = []
        bucket_params = copy.deepcopy(
            self.bucket_base_params['membase']['non_ephemeral'])
        bucket_params['size'] = bucket_size
        if self.ephemeral:
            bucket_params['bucket_type'] = 'ephemeral'
            bucket_params['eviction_policy'] = 'noEviction'
        standard_buckets = []
        for i in range(0, self.new_buckets):
            if self.recreate_bucket:
                name = self.backupset.deleted_buckets[i]
            else:
                name = 'bucket' + str(i)
            port = STANDARD_BUCKET_PORT + i + 1
            bucket_priority = None
            if self.standard_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(
                    self.standard_bucket_priority[i])

            bucket_params['bucket_priority'] = bucket_priority
            bucket_tasks.append(
                self.cluster.async_create_standard_bucket(name=name, port=port,
                                                          bucket_params=bucket_params))
            bucket = CBBucket(name=name,
                              num_replicas=self.num_replicas,
                              bucket_size=self.bucket_size,
                              port=port, master_id=server_id,
                              eviction_policy='noEviction', lww=self.lww)
            self.buckets.append(bucket)
            standard_buckets.append(bucket)
            if not self.recreate_bucket:
                self.backupset.new_buckets.append(bucket.name)
        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)
        if self.enable_time_sync:
            self._set_time_sync_on_buckets(
                ['bucket' + str(i) for i in range(
                    self.new_buckets)])
        for bucket in standard_buckets:
            self._load_bucket(bucket, self.master, self.initial_load_gen,
                              "create", self.expires)

    def create_bucket_with_ops(self):
        """
        Create new buckets and populate the bucket with items while ops are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        bucket_size = self._reconfigure_bucket_memory(self.new_buckets)
        rest = RestConnection(self.master)
        server_id = rest.get_nodes_self().id
        bucket_tasks = []
        bucket_params = copy.deepcopy(
            self.bucket_base_params['membase']['non_ephemeral'])
        bucket_params['size'] = bucket_size
        if self.ephemeral:
            bucket_params['bucket_type'] = 'ephemeral'
            bucket_params['eviction_policy'] = 'noEviction'
        standard_buckets = []
        for i in range(0, self.new_buckets):
            if self.recreate_bucket:
                name = self.backupset.deleted_buckets[i]
            else:
                name = 'bucket' + str(i)
            port = STANDARD_BUCKET_PORT + i + 1
            bucket_priority = None
            if self.standard_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(
                    self.standard_bucket_priority[i])

            bucket_params['bucket_priority'] = bucket_priority
            bucket_tasks.append(
                self.cluster.async_create_standard_bucket(name=name, port=port,
                                                          bucket_params=bucket_params))
            bucket = Bucket(name=name,
                            num_replicas=self.num_replicas,
                            bucket_size=self.bucket_size,
                            port=port, master_id=server_id,
                            eviction_policy='noEviction', lww=self.lww)
            self.buckets.append(bucket)
            standard_buckets.append(bucket)
            if not self.recreate_bucket:
                self.backupset.new_buckets.append(bucket.name)
        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)
        if self.enable_time_sync:
            self._set_time_sync_on_buckets(
                ['bucket' + str(i) for i in range(
                    self.new_buckets)])
        for task in ops_tasks:
            task.result()
        for bucket in standard_buckets:
            self._load_bucket(bucket, self.master, self.initial_load_gen,
                              "create", self.expires)

    def delete_bucket(self):
        """
        Delete a bucket from the cluster
        :return: Nothing
        """
        self.log.info("Deleting a bucket")
        bucket_to_delete = self.buckets[-1].name
        self.backupset.deleted_buckets.append(bucket_to_delete)
        BucketOperationHelper.delete_bucket_or_assert(self.master,
                                                      bucket_to_delete,
                                                      self)
        self.buckets.pop()

    def delete_bucket_with_ops(self):
        """
        Delete a bucket from the cluster while bucket operations are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.log.info("Deleting a bucket")
        bucket_to_delete = self.buckets[-1].name
        self.backupset.deleted_buckets.append(bucket_to_delete)
        BucketOperationHelper.delete_bucket_or_assert(self.master,
                                                      bucket_to_delete,
                                                      self)
        self.buckets = RestConnection(self.master).get_buckets()
        for task in ops_tasks:
            task.result()

    def delete_backup(self):
        """
        Delete an incr backup
        :return: Nothing
        """
        backup_to_delete = self.backups.pop(self.backup_to_delete - 1)
        self.log.info("Deleting backup " + backup_to_delete)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        o, e = conn.execute_command(
            "rm -rf " + self.backupset.directory + "/" + self.backupset.name + "/" + backup_to_delete)
        conn.log_command_output(o, e)
        self.backupset.deleted_backups.append(backup_to_delete)
        self.number_of_backups_taken -= 1
        conn.disconnect()

    def corrupt_backup(self):
        """
        Corrupt an incr backup
        :return: Nothing
        """
        backup_to_corrupt = self.backups[self.backup_to_corrupt - 1]
        self.log.info("Corrupting backup " + backup_to_corrupt)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1}/{2} | tail".format(self.backupset.directory, self.backupset.name,
                                                     backup_to_corrupt)
        o, e = conn.execute_command(command)
        conn.log_command_output(o, e)
        data_dir = o[0]
        o, e = conn.execute_command("dd if=/dev/zero of=" + self.backupset.directory + "/" + self.backupset.name + "/" +
                                    backup_to_corrupt + "/" + data_dir + "/data/shard_0.sqlite.0" +
                                    " bs=1024 count=100 seek=10 conv=notrunc")
        conn.log_command_output(o, e)
        self.backup_corrupted = True
        conn.disconnect()

    def _flush_bucket(self, bucket_to_flush):
        """
        Flush a bucket from the cluster.
        :param bucket_to_flush:
        :return: Nothing
        """
        self.log.info("Flushing {} buckets")
        rest = RestConnection(self.master)
        bucket_name = self.buckets[bucket_to_flush].name
        rest.flush_bucket(bucket_name)
        self.backupset.flushed_buckets.append(bucket_name)

    def flush_buckets(self):
        """
        Flush buckets from the cluster
        :return: Nothing
        """
        self.log.info("Flushing {} buckets".format(self.bucket_to_flush))
        for i in range(self.buckets.__len__() - self.bucket_to_flush,
                       self.buckets.__len__()):
            self._flush_bucket(i)

    def flush_buckets_with_ops(self):
        """
        Flush buckets from the cluster while ops are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.log.info("Flushing {} buckets".format(self.bucket_to_flush))
        for i in range(self.buckets.__len__() - self.bucket_to_flush,
                       self.buckets.__len__()):
            self._flush_bucket(i)
        for task in ops_tasks:
            task.result()

    def load_flushed_buckets(self):
        """
        Reload the flushed buckets in the cluster.
        :return: Nothing
        """
        self.log.info("Loading data into buckets that were flushed")
        tasks = []
        for i in range(self.buckets.__len__() - self.bucket_to_flush,
                       self.buckets.__len__()):
            self._load_bucket(self.buckets[i], self.master,
                              self.initial_load_gen, "create", self.expires)

    def compact_buckets(self):
        """
        Compact all the buckets in the cluster
        :return: Nothing
        """
        self._run_compaction(number_of_times=1)

    def compact_buckets_with_ops(self):
        """
        Compact all the buckets in the cluster while ops are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self._run_compaction(number_of_times=1)
        for task in ops_tasks:
            task.result()

    def rollback(self):
        buckets = self.buckets
        rest = RestConnection(self.backupset.cluster_host)
        nodes = rest.get_nodes()

        for bucket in buckets:
            # Stop Persistence on Node A & Node B
            for node in nodes:
                mem_client = MemcachedClientHelper.direct_client(node, bucket)
                mem_client.stop_persistence()

            # Perform mutations on the bucket
            self.ops_on_buckets()

            # Kill memcached on Node A so that Node B becomes master
            shell = RemoteMachineShellConnection(self.backupset.cluster_host)
            shell.kill_memcached()

            # Start persistence on Node B
            mem_client = MemcachedClientHelper.direct_client(nodes[1], bucket)
            mem_client.start_persistence()

            # Failover Node B
            self.async_failover(nodes[1].ip)

            # Wait for Failover & rollback to complete
            self.sleep(60)

            shell.stop_couchbase()
            shell.start_couchbase()

            ready = RestHelper(rest).is_ns_server_running()
            if not ready:
                self.fail("CB server failed to start")
            shell.disconnect()

    def create_indexes(self):
        rest_src = RestConnection(self.backupset.cluster_host)
        rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                          self.servers[1].ip, services=['index', 'fts'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
        rebalance.result()
        storage_mode = "plasma"
        cluster_storage_mode = rest_src.get_index_storage_mode()
        self.log.info("Cluster storage mode: {0}".format(cluster_storage_mode))
        if storage_mode != cluster_storage_mode:
            storage_mode = cluster_storage_mode

        cmd = "cbindex -type create -bucket default -using {2} -index " \
              "age_idx -fields=age -auth {0}:{1}".format(self.servers[0].rest_username,
                                                         self.servers[0].rest_password,
                                                         storage_mode)
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        cluster_version = RestConnection(self.backupset.cluster_host).get_nodes_version()
        if not self._check_output("Index created", output):
            if cluster_version[:3] >= "7.0":
                err_msg = "cannot proceed due to rebalance in progress"
            else:
                err_msg = ['Fail to create index due to rebalancing',
                           'Indexer Cannot Process Create Index - Rebalance In Progress']

            if self._check_output(err_msg, output) or \
                    self._check_output(err_msg, error):
                self.sleep(15, "wait for rebalance complete")
                output, error = remote_client.execute_command(command)
                if not self._check_output("Index created", output):
                    self.fail("GSI index cannot be created after rebalance")
            else:
                self.fail("GSI index cannot be created")
        cmd = "cbindex -type create -bucket default -using {2} -index " \
              "name_idx -fields=name -auth {0}:{1}".format(self.servers[0].rest_username,
                                                           self.servers[0].rest_password,
                                                           storage_mode)
        remote_client.disconnect()
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if not self._check_output("Index created", output):
            self.fail("GSI index cannot be created")

        index_definition = INDEX_DEFINITION
        index_name = index_definition['name'] = "age"
        fts_server = self.get_nodes_from_services_map(service_type="fts")
        rest_fts = RestConnection(fts_server)
        try:
            self.log.info("Create fts index")
            rest_fts.create_fts_index(index_name, index_definition)
        except Exception as ex:
            self.fail(ex)

        if not self.skip_restore_indexes:
            rest_src = RestConnection(self.backupset.restore_cluster_host)
            rest_src.add_node(self.servers[2].rest_username,
                              self.servers[2].rest_password,
                              self.servers[2].ip, services=['index', 'fts'])
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [],
                                                     [])
            rebalance.result()

            rest_helper = RestHelper(rest_src)
            ram_size = rest_src.get_nodes_self().memoryQuota
            bucket_size = self._get_bucket_size(ram_size, self.total_buckets)
            for bucket in self.buckets:
                bucket_name = bucket.name
                if not rest_helper.bucket_exists(bucket_name):
                    self.log.info("Creating bucket {0} in restore host {1}"
                                  .format(bucket_name,
                                          self.backupset.restore_cluster_host.ip))
                    if self.bucket_type == "ephemeral":
                        self.eviction_policy = "noEviction"
                    rest_src.create_bucket(bucket=bucket_name,
                                           ramQuotaMB=bucket_size,
                                           bucketType=self.bucket_type,
                                           proxyPort=bucket.port,
                                           evictionPolicy=self.eviction_policy,
                                           lww=self.lww_new)
                    bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
                    if not bucket_ready:
                        self.fail("Bucket %s not created after 120 seconds."
                                  % bucket_name)

            if self.overwrite_indexes:
                err_msg = "cannot create index because an index with the same name already exists"
                cmd = "cbindex -type create -bucket default -using {2} -index " \
                      "age_idx1 -fields=age -auth {0}:{1}".format(self.servers[0].rest_username,
                                                                  self.servers[0].rest_password,
                                                                  storage_mode)
                remote_client = RemoteMachineShellConnection(
                    self.backupset.restore_cluster_host)
                command = "{0}/{1}".format(self.cli_command_location, cmd)
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
                if error or not self._check_output("Index created", output):
                    self.fail("GSI index cannot be created")

                index_definition = INDEX_DEFINITION
                index_name = index_definition['name'] = "age"
                rest_fts = RestConnection(self.servers[2])
                try:
                    self.log.info("Create fts index")
                    rest_fts.create_fts_index(index_name, index_definition)
                except Exception as ex:
                    if ex:
                        print("\nException error: ", str(ex))
                    if err_msg not in str(ex):
                        self.log.error("It should not create same name index")
                        self.fail(ex)
        remote_client.disconnect()

    def update_indexes(self):
        index_definition = INDEX_DEFINITION
        index_definition['name'] = "age1"
        rest_fts = RestConnection(self.servers[2])
        try:
            self.log.info("Update fts index")
            rest_fts.update_fts_index("age1", index_definition)
        except Exception as ex:
            self.fail(ex)

    def do_backup_merge_actions(self):
        """
        Perform the actions mentioned the self.actions param.
        :return: Nothing
        """
        self.backupset.number_of_backups = 0
        # running testrunner with conf file takes in the quotes " too into
        # the parameter. Below step removes the quotes from the parameter.
        self.actions = self.actions.replace('"', '')
        actions = self.actions.split(",")
        for action in actions:
            iterations = 1
            if ":" in action:
                action = action.split(':')
                params = action[1].split('&')
                action = action[0]
                if "backup" in action:
                    iterations = params[0]
                    self.backupset.number_of_backups += int(iterations)
                elif "failover" in action or "recover" in action:
                    if 'hard' in params:
                        self.graceful = False
                    else:
                        self.graceful = True
                    if 'delta' in params:
                        self.recoveryType = 'delta'
                    else:
                        self.recoveryType = 'full'
                elif "merge" in action:
                    if params.__len__() == 0:
                        iterations = 1
                        self.backupset.start = 1
                        self.backupset.end = len(self.backups)
                    if params.__len__() == 2:
                        iterations = 1
                        self.backupset.start = int(params[0])
                        self.backupset.end = int(params[1])
                    if params.__len__() == 3:
                        iterations = params[0]
                        self.backupset.start = int(params[1])
                        self.backupset.end = int(params[2])
                elif "compact_backup" in action:
                    self.backupset.backup_to_compact = int(params[0])
                elif "flush" in action:
                    self.bucket_to_flush = int(params[0])
                elif "create_buckets" in action:
                    self.new_buckets = int(params[0])
                    if params.__len__() == 2:
                        if params[1] == "ephemeral":
                            self.ephemeral = True
                elif "recreate_buckets" in action:
                    self.new_buckets = int(params[0])
                    self.recreate_bucket = True
                    if params.__len__() == 2:
                        if params[1] == "ephemeral":
                            self.ephemeral = True
                elif "delete_bkup" in action:
                    iterations = 1
                    self.backup_to_delete = int(params[0])
                elif "corrupt_bkup" in action:
                    iterations = 1
                    self.backup_to_corrupt = int(params[0])
                else:
                    if params[0].isdigit():
                        iterations = int(params[0])
            else:
                iterations = 1
            for i in range(0, int(iterations)):
                self.log.info("Performing {} action for {}th time".format(
                    action, i + 1))
                self.backup_merge_actions[action](self)
                self.log.info("Finished {} action for {}th time.".format(
                    action, i + 1))

    def backup_with_memcached_crash_and_restart(self):
        """
           call bk_with_memcached_crash_and_restart in EnterpriseBackupRestoreBase
        """
        return self.bk_with_memcached_crash_and_restart()

    def backup_with_erlang_crash_and_restart(self):
        """
           call bk_with_erlang_crash_and_restart in EnterpriseBackupRestoreBase
        """
        return self.bk_with_erlang_crash_and_restart()

    def backup_with_cb_server_stop_and_restart(self):
        """
           call bk_with_cb_server_stop_and_restart in EnterpriseBackupRestoreBase
        """
        return self.bk_with_cb_server_stop_and_restart()

    def set_meta_purge_interval(self):
        rest = RestConnection(self.master)
        params = {}
        api = rest.baseUrl + "controller/setAutoCompaction"
        params["purgeInterval"] = 0.003
        params = urllib.parse.urlencode(params)
        return rest._http_request(api, "POST", params)

    backup_merge_actions = {
        "bucket_ops": ops_on_buckets,
        "backup": backup,
        "backup_with_expiry": backup_with_expiry,
        "backup_after_expiry": backup_after_expiry,
        "backup_with_memcached_crash_and_restart": backup_with_memcached_crash_and_restart,
        "backup_with_erlang_crash_and_restart": backup_with_erlang_crash_and_restart,
        "backup_with_cb_server_stop_and_restart": backup_with_cb_server_stop_and_restart,
        "merge": merge,
        "merge_with_memcached_crash_and_restart": merge_with_memcached_crash_and_restart,
        "merge_with_erlang_crash_and_restart": merge_with_erlang_crash_and_restart,
        "merge_with_cb_server_stop_and_restart": merge_with_cb_server_stop_and_restart,
        "compact_backup": compact_backup,
        "rebalance": rebalance,
        "failover": failover,
        "recover": recover_node,
        "remove_node": remove_failed_node,
        "failover_and_recover": failover_and_recover,
        "backup_with_ops": backup_with_ops,
        "merge_with_ops": merge_with_ops,
        "rebalance_with_ops": rebalance_with_ops,
        "failover_with_ops": failover_with_ops,
        "recover_with_ops": recover_node_with_ops,
        "failover_and_recover_with_ops": failover_and_recover_with_ops,
        "create_buckets": create_new_bucket_and_populate,
        "create_buckets_with_ops": create_bucket_with_ops,
        "recreate_buckets": create_new_bucket_and_populate,
        "flush_buckets": flush_buckets,
        "load_flushed_buckets": load_flushed_buckets,
        "flush_buckets_with_ops": flush_buckets_with_ops,
        "delete_buckets": delete_bucket,
        "delete_buckets_with_ops": delete_bucket_with_ops,
        "compact_buckets": compact_buckets,
        "compact_buckets_with_ops": compact_buckets_with_ops,
        "rollback": rollback,
        "delete_bkup": delete_backup,
        "corrupt_bkup": corrupt_backup,
        "create_indexes": create_indexes,
        "update_indexes": update_indexes
    }
