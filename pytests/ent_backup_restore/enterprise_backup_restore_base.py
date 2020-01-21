import copy
import os, shutil, ast, re, subprocess
import json
import urllib.request, urllib.parse, urllib.error, datetime

from basetestcase import BaseTestCase
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
from testconstants import LINUX_COUCHBASE_BIN_PATH,\
                          COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH_RAW,\
                          WIN_COUCHBASE_BIN_PATH_RAW, WIN_COUCHBASE_BIN_PATH, WIN_TMP_PATH_RAW,\
                          MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH,\
                          WIN_TMP_PATH, STANDARD_BUCKET_PORT, WIN_CYGWIN_BIN_PATH
from testconstants import INDEX_QUOTA, FTS_QUOTA, COUCHBASE_FROM_MAD_HATTER,\
                          CLUSTER_QUOTA_RATIO
from security.rbac_base import RbacBase
from couchbase.bucket import Bucket
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


class EnterpriseBackupRestoreBase(BaseTestCase):
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

        cmd =  'curl -g {0}:8091/diag/eval -u {1}:{2} '.format(self.master.ip,
                                                              self.master.rest_username,
                                                              self.master.rest_password)
        cmd += '-d "path_config:component_path(bin)."'
        bin_path  = subprocess.check_output(cmd, shell=True)
        try:
            bin_path = bin_path.decode()
        except AttributeError:
            pass
        if not self.skip_init_check_cbserver:
            if "bin" not in bin_path:
                self.fail("Check if cb server install on %s" % self.master.ip)
            else:
                self.cli_command_location = bin_path.replace('"', '') + "/"

        self.debug_logs = self.input.param("debug-logs", False)
        self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        self.backupset.user_env = self.input.param("user-env", False)
        self.backupset.passwd_env = self.input.param("passwd-env", False)
        self.backupset.log_archive_env = self.input.param("log-archive-env", False)
        self.backupset.no_log_output_flag = self.input.param("no-log-output-flag", False)
        self.backupset.ex_logs_path = self.input.param("ex-logs-path", None)
        self.backupset.overwrite_user_env = self.input.param("overwrite-user-env", False)
        self.backupset.overwrite_passwd_env = self.input.param("overwrite-passwd-env", False)
        self.replace_ttl_with = self.input.param("replace-ttl-with", None)
        self.num_shards = self.input.param("num_shards", None)
        self.multi_num_shards = self.input.param("multi_num_shards", False)
        self.shards_action = self.input.param("shards_action", None)
        self.bkrs_flag = self.input.param("bkrs_flag", None)
        self.sqlite_storage = self.input.param("sqlite_storage", True)
        self.force_restart_erlang = self.input.param("force_restart_erlang", False)
        self.force_restart_couchbase_server = \
                          self.input.param("force_restart_couchbase_server", False)
        self.bk_with_ttl = self.input.param("bk-with-ttl", None)
        self.backupset.user_env_with_prompt = \
                        self.input.param("user-env-with-prompt", False)
        self.backupset.passwd_env_with_prompt = \
                        self.input.param("passwd-env-with-prompt", False)
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
                                "xargs -I {} date --date='@{}' --rfc-3339=seconds | "\
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
                            "{0}xargs -I {{}} date --date=\"@'{{}}'\" --rfc-3339=seconds | "\
                                                            .format(self.cygwin_bin_path) + \
                                                                               "sed 's/ /T/'"
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
        self.backupset.backup_host = self.input.clusters[1][0]
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
        self.do_restore = self.input.param("do-restore", False)
        self.do_verify = self.input.param("do-verify", False)
        self.create_views = self.input.param("create-views", False)
        self.create_fts_index = self.input.param("create-fts-index", False)
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
        self.reset_restore_cluster = self.input.param("reset-restore-cluster", True)
        self.no_progress_bar = self.input.param("no-progress-bar", True)
        self.multi_threads = self.input.param("multi_threads", False)
        self.threads_count = self.input.param("threads_count", 1)
        self.bucket_delete = self.input.param("bucket_delete", False)
        self.bucket_flush = self.input.param("bucket_flush", False)
        if self.same_cluster:
            self.backupset.restore_cluster_host = self.servers[0]
            self.backupset.restore_cluster_host_username = self.servers[0].rest_username
            self.backupset.restore_cluster_host_password = self.servers[0].rest_password
        else:
            self.backupset.restore_cluster_host = self.input.clusters[0][0]
            self.backupset.restore_cluster_host_username = self.input.clusters[0][0].rest_username
            self.backupset.restore_cluster_host_password = self.input.clusters[0][0].rest_password
        """ new user to test RBAC """
        if self.cluster_new_user:
            self.backupset.cluster_host_username = self.cluster_new_user
            self.backupset.restore_cluster_host_username = self.cluster_new_user
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
        self.backupset.delete_old_bucket = self.input.param("delete-old-bucket", False)
        self.add_node_services = self.input.param("add-node-services", "kv")
        self.backupset.backup_compressed = \
                                      self.input.param("backup-conpressed", False)
        self.backups = []
        self.validation_helper = BackupRestoreValidations(self.backupset,
                                                          self.cluster_to_backup,
                                                          self.cluster_to_restore,
                                            self.buckets,
                                            self.backup_validation_files_location,
                                            self.backups,
                                            self.num_items,
                                            self.vbuckets)
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
        self.restore_services = self.get_services([self.backupset.restore_cluster_host],
                                                   self.services_init, start_node=0)
        if not self.restore_services:
            self.restore_services = ["kv"]
        self.per_node = self.input.param("per_node", True)
        if not os.path.exists(self.backup_validation_files_location):
            os.mkdir(self.backup_validation_files_location)
        shell.disconnect()

    def tearDown(self):
        super(EnterpriseBackupRestoreBase, self).tearDown()
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
            backup_directory += self.master.ip
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
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, self)

    def store_vbucket_seqno(self):
        vseqno = self.get_vbucket_seqnos(self.cluster_to_backup,
                                         self.buckets,
                                         self.skip_consistency, self.per_node)
        self.vbucket_seqno.append(vseqno)

    def backup_create(self, del_old_backup=True):
        args = "config --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        if self.backupset.exclude_buckets:
            args += " --exclude-buckets \"{0}\"".format(",".join(self.backupset.exclude_buckets))
        if self.backupset.include_buckets:
            args += " --include-buckets \"{0}\"".format(",".join(self.backupset.include_buckets))
        if self.backupset.disable_bucket_config:
            args += " --disable-bucket-config"
        if self.backupset.disable_views:
            args += " --disable-views"
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes"
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes"
        if self.backupset.disable_data:
            args += " --disable-data"
        if self.vbucket_filter:
            if self.vbucket_filter == "empty":
                args += " --vbucket-filter "
            elif self.vbucket_filter == "all":
                all_vbuckets = "0"
                for x in range (1, 1024):
                    all_vbuckets += "," + str(x)
                args += " --vbucket-filter {0}".format(all_vbuckets)
            else:
                args += " --vbucket-filter {0}".format(self.vbucket_filter)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        if del_old_backup:
            self.log.info("Remove any old dir before create new one")
            remote_client.execute_command("rm -rf %s" % self.backupset.directory)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        return output, error

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
        self.bk_printout = "Backup successfully completed".split(",")
        if RestHelper(RestConnection(self.backupset.backup_host)).is_ns_server_running():
            self.cbbkmgr_version = RestConnection(self.backupset.backup_host).get_nodes_version()

        if "4.6" <= self.cbbkmgr_version[:3]:
            self.cluster_flag = "--cluster"

        args = "backup --archive {0} --repo {1} {6} http{7}://{2}:{8}{3} "\
                   "{4} {5}".format(self.backupset.directory, self.backupset.name,
                   self.backupset.cluster_host.ip,
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
        if self.num_shards is not None:
            args += " --shards {0} ".format(self.num_shards)
        """
           MB-33698: there are changes in cbbackupmgr
           file chunking and sqlite will be set as default
        """
        if self.sqlite_storage:
            args += " --storage sqlite "
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
            if self.cbbkmgr_version[:5] in COUCHBASE_FROM_MAD_HATTER:
                self.bk_printout.append('Backed up bucket "{0}" succeeded'.format(bucket.name))
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
                self.fail("Taking cluster backup failed. Check printout below. "
                          "\nErrors: {0} \nOutput: {1}".format(error, output))
        self.backup_list()
        if repeats < 2 and validate_directory_structure:
            status, msg = self.validation_helper.validate_backup()
            if not status:
                self.fail(msg)
            self.log.info(msg)
        if not self.backupset.deleted_buckets:
            if not self.backupset.force_updates:
                self.store_vbucket_seqno()
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)

    def backup_restore(self):
        if self.restore_only:
            if self.create_fts_index:
                self.backups.append("2017-05-18T13_40_30.842368123-07_00")
            else:
                self.backups.append("2017-05-18T11_55_22.009680763-07_00")
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

        if "4.6" <= RestConnection(self.backupset.backup_host).get_nodes_version():
            self.cluster_flag = "--cluster"

        args = "restore --archive {0} --repo {1} {2} http{9}://{3}:{10}{4} "\
               "{5} {6} --start {7} --end {8}" \
                               .format(self.backupset.directory,
                                       self.backupset.name,
                                       self.cluster_flag,
                                       self.backupset.restore_cluster_host.ip,
                                       self.backupset.restore_cluster_host.port,
                                       user_input,
                                       password_input,
                                       backup_start, backup_end, url_format, secure_port)
        if self.backupset.no_ssl_verify:
            args += " --no-ssl-verify"
        if self.backupset.secure_conn:
            if not self.backupset.rt_no_cert:
                args += " --cacert %s" % cacert
        if self.backupset.exclude_buckets:
            args += " --exclude-buckets {0}".format(self.backupset.exclude_buckets)
        if self.backupset.include_buckets:
            args += " --include-buckets {0}".format(self.backupset.include_buckets)
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
            for bucket in self.buckets:
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
                        self._reset_storage_mode(rest_conn, self.test_storage_mode)

                    self.log.info("replica in bucket {0} is {1}".format(bucket.name, replicas))
                    rest_conn.create_bucket(bucket=bucket_name,
                                    ramQuotaMB=int(bucket_size) - 1,
                                    replicaNumber=replicas,
                                    authType=bucket.authType if bucket.authType else 'none',
                                    bucketType=self.bucket_type,
                                    proxyPort=bucket.port,
                                    evictionPolicy=self.eviction_policy,
                                    lww=self.lww_new,
                                    compressionMode=bucket_compression_mode)
                    bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
                    if not bucket_ready:
                        self.fail("Bucket {0} not created after 120 seconds.".format(bucket_name))
                    if has_index_node:
                        self.sleep(5, "wait for index service ready")
                elif self.backupset.map_buckets and self.same_cluster:
                    bucket_maps = ""
                    bucket_name = bucket.name + "_" + str(count)
                    self.log.info("replica in bucket {0} is {1}".format(bucket_name, replicas))
                    if self.backupset.delete_old_bucket:
                        BucketOperationHelper.delete_bucket_or_assert(\
                                       self.backupset.restore_cluster_host, bucket.name, self)
                    rest_conn.create_bucket(bucket=bucket_name,
                                    ramQuotaMB=int(bucket_size) - 1,
                                    replicaNumber=replicas,
                                    authType=bucket.authType if bucket.authType else 'none',
                                    bucketType=self.bucket_type,
                                    proxyPort=bucket.port,
                                    evictionPolicy=self.eviction_policy,
                                    lww=self.lww_new,
                                    compressionMode=bucket_compression_mode)
                    bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
                    if not bucket_ready:
                        self.fail("Bucket {0} not created after 120 seconds.".format(bucket_name))
                buckets.append("%s=%s" % (bucket.name, bucket_name))
                count +=1
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
                        args += " --replace-ttl {0} --replace-ttl-with 0"\
                                                 .format(self.replace_ttl)
                    else:
                        ttl_date, _ = shell.execute_command(self.rfc3339_date)
                        self.seconds_with_ttl, _ = shell.execute_command(self.seconds_with_ttl)
                        if self.seconds_with_ttl:
                            self.ttl_value = self.seconds_with_ttl[0]
                        if ttl_date and ttl_date[0]:
                            args += " --replace-ttl {0} --replace-ttl-with {1}"\
                                         .format(self.replace_ttl, ttl_date[0])
                        elif isinstance(self.replace_ttl_with, str):
                            args += " --replace-ttl {0} --replace-ttl-with {1}"\
                                     .format(self.replace_ttl, self.replace_ttl_with)
            elif self.replace_ttl == "add-none":
                args += " --replace-ttl none"
            elif self.replace_ttl == "empty-flag":
                args += " --replace-ttl-with {0}".format(self.replace_ttl_with)
            else:
                args += " --replace-ttl {0}".format(self.replace_ttl)
        command = "{3} {2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                   password_env, user_env)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        self._verify_bucket_compression_mode(bucket_compression_mode)
        errors_check = ["Unable to process value for", "Error restoring cluster",
                        "Expected argument for option"]
        if "Error restoring cluster" in output[0] or "Unable to process value" in output[0] \
            or "Expected argument for option" in output[0]:
            if not self.should_fail:
                if not self.restore_should_fail:
                    self.fail("Failed to restore cluster")
            else:
                self.log.info("This test is for negative test")
        res = output
        res.extend(error)
        error_str = "Error restoring cluster: Transfer failed. "\
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
        output, error =self.backup_restore()
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
        bk_log_file_name = "backup.log"
        if "6.5" <= RestConnection(self.backupset.backup_host).get_nodes_version():
            bk_log_file_name = "backup-*.log"
        command = "grep 'Transfer plan finished successfully' " + self.backupset.directory + \
                  "/logs/{0}".format(bk_log_file_name)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if not output:
            self.fail("Restoring backup failed.")
        command = "grep 'Transfer failed' " + self.backupset.directory + \
                  "/logs/{0}".format(bk_log_file_name)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if output:
            self.fail("Restoring backup failed.")

        self.log.info("Finished restoring backup")
        self.log.info("Get current vseqno on node %s " % self.cluster_to_restore[0].ip )

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
        args = "list --archive {0}".format(self.backupset.directory)
        if self.backupset.backup_list_name:
            args += " --repo {0}".format(self.backupset.backup_list_name)
        if self.backupset.backup_incr_backup:
            args += " --backup {0}".format(self.backupset.backup_incr_backup)
        if self.backupset.bucket_backup:
            args += " --bucket {0}".format(self.backupset.bucket_backup)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
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

    def backup_remove(self):
        args = "remove --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.verify_cluster_stats()
        if error:
            return False, error, "Removing backup failed."
        else:
            return True, output, "Removing of backup success"

    def backup_list_validate(self):
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        status, message = self.validation_helper.validate_backup_list(output)
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

    def backup_compact_deleted_keys_validation(self, delete_keys):
        self.log.info("Check deleted keys status in file after compact")
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = conn.execute_command("ls {0}/backup/201*/default*/data "\
                                                     .format(self.backupset.directory))
        deleted_key_status = {}
        if "shard_0.sqlite.0" in output:
            cmd = "{0}cbsqlitedump{1} "\
                  " -f {2}/backup/201*/default*/data/shard_0.sqlite.0 | grep -A 6 ent-backup "\
                                         % (self.cli_command_location, self.cmd_ext,\
                                         self.backupset.directory)
            dump_output, error = conn.execute_command(cmd)
            if dump_output:
                key_ids = [x.split(":")[1].strip(' ') for x in dump_output[0::8]]
                miss_keys = [x for x in delete_keys if x not in key_ids]
                if miss_keys:
                    raise Exception("Lost some keys {0} ".format(miss_keys))
                partition_ids =  [x.split(":")[1].strip(' ') for x in dump_output[1::8]]
                status_ids =     [x.split(" ")[-3].strip(' ') for x in dump_output[6::8]]
                for idx, key in enumerate(key_ids):
                    deleted_key_status[key] = \
                           {"KV store name":partition_ids[idx], "Status":status_ids[idx]}
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
        if self.num_shards is not None:
            num_shards += " --shards {0} ".format(self.num_shards)
        backup_result = self.cluster.async_backup_cluster(
                                           cluster_host=self.backupset.cluster_host,
                                           backup_host=self.backupset.backup_host,
                                           directory=self.backupset.directory,
                                           name=self.backupset.name,
                                           resume=self.backupset.resume,
                                           purge=self.backupset.purge,
                                           no_progress_bar=self.no_progress_bar,
                                           cli_command_location=self.cli_command_location,
                                           cb_version=self.cb_version,
                                           num_shards=num_shards)
        self.sleep(5)
        conn_bk = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn_bk.pause_memcached(timesleep=8)
        conn_bk.unpause_memcached()
        conn_bk.disconnect()
        output = backup_result.result(timeout=200)
        if self.debug_logs:
            if output:
                print("\nOutput from backup cluster: %s " % output)
            else:
                self.fail("No output printout.")
        self.assertTrue(self._check_output("Backup successfully completed", output),
                        "Backup failed with memcached crash and restart within 180 seconds")
        self.log.info("Backup succeeded with memcached crash and restart within 180 seconds")
        self.sleep(20)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory,
                                                    self.backupset.name)
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
        self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                         self.backup_validation_files_location, merge=True)
        conn.disconnect()

    def bk_with_erlang_crash_and_restart(self):
        num_shards = ""
        if self.num_shards is not None:
            num_shards += " --shards {0} ".format(self.num_shards)
        backup_result = self.cluster.async_backup_cluster(
                                           cluster_host=self.backupset.cluster_host,
                                           backup_host=self.backupset.backup_host,
                                           directory=self.backupset.directory,
                                           name=self.backupset.name,
                                           resume=self.backupset.resume,
                                           purge=self.backupset.purge,
                                           no_progress_bar=self.no_progress_bar,
                                           cli_command_location=self.cli_command_location,
                                           cb_version=self.cb_version,
                                           num_shards=num_shards)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup successfully completed", output),
                        "Backup failed with erlang crash and restart within 180 seconds")
        self.log.info("Backup succeeded with erlang crash and restart within 180 seconds")
        self.sleep(30)
        conn.disconnect()
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory,
                                                    self.backupset.name)
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
        self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
        conn.disconnect()

    def bk_with_cb_server_stop_and_restart(self):
        num_shards = ""
        if self.num_shards is not None:
            num_shards += " --shards {0} ".format(self.num_shards)
        backup_result = self.cluster.async_backup_cluster(
                                           cluster_host=self.backupset.cluster_host,
                                           backup_host=self.backupset.backup_host,
                                           directory=self.backupset.directory,
                                           name=self.backupset.name,
                                           resume=self.backupset.resume,
                                           purge=self.backupset.purge,
                                           no_progress_bar=self.no_progress_bar,
                                           cli_command_location=self.cli_command_location,
                                           cb_version=self.cb_version,
                                           num_shards=num_shards)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup successfully completed", output),
                        "Backup failed with couchbase stop and start within 180 seconds")
        self.log.info("Backup succeeded with couchbase stop and start within 180 seconds")
        self.sleep(30)
        conn.disconnect()
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory,
                                                    self.backupset.name)
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
        self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
        conn.disconnect()

    def bk_with_stop_and_resume(self):
        old_backup_name = ""
        new_backup_name = ""
        num_shards = ""
        if self.num_shards is not None:
            num_shards += " --shards {0} ".format(self.num_shards)
        backup_result = self.cluster.async_backup_cluster(
                                           cluster_host=self.backupset.cluster_host,
                                           backup_host=self.backupset.backup_host,
                                           directory=self.backupset.directory,
                                           name=self.backupset.name,
                                           resume=False,
                                           purge=self.backupset.purge,
                                           no_progress_bar=self.no_progress_bar,
                                           cli_command_location=self.cli_command_location,
                                           cb_version=self.cb_version,
                                           num_shards=num_shards)
        self.sleep(3)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang(self.os_name)
        output = backup_result.result(timeout=200)
        self.log.info(str(output))
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                old_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}"
                                            "_\d{2}.\d+-\d{2}_\d{2}", line).group()
                self.log.info("Backup name before resume: " + old_backup_name)
        conn.start_couchbase()
        conn.disconnect()
        self.sleep(30)
        output, error = self.backup_cluster()
        if error or not self._check_output("Backup successfully completed", output):
            self.fail("Taking cluster backup failed.")
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                new_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}"
                                            "_\d{2}.\d+-\d{2}_\d{2}", line).group()
                self.log.info("Backup name after resume: " + new_backup_name)
        self.assertEqual(old_backup_name, new_backup_name,
                         "Old backup name and new backup name are not same when resume is used")
        self.log.info("Old backup name and new backup name are same when resume is used")

    def backup_merge(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " \
                                                   + str(self.number_of_backups_taken))
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
        args = "merge --archive {0} --repo {1} --start {2} --end {3}"\
                                      .format(self.backupset.directory,
                                              self.backupset.name,
                                              backup_start, backup_end)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            return False, error, "Merging backup failed"
        elif output and "Merge completed successfully" not in output[0]:
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
        self.sleep(5, "Wait for all shards are written")
        bk_file_data, _ = data_collector.get_kv_dump_from_backup_file(server_host,
                                      self.cli_command_location, self.cmd_ext,
                                      self.backupset.directory, master_key,
                                      self.buckets)
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
                        print("key in backup file of bucket %s:  %s" \
                                                               % (bucket.name, key))
                    if validate_keys:
                        if pattern.search(key):
                            regex_backup_data[bucket.name][key] = \
                                     restore_file_data[bucket.name][key]
                            key_in_file_match_regex += 1
                    else:
                        if self.debug_logs:
                            print("value of key in backup file  ",\
                                                      restore_file_data[bucket.name][key])
                        if pattern.search(restore_file_data[bucket.name][key]["Value"]):
                            regex_backup_data[bucket.name][key] = \
                                         restore_file_data[bucket.name][key]
                            key_in_file_match_regex += 1
                if self.debug_logs:
                    print("\nKeys and value in backup file of bucket {0} \
                           that matches pattern '{1}'" \
                            .format(bucket.name, regex_pattern))
                    for x in regex_backup_data[bucket.name]:
                        print("key: ", x)
                        print("value: ", regex_backup_data[bucket.name][x]["Value"])
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
                                                bucket: {1} != {2} file"\
                                        .format(key, buckets_data[bucket.name][key],
                                            restore_file_data[bucket.name][key]["Value"]))
                                    data_matched = False
                                    count += 1
                                else:
                                    raise Exception ("Data not match in backup bucket {0}"\
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
                raise Exception ("Total key counts do not match.  Backup {0} != {1} bucket"\
                                         .format(restore_file_data[bucket.name], key_count))
            self.log.info("******** Data macth in backup file and bucket {0} ******** "\
                                                                   .format(bucket.name))
            print("Bucket: ", bucket.name)
            print("Total items in backup file:   ", len(bk_file_data[bucket.name]))
            if regex_pattern is not None:
                print("Total items to be restored with regex pattern '{0}' is {1} "\
                                                   .format(regex_pattern, key_count))
            print("Total items in bucket should be:   ", key_count)
            rest = RestConnection(server_bucket[0])
            actual_keys = rest.get_active_key_count(bucket.name)
            print("Total items actual in bucket:      ", actual_keys)
            if actual_keys != key_count:
                self.fail("Total keys matched: %s != Total keys in bucket %s: %s" \
                          "at node %s " % (key_count, bucket.name, actual_keys,
                                           server_bucket[0]))
            if self.merged:
                if key_check:
                    self.log.info("Check if deleted keys still in backup after merged" )
                    for key in restore_file_data[bucket.name]:
                        if key == key_check:
                            raise Exception ("There is an old key after delete bucket,"
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
                print("\noutput:  ", repr(output[x]))
                print("source:  ", repr(source[x]))
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
        cmd = "%s/couchbase-cli ssl-manage -c %s:8091 -u Administrator -p password "\
              " --cluster-cert-info > %s" % (self.cli_command_location,
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
                    if "shard_0.sqlite.0" in x:
                        if x.strip().split()[0][-2:] in unit_size:
                            file_info["file_size"] = \
                                      int(x.strip().split()[0][:-2].split(".")[0])

                            file_info["items"] = int(x.strip().split()[1])
                        print("output content   ", file_info)
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

    def _check_output(self, word_check, output):
        found = False
        if len(output) >=1 :
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

    def _reset_storage_mode(self, rest, storageMode):
        nodes_in_cluster = rest.get_nodes()
        for node in nodes_in_cluster:
            RestConnection(node).force_eject_node()
        rest.set_indexer_storage_mode(username='Administrator',
                                          password='password',
                                          storageMode=storageMode)
        rest.init_node()

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
        if self.backupset.log_archive_env:
            self.log.info("set log arvhive env to {0}".format(self.backupset.directory))
            log_archive_env = "unset CB_ARCHIVE_PATH; export CB_ARCHIVE_PATH={0}; "\
                                                      .format(self.backupset.directory)
            if self.backupset.ex_logs_path:
                self.log.info("overwrite env log path with flag -o")
                args_env = " -o {0}".format(ex_logs_path)
            command = "{0} {1}/cbbackupmgr collect-logs {2}"\
                                            .format(log_archive_env,
                                                    self.cli_command_location,
                                                    args_env)
        else:
            if "-o" in args and self.backupset.no_log_output_flag:
                args = args.replace("-o", " ")
            command = "{0} {1}/cbbackupmgr {2}"\
                                            .format(log_archive_env,
                                                    self.cli_command_location,
                                                    args)
        output, error = shell.execute_command(command)
        shell.disconnect()
        if self._check_output("Collecting logs succeeded", output):
            self._verify_cbbackupmgr_logs()
        elif self.backupset.no_log_output_flag and self._check_output("error", output):
            self.log.info("This is negative test")
        elif self.backupset.ex_logs_path:
            if "non-exist_dir" in self.backupset.ex_logs_path:
                self.log.info("This is negative test on non exist output logs dir")
            else:
                self.fail("Failed to collect logs")
        else:
            self.fail("Failed to collect logs. Output: {0}".format(output))

    def _verify_cbbackupmgr_logs(self):
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
        if self.debug_logs:
            print("\nlog path: ", logs_path)
            print("output : ", output)
        log_name = ""
        if output:
            for x in output:
                if "collectinfo" in x:
                    log_name = x.split(".")[0]
                    shell.execute_command("cd {0}; unzip {1}"
                                             .format(logs_path, x))
        else:
            if self.backupset.log_archive_env:
                self.fail("Failed to pass CB_ARCHIVE_PATH")
            if self.backupset.ex_logs_path:
                self.fail("Failed to run with ex log path")

        output, _ = shell.execute_command("ls {0}".format(logs_path))
        if output:
            for ele in output:
                if log_name == ele:
                    output, _ = shell.execute_command("ls {0}"\
                                               .format(logs_path + "/" + log_name))
        if "C_" in output:
            win_path = "/C_/tmp/entbackup"
            output, _ = shell.execute_command("ls {0}".format(logs_path + win_path))
        dir_list =  ["backup", "logs"]
        for ele in dir_list:
            if ele not in output:
                self.fail("Missing dir/file {0} in cbbackupmgr logs".format(ele))

        output, _ = shell.execute_command("ls {0}".format(logs_path + "/backup"))
        if output and "backup-meta.json" not in output[0]:
            self.fail("Missing file 'backup-meta.json' in backup dir")
        output, _ = shell.execute_command("ls {0}".format(logs_path + "/logs"))
        if output and ".log" not in output[0]:
            self.fail("Missing file 'backup.log' in backup logs dir")
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
            output, error = shell.execute_command("ls {0}/backup/*/{1}*/data "\
                                             .format(self.backupset.directory, bucket.name))
            filter_vbucket_keys = {}
            total_filter_keys = 0
            backup_files = []
            backup_data = []
            """ get vbucket keys pair in data base """
            self.log.info("Collecting data from backup repo ...")
            for vb in vbucket_filter:
                cmd = "{0}cbsqlitedump{1} --no-meta --no-body "\
                  " -f {2}/backup/*/*/data/shard_{3}.sqlite.0 | grep 'Key:'"\
                                 .format(self.cli_command_location, self.cmd_ext,\
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
                            self.fail("vbucketId {0} of key {1} not from vbucket filters {2}"\
                                           .format(vBucketId, key, vbucket_filter))
            else:
                self.log.info("No keys with vbucket filter {0} restored".format(vbucket_filter))
        shell.disconnect()

    def _validate_restore_replace_ttl_with(self, ttl_set):
        data_collector = DataCollector()
        bk_file_data, _ = data_collector.get_kv_dump_from_backup_file(self.backupset.backup_host,
                                      self.cli_command_location, self.cmd_ext,
                                      self.backupset.directory, "ent-backup",
                                      self.buckets)
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        rest = RestConnection(self.backupset.restore_cluster_host)
        restore_buckets_items = rest.get_buckets_itemCount()
        buckets = rest.get_buckets()
        keys_fail = {}
        for bucket in buckets:
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
                    keys_fail[bucket.name][key] = []
                    keys_fail[bucket.name][key].append(items_info[key]['meta']['expiration'])
                    keys_fail[bucket.name][key].append(int(ttl_set))
                    if self.debug_logs:
                        print(("ttl time set: ", ttl_set))
                        print(("key {0} failed to set ttl with {1}".format(key,
                                   items_info[key]['meta']['expiration'])))

            if not ttl_matched:
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
                self.sleep(10, "wait for bucket update new stats")
                restore_buckets_items = rest.get_buckets_itemCount()
                if int(restore_buckets_items[bucket.name]) > 0:
                    if self.replace_ttl == "expired" and self.bk_with_ttl is not None:
                        if sleep_time < 600:
                            self.fail("Key did not expire after wait more than 10 seconds of ttl")
        shell.disconnect()

    def _verify_bucket_compression_mode(self, restore_bucket_compression_mode):
        if self.enable_firewall:
            if self.should_fail:
                self.log.info("No need to verify.  This is negative test")
                return
            else:
                self.sleep(10)
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
        from couchbase_helper.tuq_generators import JsonGenerator
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start=start, docs_per_day=self.docs_per_day)

    def create_save_function_body_test(self, appname, appcode, description="Sample Description",
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
        script_dir = os.path.dirname(__file__)
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

    def _verify_backup_events_definition(self, bk_fxn):
        backup_path = self.backupset.directory + "/backup/{0}/".format(self.backups[0])
        events_file_name = "events.json"
        bk_file_events_dir = "/tmp/backup_events{0}/".format(self.master.ip)
        bk_file_events_path = bk_file_events_dir + events_file_name

        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        self.log.info("create local dir")
        if os.path.exists(bk_file_events_dir):
            shutil.rmtree(bk_file_events_dir)
        os.makedirs(bk_file_events_dir)
        self.log.info("copy eventing definition from remote to local")
        shell.copy_file_remote_to_local(backup_path+events_file_name,
                                        bk_file_events_path)
        local_bk_def = open(bk_file_events_path)
        bk_file_fxn = json.loads(local_bk_def.read())
        for k, v in bk_file_fxn[0]["settings"].items():
            if v != bk_fxn[0]["settings"][k]:
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

        if self.ordered(rs_fxn) != self.ordered(bk_fxn):
            self.fail("Events definition of backup and restore cluster are different")

    def ordered(self, obj):
        if isinstance(obj, dict):
            return sorted((k, ordered(v)) for k, v in list(obj.items()))
        if isinstance(obj, list):
            return sorted(ordered(x) for x in obj)
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
        self.log.info("\n**** Verify number of files with extension {0} *****"\
                                                         .format(extension_name))
        if output and int(output[0]) != int(num_shards):
            mesg = "number of shards do not match.  Pass: {0} vs actual: {1}"\
                                                 .format(num_shards, output[0])
        else:
            num_files_matched = True
            self.log.info("Number of shards with extension {0} is {1}"\
                                                 .format(extension_name, output[0]))
        return num_files_matched, mesg

    def _shards_modification(self, action):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        for bucket in self.buckets:
            num_shards = 0
            now = datetime.datetime.now()
            backup_date = now.year
            cmd1 = "cd {0}/backup/; ls  | grep{2} '07_00' "\
                           .format(self.backupset.directory, backup_date, self.cmd_ext)
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

            cmd2 = "ls {0}/backup/{1}/{2}*/data/ | grep{3} .sqlite.0 | wc -l"\
                           .format(self.backupset.directory, backup_date,
                                   bucket.name, self.cmd_ext)
            output, error = shell.execute_command(cmd2)
            if output and int(output[0]) > 0:
                num_shards = int(output[0])

            if action == "remove":
                self.log.info("Remove a shard in backup repo")
                cmd = "cd {0}/backup/{1}*/{2}*/data/; rm -f shard_1.sqlite.0  "\
                             .format(self.backupset.directory, backup_date, bucket.name)

                output, error = shell.execute_command(cmd)
                cmd = "cd {0}/backup/{1}*/{2}*/data/; ls | grep{3} .sqlite.0 | wc -l"\
                             .format(self.backupset.directory, backup_date, bucket.name,
                                     self.cmd_ext)
                output, error = shell.execute_command(cmd)
            if action == "duplicate":
                self.log.info("Duplicate one shard in backup repo")
                cmd = "cd {0}/backup/{1}*/{2}*/data/; cp shard_1.sqlite.0 shard_{3}.sqlite.0"\
                                                      .format(self.backupset.directory,
                                                       backup_date, bucket.name, num_shards + 1)
                output, error = shell.execute_command(cmd)
            if action == "corrupted":
                """ This test needs set to 20 shards to make sure all shards have data """
                if num_shards > 20:
                    self.fail("This test needs to set 20 shards to run")
                if num_shards > 0:
                    self.log.info("Corrupted a shard in backup repo")
                    cmd = "cd {0}/backup/{1}*/{2}*/data/; echo 'Hello world' > shard_1.sqlite.0"\
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
            cmd1 = "ls {0}/backup/{1}*/{2}*/data | grep \.sqlite | wc -l "\
                                 .format(self.backupset.directory, now.year, bucket.name)
            num_shards, error = shell.execute_command(cmd1)

            if num_shards and int(num_shards[0]) > 0:
                for i in range(0, int(num_shards[0])):
                    cmd2 = "{0}forestdb_dump{1} --plain-meta "\
                      "{2}/backup/{3}*/{4}*/data/shard_{5}.sqlite.0 | grep partition | wc -l "\
                                              .format(self.cli_command_location, self.cmd_ext,\
                                                      self.backupset.directory, now.year,\
                                                      bucket.name, i)
                    output, error = shell.execute_command(cmd2, debug=False)
                    vbuckets_per_shard[bucket.name][i] = int(output[0])
        shell.disconnect()

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
        self.passwd_env = False
        self.overwrite_user_env = False
        self.overwrite_passwd_env = False
        self.user_env_with_prompt = False
        self.passwd_env_with_prompt = False
        self.deleted_buckets = []
        self.new_buckets = []
        self.flushed_buckets = []
        self.deleted_backups = []


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
            self.assertTrue(str(ex) == '"User was not found."', str(ex))

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
        self.backup_cluster_validate(repeats=self.number_of_repeats)

    def backup_with_expiry(self):
        """
        Backup the cluster and validate the backupset with expiry items before they expire.
        :return: Nothing
        """
        for bucket in self.buckets:
            cb = self._get_python_sdk_client(self.master.ip, bucket, self.backupset.cluster_host)
            for i in range(int(self.num_items * 0.7) + 1, self.num_items + 1):
                cb.upsert("doc" + str(i), {"key":"value"}, ttl=self.expires)
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
                cb.upsert("doc" + str(i), {"key":"value"}, ttl=self.expires)
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
        if len(output) >=1 :
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in x.lower():
                            self.log.info("Found '{0}' in CLI output".format(ele))
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

    def merge(self):
        """
        Merge all the existing backups in the backupset.
        :return: Nothing
        """
        self.backup_merge_validate(repeats=self.number_of_repeats, skip_validation=self.skip_validation)

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
        self.sleep(30)
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)
            self.validation_helper.validate_merge(self.backup_validation_files_location)
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
        self.sleep(30)
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)
            self.validation_helper.validate_merge(self.backup_validation_files_location)
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
        self.sleep(30)
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)
            self.validation_helper.validate_merge(self.backup_validation_files_location)
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
        if ip:
            ip_to_failover = ip
        else:
            ip_to_failover = self.servers[1].ip
        for node in nodes_all:
            if node.ip == ip_to_failover:
                rest.fail_over(otpNode=node.id, graceful=self.graceful)

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
                self.sleep(60)
                rest.set_recovery_type(otpNode=node.id,
                                       recoveryType=self.recoveryType)
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
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                rest.set_recovery_type(otpNode=node.id,
                                       recoveryType=self.recoveryType)
                rest.add_back_node(otpNode=node.id)
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
        self.async_failover()

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
            bucket = CBBucket(name=name, authType=None, saslPassword=None,
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
            bucket = Bucket(name=name, authType=None, saslPassword=None,
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

            self.sleep(60)
            shell.disconnect()

    def create_indexes(self):
        rest_src = RestConnection(self.backupset.cluster_host)
        rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                          self.servers[1].ip, services=['index', 'fts'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
        rebalance.result()

        cmd = "cbindex -type create -bucket default -using plasma -index " \
              "age_idx -fields=age -auth {0}:{1}".format(self.servers[0].rest_username,
                                                         self.servers[0].rest_password,)
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")
        cmd = "cbindex -type create -bucket default -using plasma -index " \
              "name_idx -fields=name -auth {0}:{1}".format(self.servers[0].rest_username,
                                                           self.servers[0].rest_password,)
        remote_client.disconnect()
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")

        index_definition = INDEX_DEFINITION
        index_name = index_definition['name'] = "age"
        rest_fts = RestConnection(self.servers[1])
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
                                            authType=bucket.authType if bucket.authType else 'none',
                                            bucketType=self.bucket_type,
                                            proxyPort=bucket.port,
                                            saslPassword=bucket.saslPassword,
                                            evictionPolicy=self.eviction_policy,
                                            lww=self.lww_new)
                    bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
                    if not bucket_ready:
                        self.fail("Bucket %s not created after 120 seconds."
                                  % bucket_name)

            if self.overwrite_indexes:
                cmd = "cbindex -type create -bucket default -using plasma -index " \
                      "age_idx1 -fields=age -auth {0}:{1}".format(self.servers[0].rest_username,
                                                         self.servers[0].rest_password,)
                remote_client = RemoteMachineShellConnection(
                    self.backupset.restore_cluster_host)
                command = "{0}/{1}".format(self.cli_command_location, cmd)
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
                if error or "Index created" not in output[-1]:
                    self.fail("GSI index cannot be created")

                index_definition = INDEX_DEFINITION
                index_name = index_definition['name'] = "age"
                rest_fts = RestConnection(self.servers[2])
                try:
                    self.log.info("Create fts index")
                    rest_fts.create_fts_index(index_name, index_definition)
                except Exception as ex:
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
