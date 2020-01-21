import random
import time
import json, subprocess

import logger
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from testconstants import LINUX_COUCHBASE_SAMPLE_PATH, \
    WIN_COUCHBASE_SAMPLE_PATH_C, \
    WIN_BACKUP_C_PATH, LINUX_BACKUP_PATH, LINUX_COUCHBASE_LOGS_PATH, \
    WIN_COUCHBASE_LOGS_PATH, WIN_TMP_PATH, WIN_TMP_PATH_RAW, \
    WIN_BACKUP_PATH, LINUX_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, LINUX_CB_PATH,\
    MAC_COUCHBASE_BIN_PATH, WIN_COUCHBASE_BIN_PATH, WIN_ROOT_PATH, WIN_CB_PATH

from couchbase_helper.cluster import Cluster
from security.rbac_base import RbacBase
from security.rbacmain import rbacmain
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper


log = logger.Logger.get_logger()


class CliBaseTest(BaseTestCase):
    vbucketId = 0

    def setUp(self):
        self.times_teardown_called = 1
        super(CliBaseTest, self).setUp()
        self.r = random.Random()
        self.vbucket_count = 1024
        self.cluster = Cluster()
        self.clusters_dic = self.input.clusters
        if self.clusters_dic:
            if len(self.clusters_dic) > 1:
                self.dest_nodes = self.clusters_dic[1]
                self.dest_master = self.dest_nodes[0]
            elif len(self.clusters_dic) == 1:
                self.log.error("=== need 2 cluster to setup xdcr in ini file ===")
        else:
            self.log.error("**** Cluster config is setup in ini file. ****")
        self.shell = RemoteMachineShellConnection(self.master)
        if not self.skip_init_check_cbserver:
            self.rest = RestConnection(self.master)
            self.cb_version = self.rest.get_nodes_version()
            """ cli output message """
            self.cli_bucket_create_msg = "SUCCESS: Bucket created"
            self.cli_rebalance_msg = "SUCCESS: Rebalance complete"
            if self.cb_version[:3] == "4.6":
                self.cli_bucket_create_msg = "SUCCESS: bucket-create"
                self.cli_rebalance_msg = "SUCCESS: rebalanced cluster"
        self.import_back = self.input.param("import_back", False)
        if self.import_back:
            if len(self.servers) < 3:
                self.fail("This test needs minimum of 3 vms to run ")
        self.test_type = self.input.param("test_type", "import")
        self.import_file = self.input.param("import_file", None)
        self.imex_type = self.input.param("imex_type", "json")
        self.format_type = self.input.param("format_type", "lines")
        self.import_method = self.input.param("import_method", "file://")
        self.force_failover = self.input.param("force_failover", False)
        self.json_invalid_errors = self.input.param("json-invalid-errors", None)
        self.field_separator = self.input.param("field-separator", "comma")
        self.key_gen = self.input.param("key-gen", True)
        self.skip_docs = self.input.param("skip-docs", None)
        self.limit_docs = self.input.param("limit-docs", None)
        self.limit_rows = self.input.param("limit-rows", None)
        self.skip_rows = self.input.param("skip-rows", None)
        self.omit_empty = self.input.param("omit-empty", None)
        self.infer_types = self.input.param("infer-types", None)
        self.fx_generator = self.input.param("fx-generator", None)
        self.fx_gen_start = self.input.param("fx-gen-start", None)
        self.secure_conn = self.input.param("secure-conn", False)
        self.no_cacert = self.input.param("no-cacert", False)
        self.no_ssl_verify = self.input.param("no-ssl-verify", False)
        self.verify_data = self.input.param("verify-data", False)
        self.field_substitutions = self.input.param("field-substitutions", None)
        self.check_preload_keys = self.input.param("check-preload-keys", True)
        self.debug_logs = self.input.param("debug-logs", False)
        self.should_fail = self.input.param("should-fail", False)
        info = self.shell.extract_remote_info()
        self.os_version = info.distribution_version.lower()
        self.deliverable_type = info.deliverable_type.lower()
        type = info.type.lower()
        self.excluded_commands = self.input.param("excluded_commands", None)
        self.os = 'linux'
        self.full_v = None
        self.short_v = None
        self.build_number = None
        cmd =  'curl -g {0}:8091/diag/eval -u {1}:{2} '.format(self.master.ip,
                                                              self.master.rest_username,
                                                              self.master.rest_password)
        cmd += '-d "path_config:component_path(bin)."'
        bin_path  = subprocess.check_output(cmd, shell=True)
        try:
            bin_path = bin_path.decode()
        except AttributeError:
            pass
        if "bin" not in bin_path:
            self.fail("Check if cb server install on %s" % self.master.ip)
        else:
            self.cli_command_path = bin_path.replace('"', '') + "/"
        self.root_path = LINUX_ROOT_PATH
        self.tmp_path = "/tmp/"
        self.tmp_path_raw = "/tmp/"
        self.cmd_backup_path = LINUX_BACKUP_PATH
        self.backup_path = LINUX_BACKUP_PATH
        self.cmd_ext = ""
        self.src_file = ""
        self.des_file = ""
        self.sample_files_path = LINUX_COUCHBASE_SAMPLE_PATH
        self.log_path = LINUX_COUCHBASE_LOGS_PATH
        self.base_cb_path = LINUX_CB_PATH
        """ non root path """
        if self.nonroot:
            self.sample_files_path = "/home/%s%s" % (self.master.ssh_username,
                                                     LINUX_COUCHBASE_SAMPLE_PATH)
            self.log_path = "/home/%s%s" % (self.master.ssh_username,
                                            LINUX_COUCHBASE_LOGS_PATH)
            self.base_cb_path = "/home/%s%s" % (self.master.ssh_username,
                                                LINUX_CB_PATH)
            self.root_path = "/home/%s/" % self.master.ssh_username
        if type == 'windows':
            self.os = 'windows'
            self.cmd_ext = ".exe"
            self.root_path = WIN_ROOT_PATH
            self.tmp_path = WIN_TMP_PATH
            self.tmp_path_raw = WIN_TMP_PATH_RAW
            self.cmd_backup_path = WIN_BACKUP_C_PATH
            self.backup_path = WIN_BACKUP_PATH
            self.sample_files_path = WIN_COUCHBASE_SAMPLE_PATH_C
            self.log_path = WIN_COUCHBASE_LOGS_PATH
            win_format = "C:/Program Files"
            cygwin_format = "/cygdrive/c/Program\ Files"
            if win_format in self.cli_command_path:
                self.cli_command_path = self.cli_command_path.replace(win_format,
                                                                      cygwin_format)
            self.base_cb_path = WIN_CB_PATH
        if info.distribution_type.lower() == 'mac':
            self.os = 'mac'
        self.full_v, self.short_v, self.build_number = self.shell.get_cbversion(type)
        self.couchbase_usrname = "%s" % (self.input.membase_settings.rest_username)
        self.couchbase_password = "%s" % (self.input.membase_settings.rest_password)
        self.cb_login_info = "%s:%s" % (self.couchbase_usrname,
                                        self.couchbase_password)
        self.path_type = self.input.param("path_type", None)
        if self.path_type is None:
            self.log.info("Test command with absolute path ")
        elif self.path_type == "local":
            self.log.info("Test command at %s dir " % self.cli_command_path)
            self.cli_command_path = "cd %s; ./" % self.cli_command_path
        self.cli_command = self.input.param("cli_command", None)
        self.command_options = self.input.param("command_options", None)
        if self.command_options is not None:
            self.command_options = self.command_options.split(";")
        if str(self.__class__).find('couchbase_clitest.CouchbaseCliTest') == -1:
            if len(self.servers) > 1 and int(self.nodes_init) == 1:
                servers_in = [self.servers[i + 1] for i in range(self.num_servers - 1)]
                self.cluster.rebalance(self.servers[:1], servers_in, [])
        for bucket in self.buckets:
            testuser = [{'id': bucket.name, 'name': bucket.name, 'password': 'password'}]
            rolelist = [{'id': bucket.name, 'name': bucket.name, 'roles': 'admin'}]
            self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)


    def tearDown(self):
        if not self.input.param("skip_cleanup", True):
            if self.times_teardown_called > 1 :
                self.shell.disconnect()
        if self.input.param("skip_cleanup", True):
            if self.case_number > 1 or self.times_teardown_called > 1:
                self.shell.disconnect()
        self.times_teardown_called += 1
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        zones = rest.get_zone_names()
        for zone in zones:
            if zone != "Group 1":
                rest.delete_zone(zone)
        self.clusters_dic = self.input.clusters
        if self.clusters_dic:
            if len(self.clusters_dic) > 1:
                self.dest_nodes = self.clusters_dic[1]
                self.dest_master = self.dest_nodes[0]
                if self.dest_nodes and len(self.dest_nodes) > 1:
                    self.log.info("======== clean up destination cluster =======")
                    rest = RestConnection(self.dest_nodes[0])
                    rest.remove_all_remote_clusters()
                    rest.remove_all_replications()
                    BucketOperationHelper.delete_all_buckets_or_assert(self.dest_nodes, self)
                    ClusterOperationHelper.cleanup_cluster(self.dest_nodes)
            elif len(self.clusters_dic) == 1:
                self.log.error("=== need 2 cluster to setup xdcr in ini file ===")
        else:
            self.log.info("**** If run xdcr test, need cluster config is setup in ini file. ****")
        super(CliBaseTest, self).tearDown()


    """ in sherlock, there is an extra value called runCmd in the 1st element """
    def del_runCmd_value(self, output):
        if "runCmd" in output[0]:
            output = output[1:]
        return output

    def verifyCommandOutput(self, output, expect_error, message):
        """Inspects each line of the output and checks to see if the expected error was found

        Options:
        output - A list of output lines
        expect_error - Whether or not the command should have succeeded or failed
        message - The success or error message

        Returns a boolean indicating whether or not the error/success message was found in the output
        """
        if expect_error:
            for line in output:
                if line == "ERROR: " + message:
                    return True
            log.info("Did not receive expected error message `ERROR: %s`", message)
            return False
        else:
            for line in output:
                if line == "SUCCESS: " + message:
                    return True
            log.info("Did not receive expected success message `SUCCESS: %s`", message)
            return False

    def verifyWarningOutput(self, output, message):
        for line in output:
            if line == "WARNING: " + message:
                return True
        log.info("Did not receive expected error message `WARNING: %s`", message)
        return False

    def verifyServices(self, server, expected_services):
        """Verifies that the services on a given node match the expected service

            Options:
            server - A TestInputServer object of the server to connect to
            expected_services - A comma separated list of services

            Returns a boolean corresponding to whether or not the expected services
            are available on the server.
        """
        rest = RestConnection(server)
        hostname = "%s:%s" % (server.ip, server.port)
        expected_services = expected_services.replace("data", "kv")
        expected_services = expected_services.replace("query", "n1ql")
        expected_services = expected_services.split(",")

        nodes_services = rest.get_nodes_services()
        for node, services in nodes_services.items():
            if node.encode('ascii') == hostname:
                if len(services) != len(expected_services):
                    log.info("Services on %s do not match expected services (%s vs. %s)",
                             hostname, services, expected_services)
                    return False
                for service in services:
                    if service.encode("ascii") not in expected_services:
                        log.info("Services on %s do not match expected services (%s vs. %s)",
                                 hostname, services, expected_services)
                        return False
                return True

        log.info("Services on %s not found, the server may not exist", hostname)
        return False

    def verifyRamQuotas(self, server, data, index, fts):
        """Verifies that the RAM quotas for each service are set properly

        Options:
        server - A TestInputServer object of the server to connect to
        data - An int containing the data service RAM quota, None will skip the check
        index - An int containing the index service RAM quota, None will skip the check
        fts - An int containing the FTS service RAM quota, None will skip the check

        Returns a boolean corresponding to whether or not the RAM quotas were set properly
        """
        rest = RestConnection(server)
        settings = rest.get_pools_default()
        if data:
            if "memoryQuota" not in settings:
                log.info("Unable to get data service ram quota")
                return False
            if int(settings["memoryQuota"]) != int(data):
                log.info("Data service memory quota does not match (%d vs %d)",
                         int(settings["memoryQuota"]), int(data))
                return False

        if index:
            if "indexMemoryQuota" not in settings:
                log.info("Unable to get index service ram quota")
                return False
            if int(settings["indexMemoryQuota"]) != int(index):
                log.info(
                    "Index service memory quota does not match (%d vs %d)",
                    int(settings["indexMemoryQuota"]), int(index))
                return False

        if fts:
            if "ftsMemoryQuota" not in settings:
                log.info("Unable to get fts service ram quota")
                return False
            if int(settings["ftsMemoryQuota"]) != int(fts):
                log.info("FTS service memory quota does not match (%d vs %d)",
                         int(settings["ftsMemoryQuota"]), int(fts))
                return False

        return True

    def verifyBucketSettings(self, server, bucket_name, bucket_type, memory_quota,
                             eviction_policy, replica_count, enable_index_replica,
                             priority, enable_flush):
        rest = RestConnection(server)
        result = rest.get_bucket_json(bucket_name)

        if bucket_type == "couchbase":
            bucket_type = "membase"

        if bucket_type is not None and bucket_type != result["bucketType"]:
            log.info("Memory quota does not match (%s vs %s)", bucket_type,
                     result["bucketType"])
            return False

        quota = result["quota"]["rawRAM"] // 1024 // 1024
        if memory_quota is not None and memory_quota != quota:
            log.info("Bucket quota does not match (%s vs %s)", memory_quota,
                     quota)
            return False

        if eviction_policy is not None and eviction_policy != result[
            "evictionPolicy"]:
            log.info("Eviction policy does not match (%s vs %s)",
                     eviction_policy, result["evictionPolicy"])
            return False

        if replica_count is not None and replica_count != result[
            "replicaNumber"]:
            log.info("Replica count does not match (%s vs %s)", replica_count,
                     result["replicaNumber"])
            return False

        if enable_index_replica == 1:
            enable_index_replica = True
        elif enable_index_replica == 0:
            enable_index_replica = False

        if enable_index_replica is not None and enable_index_replica != result[
            "replicaIndex"]:
            log.info("Replica index enabled does not match (%s vs %s)",
                     enable_index_replica, result["replicaIndex"])
            return False

        if priority == "high":
            priority = 8
        elif priority == "low":
            priority = 3

        if priority is not None and priority != result["threadsNumber"]:
            log.info("Bucket priority does not match (%s vs %s)", priority,
                     result["threadsNumber"])
            return False

        if enable_flush is not None:
            if enable_flush == 1 and "flush" not in result["controllers"]:
                log.info("Bucket flush is not enabled, but it should be")
                return False
            elif enable_flush == 0 and "flush" in result["controllers"]:
                log.info("Bucket flush is not enabled, but it should be")
                return False

        return True

    def verifyContainsBucket(self, server, name):
        rest = RestConnection(server)
        buckets = rest.get_buckets()

        for bucket in buckets:
            if bucket.name == name:
                return True
        return False

    def verifyClusterName(self, server, name):
        rest = RestConnection(server)
        settings = rest.get_pools_default("waitChange=0")

        if name is None:
            name = ""
        if name == "empty":
            name = " "

        if "clusterName" not in settings:
            log.info("Unable to get cluster name from server")
            return False
        if settings["clusterName"] != name:
            log.info("Cluster name does not match (%s vs %s)",
                     settings["clusterName"], name)
            return False

        return True

    def isClusterInitialized(self, server):
        """Checks whether or not the server is initialized

        Options:
        server - A TestInputServer object of the server to connect to

        Checks to see whether or not the default pool was created in order to
        determine whether or no the server was initialized. Returns a boolean value
        to indicate initialization.
        """
        rest = RestConnection(server)
        settings = rest.get_pools_info()
        if "pools" in settings and len(settings["pools"]) > 0:
            return True

        return False

    def verifyNotificationsEnabled(self, server):
        rest = RestConnection(server)
        enabled = rest.get_notifications()
        if enabled:
            return True
        return False

    def verifyIndexSettings(self, server, max_rollbacks, stable_snap_interval,
                            mem_snap_interval,
                            storage_mode, threads, log_level):
        rest = RestConnection(server)
        settings = rest.get_global_index_settings()

        if storage_mode == "default":
            storage_mode = "plasma"
        elif storage_mode == "memopt":
            storage_mode = "memory_optimized"

        if max_rollbacks and str(settings["maxRollbackPoints"]) != str(
                max_rollbacks):
            log.info("Max rollbacks does not match (%s vs. %s)",
                     str(settings["maxRollbackPoints"]), str(max_rollbacks))
            return False
        if stable_snap_interval and str(
                settings["stableSnapshotInterval"]) != str(
                stable_snap_interval):
            log.info("Stable snapshot interval does not match (%s vs. %s)",
                     str(settings["stableSnapshotInterval"]),
                     str(stable_snap_interval))
            return False
        if mem_snap_interval and str(
                settings["memorySnapshotInterval"]) != str(mem_snap_interval):
            log.info("Memory snapshot interval does not match (%s vs. %s)",
                     str(settings["memorySnapshotInterval"]),
                     str(mem_snap_interval))
            return False
        if storage_mode and str(settings["storageMode"]) != str(storage_mode):
            log.info("Storage mode does not match (%s vs. %s)",
                     str(settings["storageMode"]), str(storage_mode))
            return False
        if threads and str(settings["indexerThreads"]) != str(threads):
            log.info("Threads does not match (%s vs. %s)",
                     str(settings["indexerThreads"]), str(threads))
            return False
        if log_level and str(settings["logLevel"]) != str(log_level):
            log.info("Log level does not match (%s vs. %s)",
                     str(settings["logLevel"]), str(log_level))
            return False

        return True

    def verifyAutofailoverSettings(self, server, enabled, timeout):
        rest = RestConnection(server)
        settings = rest.get_autofailover_settings()

        if enabled and not ((str(enabled) == "1" and settings.enabled) or (
                str(enabled) == "0" and not settings.enabled)):
            log.info("Enabled does not match (%s vs. %s)", str(enabled),
                     str(settings.enabled))
            return False
        if timeout and str(settings.timeout) != str(timeout):
            log.info("Timeout does not match (%s vs. %s)", str(timeout),
                     str(settings.timeout))
            return False

        return True

    def verifyAutoreprovisionSettings(self, server, enabled, max_nodes):
        rest = RestConnection(server)
        settings = rest.get_autoreprovision_settings()

        if enabled and not ((str(enabled) == "1" and settings.enabled) or (
                str(enabled) == "0" and not settings.enabled)):
            log.info("Enabled does not match (%s vs. %s)", str(max_nodes),
                     str(settings.enabled))
            return False
        if max_nodes and str(settings.max_nodes) != str(max_nodes):
            log.info("max_nodes does not match (%s vs. %s)", str(max_nodes),
                     str(settings.max_nodes))
            return False

        return True

    def verifyAuditSettings(self, server, enabled, log_path, rotate_interval):
        rest = RestConnection(server)
        settings = rest.getAuditSettings()

        if enabled and not (
            (str(enabled) == "1" and settings["auditdEnabled"]) or (
                str(enabled) == "0" and not settings["auditdEnabled"])):
            log.info("Enabled does not match (%s vs. %s)", str(enabled),
                     str(settings["auditdEnabled"]))
            return False
        if log_path and str(str(settings["logPath"])) != str(log_path):
            log.info("Log path does not match (%s vs. %s)", str(log_path),
                     str(settings["logPath"]))
            return False

        if rotate_interval and str(str(settings["rotateInterval"])) != str(
                rotate_interval):
            log.info("Rotate interval does not match (%s vs. %s)",
                     str(rotate_interval), str(settings["rotateInterval"]))
            return False

        return True

    def verifyPendingServer(self, server, server_to_add, group_name, services):
        rest = RestConnection(server)
        settings = rest.get_all_zones_info()
        if not settings or "groups" not in settings:
            log.info("Group settings payload appears to be invalid")
            return False

        expected_services = services.replace("data", "kv")
        expected_services = expected_services.replace("query", "n1ql")
        expected_services = expected_services.split(",")

        for group in settings["groups"]:
            for node in group["nodes"]:
                if node["hostname"] == server_to_add:
                    if node["clusterMembership"] != "inactiveAdded":
                        log.info("Node `%s` not in pending status",
                                 server_to_add)
                        return False

                    if group["name"] != group_name:
                        log.info("Node `%s` not in correct group (%s vs %s)",
                                 node["hostname"], group_name,
                                 group["name"])
                        return False

                    if len(node["services"]) != len(expected_services):
                        log.info("Services do not match on %s (%s vs %s) ",
                                 node["hostname"], services,
                                 ",".join(node["services"]))
                        return False

                    for service in node["services"]:
                        if service not in expected_services:
                            log.info("Services do not match on %s (%s vs %s) ",
                                     node["hostname"], services,
                                     ",".join(node["services"]))
                            return False
                    return True

        log.info("Node `%s` not found in nodes list", server_to_add)
        return False

    def verifyPendingServerDoesNotExist(self, server, server_to_add):
        rest = RestConnection(server)
        settings = rest.get_all_zones_info()
        if not settings or "groups" not in settings:
            log.info("Group settings payload appears to be invalid")
            return False

        for group in settings["groups"]:
            for node in group["nodes"]:
                if node["hostname"] == server_to_add:
                    return False

        log.info("Node `%s` not found in nodes list", server_to_add)
        return True

    def verifyActiveServers(self, server, expected_num_servers):
        return self._verifyServersByStatus(server, expected_num_servers,
                                           "active")

    def verifyFailedServers(self, server, expected_num_servers):
        return self._verifyServersByStatus(server, expected_num_servers,
                                           "inactiveFailed")

    def _verifyServersByStatus(self, server, expected_num_servers, status):
        rest = RestConnection(server)
        settings = rest.get_pools_default()

        count = 0
        for node in settings["nodes"]:
            if node["clusterMembership"] == status:
                count += 1

        return count == expected_num_servers

    def verifyRecoveryType(self, server, recovery_servers, recovery_type):
        rest = RestConnection(server)
        settings = rest.get_all_zones_info()
        if not settings or "groups" not in settings:
            log.info("Group settings payload appears to be invalid")
            return False

        if not recovery_servers:
            return True

        num_found = 0
        recovery_servers = recovery_servers.split(",")
        for group in settings["groups"]:
            for node in group["nodes"]:
                for rs in recovery_servers:
                    if node["hostname"] == rs:
                        if node["recoveryType"] != recovery_type:
                            log.info(
                                "Node %s doesn't contain recovery type %s ",
                                rs, recovery_type)
                            return False
                        else:
                            num_found = num_found + 1

        if num_found == len(recovery_servers):
            return True

        log.info("Node `%s` not found in nodes list",
                 ",".join(recovery_servers))
        return False

    def verifyUserRoles(self, server, username, roles):
        rest = RestConnection(server)
        status, content, header = rbacmain(server)._retrieve_user_roles()
        content = json.loads(content)
        temp = rbacmain()._parse_get_user_response(content, username, username, roles)
        return temp

    def verifyLdapSettings(self, server, admins, ro_admins, default, enabled):
        rest = RestConnection(server)
        settings = rest.ldapRestOperationGetResponse()

        if admins is None:
            admins = []
        else:
            admins = admins.split(",")

        if ro_admins is None:
            ro_admins = []
        else:
            ro_admins = ro_admins.split(",")

        if str(enabled) == "0":
            admins = []
            ro_admins = []

        if default == "admins" and str(enabled) == "1":
            if settings["admins"] != "asterisk":
                log.info("Admins don't match (%s vs asterisk)",
                         settings["admins"])
                return False
        elif not self._list_compare(settings["admins"], admins):
            log.info("Admins don't match (%s vs %s)", settings["admins"],
                     admins)
            return False

        if default == "roadmins" and str(enabled) == "1":
            if settings["roAdmins"] != "asterisk":
                log.info("Read only admins don't match (%s vs asterisk)",
                         settings["roAdmins"])
                return False
        elif not self._list_compare(settings["roAdmins"], ro_admins):
            log.info("Read only admins don't match (%s vs %s)",
                     settings["roAdmins"], ro_admins)
            return False

        return True

    def verifyAlertSettings(self, server, enabled, email_recipients,
                            email_sender, email_username, email_password,
                            email_host,
                            email_port, encrypted, alert_af_node,
                            alert_af_max_reached, alert_af_node_down,
                            alert_af_small,
                            alert_af_disable, alert_ip_changed,
                            alert_disk_space, alert_meta_overhead,
                            alert_meta_oom,
                            alert_write_failed, alert_audit_dropped):
        rest = RestConnection(server)
        settings = rest.get_alerts_settings()
        print(settings)

        if not enabled:
            if not settings["enabled"]:
                return True
            else:
                log.info("Alerts should be disabled")
                return False

        if encrypted is None or encrypted == "0":
            encrypted = False
        else:
            encrypted = True

        if email_recipients is not None and not self._list_compare(
                email_recipients.split(","), settings["recipients"]):
            log.info("Email recipients don't match (%s vs %s)",
                     email_recipients.split(","), settings["recipients"])
            return False

        if email_sender is not None and email_sender != settings["sender"]:
            log.info("Email sender does not match (%s vs %s)", email_sender,
                     settings["sender"])
            return False

        if email_username is not None and email_username != \
                settings["emailServer"]["user"]:
            log.info("Email username does not match (%s vs %s)",
                     email_username, settings["emailServer"]["user"])
            return False

        if email_host is not None and email_host != settings["emailServer"][
            "host"]:
            log.info("Email host does not match (%s vs %s)", email_host,
                     settings["emailServer"]["host"])
            return False

        if email_port is not None and email_port != settings["emailServer"][
            "port"]:
            log.info("Email port does not match (%s vs %s)", email_port,
                     settings["emailServer"]["port"])
            return False

        if encrypted is not None and encrypted != settings["emailServer"][
            "encrypt"]:
            log.info("Email encryption does not match (%s vs %s)", encrypted,
                     settings["emailServer"]["encrypt"])
            return False

        alerts = list()
        if alert_af_node:
            alerts.append('auto_failover_node')
        if alert_af_max_reached:
            alerts.append('auto_failover_maximum_reached')
        if alert_af_node_down:
            alerts.append('auto_failover_other_nodes_down')
        if alert_af_small:
            alerts.append('auto_failover_cluster_too_small')
        if alert_af_disable:
            alerts.append('auto_failover_disabled')
        if alert_ip_changed:
            alerts.append('ip')
        if alert_disk_space:
            alerts.append('disk')
        if alert_meta_overhead:
            alerts.append('overhead')
        if alert_meta_oom:
            alerts.append('ep_oom_errors')
        if alert_write_failed:
            alerts.append('ep_item_commit_failed')
        if alert_audit_dropped:
            alerts.append('audit_dropped_events')

        if not self._list_compare(alerts, settings["alerts"]):
            log.info("Alerts don't match (%s vs %s)", alerts,
                     settings["alerts"])
            return False

        return True

    def verify_node_settings(self, server, data_path, index_path, hostname):
        rest = RestConnection(server)
        node_settings = rest.get_nodes_self()

        if data_path != node_settings.storage[0].path:
            log.info("Data path does not match (%s vs %s)", data_path,
                     node_settings.storage[0].path)
            return False
        if index_path != node_settings.storage[0].index_path:
            log.info("Index path does not match (%s vs %s)", index_path,
                     node_settings.storage[0].index_path)
            return False
        if hostname is not None:
            if hostname != node_settings.hostname:
                log.info("Hostname does not match (%s vs %s)", hostname,
                         node_settings.hostname)
                return True
        return True

    def verifyCompactionSettings(self, server, db_frag_perc, db_frag_size,
                                 view_frag_perc, view_frag_size, from_period,
                                 to_period, abort_outside, parallel_compact,
                                 purgeInt):
        rest = RestConnection(server)
        settings = rest.get_auto_compaction_settings()
        ac = settings["autoCompactionSettings"]

        if db_frag_perc is not None and str(db_frag_perc) != str(
                ac["databaseFragmentationThreshold"]["percentage"]):
            log.info("DB frag perc does not match (%s vs %s)",
                     str(db_frag_perc),
                     str(ac["databaseFragmentationThreshold"]["percentage"]))
            return False

        if db_frag_size is not None and str(db_frag_size * 1024 ** 2) != str(
                ac["databaseFragmentationThreshold"]["size"]):
            log.info("DB frag size does not match (%s vs %s)",
                     str(db_frag_size * 1024 ** 2),
                     str(ac["databaseFragmentationThreshold"]["size"]))
            return False

        if view_frag_perc is not None and str(view_frag_perc) != str(
                ac["viewFragmentationThreshold"]["percentage"]):
            log.info("View frag perc does not match (%s vs %s)",
                     str(view_frag_perc),
                     str(ac["viewFragmentationThreshold"]["percentage"]))
            return False

        if view_frag_size is not None and str(
                        view_frag_size * 1024 ** 2) != str(
                ac["viewFragmentationThreshold"]["size"]):
            log.info("View frag size does not match (%s vs %s)",
                     str(view_frag_size * 1024 ** 2),
                     str(ac["viewFragmentationThreshold"]["size"]))
            return False

        print(from_period, to_period)
        if from_period is not None:
            fromHour, fromMin = from_period.split(":", 1)
            if int(fromHour) != int(ac["allowedTimePeriod"]["fromHour"]):
                log.info("From hour does not match (%s vs %s)", str(fromHour),
                         str(ac["allowedTimePeriod"]["fromHour"]))
                return False
            if int(fromMin) != int(ac["allowedTimePeriod"]["fromMinute"]):
                log.info("From minute does not match (%s vs %s)", str(fromMin),
                         str(ac["allowedTimePeriod"]["fromMinute"]))
                return False

        if to_period is not None:
            toHour, toMin = to_period.split(":", 1)
            if int(toHour) != int(ac["allowedTimePeriod"]["toHour"]):
                log.info("To hour does not match (%s vs %s)", str(toHour),
                         str(ac["allowedTimePeriod"]["toHour"]))
                return False
            if int(toMin) != int(ac["allowedTimePeriod"]["toMinute"]):
                log.info("To minute does not match (%s vs %s)", str(toMin),
                         str(ac["allowedTimePeriod"]["toMinute"]))
                return False

        if str(abort_outside) == "1":
            abort_outside = True
        elif str(abort_outside) == "0":
            abort_outside = False

        if abort_outside is not None and abort_outside != \
                ac["allowedTimePeriod"]["abortOutside"]:
            log.info("Abort outside does not match (%s vs %s)", abort_outside,
                     ac["allowedTimePeriod"]["abortOutside"])
            return False

        if str(parallel_compact) == "1":
            parallel_compact = True
        elif str(parallel_compact) == "0":
            parallel_compact = False

        if parallel_compact is not None and parallel_compact != ac[
            "parallelDBAndViewCompaction"]:
            log.info("Parallel compact does not match (%s vs %s)",
                     str(parallel_compact),
                     str(ac["parallelDBAndViewCompaction"]))
            return False

        if purgeInt is not None and str(purgeInt) != str(
                settings["purgeInterval"]):
            log.info("Purge interval does not match (%s vs %s)", str(purgeInt),
                     str(settings["purgeInterval"]))
            return False

        return True

    def verify_gsi_compact_settings(self, compact_mode, compact_percent,
                                    compact_interval,
                                    from_period, to_period, enable_abort):
        rest = RestConnection(self.master)
        settings = rest.get_auto_compaction_settings()
        ac = settings["autoCompactionSettings"]["indexFragmentationThreshold"]
        cc = settings["autoCompactionSettings"]["indexCircularCompaction"]
        if compact_mode is not None:
            if compact_mode == "append":
                self.log.info("append compactino settings %s " % ac)
                if compact_percent is not None and \
                                compact_percent != ac["percentage"]:
                    raise Exception(
                        "setting percent does not match.  Set: %s vs %s :Actual"
                        % (compact_percent, ac["percentage"]))
            if compact_mode == "circular":
                self.log.info("circular compaction settings %s " % cc)
                if enable_abort and not cc["interval"]["abortOutside"]:
                    raise Exception("setting enable abort failed")
                if compact_interval is not None:
                    if compact_interval != cc["daysOfWeek"]:
                        raise Exception(
                            "Failed to set compaction on %s " % compact_interval)
                    elif from_period is None and int(
                            cc["interval"]["fromHour"]) != 0 and \
                                    int(cc["interval"]["fromMinute"]) != 0:
                        raise Exception(
                            "fromHour and fromMinute should be zero")
                if compact_interval is None:
                    if (from_period != str(cc["interval"][
                                                    "fromHour"]) + ":" + str(
                                cc["interval"]["fromMinute"])) \
                    and (to_period != str(cc["interval"]["toHour"]) + ":" + str(
                                cc["interval"]["toMinute"])):
                        raise Exception(
                            "fromHour and fromMinute do not set correctly")
        return True

    def verifyGroupExists(self, server, name):
        rest = RestConnection(server)
        groups = rest.get_zone_names()
        print(groups)

        for gname, _ in groups.items():
            if name == gname:
                return True

        return False

    def _list_compare(self, list1, list2):
        if len(list1) != len(list2):
            return False
        for elem1 in list1:
            found = False
            for elem2 in list2:
                if elem1 == elem2:
                    found = True
                    break
            if not found:
                return False
        return True

    def waitForItemCount(self, server, bucket_name, count, timeout=30):
        rest = RestConnection(server)
        for sec in range(timeout):
            items = int(
                rest.get_bucket_json(bucket_name)["basicStats"]["itemCount"])
            if items != count:
                time.sleep(1)
            else:
                return True
        log.info("Waiting for item count to be %d timed out", count)
        return False
