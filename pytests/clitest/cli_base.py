from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from testconstants import LINUX_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH, WIN_ROOT_PATH
from testconstants import MAC_COUCHBASE_BIN_PATH
from testconstants import LINUX_COUCHBASE_SAMPLE_PATH, WIN_COUCHBASE_SAMPLE_PATH,\
                          WIN_BACKUP_C_PATH, LINUX_BACKUP_PATH
import logger
import random
import time
import zlib

log = logger.Logger.get_logger()

class CliBaseTest(BaseTestCase):
    vbucketId = 0
    def setUp(self):
        self.times_teardown_called = 1
        super(CliBaseTest, self).setUp()
        self.r = random.Random()
        self.vbucket_count = 1024
        self.shell = RemoteMachineShellConnection(self.master)
        self.rest = RestConnection(self.master)
        self.node_version = self.rest.get_nodes_version()
        self.force_failover = self.input.param("force_failover", False)
        info = self.shell.extract_remote_info()
        type = info.type.lower()
        self.excluded_commands = self.input.param("excluded_commands", None)
        self.os = 'linux'
        self.full_v = None
        self.short_v = None
        self.build_number = None
        self.cli_command_path = LINUX_COUCHBASE_BIN_PATH
        self.root_path = LINUX_ROOT_PATH
        self.tmp_path = "/tmp/"
        self.cmd_backup_path = LINUX_BACKUP_PATH
        self.backup_path = LINUX_BACKUP_PATH
        self.cmd_ext = ""
        self.sample_files_path = LINUX_COUCHBASE_SAMPLE_PATH
        if type == 'windows':
            self.os = 'windows'
            self.cmd_ext = ".exe"
            self.root_path = WIN_ROOT_PATH
            self.tmp_path = WIN_TMP_PATH
            self.cmd_backup_path = WIN_BACKUP_C_PATH
            self.backup_path = WIN_BACKUP_PATH
            self.cli_command_path = WIN_COUCHBASE_BIN_PATH
            self.sample_files_path = WIN_COUCHBASE_SAMPLE_PATH
        if info.distribution_type.lower() == 'mac':
            self.os = 'mac'
            self.cli_command_path = MAC_COUCHBASE_BIN_PATH
        self.full_v, self.short_v, self.build_number = self.shell.get_cbversion(type)
        self.couchbase_usrname = "%s" % (self.input.membase_settings.rest_username)
        self.couchbase_password = "%s" % (self.input.membase_settings.rest_password)
        self.cb_login_info = "%s:%s" % (self.couchbase_usrname,
                                        self.couchbase_password)
        self.cli_command = self.input.param("cli_command", None)
        self.command_options = self.input.param("command_options", None)
        if self.command_options is not None:
            self.command_options = self.command_options.split(";")
        if str(self.__class__).find('couchbase_clitest.CouchbaseCliTest') == -1:
            servers_in = [self.servers[i + 1] for i in range(self.num_servers - 1)]
            self.cluster.rebalance(self.servers[:1], servers_in, [])

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

            Returns a boolean corresponding to whether or not the expected services are available on the server.
        """
        rest = RestConnection(server)
        hostname = "%s:%s" % (server.ip, server.port)
        expected_services = expected_services.replace("data", "kv")
        expected_services = expected_services.split(",")

        nodes_services = rest.get_nodes_services()
        for node, services in nodes_services.iteritems():
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
                log.info("Index service memory quota does not match (%d vs %d)",
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

    def verifyBucketSettings(self, server, bucket_name, bucket_password, bucket_type, memory_quota, eviction_policy,
                             replica_count, enable_index_replica, priority, enable_flush):
        rest = RestConnection(server)
        result = rest.get_bucket_json(bucket_name)
        if bucket_password is not None and bucket_password != result["saslPassword"]:
            log.info("Bucket password does not match (%s vs %s)", bucket_password, result["saslPassword"])
            return False

        if bucket_type == "couchbase":
            bucket_type = "membase"

        if bucket_type is not None and bucket_type != result["bucketType"]:
            log.info("Memory quota does not match (%s vs %s)", bucket_type, result["bucketType"])
            return False

        quota = result["quota"]["rawRAM"] / 1024 / 1024
        if memory_quota is not None and memory_quota != quota:
            log.info("Bucket quota does not match (%s vs %s)", memory_quota, quota)
            return False

        if eviction_policy is not None and eviction_policy != result["evictionPolicy"]:
            log.info("Eviction policy does not match (%s vs %s)", eviction_policy, result["evictionPolicy"])
            return False

        if replica_count is not None and replica_count != result["replicaNumber"]:
            log.info("Replica count does not match (%s vs %s)", replica_count, result["replicaNumber"])
            return False

        if enable_index_replica == 1:
            enable_index_replica = True
        elif enable_index_replica == 0:
            enable_index_replica = False

        if enable_index_replica is not None and enable_index_replica != result["replicaIndex"]:
            log.info("Replica index enabled does not match (%s vs %s)", enable_index_replica, result["replicaIndex"])
            return False

        if priority == "high":
            priority = 8
        elif priority == "low":
            priority = 3

        if priority is not None and priority != result["threadsNumber"]:
            log.info("Bucket priority does not match (%s vs %s)", priority, result["threadsNumber"])
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

        if "clusterName" not in settings:
            log.info("Unable to get cluster name from server")
            return False
        if settings["clusterName"] != name:
            log.info("Cluster name does not match (%s vs %s)", settings["clusterName"], name)
            return False

        return True

    def isClusterInitialized(self, server):
        """Checks whether or not the server is initialized

        Options:
        server - A TestInputServer object of the server to connect to

        Checks to see whether or not the default pool was created in order to determine whether
        or no the server was initialized. Returns a boolean value to indicate initialization.
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

    def verifyIndexSettings(self, server, max_rollbacks, stable_snap_interval, mem_snap_interval,
                                                     storage_mode, threads, log_level):
        rest = RestConnection(server)
        settings = rest.get_global_index_settings()

        if storage_mode == "default":
            storage_mode = "forestdb"
        elif storage_mode == "memopt":
            storage_mode = "memory_optimized"

        if max_rollbacks and str(settings["maxRollbackPoints"]) != str(max_rollbacks):
            log.info("Max rollbacks does not match (%s vs. %s)", str(settings["maxRollbackPoints"]), str(max_rollbacks))
            return False
        if stable_snap_interval and str(settings["stableSnapshotInterval"]) != str(stable_snap_interval):
            log.info("Stable snapshot interval does not match (%s vs. %s)", str(settings["stableSnapshotInterval"]),
                     str(stable_snap_interval))
            return False
        if mem_snap_interval and str(settings["memorySnapshotInterval"]) != str(mem_snap_interval):
            log.info("Memory snapshot interval does not match (%s vs. %s)", str(settings["memorySnapshotInterval"]),
                     str(mem_snap_interval))
            return False
        if storage_mode and str(settings["storageMode"]) != str(storage_mode):
            log.info("Storage mode does not match (%s vs. %s)", str(settings["storageMode"]), str(storage_mode))
            return False
        if threads and str(settings["indexerThreads"]) != str(threads):
            log.info("Threads does not match (%s vs. %s)", str(settings["indexerThreads"]), str(threads))
            return False
        if log_level and str(settings["logLevel"]) != str(log_level):
            log.info("Log level does not match (%s vs. %s)", str(settings["logLevel"]), str(log_level))
            return False

        return True

    def verifyAutofailoverSettings(self, server, enabled, timeout):
        rest = RestConnection(server)
        settings = rest.get_autofailover_settings()

        if enabled and not ((str(enabled) == "1" and settings.enabled) or (str(enabled) == "0" and not settings.enabled)):
            log.info("Enabled does not match (%s vs. %s)", str(enabled), str(settings.enabled))
            return False
        if timeout and str(settings.timeout) != str(timeout):
            log.info("Timeout does not match (%s vs. %s)", str(timeout), str(settings.timeout))
            return False

        return True

    def waitForItemCount(self, server, bucket_name, count, timeout=30):
        rest = RestConnection(server)
        for sec in range(timeout):
            items = int(rest.get_bucket_json(bucket_name)["basicStats"]["itemCount"])
            if items != count:
                time.sleep(1)
            else:
                return True
        log.info("Waiting for item count to be %d timed out", count)
        return False
