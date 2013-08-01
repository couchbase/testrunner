from clitest.cli_base import CliBaseTest
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket
import testconstants
import time
import logger

LOG_FILE_NAME_LIST = ["couchbase.log", "diag.log", "ddocs.log", "ini.log", "syslog.tar.gz",
                      "ns_server.couchdb.log", "ns_server.debug.log", "ns_server.babysitter.log",
                      "ns_server.error.log", "ns_server.info.log",
                      "ns_server.views.log", "stats.log",
                      "memcached.log", "ns_server.mapreduce_errors.log",
                      "ns_server.stats.log", "ns_server.xdcr_errors.log",
                      "ns_server.xdcr.log"]

class collectinfoTests(CliBaseTest):

    def setUp(self):
        super(collectinfoTests, self).setUp()
        self.log_filename = self.input.param("filename", "info")
        self.doc_ops = self.input.param("doc_ops", None)
        self.expire_time = self.input.param("expire_time", 5)
        self.value_size = self.input.param("value_size", 256)
        self.node_down = self.input.param("node_down", False)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

    def tearDown(self):
        super(collectinfoTests, self).tearDown()

    def collectinfo_test(self):
        """We use cbcollect_info to automatically collect the logs for server node

        First we load some items to the node. Optionally you can do some mutation
        against these items. Then we use cbcollect_info the automatically generate
        the zip file containing all the logs about the node. We want to verify we have
        all the log files according to the LOG_FILE_NAME_LIST and in stats.log, we have
        stats for all the buckets we have created"""

        gen_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        gen_update = BlobGenerator('nosql', 'nosql-', self.value_size, end=(self.num_items/2-1))
        gen_expire = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items/2, end=(self.num_items*3/4-1))
        gen_delete = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items*3/4, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_update, "update", 0)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_delete, "delete", 0)
            if("expire" in self.doc_ops):
                self._load_all_buckets(self.master, gen_expire, "update", self.expire_time)
                time.sleep(self.expire_time + 1)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self.shell.delete_files("%s.zip" % (self.log_filename))
        self.shell.delete_files("cbcollect_info*") #This is the folder generated after unzip the log package

        if self.node_down:
            if self.os == 'linux':
                output, error = self.shell.execute_command("killall -9 memcached & killall -9 beam.smp")
                self.shell.log_command_output(output, error)

        output, error = self.shell.execute_cbcollect_info("%s.zip" % (self.log_filename))

        if self.os != "windows":
            if len(error) > 0:
                raise Exception("Command throw out error message. Please check the output of remote_util")
            for output_line in output:
                if output_line.find("ERROR") >= 0 or output_line.find("Error") >= 0:
                    raise Exception("Command throw out error message. Please check the output of remote_util")
        self.verify_results(self.log_filename)

        if self.node_down:
            if self.os == 'linux':
                output, error = self.shell.execute_command("/etc/init.d/couchbase-server restart")
                self.shell.log_command_output(output, error)
                time.sleep(self.wait_timeout)


    def verify_results(self, file):
        os = "linux"
        zip_file = "%s.zip" % (file)
        info = self.shell.extract_remote_info()
        type = info.type.lower()
        if type == 'windows':
            os = "windows"

        if os == "linux":
            command = "unzip %s" % (zip_file)
            output, error = self.shell.execute_command(command.format(command))
            self.shell.log_command_output(output, error)
            if len(error) > 0:
                raise Exception("uable to unzip the files. Check unzip command output for help")

            command = "ls cbcollect_info*/"
            output, error = self.shell.execute_command(command.format(command))
            self.shell.log_command_output(output, error)
            if len(error) > 0:
                raise Exception("uable to list the files. Check ls command output for help")
            missing_logs = False
            for x in LOG_FILE_NAME_LIST:
                find_log = False
                for output_line in output:
                    if output_line.find(x) >= 0:
                        find_log = True
                if not find_log:
                   missing_logs = True
                   self.log.error("The log zip file miss %s" % (x))

            missing_buckets = False
            if not self.node_down:
                for bucket in self.buckets:
                    command = "grep %s cbcollect_info*/stats.log" % (bucket.name)
                    output, error = self.shell.execute_command(command.format(command))
                    self.shell.log_command_output(output, error)
                    if len(error) > 0:
                        raise Exception("uable to grep key words. Check grep command output for help")
                    if len(output) == 0:
                        missing_buckets = True
                        self.log.error("%s stats are missed in stats.log" % (bucket.name))

            command = "du -s cbcollect_info*/*"
            output, error = self.shell.execute_command(command.format(command))
            self.shell.log_command_output(output, error)
            empty_logs = False
            if len(error) > 0:
                raise Exception("uable to list file size. Check du command output for help")
            for output_line in output:
                output_line = output_line.split()
                file_size = int(output_line[0])
                if file_size == 0:
                    empty_logs = True
                    self.log.error("%s is empty" % (output_line[1]))

            if missing_logs:
                raise Exception("Bad log file package generated. Missing logs")
            if missing_buckets:
                raise Exception("Bad stats.log which miss some bucket information")
            if empty_logs:
                raise Exception("Collect empty log files")
        elif os == "windows":
            # try to figure out what command works for windows for verification
            pass

        self.shell.delete_files(zip_file)
        self.shell.delete_files("cbcollect_info*")