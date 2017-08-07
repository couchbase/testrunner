import subprocess, time, os
from subprocess import call
from clitest.cli_base import CliBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper

from couchbase_helper.document import View

LOG_FILE_NAME_LIST = ["couchbase.log", "diag.log", "ddocs.log", "ini.log", "syslog.tar.gz",
                      "ns_server.couchdb.log", "ns_server.debug.log", "ns_server.babysitter.log",
                      "ns_server.error.log", "ns_server.info.log",
                      "ns_server.views.log", "stats.log",
                      "memcached.log", "ns_server.mapreduce_errors.log",
                      "ns_server.stats.log", "ns_server.xdcr_errors.log",
                      "ns_server.xdcr.log"]


class CollectinfoTests(CliBaseTest):

    def setUp(self):
        super(CollectinfoTests, self).setUp()
        self.log_filename = self.input.param("filename", "info")
        self.doc_ops = self.input.param("doc_ops", None)
        self.expire_time = self.input.param("expire_time", 5)
        self.value_size = self.input.param("value_size", 256)
        self.node_down = self.input.param("node_down", False)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

    def tearDown(self):
        super(CollectinfoTests, self).tearDown()

    def collectinfo_test(self):
        """We use cbcollect_info to automatically collect the logs for server node

        First we load some items to the node. Optionally you can do some mutation
        against these items. Then we use cbcollect_info the automatically generate
        the zip file containing all the logs about the node. We want to verify we have
        all the log files according to the LOG_FILE_NAME_LIST and in stats.log, we have
        stats for all the buckets we have created"""

        gen_load = BlobGenerator('nosql', 'nosql-', self.value_size,
                                                  end=self.num_items)
        gen_update = BlobGenerator('nosql', 'nosql-', self.value_size,
                                          end=(self.num_items / 2 - 1))
        gen_expire = BlobGenerator('nosql', 'nosql-', self.value_size,
                                             start=self.num_items / 2,
                                             end=(self.num_items * 3 / 4 - 1))
        gen_delete = BlobGenerator('nosql', 'nosql-', self.value_size,
                                                 start=self.num_items * 3 / 4,
                                                          end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_update, "update", 0)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_delete, "delete", 0)
            if("expire" in self.doc_ops):
                self._load_all_buckets(self.master, gen_expire, "update",\
                                                               self.expire_time)
                self.sleep(self.expire_time + 1)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self.shell.delete_files("%s.zip" % (self.log_filename))
        """ This is the folder generated after unzip the log package """
        self.shell.delete_files("cbcollect_info*")

        if self.node_down:
            if self.os == 'linux':
                output, error = self.shell.execute_command(
                                    "killall -9 memcached & killall -9 beam.smp")
                self.shell.log_command_output(output, error)
        output, error = self.shell.execute_cbcollect_info("%s.zip"
                                                           % (self.log_filename))

        if self.os != "windows":
            if len(error) > 0:
                """ Restart cb server if test node down """
                if self.node_down:
                    self.shell.start_server()
                raise Exception("Command throw out error: %s " % error)

            for output_line in output:
                if output_line.find("ERROR") >= 0 or output_line.find("Error") >= 0:
                    if "from http endpoint" in output_line.lower():
                        continue
                    if self.node_down:
                        self.shell.start_server()
                    raise Exception("Command throw out error: %s " % output_line)
        try:
            self.verify_results(self, self.log_filename)
        finally:
            if self.node_down:
                if self.os == 'linux':
                    self.shell.start_server()
                    rest = RestConnection(self.master)
                    RestHelper(rest).is_ns_server_running(timeout_in_seconds=60)

    def test_cbcollectinfo_detect_container(self):
        """ this test only runs inside docker host and
            detect if a node is a docker container.
            It should run with param skip_init_check_cbserver=true """
        docker_id = None
        if "." in self.ip:
            self.fail("This test only run in docker host")
        elif self.ip is not None:
            docker_id = self.ip
            os.system("docker exec %s %scbcollect_info testlog.zip"
                                        % (docker_id, self.cli_command_path))
            os.system("docker cp %s:/testlog.zip ." % (docker_id))
            os.system("unzip testlog.zip")
            output = call("cd cbcollect_info_*; grep 'docker' ./* ")
            if output and "docker" in output:
                self.log.info("cbcollect log detected docker container")
            else:
                self.fail("cbcollect info could not detect docker container")
        os.system("docker exec %s rm testlog.zip" % (docker_id))

    @staticmethod
    def verify_results(self, output_file_name):
        try:
            os = "linux"
            zip_file = "%s.zip" % (output_file_name)
            info = self.shell.extract_remote_info()
            type = info.type.lower()
            if type == 'windows':
                os = "windows"

            if os == "linux":
                command = "unzip %s" % (zip_file)
                output, error = self.shell.execute_command(command)
                if self.debug_logs:
                    self.shell.log_command_output(output, error)
                if len(error) > 0:
                    raise Exception("unable to unzip the files. Check unzip command output for help")

                command = "ls cbcollect_info*/"
                output, error = self.shell.execute_command(command)
                if self.debug_logs:
                    self.shell.log_command_output(output, error)
                if len(error) > 0:
                    raise Exception("unable to list the files. Check ls command output for help")
                missing_logs = False
                for x in LOG_FILE_NAME_LIST:
                    find_log = False
                    for output_line in output:
                        if output_line.find(x) >= 0:
                            find_log = True
                    if not find_log:
                        # missing syslog.tar.gz in mac as in ticket MB-9110
                        # need to remove 3 lines below if it is fixed in 2.2.1
                        # in mac os
                        if x == "syslog.tar.gz" and info.distribution_type.lower() == "mac":
                            missing_logs = False
                        else:
                            missing_logs = True
                            self.log.error("The log zip file miss %s" % (x))

                missing_buckets = False
                if not self.node_down:
                    for bucket in self.buckets:
                        command = "grep %s cbcollect_info*/stats.log" % (bucket.name)
                        output, error = self.shell.execute_command(command)
                        if self.debug_logs:
                            self.shell.log_command_output(output, error)
                        if len(error) > 0:
                            raise Exception("unable to grep key words. Check grep command output for help")
                        if len(output) == 0:
                            missing_buckets = True
                            self.log.error("%s stats are missed in stats.log" % (bucket.name))

                command = "du -s cbcollect_info*/*"
                output, error = self.shell.execute_command(command)
                if self.debug_logs:
                    self.shell.log_command_output(output, error)
                empty_logs = False
                if len(error) > 0:
                    raise Exception("unable to list file size. Check du command output for help")
                for output_line in output:
                    output_line = output_line.split()
                    file_size = int(output_line[0])
                    if self.debug_logs:
                        print "File size: ", file_size
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

        finally:
            self.shell.delete_files(zip_file)
            self.shell.delete_files("cbcollect_info*")

    def collectinfo_test_for_views(self):
        self.default_design_doc_name = "Doc1"
        self.view_name = self.input.param("view_name", "View")
        self.generate_map_reduce_error = self.input.param("map_reduce_error", False)
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.gen_load = BlobGenerator('couch', 'cb-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        self.reduce_fn = "_count"
        expected_num_items = self.num_items
        if self.generate_map_reduce_error:
            self.reduce_fn = "_sum"
            expected_num_items = None

        view = View(self.view_name, self.default_map_func, self.reduce_fn, dev_view=False)
        self.cluster.create_view(self.master, self.default_design_doc_name, view,
                                 'default', self.wait_timeout * 2)
        query = {"stale": "false", "connection_timeout": 60000}
        try:
            self.cluster.query_view(self.master, self.default_design_doc_name, self.view_name, query,
                                expected_num_items, 'default', timeout=self.wait_timeout)
        except Exception, ex:
            if not self.generate_map_reduce_error:
                raise ex
        self.shell.execute_cbcollect_info("%s.zip" % (self.log_filename))
        self.verify_results(self, self.log_filename)
