from clitest.cli_base import CliBaseTest
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket
import time
import logger

class healthcheckerTests(CliBaseTest):

    def setUp(self):
        super(healthcheckerTests, self).setUp()
        self.report_folder_name = self.input.param("foler_name", "reports")
        self.doc_ops = self.input.param("doc_ops", None)
        self.expire_time = self.input.param("expire_time", 5)
        self.value_size = self.input.param("value_size", 256)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

    def tearDown(self):
        super(healthcheckerTests, self).tearDown()

    def healthchecker_test(self):

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

        self.shell.delete_files(self.report_folder_name)

        output, error = self.shell.execute_cbhealthchecker(self.couchbase_usrname, self.couchbase_password, self.command_options)

        if self.os != "windows":
            if len(error) > 0:
                raise Exception("Command throw out error message. Please check the output of remote_util")
        for output_line in output:
            if output_line.find("ERROR") >= 0 or output_line.find("Error") >= 0:
                raise Exception("Command throw out error message. Please check the output of remote_util")
            if output_line.find('Exception launched') >= 0:
                raise Exception("There are python code exceptions when execute the cbhealthchecker")
        self.verify_results(output)


    def verify_results(self, command_output):
        for bucket in self.buckets:
            find_bucket = False
            for line in command_output:
                if line.find(bucket.name) != -1:
                    find_bucket = True

            if not find_bucket:
                raise Exception("cbhealthchecker does not generate report for %s" % bucket.name)

        if self.os == "linux" or self.os == "mac":
            command = "du -s %s/*" % self.report_folder_name
            output, error = self.shell.execute_command(command.format(command))
            self.shell.log_command_output(output, error)
            empty_reports = False
            if len(error) > 0:
                raise Exception("uable to list file size. Check du command output for help")
            for output_line in output:
                output_line = output_line.split()
                file_size = int(output_line[0])
                if file_size == 0:
                    empty_reports = True
                    self.log.error("%s is empty" % (output_line[1]))

            if empty_reports:
                raise Exception("Collect empty cbhealthchecker reports")
        elif os == "windows":
            # try to figure out what command works for windows for verification
            pass

        self.shell.delete_files(self.report_folder_name)