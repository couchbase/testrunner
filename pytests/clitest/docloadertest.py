from clitest.cli_base import CliBaseTest
from membase.api.rest_client import RestConnection
import testconstants
import json
from testconstants import COUCHBASE_FROM_WATSON
from membase.helper.bucket_helper import BucketOperationHelper

class docloaderTests(CliBaseTest):

    def setUp(self):
        super(docloaderTests, self).setUp()
        self.load_filename = self.input.param("filename", "gamesim-sample")
        self.memory_quota = self.input.param("memory_quota", 100)
        info = self.shell.extract_remote_info()
        type = info.type.lower()
        self.os = 'linux'
        if type == 'windows':
            self.os = "windows"
        if info.distribution_type.lower() == 'mac':
            self.os = 'mac'

    def tearDown(self):
        super(docloaderTests, self).tearDown()

    def docloader_test(self):
        """We use cbdocloader to load sample files into cluster

        First use cbdocloader to load the sample files under /opt/couchbase/samples
        into the cluster. Then we try to verify all the docs are loaded into the
        cluster. We verify by compare the number of items in cluster with number of
        doc files in zipped sample file package"""

        if self.short_v not in COUCHBASE_FROM_WATSON:
            for bucket in self.buckets:
                output, error = self.shell.execute_cbdocloader(self.couchbase_usrname,
                                                              self.couchbase_password,
                                                                          bucket.name,
                                                                    self.memory_quota,
                                                                   self.load_filename)
        elif self.short_v in COUCHBASE_FROM_WATSON:
            self.log.info("cluster version: %s " % self.short_v)
            self.log.info("delete all buckets to create new bucket")
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
            output, error = self.shell.execute_cbdocloader(self.couchbase_usrname,
                                                              self.couchbase_password,
                                                                   self.load_filename,
                                                                    self.memory_quota,
                                                                   self.load_filename)

        self.buckets = RestConnection(self.master).get_buckets()
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self.shell.delete_files(self.load_filename)

        if self.os != "windows":
            command = "unzip %ssamples/%s.zip" % (testconstants.LINUX_CB_PATH,
                                                           self.load_filename)
            if self.os == 'mac':
                command = "unzip %ssamples/%s.zip" % (testconstants.MAC_CB_PATH,
                                                             self.load_filename)
            output, error = self.shell.execute_command(command)
            self.shell.log_command_output(output, error)

        self.verify_results(self.load_filename)
        self.verify_ddoc(self.load_filename)
        self.shell.delete_files(self.load_filename)

    def verify_results(self, file):
        stats_tasks = []
        for bucket in self.buckets:
            items = self.get_number_of_files(file)
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'curr_items', '==', items))
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'vb_active_curr_items', '==', items))

            available_replicas = self.num_replicas
            if len(self.servers) == self.num_replicas:
                available_replicas = len(self.servers) - 1
            elif len(self.servers) <= self.num_replicas:
                available_replicas = len(self.servers) - 1

            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'vb_replica_curr_items', '==', items * available_replicas))
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'curr_items_tot', '==', items * (available_replicas + 1)))

        for task in stats_tasks:
            task.result(60)

    def get_number_of_files(self, file):
        if self.os != "windows":
            command = "find %s/ -name *.json | wc -l" % (file)
            output, error = self.shell.execute_command(command)
            self.shell.log_command_output(output, error)
            if 'unable to resolve host' in output[0]:
                #handle situation when DNS server does not have any entry for host, when
                #we get output with 2 lines: error message and the number of received items
                a = int(output[1])
            else:
                a = int(output[0])

            command = "find %s/design_docs/ -name *.json | wc -l" % (file)
            output, error = self.shell.execute_command(command)
            self.shell.log_command_output(output, error)
            if 'unable to resolve host' in output[0]:
                b = int(output[1])
            else:
                b = int(output[0])
            number_of_items = a - b  #design doc create views not items in cluster

            return number_of_items

        elif self.os == "windows":
            if file == "gamesim-sample":
                return 586
            elif file == "beer-sample":
                return 7303
            else:
                raise Exception("Sample file %s.zip doesn't exists" % (file))

    def verify_ddoc(self, file):
        rest = RestConnection(self.master)
        ddoc_names = self.get_ddoc_names(file)
        for bucket in self.buckets:
            for ddoc_name in ddoc_names:
                ddoc_json, header = rest.get_ddoc(bucket, ddoc_name)
                if ddoc_json is not None:
                    self.log.info('Database Document {0} details : {1}'.format(ddoc_name, json.dumps(ddoc_json)))
                else:
                    raise Exception("ddoc %s is not imported" % ddoc_name)

    def get_ddoc_names(self, file):
        if self.os == 'windows':
            return []

        ddoc_names = []
        command = "find %s/design_docs/ -name *.json | cut -d \"/\" -f3" % (file)
        if self.os == 'mac':
            command = "find %s/design_docs/ -name *.json | cut -d \"/\" -f4" % (file)
        output, error = self.shell.execute_command(command)
        self.shell.log_command_output(output, error)

        for line in output:
            if self.short_v in COUCHBASE_FROM_WATSON:
                if "indexes" in line.split(".")[0]:
                    continue
            ddoc_names.append(line.split(".")[0])
        return ddoc_names
