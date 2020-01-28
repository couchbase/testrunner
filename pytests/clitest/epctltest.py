from clitest.cli_base import CliBaseTest
from membase.api.rest_client import RestConnection
from testconstants import COUCHBASE_FROM_WATSON


class epctlTests(CliBaseTest):

    def setUp(self):
        super(epctlTests, self).setUp()
        #You have 3 choices: start, stop, drain
        self.persistence = self.input.param("persistence_choice", "")
        #You have 3 choices: checkpoint_param, flush_param, tap_param
        self.param_type = self.input.param("param_type", "set flush_param")
        self.param = self.input.param("param", "max_size")
        self.param_value = self.input.param("param_value", 1000000)
        self.server = self.master
        self.rest = RestConnection(self.server)

    def tearDown(self):
        super(epctlTests, self).tearDown()

    def epctl_test(self):
        """We use cbepctl to do persistence or set param operatins and verify
        verify the result by checking the command output"""
        if self.cb_version[:5] == "6.5.0":
            self.log.info("\n\n******* Due to issue in MB-36904, \
                           \nthis test will be skipped in 6.5.0 ********\n")
            return
        for bucket in self.buckets:
            if self.cb_version[:5] in COUCHBASE_FROM_WATSON:
                if self.param == "item_num_based_new_chk":
                    self.param_value = "true"
                """ from Watson, there is not tap_throttle_threshold param """
                if self.param == "tap_throttle_threshold":
                    self.param = "replication_throttle_threshold"
            if self.persistence == "start":
                output, error = self.shell.execute_cbepctl(bucket, "stop",
                                                          self.param_type,
                                                               self.param,
                                                         self.param_value)
            output, error = self.shell.execute_cbepctl(bucket, self.persistence,
                                                                self.param_type,
                                                                     self.param,
                                                               self.param_value)
            self.verify_results(output, error)

    def verify_results(self, output, error):
        if len(error) > 0 :
            raise Exception("Command throw out error message. "
                            "Please check the output of remote_util")
        if self.persistence != "":
            if output[0].find("Error") != -1:
                raise Exception("Command throw out error message. "
                                "Please check the output of remote_util")
            if self.persistence == "start":
                if output[0].find("Persistence started") == -1:
                    raise Exception("Persistence start failed")
            elif self.persistence == "stop":
                if output[0].find("Persistence stopped") == -1:
                    raise Exception("Persistence stop failed")
            elif self.persistence == "drain":
                if output[0].find("done") == -1:
                    raise Exception("wait until queues are drained operation failed")
        else:
            if output[1].find("Error") != -1:
                raise Exception("Command throw out error message. "
                                "Please check the output of remote_util")
            if output[1].find(self.param) == -1 or \
               output[1].find(str(self.param_value)) == -1:
                raise Exception("set %s failed" % self.param)

