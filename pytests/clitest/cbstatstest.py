from clitest.cli_base import CliBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper


class cbstatsTests(CliBaseTest):
    def setUp(self):
        super(cbstatsTests, self).setUp()
        self.command = self.input.param("command", "")
        self.vbucketId = self.input.param("vbid", -1)
        self.timeout = 6000


    def tearDown(self):
        super(cbstatsTests, self).tearDown()


    def cbstats_test(self):
        """We use cbstas to check the various stats of server"""
        if self.command != "key":
            for bucket in self.buckets:
                output, error = self.shell.execute_cbstats(bucket, self.command)
                self.verify_results(output, error)
                self._verify_direct_client_stats(bucket, self.command, output)
        else:
            self._load_doc_data_all_buckets('create')
            keys = ["test_docs-%s" % (i) for i in range(0, self.num_items)]
            for key in keys:
                self._set_vbucket(key, self.vbucketId)
                output, error = self.shell.execute_cbstats(self.buckets[0], self.command, key, self.vbucketId)
                self.verify_results(output, error)



    def verify_results(self, output, error):
        if len(error) > 0 :
            raise Exception("Command throw out error message. Please check the output of remote_util")
        else:
            if output[1].find("Error") != -1:
                raise Exception("Command throw out error message. Please check the output of remote_util")
            elif output.__len__() < 1:
                raise Exception("Command does not throw out error message but the output is empty. Please check the output of remote_util")

    def _verify_direct_client_stats(self, bucket, command, output):
        mc_conn = MemcachedClientHelper.direct_client(self.master, bucket.name, self.timeout)
        for line in output:
            stats = line.split(":")
            self.log.info("CbStats###### for %s:::%s==%s" % (stats[0], mc_conn.stats(command)[stats[0].strip()], stats[1].strip()))
            if stats[1].strip() == mc_conn.stats(command)[stats[0].strip()]:
                continue
            else:
                raise Exception("Command does not throw out error message but cbstats does not match.")
