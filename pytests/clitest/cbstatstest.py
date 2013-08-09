import json
from clitest.cli_base import CliBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper
from membase.api.rest_client import RestConnection


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
        if self.command == "kvstore":
            self.verify_cluster_stats()
        if self.command != "key":
            for bucket in self.buckets:
                output, error = self.shell.execute_cbstats(bucket, self.command)
                self.verify_results(output, error)
                if self.command == "allocator":
                    self.log.warn("We will not verify exact values for this stat")
                else:
                    self._verify_direct_client_stats(bucket, self.command, output)
        else:
            mc_conn = MemcachedClientHelper.direct_client(self.master, self.buckets[0].name, self.timeout)
            bucket_info = RestConnection(self.master).get_bucket(self.buckets[0])
            keys_map = {}
            for i in range(self.num_items):
                vb_id = i - len(bucket_info.vbuckets) * int(i/len(bucket_info.vbuckets))
                mc_conn.set("test_docs-%s" % i, 0, 0, json.dumps('{ "test" : "test"}').encode("ascii", "ignore"), vb_id)
                keys_map["test_docs-%s" % i] = vb_id
            for key, vb_id in keys_map.iteritems():
                output, error = self.shell.execute_cbstats(self.buckets[0], self.command, key, vb_id)
                self.verify_results(output, error)



    def verify_results(self, output, error):
<<<<<<< HEAD
        if len(error) > 0 and '\n'.join(error).find("DeprecationWarning") == -1:
            raise Exception("Command throw out error message. Please check the output of remote_util")
        else:
            if '\n'.join(output).lower().find("not found") != -1:
=======
        if len(error) > 0  and error.find("DeprecationWarning") == -1:
            raise Exception("Command throw out error message. Please check the output of remote_util")
        else:
            if '\n'.join(output).lower().find("error") != -1:
>>>>>>> 4b40e9a... CBQE-1507: fix cbstats key tests
                raise Exception("Command throw out error message. Please check the output of remote_util")
            elif output.__len__() < 1:
                raise Exception("Command does not throw out error message but the output is empty. Please check the output of remote_util")

    def _verify_direct_client_stats(self, bucket, command, output):
        mc_conn = MemcachedClientHelper.direct_client(self.master, bucket.name, self.timeout)
        for line in output:
            stats = line.rsplit(":", 1)
            self.log.info("CbStats###### for %s:::%s==%s" % (stats[0], mc_conn.stats(command)[stats[0].strip()], stats[1].strip()))
            if stats[1].strip() == mc_conn.stats(command)[stats[0].strip()]:
                continue
            else:
                if stats[0].find('tcmalloc') != -1 or stats[0].find('bytes') != -1 or\
                stats[0].find('mem_used') != -1:
                    self.log.warn("Stat didn't match, but it can be changed, not a bug")
                    continue
                raise Exception("Command does not throw out error message but cbstats does not match.")
