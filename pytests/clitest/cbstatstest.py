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
        self.num_items = self.input.param("items", 1000)
        self.command_options = self.input.param("command_options", '')
        self.set_get_ratio = self.input.param("set_get_ratio", 0.9)
        self.item_size = self.input.param("item_size", 128)


    def tearDown(self):
        super(cbstatsTests, self).tearDown()


    def cbstats_test(self):
        """We use cbstas to check the various stats of server"""
        cluster_len = RestConnection(self.master).get_cluster_size()
        if self.command == "kvstore":
            self.verify_cluster_stats()
        if self.command != "key":
            if "tapagg" in self.command and cluster_len == 1:
                self.log.info("This command only works with cluster with 2 nodes or more")
                raise Exception("This command does not work with one node cluster")
            else:
                # tapagg needs replica items to print out results
                if "tapagg" in self.command:
                    for bucket in self.buckets:
                        self.shell.execute_cbworkloadgen(self.couchbase_usrname, \
                                        self.couchbase_password, self.num_items, \
                                        self.set_get_ratio, bucket.name, \
                                        self.item_size, self.command_options)
                        self.sleep(5)
                for bucket in self.buckets:
                    output, error = self.shell.execute_cbstats(bucket, self.command)
                    self.verify_results(output, error)
                    if self.command in ["allocator", "kvtimings", "timings"]:
                        self.log.warn("We will not verify exact values for this stat")
                    else:
                        self._verify_direct_client_stats(bucket, self.command, output)
        else:
            mc_conn = MemcachedClientHelper.direct_client(self.master, self.buckets[0].name, self.timeout)
            bucket_info = RestConnection(self.master).get_bucket(self.buckets[0])
            keys_map = {}
            for i in range(self.num_items):
                vb_id = i - len(bucket_info.vbuckets) * int(i / len(bucket_info.vbuckets))
                mc_conn.set("test_docs-%s" % i, 0, 0, json.dumps('{ "test" : "test"}').encode("ascii", "ignore"), vb_id)
                keys_map["test_docs-%s" % i] = vb_id
            for key, vb_id in keys_map.iteritems():
                output, error = self.shell.execute_cbstats(self.buckets[0], self.command, key, vb_id)
                self.verify_results(output, error)

    def kvtimings_test(self):
        """
        Test to validate MB-25630: Add read-only KVStore fsTimings to timing stats
        Automation for CBQE-4361 : Add testcases for MB-25630 (CBSE-4029)
        :return: Nothing
        """
        self.command = "kvtimings"
        self.set_get_ratio = 0.5
        # Load some data using cb_workloadgen to perform some KV operations (both sets and gets).
        for bucket in self.buckets:
            self.shell.execute_cbworkloadgen(self.couchbase_usrname, self.couchbase_password,
                                             self.num_items, self.set_get_ratio, bucket.name,
                                             self.item_size, self.command_options)
        # Verify that cbstats with kvtimings contains both read write and read only stats shown in the stats.
        for bucket in self.buckets:
            output, error  = self.shell.execute_cbstats(bucket, self.command)
            self.verify_results(output, error)
            read_only_stats = False
            write_stats = False
            for _output in output:
                if "ro_" in _output:
                    read_only_stats = True
                if "rw_" in _output:
                    write_stats = True
            self.assertTrue(read_only_stats, "Read only stats were not included in the cbstats kvtimings output. "
                                             "Check the output")
            self.assertTrue(write_stats, "Write stats were not included in the cbstats kvtimings output. Check the "
                                         "output")

    def verify_results(self, output, error):
        if len(error) > 0 and '\n'.join(error).find("DeprecationWarning") == -1:
            raise Exception("Command throw out error message. Please check the output of remote_util")
        else:
            if '\n'.join(output).lower().find("not found") != -1:
                raise Exception("Command throw out error message. Please check the output of remote_util")
            elif output.__len__() < 1:
                raise Exception("Command does not throw out error message but the output is empty. Please check the output of remote_util")

    def _verify_direct_client_stats(self, bucket, command, output):
        mc_conn = MemcachedClientHelper.direct_client(self.master, bucket.name, self.timeout)
        for line in output:
            stats = line.rsplit(":", 1)
            collect_stats = ""
            commands = ["hash", "tapagg"]
            if command in commands:
                output, error = self.shell.execute_cbstats(bucket, command)
                d = []
                if len(output) > 0:
                    d = dict(s.strip().rsplit(':', 1) for s in output)
                    collect_stats = d[stats[0].strip()].strip()
                else:
                    raise Exception("Command does not throw out error message but cbstats gives no output. \
                                      Please check the output manually")
            else:
                collect_stats = mc_conn.stats(command)[stats[0].strip()]
            self.log.info("CbStats###### for %s:::%s==%s" % (stats[0].strip(), collect_stats, stats[1].strip()))
            if stats[1].strip() == collect_stats:
                continue
            else:
                if stats[0].find('tcmalloc') != -1 or stats[0].find('bytes') != -1 or\
                stats[0].find('mem_used') != -1:
                    self.log.warn("Stat didn't match, but it can be changed, not a bug")
                    continue
                raise Exception("Command does not throw out error message but cbstats does not match.")
