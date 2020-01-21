import json
from clitest.cli_base import CliBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


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
                vb_id = i - len(bucket_info.vbuckets) * int(i // len(bucket_info.vbuckets))
                mc_conn.set("test_docs-%s" % i, 0, 0, json.dumps('{ "test" : "test"}').encode("ascii", "ignore"), vb_id)
                keys_map["test_docs-%s" % i] = vb_id
            for key, vb_id in keys_map.items():
                output, error = self.shell.execute_cbstats(self.buckets[0], self.command, key, vb_id)
                self.verify_results(output, error)

    def test_software_version(self):
        """
          This test requires to pass 3 params to run:
            software_name
            software_version
            check_in_file
        """
        self.software_name = self.input.param("software_name", None)
        self.software_version = self.input.param("software_version", None)
        self.check_in_file = self.input.param("check_in_file", "manifest.xml")
        if self.software_name is None or self.software_version is None:
            self.fail("This test needs to pass param 'software_name'\
                                                 and software_version to run")
        go_software = ["gocb", "gocbcore"]
        go_sw_in_version = ["5.1.2", "5.5.1"]
        if self.software_name in go_software and \
            (self.cb_version[:5] in go_sw_in_version or 6.0 <= float(self.cb_version[:3])):
            shell = RemoteMachineShellConnection(self.master)
            output, error = shell.execute_command('cat {0}/{1} | grep \'"{2}"\' '
                                                  .format(self.base_cb_path,
                                                          self.check_in_file,
                                                          self.software_name))
            shell.disconnect()
            found_version = False
            if output:
                for ele in output:
                    if "gocb" in ele and self.software_version in ele:
                        found_version = True
                        self.log.info("software info: {0}".format(ele))
                        break
            if not found_version:
                self.fail("version of {0} does not match as in: {0}"\
                                  .format(self.software_name, output))
        else:
            self.log.info("software name/version are not in running cb version")

    def test_uninstall_install_server(self):
        """
           This test will test uninstall and install again couchbase server.
           There was an issue that couchbase server does not start in debian
           if we uninstall with only -r flag, not follow by --purge flag
        """
        self.package_type = self.input.param("package_type", "deb")
        if len(self.servers) < 2:
            self.log.info("This test needs 2 or more server to run.")
            return
        debian_systemd = ["ubuntu 16.04", "ubuntu 18.04"]
        if self.os_version not in debian_systemd:
            self.log.info("This test only test in/uninstall server with systemd debian server.")
            return
        shell = RemoteMachineShellConnection(self.servers[1])
        try:
            self.log.info("** Start test debian uninstall with only -r flag **")
            shell.execute_command("dpkg -r couchbase-server")
            shell.execute_command("rm -rf /opt/couchbase")
            self.sleep(10)
            shell.execute_command("dpkg -i /tmp/couchbase-server-en*")
            output, error = shell.execute_command("systemctl list-unit-files |  grep couchbase")
            if output:
                self.log.info("output from list-unit-files |  grep couchbase: \n{0}"
                                                                    .format(output))
                if "masked" in output[0]:
                    self.fail("couchbase server is masked in systemd server => {0}"
                                                                .format(output[0]))
        finally:
            cmd1 = "dpkg -r couchbase-server; dpkg --purge couchbase-server"
            shell.execute_command(cmd1)
            shell.execute_command("rm -rf /opt/couchbase")
            self.sleep(10)
            shell.execute_command("dpkg -i /tmp/couchbase-server-*")
            output, error = shell.execute_command("systemctl list-unit-files |  grep couchbase")
            self.log.info("output from list-unit-files |  grep couchbase: \n{0}"
                                                                .format(output))
            shell.disconnect()

    def verify_results(self, output, error):
        if len(error) > 0 and '\n'.join(error).find("DeprecationWarning") == -1:
            raise Exception("Command throw out error message.\
                             Please check the output of remote_util")
        else:
            if '\n'.join(output).lower().find("not found") != -1:
                raise Exception("Command throw out error message.\
                                 Please check the output of remote_util")
            elif output.__len__() < 1:
                raise Exception("Command does not throw out error message \
                                 but the output is empty. \
                                 Please check the output of remote_util")

    def _verify_direct_client_stats(self, bucket, command, output):
        mc_conn = MemcachedClientHelper.direct_client(self.master,
                                                      bucket.name, self.timeout)
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
                    raise Exception("Command does not throw out error message \
                                     but cbstats gives no output. \
                                     Please check the output manually")
            else:
                collect_stats = mc_conn.stats(command)[stats[0].strip()]
            self.log.info("CbStats###### for {0}:::{1}=={2}" \
                          .format(stats[0].strip(), collect_stats, stats[1].strip()))
            if stats[1].strip() == collect_stats:
                continue
            else:
                if stats[0].find('tcmalloc') != -1 or stats[0].find('bytes') != -1 or\
                stats[0].find('mem_used') != -1:
                    self.log.warn("Stat didn't match, but it can be changed, not a bug")
                    continue
                raise Exception("Command does not throw out error message \
                                 but cbstats does not match.")
