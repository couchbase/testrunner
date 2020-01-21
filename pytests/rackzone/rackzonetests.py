import json
import time
import unittest
import testconstants
from TestInput import TestInputSingleton

from rackzone.rackzone_base import RackzoneBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper
from membase.api.rest_client import RestConnection, Bucket
from membase.helper.rebalance_helper import RebalanceHelper
from membase.api.exception import RebalanceFailedException
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.bucket_helper import BucketOperationHelper
from scripts.install import InstallerJob
from testconstants import COUCHBASE_VERSION_2
from testconstants import COUCHBASE_FROM_VERSION_3



class RackzoneTests(RackzoneBaseTest):
    def setUp(self):
        super(RackzoneTests, self).setUp()
        self.command = self.input.param("command", "")
        self.zone = self.input.param("zone", 1)
        self.replica = self.input.param("replica", 1)
        self.command_options = self.input.param("command_options", '')
        self.set_get_ratio = self.input.param("set_get_ratio", 0.9)
        self.item_size = self.input.param("item_size", 128)
        self.shutdown_zone = self.input.param("shutdown_zone", 1)
        self.do_verify = self.input.param("do-verify", True)
        self.num_node = self.input.param("num_node", 4)
        self.timeout = 6000


    def tearDown(self):
        super(RackzoneTests, self).tearDown()

    def test_check_default_zone_create_by_default(self):
        zone_name = "Group 1"
        self._verify_zone(zone_name)

    def test_create_second_default_zone(self):
        zone_name = "Group 1"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create additional default zone")
            rest.add_zone(zone_name)
        except Exception as e :
            print(e)

    def test_create_zone_with_upper_case_name(self):
        zone_name = "ALLWITHUPTERCASE"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception as e :
            print(e)
        self._verify_zone(zone_name)

    def test_create_zone_with_lower_case_name(self):
        zone_name = "allwithlowercaseeeeeee"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception as e :
            print(e)
        self._verify_zone(zone_name)

    def test_create_zone_with_all_number_name(self):
        zone_name = "3223345557666760"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception as e :
            print(e)
        self._verify_zone(zone_name)

    def test_create_zone_with_upper_lower_number_name(self):
        zone_name = "AAABBBCCCaakkkkmmm345672"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception as e :
            print(e)
        self._verify_zone(zone_name)

    def test_create_zone_with_upper_lower_number_and_space_name(self):
        zone_name = " AAAB BBCCC aakkk kmmm3 456 72 "
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception as e :
            print(e)
        self._verify_zone(zone_name)

    def test_create_zone_with_none_ascii_name(self):
        # zone name is limited to 64 bytes
        zone_name = "abcdGHIJKLMNOPQRSTUVWXYZ0123456789efghijklmnopqrstuvwyABCDEF_-.%"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception as e :
            print(e)
        self._verify_zone(zone_name)

    def test_delete_empty_defautl_zone(self):
        zone_name ="test1"
        default_zone = "Group 1"
        moved_node = []
        serverInfo = self.servers[0]
        moved_node.append(serverInfo.ip)
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
            if rest.is_zone_exist(zone_name):
                self.log.info("Move node {0} from zone {1} to zone {2}" \
                              .format(moved_node, default_zone, zone_name))
                status = rest.shuffle_nodes_in_zones(moved_node, default_zone, zone_name)
                if status:
                    rest.delete_zone(default_zone)
                else:
                    self.fail("Failed to move node {0} from zone {1} to zone {2}" \
                              .format(moved_node, default_zone, zone_name))
                if not rest.is_zone_exist(default_zone):
                    self.log.info("successful delete default zone")
                else:
                    raise Exception("Failed to delete default zone")
            rest.rename_zone(zone_name, default_zone)
        except Exception as e :
            print(e)

    """ test params:
         -p shutdown_zone=1,items=100000,shutdown_zone=1,zone=2,replicas=1 """
    def test_replica_distribution_in_zone(self):
        if len(self.servers) < int(self.num_node):
            msg = "This test needs minimum {1} servers to run.\n  Currently in ini file \
                   has only {0} servers".format(len(self.servers), self.num_node)
            self.log.error("{0}".format(msg))
            raise Exception(msg)
        if self.shutdown_zone >= self.zone:
            msg = "shutdown zone should smaller than zone"
            raise Exception(msg)
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        zones = []
        zones.append("Group 1")
        nodes_in_zone = {}
        nodes_in_zone["Group 1"] = [serverInfo.ip]
        """ Create zone base on params zone in test"""
        try:
            if int(self.zone) > 1:
                for i in range(1, int(self.zone)):
                    a = "Group "
                    zone_name = a + str(i + 1)
                    zones.append(zone_name)
                    rest.add_zone(zone_name)
            servers_rebalanced = []
            self.user = serverInfo.rest_username
            self.password = serverInfo.rest_password
            if len(self.servers)%int(self.zone) != 0:
                msg = "unbalance zone.  Recaculate to make balance ratio node/zone"
                raise Exception(msg)
            """ Add node to each zone """
            k = 1
            for i in range(0, self.zone):
                if "Group 1" in zones[i]:
                    total_node_per_zone = int(len(self.servers))//int(self.zone) - 1
                else:
                    nodes_in_zone[zones[i]] = []
                    total_node_per_zone = int(len(self.servers))//int(self.zone)
                for n in range(0, total_node_per_zone):
                    nodes_in_zone[zones[i]].append(self.servers[k].ip)
                    rest.add_node(user=self.user, password=self.password, \
                        remoteIp=self.servers[k].ip, port='8091', zone_name=zones[i])
                    k += 1
            otpNodes = [node.id for node in rest.node_statuses()]
            """ Start rebalance and monitor it. """
            started = rest.rebalance(otpNodes, [])

            if started:
                try:
                    result = rest.monitorRebalance()
                except RebalanceFailedException as e:
                    self.log.error("rebalance failed: {0}".format(e))
                    return False, servers_rebalanced
                msg = "successfully rebalanced cluster {0}"
                self.log.info(msg.format(result))
            """ Verify replica of one node should not in same zone of active. """
            self._verify_replica_distribution_in_zones(nodes_in_zone, "tap")

            """ Simulate entire nodes down in zone(s) by killing erlang process"""
            shutdown_nodes = []
            if self.shutdown_zone >= 1 and self.zone >=2:
                self.log.info("Start to shutdown nodes in zone to failover")
                for down_zone in range(1, self.shutdown_zone + 1):
                    down_zone = "Group " + str(down_zone + 1)
                    for sv in nodes_in_zone[down_zone]:
                        for si in self.servers:
                            if si.ip == sv:
                                server = si
                                shutdown_nodes.append(si)

                        shell = RemoteMachineShellConnection(server)
                        shell.kill_erlang(self.os_name)
                        """ Failover down node(s)"""
                        self.log.info("----> start failover node %s" % server.ip)
                        failed_over = rest.fail_over("ns_1@" + server.ip)
                        if not failed_over:
                            self.log.info("unable to failover the node the first time. \
                                           try again in 75 seconds..")
                            time.sleep(75)
                            failed_over = rest.fail_over("ns_1@" + server.ip)
                        self.assertTrue(failed_over,
                                        "unable to failover node after erlang was killed")
            otpNodes = [node.id for node in rest.node_statuses()]
            self.log.info("----> start rebalance after failover.")
            """ Start rebalance and monitor it. """
            started = rest.rebalance(otpNodes, [])
            if started:
                try:
                    result = rest.monitorRebalance()
                except RebalanceFailedException as e:
                    self.log.error("rebalance failed: {0}".format(e))
                    return False, servers_rebalanced
                msg = "successfully rebalanced in selected nodes from the cluster ? {0}"
                self.log.info(msg.format(result))
            """ Compare current keys in bucekt with initial loaded keys count. """
            self._verify_total_keys(self.servers[0], self.num_items)
        except Exception as e:
            self.log.error(e)
            raise
        finally:
            self.log.info("---> remove all nodes in all zones")
            if shutdown_nodes:
                for node in shutdown_nodes:
                    conn = RemoteMachineShellConnection(node)
                    self.log.info("---> re-start nodes %s after erlang killed " % node.ip)
                    conn.start_couchbase()
                    time.sleep(5)
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
            ClusterOperationHelper.cleanup_cluster(self.servers, master=self.master)
            self.log.info("---> remove all zones in cluster")
            rm_zones = rest.get_zone_names()
            for zone in rm_zones:
                if zone != "Group 1":
                    rest.delete_zone(zone)

    """ to run this test, use these params:
            nodes_init=3,version=2.5.1-xx,type=community   """
    def test_zone_enable_after_upgrade_from_ce_to_ee(self):
        self.services = self.input.param("services", "kv")
        params = {}
        params['product'] = self.product
        params['version'] = self.version
        params['vbuckets'] = [self.vbuckets]
        params['type'] = self.type
        """ install couchbasse server community edition to run the test """
        InstallerJob().parallel_install(self.servers[:3], params)

        params["type"] = "enterprise"
        zone_name = "AAABBBCCCaakkkkmmm345672"
        serverInfo = self.servers[0]
        ini_servers = self.servers[:self.nodes_init]
        rest = RestConnection(serverInfo)
        self.user = serverInfo.rest_username
        self.password = serverInfo.rest_password
        if len(ini_servers) > 1:
            self.cluster.rebalance([ini_servers[0]], ini_servers[1:], [],\
                                                 services = self.services)
        rest = RestConnection(self.master)
        self._bucket_creation()

        """ verify all nodes in cluster in CE """
        if rest.is_enterprise_edition():
            raise Exception("This test needs couchbase server community edition to run")

        self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)
        try:
            self.log.info("create zone {0}".format(zone_name))
            result = rest.add_zone(zone_name)
            if result:
                raise Exception("Zone feature should not be available in CE version")
        except Exception as e :
            if "Failed" in e:
                pass

        for i in range(1, int(self.nodes_init) + 1):
            if i == 1:
                """ install EE on one node to do swap rebalance """
                InstallerJob().parallel_install(self.servers[3:], params)
                self.cluster.rebalance([ini_servers[0]], \
                                       [self.servers[int(self.nodes_init)]],\
                                       [self.servers[int(self.nodes_init) - i]],
                                                       services = self.services)
                self.log.info("sleep  5 seconds")
                time.sleep(5)
                try:
                    self.log.info("try to create zone {0} "
                                          "when cluster is not completely EE"
                                                          .format(zone_name))
                    result = rest.add_zone(zone_name)
                    if result:
                        raise Exception(\
                            "Zone feature should not be available in CE version")
                except Exception as e :
                    if "Failed" in e:
                        pass
            else:
                InstallerJob().parallel_install([self.servers[int(self.nodes_init)\
                                                               - (i - 1)]], params)
                self.cluster.rebalance([ini_servers[0]],\
                                    [self.servers[int(self.nodes_init) - (i -1)]],
                                         [self.servers[int(self.nodes_init) - i]],
                                                         services = self.services)
                self.sleep(12, "wait 12 seconds after rebalance")
                if i < int(self.nodes_init):
                    try:
                        self.log.info("try to create zone {0} "
                            "when cluster is not completely EE".format(zone_name))
                        result = rest.add_zone(zone_name)
                        if result:
                            raise Exception(\
                             "Zone feature should not be available in CE version")
                    except Exception as e :
                        if "Failed" in e:
                            pass
        serverInfo = self.servers[1]
        rest = RestConnection(serverInfo)
        self.user = serverInfo.rest_username
        self.password = serverInfo.rest_password
        if not rest.is_enterprise_edition():
            raise Exception("Test failed to upgrade cluster from CE to EE")
        self.log.info("try to create zone {0} "
             "when cluster {1} is completely EE".format(zone_name, serverInfo.ip))
        result = rest.add_zone(zone_name)
        self.log.info("sleep  5 seconds")
        time.sleep(5)
        if result:
            self.log.info("Zone feature is available in this cluster")
        else:
            raise Exception("Could not create zone with name: %s in cluster.  "
                                                          "It's a bug" % zone_name)
        if rest.is_zone_exist(zone_name.strip()):
            self.log.info("verified! zone '{0}' is existed"
                                                        .format(zone_name.strip()))
        else:
            raise Exception("There is not zone with name: %s in cluster.  "
                                                          "It's a bug" % zone_name)

        """ re-install enterprise edition for next test if there is any """
        InstallerJob().parallel_install([self.servers[0]], params)

        """ reset master node to new node to teardown cluster """
        self.log.info("Start to clean up cluster")
        self.master = self.servers[1]
        self.servers = self.servers[1:]

    def _verify_zone(self, name):
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        if rest.is_zone_exist(name.strip()):
            self.log.info("verified! zone '{0}' is existed".format(name.strip()))
        else:
            raise Exception("There is not zone with name: %s in cluster" % name)

    def _verify_replica_distribution_in_zones(self, nodes, command, saslPassword = ""):
        shell = RemoteMachineShellConnection(self.servers[0])
        saslPassword = ''
        versions = RestConnection(self.master).get_nodes_versions()
        for group in nodes:
            for node in nodes[group]:
                buckets = RestConnection(self.servers[0]).get_buckets()
                if versions[0][:5] in COUCHBASE_VERSION_2:
                    command = "tap"
                    cmd  = "%s %s:11210 %s -b %s -p '%s' "\
                            % (self.cbstat_command, node, command, buckets[0].name, saslPassword)
                    cmd += "| grep :vb_filter: "\
                           "| gawk '{print $1}' "\
                           "| sed 's/eq_tapq:replication_ns_1@//g' "\
                           "| sed 's/:vb_filter://g' "
                    output, error = shell.execute_command(cmd)
                elif versions[0][:5] in COUCHBASE_FROM_VERSION_3:
                    command = "dcp"
                    if 5 <= int(versions[0]):
                        saslPassword = self.master.rest_password
                    cmd  = "%s %s:11210 %s -b %s -u Administrator -p '%s' "\
                            % (self.cbstat_command, node, command, buckets[0].name, saslPassword)
                    cmd += "| grep :replication:ns_1@%s |  grep vb_uuid "\
                           "| gawk '{print $1}' "\
                           "| sed 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' "\
                           "| sed 's/:.*//g' "\
                            % (node, node)
                    output, error = shell.execute_command(cmd)
                    output = sorted(set(output))
                shell.log_command_output(output, error)
                output = output[0].split(" ")
                if node not in output:
                    self.log.info("{0}".format(nodes))
                    self.log.info("replicas of node {0} are in nodes {1}"
                                                   .format(node, output))
                    self.log.info("replicas of node {0} are not in its zone {1}"
                                                   .format(node, group))
                else:
                    raise Exception("replica of node {0} are on its own zone {1}"
                                                   .format(node, group))
        shell.disconnect()

    def _verify_total_keys(self, server, loaded_keys):
        rest = RestConnection(server)
        buckets = rest.get_buckets()
        for bucket in buckets:
            self.log.info("start to verify bucket: {0}".format(bucket))
            stats = rest.get_bucket_stats(bucket)
            if stats["curr_items"] == loaded_keys:
                self.log.info("{0} keys in bucket {2} match with \
                               pre-loaded keys: {1}".format(stats["curr_items"],
                                                            loaded_keys, bucket))
            else:
                raise Exception("{%s keys in bucket %s does not match with \
                                 loaded %s keys" % (stats["curr_items"], bucket, loaded_keys))

