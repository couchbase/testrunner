import logger
import re
import time
import unittest
from TestInput import TestInputSingleton
from builds.build_query import BuildQuery
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
import testconstants

TIMEOUT_SECS = 30

def _get_build(master, version, is_amazon=False):
    log = logger.Logger.get_logger()
    remote = RemoteMachineShellConnection(master)
    info = remote.extract_remote_info()
    remote.disconnect()
    builds, changes = BuildQuery().get_all_builds()
    log.info("finding build {0} for machine {1}".format(version, master))
    result = re.search('r', version)
    product = 'membase-server-enterprise'
    if re.search('1.8',version):
        product = 'couchbase-server-enterprise'

    if result is None and not version.startswith("1.8.1"):
        appropriate_build = BuildQuery().find_membase_release_build(product,
                                                                    info.deliverable_type,
                                                                    info.architecture_type,
                                                                    version.strip(),
                                                                    is_amazon=is_amazon)
    else:
        appropriate_build = BuildQuery().find_membase_build(builds, product,
                                                            info.deliverable_type,
                                                            info.architecture_type,
                                                            version.strip(),
                                                            is_amazon=is_amazon)
    return appropriate_build

def _create_load_multiple_bucket(self, server, bucket_data, howmany=2):
    created = BucketOperationHelper.create_multiple_buckets(server, 1, howmany=howmany)
    self.assertTrue(created, "unable to create multiple buckets")
    rest = RestConnection(server)
    buckets = rest.get_buckets()
    for bucket in buckets:
        bucket_data[bucket.name] = {}
        ready = BucketOperationHelper.wait_for_memcached(server, bucket.name)
        self.assertTrue(ready, "wait_for_memcached failed")
        #let's insert some data
        distribution = {2 * 1024: 0.5, 20: 0.5}
        bucket_data[bucket.name]["inserted_keys"], bucket_data[bucket.name]["reject_keys"] =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[server], name=bucket.name,
                                                              ram_load_ratio=2.0,
                                                              number_of_threads=2,
                                                              value_size_distribution=distribution,
                                                              write_only=True,
                                                              moxi=True)
        RebalanceHelper.wait_for_stats(server, bucket.name, 'ep_queue_size', 0)
        RebalanceHelper.wait_for_stats(server, bucket.name, 'ep_flusher_todo', 0)


class SingleNodeUpgradeTests(unittest.TestCase):
    #test descriptions are available http://techzone.couchbase.com/wiki/display/membase/Test+Plan+1.7.0+Upgrade

    def _install_and_upgrade(self, initial_version='1.6.5.3',
                             initialize_cluster=False,
                             create_buckets=False,
                             insert_data=False):
        log = logger.Logger.get_logger()
        input = TestInputSingleton.input
        version = input.test_params['version']
        rest_settings = input.membase_settings
        servers = input.servers
        server = servers[0]
        is_amazon = False
        if input.test_params.get('amazon',False):
            is_amazon = True
        remote = RemoteMachineShellConnection(server)
        rest = RestConnection(server)
        info = remote.extract_remote_info()
        remote.membase_uninstall()
        remote.couchbase_uninstall()
        builds, changes = BuildQuery().get_all_builds()
        #release_builds = BuildQuery().get_all_release_builds(initial_version)

        #if initial_version == "1.7.2":
         #   initial_version = "1.7.2r-20"
        older_build = BuildQuery().find_membase_release_build(deliverable_type=info.deliverable_type,
                                                              os_architecture=info.architecture_type,
                                                              build_version=initial_version,
                                                              product='membase-server-enterprise', is_amazon=is_amazon)
        if info.type.lower() == 'windows':
            if older_build.product_version.startswith("1.8"):
                abbr_product = "cb"
            else:
                abbr_product = "mb"
            remote.download_binary_in_win(older_build.url, abbr_product, initial_version)
            remote.membase_install_win(older_build, initial_version)
            RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
            rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
            bucket_data = {}
            if initialize_cluster:
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                if create_buckets:
                    _create_load_multiple_bucket(self, server, bucket_data, howmany=2)
            if version.startswith("1.8"):
                abbr_product = "cb"
            appropriate_build = _get_build(servers[0], version, is_amazon=is_amazon)
            self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(version))
            remote.download_binary_in_win(appropriate_build.url, abbr_product, version)
            remote.stop_membase()
            log.info("###### START UPGRADE. #########")
            remote.membase_upgrade_win(info.architecture_type, info.windows_name, version, initial_version)
            remote.disconnect()
            RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)

            pools_info = rest.get_pools_info()

            rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
            time.sleep(TIMEOUT_SECS)
            # verify admin_creds still set

            self.assertTrue(pools_info['implementationVersion'], appropriate_build.product_version)
            if initialize_cluster:
                #TODO: how can i verify that the cluster init config is preserved
                if create_buckets:
                    self.assertTrue(BucketOperationHelper.wait_for_bucket_creation('bucket-0', rest),
                                    msg="bucket 'default' does not exist..")
                if insert_data:
                    buckets = rest.get_buckets()
                    for bucket in buckets:
                        BucketOperationHelper.keys_exist_or_assert(bucket_data[bucket.name]["inserted_keys"],
                                                                   server,
                                                                   bucket.name, self)
        else:
            log.error("This is not windows server!")


    def test_single_node_upgrade_s4_1_6_5_3(self):
        self._install_and_upgrade(initial_version='1.6.5.3',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_5_3_1(self):
        self._install_and_upgrade(initial_version='1.6.5.3.1',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_5_4(self):
        self._install_and_upgrade(initial_version='1.6.5.4',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_5_2_1(self):
        self._install_and_upgrade(initial_version='1.6.5.2.1',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_5_1(self):
        self._install_and_upgrade(initial_version='1.6.5.1',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_5(self):
        self._install_and_upgrade(initial_version='1.6.5',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_4_1(self):
        self._install_and_upgrade(initial_version='1.6.4.1',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_4(self):
        self._install_and_upgrade(initial_version='1.6.4',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_3(self):
        self._install_and_upgrade(initial_version='1.6.3',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_1(self):
        self._install_and_upgrade(initial_version='1.6.1',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_6_0(self):
        self._install_and_upgrade(initial_version='1.6.0',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_7_0(self):
        self._install_and_upgrade(initial_version='1.7.0',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_7_1(self):
        self._install_and_upgrade(initial_version='1.7.1',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_7_1_1(self):
        self._install_and_upgrade(initial_version='1.7.1.1',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_7_2(self):
        self._install_and_upgrade(initial_version='1.7.2',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)


    def test_single_node_upgrade_s4_1_8_0(self):
        self._install_and_upgrade(initial_version='1.8.0',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

class MultipleNodeUpgradeTests(unittest.TestCase):

    def test_m6_1_6_5_3_1(self):
        self._install_and_upgrade('1.6.5.3.1', True, True, True, 10)

    def test_m6_1_6_5(self):
        self._install_and_upgrade('1.6.5', True, True, True, 10)

    def test_m6_1_6_5_3(self):
        self._install_and_upgrade('1.6.5.3', True, True, True, 10)

    def test_m6_1_6_5_4(self):
        self._install_and_upgrade('1.6.5.4', True, True, True, 10)

    def test_m6_1_6_5_2_1(self):
        self._install_and_upgrade('1.6.5.2.1', True, True, True, 10)

    def test_m6_1_6_4_1(self):
        self._install_and_upgrade('1.6.4.1', True, True, True, 10)

    def test_m6_1_6_4(self):
        self._install_and_upgrade('1.6.4', True, True, True, 10)

    def test_m6_1_6_3(self):
        self._install_and_upgrade('1.6.3', True, True, True, 10)

    def test_m6_1_6_1(self):
        self._install_and_upgrade('1.6.1', True, True, True, 10)

    def test_m6_1_6_0(self):
        self._install_and_upgrade('1.6.0', True, True, True, 10)

    def test_m6_1_7_0(self):
        self._install_and_upgrade('1.7.0', True, True, True, 10)

    def test_m6_1_7_1(self):
        self._install_and_upgrade('1.7.1', True, True, True, 10)

    def test_m6_1_7_1_1(self):
        self._install_and_upgrade('1.7.1.1', True, True, True, 10)

    def test_m6_1_7_2(self):
        self._install_and_upgrade('1.7.2', True, True, True, 10)

    def test_m6_1_8_0(self):
        self._install_and_upgrade('1.8.0', True, True, True, 10)

    # Rolling Upgrades
    def test_multiple_node_rolling_upgrade_1_7_0(self):
        self._install_and_upgrade('1.7.0', True, True, False, -1, True)

    def test_multiple_node_rolling_upgrade_1_7_1(self):
        self._install_and_upgrade('1.7.1', True, True, False, -1, True)

    def test_multiple_node_rolling_upgrade_1_7_1_1(self):
        self._install_and_upgrade('1.7.1.1', True, True, False, -1, True)

    def test_multiple_node_rolling_upgrade_1_7_2(self):
        self._install_and_upgrade('1.7.2', True, True, False, -1, True)

    def test_multiple_node_rolling_upgrade_1_8_0(self):
        self._install_and_upgrade('1.8.0', True, True, False, -1, True)

    # Multiple Version Upgrades
    def test_multiple_version_upgrade_start_one_1(self):
        upgrade_path = ['1.7.0', '1.7.1.1']
        self._install_and_upgrade('1.6.5.4', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_1(self):
        upgrade_path = ['1.7.0', '1.7.1.1']
        self._install_and_upgrade('1.6.5.4', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_2(self):
        upgrade_path = ['1.7.1.1']
        self._install_and_upgrade('1.6.5.4', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_3(self):
        upgrade_path = ['1.7.0']
        self._install_and_upgrade('1.6.5.4', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_3(self):
        upgrade_path = ['1.7.0']
        self._install_and_upgrade('1.6.5.4', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_4(self):
        upgrade_path = ['1.7.0', '1.7.1.1','1.7.2']
        self._install_and_upgrade('1.6.5.4', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_4(self):
        upgrade_path = ['1.7.0', '1.7.1.1','1.7.2']
        self._install_and_upgrade('1.6.5.4', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_2(self):
        upgrade_path = ['1.7.1.1']
        self._install_and_upgrade('1.6.5.4', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_5(self):
        upgrade_path = ['1.7.1.1', '1.7.2']
        self._install_and_upgrade('1.6.5.4', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_5(self):
        upgrade_path = ['1.7.1.1', '1.7.2']
        self._install_and_upgrade('1.6.5.4', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_6(self):
        upgrade_path = ['1.7.0', '1.7.2']
        self._install_and_upgrade('1.6.5.4', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_6(self):
        upgrade_path = ['1.7.0', '1.7.2']
        self._install_and_upgrade('1.6.5.4', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_7(self):
        upgrade_path = ['1.7.2']
        self._install_and_upgrade('1.7.0', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_7(self):
        upgrade_path = ['1.7.2']
        self._install_and_upgrade('1.7.0', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_8(self):
        upgrade_path = ['1.7.2']
        self._install_and_upgrade('1.7.1', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_8(self):
        upgrade_path = ['1.7.2']
        self._install_and_upgrade('1.7.1', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_9(self):
        upgrade_path = ['1.7.2']
        self._install_and_upgrade('1.7.1.1', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_9(self):
        upgrade_path = ['1.7.2']
        self._install_and_upgrade('1.7.1.1', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_10(self):
        upgrade_path = ['1.7.2', '1.8.0']
        self._install_and_upgrade('1.7.1.1', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_10(self):
        upgrade_path = ['1.7.2', '1.8.0']
        self._install_and_upgrade('1.7.1.1', True, True, False, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_one_11(self):
        upgrade_path = ['1.8.0']
        self._install_and_upgrade('1.7.2', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_11(self):
        upgrade_path = ['1.8.0']
        self._install_and_upgrade('1.7.2', True, True, False, 10, False, upgrade_path)

    #do some bucket/init related operation
    #now only option x nodes
    #power on upgraded ones first and then the non-upgraded ones
    #start everything and wait for some time
    def _install_and_upgrade(self, initial_version='1.6.5.3',
                             create_buckets=False,
                             insert_data=False,
                             start_upgraded_first=True,
                             load_ratio=-1,
                             roll_upgrade=False,
                             upgrade_path=[]):
        node_upgrade_path = []
        node_upgrade_path.extend(upgrade_path)
        #then start them in whatever order you want
        inserted_keys = []
        log = logger.Logger.get_logger()
        if roll_upgrade:
            log.info("performing a rolling upgrade")
        input = TestInputSingleton.input
        input_version = input.test_params['version']
        rest_settings = input.membase_settings
        servers = input.servers
        is_amazon = False
        if input.test_params.get('amazon',False):
            is_amazon = True

        # install older build on all nodes
        for server in servers:
            remote = RemoteMachineShellConnection(server)
            rest = RestConnection(server)
            info = remote.extract_remote_info()
            older_build = BuildQuery().find_membase_release_build(deliverable_type=info.deliverable_type,
                                                              os_architecture=info.architecture_type,
                                                              build_version=initial_version,
                                                              product='membase-server-enterprise', is_amazon=is_amazon)

            remote.membase_uninstall()
            remote.couchbase_uninstall()
            if older_build.product_version.startswith("1.8"):
                abbr_product = "cb"
            else:
                abbr_product = "mb"
            remote.download_binary_in_win(older_build.url, abbr_product, initial_version)
            #now let's install ?
            remote.membase_install_win(older_build, initial_version)
            RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
            rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
            rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
            remote.disconnect()

        bucket_data = {}
        master = servers[0]
        # cluster all the nodes together
        ClusterOperationHelper.add_all_nodes_or_assert(master,
                                                       servers,
                                                       rest_settings, self)
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        otpNodeIds = []
        for node in nodes:
            otpNodeIds.append(node.id)
        rebalanceStarted = rest.rebalance(otpNodeIds, [])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(master.ip))
        log.info('started rebalance operation on master node {0}'.format(master.ip))
        rebalanceSucceeded = rest.monitorRebalance()
        self.assertTrue(rebalanceSucceeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))

        if create_buckets:
            #let's create buckets
            #wait for the bucket
            #bucket port should also be configurable , pass it as the
            #parameter to this test ? later

            self._create_default_bucket(master)
            inserted_keys = self._load_data(master, load_ratio)
            _create_load_multiple_bucket(self, master, bucket_data, howmany=2)

        #if initial_version == "1.7.0" or initial_version == "1.7.1":
         #   self._save_config(rest_settings, master)

        node_upgrade_path.append(input_version)
        #if we dont want to do roll_upgrade ?
        log.info("Upgrade path: {0} -> {1}".format(initial_version, node_upgrade_path))
        log.info("List of servers {0}".format(servers))
        if not roll_upgrade:
            for version in node_upgrade_path:
                if version is not initial_version:
                    log.info("SHUTDOWN ALL CB OR MB SERVERS IN CLUSTER BEFORE DOING UPGRADE")
                    for server in servers:
                        shell = RemoteMachineShellConnection(server)
                        shell.stop_membase()
                        shell.disconnect()
                    log.info("Upgrading to version {0}".format(version))
                    appropriate_build = _get_build(servers[0], version, is_amazon=is_amazon)
                    self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(version))
                    for server in servers:
                        remote = RemoteMachineShellConnection(server)
                        if version.startswith("1.8"):
                            abbr_product = "cb"
                        remote.download_binary_in_win(appropriate_build.url, abbr_product, version)
                        log.info("###### START UPGRADE. #########")
                        remote.membase_upgrade_win(info.architecture_type, info.windows_name, version, initial_version)
                        RestHelper(RestConnection(server)).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)

                        #verify admin_creds still set
                        pools_info = RestConnection(server).get_pools_info()
                        self.assertTrue(pools_info['implementationVersion'], appropriate_build.product_version)

                        if not start_upgraded_first:
                            remote.stop_membase()

                        remote.disconnect()
                    if not start_upgraded_first:
                        log.info("Starting all servers together")
                        self._start_membase_servers(servers)
                    time.sleep(TIMEOUT_SECS)

                    if create_buckets:
                        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation('default', RestConnection(master)),
                                        msg="bucket 'default' does not exist..")
                    if insert_data:
                        self._verify_data(master, rest, inserted_keys)

        # rolling upgrade
        else:
            version = input.test_params['version']
            if version.startswith("1.8"):
                abbr_product = "cb"
            appropriate_build = _get_build(servers[0], version, is_amazon=is_amazon)
            self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(version))
            # rebalance node out
            # remove membase from node
            # install destination version onto node
            # rebalance it back into the cluster
            for server_index in range(len(servers)):
                server = servers[server_index]
                master = servers[server_index - 1]
                log.info("current master is {0}, rolling node is {1}".format(master, server))

                rest = RestConnection(master)
                nodes = rest.node_statuses()
                allNodes = []
                toBeEjectedNodes = []
                for node in nodes:
                    allNodes.append(node.id)
                    if "{0}:{1}".format(node.ip, node.port) == "{0}:{1}".format(server.ip, server.port):
                        toBeEjectedNodes.append(node.id)
                helper = RestHelper(rest)
                removed = helper.remove_nodes(knownNodes=allNodes, ejectedNodes=toBeEjectedNodes)
                self.assertTrue(removed, msg="Unable to remove nodes {0}".format(toBeEjectedNodes))
                remote = RemoteMachineShellConnection(server)
                remote.membase_uninstall()
                remote.couchbase_uninstall()
                if appropriate_build.product == 'membase-server-enterprise':
                    abbr_product = "mb"
                else:
                    abbr_product = "cb"
                remote.download_binary_in_win(appropriate_build.url, abbr_product, version)
                remote.membase_install_win(appropriate_build, version)
             #   remote.membase_install_win(appropriate_build)
                RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
                time.sleep(TIMEOUT_SECS)
                rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                remote.disconnect()

                #readd this to the cluster
                ClusterOperationHelper.add_all_nodes_or_assert(master, [server], rest_settings, self)
                nodes = rest.node_statuses()
                log.info("wait 30 seconds before asking older node for start rebalance")
                time.sleep(30)
                otpNodeIds = []
                for node in nodes:
                    otpNodeIds.append(node.id)
                rebalanceStarted = rest.rebalance(otpNodeIds, [])
                self.assertTrue(rebalanceStarted,
                                "unable to start rebalance on master node {0}".format(master.ip))
                log.info('started rebalance operation on master node {0}'.format(master.ip))
                rebalanceSucceeded = rest.monitorRebalance()
                self.assertTrue(rebalanceSucceeded,
                                "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
                #ClusterOperationHelper.verify_persistence(servers, self)

            #TODO: how can i verify that the cluster init config is preserved
            # verify data on upgraded nodes
            if create_buckets:
                self.assertTrue(BucketOperationHelper.wait_for_bucket_creation('default', RestConnection(master)),
                                msg="bucket 'default' does not exist..")
            if insert_data:
                self._verify_data(master, rest, inserted_keys)
                rest = RestConnection(master)
                buckets = rest.get_buckets()
                for bucket in buckets:
                    BucketOperationHelper.keys_exist_or_assert(bucket_data[bucket.name]["inserted_keys"],
                                                               master,
                                                               bucket.name, self)

    def _start_membase_servers(self, servers):
        for server in servers:
            remote = RemoteMachineShellConnection(server)
            remote.start_membase()
            remote.disconnect()

    def _stop_membase_servers(self, servers):
        for server in servers:
            remote = RemoteMachineShellConnection(server)
            remote.stop_membase()
            remote.disconnect()

    def _load_data(self, master, load_ratio):
        log = logger.Logger.get_logger()
        if load_ratio == -1:
            #let's load 0.1 data
            load_ratio = 0.1
        distribution = {1024: 0.5, 20: 0.5}
        #TODO: with write_only = False, sometimes the load hangs, debug this
        inserted_keys, rejected_keys =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[master],
                                                              ram_load_ratio=load_ratio,
                                                              number_of_threads=1,
                                                              value_size_distribution=distribution,
                                                              write_only=True)
        log.info("wait until data is completely persisted on the disk")
        RebalanceHelper.wait_for_stats(master, "default", 'ep_queue_size', 0)
        RebalanceHelper.wait_for_stats(master, "default", 'ep_flusher_todo', 0)
        return inserted_keys

    def _create_default_bucket(self, master):
        BucketOperationHelper.create_default_buckets(servers=[master], assert_on_test=self)
        ready = BucketOperationHelper.wait_for_memcached(master, "default")
        self.assertTrue(ready, "wait_for_memcached failed")

    def _verify_data(self, master, rest, inserted_keys):
        log = logger.Logger.get_logger()
        log.info("Verifying data")
        ready = RebalanceHelper.wait_for_stats_on_all(master, 'default', 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats_on_all(master, 'default', 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        BucketOperationHelper.keys_exist_or_assert(keys=inserted_keys, server=master, bucket_name='default',
                                                                   test=self)
