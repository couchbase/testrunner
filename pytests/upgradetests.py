import unittest
from TestInput import TestInputSingleton
from builds.build_query import BuildQuery
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
import logger
import time

class SingleNodeUpgradeTests(unittest.TestCase):
    #test descriptions are available http://techzone.couchbase.com/wiki/display/membase/Test+Plan+1.7.0+Upgrade

    def _install_and_upgrade(self, initial_version='1.6.5.3',
                             initialize_cluster=False,
                             create_buckets=False,
                             insert_data=False):
        log = logger.Logger.get_logger()
        input = TestInputSingleton.input
        rest_settings = input.membase_settings
        servers = input.servers
        server = servers[0]
        remote = RemoteMachineShellConnection(server)
        rest = RestConnection(server)
        info = remote.extract_remote_info()
        remote.membase_uninstall()
        builds, changes = BuildQuery().get_all_builds()
        older_build = BuildQuery().find_membase_build(builds=builds,
                                                          deliverable_type=info.deliverable_type,
                                                          os_architecture=info.architecture_type,
                                                          build_version=initial_version,
                                                          product='membase-server-enterprise')
        remote.execute_command('/etc/init.d/membase-server stop')
        remote.download_build(older_build)
        #now let's install ?
        remote.membase_install(older_build)
        RestHelper(rest).is_ns_server_running(120)
        log.info("sleep for 10 seconds to wait for membase-server to start...")
        rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
        bucket_data = {}
        
        if initialize_cluster:
            rest.init_cluster_memoryQuota()
            if create_buckets:
                created = BucketOperationHelper.create_multiple_buckets(server, 1)
                self.assertTrue(created, "unable to create multiple buckets")
                buckets = rest.get_buckets()

                for bucket in buckets:
                    bucket_data[bucket.name] = {}
                    ready = BucketOperationHelper.wait_for_memcached(server, bucket.name)
                    self.assertTrue(ready, "wait_for_memcached failed")
                    if insert_data:
                    #let's insert some data
                        distribution = {2 * 1024: 0.5, 20: 0.5}
                        bucket_data[bucket.name]["inserted_keys"], bucket_data[bucket.name]["reject_keys"] =\
                        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[server],
                                                                              name=bucket.name,
                                                                              ram_load_ratio=2.0,
                                                                              number_of_threads=1,
                                                                              value_size_distribution=distribution,
                                                                              write_only=True,
                                                                              moxi=False)
                        RebalanceHelper.wait_for_stats(server, bucket.name, 'ep_queue_size', 0)
                        RebalanceHelper.wait_for_stats(server, bucket.name, 'ep_flusher_todo', 0)



        version = input.test_params['version']
        #pick the first one in the list
        appropriate_build = BuildQuery().find_membase_build(builds,
                                 'membase-server-enterprise',
                                 info.deliverable_type,
                                 info.architecture_type,
                                 version.strip())

        remote.download_build(appropriate_build)
        remote.membase_upgrade(appropriate_build)
        remote.disconnect()
        RestHelper(rest).is_ns_server_running(120)

        pools_info = rest.get_pools_info()

        rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
        time.sleep(10)
        #verify admin_creds still set

        self.assertTrue(pools_info['implementationVersion'], appropriate_build.product_version)
        if initialize_cluster:
            #TODO: how can i verify that the cluster init config is preserved
            if create_buckets:
                self.assertTrue(BucketOperationHelper.wait_for_bucket_creation('bucket-0', rest),
                                msg="bucket 'default' does not exist..")
            if insert_data:
                buckets = rest.get_buckets()
                for bucket in buckets:
                    BucketOperationHelper.keys_exist_or_assert(bucket_data[bucket.name]["inserted_keys"], server,
                                                               bucket.name, self)


    def test_single_node_upgrade_s1_1_6_5_3(self):
        self._install_and_upgrade(initial_version='1.6.5.3',
                                  initialize_cluster=False,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s1_1_6_5_2(self):
        self._install_and_upgrade(initial_version='1.6.5.2',
                                  initialize_cluster=False,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s1_1_6_5_1(self):
        self._install_and_upgrade(initial_version='1.6.5.1',
                                  initialize_cluster=False,
                                  insert_data=False,
                                  create_buckets=False)


    def test_single_node_upgrade_s1_1_6_5(self):
        self._install_and_upgrade(initial_version='1.6.5',
                                  initialize_cluster=False,
                                  insert_data=False,
                                  create_buckets=False)


    def test_single_node_upgrade_s2_1_6_5_3(self):
        self._install_and_upgrade(initial_version='1.6.5.3',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s2_1_6_5_2(self):
        self._install_and_upgrade(initial_version='1.6.5.2',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s2_1_6_5_1(self):
        self._install_and_upgrade(initial_version='1.6.5.1',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s2_1_6_5(self):
        self._install_and_upgrade(initial_version='1.6.5',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=False)


    def test_single_node_upgrade_s3_1_6_5_3(self):
        self._install_and_upgrade(initial_version='1.6.5.3',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=True)

    def test_single_node_upgrade_s3_1_6_5_2(self):
        self._install_and_upgrade(initial_version='1.6.5.2',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=True)

    def test_single_node_upgrade_s3_1_6_5_1(self):
        self._install_and_upgrade(initial_version='1.6.5.1',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=True)

    def test_single_node_upgrade_s3_1_6_5(self):
        self._install_and_upgrade(initial_version='1.6.5',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=True)

    def test_single_node_upgrade_s5(self):
        self._install_and_upgrade(initial_version='1.6.5.2',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

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


    #1.6.0
    #1.6.1
    #1.6.3
    #1.6.4
    #1.6.4.1
    #1.6.5
    #1.6.5.1
    #1.6.5.2.1
    #1.6.5.3
    #1.6.5.3.1
    #1.6.5.4

    def test_single_node_upgrade_s5(self):
        #install the latest version and upgrade to itself
        input = TestInputSingleton.input
        servers = input.servers
        server = servers[0]
        builds, changes = BuildQuery().get_all_builds()
        remote = RemoteMachineShellConnection(server)
        info = remote.extract_remote_info()
        remote.disconnect()
        filtered_builds = []
        for build in builds:
            if build.deliverable_type == info.deliverable_type and\
               build.architecture_type == info.architecture_type:
                filtered_builds.append(build)
        sorted_builds = BuildQuery().sort_builds_by_version(filtered_builds)
        latest_version = sorted_builds[0].product_version
        self._install_and_upgrade(initial_version=latest_version
        )

        #TODO : expect a message like 'package membase-server-1.7~basestar-1.x86_64 is already installed'


class MultipleNodeUpgradeTests(unittest.TestCase):
    #in a 3 node cluster with no buckets shut down all the nodes update all
    # nodes one by one and then restart node(1),node(2) and node(3)
    def test_multiple_node_upgrade_m1_1_6_5_3(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.5.3', False, False, True, len(servers))

    #in a 3 node cluster with default bucket without any keys shut down all the nodes update
    # all nodes one by one and then restart node(1),node(2) and node(3)
    def test_multiple_node_upgrade_m2_1_6_5_3(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.5.3', True, False, len(servers))


        #in a 3 node cluster with default bucket with some keys shut down all the
        # nodes update all nodes one by one and then restart node(1),node(2) and node(3)

    def test_multiple_node_upgrade_m3_1_6_5_3(self):
        self._install_and_upgrade('1.6.5.3', True, True)

    #m3 with 50% ram full ?

    def test_m6_1_6_5_3_1(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.5.3.1', True, True, len(servers), True, 10)

    def test_m6_1_6_5_3(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.5.3', True, True, len(servers), True, 10)

    def test_m6_1_6_5_4(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.5.4', True, True, len(servers), True, 10)

    def test_m6_1_6_5_2_1(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.5.2.1', True, True, len(servers), True, 10)

    def test_m6_1_6_4_1(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.4.1', True, True, len(servers), True, 10)

    def test_m6_1_6_4(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.4', True, True, len(servers), True, 10)

    def test_m6_1_6_3(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.3', True, True, len(servers), True, 10)

    def test_m6_1_6_1(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.1', True, True, len(servers), True, 10)

    def test_m6_1_6_0(self):
        input = TestInputSingleton.input
        servers = input.servers
        self._install_and_upgrade('1.6.0', True, True, len(servers), True, 10)


    def test_multiple_node_upgrade_m5_1_6_5_3(self):
        self._install_and_upgrade('1.6.5.3', True, False, 1, False)

    def test_multiple_node_rolling_upgrade_1_6_5(self):
        self._install_and_upgrade('1.6.5', True, True, 1, False, -1, 1)

    def test_multiple_node_rolling_upgrade_1_6_5_1(self):
        self._install_and_upgrade('1.6.5.1', True, True, 1, False, -1, 1)

    def test_multiple_node_rolling_upgrade_1_6_5_2(self):
        self._install_and_upgrade('1.6.5.2', True, True, 1, False, -1, 1)






        #for


    #let's install 1.6.5.3
    #do some bucket/init related operation
    #now only option x nodes
    #power on upgraded ones first and then the non-upgraded ones
    #start everything and wait for some time

    #return "
    def _install_and_upgrade(self, initial_version='1.6.5.3',
                             create_buckets=False,
                             insert_data=False,
                             upgrade_how_many=1,
                             start_upgraded_first=True,
                             load_ratio=-1,
                             how_many_roll_upgrade=0):
        node_upgrade_status = {}
        #then start them in whatever order you want
        inserted_keys = []
        log = logger.Logger.get_logger()
        log.info("how_many_roll_upgrade : {0}".format(how_many_roll_upgrade))
        input = TestInputSingleton.input
        rest_settings = input.membase_settings
        servers = input.servers
        builds, changes = BuildQuery().get_all_builds()

        for server in servers[:len(servers) - how_many_roll_upgrade]:
            remote = RemoteMachineShellConnection(server)
            rest = RestConnection(server)
            info = remote.extract_remote_info()
            older_build = BuildQuery().find_membase_build(builds=builds,
                                                              deliverable_type=info.deliverable_type,
                                                              os_architecture=info.architecture_type,
                                                              build_version=initial_version,
                                                              product='membase-server-enterprise')

            remote.membase_uninstall()
            remote.execute_command('/etc/init.d/membase-server stop')
            remote.download_build(older_build)
            #now let's install ?
            remote.membase_install(older_build)
            RestHelper(rest).is_ns_server_running(120)
            log.info("sleep for 10 seconds to wait for membase-server to start...")
            rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
            rest.init_cluster_memoryQuota()
            node_upgrade_status[server] = "installed"
            remote.disconnect()

        master = servers[0]
        if create_buckets:
            #let's create buckets
            #wait for the bucket
            #bucket port should also be configurable , pass it as the
            #parameter to this test ? later

            BucketOperationHelper.create_default_buckets(servers=[master],
                                                         assert_on_test=self)


            RebalanceHelper.wait_for_stats(master, "default", 'ep_queue_size', 0)
            RebalanceHelper.wait_for_stats(master, "default", 'ep_flusher_todo', 0)
            ready = BucketOperationHelper.wait_for_memcached(master, "default")
            self.assertTrue(ready, "wait_for_memcached failed")

            if insert_data:
                #let's insert some data
                if load_ratio == -1:
                    #let's load 0.1 data
                    load_ratio = 0.1
                distribution = {1024: 0.5, 20: 0.5}
                inserted_keys, rejected_keys =\
                MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[master],
                                                                      port=11211,
                                                                      ram_load_ratio=load_ratio,
                                                                      number_of_threads=1,
                                                                      value_size_distribution=distribution)
                log.info("wait until data is completely persisted on the disk")

        ClusterOperationHelper.add_all_nodes_or_assert(master,
                                                       servers[:len(servers) - how_many_roll_upgrade],
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

        filtered_builds = []
        for build in builds:
            if build.deliverable_type == info.deliverable_type and\
               build.architecture_type == info.architecture_type:
                filtered_builds.append(build)
        sorted_builds = BuildQuery().sort_builds_by_version(filtered_builds)
        latest_version = sorted_builds[0].product_version
        #if we dont want to do roll_upgrade ?
        if how_many_roll_upgrade < 1:
            for server in servers:
                remote = RemoteMachineShellConnection(server)
                remote.stop_membase()
                remote.disconnect()

        if how_many_roll_upgrade > 0:
            #install 1.7.0 on the last nodes
            for server in servers[len(servers) - how_many_roll_upgrade:]:
                remote = RemoteMachineShellConnection(server)
                info = remote.extract_remote_info()
                log.info("finding build {0} for machine {1}".format(latest_version, server))
                appropriate_build = BuildQuery().find_membase_build(builds=builds,
                                                                    product='membase-server-enterprise',
                                                                    build_version=latest_version,
                                                                    deliverable_type=info.deliverable_type,
                                                                    os_architecture=info.architecture_type)
                self.assertTrue(appropriate_build.url, msg="unable to find build for this machine {0}".format(server))
                remote.membase_uninstall()
                remote.download_build(appropriate_build)

                remote.membase_install(appropriate_build)
                RestHelper(rest).is_ns_server_running(120)
                log.info("sleep for 10 seconds to wait for membase-server to start...")
                rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
                rest    .init_cluster_memoryQuota()
                remote.disconnect()
                #now add these nodes to the cluster

            for server in servers[len(servers) - how_many_roll_upgrade:]:
                #add this to the cluster
                ClusterOperationHelper.add_all_nodes_or_assert(master, [server], rest_settings, self)
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

        else:
            count = 0
            for server in servers:
                remote = RemoteMachineShellConnection(server)
                info = remote.extract_remote_info()
                log.info("finding build {0} for machine {1}".format(latest_version, server))
                version = input.test_params['version']
                #pick the first one in the list
                appropriate_build = BuildQuery().find_membase_build(builds,
                                             'membase-server-enterprise',
                                             info.deliverable_type,
                                             info.architecture_type,
                                             version.strip())
                remote.download_build(appropriate_build)
                remote.membase_upgrade(appropriate_build)
                RestHelper(RestConnection(server)).is_ns_server_running(120)

                pools_info = RestConnection(server).get_pools_info()

                node_upgrade_status[server] = "upgraded"
                count += 1
                if upgrade_how_many == count:
                    break
                remote.disconnect()

                #verify admin_creds still set

                self.assertTrue(pools_info['implementationVersion'], appropriate_build.product_version)

            if start_upgraded_first:
                for server in node_upgrade_status:
                    if node_upgrade_status[server] == "upgraded":
                        remote = RemoteMachineShellConnection(server)
                        remote.start_membase()
                        remote.disconnect()
            else:
                for server in node_upgrade_status:
                    if node_upgrade_status[server] == "installed":
                        remote = RemoteMachineShellConnection(server)
                        remote.start_membase()
                        remote.disconnect()

            for server in servers:
                remote = RemoteMachineShellConnection(server)
                remote.start_membase()
                remote.disconnect()



            #TODO: how can i verify that the cluster init config is preserved
            if create_buckets:
                self.assertTrue(BucketOperationHelper.wait_for_bucket_creation('default', RestConnection(master)),
                                msg="bucket 'default' does not exist..")
            if insert_data:
                BucketOperationHelper.keys_exist_or_assert(keys=inserted_keys,
                                                           server=master,
                                                           name='default',
                                                           test=self)
        return node_upgrade_status