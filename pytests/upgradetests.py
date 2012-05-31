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
    if version.startswith("1.6") or version.startswith("1.7"):
        product = 'membase-server-enterprise'
    else:
        product = 'couchbase-server-enterprise'

    if result is None:
        appropriate_build = BuildQuery().\
            find_membase_release_build(product, info.deliverable_type,
                info.architecture_type, version.strip(), is_amazon=is_amazon)
    else:
        appropriate_build = BuildQuery().\
            find_membase_build(builds, product, info.deliverable_type,
                info.architecture_type, version.strip(), is_amazon=is_amazon)

    return appropriate_build


def _create_load_multiple_bucket(self, server, bucket_data, howmany=2):
    created = BucketOperationHelper.create_multiple_buckets(server,
        1, howmany=howmany)
    self.assertTrue(created, "unable to create multiple buckets")
    rest = RestConnection(server)
    buckets = rest.get_buckets()
    for bucket in buckets:
        bucket_data[bucket.name] = {}
        ready = BucketOperationHelper.wait_for_memcached(server, bucket.name)
        self.assertTrue(ready, "wait_for_memcached failed")
        #let's insert some data
        distribution = {2 * 1024: 0.5, 20: 0.5}
        bucket_data[bucket.name]["inserted_keys"],\
            bucket_data[bucket.name]["reject_keys"] =\
        MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[server],
            name=bucket.name, ram_load_ratio=2.0, number_of_threads=2,
            value_size_distribution=distribution, write_only=True,
            moxi=True)
        RebalanceHelper.wait_for_stats(server, bucket.name, 'ep_queue_size', 0)
        RebalanceHelper.wait_for_stats(server, bucket.name, 'ep_flusher_todo',
            0)


class SingleNodeUpgradeTests(unittest.TestCase):
    #test descriptions are available http://techzone.couchbase.com/wiki/display/membase/Test+Plan+1.7.0+Upgrade

    def _install_and_upgrade(self, initial_version='1.6.5.3',
                             initialize_cluster=False,
                             create_buckets=False,
                             insert_data=False):
        input = TestInputSingleton.input
        rest_settings = input.membase_settings
        servers = input.servers
        server = servers[0]
        save_upgrade_config = False
        if initial_version.startswith("1.7") and input.test_params['version'].startswith("1.8"):
            save_upgrade_config = True
        is_amazon = False
        if input.test_params.get('amazon', False):
            is_amazon = True
        if initial_version.startswith("1.6") or initial_version.startswith("1.7"):
            product = 'membase-server-enterprise'
        else:
            product = 'couchbase-server-enterprise'
        remote = RemoteMachineShellConnection(server)
        rest = RestConnection(server)
        info = remote.extract_remote_info()
        remote.membase_uninstall()
        remote.couchbase_uninstall()
        builds, changes = BuildQuery().get_all_builds()
        # check to see if we are installing from latestbuilds or releases
        # note: for newer releases (1.8.0) even release versions can have the
        #  form 1.8.0r-55
        if re.search('r', initial_version):
            builds, changes = BuildQuery().get_all_builds()
            older_build = BuildQuery().find_membase_build(builds, deliverable_type=info.deliverable_type,
                                                          os_architecture=info.architecture_type,
                                                          build_version=initial_version,
                                                          product=product, is_amazon=is_amazon)
        else:
            older_build = BuildQuery().find_membase_release_build(deliverable_type=info.deliverable_type,
                                                                  os_architecture=info.architecture_type,
                                                                  build_version=initial_version,
                                                                  product=product, is_amazon=is_amazon)
        remote.stop_membase()
        remote.stop_couchbase()
        remote.download_build(older_build)
        #now let's install ?
        remote.membase_install(older_build)
        RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
        rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
        bucket_data = {}
        if initialize_cluster:
            rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
            if create_buckets:
                _create_load_multiple_bucket(self, server, bucket_data, howmany=2)
        version = input.test_params['version']

        appropriate_build = _get_build(servers[0], version, is_amazon=is_amazon)
        self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(version))

        remote.download_build(appropriate_build)
        remote.membase_upgrade(appropriate_build, save_upgrade_config=save_upgrade_config)
        remote.disconnect()
        RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)

        pools_info = rest.get_pools_info()

        rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
        time.sleep(TIMEOUT_SECS)
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
                    BucketOperationHelper.keys_exist_or_assert(bucket_data[bucket.name]["inserted_keys"],
                                                               server,
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

    def test_single_node_upgrade_s1_1_7_0(self):
        self._install_and_upgrade(initial_version='1.7.0',
                                  initialize_cluster=False,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s2_1_7_0(self):
        self._install_and_upgrade(initial_version='1.7.0',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s3_1_7_0(self):
        self._install_and_upgrade(initial_version='1.7.0',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=True)

    def test_single_node_upgrade_s4_1_7_0(self):
        self._install_and_upgrade(initial_version='1.7.0',
                                  initialize_cluster=True,
                                  insert_data=True,
                                  create_buckets=True)

    def test_single_node_upgrade_s1_1_7_1(self):
        self._install_and_upgrade(initial_version='1.7.1',
                                  initialize_cluster=False,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s2_1_7_1(self):
        self._install_and_upgrade(initial_version='1.7.1',
                                  initialize_cluster=True,
                                  insert_data=False,
                                  create_buckets=False)

    def test_single_node_upgrade_s3_1_7_1(self):
        self._install_and_upgrade(initial_version='1.7.1',
                                  initialize_cluster=True,
                                  insert_data=False,
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

        #TODO : expect a message like 'package membase-server-1.7~basestar-1.x86_64 is already installed'

    def test_upgrade(self):
        # singlenode paramaterized install
        input = TestInputSingleton.input
        initial_version = input.param('initial_version', '1.6.5.3')
        initialize_cluster = input.param('initialize_cluster', True)
        create_buckets = input.param('create_buckets', True)
        insert_data = input.param('insert_data', True)
        self._install_and_upgrade(initial_version=initial_version,
                                  initialize_cluster=initialize_cluster,
                                  create_buckets=create_buckets,
                                  insert_data=insert_data)


class MultipleNodeUpgradeTests(unittest.TestCase):
    #in a 3 node cluster with no buckets shut down all the nodes update all
    # nodes one by one and then restart node(1),node(2) and node(3)
    def test_multiple_node_upgrade_m1_1_6_5_3(self):
        self._install_and_upgrade('1.6.5.3', False, False, True)

    #in a 3 node cluster with default bucket without any keys shut down all the nodes update
    # all nodes one by one and then restart node(1),node(2) and node(3)
    def test_multiple_node_upgrade_m2_1_6_5_3(self):
        self._install_and_upgrade('1.6.5.3', True, False)

        #in a 3 node cluster with default bucket with some keys shut down all the
        # nodes update all nodes one by one and then restart node(1),node(2) and node(3)

    def test_multiple_node_upgrade_m3_1_6_5_3(self):
        self._install_and_upgrade('1.6.5.3', True, True)

    #m3 with 50% ram full ?

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

    def test_multiple_node_upgrade_m5_1_6_5_3(self):
        self._install_and_upgrade('1.6.5.3', True, False, False)

    def test_multiple_node_upgrade_m1_1_7_0(self):
        self._install_and_upgrade('1.7.0', False, False, True)

    def test_multiple_node_upgrade_m2_1_7_0(self):
        self._install_and_upgrade('1.7.0', True, False)

    def test_multiple_node_upgrade_m3_1_7_0(self):
        self._install_and_upgrade('1.7.0', True, True)

    def test_multiple_node_upgrade_m5_1_7_0(self):
        self._install_and_upgrade('1.7.0', True, False, False)

    def test_m6_1_7_0(self):
        self._install_and_upgrade('1.7.0', True, True, True, 10)

    def test_multiple_node_upgrade_m1_1_7_1(self):
        self._install_and_upgrade('1.7.1', False, False, True)

    def test_multiple_node_upgrade_m2_1_7_1(self):
        self._install_and_upgrade('1.7.1', True, False)

    def test_multiple_node_upgrade_m3_1_7_1(self):
        self._install_and_upgrade('1.7.1', True, True)

    def test_multiple_node_upgrade_m5_1_7_1(self):
        self._install_and_upgrade('1.7.1', True, False, False)

    def test_m6_1_7_1(self):
        self._install_and_upgrade('1.7.1', True, True, True, 10)

    def test_m6_1_7_1_1(self):
        self._install_and_upgrade('1.7.1.1', True, True, True, 10)

    def test_m6_1_7_2(self):
        self._install_and_upgrade('1.7.2', True, True, True, 10)

    # Rolling Upgrades
    def test_multiple_node_rolling_upgrade_1_7_0(self):
        self._install_and_upgrade('1.7.0', True, True, False, -1, True)

    def test_multiple_node_rolling_upgrade_1_7_1(self):
        self._install_and_upgrade('1.7.1', True, True, False, -1, True)

    def test_multiple_node_rolling_upgrade_1_7_1_1(self):
        self._install_and_upgrade('1.7.1.1', True, True, False, -1, True)

    def test_multiple_node_rolling_upgrade_1_7_2(self):
        self._install_and_upgrade('1.7.2', True, True, False, -1, True)

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
        upgrade_path = ['1.7.0', '1.7.1.1', '1.7.2']
        self._install_and_upgrade('1.6.5.4', True, True, True, 10, False, upgrade_path)

    def test_multiple_version_upgrade_start_all_4(self):
        upgrade_path = ['1.7.0', '1.7.1.1', '1.7.2']
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

    def test_upgrade(self):
        # multinode paramaterized install
        input = TestInputSingleton.input
        initial_version = input.param('initial_version', '1.6.5.3')
        create_buckets = input.param('create_buckets', True)
        insert_data = input.param('insert_data', True)
        start_upgraded_first = input.param('start_upgraded_first', True)
        load_ratio = input.param('load_ratio', -1)
        online_upgrade = input.param('online_upgrade', False)
        upgrade_path = input.param('upgrade_path', [])
        do_new_rest = input.param('do_new_rest', False)
        if upgrade_path:
            upgrade_path = upgrade_path.split(",")

        self._install_and_upgrade(initial_version=initial_version,
                                  create_buckets=create_buckets,
                                  insert_data=insert_data,
                                  start_upgraded_first=start_upgraded_first,
                                  load_ratio=load_ratio,
                                  roll_upgrade=online_upgrade,
                                  upgrade_path=upgrade_path,
                                  do_new_rest=do_new_rest)

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
                             upgrade_path=[],
                             do_new_rest=False):
        node_upgrade_path = []
        node_upgrade_path.extend(upgrade_path)
        #then start them in whatever order you want
        inserted_keys = []
        log = logger.Logger.get_logger()
        if roll_upgrade:
            log.info("performing an online upgrade")
        input = TestInputSingleton.input
        rest_settings = input.membase_settings
        servers = input.servers
        save_upgrade_config = False
        is_amazon = False
        if input.test_params.get('amazon', False):
            is_amazon = True
        if initial_version.startswith("1.6") or initial_version.startswith("1.7"):
            product = 'membase-server-enterprise'
        else:
            product = 'couchbase-server-enterprise'
        # install older build on all nodes
        for server in servers:
            remote = RemoteMachineShellConnection(server)
            rest = RestConnection(server)
            info = remote.extract_remote_info()
            # check to see if we are installing from latestbuilds or releases
            # note: for newer releases (1.8.0) even release versions can have the
            #  form 1.8.0r-55
            if re.search('r', initial_version):
                builds, changes = BuildQuery().get_all_builds()
                older_build = BuildQuery().find_membase_build(builds, deliverable_type=info.deliverable_type,
                                                              os_architecture=info.architecture_type,
                                                              build_version=initial_version,
                                                              product=product, is_amazon=is_amazon)

            else:
                older_build = BuildQuery().find_membase_release_build(deliverable_type=info.deliverable_type,
                                                                      os_architecture=info.architecture_type,
                                                                      build_version=initial_version,
                                                                      product=product, is_amazon=is_amazon)

            remote.membase_uninstall()
            remote.couchbase_uninstall()
            remote.stop_membase()
            remote.stop_couchbase()
            remote.download_build(older_build)
            #now let's install ?
            remote.membase_install(older_build)
            RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
            rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
            rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
            remote.disconnect()

        bucket_data = {}
        master = servers[0]
        if create_buckets:
            #let's create buckets
            #wait for the bucket
            #bucket port should also be configurable , pass it as the
            #parameter to this test ? later

            self._create_default_bucket(master)
            inserted_keys = self._load_data(master, load_ratio)
            _create_load_multiple_bucket(self, master, bucket_data, howmany=2)

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

        if initial_version == "1.7.0" or initial_version == "1.7.1":
            self._save_config(rest_settings, master)

        input_version = input.test_params['version']
        node_upgrade_path.append(input_version)
        current_version = initial_version
        previous_version = current_version
        #if we dont want to do roll_upgrade ?
        log.info("Upgrade path: {0} -> {1}".format(initial_version, node_upgrade_path))
        log.info("List of servers {0}".format(servers))
        if not roll_upgrade:
            for version in node_upgrade_path:
                previous_version = current_version
                current_version = version
                if version != initial_version:
                    log.info("Upgrading to version {0}".format(version))
                    self._stop_membase_servers(servers)
                    if previous_version.startswith("1.7") and current_version.startswith("1.8"):
                        save_upgrade_config = True
                    # No need to save the upgrade config from 180 to 181
                    if previous_version.startswith("1.8.0") and current_version.startswith("1.8.1"):
                        save_upgrade_config = False
                    appropriate_build = _get_build(servers[0], version, is_amazon=is_amazon)
                    self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(version))
                    for server in servers:
                        remote = RemoteMachineShellConnection(server)
                        remote.download_build(appropriate_build)
                        remote.membase_upgrade(appropriate_build, save_upgrade_config=save_upgrade_config)
                        RestHelper(RestConnection(server)).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)

                        #verify admin_creds still set
                        pools_info = RestConnection(server).get_pools_info()
                        self.assertTrue(pools_info['implementationVersion'], appropriate_build.product_version)

                        if start_upgraded_first:
                            log.info("Starting server {0} post upgrade".format(server))
                            remote.start_membase()
                        else:
                            remote.stop_membase()

                        remote.disconnect()
                    if not start_upgraded_first:
                        log.info("Starting all servers together")
                        self._start_membase_servers(servers)
                    time.sleep(TIMEOUT_SECS)
                    if version == "1.7.0" or version == "1.7.1":
                        self._save_config(rest_settings, master)

                    if create_buckets:
                        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation('default', RestConnection(master)),
                                        msg="bucket 'default' does not exist..")
                    if insert_data:
                        self._verify_data(master, rest, inserted_keys)

        # rolling upgrade
        else:
            version = input.test_params['version']
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
                remote.download_build(appropriate_build)
                # if initial version is 180
                # Don't uninstall the server
                if not initial_version.startswith('1.8.0'):
                    remote.membase_uninstall()
                    remote.couchbase_uninstall()
                    remote.membase_install(appropriate_build)
                else:
                    remote.membase_upgrade(appropriate_build)

                RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
                log.info("sleep for 10 seconds to wait for membase-server to start...")
                time.sleep(TIMEOUT_SECS)
                rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                remote.disconnect()

                #readd this to the cluster
                ClusterOperationHelper.add_all_nodes_or_assert(master, [server], rest_settings, self)
                nodes = rest.node_statuses()
                otpNodeIds = []
                for node in nodes:
                    otpNodeIds.append(node.id)
                # Issue rest call to the newly added node
                # MB-5108
                if do_new_rest:
                    master = server
                    rest = RestConnection(master)
                rebalanceStarted = rest.rebalance(otpNodeIds, [])
                self.assertTrue(rebalanceStarted,
                                "unable to start rebalance on master node {0}".format(master.ip))
                log.info('started rebalance operation on master node {0}'.format(master.ip))
                rebalanceSucceeded = rest.monitorRebalance()
                self.assertTrue(rebalanceSucceeded,
                                "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))

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

    def _save_config(self, rest_settings, server):
        log = logger.Logger.get_logger()
        #TODO: Use rest_client instead
        remote = RemoteMachineShellConnection(server)
        cmd = "wget -O- -q --post-data='rpc:eval_everywhere(ns_config, resave, []).' --user={0} --password={1} \
        http://localhost:8091/diag/eval".format(rest_settings.rest_username, rest_settings.rest_password)
        log.info('Executing command to save config {0}'.format(cmd))
        remote.execute_command(cmd, debug=True)
        remote.disconnect()

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
