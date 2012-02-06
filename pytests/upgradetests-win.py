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

    if result is None:
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
        rest_settings = input.membase_settings
        servers = input.servers
        server = servers[0]
        save_upgrade_config = False
        if re.search('1.8',input.test_params['version']):
            save_upgrade_config = True
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
            if older_build.product == 'membase-server-enterprise':
                abbr_product = "mb"
            else:
                abbr_product = "cb"
            remote.download_binary_in_win(older_build.url, abbr_product, initial_version)

            print older_build.url
            print older_build.deliverable_type
            remote.membase_install_win(older_build, initial_version)
            RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
            rest.init_cluster_port(rest_settings.rest_username, rest_settings.rest_password)
            bucket_data = {}
            if initialize_cluster:
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                if create_buckets:
                    print "stop insert keys for debug :) "
                    _create_load_multiple_bucket(self, server, bucket_data, howmany=2)
            version = input.test_params['version']
            if version == "1.8.0":
                abbr_product = "cb"
                version = "1.8.0r-55-g80f24f2"
            appropriate_build = _get_build(servers[0], version, is_amazon=is_amazon)
            self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(version))
            remote.download_binary_in_win(appropriate_build.url, abbr_product, version)
            remote.stop_membase()
            log.info("###### START UPGRADE. #########")
            remote.membase_upgrade_win(info.architecture_type, info.windows_name, version)
            log.info("THIS IS END OF UPGRADE")
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

