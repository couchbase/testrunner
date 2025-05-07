import os
import re
import shutil
import testconstants
import gc
import sys, json
import traceback
import queue
import struct
import base64
import time
import copy
import json
import random
import threading



from threading import Thread
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedError
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.document import DesignDocument, View
from couchbase_helper.documentgenerator import BlobGenerator

from TestInput import TestInputSingleton
from lib.Cb_constants.CBServer import CbServer
from pytests.fts.fts_base import QUERY
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.x509_multiple_CA_util import x509main
from scripts.install import InstallerJob
from builds.build_query import BuildQuery
from query_tests_helper import QueryHelperTests
from couchbase_helper.tuq_generators import JsonGenerator
from fts.fts_base import FTSIndex, FTSBaseTest
from pytests.fts.fts_callable import FTSCallable
from pprint import pprint
from testconstants import CB_REPO, CB_RELEASE_BUILDS
from testconstants import MV_LATESTBUILD_REPO
from testconstants import CB_VERSION_NAME
from testconstants import FUTURE_BUILD_NUMBER
from pytests.fts.fts_backup_restore import FTSIndexBackupClient
from pytests.security.rbac_base import RbacBase
from pytests.tuqquery.n1ql_callable import N1QLCallable

from pytests.fts.vector_dataset_generator.vector_dataset_loader import GoVectorLoader, VectorLoader
from pytests.fts.vector_dataset_generator.vector_dataset_generator import VectorDataset
from lib.membase.api.exception import FTSException
from lib.membase.api.on_prem_rest_client import RestConnection

try:
    from lib.sdk_client import SDKClient
except:
    from lib.sdk_client3 import SDKClient




class NewUpgradeBaseTest(BaseTestCase):
    def setUp(self):
        super(NewUpgradeBaseTest, self).setUp()

        self.start_version = self.input.param('initial_version', '2.5.1-1083')
        self.run_partition_validation = self.input.param("run_partition_validation", False)
        self.vector_upgrade = self.input.param("vector_upgrade",False)
        self.fts_upgrade = self.input.param("fts_upgrade",False)
        self.isRebalanceComplete = False
        self.skip_partition_check_inbetween = self.input.param("skip_partition_check_inbetween", True)
        self.vector_flag = self.input.param("vector_search_test", False)
        self.fts_quota = self.input.param("fts_quota", 600)
        self.passed = True
        self.inbetween_active = False
        self.inbetween_tests = 0
        self.vector_queries_count = self.input.param("vector_queries_count", 5)
        self.run_n1ql_search_function = self.input.param("run_n1ql_search_function", True)
        self.k = self.input.param("k", 2)
        self.partition_list = eval(self.input.param("partition_list","[7,3,2]"))
        self.load_data_pause = self.input.param("load_data_pause",80)


        self.upgrade_type = self.input.param("upgrade_type", "online")
        self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                      "knn": [{"field": "vector_data", "k": self.k,
                               "vector": []}]}
        self.expected_accuracy_and_recall = self.input.param("expected_accuracy_and_recall", 70)

        self.fts_indexes_store = None

        self.fts_indexes_persist = []

        self.skip_validation_if_no_query_hits = self.input.param("skip_validation_if_no_query_hits", True)
        self.validate_memory_leak = self.input.param("validate_memory_leak", False)


        self.log.info("==============  NewUpgradeBaseTest setup has started ==============")
        self.released_versions = ["2.0.0-1976-rel", "2.0.1", "2.5.0", "2.5.1",
                                  "2.5.2", "3.0.0", "3.0.1",
                                  "3.0.1-1444", "3.0.2", "3.0.2-1603", "3.0.3",
                                  "3.1.0", "3.1.0-1776", "3.1.1", "3.1.1-1807",
                                  "3.1.2", "3.1.2-1815", "3.1.3", "3.1.3-1823",
                                  "4.0.0", "4.0.0-4051", "4.1.0", "4.1.0-5005",
                                  "4.5.0", "4.5.0-2601", "4.5.1", "4.5.1-2817",
                                  "4.6.0", "4.6.0-3573", '4.6.2', "4.6.2-3905"]
        self.use_hostnames = self.input.param("use_hostnames", False)
        self.product = self.input.param('product', 'couchbase-server')
        self.initial_version = self.input.param('initial_version', '2.5.1-1083')
        self.initial_vbuckets = self.input.param('initial_vbuckets', 1024)
        self.upgrade_versions = self.input.param('upgrade_version', '2.0.1-170-rel')
        self.upgrade_to_future_build = self.input.param('upgrade_to_future_build', False)
        self.travel_sample_bucket = self.input.param("travel_sample_bucket", False)
        self.call_ftsCallable = True
        self.upgrade_versions = self.upgrade_versions.split(";")
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.debug_logs = self.input.param("debug_logs", False)
        self.init_nodes = self.input.param('init_nodes', True)

        self.is_downgrade = self.input.param('downgrade', False)
        if self.is_downgrade:
            self.initial_version, self.upgrade_versions = self.upgrade_versions[0], [self.initial_version]

        upgrade_path = self.input.param('upgrade_path', [])
        if upgrade_path:
            upgrade_path = upgrade_path.split(",")
        self.upgrade_versions = upgrade_path + self.upgrade_versions
        if self.input.param('released_upgrade_version', None) is not None:
            self.upgrade_versions = [self.input.param('released_upgrade_version', None)]

        self.initial_build_type = self.input.param('initial_build_type', None)
        self.start_upgrade_server = False
        self.ce_to_ee = self.input.param('ce_to_ee', False)
        self.stop_persistence = self.input.param('stop_persistence', False)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
        self.cb_bucket_name = self.input.param('cb_bucket_name', 'travel-sample')
        self.cb_server_ip = self.input.param("cb_server_ip", None)
        self.cbas_bucket_name = self.input.param('cbas_bucket_name', 'travel')
        self.cbas_dataset_name = self.input.param("cbas_dataset_name", 'travel_ds')
        self.cbas_dataset_name_invalid = self.input.param('cbas_dataset_name_invalid', self.cbas_dataset_name)
        self.cbas_bucket_name_invalid = self.input.param('cbas_bucket_name_invalid', self.cbas_bucket_name)
        self.num_index_replicas = self.input.param("num_index_replica", 0)
        self.num_indexes = self.input.param("num_indexes", 3)
        self.expected_err_msg = self.input.param("expected_err_msg", None)
        self.rest_settings = self.input.membase_settings
        self.multi_nodes_services = False
        self.rest = None
        self.rest_helper = None
        self.is_ubuntu = False
        self.is_rpm = False
        self.is_centos7 = False
        self.sleep_time = 15
        self.ddocs = []
        self.item_flag = self.input.param('item_flag', 0)
        self.expire_time = self.input.param('expire_time', 0)
        self.wait_expire = self.input.param('wait_expire', False)
        self.default_view_name = "upgrade-test-view"
        self.ddocs_num = self.input.param("ddocs_num", 1)
        self.view_num = self.input.param("view_per_ddoc", 2)
        self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
        self.offline_failover_upgrade = self.input.param("offline_failover_upgrade", False)

        self.use_rest = self.input.param("use_rest", True)
        self.use_cli = self.input.param("use_cli", False)
        self.custom_scopes = self.input.param("custom_scopes", False)
        self.custom_collections = self.input.param("custom_collections", False)
        self.map_data_collection = self.input.param("map_data_collection", False)
        self.col_per_scope = self.input.param("col_per_scope", 1)
        self.load_to_collection = self.input.param("load_to_collection", False)
        self.collection_string = self.input.param("collection_string", None)
        self.non_ascii_name = self.input.param("non_ascii_name", None)
        self.create_scopes = self.input.param("create_scopes", True)
        self.num_scopes = self.input.param("num_scopes", 2)
        self.create_collections = self.input.param("create_collections", True)
        self.relative_path = self.input.param("relative_path", False)
        self.buckets_only = self.input.param("buckets_only", False)
        self.scopes_only = self.input.param("scopes_only", False)
        self.collections_only = self.input.param("collections_only", False)
        self.drop_scopes = self.input.param("drop_scopes", False)
        self.drop_collections = self.input.param("drop_collections", False)
        self.check_scopes = self.input.param("check_scopes", False)
        self.check_scopes_details = self.input.param("check_scopes_details", False)
        self.check_collections = self.input.param("check_collections", False)
        self.check_collections_details = self.input.param("check_collections_details", False)
        self.check_collection_enable = self.input.param("check_collection_enable", True)
        self.json_output = self.input.param("json_output", False)
        self.load_json_format = self.input.param("load_json_format", False)
        self.load_to_scopes = self.input.param("load_to_scopes", False)
        self.load_to_collections = self.input.param("load_to_collections", False)
        self.load_to_default_scopes = self.input.param("load_to_scopes", False)
        self.cbwg_no_value_in_col_flag = self.input.param("cbwg_no_value_in_col_flag", False)
        self.cbwg_invalid_value_in_col_flag = self.input.param("cbwg_invalid_value_in_col_flag", False)
        self.load_to_default_collections = self.input.param("load_to_collections", False)
        self.create_existing_scope = self.input.param("create_existing_scope", False)
        self.create_existing_collection = self.input.param("create_existing_collection", False)
        self.block_char = self.input.param("block_char", "")
        self.drop_default_scope = self.input.param("drop_default_scope", False)
        self.drop_default_collection = self.input.param("drop_default_collection", False)
        self.log_filename = self.input.param("filename", "loginfo_collecion")
        self.node_down = self.input.param("node_down", False)
        self.scan_consistency = self.input.param("scan_consistency", "request_plus")

        self.load_scope_id = ""
        self.load_scope = ""
        self.load_collection_id = ""
        self.scopes = None
        self.collections = None
        self.upgrade_master_node = None
        self.n1ql = QueryHelperTests

        self.during_ops = None
        if "during-ops" in self.input.test_params:
            self.during_ops = self.input.param("during-ops", None).split(",")
        if self.initial_version.startswith("1.6") or self.initial_version.startswith("1.7"):
            self.product = 'membase-server'
        else:
            self.product = 'couchbase-server'
        self.index_replicas = self.input.param("index_replicas", None)
        self.index_kv_store = self.input.param("kvstore", None)
        self.partitions_per_pindex = \
            self.input.param("max_partitions_pindex", 32)
        self.dataset = self.input.param("dataset", "emp")
        self.expiry = self.input.param("expiry", 0)
        self.create_ops_per = self.input.param("create_ops_per", 0)
        self.expiry_ops_per = self.input.param("expiry_ops_per", 0)
        self.delete_ops_per = self.input.param("delete_ops_per", 0)
        self.update_ops_per = self.input.param("update_ops_per", 0)
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.batch_size = self.input.param("batch_size", 1000)
        self.doc_ops = self.input.param("doc_ops", False)
        if self.doc_ops:
            self.ops_dist_map = self.calculate_data_change_distribution(
                create_per=self.create_ops_per, update_per=self.update_ops_per,
                delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                start=0, end=self.docs_per_day)
            self.log.info(self.ops_dist_map)
            self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
            # self.full_docs_list_after_ops = self.generate_full_docs_list_after_ops(self.docs_gen_map)
        if self.max_verify is None:
            self.max_verify = min(self.num_items, 100000)
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        os_version = shell.extract_remote_info().distribution_version
        shell.disconnect()
        if type.lower() == 'windows':
            self.is_linux = False
        else:
            self.is_linux = True
        if type.lower() == "ubuntu":
            self.is_ubuntu = True
        if type.lower() == "centos":
            self.is_rpm = True
            if os_version.lower() == "centos 7":
                self.is_centos7 = True
        self.queue = queue.Queue()
        self.upgrade_servers = []
        self.index_replicas = self.input.param("index_replicas", None)
        self.partitions_per_pindex = \
            self.input.param("max_partitions_pindex", 32)
        self.index_kv_store = self.input.param("kvstore", None)
        self.fts_obj = None
        self.validate_system_event_logs = TestInputSingleton.input.param("validate_sys_event_logs", False)
        if self.validate_system_event_logs:
            self.system_events.set_test_start_time()
        self.log.info("==============  NewUpgradeBaseTest setup has completed ==============")


    def tearDown(self):
        self.product = self.input.param('product', 'couchbase-server')
        self.initial_version = self.input.param('initial_version', '6.6.2-9600')
        self.initial_vbuckets = self.input.param('initial_vbuckets', 1024)
        self.upgrade_versions = self.input.param('upgrade_version', '7.0.0-5075')
        self.debug_logs = self.input.param("debug_logs", False)
        self.init_nodes = self.input.param('init_nodes', True)
        self.initial_build_type = self.input.param('initial_build_type', None)
        self.use_hostnames = self.input.param("use_hostnames", False)
        if CbServer.use_https:
            self.log.info('###################### Disabling n2n encryption')
            ntonencryptionBase().disable_nton_cluster([self.master])
        try:
            if CbServer.multiple_ca:
                CbServer.use_client_certs = False
                CbServer.cacert_verify = False
                self.x509 = x509main(host=self.master)
                self.x509.teardown_certs(servers=TestInputSingleton.input.servers)
                self.use_https = False
                CbServer.use_https = False
        except Exception as e:
            self.log.info(e)

        self.log.info("==============  NewUpgradeBaseTest tearDown has started ==============")
        test_failed = (hasattr(self, '_resultForDoCleanups') and \
                       len(self._resultForDoCleanups.failures or \
                           self._resultForDoCleanups.errors)) or \
                      (hasattr(self, '_exc_info') and \
                       self._exc_info()[1] is not None)
        if test_failed and self.skip_cleanup:
            self.log.warning("CLEANUP WAS SKIPPED DUE TO FAILURES IN UPGRADE TEST")
            self.cluster.shutdown(force=True)
            self.log.info("Test Input params were:")
            pprint(self.input.test_params)

            if self.input.param('BUGS', False):
                self.log.warning("Test failed. Possible reason is: {0}"
                                 .format(self.input.param('BUGS', False)))
        else:
            if not hasattr(self, 'rest'):
                return
            try:
                # cleanup only nodes that are in cluster
                # not all servers have been installed
                if self.rest is None:
                    self._new_master(self.master)
                nodes = self.rest.get_nodes()
                temp = []
                for server in self.servers:
                    if server.ip in [node.ip for node in nodes]:
                        temp.append(server)
                self.servers = temp
            except Exception as e:
                if e:
                    print("Exception ", e)
                self.cluster.shutdown(force=True)
                self.fail(e)
            super(NewUpgradeBaseTest, self).tearDown()
            if self.upgrade_servers:
                self._install(self.upgrade_servers)
                self.servers = self.upgrade_servers
        self.sleep(20, "sleep 20 seconds before run next test")
        self.log.info("==============  NewUpgradeBaseTest tearDown has completed ==============")

    def rebalance_load_setup_update(self,bucket,scope,collection):
        self.target_bucket = bucket
        self.target_scope = scope
        self.target_collection = collection
        

    def _install(self, servers):
        params = {}
        params['num_nodes'] = len(servers)
        params['product'] = self.product
        params['version'] = self.initial_version
        params['vbuckets'] = [self.initial_vbuckets]
        params['init_nodes'] = self.init_nodes
        params['debug_logs'] = self.debug_logs
        if self.initial_build_type is not None:
            if not self.start_upgrade_server:
                params['type'] = self.initial_build_type
            else:
                if self.ce_to_ee:
                    params['type'] = "enterprise"
                else:
                    params['type'] = self.initial_build_type
        if 5 <= int(self.initial_version[:1]) or 5 <= int(self.upgrade_versions[0][:1]):
            params['fts_query_limit'] = 10000000

        self.log.info("will install {0} on {1}" \
                      .format(self.initial_version, [s.ip for s in servers]))
        InstallerJob().parallel_install(servers, params)
        if self.product in ["couchbase", "couchbase-server", "cb"]:
            success = True
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                info = shell.extract_remote_info()
                success &= shell.is_couchbase_installed()
                self.sleep(5, "sleep 5 seconds to let cb up completely")
                ready = RestHelper(RestConnection(server)).is_ns_server_running(60)
                if not ready:
                    if "cento 7" in info.distribution_version.lower():
                        self.log.info("run systemctl daemon-reload")
                        shell.execute_command("systemctl daemon-reload", debug=False)
                        shell.start_server()
                    else:
                        self.log.error("Couchbase-server did not start...")
                shell.disconnect()
                if not success:
                    sys.exit("some nodes were not install successfully!")
        if self.rest is None:
            self._new_master(self.master)
        if self.use_hostnames:
            for server in self.servers[:self.nodes_init]:
                hostname = RemoteUtilHelper.use_hostname_for_server_settings(server)
                server.hostname = hostname

    def construct_custom_plan_params(self, replicas, partitions):
        plan_params = {}
        plan_params['numReplicas'] = replicas
        plan_params['indexPartitions'] = partitions
        return plan_params

    def initial_services(self, services=None):
        if services is not None:
            if "-" in services:
                set_services = services.split("-")
            elif "," in services:
                if self.multi_nodes_services:
                    set_services = services.split()
                else:
                    set_services = services.split(",")
        else:
            set_services = services
        return set_services

    def initialize_nodes(self, servers, services=None):
        set_services = self.initial_services(services)
        if 4.5 > float(self.initial_version[:3]):
            self.gsi_type = "forestdb"
        self.quota = self._initialize_nodes(self.cluster, servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port,
                                            services=set_services)

    def operations(self, servers, services=None):
        set_services = self.initial_services(services)

        if 4.5 > float(self.initial_version[:3]):
            self.gsi_type = "forestdb"
        self.quota = self._initialize_nodes(self.cluster, servers,
                                            self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled,
                                            self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers,
                                            self.maxParallelReplicaIndexers, self.port,
                                            services=set_services)

        if self.port and self.port != '8091':
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)
        if 5.0 <= float(self.initial_version[:3]):
            self.add_built_in_server_user()
        self.sleep(7, "wait to make sure node is ready")
        if len(servers) > 1:
            if services is None:
                self.cluster.rebalance([servers[0]], servers[1:], [],
                                       use_hostnames=self.use_hostnames)
            else:
                for i in range(1, len(set_services)):
                    self.cluster.rebalance([servers[0]], [servers[i]], [],
                                           use_hostnames=self.use_hostnames,
                                           services=[set_services[i]])
                    self.sleep(10)
        self.buckets = []
        gc.collect()
        if self.input.param('extra_verification', False):
            self.total_buckets += 2
            print(self.total_buckets)
        self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
        if self.dgm_run:
            self.bucket_size = 256
        self._bucket_creation()
        if self.stop_persistence:
            for server in servers:
                for bucket in self.buckets:
                    client = MemcachedClientHelper.direct_client(server, bucket)
                    client.stop_persistence()
            self.sleep(10)
        gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)
        if not self.stop_persistence:
            self._wait_for_stats_all_buckets(servers)
        else:
            for bucket in self.buckets:
                drain_rate = 0
                for server in servers:
                    client = MemcachedClientHelper.direct_client(server, bucket)
                    drain_rate += int(client.stats()["ep_queue_size"])
                self.sleep(3, "Pause to load all items")
                self.assertEqual(self.num_items * (self.num_replicas + 1), drain_rate,
                                 "Persistence is stopped, drain rate is incorrect %s. Expected %s" % (
                                     drain_rate, self.num_items * (self.num_replicas + 1)))
        self.change_settings()

    def _get_build(self, server, version, remote, is_amazon=False, info=None):
        if info is None:
            info = remote.extract_remote_info()
        build_repo = CB_REPO
        if version[:5] in CB_RELEASE_BUILDS.keys():
            build_repo = CB_REPO + CB_VERSION_NAME[version[:3]] + "/"
        builds, changes = BuildQuery().get_all_builds(version=version, timeout=self.wait_timeout * 5, \
                                                      deliverable_type=info.deliverable_type,
                                                      architecture_type=info.architecture_type, \
                                                      edition_type="couchbase-server-enterprise", repo=build_repo, \
                                                      distribution_version=info.distribution_version.lower())
        self.log.info("finding build %s for machine %s" % (version, server))

        if re.match(r'[1-9].[0-9].[0-9]-[0-9]+$', version):
            version = version + "-rel"
        if version[:5] in self.released_versions:
            appropriate_build = BuildQuery(). \
                find_couchbase_release_build('%s-enterprise' % (self.product),
                                             info.deliverable_type,
                                             info.architecture_type,
                                             version.strip(),
                                             is_amazon=is_amazon,
                                             os_version=info.distribution_version)
        else:
            appropriate_build = BuildQuery(). \
                find_build(builds, '%s-enterprise' % (self.product), info.deliverable_type,
                           info.architecture_type, version.strip())

        if appropriate_build is None:
            self.log.info(
                "builds are: %s \n. Remote is %s, %s. Result is: %s" % (builds, remote.ip, remote.username, version))
            raise Exception("Build %s for machine %s is not found" % (version, server))
        return appropriate_build

    def _upgrade(self, upgrade_version, server, queue=None, skip_init=False, info=None,
                 save_upgrade_config=False, fts_query_limit=None, debug_logs=False):
        try:
            if self.upgrade_to_future_build:
                tmp = upgrade_version.split("-")
                tmp[1] = str(int(tmp[1]) + FUTURE_BUILD_NUMBER)
                upgrade_version = "-".join(tmp)
            remote = RemoteMachineShellConnection(server)
            appropriate_build = self._get_build(server, upgrade_version, remote, info=info)
            self.assertTrue(appropriate_build.url,
                            msg="unable to find build {0}" \
                            .format(upgrade_version))
            download_file = remote.download_build(appropriate_build)
            if not download_file:
                self.sleep(10)
                self.log.info("Try to download again")
                download_file = remote.download_build(appropriate_build)
                if not download_file:
                    raise Exception("Build wasn't downloaded!")
            o, e = remote.couchbase_upgrade(appropriate_build,
                                            save_upgrade_config=False,
                                            forcefully=self.is_downgrade,
                                            fts_query_limit=fts_query_limit,
                                            debug_logs=debug_logs)
            remote.log_command_output(o, e)
            self.log.info("upgrade {0} to version {1} is completed"
                          .format(server.ip, upgrade_version))
            if 5.0 > float(self.initial_version[:3]) and self.is_centos7:
                remote.execute_command("systemctl daemon-reload")
                remote.start_server()
            # TBD: For now adding sleep as the server is not fully up and tests failing during py3
            self.sleep(30)
            self.rest = RestConnection(server)
            if self.is_linux:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
            else:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10, wait_if_warmup=True,
                                         check_service=True)
            if not skip_init:
                self.rest.init_cluster(self.rest_settings.rest_username, self.rest_settings.rest_password)
            self.sleep(self.sleep_time)
            remote.disconnect()
            self.sleep(30)
            return o, e
        except Exception as e:
            print(traceback.extract_stack())
            if queue is not None:
                queue.put(False)
                raise e
        if queue is not None:
            queue.put(True)

    def _async_update(self, upgrade_version, servers, queue=None, skip_init=False):
        self.log.info("\n\n*** servers {0} will be upgraded to {1} version ***\n".
                      format([server.ip for server in servers], upgrade_version))
        q = queue or self.queue
        upgrade_threads = []
        for server in servers:
            upgrade_thread = Thread(target=self._upgrade,
                                    name="upgrade_thread" + server.ip,
                                    args=(upgrade_version, server, q, skip_init))
            upgrade_threads.append(upgrade_thread)
            upgrade_thread.start()
        return upgrade_threads

    def _new_master(self, server):
        self.master = server
        self.rest = RestConnection(self.master)
        self.rest_helper = RestHelper(self.rest)

    def verification(self, servers, check_items=True):
        if self.use_hostnames:
            for server in servers:
                node_info = RestConnection(server).get_nodes_self()
                new_hostname = node_info.hostname
                self.assertEqual("%s:%s" % (server.hostname, server.port), new_hostname,
                                 "Hostname is incorrect for server %s. Settings are %s" % (server.ip, new_hostname))
        if self.master.ip != self.rest.ip or \
                self.master.ip == self.rest.ip and str(self.master.port) != str(self.rest.port):
            if self.port:
                self.master.port = self.port
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)
        if self.port and self.port != '8091':
            settings = self.rest.get_cluster_settings()
            if settings and 'port' in settings:
                self.assertTrue(self.port == str(settings['port']),
                                'Expected cluster port is %s, but is %s' % (self.port, settings['port']))
        for bucket in self.buckets:
            if not self.rest_helper.bucket_exists(bucket.name):
                raise Exception("bucket: %s not found" % bucket.name)
        self.verify_cluster_stats(servers, max_verify=self.max_verify, \
                                  timeout=self.wait_timeout * 20, check_items=check_items)

        if self.ddocs:
            self.verify_all_queries()
        if "update_notifications" in self.input.test_params:
            if self.rest.get_notifications() != self.input.param("update_notifications", True):
                self.fail("update notifications settings wasn't saved")
        if "autofailover_timeout" in self.input.test_params:
            if self.rest.get_autofailover_settings().timeout != self.input.param("autofailover_timeout", None):
                self.fail("autofailover settings wasn't saved")
        if "autofailover_alerts" in self.input.test_params:
            alerts = self.rest.get_alerts_settings()
            if alerts["recipients"] != ['couchbase@localhost']:
                self.fail("recipients value wasn't saved")
            if alerts["sender"] != 'root@localhost':
                self.fail("sender value wasn't saved")
            if alerts["emailServer"]["user"] != 'user':
                self.fail("email_username value wasn't saved")
            if alerts["emailServer"]["pass"] != '':
                self.fail("email_password should be empty for security")
        if "autocompaction" in self.input.test_params:
            cluster_status = self.rest.cluster_status()
            if cluster_status["autoCompactionSettings"]["viewFragmentationThreshold"] \
                    ["percentage"] != self.input.param("autocompaction", 50):
                self.log.info("Cluster status: {0}".format(cluster_status))
                self.fail("autocompaction settings weren't saved")

    def verify_all_queries(self, queue=None):
        query = {"connectionTimeout": 60000}
        expected_rows = self.num_items
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        if self.input.param("wait_expiration", False):
            expected_rows = 0
        self.log.info("Start verify queries...")
        for bucket in self.buckets:
            for ddoc in self.ddocs:
                prefix = ("", "dev_")[ddoc.views[0].dev_view]
                try:
                    self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name,
                                                query, bucket=bucket,
                                                wait_time=self.wait_timeout * 5,
                                                expected_rows=expected_rows,
                                                retry_time=10)
                except Exception as e:
                    print(e)
                    if queue is not None:
                        queue.put(False)
                if queue is not None:
                    queue.put(True)

    def change_settings(self):
        status = True
        if "update_notifications" in self.input.test_params:
            status &= self.rest.update_notifications(str(self.input.param("update_notifications", 'true')).lower())
        if "autofailover_timeout" in self.input.test_params:
            status &= self.rest.update_autofailover_settings(True, self.input.param("autofailover_timeout", None))
        if "autofailover_alerts" in self.input.test_params:
            status &= self.rest.set_alerts_settings('couchbase@localhost', 'root@localhost', 'user', 'pwd')
        if "autocompaction" in self.input.test_params:
            tmp, _, _ = self.rest.set_auto_compaction(viewFragmntThresholdPercentage=
                                                      self.input.param("autocompaction", 50))
            status &= tmp
            if not status:
                self.fail("some settings were not set correctly!")

    def warm_up_node(self, warmup_nodes=None):
        if not warmup_nodes:
            warmup_nodes = [self.servers[:self.nodes_init][-1], ]
        for warmup_node in warmup_nodes:
            shell = RemoteMachineShellConnection(warmup_node)
            shell.stop_couchbase()
            shell.disconnect()
        self.sleep(20)
        for warmup_node in warmup_nodes:
            shell = RemoteMachineShellConnection(warmup_node)
            shell.start_couchbase()
            shell.disconnect()
        ClusterOperationHelper.wait_for_ns_servers_or_assert(warmup_nodes, self)

    def start_index(self):
        if self.ddocs:
            query = {"connectionTimeout": 60000}
            for bucket in self.buckets:
                for ddoc in self.ddocs:
                    prefix = ("", "dev_")[ddoc.views[0].dev_view]
                    self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name, query, bucket=bucket)

    def failover(self):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        nodes = [node for node in nodes
                 if node.ip != self.master.ip or str(node.port) != self.master.port]
        self.failover_node = nodes[0]
        rest.fail_over(self.failover_node.id)

    def add_back_failover(self):
        rest = RestConnection(self.master)
        rest.add_back_node(self.failover_node.id)

    def create_ddocs_and_views(self, queue=None):
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            if int(self.ddocs_num) > 0:
                for i in range(int(self.ddocs_num)):
                    views = self.make_default_views(self.default_view_name,
                                                    self.view_num, self.is_dev_ddoc, different_map=True)
                    ddoc = DesignDocument(self.default_view_name + str(i), views)
                    self.ddocs.append(ddoc)
                    for view in views:
                        try:
                            self.cluster.create_view(self.master, ddoc.name, view,
                                                     bucket=bucket)
                        except Exception as e:
                            print(e)
                            if queue is not None:
                                queue.put(False)
                        if queue is not None:
                            queue.put(True)
            else:
                self.fail("Check param ddocs_num value")

    def delete_data(self, servers, paths_to_delete):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            for path in paths_to_delete:
                output, error = shell.execute_command("rm -rf {0}".format(path))
                shell.log_command_output(output, error)
                # shell._ssh_client.open_sftp().rmdir(path)
            shell.disconnect()

    def check_seqno(self, seqno_expected, comparator='=='):
        for bucket in self.buckets:
            if bucket.type == 'memcached':
                continue
            ready = BucketOperationHelper.wait_for_memcached(self.master,
                                                             bucket.name)
            self.assertTrue(ready, "wait_for_memcached failed")
            client = VBucketAwareMemcached(RestConnection(self.master), bucket)
            valid_keys, deleted_keys = bucket.kvs[1].key_set()
            for valid_key in valid_keys:
                try:
                    _, flags, exp, seqno, cas = client.memcached(valid_key).getMeta(valid_key)
                except MemcachedError as e:
                    print(e)
                    client.reset(RestConnection(self.master))
                    _, flags, exp, seqno, cas = client.memcached(valid_key).getMeta(valid_key)
                self.assertTrue((comparator == '==' and seqno == seqno_expected) or
                                (comparator == '>=' and seqno >= seqno_expected),
                                msg="seqno {0} !{1} {2} for key:{3}".
                                format(seqno, comparator, seqno_expected, valid_key))
            client.done()

    def force_reinstall(self, servers):
        for server in servers:
            try:
                remote = RemoteMachineShellConnection(server)
                appropriate_build = self._get_build(server, self.initial_version, remote)
                self.assertTrue(appropriate_build.url, msg="unable to find build {0}".format(self.initial_version))
                remote.download_build(appropriate_build)
                remote.install_server(appropriate_build, force=True)
                self.log.info("upgrade {0} to version {1} is completed".format(server.ip, self.initial_version))
                remote.disconnect()
                self.sleep(10)
                if self.is_linux:
                    self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
                else:
                    self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10,
                                             wait_if_warmup=True, check_service=True)
            except Exception as e:
                print(traceback.extract_stack())
                if queue is not None:
                    queue.put(False)
                    if not self.is_linux:
                        remote = RemoteMachineShellConnection(server)
                        output, error = remote.execute_command("cmd /c schtasks /Query /FO LIST /TN installme /V")
                        remote.log_command_output(output, error)
                        remote.disconnect()
                    raise e

    def _verify_vbucket_nums_for_swap(self, old_vbs, new_vbs):
        out_servers = set(old_vbs) - set(new_vbs)
        in_servers = set(new_vbs) - set(old_vbs)
        self.assertEqual(len(out_servers), len(in_servers),
                         "Seems like it wasn't swap rebalance. Out %s, in %s" % (
                             len(out_servers), len(in_servers)))
        for vb_type in ["active_vb", "replica_vb"]:
            self.log.info("Checking %s on nodes that remain in cluster..." % vb_type)
            for server, stats in old_vbs.items():
                if server in new_vbs:
                    self.assertTrue(sorted(stats[vb_type]) == sorted(new_vbs[server][vb_type]),
                                    "Server %s Seems like %s vbuckets were shuffled, old vbs is %s, new are %s" % (
                                        server.ip, vb_type, stats[vb_type], new_vbs[server][vb_type]))
            self.log.info("%s vbuckets were not suffled" % vb_type)
            self.log.info("Checking in-out nodes...")
            vbs_servs_out = vbs_servs_in = []
            for srv, stat in old_vbs.items():
                if srv in out_servers:
                    vbs_servs_out.extend(stat[vb_type])
            for srv, stat in new_vbs.items():
                if srv in in_servers:
                    vbs_servs_in.extend(stat[vb_type])
            self.assertTrue(sorted(vbs_servs_out) == sorted(vbs_servs_in),
                            "%s vbuckets seem to be suffled" % vb_type)

    def monitor_dcp_rebalance(self):
        """ released_upgrade_version """
        if self.master.ip != self.rest.ip:
            self.rest = RestConnection(self.master)
            self.rest_helper = RestHelper(self.rest)
        self.log.info("No need to do DCP rebalance upgrade")

    def _offline_upgrade(self, skip_init=False, nodes_to_upgrade=None):
        try:
            self.log.info("offline_upgrade")
            if nodes_to_upgrade:
                stoped_nodes = nodes_to_upgrade
            else:
                stoped_nodes = self.servers[:self.nodes_init]
            for upgrade_version in self.upgrade_versions:
                self.sleep(self.sleep_time, "Pre-setup of old version is done. "
                                            " Wait for upgrade to {0} version".format(upgrade_version))
                for server in stoped_nodes:
                    remote = RemoteMachineShellConnection(server)
                    remote.stop_server()
                    remote.disconnect()
                self.sleep(self.sleep_time)
                upgrade_threads = self._async_update(upgrade_version, stoped_nodes,
                                                     None, skip_init)
                for upgrade_thread in upgrade_threads:
                    upgrade_thread.join()
                success_upgrade = True
                while not self.queue.empty():
                    success_upgrade &= self.queue.get()
                if not success_upgrade:
                    self.fail("Upgrade failed!")

                if self.run_partition_validation:
                    try:
                        self.partition_validation()
                    except Exception as ex:
                        self.fail(ex)
            """ set install cb version to upgrade version after done upgrade """
            self.initial_version = self.upgrade_versions[0]

        except Exception as ex:
            self.log.info(ex)
            raise

    def offline_fail_over_upgrade(self):
        try:
            self.log.info("offline_failover_upgrade")
            upgrade_version = self.upgrade_versions[0]
            upgrade_nodes = self.servers[:self.nodes_init]
            total_nodes = len(upgrade_nodes)
            load_data_node = upgrade_nodes[0]
            iterator = 0
            for server in upgrade_nodes:
                if iterator == 0:
                    load_data_node = upgrade_nodes[1]

                self.rest.fail_over('ns_1@' + upgrade_nodes[total_nodes - 1].ip,
                                    graceful=True)
                self.sleep(timeout=60)
                self.rest.set_recovery_type('ns_1@' + upgrade_nodes[total_nodes - 1].ip,
                                            "full")
                output, error = self._upgrade(upgrade_version,
                                              upgrade_nodes[total_nodes - 1],
                                              fts_query_limit=10000000)
                if "You have successfully installed Couchbase Server." not in output:
                    self.fail("Upgrade failed. See logs above!")
                
                try:
                    thread1 = threading.Thread(target=self.load_items_during_rebalance, args=(load_data_node,))
                    thread2 = threading.Thread(target=self.cluster.rebalance, args=(self.servers[:self.nodes_init], [], [],))
                    
                    thread1.start()
                    thread2.start()

                    thread2.join()
                    self.isRebalanceComplete = True
                    thread1.join()

                except Exception as ex:
                    self.log.info("Could not push data while rebalancing")
                    self.log.info(ex)

                total_nodes -= 1
                if self.run_partition_validation:
                    try:
                        self.partition_validation()
                    except Exception as ex:
                        self.fail(ex)
            if total_nodes == 0:
                self.rest = RestConnection(upgrade_nodes[total_nodes])
        except Exception as ex:
            self.log.info(ex)
            raise
    
    def load_items_during_rebalance(self,server):
        if not self.fts_upgrade:
            return
        
        self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False)
        while(not self.isRebalanceComplete):
            if self.vector_upgrade:
                """delete 20% of docs"""
                status = self.fts_obj.delete_doc_by_key(server,10000001,10005001,0.2)

                if not status:
                    self.fail("CRUD operation failed during rebalance. OPS : DELETE")
                else:
                    self.log.info("CRUD operation successful during rebalance. OPS : DELETE")

                """upsert (update) the first 1000 docs out of 5000 docs present in the cluster"""
                self.fts_obj.load_data(1000)

                self.sleep(10)

                """upserting the vector data"""
                try:
                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password),xattr=True,base64Flag=True, end_index=10001001)

                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password), xattr=True, base64Flag=False, end_index=10001001)

                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password), xattr=False, base64Flag=False, end_index=10001001)

                    self.fts_obj.push_vector_data(server, str(self.rest_settings.rest_username),
                                        str(self.rest_settings.rest_password), xattr=False, base64Flag=True, end_index=10001001)

                except Exception as e:
                    self.log.info(e)
            else:
                self.fts_obj.load_data(1000)
                self.sleep(10)
                status = self.fts_obj.delete_doc_by_key(server,10000001,10001000,0.2,
                                                        bucket_name=self.target_bucket,scope_name=self.target_scope,collection_name=self.target_collection)

                if not status:
                    self.fail("CRUD operation failed during rebalance. OPS : DELETE")
                else:
                    self.log.info("CRUD operation successful during rebalance. OPS : DELETE")
            
            time.sleep(int(self.load_data_pause))
            break
        
        self.isRebalanceComplete = False
        

    def generate_map_nodes_out_dist_upgrade(self, nodes_out_dist):
        self.nodes_out_dist = nodes_out_dist
        self.generate_map_nodes_out_dist()

    """ subdoc base test starts here """

    def generate_json_for_nesting(self):
        json = {
            "not_tested_integer_zero": 0,
            "not_tested_integer_big": 1038383839293939383938393,
            "not_tested_double_zero": 0.0,
            "not_tested_integer": 1,
            "not_tested_integer_negative": -1,
            "not_tested_double": 1.1,
            "not_tested_double_negative": -1.1,
            "not_tested_float": 2.99792458e8,
            "not_tested_float_negative": -2.99792458e8,
            "not_tested_array_numbers_integer": [1, 2, 3, 4, 5],
            "not_tested_array_numbers_double": [1.1, 2.2, 3.3, 4.4, 5.5],
            "not_tested_array_numbers_float": [2.99792458e8, 2.99792458e8, 2.99792458e8],
            "not_tested_array_numbers_mix": [0, 2.99792458e8, 1.1],
            "not_tested_array_array_mix": [[2.99792458e8, 2.99792458e8, 2.99792458e8], [0, 2.99792458e8, 1.1], [],
                                           [0, 0, 0]],
            "not_tested_simple_string_lower_case": "abcdefghijklmnoprestuvxyz",
            "not_tested_simple_string_upper_case": "ABCDEFGHIJKLMNOPQRSTUVWXZYZ",
            "not_tested_simple_string_empty": "",
            "not_tested_simple_string_datetime": "2012-10-03 15:35:46.461491",
            "not_tested_simple_string_special_chars": r"_-+!#@$%&*(){}\][;.,<>?/",
            "not_test_json": {"not_to_bes_tested_string_field1": "not_to_bes_tested_string"}
        }
        return json

    def generate_simple_data_null(self):
        json = {
            "null": None,
            "null_array": [None, None]
        }
        return json

    def generate_simple_data_boolean(self):
        json = {
            "true": True,
            "false": False,
            "array": [True, False, True, False]
        }
        return json

    def generate_nested_json(self):
        json_data = self.generate_json_for_nesting()
        json = {
            "json_1": {"json_2": {"json_3": json_data}}
        }
        return json

    def generate_simple_data_numbers(self):
        json = {
            "integer_zero": 0,
            "integer_big": 1038383839293939383938393,
            "double_zero": 0.0,
            "integer": 1,
            "integer_negative": -1,
            "double": 1.1,
            "double_negative": -1.1,
            "float": 2.99792458e8,
            "float_negative": -2.99792458e8,
        }
        return json

    def subdoc_direct_client(self, server, bucket, timeout=30):
        # CREATE SDK CLIENT
        if self.use_sdk_client:
            try:
                scheme = "couchbase"
                host = self.master.ip
                if self.master.ip == "127.0.0.1":
                    scheme = "http"
                    host = "{0}:{1}".format(self.master.ip, self.master.port)
                return SDKClient(scheme=scheme, hosts=[host], bucket=bucket.name)
            except Exception as ex:
                self.log.info("cannot load sdk client due to error {0}".format(str(ex)))
        # USE MC BIN CLIENT WHEN NOT USING SDK CLIENT
        return self.direct_mc_bin_client(server, bucket, timeout=timeout)

    """ subdoc base test ends here """

    def construct_plan_params(self):
        plan_params = {}
        plan_params['numReplicas'] = 0
        if self.index_replicas:
            plan_params['numReplicas'] = self.index_replicas
        if self.partitions_per_pindex:
            plan_params['maxPartitionsPerPIndex'] = self.partitions_per_pindex
        return plan_params

    def construct_cbft_query_json(self, query, fields=None, timeout=None):
        max_matches = TestInputSingleton.input.param("query_max_matches", 10000000)
        query_json = QUERY.JSON
        # query is a unicode dict
        query_json['query'] = query
        query_json['indexName'] = self.name
        if max_matches:
            query_json['size'] = int(max_matches)
        if timeout:
            query_json['timeout'] = int(timeout)
        if fields:
            query_json['fields'] = fields
        return query_json

    def generate_ops_docs(self, num_items, start=0):
        try:
            json_generator = JsonGenerator()
            if self.dataset == "simple":
                return self.generate_ops(num_items, start, json_generator.generate_docs_simple)
            if self.dataset == "sales":
                return self.generate_ops(num_items, start, json_generator.generate_docs_sales)
            if self.dataset == "employee" or self.dataset == "default":
                return self.generate_ops(num_items, start, json_generator.generate_docs_employee)
            if self.dataset == "sabre":
                return self.generate_ops(num_items, start, json_generator.generate_docs_sabre)
            if self.dataset == "bigdata":
                return self.generate_ops(num_items, start, json_generator.generate_docs_bigdata)
            if self.dataset == "array":
                return self.generate_ops(num_items, start, json_generator.generate_docs_array)
        except Exception as ex:
            self.log.info(ex)
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_ops(self, docs_per_day, start=0, method=None):
        gen_docs_map = {}
        for key in list(self.ops_dist_map.keys()):
            isShuffle = False
            if key == "update":
                isShuffle = True
            if self.dataset != "bigdata":
                gen_docs_map[key] = method(docs_per_day=self.ops_dist_map[key]["end"],
                                           start=self.ops_dist_map[key]["start"])
            else:
                gen_docs_map[key] = method(value_size=self.value_size,
                                           end=self.ops_dist_map[key]["end"],
                                           start=self.ops_dist_map[key]["start"])
        return gen_docs_map

    def _convert_server_map(self, servers):
        map = {}
        for server in servers:
            key = self._gen_server_key(server)
            map[key] = server
        return map

    def _gen_server_key(self, server):
        return "{0}:{1}".format(server.ip, server.port)

    def generate_docs_simple(self, num_items, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start=start, docs_per_day=self.docs_per_day)

    def generate_docs_default(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day, start)

    def generate_docs(self, num_items, start=0):
        try:
            if self.dataset == "simple":
                return self.generate_docs_simple(num_items, start)
            if self.dataset == "array":
                return self.generate_docs_array(num_items, start)
            return getattr(self, 'generate_docs_' + self.dataset)(num_items, start)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def create_save_function_body_test(self, appname, appcode, description="Sample Description",
                                       checkpoint_interval=10000, cleanup_timers=False,
                                       dcp_stream_boundary="everything", deployment_status=True,
                                       skip_timer_threshold=86400,
                                       sock_batch_size=1, tick_duration=60000, timer_processing_tick_interval=500,
                                       timer_worker_pool_size=3, worker_count=3, processing_status=True,
                                       cpp_worker_thread_count=1, multi_dst_bucket=False, execution_timeout=3,
                                       data_chan_size=10000, worker_queue_cap=100000, deadline_timeout=6
                                       ):
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        if multi_dst_bucket:
            body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        body['depcfg']['metadata_bucket'] = self.metadata_bucket_name
        body['depcfg']['source_bucket'] = self.src_bucket_name
        body['settings'] = {}
        body['settings']['checkpoint_interval'] = checkpoint_interval
        body['settings']['cleanup_timers'] = cleanup_timers
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = deployment_status
        body['settings']['description'] = description
        body['settings']['log_level'] = self.eventing_log_level
        body['settings']['skip_timer_threshold'] = skip_timer_threshold
        body['settings']['sock_batch_size'] = sock_batch_size
        body['settings']['tick_duration'] = tick_duration
        body['settings']['timer_processing_tick_interval'] = timer_processing_tick_interval
        body['settings']['timer_worker_pool_size'] = timer_worker_pool_size
        body['settings']['worker_count'] = worker_count
        body['settings']['processing_status'] = processing_status
        body['settings']['cpp_worker_thread_count'] = cpp_worker_thread_count
        body['settings']['execution_timeout'] = execution_timeout
        body['settings']['data_chan_size'] = data_chan_size
        body['settings']['worker_queue_cap'] = worker_queue_cap
        body['settings']['use_memory_manager'] = self.use_memory_manager
        if execution_timeout != 3:
            deadline_timeout = execution_timeout + 1
        body['settings']['deadline_timeout'] = deadline_timeout
        return body

    def _verify_backup_events_definition(self, bk_fxn):
        backup_path = self.backupset.directory + "/backup/{0}/".format(self.backups[0])
        events_file_name = "events.json"
        bk_file_events_dir = "/tmp/backup_events{0}/".format(self.master.ip)
        bk_file_events_path = bk_file_events_dir + events_file_name

        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        self.log.info("create local dir")
        if os.path.exists(bk_file_events_dir):
            shutil.rmtree(bk_file_events_dir)
        os.makedirs(bk_file_events_dir)
        self.log.info("copy eventing definition from remote to local")
        shell.copy_file_remote_to_local(backup_path + events_file_name,
                                        bk_file_events_path)
        local_bk_def = open(bk_file_events_path)
        bk_file_fxn = json.loads(local_bk_def.read())
        for k, v in bk_file_fxn[0]["settings"].items():
            if v != bk_fxn[0]["settings"][k]:
                self.log.info("key {0} has value not match".format(k))
                self.log.info("{0} : {1}".format(v, bk_fxn[0]["settings"][k]))
        self.log.info("remove tmp file in slave")
        if os.path.exists(bk_file_events_dir):
            shutil.rmtree(bk_file_events_dir)

    def _verify_restore_events_definition(self, bk_fxn):
        backup_path = self.backupset.directory + "/backup/{0}/".format(self.backups[0])
        events_file_name = "events.json"
        bk_file_events_dir = "/tmp/backup_events{0}/".format(self.master.ip)
        bk_file_events_path = bk_file_events_dir + events_file_name
        rest = RestConnection(self.backupset.restore_cluster_host)
        rs_fxn = rest.get_all_functions()

        if self.ordered(rs_fxn) != self.ordered(bk_fxn):
            self.fail("Events definition of backup and restore cluster are different")

    def ordered(self, obj):
        if isinstance(obj, dict):
            return sorted((k, self.ordered(v)) for k, v in list(obj.items()))
        if isinstance(obj, list):
            return sorted(self.ordered(x) for x in obj)
        else:
            return obj

    def create_fts_index_query_compare(self, queue=None):

        """
        Call before upgrade
        1. creates a default index, one per bucket
        2. Loads fts json data
        3. Runs queries and compares the results against ElasticSearch
        """
        try:
            self.fts_obj = FTSCallable(nodes=self.servers, es_validate=True)
            for bucket in self.buckets:
                for i in range(self.num_indexes):
                    plans = self.construct_custom_plan_params(0, self.partition_list[i % self.num_indexes])
                    self.fts_obj.create_default_index(
                        index_name="index_{0}".format(i+1),
                        bucket_name=bucket.name,
                        plan_params=plans)
            self.fts_obj.load_data(self.num_items)
            self.fts_obj.wait_for_indexing_complete()

            self.log.info("Triggering additional wait time for index partitions to get distributed")
            time.sleep(100)

            for index in self.fts_obj.fts_indexes:
                self.fts_obj.run_query_and_compare(index=index, num_queries=20)
            
            return self.fts_obj
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def partition_validation(self, skip_check = True):
        if not self.run_partition_validation:
            pass
        if skip_check and self.skip_partition_check_inbetween:
            pass
        else:
            try:
                fts_node = self.get_nodes_from_services_map(service_type="fts")
                frest = RestConnection(fts_node)
                error = self.fts_obj.validate_partition_distribution(frest)
                if error:
                    self.log.info(f"partition distribution error: {error}")
                else:
                    self.log.info(f"partition distribution OK")
            except Exception as ex:
                self.log.info(ex)

    def create_fts_vector_index_query_compare(self, queue=None):
        try:
            self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False, servers=self.servers)
            is_passed = True

            for bucket in self.buckets:
                for i in range(self.num_indexes):
                    plans = self.construct_custom_plan_params(1, self.partition_list[i % self.num_indexes])
                    self.fts_obj.create_default_index(
                        index_name="index_{0}".format(i+1),
                        bucket_name=bucket.name,
                        plan_params=plans) 
            self.fts_obj.load_data(self.num_items)
            self.fts_obj.wait_for_indexing_complete()

            self.log.info("Triggering additional wait time for index partitions to get distributed")
            self.sleep(100)

            for index in self.fts_obj.fts_indexes:
                self.fts_obj.run_query_and_compare(index=index, num_queries=20)
            
            for bucket in self.buckets:
                plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                self.fts_obj.create_default_index(
                    index_name="index_{0}_persist".format(bucket.name),
                    bucket_name=bucket.name,
                    plan_params=plans)

            self.fts_indexes_store = self.fts_obj.fts_indexes[self.num_indexes]

            try:
                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password),xattr=True,base64Flag=True)

                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password), xattr=True, base64Flag=False)

                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password), xattr=False, base64Flag=False)

                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password), xattr=False, base64Flag=True)

            except Exception as e:
                print(e)

            pass_count = 0
            if float(str(self.start_version)[0:3]) >= 7.6:
                for i in range(self.num_indexes):
                    plans = self.construct_custom_plan_params(1, self.partition_list[i % self.num_indexes])
                    index_name = "index_default_vector_" + str(i)
                    _,status = self.fts_obj.create_vector_index(False, False,index_name,plans)
                    if status == 200 :
                        time.sleep(150)
                        if self.fts_obj.run_vector_queries(index_name=index_name):
                            pass_count +=1
                    

                if pass_count!= self.num_indexes:
                    self.fail("Vector queries failed. Terminating the test")

            return self.fts_obj
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def error_validation(self,result):
        possible_error_strings = [
            "xattr fields and properties not supported in this cluster",
            "unknown field type: 'vector_base64'",
            "vector_base64 typed fields not supported in this cluster",
            "field mapping contains invalid keys",
            "vector typed fields not supported in this cluster",
            "concurrent",
            "4096"
        ]
        for i in possible_error_strings:
            if i in result:
                return True
        return False

    def run_inbetween_tests(self):
        if self.inbetween_tests == 0:
            self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False,variable_node=self.servers[0],servers=self.servers)
            self.inbetween_tests = 1
            self.fts_obj.inbetween_tests = 1
            self.fts_obj.inbetween_active = True

            indexes_test_list = ["index_1"]
            if float(str(self.start_version)[0:3]) >= 7.6:
                indexes_test_list.append("index_default_vector_1")

            
            if float(str(self.upgrade_versions[0])[0:3]) >= 7.6 and str(self.upgrade_versions[0])[0:5] != "7.6.1" and str(self.upgrade_versions[0])[0:5] != "7.6.0":

                """
                try to create and update the index to support vector search via base64.
                expected case : the index update/create should fail.
                But at the same time, the normal vector search queries should work. (without base64)
                Pre: at the start of the tests, 2 indices are created. One to perform all these operations and
                one to check whether we can delete an index in mixed cluster state
                So we will perform a delete index operation on the index suffixed with delete
                If all these conditions returns true then we proceed further.
                """

                self.log.info("Update Index phase")
                for index_itr in indexes_test_list:
                    try:
                        self.log.info("updating index to support vectors in xattr")
                        result, status_code = self.fts_obj.update_vector_index(True,False,index_itr)

                        if status_code != 200:
                            self.log.info("SUCCESS. Cannot update index to xattr vector index in 7.6.0 mode")
                        else:
                            self.log.info(result)
                            self.fail("FAILURE [updated index to xattr vector index]")

                        self.log.info("updating index to support base64 in xattr")
                        result, status_code = self.fts_obj.update_vector_index(True,True,index_itr)

                        if status_code != 200:
                            self.log.info("SUCCESS. Cannot update index to xattr vector base64 index in 7.6.0 mode")
                        else:
                            self.log.info(result)
                            self.fail("FAILURE [updated index to xattr vector base64 index]")

                        self.log.info("updating index to support base64")
                        result, status_code = self.fts_obj.update_vector_index(False, True, index_itr)

                        if status_code != 200:
                            self.log.info("SUCCESS. Cannot update index to vector base64 index in 7.6.0 mode")
                        else:
                            self.log.info(result)
                            self.fail("FAILURE [updated index to vector base64 index] ")

                        self.log.info("updating index to support 4096 dimensions")
                        result, status_code = self.fts_obj.update_vector_index(False, False, index_itr,dimensions=4096)

                        if status_code != 200:
                            self.log.info("SUCCESS. Cannot update index to support 4096 dimensions in 7.6.0 mode")
                        else:
                            self.log.info(result)
                            self.fail("FAILURE [updated index to support 4096 dimensions in 7.6.0 mode]")

                    except Exception as e:
                        self.fail(e)

                self.log.info("Create Index phase")

                try:
                    self.log.info("creating index to support vectors in xattr")
                    plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                    result, status_code = self.fts_obj.create_vector_index(True,False,"index_default_vector_new_1",plans)

                    if status_code != 200:
                        self.log.info("SUCCESS. Cannot create index to xattr vector index in 7.6.0 mode")
                    else:
                        self.log.info(result)
                        self.fail("FAILURE [created index to xattr vector index]")

                    self.log.info("creating index to support base64 in xattr")
                    plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                    result, status_code = self.fts_obj.create_vector_index(True, True, "index_default_vector_new_2",plans)

                    if status_code != 200:
                        self.log.info("SUCCESS. Cannot create index to xattr vector base64 index in 7.6.0 mode")
                    else:
                        self.log.info(result)
                        self.fail("FAILURE [create index to xattr vector base64 index]")

                    self.log.info("creating index to support base64")
                    plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                    result, status_code = self.fts_obj.create_vector_index(False, True, "index_default_vector_new_3",plans)

                    if status_code != 200:
                        self.log.info("SUCCESS. Cannot create index to base64 index in 7.6.0 mode")
                    else:
                        self.log.info(result)
                        self.fail("FAILURE [create index to base64 index]")

                    self.log.info("creating index to support 4096 dimensions")
                    plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                    result, status_code = self.fts_obj.create_vector_index(False, True, "index_default_vector_new_4",plans,dimensions=4096)

                    if status_code != 200:
                        self.log.info("SUCCESS. Cannot create index to support 4096 dimensions in 7.6.0 mode")
                    else:
                        self.log.info(result)
                        self.fail("FAILURE [updated index to support 4096 dimensions in 7.6.0 mode]")

                except Exception as e:
                    print(e)

                if float(str(self.start_version)[0:3]) >= 7.6:
                    try:
                        self.log.info("Running vector queries")
                        if not self.fts_obj.run_vector_queries(index_name="index_default_vector_1"):
                            self.fail("Error running Vector queries. Terminating the test")
                    except Exception as e:
                        print(e)

            else:
                try:
                    self.log.info("Update Index phase")
                    self.log.info("updating index to support vector search")
                    result, status_code = self.fts_obj.update_vector_index(False, False, "index_1")

                    if status_code != 200:
                        self.log.info("SUCCESS. Cannot update index to support vector search in 7.2.0 mode")
                    else:
                        self.log.info(result)
                        self.fail("FAILURE [updated index to support vector search in 7.2.0 mode]")

                    self.log.info("Create Index phase")
                    self.log.info("creating index to support vector search")
                    plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                    result, status_code = self.fts_obj.create_vector_index(False, False, "index_default_vector_new",plans)

                    if status_code != 200:
                        self.log.info("SUCCESS. Cannot create index to support vector search in 7.2.0 mode")
                    else:
                        self.log.info(result)
                        self.fail("FAILURE [created index to support vector search in 7.2.0 mode]")

                except Exception as e:
                    print(e)

            try:
                self.log.info("Running FTS queries")
                self.fts_obj.run_query_and_compare(index=self.fts_indexes_store, num_queries=20)
            except Exception as e:
                print(e)


            self.inbetween_active = False

    def run_fts_vector_query_and_compare(self, queue=None):
        
        self.sleep(100)

        self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False, variable_node=self.servers[0], servers=self.servers)

        """delete 20% of docs"""
        status = self.fts_obj.delete_doc_by_key(self.servers[0],10000001,10005001,0.2)

        if status:
            self.log.info("CRUD Operation post upgrade successful. OPS: Delete")
        else:
            self.fail("CRUD Operation post upgrade failure. OPS: Delete")

        self.fts_obj.load_data(1000)

        try:
            self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                str(self.rest_settings.rest_password),xattr=True,base64Flag=True, end_index=10001001)

            self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                str(self.rest_settings.rest_password), xattr=True, base64Flag=False, end_index=10001001)

            self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                str(self.rest_settings.rest_password), xattr=False, base64Flag=False, end_index=10001001)

            self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                str(self.rest_settings.rest_password), xattr=False, base64Flag=True, end_index=10001001)

        except Exception as e:
            print(e)

        try:
            self.log.info("Running FTS queries")
            self.fts_obj.run_query_and_compare(index=self.fts_indexes_store, num_queries=20)
        except Exception as e:
            print(e)

        self.sleep(100)

        if float(str(self.upgrade_versions[0])[0:3]) >= 7.6 and str(self.upgrade_versions[0])[0:5] != "7.6.1" and str(self.upgrade_versions[0])[0:5] != "7.6.0":
            try:
                self.fts_obj.update_vector_index(False, False, "index_1")
                ok1 = self.fts_obj.run_vector_queries(index_name="index_1")

                self.fts_obj.update_vector_index(True, True, "index_1")
                ok2 = self.fts_obj.run_vector_queries(True, True, "vector_encoded", "vector_base64", index_name="index_1")

                self.fts_obj.update_vector_index(True, False, "index_1")
                ok3 =self.fts_obj.run_vector_queries(True, False, "vector_data", "vector", index_name="index_1")

                self.fts_obj.update_vector_index(False, True, "index_1")
                ok4 =self.fts_obj.run_vector_queries(False, True, "vector_data_base64", "vector_base64",
                                        index_name="index_1")

                check = True
                if float(str(self.start_version)[0:3]) >= 7.6:
                    check = False
                    self.fts_obj.update_vector_index(True, True, "index_default_vector_1")
                    ok5 =self.fts_obj.run_vector_queries(True, True, "vector_encoded", "vector_base64",index_name="index_default_vector_1")

                    self.fts_obj.update_vector_index(True, False, "index_default_vector_1")
                    ok6 =self.fts_obj.run_vector_queries(True, False, "vector_data", "vector",index_name="index_default_vector_1")

                    self.fts_obj.update_vector_index(False, True, "index_default_vector_1")
                    ok7 =self.fts_obj.run_vector_queries(False, True, "vector_data_base64", "vector_base64",index_name="index_default_vector_1")

                    if (ok5 and ok6 and ok7):
                        check = True

                if not (ok1 and ok2 and ok3 and ok4 and check):
                    self.fail("Vector queries failed. Terminating the test")

                plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                result, status_code = self.fts_obj.create_vector_index(False,False,"index_4096_check",plans,dimensions=4096)
                if status_code == 200:
                    self.log.info("Index updated to 4096 dimensions successfully")
                else:
                    self.fail(f"Failure. Cannot create a 4096 dimensions vector index. Result : {result}")

            except Exception as e:
                print(e)
        else:
            try:
                plans = self.construct_custom_plan_params(1, random.choice(self.partition_list))
                result, status_code = self.fts_obj.create_vector_index(False, False, "index_default_vector_post",plans)
                if status_code == 200:
                    if not self.fts_obj.run_vector_queries(index_name="index_default_vector_post"):
                        self.fail("Vector queries failed. Terminating the test")
                else:
                    self.fail(f"Failure. Cannot create index to a vector index. Result : {result}")

                result, status_code = self.fts_obj.update_vector_index(False, False, "index_1")
                if status_code == 200:
                    if not self.fts_obj.run_vector_queries(index_name="index_1"):
                        self.fail("Vector queries failed. Terminating the test")
                else:
                    self.fail(f"Failure. Cannot update index to a vector index. Result : {result}")

            except Exception as e:
                print(e)


    def setup_for_test(self, queue=None):
        self.set_bleve_max_result_window()

    def set_bleve_max_result_window(self):
        bmrw_value = 100000000
        for node in self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True):
            self.log.info("updating bleve_max_result_window of node : {0}".format(node))
            rest = RestConnection(node)
            rest.set_bleve_max_result_window(bmrw_value)

    def test_offline_upgrade(self, queue=None):
        try:
            post_upgrade_errors = {}
            fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
            fts_callable.load_data(2000)
            status = self.fts_obj.delete_doc_by_key(self.servers[0],10000001,10002000,0.2)
            if status:
                self.log.info("CRUD Operation post upgrade successful. OPS: Delete")
            else:
                self.fail("CRUD Operation post upgrade failure. OPS: Delete")
            fts_callable.load_data(2000)

            RestConnection(self.servers[0]).modify_memory_quota(kv_quota=812,fts_quota = 1700)
            
            fts_query = {"query": "dept:Engineering"}

            self.log.info("="*20 + " Starting post offline upgrade tests")
            fts_callable.flush_buckets(["default"])
            errors = self._test_create_single_collection_index()
            if len(errors) > 0:
                post_upgrade_errors['_test_create_single_collection_index'] = errors
            errors = self._test_create_multicollection_index()
            if len(errors) > 0:
                post_upgrade_errors['_test_create_multicollection_index'] = errors
            errors = self._test_create_bucket_index()
            if len(errors) > 0:
                post_upgrade_errors['_test_create_bucket_index'] = errors
            errors = self._test_backup_restore()
            if len(errors) > 0:
                post_upgrade_errors['_test_backup_restore'] = errors
            errors = self._test_rbac_admin()
            if len(errors) > 0:
                post_upgrade_errors['_test_rbac_admin'] = errors
            errors = self._test_rbac_searcher()
            if len(errors) > 0:
                post_upgrade_errors['_test_rbac_searcher'] = errors
            errors = self._test_flex_pushdown_in()
            if len(errors) > 0:
                post_upgrade_errors['_test_flex_pushdown_in'] = errors
            errors = self._test_flex_pushdown_like()
            if len(errors) > 0:
                post_upgrade_errors['_test_flex_pushdown_like'] = errors
            errors = self._test_flex_pushdown_sort()
            if len(errors) > 0:
                post_upgrade_errors['_test_flex_pushdown_sort'] = errors
            errors = self._test_flex_doc_id()
            if len(errors) > 0:
                post_upgrade_errors['_test_flex_doc_id'] = errors
            errors = self._test_flex_pushdown_negative_numeric_ranges()
            if len(errors) > 0:
                post_upgrade_errors['_test_flex_pushdown_negative_numeric_ranges'] = errors
            errors = self._test_flex_and_search_pushdown()
            if len(errors) > 0:
                post_upgrade_errors['_test_flex_and_search_pushdown'] = errors
            errors = self._test_search_before()
            if len(errors) > 0:
                post_upgrade_errors['_test_search_before'] = errors
            errors = self._test_search_after()
            if len(errors) > 0:
                post_upgrade_errors['_test_search_after'] = errors
            errors = self._test_new_metrics(endpoint='_prometheusMetrics')
            if len(errors) > 0:
                post_upgrade_errors["_test_new_metrics(endpoint='_prometheusMetrics')"] = errors
            errors = self._test_new_metrics(endpoint='_prometheusMetricsHigh')
            if len(errors) > 0:
                post_upgrade_errors["_test_new_metrics(endpoint='_prometheusMetricsHigh')"] = errors

            self.assertEquals(len(post_upgrade_errors.keys()), 0,
                              f"The following post upgrade tests are failed: {post_upgrade_errors}")
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def update_delete_fts_data_run_queries(self, queue=None):
        """
        To call after (preferably) upgrade
        :param fts_obj: the FTS object created in create_fts_index_query_compare()
        """
        try:
            if not self.fts_obj:
                self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False)
            self.fts_obj.async_perform_update_delete()
            self.fts_obj.wait_for_indexing_complete()
            for index in self.fts_obj.fts_indexes:
                self.fts_obj.run_query_and_compare(index)
        except Exception as ex:
            print(ex)

    def delete_all_fts_artifacts(self, queue=None):
        """
        Call during teardown of upgrade test
        :param fts_obj: he FTS object created in create_fts_index_query_compare()
        """
        self.fts_obj.delete_all()

    def run_fts_query_and_compare(self, queue=None):
        self.fts_obj = FTSCallable(nodes=self.servers, es_validate=True)
        try:
            self.log.info("Verify fts via queries again")
            self.fts_obj.load_data(1000)
            """delete 20% of docs"""
            status = self.fts_obj.delete_doc_by_key(self.servers[0],10000001,10001000,0.2,
                                                        bucket_name=self.target_bucket,scope_name=self.target_scope,collection_name=self.target_collection)

            if status:
                self.log.info("CRUD Operation post upgrade successful. OPS: Delete")
            else:
                self.fail("CRUD Operation post upgrade failure. OPS: Delete")

            self.fts_obj.load_data(1000)

            try:
                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password),xattr=True,base64Flag=True, end_index=10001001)

                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password), xattr=True, base64Flag=False, end_index=10001001)

                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password), xattr=False, base64Flag=False, end_index=10001001)

                self.fts_obj.push_vector_data(self.servers[0], str(self.rest_settings.rest_username),
                                    str(self.rest_settings.rest_password), xattr=False, base64Flag=True, end_index=10001001)

            except Exception as e:
                print(e)

            self.update_delete_fts_data_run_queries(self.fts_obj)
        except Exception as ex:
            self.fail(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def update_indexes(self, queue=None):
        pre_upgrade_index_type = self.input.param("index_type", "upside_down")
        after_upgrade_index_type = self.input.param("after_upgrade_index_type", pre_upgrade_index_type)
        try:
            for index in self.fts_obj.fts_indexes:
                if index.is_scorch() and after_upgrade_index_type == "scorch":
                    self.log.info("Index type is scorch, now converting to upside_down... ")
                    index.update_index_to_upside_down()
                elif index.is_upside_down() and after_upgrade_index_type == "upside_down":
                    self.log.info("Index type is upside_down, now converting to scorch... ")
                    index.update_index_to_scorch()
                else:
                    self.fail("Index {0} which was {1} pre-upgrade is now automatically "
                              "converted to {2} post upgrade".
                              format(index.name,
                                     pre_upgrade_index_type,
                                     index.get_index_type()))
            self.fts_obj.wait_for_indexing_complete()
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def enforce_tls_https(self, queue=None):
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        CbServer.use_https = True
        if queue is not None:
            queue.put(True)

    def enable_multiple_ca(self, queue=None):
        CbServer.multiple_ca = True
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "des3")
        self.use_client_certs = self.input.param("use_client_certs", False)
        self.cacert_verify = self.input.param("cacert_verify", False)
        spec_file = self.input.param("spec_file", "default")
        self.use_https = True
        CbServer.use_https = True
        CbServer.use_client_certs = self.use_client_certs
        CbServer.cacert_verify = self.cacert_verify
        ntonencryptionBase().disable_nton_cluster([self.master])
        CbServer.x509 = x509main(host=self.master, encryption_type=self.encryption_type,
                                 passphrase_type=self.passphrase_type)
        for server in self.input.servers:
            CbServer.x509.delete_inbox_folder_on_server(server=server)
        CbServer.x509.generate_multiple_x509_certs(servers=self.input.servers, spec_file_name=spec_file)
        self.log.info("Manifest #########\n {0}".format(json.dumps(CbServer.x509.manifest, indent=4)))
        for server in self.input.servers:
            _ = CbServer.x509.upload_root_certs(server)
        CbServer.x509.upload_node_certs(servers=self.input.servers)
        testuser = [{'id': 'clientuser', 'name': 'clientuser', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)

        # Assign user to role
        role_list = [{'id': 'clientuser', 'name': 'clientuser', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.master), 'builtin')
        CbServer.x509.upload_client_cert_settings(server=self.master)
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        if queue is not None:
            queue.put(True)

    def enforce_control_encryption(self, queue=None):
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="control")
        CbServer.use_https = False
        if queue is not None:
            queue.put(True)

    def modify_num_pindexes(self, queue=None):
        for index in self.fts_obj.fts_indexes:
            index.update_num_pindexes(8)
        self.fts_obj.wait_for_indexing_complete()

    def modify_num_replicas(self, queue=None):
        for index in self.fts_obj.fts_indexes:
            index.update_num_replicas(1)
        self.fts_obj.wait_for_indexing_complete()

    def update_index_to_scorch(self, queue=None):
        try:
            for index in self.fts_obj.fts_indexes:
                if index.is_upside_down() or index.is_type_unspecified():
                    self.log.info("Index type is {0}, now converting to scorch... ".format(index.get_index_type()))
                    index.update_index_to_scorch()
                else:
                    self.fail("FAIL: Index {0} was not meant to be of type {1}".
                              format(index.name,
                                     index.get_index_type()))
            self.fts_obj.wait_for_indexing_complete()
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def delete_fts_indexes(self, queue=None):
        rest = RestConnection(self.servers[0])
        for index in self.fts_obj.fts_indexes:
            rest.delete_fts_index(index.name)

    def update_index_to_upside_down(self, queue=None):
        try:
            for index in self.fts_obj.fts_indexes:
                if index.is_scorch() or index.is_type_unspecified():
                    self.log.info("Index type is {0}, now converting to upside_down... ".format(index.get_index_type()))
                    index.update_index_to_upside_down()
                else:
                    self.fail("FAIL: Index {0} was not meant to be of type {1}".
                              format(index.name,
                                     index.get_index_type()))
            self.fts_obj.wait_for_indexing_complete()
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def check_index_type(self, queue=None):
        check_index_type = self.input.param("check_index_type", None)
        try:
            for index in self.fts_obj.fts_indexes:
                type = index.get_index_type()
                if type == check_index_type:
                    self.log.info("SUCCESS: Index type check returns {0}".format(type))
                else:
                    self.fail("FAIL: Index {0} of type {1} is expected to be of type {2}".
                              format(index.name,
                                     type,
                                     check_index_type))
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    """ Use n1ql callable to create, query n1ql """

    def create_n1ql_index_and_query(self, queue=None):
        """
        Call this before upgrade
        1. creates a gsi index, one per bucket
        2. Loads fts json data
        3. Runs queries and compares the results against ElasticSearch
        """
        try:
            n1ql_obj = N1QLCallable(self.servers)
            for bucket in self.buckets:
                n1ql_obj.create_gsi_index(keyspace=bucket.name, name="test_idx1",
                                          fields="email", using="gsi", is_primary=False,
                                          index_condition="")
                result = n1ql_obj.run_n1ql_query("select * from system:indexes")
                n1ql_obj.drop_gsi_index(keyspace=bucket.name, name="test_idx1",
                                        is_primary=False)
            # return self.n1ql_obj
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    def run_n1ql_query(self, queue=None):
        try:
            self.log.info("Run queries again")
            n1ql_obj = N1QLCallable(self.servers)
            result = n1ql_obj.run_n1ql_query("select * from system:indexes")
        except Exception as ex:
            print(ex)
            if queue is not None:
                queue.put(False)
        if queue is not None:
            queue.put(True)

    """ for cbas test """

    def load_sample_buckets(self, servers=None, bucketName=None, total_items=None,
                            rest=None):
        """ Load the specified sample bucket in Couchbase """
        self.assertTrue(rest.load_sample(bucketName),
                        "Failure while loading sample bucket: {0}".format(bucketName))

        """ check for load data into travel-sample bucket """
        if total_items:
            import time
            end_time = time.time() + 600
            while time.time() < end_time:
                self.sleep(20)
                num_actual = 0
                if not servers:
                    num_actual = self.get_item_count(self.master, bucketName)
                else:
                    for server in servers:
                        num_actual += self.get_item_count(server, bucketName)
                if int(num_actual) == total_items:
                    self.log.info("{0} all items are loaded in the {1} bucket" \
                                  .format(num_actual, bucketName))
                    break
                self.log.info("{0} items are loaded in the {1} bucket.  Wait.." \
                              .format(num_actual, bucketName))
            if int(num_actual) != total_items:
                return False
        else:
            self.sleep(120)

        return True

    def execute_statement_on_cbas_via_rest(self, statement, mode=None, rest=None,
                                           timeout=120, client_context_id=None,
                                           username=None, password=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        pretty = "true"
        if not rest:
            rest = RestConnection(self.cbas_node)
        try:
            self.log.info("Running query on cbas: {0}".format(statement))
            response = rest.execute_statement_on_cbas(statement, mode, pretty,
                                                      timeout, client_context_id,
                                                      username, password)
            response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
            else:
                errors = None

            if "results" in response:
                results = response["results"]
            else:
                results = None

            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None

            return response["status"], response[
                "metrics"], errors, results, handle

        except Exception as e:
            raise Exception(str(e))

    def create_bucket_on_cbas(self, cbas_bucket_name, cb_bucket_name,
                              cb_server_ip=None,
                              validate_error_msg=False,
                              username=None, password=None):
        """
        Creates a bucket on CBAS
        """
        if cb_server_ip:
            cmd_create_bucket = "create bucket " + cbas_bucket_name + \
                                " with {\"name\":\"" + cb_bucket_name + \
                                "\",\"nodes\":\"" + cb_server_ip + "\"};"
        else:
            '''DP3 doesn't need to specify cb server ip as cbas node is
               part of the cluster.
            '''
            cmd_create_bucket = "create bucket " + cbas_bucket_name + \
                                " with {\"name\":\"" + cb_bucket_name + "\"};"
        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_via_rest(cmd_create_bucket,
                                                    username=username,
                                                    password=password)

        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_bucket(self, cbas_bucket_name, cbas_dataset_name,
                                 where_field=None, where_value=None,
                                 validate_error_msg=False, username=None,
                                 password=None):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        cmd_create_dataset = "create shadow dataset {0} on {1};".format(
            cbas_dataset_name, cbas_bucket_name)
        if where_field and where_value:
            cmd_create_dataset = "create shadow dataset {0} on {1} WHERE `{2}`=\"{3}\";" \
                .format(cbas_dataset_name, cbas_bucket_name,
                        where_field, where_value)
        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_via_rest(cmd_create_dataset,
                                                    username=username,
                                                    password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors)
        else:
            if status != "success":
                return False
            else:
                return True

    def test_create_dataset_on_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        result = self.create_dataset_on_bucket(
            cbas_bucket_name=self.cbas_bucket_name_invalid,
            cbas_dataset_name=self.cbas_dataset_name,
            validate_error_msg=self.validate_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def create_scope(self, num_scope=2, rest=None, cli=None):
        bucket_name = self.buckets[0].name
        if rest:
            rest_client = rest
        else:
            rest_client = self.rest_col
        if cli:
            cli_client = cli
        else:
            cli_client = self.cli_col
        for x in range(num_scope):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                rest_client.create_scope(bucket=bucket_name, scope=scope_name,
                                         params=None)
            else:
                cli_client.create_scope(bucket=bucket_name, scope=scope_name)

    def delete_scope(self, num_scope=2):
        bucket_name = self.buckets[0].name
        for x in range(num_scope):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                self.rest_col.delete_scope(bucket_name, scope_name)
            else:
                self.cli_col.delete_scope(scope_name, bucket=bucket_name)

    def create_collection(self, num_collection=1, rest=None, cli=None):
        bucket_name = self.buckets[0].name
        if rest:
            rest_client = rest
        else:
            rest_client = self.rest_col
        if cli:
            cli_client = cli
        else:
            cli_client = self.cli_col

        scopes = self.get_bucket_scope(rest_client, cli_client)
        if scopes:
            for x in range(num_collection):
                for scope in scopes:
                    if bucket_name in scope:
                        continue
                    if self.use_rest:
                        rest_client.create_collection(bucket=bucket_name, scope=scope,
                                                      collection="mycollection_{0}_{1}".format(scope, x))
                    else:
                        cli_client.create_collection(bucket=bucket_name, scope=scope,
                                                     collection="mycollection_{0}_{1}".format(scope, x))
        self.sleep(10, "time needs for stats up completely")

    def delete_collection(self, num_collection=1):
        bucket_name = self.buckets[0].name
        for x in range(num_collection):
            if self.use_rest:
                self.rest_col.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                                collection="_{0}".format(bucket_name))
            else:
                self.cli_col.delete_collection(bucket=bucket_name, scope="_{0}".format(bucket_name),
                                               collection="_{0}".format(bucket_name))

    def get_col_item_count(self, server=None, bucket=None, scope=None, collection=None, cluster_stats=None):
        if not server:
            raise ("Need to pass which server to get item count")
        if not cluster_stats:
            raise ("Need to pass cluster stats to get item count")
        if not scope:
            scope = "_default"
        if not collection:
            collection = "_default"
        if not bucket:
            bucket = "default"
        return cluster_stats.get_collection_item_count(bucket, scope, collection, server, cluster_stats)

    def _create_scope_collection(self, rest=None, cli=None, bucket=None):
        if rest:
            self.rest_col = rest
        if cli:
            self.cli_col = cli
        if bucket:
            bucket_name = bucket
        else:
            bucket_name = self.buckets[0].name
        for x in range(self.num_scopes):
            scope_name = "scope{0}".format(x)
            if self.non_ascii_name:
                scope_name = self.non_ascii_name + str(x)
            if self.use_rest:
                self.rest_col.create_scope_collection(bucket_name, scope_name,
                                                      "mycollection_{0}".format(scope_name))
            else:
                self.cli_col.create_scope_collection(bucket_name, scope_name,
                                                     "mycollection_{0}".format(scope_name))

    def get_bucket_scope(self, rest=None, cli=None):
        bucket_name = self.buckets[0].name
        if rest:
            rest_client = rest
        else:
            rest_client = self.rest_col
        if cli:
            cli_client = cli
        else:
            cli_client = self.cli_col
        if self.use_rest:
            scopes = rest_client.get_bucket_scopes(bucket_name)
        else:
            scopes = cli_client.get_bucket_scopes(bucket_name)[0]
        return scopes

    def get_bucket_collection(self, ):
        bucket_name = self.buckets[0].name
        collections = None
        if self.use_rest:
            collections = self.rest_col.get_bucket_collections(bucket_name)
        else:
            collections = self.cli_col.get_bucket_collections(bucket_name)
        return collections

    def get_collection_stats(self, buckets=None):
        """ return output, error """
        if buckets:
            bucket = buckets[0]
        else:
            bucket = self.buckets[0]
        return self.stat_col.get_collection_stats(bucket)

    def get_collection_names(self):
        bucket = self.buckets[0]
        output, error = self.shell.execute_cbstats(bucket, "collections",
                                                   cbadmin_user="Administrator",
                                                   options=" | grep ':name'")
        collection_names = []
        if output:
            for x in output:
                if "_default" in x:
                    continue
                collection_names.append(x.split(":name:")[1].strip())
        return collection_names, error

    def get_scopes_id(self, scope, bucket=None, stat_col=None):
        if bucket:
            get_bucket = bucket
        else:
            get_bucket = self.buckets[0]
        if stat_col:
            self.stat_col = stat_col
        return self.stat_col.get_scope_id(get_bucket, scope)

    def get_collections_id(self, scope, collection, bucket=None, stat_col=None):
        if bucket:
            get_bucket = bucket
        else:
            get_bucket = self.buckets[0]
        if stat_col:
            self.stat_col = stat_col
        return self.stat_col.get_collection_id(get_bucket, scope, collection)

    def get_collection_load_id(self):
        scopes = self.get_bucket_scope()
        scopes_id = []
        scopes_names_ids = {}
        for scope in scopes:
            if scope == "_default":
                scopes.remove(scope)
                continue
            self.log.info("get scope id of scope: {0}".format(scope))
            scope_id = self.get_scopes_id(scope)
            if scope_id is None:
                self.sleep(5, "wait for stats is up completely")
                scope_id = self.get_scopes_id(scope)
            scopes_names_ids[scope] = scope_id
            scopes_id.append(scope_id)
        self.load_scope = scopes[0]
        collections = self.get_bucket_collection()
        collections_id = []
        for collection in collections:
            if collection == "_default":
                collections.remove(collection)
                continue
            if self.load_scope not in collection:
                continue
            collection_id = self.get_collections_id(self.load_scope, collection)
            collections_id.append(self.get_collections_id(self.load_scope, collection))

        collections_id = list(filter(None, collections_id))
        if collections_id:
            self.load_scope_id = scopes_names_ids[self.load_scope]
            return collections_id[0]
        else:
            return "0x0"

    def load_collection_all_buckets(self, cluster=None, item_size=125, ratio=0.9, command_options=""):
        cluster = self.master
        shell = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            self.sleep(7)
            shell.execute_cbworkloadgen(cluster.rest_username,
                                        cluster.rest_password,
                                        self.num_items,
                                        ratio,
                                        bucket.name,
                                        item_size,
                                        command_options)

    def load_to_collections_bucket(self):
        self.load_collection_id = self.get_collection_load_id()
        option = " -c {0} ".format(self.load_collection_id)
        self.sleep(10)
        self.load_collection_all_buckets(command_options=option)

    def _verify_collection_data(self):
        items_match = False
        self.sleep(10)
        upgrade_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True,
                                                         master=self.upgrade_master_node)
        items = self.stat_col.get_scope_item_count(self.buckets[0], self.load_scope,
                                                   node=upgrade_nodes)
        if int(self.num_items) == int(items):
            items_match = True
            self.log.info("data loaded to collecion")
        if not items_match:
            self.log.error("Failed to load to collection")

    def pre_upgrade(self, servers):
        if self.rest is None:
            self._new_master(self.master)
        self.ddocs_num = self.input.param("ddocs_num", 0)
        if int(self.ddocs_num) > 0:
            self.create_ddocs_and_views()
            verify_data = False
            if self.scan_consistency != "request_plus":
                verify_data = True
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.load(self.gens_load, flag=self.item_flag,
                      verify_data=verify_data, batch_size=self.batch_size)
        rest = RestConnection(servers[0])
        output, rq_content, header = rest.set_auto_compaction(dbFragmentThresholdPercentage=20,
                                                              viewFragmntThresholdPercentage=20)
        self.assertTrue(output, "Error in set_auto_compaction... {0}".format(rq_content))
        status, content, header = rest.set_indexer_compaction(mode="full", fragmentation=20)
        self.assertTrue(status, "Error in setting Append Only Compaction... {0}".format(content))
        operation_type = self.input.param("pre_upgrade", "")
        if operation_type:
            self.n1ql.run_async_index_operations(operation_type)

    def during_upgrade(self, servers):
        self.ddocs_num = self.input.param("ddocs_num", 0)
        if int(self.ddocs_num) > 0:
            self.create_ddocs_and_views()
            kv_tasks = self.async_run_doc_ops()
            operation_type = self.input.param("during_upgrade", "")
            self.n1ql.run_async_index_operations(operation_type)
            for task in kv_tasks:
                task.result()

    def post_upgrade(self, servers):
        self.log.info(" Doing post upgrade")
        self.ddocs_num = self.input.param("ddocs_num", 0)
        self.add_built_in_server_user()
        if int(self.ddocs_num) > 0:
            self.create_ddocs_and_views(servers[0])
            kv_tasks = self.async_run_doc_ops()
            operation_type = self.input.param("post_upgrade", "")
            self.n1ql.run_async_index_operations(operation_type)
            for task in kv_tasks:
                task.result()
        self.verification(servers, check_items=False)

    def _create_ephemeral_buckets(self):
        create_ephemeral_buckets = self.input.param(
            "create_ephemeral_buckets", False)
        if not create_ephemeral_buckets:
            return
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "5" > version:
                self.log.info("Atleast one of the nodes in the cluster is "
                              "pre 5.0 version. Hence not creating ephemeral"
                              "bucket for the cluster.")
                return
        num_ephemeral_bucket = self.input.param("num_ephemeral_bucket", 1)
        server = self.master
        server_id = RestConnection(server).get_nodes_self().id
        ram_size = RestConnection(server).get_nodes_self().memoryQuota
        bucket_size = self._get_bucket_size(ram_size, self.bucket_size +
                                            num_ephemeral_bucket)
        self.log.info("Creating ephemeral buckets")
        self.log.info("Changing the existing buckets size to accomodate new "
                      "buckets")
        for bucket in self.buckets:
            rest.change_bucket_props(bucket, ramQuotaMB=bucket_size)

        bucket_tasks = []
        bucket_params = copy.deepcopy(
            self.bucket_base_params['membase']['non_ephemeral'])
        bucket_params['size'] = bucket_size
        bucket_params['bucket_type'] = 'ephemeral'
        bucket_params['eviction_policy'] = 'noEviction'
        ephemeral_buckets = []
        self.log.info("Creating ephemeral buckets now")
        for i in range(num_ephemeral_bucket):
            name = 'ephemeral_bucket' + str(i)
            port = STANDARD_BUCKET_PORT + i + 1
            bucket_priority = None
            if self.standard_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(
                    self.standard_bucket_priority[i])

            bucket_params['bucket_priority'] = bucket_priority
            bucket_tasks.append(
                self.cluster.async_create_standard_bucket(name=name, port=port,
                                                          bucket_params=bucket_params))
            bucket = Bucket(name=name,
                            num_replicas=self.num_replicas,
                            bucket_size=self.bucket_size,
                            port=port, master_id=server_id,
                            eviction_policy='noEviction', lww=self.lww)
            self.buckets.append(bucket)
            ephemeral_buckets.append(bucket)

        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

        if self.enable_time_sync:
            self._set_time_sync_on_buckets(
                ['standard_bucket' + str(i) for i in range(
                    num_ephemeral_bucket)])
        load_gen = BlobGenerator('upgrade', 'upgrade-', self.value_size,
                                 end=self.num_items)
        for bucket in ephemeral_buckets:
            self._load_bucket(bucket, self.master, load_gen, "create",
                              self.expire_time)

    def _test_create_single_collection_index(self):
        self.log.info("=" * 20 + " _test_create_single_collection_index")

        errors = []
        self._create_collections(scope="scope1", collection="collection1")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection1", collection_index=True)
        try:
            fts_callable.load_data(100)
        except Exception as e:
            errors.append(f"Could not load data into collection: {e}")
            return errors

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection1")

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope="scope1", collections=["collection1"], no_check=False)
        fts_callable.wait_for_indexing_complete(100)

        docs_indexed = fts_idx.get_indexed_doc_count()
        container_doc_count = fts_idx.get_src_bucket_doc_count()
        self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
        if docs_indexed == 0:
            errors.append(f"No docs were indexed for index {fts_idx.name}")
        if docs_indexed != container_doc_count:
            errors.append(f"Bucket doc count = {container_doc_count}, index doc count={docs_indexed}")

        fts_callable.delete_fts_index("idx")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_create_multicollection_index(self):
        self.log.info("=" * 20 + " _test_create_multicollection_index")
        errors = []
        self._create_collections(scope="scope1", collection=["collection2", "collection3", "collection4"])
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections=["collection2", "collection3", "collection4"], collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection=["collection2", "collection3",
                                                                              "collection4"])

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope="scope1",
                                                collections=["collection2", "collection3", "collection4"],
                                                no_check=False)
        fts_callable.wait_for_indexing_complete(300)

        docs_indexed = fts_idx.get_indexed_doc_count()

        container_doc_count = fts_idx.get_src_bucket_doc_count()

        self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
        if docs_indexed == 0:
            errors.append(f"No docs were indexed for index {fts_idx.name}")
        if docs_indexed != container_doc_count:
            errors.append(f"kv doc count = {container_doc_count}, index doc count={docs_indexed}")

        fts_callable.delete_fts_index("idx")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_create_bucket_index(self):
        self.log.info("=" * 20 + " _test_create_bucket_index")
        errors = []
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_callable.load_data(100)

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard",
                                                no_check=False)
        fts_callable.wait_for_indexing_complete(100)

        docs_indexed = fts_idx.get_indexed_doc_count()

        if fts_idx.collections:
            container_doc_count = fts_idx.get_src_collections_doc_count()
        else:
            container_doc_count = fts_idx.get_src_bucket_doc_count()

        self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
        if docs_indexed == 0:
            errors.append(f"No docs were indexed for index {fts_idx.name}")
        if docs_indexed != container_doc_count:
            errors.append(f"kv doc count = {container_doc_count}, index doc count={docs_indexed}")

        fts_callable.delete_fts_index("idx")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_scope_limit_num_fts_indexes(self):
        self.log.info("=" * 20 + " _test_scope_limit_num_fts_indexes")
        limit_scope = "inventory"
        limit_value = 3
        errors = []
        self.sample_bucket_name = "travel-sample"
        self.sample_index_name = "travel-sample-index"
        fts_node = self.get_nodes_from_services_map(service_type="fts")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        self.fts_rest = RestConnection(fts_node)
        self.fts_rest.load_sample(self.sample_bucket_name)
        self.fts_rest.set_internalSetting("enforceLimits", True)
        self.fts_rest.set_fts_tier_limit(bucket=self.sample_bucket_name, scope=limit_scope, limit=limit_value)
        collection = ["airline"]
        _type = self.__define_index_parameters_collection_related(container_type="collection", scope=limit_scope,
                                                                  collection=collection)
        for i in range(limit_value):
            fts_callable.create_fts_index(name=f'{self.sample_index_name}_{i}', source_name=self.sample_bucket_name,
                                          collection_index=True, _type=_type, scope=limit_scope,
                                          collections=self.collection)
        self.sleep(30)
        try:
            fts_callable.create_fts_index(name=f'{self.sample_index_name}_{i + 2}', source_name=self.sample_bucket_name,
                                          collection_index=True, _type=_type, scope=limit_scope,
                                          collections=self.collection)
        except Exception as e:
            self.log.info(str(e))
            if "num_fts_indexes" not in str(e):
                errors = "expected error message not found"

        fts_callable.delete_bucket(self.sample_bucket_name)
        return errors

    def _test_backup_restore(self):
        self.log.info("=" * 20 + " _test_backup_restore")
        test_errors = []
        index_definitions = {}

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_idx = fts_callable.create_fts_index("idx_backup_restore", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard", no_check=False)
        index_definitions['idx_backup_restore'] = {}
        index_definitions['idx_backup_restore']['initial_def'] = {}
        index_definitions['idx_backup_restore']['backup_def'] = {}
        index_definitions['idx_backup_restore']['restored_def'] = {}

        _, index_def = fts_idx.get_index_defn()

        initial_index_def = index_def['indexDef']
        index_definitions['idx_backup_restore']['initial_def'] = initial_index_def

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) == 0:
            test_errors.append("At least 1 fts node must be presented in cluster")
        rest = RestConnection(fts_nodes[0])

        backup_filter = {"option": "include", "containers": ["default"]}
        backup_client = FTSIndexBackupClient(fts_nodes[0])

        status, content = backup_client.backup(_filter=backup_filter)
        backup_json = json.loads(content)
        backup = backup_json['indexDefs']['indexDefs']

        # store backup index definitions
        indexes_for_backup = ["idx_backup_restore"]
        for idx in indexes_for_backup:
            backup_index_def = backup[idx]
            index_definitions[idx]['backup_def'] = backup_index_def

        # delete all indexes before restoring from backup
        self.fts_obj.delete_all()

        # restoring indexes from backup
        backup_client.restore()

        # getting restored indexes definitions and storing them in indexes definitions dict
        for ix_name in indexes_for_backup:
            _, restored_index_def = rest.get_fts_index_definition(ix_name)
            index_definitions[ix_name]['restored_def'] = restored_index_def

        #compare all 3 types of index definitions: initial, backed up, and restored from backup
        errors = self._check_indexes_definitions(index_definitions=index_definitions,
                                                 indexes_for_backup=indexes_for_backup)

        #errors analysis
        if len(errors.keys()) > 0:
            err_msg = ""
            for err in errors.keys():
                index_errors = errors[err]
                for msg in index_errors:
                    err_msg = err_msg + msg + "\n"
            test_errors.append(err_msg)

        fts_callable.flush_buckets(["default"])
        return test_errors

    def _check_indexes_definitions(self, index_definitions={}, indexes_for_backup=[]):
        errors = {}

        #check backup filters
        for ix_name in indexes_for_backup:
            if index_definitions[ix_name]['backup_def'] == {} and ix_name in indexes_for_backup:
                error = f"Index {ix_name} is expected to be in backup, but it is not found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {} and ix_name not in indexes_for_backup:
                error = f"Index {ix_name} is not expected to be in backup, but it is found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        #check backup json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                backup_index_defn = index_definitions[ix_name]['backup_def']

                backup_check = self._validate_backup(backup_index_defn, initial_index_defn)
                if not backup_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(
                        f"Backup fts index signature differs from original signature for index {ix_name}.")

        #check restored json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['restored_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                restored_index_defn = index_definitions[ix_name]['restored_def']['indexDef']
                restore_check = self._validate_restored(restored_index_defn, initial_index_defn)
                if not restore_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(
                        f"Restored fts index signature differs from original signature for index {ix_name}")

        return errors

    def _validate_backup(self, backup, initial):
        if 'uuid' in initial.keys():
            del initial['uuid']
        if 'sourceUUID' in initial.keys():
            del initial['sourceUUID']
        if 'uuid' in backup.keys():
            del backup['uuid']
        return backup == initial

    def _validate_restored(self, restored, initial):
        del restored['uuid']
        if 'kvStoreName' in restored['params']['store'].keys():
            del restored['params']['store']['kvStoreName']
        if restored != initial:
            self.log(f"Initial index JSON: {initial}")
            self.log(f"Restored index JSON: {restored}")
            return False
        return True

    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users, 'builtin', self.master)
        self.log.info("SUCCESS: User(s) %s created"
                      % ','.join([user['name'] for user in users]))

    def assign_role(self, rest=None, roles=None):
        if not rest:
            rest = RestConnection(self.master)
        #Assign roles to users
        if not roles:
            roles = self.roles
        RbacBase().add_user_role(roles, rest, 'builtin')
        for user_role in roles:
            self.log.info("SUCCESS: Role(s) %s assigned to %s"
                          % (user_role['roles'], user_role['id']))

    def create_index_with_credentials(self, username, password, index_name, bucket_name="default",
                                      collection_index=False, _type=None, analyzer="standard", scope=None,
                                      collections=None):
        from pytests.fts.fts_base import CouchbaseCluster
        cb_cluster = CouchbaseCluster(name="C1", nodes=self.servers, log=self.log)

        index = FTSIndex(cb_cluster, name=index_name, source_name=bucket_name, scope=scope, collections=collections)
        if collection_index:
            if type(_type) is list:
                for typ in _type:
                    index.add_type_mapping_to_index_definition(type=typ, analyzer=analyzer)
            else:
                index.add_type_mapping_to_index_definition(type=_type, analyzer=analyzer)

            doc_config = {}
            doc_config['mode'] = 'scope.collection.type_field'
            doc_config['type_field'] = "type"
            index.index_definition['params']['doc_config'] = {}
            index.index_definition['params']['doc_config'] = doc_config

        rest = self.get_rest_handle_for_credentials(username, password)
        index.create(rest)
        return index

    def get_rest_handle_for_credentials(self, user, password):
        fts_node = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)
        rest = RestConnection(fts_node)
        rest.username = user
        rest.password = password
        return rest

    def get_user_list(self, inp_users=None):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
        'password': 'passw0rd'}
        """
        user_list = []
        for user in inp_users:
            user_list.append({att: user[att] for att in ('id',
                                                         'name',
                                                         'password')})
        return user_list

    def get_user_role_list(self, inp_users=None):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
         'roles': 'admin:fts_admin[default]'}
        """
        user_role_list = []
        for user in inp_users:
            user_role_list.append({att: user[att] for att in ('id',
                                                              'name',
                                                              'roles',
                                                              'password')})
        return user_role_list

    def create_alias_with_credentials(self, username, password, alias_name,
                                      target_indexes):
        from pytests.fts.fts_base import CouchbaseCluster
        cb_cluster = CouchbaseCluster(name="C1", nodes=self.servers, log=self.log)

        alias_def = {"targets": {}}
        for index in target_indexes:
            alias_def['targets'][index.name] = {}
            alias_def['targets'][index.name]['indexUUID'] = index.get_uuid()
        alias = FTSIndex(cb_cluster, name=alias_name,
                         index_type='fulltext-alias', index_params=alias_def)
        rest = self.get_rest_handle_for_credentials(username, password)
        alias.create(rest)
        return alias

    def edit_index_with_credentials(self, index, username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        _, defn = index.get_index_defn(rest)
        self.log.info(f"Old definition: {defn['indexDef']}")
        new_plan_param = {"maxPartitionsPerPIndex": 10}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        index.update(rest)
        _, defn = index.get_index_defn()
        self.log.info(f"New definition: {defn['indexDef']}")

    def query_index_with_credentials(self, index, username, password):
        sample_query = {"match": "Safiya Morgan", "field": "name"}

        rest = self.get_rest_handle_for_credentials(username, password)
        self.log.info("Now querying with credentials %s:%s" % (username,
                                                               password))
        hits, _, _, _ = rest.run_fts_query(index.name,
                                           {"query": sample_query})
        self.log.info("Hits: %s" % hits)

    def delete_index_with_credentials(self, index, username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        index.delete(rest)

    def _test_rbac_admin(self):
        self.log.info("=" * 20 + " _test_rbac_admin")
        errors = []
        self._create_collections(scope="scope1", collection="collection1")

        users = [{"id": "johnDoe",
                  "name": "Jonathan Downing",
                  "password": "password1",
                  "roles": "fts_admin[default]:cluster_admin"
                  }]
        users_list = self.get_user_list(inp_users=users)
        roles_list = self.get_user_role_list(inp_users=users)

        self.create_users(users=users_list)
        self.assign_role(roles=roles_list)

        for user in users_list:
            try:
                collection_index = True
                _type = 'scope1.collection1'
                index_scope = 'scope1'
                index_collections = 'collection1'
                index = self.create_index_with_credentials(
                    username=user['id'],
                    password=user['password'],
                    index_name="%s_%s_idx" % (user['id'], "default"),
                    bucket_name="default",
                    collection_index=collection_index,
                    _type=_type,
                    scope=index_scope,
                    collections=index_collections
                )

                alias = self.create_alias_with_credentials(
                    username=user['id'],
                    password=user['password'],
                    target_indexes=[index],
                    alias_name="%s_%s_alias" % (user['id'], "default"))
                try:
                    self.edit_index_with_credentials(
                        index=index,
                        username=user['id'],
                        password=user['password'])
                    self.sleep(60, "Waiting for index rebuild after "
                                   "update...")
                    self.query_index_with_credentials(
                        index=index,
                        username=user['id'],
                        password=user['password'])
                    self.delete_index_with_credentials(
                        alias,
                        user['id'],
                        user['password'])
                    self.delete_index_with_credentials(
                        index=index,
                        username=user['id'],
                        password=user['password'])
                except Exception as e:
                    errors.append("The user failed to edit/query/delete fts "
                                  "index %s : %s" % (user['id'], e))
            except Exception as e:
                errors.append("The user failed to create fts index/alias"
                              " %s : %s" % (user['id'], e))
            return errors

    def _test_rbac_searcher(self):
        self.log.info("=" * 20 + " _test_rbac_searcher")
        errors = []
        self._create_collections(scope="scope1", collection="collection2")

        users = [{"id": "johnDoe", "name": "Jonathan Downing", "password": "password1",
                  "roles": "fts_searcher[default:scope1]"}]
        users_list = self.get_user_list(inp_users=users)
        roles_list = self.get_user_role_list(inp_users=users)

        self.create_users(users=users_list)
        self.assign_role(roles=roles_list)

        for user in users_list:
            try:
                collection_index = True
                _type = 'scope1.collection2'
                index_scope = 'scope1'
                index_collections = 'collection2'
                self.create_index_with_credentials(
                    username=user['id'],
                    password=user['password'],
                    index_name="%s_%s_idx" % (user['id'], "default"),
                    bucket_name="default",
                    collection_index=collection_index,
                    _type=_type,
                    scope=index_scope,
                    collections=index_collections
                )
            except Exception as e:
                self.log.info("Expected exception: %s" % e)
            else:
                errors.append("An fts_searcher is able to create index!")

            # creating an alias
            try:
                self.log.info("Creating index as administrator...")
                collection_index = True
                _type = 'scope1.collection2'
                index_scope = 'scope1'
                index_collections = 'collection2'
                index = self.create_index_with_credentials(
                    username='Administrator',
                    password='password',
                    index_name="%s_%s_idx" % ('Admin', "default"),
                    bucket_name="default",
                    collection_index=collection_index,
                    _type=_type,
                    scope=index_scope,
                    collections=index_collections
                )
                self.log.info("Creating alias as fts_searcher...")
                self.create_alias_with_credentials(
                    username=user['id'],
                    password=user['password'],
                    target_indexes=[index],
                    alias_name="%s_%s_alias" % (user['id'], "default"))
            except Exception as e:
                self.log.info(f"Expected exception: {e}")
            else:
                errors.append("An fts_searcher is able to create alias!")
        return errors

    def _test_flex_pushdown_in(self):
        self.log.info("=" * 20 + " _test_flex_pushdown_in")
        errors = []
        self._create_collections(scope="scope1", collection="collection10")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection10", collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection",
                                                                  scope="scope1", collection="collection10")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type,
                                                analyzer="keyword", scope="scope1", collections=["collection10"],
                                                no_check=False)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100)

        _data_types = {
            "text": {"field": "type", "vals": ["emp", "emp1"]},
            "number": {"field": "mutated", "vals": [0, 1]},
            "boolean": {"field": "is_manager", "vals": [True, False]},
            "datetime": {"field": "join_date", "vals": ["1970-07-02T11:50:10", "1951-11-16T13:37:10"]}
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for _key in _data_types.keys():
            flex_query = "select count(*) from `default`.scope1.collection10 USE INDEX({0}) where {1} in {2}". \
                format(index_hint, _data_types[_key]['field'], _data_types[_key]['vals'])
            gsi_query = "select count(*) from `default`.scope1.collection10 where {1} in {2}". \
                format(index_hint, _data_types[_key]['field'], _data_types[_key]['vals'])
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        n1ql_obj = N1QLCallable(self.servers)
        n1ql_obj.run_n1ql_query(query="create primary index on `default`.scope1.collection10")
        self.sleep(10)
        for test in tests:
            result = n1ql_obj.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        n1ql_obj.run_n1ql_query(query="drop primary index on `default`.scope1.collection10")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=False)
        if errors_found:
            errors.append("Errors are detected for IN/NOT Flex queries. Check logs for details.")
        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _perform_results_checks(self, tests=None, index_configuration="", custom_mapping=False, check_pushdown=True):
        n1q1_obj = N1QLCallable(self.servers)
        for test in tests:
            result = n1q1_obj.run_n1ql_query("explain " + test['flex_query'])
            if check_pushdown:
                if "index_group_aggs" not in str(result):
                    error = {}
                    error['error_message'] = "Index aggregate pushdown is not detected."
                    error['query'] = test['flex_query']
                    error['indexing_config'] = index_configuration
                    error['custom_mapping'] = str(custom_mapping)
                    error['collections'] = str(self.collection)
                    test['errors'].append(error)
            result = n1q1_obj.run_n1ql_query(test['flex_query'])

            test['flex_result'] = result['results']
            if result['status'] != 'success':
                error = {}
                error['error_message'] = "Flex query was not executed successfully."
                error['query'] = test['flex_query']
                error['indexing_config'] = index_configuration
                error['custom_mapping'] = str(custom_mapping)
                error['collections'] = str(self.collection)
                test['errors'].append(error)
            if test['flex_result'] != test['gsi_result']:
                error = {}
                error['error_message'] = "Flex query results and GSI query results are different."
                error['query'] = test['flex_query']
                error['indexing_config'] = index_configuration
                error['custom_mapping'] = str(custom_mapping)
                error['collections'] = str(self.collection)
                test['errors'].append(error)

        errors_found = False
        for test in tests:
            if len(test['errors']) > 0:
                errors_found = True
                self.log.error("The following errors are detected:\n")
                for error in test['errors']:
                    self.log.error("=" * 10)
                    self.log.error(error['error_message'])
                    self.log.error("query: " + error['query'])
                    self.log.error("indexing config: " + error['indexing_config'])
                    self.log.error("custom mapping: " + str(error['custom_mapping']))
                    self.log.error("collections set: " + str(error['collections']))
        return errors_found

    def _test_flex_pushdown_like(self):
        self.log.info("=" * 20 + " _test_flex_pushdown_like")
        errors = []
        self._create_collections(scope="scope1", collection="collection11")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection11", collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection11")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection11"], no_check=False)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100)

        check_pushdown = False

        like_types = ["left", "right", "left_right"]
        like_conditions = ["LIKE"]
        _data_types = {
            "text": {"field": "type", "vals": "emp"},
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for _key in _data_types.keys():
            for like_type in like_types:
                for like_condition in like_conditions:
                    if like_type == "left":
                        like_expression = "'%" + _data_types[_key]['vals'] + "'"
                    elif like_type == "right":
                        like_expression = "'" + _data_types[_key]['vals'] + "%'"
                    else:
                        like_expression = "'%" + _data_types[_key]['vals'] + "%'"
                    flex_query = "select count(*) from `default`.scope1.collection11 USE INDEX({0}) where {1} {2} {3}". \
                        format(index_hint, _data_types[_key]['field'], like_condition, like_expression)
                    gsi_query = "select count(*) from `default`.scope1.collection11 where {1} {2} {3}". \
                        format(index_hint, _data_types[_key]['field'], like_condition, like_expression)
                    test = {}
                    test['flex_query'] = flex_query
                    test['gsi_query'] = gsi_query
                    test['flex_result'] = {}
                    test['flex_explain'] = {}
                    test['gsi_result'] = {}
                    test['errors'] = []
                    tests.append(test)

        n1ql_obj = N1QLCallable(self.servers)
        n1ql_obj.run_n1ql_query("create primary index on `default`.scope1.collection11")
        self.sleep(10)
        for test in tests:
            result = n1ql_obj.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        n1ql_obj.run_n1ql_query("drop primary index on `default`.scope1.collection11")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)

        if errors_found:
            errors.append("Errors are detected for LIKE Flex queries. Check logs for details.")
        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_pushdown_sort(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection12")
        fts_callable = FTSCallable(self.servers, es_validate=False,
                                   es_reset=False, scope="scope1", collections="collection12", collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection12")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection12"], no_check=False)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100)

        check_pushdown = False

        sort_directions = ["ASC", "DESC", ""]
        limits = ["LIMIT 10", ""]
        offsets = ["OFFSET 5", ""]
        custom_mapping = False
        _data_types = {
            "text": {"field": "type", "flex_condition": "type='emp'"},
            "number": {"field": "mutated", "flex_condition": "mutated=0"},
            "boolean": {"field": "is_manager", "flex_condition": "is_manager=true"},
            "datetime": {"field": "join_date",
                         "flex_condition": "join_date > '2001-10-09' AND join_date < '2020-10-09'"}
        }
        index_configuration = "FTS"
        index_hint = "USING FTS"

        tests = []
        for _key in _data_types.keys():
            for sort_direction in sort_directions:
                for limit in limits:
                    for offset in offsets:
                        flex_query = "select meta().id from `default`.scope1.collection12 USE INDEX({0}) where {1} order by {2} {3} {4} {5}". \
                            format(index_hint, _data_types[_key]['flex_condition'], "meta().id", sort_direction, limit,
                                   offset)
                        gsi_query = "select meta().id from `default`.scope1.collection12 USE INDEX({0}) where {1} order by {2} {3} {4} {5}". \
                            format(index_hint, _data_types[_key]['flex_condition'], "meta().id", sort_direction, limit,
                                   offset)
                        test = {}
                        test['flex_query'] = flex_query
                        test['gsi_query'] = gsi_query
                        test['flex_result'] = {}
                        test['flex_explain'] = {}
                        test['gsi_result'] = {}
                        test['errors'] = []
                        tests.append(test)

        n1ql_obj = N1QLCallable(self.servers)
        n1ql_obj.run_n1ql_query("create primary index on `default`.scope1.collection12")
        self.sleep(10)
        for test in tests:
            result = n1ql_obj.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        n1ql_obj.run_n1ql_query("drop primary index on `default`.scope1.collection12")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=False)
        if errors_found:
            errors.append("Errors are detected for ORDER BY Flex queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_doc_id(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection13")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection13", collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection13")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection13"], no_check=False)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['params']['doc_config']['docid_prefix_delim'] = "_"
        fts_idx.index_definition['params']['doc_config']['docid_regexp'] = ""
        fts_idx.index_definition['params']['doc_config']['mode'] = "scope.collection.docid_prefix"
        fts_idx.index_definition['params']['doc_config']['type_field'] = "type"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100)

        check_pushdown = False
        like_expressions = ["LIKE"]
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for like_expression in like_expressions:
            flex_query = "select count(*) from `default`.scope1.collection13 USE INDEX({0}) where meta().id {1} 'emp_%' and type='emp'". \
                format(index_hint, like_expression)
            gsi_query = "select count(*) from `default`.scope1.collection13 where meta().id {1} 'emp_%' and type='emp'". \
                format(index_hint, like_expression)
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        n1ql_obj = N1QLCallable(self.servers)
        n1ql_obj.run_n1ql_query("create primary index on `default`.scope1.collection13")
        self.sleep(10)
        for test in tests:
            result = n1ql_obj.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        n1ql_obj.run_n1ql_query("drop primary index on `default`.scope1.collection13")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            errors.append("Errors are detected for DOC_ID prefix Flex queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_pushdown_negative_numeric_ranges(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection14")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection14", collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection14")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection14"], no_check=False)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100)

        check_pushdown = False
        relations = ['<', '<=', '=', '>', '>=']
        _data_types = {
            "number": {"field": "salary"}
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for relation in relations:
            condition = ""
            if relation == '<':
                condition = ' salary > -100 and salary < -10'
            elif relation == '<=':
                condition = ' salary >= -100 and salary <= -10'
            elif relation == '>':
                condition = ' salary > -10 and salary < -1'
            elif relation == '>=':
                condition = ' salary >= -10 and salary <= -1'
            elif relation == "=":
                condition = ' salary = -10 '

            flex_query = "select count(*) from `default`.scope1.collection14 USE INDEX({0}) where {1}". \
                format(index_hint, condition)
            gsi_query = "select count(*) from `default`.scope1.collection14 USE INDEX({0}) where {1}". \
                format(index_hint, condition)
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        n1ql_obj = N1QLCallable(self.servers)
        n1ql_obj.run_n1ql_query("create primary index on `default`.scope1.collection14")
        self.sleep(10)
        for test in tests:
            result = n1ql_obj.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        n1ql_obj.run_n1ql_query("drop primary index on `default`.scope1.collection14")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            errors.append("Errors are detected for negative numeric ranges Flex queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_and_search_pushdown(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection15")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection15", collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection15")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection15"], no_check=False)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100)

        check_pushdown = False
        _data_types = {
            "text": {"field": "type",
                     "search_condition": "{'query':{'field': 'type', 'match':'emp'}}",
                     "flex_condition": "a.`type`='emp'"},
            "number": {"field": "salary",
                       "search_condition": "{'query':{'min': 1000, 'max': 100000, 'field': 'salary'}}",
                       "flex_condition": "a.salary>1000 and a.salary<100000"},
            "boolean": {"field": "is_manager",
                        "search_condition": "{'query':{'bool': true, 'field': 'is_manager'}}",
                        "flex_condition": "a.is_manager=true"},
            "datetime": {"field": "join_date",
                         "search_condition": "{'start': '2001-10-09', 'end': '2016-10-31', 'field': 'join_date'}",
                         "flex_condition": "a.join_date > '2001-10-09' and a.join_date < '2016-10-31'"}
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for _key1 in _data_types.keys():
            for _key2 in _data_types.keys():
                flex_query = "select count(*) from `default`.scope1.collection15 a USE INDEX({0}) where {1} and search(a, {2})". \
                    format(index_hint, _data_types[_key1]['flex_condition'], _data_types[_key2]['search_condition'])
                gsi_query = "select count(*) from `default`.scope1.collection15 a where {0} and search(a, {1})". \
                    format(_data_types[_key1]['flex_condition'], _data_types[_key2]['search_condition'])
                test = {}
                test['flex_query'] = flex_query
                test['gsi_query'] = gsi_query
                test['flex_result'] = {}
                test['flex_explain'] = {}
                test['gsi_result'] = {}
                test['errors'] = []
                tests.append(test)

        n1ql_obj = N1QLCallable(self.servers)
        n1ql_obj.run_n1ql_query("create primary index on `default`.scope1.collection15")
        self.sleep(10)
        for test in tests:
            result = n1ql_obj.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        n1ql_obj.run_n1ql_query("drop primary index on `default`.scope1.collection15")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            errors.append("Errors are detected for Flex + Search queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    test_data = {
        "doc_1": {
            "num": 1,
            "str": "str_1",
            "bool": True,
            "array": ["array1_1", "array1_2"],
            "obj": {"key": "key1", "val": "val1"},
            "filler": "filler"
        },
        "doc_2": {
            "num": 2,
            "str": "str_2",
            "bool": False,
            "array": ["array2_1", "array2_2"],
            "obj": {"key": "key2", "val": "val2"},
            "filler": "filler"
        },
        "doc_3": {
            "num": 3,
            "str": "str_3",
            "bool": True,
            "array": ["array3_1", "array3_2"],
            "obj": {"key": "key3", "val": "val3"},
            "filler": "filler"
        },
        "doc_4": {
            "num": 4,
            "str": "str_4",
            "bool": False,
            "array": ["array4_1", "array4_2"],
            "obj": {"key": "key4", "val": "val4"},
            "filler": "filler"
        },
        "doc_5": {
            "num": 5,
            "str": "str_5",
            "bool": True,
            "array": ["array5_1", "array5_2"],
            "obj": {"key": "key5", "val": "val5"},
            "filler": "filler"
        },
        "doc_10": {
            "num": 10,
            "str": "str_10",
            "bool": False,
            "array": ["array10_1", "array10_2"],
            "obj": {"key": "key10", "val": "val10"},
            "filler": "filler"
        },
    }

    def _load_search_before_search_after_test_data(self, bucket, test_data):
        n1ql_obj = N1QLCallable(self.servers)
        for key in test_data:
            query = "insert into " + bucket + " (KEY, VALUE) VALUES " \
                                              "('" + str(key) + "', " \
                                                                "" + str(test_data[key]) + ")"
            n1ql_obj.run_n1ql_query(query=query)

    def _test_search_before(self):
        errors = []
        bucket = RestConnection(self.master).get_bucket_by_name('default')[0]
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, collection_index=False)

        _type = None

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=_type, analyzer="standard", no_check=False)
        fts_callable.wait_for_indexing_complete(len(self.test_data))

        full_size = len(self.test_data)
        partial_size = 1
        partial_start_index = 3
        sort_mode = ['_id']

        cluster = fts_idx.get_cluster()
        self.sleep(10)
        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                         "query": {"match": "filler", "field": "filler"}, "size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_idx.name, all_fts_query)
        if all_hits is None or all_matches is None:
            errors.append(f"test is failed: no results were returned by fts query: {all_fts_query}")
            return errors
        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                                   "query": {"match": "filler", "field": "filler"}, "size": partial_size,
                                   "sort": sort_mode, "search_before": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(fts_idx.name, search_before_fts_query)

        all_results_ids = []
        search_before_results_ids = []

        for match in all_matches:
            all_results_ids.append(match['id'])

        for match in search_before_matches:
            search_before_results_ids.append(match['id'])

        for i in range(0, partial_size - 1):
            if i in range(0, len(search_before_results_ids) - 1):
                if search_before_results_ids[i] != all_results_ids[partial_start_index - partial_size + i]:
                    errors.append("test is failed")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_search_after(self):
        errors = []
        bucket = RestConnection(self.master).get_bucket_by_name('default')[0]
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, collection_index=False)

        _type = None

        index = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                              source_name="default", index_type='fulltext-index',
                                              index_params=None, plan_params=None,
                                              source_params=None, source_uuid=None, collection_index=False,
                                              _type=_type, analyzer="standard", no_check=False)
        fts_callable.wait_for_indexing_complete(len(self.test_data))
        self.sleep(10)

        full_size = len(self.test_data)
        partial_size = 1
        partial_start_index = 3
        sort_mode = ['_id']

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                         "query": {"match": "filler", "field": "filler"}, "size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                                   "query": {"match": "filler", "field": "filler"}, "size": partial_size,
                                   "sort": sort_mode, "search_after": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)
        all_results_ids = []
        search_before_results_ids = []

        for match in all_matches:
            all_results_ids.append(match['id'])

        for match in search_before_matches:
            search_before_results_ids.append(match['id'])

        for i in range(0, partial_size - 1):
            if i in range(0, len(search_before_results_ids) - 1):
                if search_before_results_ids[i] != all_results_ids[partial_start_index + 1 + i]:
                    errors.append("test is failed")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_new_metrics(self, endpoint=None):
        errors = []
        fts_node = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)

        self._create_collections(scope="scope1", collection="collection25")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection25", collection_index=True)
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection25")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard", scope="scope1",
                                                collections=["collection25"], no_check=False)
        fts_callable.wait_for_indexing_complete(100)
        rest = RestConnection(fts_node)
        fts_port = fts_node.fts_port or self.fts_port
        status, content = rest.get_rest_endpoint_data(endpoint, ip=fts_node.ip, port=fts_port)
        if not status:
            errors.append(f"Endpoint {endpoint} is not accessible.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def get_nodes_in_cluster_after_upgrade(self, master_node=None):
        if master_node is None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.input.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set

    def gsi_scope_free_tier(self, bucket, scope, collection, scope_tier_limit):
        expected_err = 'Limit for number of indexes that can be created per scope has been reached'
        n1ql_obj = N1QLCallable(self.servers)
        n1ql_obj.set_gsi_scope_tier_limit(bucket=bucket, scope=scope, scope_tier_limit=scope_tier_limit)
        keyspace = f'{bucket}.{scope}.{collection}'
        for item in range(scope_tier_limit + 2):
            index_name = f'idx_tier_limit_{item}'
            try:
                n1ql_obj.create_gsi_index(keyspace=keyspace, name=index_name,
                                          fields="email", using="gsi", is_primary=False,
                                          index_condition="")
            except Exception as err:
                if expected_err not in str(err):
                    self.fail(err)

    def _create_collections(self, scope=None, collection=None):
        rest = RestConnection(self.master)
        rest.create_scope(bucket="default", scope=scope)
        if type(collection) is list:
            for c in collection:
                rest.create_collection(bucket="default", scope=scope, collection=c)
        else:
            rest.create_collection(bucket="default", scope=scope, collection=collection)

    @staticmethod
    def __define_index_parameters_collection_related(container_type="bucket", scope=None, collection=None):
        if container_type == 'bucket':
            _type = "emp"
        else:
            index_collections = []
            if type(collection) is list:
                _type = []
                for c in collection:
                    _type.append(f"{scope}.{c}")
                    index_collections.append(c)
            else:
                _type = f"{scope}.{collection}"
                index_collections.append(collection)
        return _type
