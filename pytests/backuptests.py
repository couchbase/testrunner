import time
import unittest
import uuid
import crc32
from TestInput import TestInputSingleton
import logger
import mc_bin_client
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from builds.build_query import BuildQuery
import testconstants
import copy

from basetestcase import BaseTestCase

class BackupRestoreTests(BaseTestCase):
    input = None
    servers = None
    log = None
    membase = None
    shell = None
    remote_tmp_folder = None
    master = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.shell = RemoteMachineShellConnection(self.master)

        # When using custom data_paths, (smaller // sizes), creates
        # backup in those custom paths ( helpful when running on ec2)
        info = RestConnection(self.master).get_nodes_self()
        data_path = info.storage[0].get_data_path()
        self.remote_tmp_folder = None
        self.remote_tmp_folder = "{2}/{0}-{1}".format("backup", uuid.uuid4(), data_path)
        self.is_membase = False
        self.perm_command = "mkdir -p {0}".format(self.remote_tmp_folder)
        if not self.shell.is_couchbase_installed():
            self.is_membase = True


    def common_setUp(self):
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.stop_membase()
            shell.stop_couchbase()
            shell.start_membase()
            shell.start_couchbase()
            RestHelper(RestConnection(server)).is_ns_server_running(timeout_in_seconds=120)
            shell.disconnect()

    def tearDown(self):

        for server in self.servers:
            self.log.info("delete remote folder @ {0}".format(self.remote_tmp_folder))
            shell = RemoteMachineShellConnection(server)
            shell.remove_directory(self.remote_tmp_folder)
            shell.disconnect()

    def add_node_and_rebalance(self, master, servers):
        ClusterOperationHelper.add_all_nodes_or_assert(master, servers, self.input.membase_settings, self)
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        otpNodeIds = []
        for node in nodes:
            otpNodeIds.append(node.id)
        rebalanceStarted = rest.rebalance(otpNodeIds, [])
        self.assertTrue(rebalanceStarted,
                        "unable to start rebalance on master node {0}".format(master.ip))
        self.log.info('started rebalance operation on master node {0}'.format(master.ip))
        rebalanceSucceeded = rest.monitorRebalance()
        self.assertTrue(rebalanceSucceeded,
                        "rebalance operation for nodes: {0} was not successful".format(otpNodeIds))
        self.log.info('rebalance operaton succeeded for nodes: {0}'.format(otpNodeIds))
        #now remove the nodes
        #make sure its rebalanced and node statuses are healthy
        helper = RestHelper(rest)
        self.assertTrue(helper.is_cluster_healthy, "cluster status is not healthy")
        self.assertTrue(helper.is_cluster_rebalanced, "cluster is not balanced")

    def add_nodes_and_rebalance(self):
        self.add_node_and_rebalance(master=self.servers[0], servers=self.servers)

    def _test_backup_add_restore_bucket_body(self,
                                             bucket,
                                             delay_after_data_load,
                                             startup_flag,
                                             single_node):
        server = self.master
        rest = RestConnection(server)
        info = rest.get_nodes_self()
        size = int(info.memoryQuota * 2.0 / 3.0)
        if bucket == "default":
            rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=info.moxi)
        else:
            proxyPort = info.moxi + 500
            rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=proxyPort,
                               authType="sasl", saslPassword="password")

        ready = BucketOperationHelper.wait_for_memcached(server, bucket)
        self.assertTrue(ready, "wait_for_memcached failed")
        if not single_node:
            self.add_nodes_and_rebalance()
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             name=bucket,
                                                                                             ram_load_ratio=1,
                                                                                             value_size_distribution=distribution,
                                                                                             moxi=True,
                                                                                             write_only=True,
                                                                                             number_of_threads=2)

        if not single_node:
            rest = RestConnection(self.master)
            self.assertTrue(RebalanceHelper.wait_for_replication(rest.get_nodes(), timeout=180),
                            msg="replication did not complete")

        self.log.info("Sleep {0} seconds after data load".format(delay_after_data_load))
        ready = RebalanceHelper.wait_for_persistence(self.master, bucket, bucket_type=self.bucket_type)
        self.assertTrue(ready, "not all items persisted. see logs")
        node = RestConnection(self.master).get_nodes_self()
        if not startup_flag:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.stop_membase()
                shell.stop_couchbase()
                shell.disconnect()

        output, error = self.shell.execute_command(self.perm_command)
        self.shell.log_command_output(output, error)

        #now let's back up
        BackupHelper(self.master, self).backup(bucket, node, self.remote_tmp_folder)

        if not startup_flag:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.start_membase()
                shell.start_couchbase()
                RestHelper(RestConnection(server)).is_ns_server_running()
                shell.disconnect()

        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)

        if bucket == "default":
            rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=info.moxi)
        else:
            proxyPort = info.moxi + 500
            rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=proxyPort,
                               authType="sasl", saslPassword="password")
        BucketOperationHelper.wait_for_memcached(self.master, bucket)

        if bucket == "default":
            BackupHelper(self.master, self).restore(backup_location=self.remote_tmp_folder, moxi_port=info.moxi)
        else:
            BackupHelper(self.master, self).restore(backup_location=self.remote_tmp_folder, moxi_port=info.moxi, username=bucket, password='password')

        keys_exist = BucketOperationHelper.keys_exist_or_assert_in_parallel(inserted_keys, self.master, bucket, self, concurrency=4)
        self.assertTrue(keys_exist, msg="unable to verify keys after restore")

    def _test_backup_add_restore_bucket_with_expiration_key(self, replica):
        bucket = "default"
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        size = int(info.memoryQuota * 2.0 / 3.0)
        rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=info.moxi, replicaNumber=replica)
        BucketOperationHelper.wait_for_memcached(self.master, bucket)
        client = MemcachedClientHelper.direct_client(self.master, bucket)
        expiry = 60
        test_uuid = uuid.uuid4()
        keys = ["key_%s_%d" % (test_uuid, i) for i in range(5000)]
        self.log.info("pushing keys with expiry set to {0}".format(expiry))
        for key in keys:
            try:
                client.set(key, expiry, 0, key)
            except mc_bin_client.MemcachedError as error:
                msg = "unable to push key : {0} to bucket : {1} error : {2}"
                self.log.error(msg.format(key, client.vbucketId, error.status))
                self.fail(msg.format(key, client.vbucketId, error.status))
        client.close()
        self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry))
        ready = RebalanceHelper.wait_for_persistence(self.master, bucket, bucket_type=self.bucket_type)
        self.assertTrue(ready, "not all items persisted. see logs")
        node = RestConnection(self.master).get_nodes_self()

        output, error = self.shell.execute_command(self.perm_command)
        self.shell.log_command_output(output, error)
        backupHelper = BackupHelper(self.master, self)
        backupHelper.backup(bucket, node, self.remote_tmp_folder)

        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)
        rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=info.moxi)
        BucketOperationHelper.wait_for_memcached(self.master, bucket)
        backupHelper.restore(self.remote_tmp_folder)
        time.sleep(60)
        client = MemcachedClientHelper.direct_client(self.master, bucket)
        self.log.info('verifying that all those keys have expired...')
        for key in keys:
            try:
                client.get(key=key)
                msg = "expiry was set to {0} but key: {1} did not expire after waiting for {2}+ seconds"
                self.fail(msg.format(expiry, key, expiry))
            except mc_bin_client.MemcachedError as error:
                self.assertEqual(error.status, 1,
                                  msg="expected error code {0} but saw error code {1}".format(1, error.status))
        client.close()
        self.log.info("verified that those keys inserted with expiry set to {0} have expired".format(expiry))

    def _test_backup_and_restore_bucket_overwriting_body(self, overwrite_flag=True):
        bucket = "default"
        BucketOperationHelper.create_bucket(serverInfo=self.master, test_case=self)
        BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.add_nodes_and_rebalance()

        client = MemcachedClientHelper.direct_client(self.master, "default")
        expiry = 2400
        test_uuid = uuid.uuid4()
        keys = ["key_%s_%d" % (test_uuid, i) for i in range(500)]
        self.log.info("pushing keys with expiry set to {0}".format(expiry))
        for key in keys:
            try:
                client.set(key, expiry, 0, "1")
            except mc_bin_client.MemcachedError as error:
                msg = "unable to push key : {0} to bucket : {1} error : {2}"
                self.log.error(msg.format(key, client.vbucketId, error.status))
                self.fail(msg.format(key, client.vbucketId, error.status))
        self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry))

        ready = RebalanceHelper.wait_for_persistence(self.master, bucket, bucket_type=self.bucket_type)
        self.assertTrue(ready, "not all items persisted. see logs")

        for server in self.servers:
            shell = RemoteMachineShellConnection(server)

            output, error = shell.execute_command(self.perm_command)
            shell.log_command_output(output, error)
            node = RestConnection(server).get_nodes_self()
            BackupHelper(server, self).backup(bucket, node, self.remote_tmp_folder)
            shell.disconnect()

        for key in keys:
            try:
                client.replace(key, expiry, 0, "2")
            except mc_bin_client.MemcachedError as error:
                msg = "unable to replace key : {0} in bucket : {1} error : {2}"
                self.log.error(msg.format(key, client.vbucketId, error.status))
                self.fail(msg.format(key, client.vbucketId, error.status))
        self.log.info("replaced {0} keys with expiry set to {1}".format(len(keys), expiry))

        for server in self.servers:
            BackupHelper(server, self).restore(self.remote_tmp_folder, overwrite_flag)
            time.sleep(10)

        self.log.info('verifying that all those keys...')
        for key in keys:
            if overwrite_flag:
                self.assertEqual("2", client.get(key=key), key + " should has value = 2")
            else:
                self.assertNotEqual("2", client.get(key=key), key + " should not has value = 2")
        self.log.info("verified that those keys inserted with expiry set to {0} have expired".format(expiry))

    def _test_cluster_topology_change_body(self):
        bucket = "default"
        BucketOperationHelper.create_bucket(serverInfo=self.master, test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.assertTrue(ready, "wait_for_memcached failed")
        self.add_nodes_and_rebalance()

        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}

        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             ram_load_ratio=1,
                                                                                             value_size_distribution=distribution,
                                                                                             moxi=True,
                                                                                             write_only=True,
                                                                                             number_of_threads=2)

        self.log.info("Sleep after data load")
        ready = RebalanceHelper.wait_for_persistence(self.master, bucket, bucket_type=self.bucket_type)
        self.assertTrue(ready, "not all items persisted. see logs")
        #let's create a unique folder in the remote location
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.execute_command(self.perm_command)
            shell.log_command_output(output, error)
            node = RestConnection(server).get_nodes_self()
            BackupHelper(server, self).backup(bucket, node, self.remote_tmp_folder)
            shell.disconnect()

        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)

        servers = []
        for i in range(0, len(self.servers) - 1):
            servers.append(self.servers[i])

        self.add_node_and_rebalance(servers[0], servers)

        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)
        BucketOperationHelper.create_bucket(serverInfo=self.master, test_case=self)

        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.assertTrue(ready, "wait_for_memcached failed")

        for server in self.servers:
            BackupHelper(server, self).restore(self.remote_tmp_folder)
            time.sleep(10)

        BucketOperationHelper.verify_data(self.master, inserted_keys, False, False, 11210, self)

    def _test_delete_key_and_backup_and_restore_body(self):
        bucket = "default"
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket, test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.assertTrue(ready, "wait_for_memcached failed")

        self.add_nodes_and_rebalance()

        client = MemcachedClientHelper.direct_client(self.master, "default")
        expiry = 2400
        test_uuid = uuid.uuid4()
        keys = ["key_%s_%d" % (test_uuid, i) for i in range(500)]
        self.log.info("pushing keys with expiry set to {0}".format(expiry))
        for key in keys:
            try:
                client.set(key, expiry, 0, "1")
            except mc_bin_client.MemcachedError as error:
                msg = "unable to push key : {0} to bucket : {1} error : {2}"
                self.log.error(msg.format(key, client.vbucketId, error.status))
                self.fail(msg.format(key, client.vbucketId, error.status))
        self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry))

        client.delete(keys[0])

        ready = RebalanceHelper.wait_for_persistence(self.master, bucket, bucket_type=self.bucket_type)
        self.assertTrue(ready, "not all items persisted. see logs")
        #let's create a unique folder in the remote location
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.execute_command(self.perm_command)
            shell.log_command_output(output, error)
            node = RestConnection(server).get_nodes_self()
            BackupHelper(server, self).backup(bucket, node, self.remote_tmp_folder)
            shell.disconnect()

        for server in self.servers:
            BackupHelper(server, self).restore(self.remote_tmp_folder)
            time.sleep(10)

        self.log.info('verifying that all those keys...')
        missing_keys = []
        verify_keys = []
        for key in keys:
            vBucketId = crc32.crc32_hash(key) & 1023  # or & 0x3FF
            client.vbucketId = vBucketId
            if key == keys[0]:
                missing_keys.append(key)
            else:
                verify_keys.append(key)

        self.assertTrue(BucketOperationHelper.keys_dont_exist(self.master, missing_keys, self),
                        "Keys are not empty")
        self.assertTrue(BucketOperationHelper.verify_data(self.master, verify_keys, False, False, 11210, self),
                        "Missing keys")

    def _test_backup_and_restore_on_different_port_body(self):
        bucket_before_backup = "bucket_before_backup"
        bucket_after_backup = "bucket_after_backup"
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket_before_backup, port=11212,
                                            test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket_before_backup)
        self.assertTrue(ready, "wait_for_memcached failed")

        self.add_nodes_and_rebalance()

        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}

        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             name=bucket_before_backup,
                                                                                             ram_load_ratio=1,
                                                                                             value_size_distribution=distribution,
                                                                                             write_only=True,
                                                                                             moxi=True,
                                                                                             number_of_threads=2)

        self.log.info("Sleep after data load")
        ready = RebalanceHelper.wait_for_persistence(self.master, bucket, bucket_type=self.bucket_type)
        self.assertTrue(ready, "not all items persisted. see logs")
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.execute_command(self.perm_command)
            shell.log_command_output(output, error)
            node = RestConnection(server).get_nodes_self()
            BackupHelper(server, self).backup(bucket_before_backup, node, self.remote_tmp_folder)
            shell.disconnect()

        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket_before_backup, self)
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket_after_backup, port=11213,
                                            test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket_after_backup)
        self.assertTrue(ready, "wait_for_memcached failed")

        for server in self.servers:
            BackupHelper(server, self).restore(self.remote_tmp_folder, moxi_port=11213)
            time.sleep(10)

        self.assertTrue(BucketOperationHelper.verify_data(self.master, inserted_keys, False, False, 11213, debug=False,
                                                          bucket=bucket_after_backup), "Missing keys")

    def _test_backup_and_restore_from_to_different_buckets(self):
        bucket_before_backup = "bucket_before_backup"
        bucket_after_backup = "bucket_after_backup"
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket_before_backup, port=11212,
                                            test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket_before_backup)
        self.assertTrue(ready, "wait_for_memcached failed")

        self.add_nodes_and_rebalance()

        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             name=bucket_before_backup,
                                                                                             ram_load_ratio=20,
                                                                                             value_size_distribution=distribution,
                                                                                             write_only=True,
                                                                                             moxi=True,
                                                                                             number_of_threads=2)

        self.log.info("Sleep after data load")
        ready = RebalanceHelper.wait_for_persistence(self.master, bucket, bucket_type=self.bucket_type)
        self.assertTrue(ready, "not all items persisted. see logs")

        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.execute_command(self.perm_command)
            shell.log_command_output(output, error)
            node = RestConnection(server).get_nodes_self()
            BackupHelper(server, self).backup(bucket_before_backup, node, self.remote_tmp_folder)
            shell.disconnect()

        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket_before_backup, self)
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket_after_backup, port=11212,
                                            test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket_after_backup)
        self.assertTrue(ready, "wait_for_memcached failed")

        for server in self.servers:
            BackupHelper(server, self).restore(self.remote_tmp_folder, moxi_port=11212)
            time.sleep(10)

        ready = RebalanceHelper.wait_for_stats_on_all(self.master, bucket_after_backup, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats_on_all(self.master, bucket_after_backup, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        self.assertTrue(BucketOperationHelper.verify_data(self.master, inserted_keys, False, False, 11212, debug=False,
                                                          bucket=bucket_after_backup), "Missing keys")


    def test_backup_upgrade_restore_default(self):
        if len(self.servers) < 2:
            self.log.error("At least 2 servers required for this test ..")
            return
        original_set = copy.copy(self.servers)
        worker = self.servers[len(self.servers) - 1]
        self.servers = self.servers[:len(self.servers) - 1]
        shell = RemoteMachineShellConnection(self.master)
        o, r = shell.execute_command("cat /opt/couchbase/VERSION.txt")
        fin = o[0]
        shell.disconnect()
        initial_version = self.input.param("initial_version", fin)
        final_version = self.input.param("final_version", fin)
        if initial_version == final_version:
            self.log.error("Same initial and final versions ..")
            return
        if not final_version.startswith('2.0'):
            self.log.error("Upgrade test not set to run from 1.8.1 -> 2.0 ..")
            return
        builds, changes = BuildQuery().get_all_builds(version=final_version)
        product = 'couchbase-server-enterprise'
        #CASE where the worker isn't a 2.0+
        worker_flag = 0
        shell = RemoteMachineShellConnection(worker)
        o, r = shell.execute_command("cat /opt/couchbase/VERSION.txt")
        temp = o[0]
        if not temp.startswith('2.0'):
            worker_flag = 1
        if worker_flag == 1:
            self.log.info("Loading version {0} on worker.. ".format(final_version))
            remote = RemoteMachineShellConnection(worker)
            info = remote.extract_remote_info()
            older_build = BuildQuery().find_build(builds, product, info.deliverable_type,
                                                  info.architecture_type, final_version)
            remote.stop_couchbase()
            remote.couchbase_uninstall()
            remote.download_build(older_build)
            remote.install_server(older_build)
            remote.disconnect()

        remote_tmp = "{1}/{0}".format("backup", "/root")
        perm_comm = "mkdir -p {0}".format(remote_tmp)
        if not initial_version == fin:
            for server in self.servers:
                remote = RemoteMachineShellConnection(server)
                info = remote.extract_remote_info()
                self.log.info("Loading version ..  {0}".format(initial_version))
                older_build = BuildQuery().find_build(builds, product, info.deliverable_type,
                                                      info.architecture_type, initial_version)
                remote.stop_couchbase()
                remote.couchbase_uninstall()
                remote.download_build(older_build)
                remote.install_server(older_build)
                rest = RestConnection(server)
                RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
                rest.init_cluster(server.rest_username, server.rest_password)
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                remote.disconnect()

        self.common_setUp()
        bucket = "default"
        if len(self.servers) > 1:
            self.add_nodes_and_rebalance()
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        size = int(info.memoryQuota * 2.0 / 3.0)
        rest.create_bucket(bucket, ramQuotaMB=size)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.assertTrue(ready, "wait_for_memcached_failed")
        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}
        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             name=bucket,
                                                                                             ram_load_ratio=0.5,
                                                                                             value_size_distribution=distribution,
                                                                                             moxi=True,
                                                                                             write_only=True,
                                                                                             delete_ratio=0.1,
                                                                                             number_of_threads=2)
        if len(self.servers) > 1:
            rest = RestConnection(self.master)
            self.assertTrue(RebalanceHelper.wait_for_replication(rest.get_nodes(), timeout=180),
                            msg="replication did not complete")

        ready = RebalanceHelper.wait_for_stats_on_all(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats_on_all(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        node = RestConnection(self.master).get_nodes_self()
        shell = RemoteMachineShellConnection(worker)
        o, r = shell.execute_command(perm_comm)
        shell.log_command_output(o, r)
        shell.disconnect()

        #Backup
        #BackupHelper(self.master, self).backup(bucket, node, remote_tmp)
        shell = RemoteMachineShellConnection(worker)
        shell.execute_command("/opt/couchbase/bin/cbbackup http://{0}:{1} {2}".format(
                                                            self.master.ip, self.master.port, remote_tmp))
        shell.disconnect()
        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)
        time.sleep(30)

        #Upgrade
        for server in self.servers:
            self.log.info("Upgrading to current version {0}".format(final_version))
            remote = RemoteMachineShellConnection(server)
            info = remote.extract_remote_info()
            new_build = BuildQuery().find_build(builds, product, info.deliverable_type,
                                                info.architecture_type, final_version)
            remote.stop_couchbase()
            remote.couchbase_uninstall()
            remote.download_build(new_build)
            remote.install_server(new_build)
            rest = RestConnection(server)
            RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
            rest.init_cluster(server.rest_username, server.rest_password)
            rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
            remote.disconnect()
        time.sleep(30)

        #Restore
        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        size = int(info.memoryQuota * 2.0 / 3.0)
        rest.create_bucket(bucket, ramQuotaMB=size)
        ready = BucketOperationHelper.wait_for_memcached(server, bucket)
        self.assertTrue(ready, "wait_for_memcached_failed")
        #BackupHelper(self.master, self).restore(backup_location=remote_tmp, moxi_port=info.moxi)
        shell = RemoteMachineShellConnection(worker)
        shell.execute_command("/opt/couchbase/bin/cbrestore {2} http://{0}:{1} -b {3}".format(
                                                            self.master.ip, self.master.port, remote_tmp, bucket))
        shell.disconnect()
        time.sleep(60)
        keys_exist = BucketOperationHelper.keys_exist_or_assert_in_parallel(inserted_keys, self.master, bucket, self, concurrency=4)
        self.assertTrue(keys_exist, msg="unable to verify keys after restore")
        time.sleep(30)
        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)
        rest = RestConnection(self.master)
        helper = RestHelper(rest)
        nodes = rest.node_statuses()
        master_id = rest.get_nodes_self().id
        if len(self.servers) > 1:
                removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                          ejectedNodes=[node.id for node in nodes if node.id != master_id],
                                          wait_for_rebalance=True)

        shell = RemoteMachineShellConnection(worker)
        shell.remove_directory(remote_tmp)
        shell.disconnect()

        self.servers = copy.copy(original_set)
        if initial_version == fin:
            builds, changes = BuildQuery().get_all_builds(version=initial_version)
            for server in self.servers:
                remote = RemoteMachineShellConnection(server)
                info = remote.extract_remote_info()
                self.log.info("Loading version ..  {0}".format(initial_version))
                older_build = BuildQuery().find_build(builds, product, info.deliverable_type,
                                                      info.architecture_type, initial_version)
                remote.stop_couchbase()
                remote.couchbase_uninstall()
                remote.download_build(older_build)
                remote.install_server(older_build)
                rest = RestConnection(server)
                RestHelper(rest).is_ns_server_running(testconstants.NS_SERVER_TIMEOUT)
                rest.init_cluster(server.rest_username, server.rest_password)
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                remote.disconnect()

    def test_backup_add_restore_default_bucket_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("default", 180, True, True)

    def test_non_default_bucket(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body(str(uuid.uuid4()), 60, True, True)

    def test_default_bucket(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("default", 60, True, True)

    def test_backup_add_restore_non_default_bucket_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("testb", 180, True, True)
        #self._test_backup_add_restore_bucket_body(bucket="test_bucket")

    def test_backup_add_restore_default_bucket_non_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("default", 180, False, True)

    def test_backup_add_restore_non_default_bucket_non_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("testb", 180, False, True)
        #self._test_backup_add_restore_bucket_body(bucket="test_bucket", startup_flag = False)

    def test_backup_add_restore_when_ide(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("default", 120, True, True)

    def test_expired_keys_1_replica(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_with_expiration_key(1)

    def test_expired_keys_2_replica(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_with_expiration_key(2)

    def test_expired_keys_3_replica(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_with_expiration_key(3)

    def test_backup_and_restore_bucket_without_overwrite(self):
        self.common_setUp()
        self._test_backup_and_restore_bucket_overwriting_body(False)

    def test_backup_and_restore_bucket_with_overwrite(self):
        self.common_setUp()
        self._test_backup_and_restore_bucket_overwriting_body()

    def test_cluster_topology_change(self):
        self.common_setUp()
        self._test_cluster_topology_change_body()

    def test_delete_key_and_backup_and_restore(self):
        self.common_setUp()
        self._test_delete_key_and_backup_and_restore_body()

    def test_backup_and_restore_on_different_port(self):
        self.common_setUp()
        self._test_backup_and_restore_on_different_port_body()

    def test_backup_and_restore_from_to_different_buckets(self):
        self.common_setUp()
        self._test_backup_and_restore_from_to_different_buckets()


class BackupHelper(object):
    def __init__(self, serverInfo, test):
        self.server = serverInfo
        self.log = logger.Logger.get_logger()
        self.test = test

    #data_file = default-data/default
    def backup(self, bucket, node, backup_location):
        mbbackup_path = "{0}/{1}".format("/opt/couchbase/bin", "cbbackup")
        if self.test.is_membase:
            mbbackup_path = "{0}/{1}".format("/opt/membase/bin", "mbbackup")
        data_directory = "{0}/{1}-{2}/{3}".format(node.storage[0].path, bucket, "data", bucket)
        command = "{0} {1} {2}".format(mbbackup_path,
                                       data_directory,
                                       backup_location)
        output, error = self.test.shell.execute_command(command)
        self.test.shell.log_command_output(output, error)

    def restore(self, backup_location, moxi_port=None, overwrite_flag=False, username=None, password=None):
        command = "{0}/{1}".format("/opt/couchbase/bin", "cbrestore")
        if self.test.is_membase:
            command = "{0}/{1}".format("/opt/membase/bin", "mbrestore")

        if not overwrite_flag:
            command += " -a"

        if not moxi_port is None:
            command += " -p {0}".format(moxi_port)
        if username is not None:
            command += " -u {0}".format(username)
        if password is not None:
            command += " -P {0}".format(password)

        files = self.test.shell.list_files(backup_location)
        for file in files:
            command += " " + file['path'] + "/" + file['file']
        command = "{0}".format(command)
        self.log.info(command)

        output, error = self.test.shell.execute_command(command)
        self.test.shell.log_command_output(output, error)
