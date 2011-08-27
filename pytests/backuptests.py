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

class BackupRestoreTests(unittest.TestCase):
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
        self.shell = RemoteMachineShellConnection(self.servers[0])
        self.remote_tmp_folder = None
        self.remote_tmp_folder = "/tmp/{0}-{1}".format("mbbackuptestdefaultbucket", uuid.uuid4())
        self.master = self.servers[0]


    def common_setUp(self):
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.stop_membase()
            shell.start_membase()
            RestHelper(RestConnection(server)).is_ns_server_running(timeout_in_seconds=120)
            shell.disconnect()


    def tearDown(self):
        self.log.info("delete remote folder @ {0}".format(self.remote_tmp_folder))
        self.shell.remove_directory(self.remote_tmp_folder)

        rest = RestConnection(self.master)
        helper = RestHelper(rest)

        if not helper.is_ns_server_running(2):
            self.shell.start_membase()
            helper.is_ns_server_running(60)

        self.shell.disconnect()


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
                                                                                             ram_load_ratio=30,
                                                                                             value_size_distribution=distribution,
                                                                                             moxi=True,
                                                                                             write_only=True,
                                                                                             number_of_threads=2)

        if not single_node:
            rest = RestConnection(self.master)
            self.assertTrue(RestHelper(rest).wait_for_replication(180), msg="replication did not complete")

        self.log.info("Sleep {0} seconds after data load".format(delay_after_data_load))
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        node = RestConnection(self.master).get_nodes_self()
        if not startup_flag:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.stop_membase()
                shell.disconnect()

        output, error = self.shell.execute_command("mkdir -p {0}".format(self.remote_tmp_folder))
        self.shell.log_command_output(output, error)

        #now let's back up
        BackupHelper(self.master, self).backup(bucket, node, self.remote_tmp_folder)
        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)

        if not startup_flag:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.start_membase()
                RestHelper(RestConnection(server)).is_ns_server_running()
                shell.disconnect()

        if bucket == "default":
            rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=info.moxi)
        else:
            proxyPort = info.moxi + 500
            rest.create_bucket(bucket, ramQuotaMB=size, proxyPort=proxyPort,
                               authType="sasl", saslPassword="password")
        BucketOperationHelper.wait_for_memcached(server, bucket)
        BackupHelper(self.master, self).restore(backup_location=self.remote_tmp_folder, moxi_port=info.moxi)
        keys_exist = BucketOperationHelper.keys_exist_or_assert(inserted_keys, rest, bucket, self)
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
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        node = RestConnection(self.master).get_nodes_self()
        output, error = self.shell.execute_command("mkdir -p {0}".format(self.remote_tmp_folder))
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
                self.assertEquals(error.status, 1,
                                  msg="expected error code {0} but saw error code {1}".format(1, error.status))
        client.close()
        self.log.info("verified that those keys inserted with expiry set to {0} have expired".format(expiry))


    def _test_backup_and_restore_bucket_overwriting_body(self, overwrite_flag=True):
        bucket = "default"
        BucketOperationHelper.create_bucket(serverInfo=self.master, test_case=self)
        BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.add_nodes_and_rebalance()

        client = MemcachedClientHelper.direct_client(self.master, "default")
        vbucket_count = RestConnection(self.master, "default")
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

        node = RestConnection(self.master).get_nodes_self()
        backupHelper = BackupHelper(self.master, self)
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")

        backupHelper.backup(bucket, node, self.remote_tmp_folder)

        for key in keys:
            try:
                client.replace(key, expiry, 0, "2")
            except mc_bin_client.MemcachedError as error:
                msg = "unable to replace key : {0} in bucket : {1} error : {2}"
                self.log.error(msg.format(key, client.vbucketId, error.status))
                self.fail(msg.format(key, client.vbucketId, error.status))
        self.log.info("replaced {0} keys with expiry set to {1}".format(len(keys), expiry))

        backupHelper.restore(self.remote_tmp_folder, overwrite_flag)

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

        rest = RestConnection(self.master)

        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}

        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             ram_load_ratio=1,
                                                                                             value_size_distribution=distribution,
                                                                                             moxi=True,
                                                                                             write_only=True,
                                                                                             number_of_threads=2)

        self.assertTrue(RestHelper(rest).wait_for_replication(180), msg="replication did not complete")

        self.log.info("Sleep after data load")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")


        #let's create a unique folder in the remote location
        output, error = self.shell.execute_command("mkdir -p {0}".format(self.remote_tmp_folder))
        self.shell.log_command_output(output, error)

        #now let's back up
        bucket = "default"
        node = RestConnection(self.master).get_nodes_self()
        BackupHelper(self.master, self).backup(bucket, node, self.remote_tmp_folder)

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

        BackupHelper(self.master, self).restore(self.remote_tmp_folder)
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

        node = RestConnection(self.master).get_nodes_self()
        backupHelper = BackupHelper(self.master, self)
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")

        backupHelper.backup(bucket, node, self.remote_tmp_folder)

        backupHelper.restore(self.remote_tmp_folder, overwrite_flag=True)

        self.log.info('verifying that all those keys...')
        missing_keys = []
        verify_keys = []
        for key in keys:
            vBucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
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
        bucket = "testb"
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket, port=11212, test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.assertTrue(ready, "wait_for_memcached failed")

        self.add_nodes_and_rebalance()

        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}

        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             ram_load_ratio=1,
                                                                                             value_size_distribution=distribution,
                                                                                             write_only=True,
                                                                                             moxi=True,
                                                                                             number_of_threads=2)

        rest = RestConnection(self.master)
        self.assertTrue(RestHelper(rest).wait_for_replication(180), msg="replication did not complete")

        self.log.info("Sleep after data load")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")

        node = RestConnection(self.master).get_nodes_self()
        BackupHelper(self.master, self).backup(bucket, node, self.remote_tmp_folder)

        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket, port=11213, test_case=self)

        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.assertTrue(ready, "wait_for_memcached failed")

        BackupHelper(self.master, self).restore(self.remote_tmp_folder, moxi_port=11213)
        self.assertTrue(BucketOperationHelper.verify_data(self.master, inserted_keys, False, False, 11213, self),
                        "Missing keys")


    def _test_backup_and_restore_from_to_different_buckets(self):
        bucket = "testb"
        BucketOperationHelper.create_bucket(serverInfo=self.master, name=bucket, port=11212, test_case=self)
        ready = BucketOperationHelper.wait_for_memcached(self.master, bucket)
        self.assertTrue(ready, "wait_for_memcached failed")

        self.add_nodes_and_rebalance()

        distribution = {10: 0.2, 20: 0.5, 30: 0.25, 40: 0.05}

        inserted_keys, rejected_keys = MemcachedClientHelper.load_bucket_and_return_the_keys(servers=[self.master],
                                                                                             ram_load_ratio=1,
                                                                                             value_size_distribution=distribution,
                                                                                             write_only=True,
                                                                                             moxi=True,
                                                                                             number_of_threads=2)

        rest = RestConnection(self.master)
        self.assertTrue(RestHelper(rest).wait_for_replication(180), msg="replication did not complete")

        self.log.info("Sleep after data load")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_queue_size', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")
        ready = RebalanceHelper.wait_for_stats(self.master, bucket, 'ep_flusher_todo', 0)
        self.assertTrue(ready, "wait_for ep_queue_size == 0 failed")

        node = RestConnection(self.master).get_nodes_self()
        BackupHelper(self.master, self).backup(bucket, node, self.remote_tmp_folder)

        BucketOperationHelper.delete_bucket_or_assert(self.master, bucket, self)
        BucketOperationHelper.create_bucket(serverInfo=self.master, name="testb2", port=11212,
                                            test_case=self)

        BucketOperationHelper.wait_for_memcached(self.master, "testb2")
        BackupHelper(self.master, self).restore(self.remote_tmp_folder, moxi_port=11212)
        time.sleep(10)

        self.assertTrue(BucketOperationHelper.verify_data(self.master, inserted_keys, False, False, 11212, self),
                        "Missing keys")


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
        mbbackup_path = "{0}/{1}".format(self.server.cli_path, "mbbackup")
        data_directory = "{0}/{1}-{2}/{3}".format(node.storage[0].path, bucket, "data", bucket)
        command = "{0} {1} {2}".format(mbbackup_path,
                                       data_directory,
                                       backup_location)
        output, error = self.test.shell.execute_command(command.format(command))
        self.test.shell.log_command_output(output, error)


    def restore(self, backup_location, moxi_port=None, overwrite_flag=False):
        command = self.server.cli_path + "/mbrestore"
        if not overwrite_flag:
            command += " -a"

        if not moxi_port is None:
            command += " -p {0}".format(moxi_port)

        files = self.test.shell.list_files(backup_location)
        for file in files:
            command += " " + file['path'] + "/" + file['file']

        self.log.info(command)

        output, error = self.test.shell.execute_command(command)
        self.test.shell.log_command_output(output, error)
