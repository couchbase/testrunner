import unittest
import uuid
from TestInput import TestInputSingleton
import logger
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

class BackupAndRestoreTests(unittest.TestCase):

    input = None
    servers = None
    log = None
    membase = None
    shell = None

    # simple addnode tests without rebalancing
    # add node to itself
    # add an already added node
    # add node and remove them 10 times serially
    # add node and remove the node in parallel threads later...

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.shell = RemoteMachineShellConnection(self.servers[0])
        self.remote_tmp_folder = None

    #we dont necessarily care about the test case
    def common_setUp(self):
        self.shell.start_membase()
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers,self)

    def tearDown(self):
        self.log.info("delete remote folder @ {0}".format(self.remote_tmp_folder))
        self.shell.remove_directory(self.remote_tmp_folder)
        self.shell.stop_membase()

    #add nodes one by one
    def _test_backup_add_restore_bucket_body(self, bucket="default", port = 11211, startup_flag = True):

        self.remote_tmp_folder = "/tmp/{0}-{1}".format("mbbackuptestdefaultbucket", uuid.uuid4())
        master = self.servers[0]

        node = RestConnection(master).get_nodes_self()
        BucketOperationHelper.delete_bucket_or_assert(master, bucket, self)
        BucketOperationHelper.create_bucket(master, bucket, port, test_case=self)
        keys = BucketOperationHelper.load_data_or_assert(master, bucket_name=bucket, test = self)

        if not startup_flag:
            self.shell.stop_membase()

        #let's create a unique folder in the remote location
        output, error = self.shell.execute_command("mkdir -p {0}".format(self.remote_tmp_folder))
        self.shell.log_command_output(output,error)

        #now let's back up
        BackupHelper(master, self).backup(bucket, node, self.remote_tmp_folder)

        if not startup_flag:
            self.shell.start_membase()

        BucketOperationHelper.delete_bucket_or_assert(master, bucket, self)
        BucketOperationHelper.create_bucket(master, bucket, port, test_case = self)

        if not startup_flag:
            self.shell.stop_membase()

        BackupHelper(master, self).restore(self.remote_tmp_folder)

        if not startup_flag:
            self.shell.start_membase()

        BucketOperationHelper.verify_data(master.ip, keys, False, False, port, self)

    def test_backup_add_restore_default_bucket_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body()

    def test_backup_add_restore_non_default_bucket_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("test_bucket")

    def test_backup_add_restore_default_bucket_non_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body(startup_flag = False)

    def test_backup_add_restore_non_default_bucket_non_started_server(self):
        self.common_setUp()
        self._test_backup_add_restore_bucket_body("test_bucket", startup_flag = False)

class BackupHelper(object):

    def __init__(self,serverInfo, test):

        self.server = serverInfo
        self.log = logger.Logger.get_logger()
        self.test = test

    #data_file = default-data/default
    def backup(self, bucket, node, backup_location):
        mbbackup_path = "{0}/{1}".format(self.server.cli_path, "mbbackup")
        data_directory = "{0}/{1}-{2}/{3}".format(node.storage[0].path,bucket,"data",bucket)
        command = "{0} {1} {2}".format(mbbackup_path,
                                       data_directory,
                                       backup_location)
        output, error = self.test.shell.execute_command(command.format(command))
        if output:
            self.log.info(output)
        if error:
            self.log.info(error)

    def restore(self, backup_location):
        command = "{0}/mbrestore -a".format(self.server.cli_path)

        files = self.test.shell.list_files(backup_location)
        for file in files:
            command += " "
            command += file['path'] + "/" + file['file']

        self.log.info(command)

        #node = RestConnection(self.server).get_nodes_self()
        #data_directory = "{0}/{1}-{2}/{3}".format(node.storage[0].path,bucket,"data",bucket)
        output, error = self.test.shell.execute_command(command.format(command))
        if output:
            self.log.info(output)
        if error:
            self.log.info(error)

    def load_sqlite(self,files):
        #for each file , load the sqllite file
        #for each kv_x get the count and add up the numbers
        total_count = 0
        for file in data_files:
            import sqlite3
            connect = sqlite3.connect("{0}/{1}".format(file['path'],file['file']))
            connect.execute("select name from sqlite_master where name like 'kv%'")
        pass