import unittest
import uuid
import logger
import os

from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection

class BackupTests(unittest.TestCase):

    servers = None
    clients = None
    log = None
    input = None



    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers,self)
        self.remote_tmp_folder = None

    def tearDown(self):
        self.log.info("delete remote folder @ {0}".format(self.remote_tmp_folder))

        

    def test_default_bucket(self):
        master = self.servers[0]
        BucketOperationHelper.create_bucket(serverInfo=master, test_case=self)
        #let's create a unique folder in the remote location
        shell = RemoteMachineShellConnection(master)
        self.remote_tmp_folder = "/tmp/{0}-{1}".format("mbbackuptestdefaultbucket-", uuid.uuid4())
        output, error = shell.execute_command("mkdir -p {0}".format(self.remote_tmp_folder))
        shell.log_command_output(output,error)
        #now let's back up
        BackupHelper(master).backup('default',self.remote_tmp_folder)
        backup_files = BackupHelper(master).download_backups(self.remote_tmp_folder)
        print 'backup rertued'
        for backup_file in backup_files:
            self.log.info(backup_file)


    #test case to run mbbackup
    #check if those data files are

    # find the data files
    # login to the machine
    # run mbakcup
    # put it in the temp folder
    # copy it back to testrunner temp folder


class BackupHelper(object):

    def __init__(self,serverInfo):

        self.server = serverInfo
        self.log = logger.Logger.get_logger()

    #data_file = default-data/default
    def backup(self,bucket,backup_location):
        shell = RemoteMachineShellConnection(self.server)
        mbbackup_path = "{0}/{1}".format(self.server.cli_path, "mbbackup")
        node = RestConnection(self.server).get_nodes_self()
        data_directory = "{0}/{1}-{2}/{3}".format(node.storage[0].path,bucket,"data",bucket)
        command = "{0} {1} {2}".format(mbbackup_path,
                                       data_directory,
                                       backup_location)
        output, error = shell.execute_command(command.format(command))
        if output:
            self.log.info(output)
        if error:
            self.log.info(error)

    def download_backups(self,backup_location):
        #connect and list all the files under that
        #location and download those files
        #create random folder in local machine
        #create temp folder and then uuid under temp os.getcwd()
        local_files = []
        cwd = os.getcwd()
        local_dir = "{0}/out/tmp/{1}".format(cwd, uuid.uuid4())
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self.log.info("created {0} folder in the local machine...".format(local_dir))
        shell = RemoteMachineShellConnection(self.server)
        files = shell.list_files(backup_location)
        for file in files:
            self.log.info("downloading remote file {0}".format(file))
            shell.get_file(file['path'], file['file'], "{0}/{1}".format(local_dir,file['file']))
            local_files.append({'path': local_dir, 'file': file['file']})
        shell.remove_directory(backup_location)
        #now we can remove these files:

        return local_files

    def load_sqlite(self,files):
        #for each file , load the sqllite file
        #for each kv_x get the count and add up the numbers
        total_count = 0
        for file in data_files:
            import sqlite3
            connect = sqlite3.connect("{0}/{1}".format(file['path'],file['file']))
            connect.execute("select name from sqlite_master where name like 'kv%'")
        pass

