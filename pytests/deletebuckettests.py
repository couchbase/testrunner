import unittest
import uuid
from TestInput import TestInputSingleton
import logger
import time
import datetime
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection

class DeleteMembaseBuckets(unittest.TestCase):

    servers = None
    input = None
    log = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        self._log_start()

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        self._log_finish()

    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass


    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    #TODO: move these methods to a helper class
    def wait_for_data_files_deletion(self,
                                     bucket,
                                     remote_connection,
                                     rest,
                                     timeout_in_seconds=120):
        self.log.info('waiting for bucket data files deletion from the disk ....')
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if self.verify_data_files_deletion(bucket, remote_connection, rest):
                return True
            else:
                data_file = '{0}-data'.format(bucket)
                self.log.info("still waiting for deletion of {0} ...".format(data_file))
                time.sleep(2)
        return False


    def verify_data_files_deletion(self,
                                   bucket,
                                   remote_connection,
                                   rest):
        node = rest.get_nodes_self()
        for item in node.storage:
            #get the path
            data_file = '{0}-data'.format(bucket)
            if remote_connection.file_exists(item.path, data_file):
                return False
        return True
