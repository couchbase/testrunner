import unittest
from TestInput import TestInputSingleton
import mc_bin_client
import uuid
import logger
import datetime
import time
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from security.rbac_base import RbacBase

class SimpleSetGetTestBase(object):
    log = None
    keys = None
    servers = None
    input = None
    test = None

    def setUp_bucket(self, unittest):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        unittest.assertTrue(self.input, msg="input parameters missing...")
        self.test = unittest
        self.master = self.input.servers[0]
        rest = RestConnection(self.master)
        rest.init_cluster(username=self.master.rest_username, password=self.master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
        ClusterOperationHelper.cleanup_cluster([self.master])
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self.test)

        serverInfo = self.master
        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.memoryQuota)

        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)

        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.master), 'builtin')
        
    def set_get_test(self, value_size, number_of_items):
        fixed_value = MemcachedClientHelper.create_value("S", value_size)
        specs = [("default", 0),
                ("set-get-bucket-replica-1", 1),
                ("set-get-bucket-replica-2", 2),
                ("set-get-bucket-replica-3", 3)]
        serverInfo = self.master
        rest = RestConnection(serverInfo)
        bucket_ram = int(rest.get_nodes_self().memoryQuota // 4)

        mcport = rest.get_nodes_self().memcached
        for name, replica in specs:
            rest.create_bucket(name, bucket_ram, "sasl", "password", replica, mcport)

        bucket_data = {}
        buckets = RestConnection(serverInfo).get_buckets()
        for bucket in buckets:
            bucket_data[bucket.name] = {}
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, bucket.name)
            self.test.assertTrue(ready, "wait_for_memcached failed")

            client = MemcachedClientHelper.direct_client(serverInfo, bucket.name)
            inserted = []
            rejected = []
            while len(inserted) <= number_of_items and len(rejected) <= number_of_items:
                try:
                    key = str(uuid.uuid4())
                    client.set(key, 0, 0, fixed_value)
                    inserted.append(key)
                except mc_bin_client.MemcachedError:
                    pass

            retry = 0
            remaining_items = []
            remaining_items.extend(inserted)
            msg = "memcachedError : {0} - unable to get a pre-inserted key : {1}"
            while retry < 10 and len(remaining_items) > 0:
                verified_keys = []
                for key in remaining_items:
                    try:
                        flag, keyx, value = client.get(key=key)
                        try:
                          value = value.decode()
                        except AttributeError:
                          pass
                        if not value == fixed_value:
                            self.test.fail("value {0},{1} mismatch for key {2}".format(value,fixed_value,key))
                        verified_keys.append(key)
                    except mc_bin_client.MemcachedError as error:
                        self.log.error(msg.format(error.status, key))
                    retry += 1
                [remaining_items.remove(x) for x in verified_keys]

            print_count = 0
            for key in remaining_items:
                if print_count > 100:
                    break
                print_count += 1
                self.log.error("unable to verify key : {0}".format(key))
            if remaining_items:
                self.test.fail("unable to verify {0} keys".format(len(remaining_items)))


    def tearDown_bucket(self):
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self.test)

        # Remove rbac user in teardown
        role_del = ['cbadminbucket']
        temp = RbacBase().remove_user_role(role_del, RestConnection(self.master))



class MembaseBucket(unittest.TestCase):

    simpleSetGetTestBase = None
    def suite_setUp(self):
        print("suite_setUp...")
     
    def suite_tearDown(self):
        print("suite_tearDown...")

    def setUp(self):
        self.simpleSetGetTestBase = SimpleSetGetTestBase()
        self.simpleSetGetTestBase.setUp_bucket(self)

        self._log_start()

    def test_value_100b(self):
        self.simpleSetGetTestBase.set_get_test(128, 4000)

    def test_value_500kb(self):
        self.simpleSetGetTestBase.set_get_test(512 * 1024, 400)

    def test_value_10mb(self):
        self.simpleSetGetTestBase.set_get_test(10 * 1024 * 1024, 10)

    def test_value_1mb(self):
        self.simpleSetGetTestBase.set_get_test(1 * 1024 * 1024, 40)

    def tearDown(self):
        if self.simpleSetGetTestBase:
            self.simpleSetGetTestBase.tearDown_bucket()


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
