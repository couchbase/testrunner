import logger
import unittest

from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.cluster import Cluster
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection

from membase.helper.subdoc_helper import SubdocHelper
from subdoc_sanity import SubdocSanityTests, SimpleDataSet, DeeplyNestedDataSet
from random import randint

class SubdocErrorTests(SubdocSanityTests):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SubdocHelper(self, "default")
        self.helper.setup_cluster()
        self.cluster = Cluster()
        self.servers = self.helper.servers

    def tearDown(self):
        self.helper.cleanup_cluster()

    def error_test_simple_dataset_get(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Error testing path error for CMD_GET on simple_dataset"
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        '''invalid path'''
        self.log.info('Testing invalid path ')
        self.error_gets(inserted_keys, path='array[-5]', error="Memcached error #194 'Invalid path'")
        '''path does not exist'''
        self.log.info('Testing path does not exist')
        self.error_gets(inserted_keys, path='  ', error="Memcached error #192 'Path not exists'")

    def error_test_deep_nested_dataset_get(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Error testing path error for CMD_GET for deep nested dataset "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys, levels = data_set.load()

        '''path does not exist'''
        self.log.info('Testing last+1 element dictionary')
        self.error_gets(inserted_keys, path = self._get_path('child', levels+1), error="Memcached error #192 'Path not exists'")

        '''Invalid path'''
        self.log.info('Testing Dict.Array')
        self.error_gets(inserted_keys, path = self._get_path('child', levels-2)+'.array[-5]', error="Memcached error #194 'Invalid path'")

        '''path too big'''
        self.log.info('Testing Intermediate element Dict. Array')
        self.error_gets(inserted_keys, path = self._get_path('child', 40), error="Memcached error #195 'Path too big'")

        '''Malformed path'''
        self.log.info('Testing Malformed path Dict. Array')
        self.error_gets(inserted_keys, path = self._get_path('child', levels-2)+'.`array[0]`', error="Memcached error #192 'Path not exists'")

        '''Invalid Path'''
        self.log.info('Testing ENOENT')
        self.error_gets(inserted_keys, path = self._get_path('child', levels-2)+'.array[100]', error="Memcached error #192 'Path not exists'")

        '''Path too long'''
        data_set_long = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys_long, levels_long = data_set_long.load(long_path=True)
        self.log.info('Testing long path ')
        self.error_gets(inserted_keys_long, path = self._get_path('child12345678901234567890123456789', levels_long), error="Memcached error #192 'Path too long'")

    def error_gets(self, inserted_keys, path, error):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.get_sd(in_key, path)
                print data
            except Exception as ex:
                self.assertTrue(str(ex).find(error) != -1,
                    "Error is incorrect.Actual %s.Expected: %s." %(
                        str(ex), error))
            else:
                self.fail("There were no errors. Error expected: %s" % error)


