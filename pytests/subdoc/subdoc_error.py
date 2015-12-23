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

    def error_test_deep_nested_dataset_exists(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Error testing path error for CMD_EXISTS for deep nested dataset "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys, levels = data_set.load()

        '''path does not exist'''
        self.log.info('Testing last+1 element dictionary')
        self.error_exists(inserted_keys, path = self._get_path('child', levels+1), error="Memcached error #192 'Path not exists'")

        '''Invalid path'''
        self.log.info('Testing Dict.Array')
        self.error_exists(inserted_keys, path = self._get_path('child', levels-2)+'.array[-5]', error="Memcached error #194 'Invalid path'")

        '''path too big'''
        self.log.info('Testing Intermediate element Dict. Array')
        self.error_exists(inserted_keys, path = self._get_path('child', 40), error="Memcached error #195 'Path too big'")

        '''Malformed path'''
        self.log.info('Testing Malformed path Dict. Array')
        self.error_exists(inserted_keys, path = self._get_path('child', levels-2)+'.`array[0]`', error="Memcached error #192 'Path not exists'")

        '''Invalid Path'''
        self.log.info('Testing ENOENT')
        self.error_exists(inserted_keys, path = self._get_path('child', levels-2)+'.array[100]', error="Memcached error #192 'Path not exists'")

        '''Path too long'''
        data_set_long = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys_long, levels_long = data_set_long.load(long_path=True)
        self.log.info('Testing long path ')
        self.error_exists(inserted_keys_long, path = self._get_path('child12345678901234567890123456789', levels_long), error="Memcached error #192 'Path too long'")

    ''' Change error behaviour , there is something wrong on the call '''
    def error_test_deep_nested_dataset_dict_add(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Error testing path error for CMD_DICT_ADD for deep nested dataset "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys,levels = data_set.load()

        '''path does not exist'''
        self.log.info('Testing empty path for dictionary')
        self.error_add_dict(inserted_keys, add_str = 'child', path = self._get_path('child', levels-2), error="Memcached error #197 'Cant insert'")

        '''path does not exist'''
        self.log.info('Testing empty path for dictionary')
        self.error_add_dict(inserted_keys, add_str="new_value", path = self._get_path('child', levels-2), error="Memcached error #197 'Cant insert'")

        ''' Change error behaviour , there is something wrong on the call '''
    def error_test_deep_nested_dataset_dict_upsert(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Error testing path error for CMD_DICT_UPSERT for deep nested dataset "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys,levels = data_set.load()

        '''path does not exist'''
        self.log.info('Testing empty path for dictionary')
        self.error_upsert_dict(inserted_keys, add_str = 'child', path = self._get_path('child', levels-2), error="Memcached error #197 'Cant insert'")

        '''path does not exist'''
        self.log.info('Testing empty path for dictionary')
        self.error_upsert_dict(inserted_keys, add_str="new_value", path = self._get_path('child', levels-2), error="Memcached error #197 'Cant insert'")

    def error_test_deep_nested_dataset_delete(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Error testing path error for CMD_delete for deep nested dataset "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys,levels = data_set.load()

        '''path does not exist'''
        self.log.info('Testing path not exists')
        self.error_delete(inserted_keys, path = self._get_path('child', levels)+'.child', error="Memcached error #192 'Path not exists'")

        '''Last element Array of Array'''
        last_path ='child'
        for i in xrange(levels-3):
            last_path +='.child'
        last_path +='.array[-1]'
        #data_set.get_all_docs(inserted_keys, path = last_path)

        '''path does not exist on array'''
        self.log.info('Testing path not exists on dict.array with array [-5]')
        self.error_delete(inserted_keys, path = last_path+'[-1]', error="Memcached error #193 'Path mismatch'")
        self.error_delete(inserted_keys, path = last_path+'[-5]', error="Memcached error #194 'Invalid path'")

        '''path missing CHECK if this error is expected.'''
        self.log.info('Testing path missing delete')
        self.error_delete(inserted_keys, path = '', error="Memcached error #4 'Invalid'")

    def error_test_deep_nested_dataset_replace(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Error testing path error for CMD_REPLACE for deep nested dataset "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys,levels = data_set.load()

        '''path does not exist'''
        self.log.info('Testing path not exists')
        self.error_delete(inserted_keys, path = self._get_path('child', levels)+'.child', error="Memcached error #192 'Path not exists'")

        '''Last element Array of Array'''
        last_path ='child'
        for i in xrange(levels-3):
            last_path +='.child'
        last_path +='.array[-1]'
        #data_set.get_all_docs(inserted_keys, path = last_path)

        '''path does not exist on array'''
        self.log.info('Testing path not exists on dict.array with array [--1]')
        self.error_replace(inserted_keys, path = last_path+'[-1]', error="Memcached error #193 'Path mismatch'", replace_str=1000000)

        '''path missing replace string array Memcached error #4 'Invalid and Cant Insert'''
        self.log.info('Testing path not exists on dict.array with array [--1]')
        self.error_replace(inserted_keys, path = last_path+'[-1]', error="Memcached error #193 'Path mismatch'", replace_str='abc')

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


    def error_exists(self, inserted_keys, path, error):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.exists_sd(in_key, path)
                print data
            except Exception as ex:
                self.assertTrue(str(ex).find(error) != -1,
                    "Error is incorrect.Actual %s.Expected: %s." %(
                        str(ex), error))
            else:
                self.fail("There were no errors. Error expected: %s" % error)
                print ex.message

    def error_add_dict(self, inserted_keys, add_str, path, error):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.dict_add_sd(in_key, path, add_str)
                print data
            except Exception as ex:
                self.assertTrue(str(ex).find(error) != -1,
                    "Error is incorrect.Actual %s.Expected: %s." %(
                        str(ex), error))
            else:
                self.fail("There were no errors. Error expected: %s" % error)

    def error_upsert_dict(self, inserted_keys, add_str, path, error):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.dict_upsert_sd(in_key, path, add_str)
                print data
            except Exception as ex:
                self.assertTrue(str(ex).find(error) != -1,
                    "Error is incorrect.Actual %s.Expected: %s." %(
                        str(ex), error))
            else:
                self.fail("There were no errors. Error expected: %s" % error)

    def error_delete(self, inserted_keys, path, error):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.delete_sd(in_key, path)
                print data
            except Exception as ex:
                self.assertTrue(str(ex).find(error) != -1,
                    "Error is incorrect.Actual %s.Expected: %s." %(
                        str(ex), error))
            else:
                self.fail("There were no errors. Error expected: %s" % error)

    def error_replace(self, inserted_keys, path, error, replace_str):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.replace_sd(in_key, path, replace_str)
                print data
            except Exception as ex:
                self.assertTrue(str(ex).find(error) != -1,
                    "Error is incorrect.Actual %s.Expected: %s." %(
                        str(ex), error))
            else:
                self.fail("There were no errors. Error expected: %s" % error)