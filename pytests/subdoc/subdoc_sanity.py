import logger
import unittest

from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.cluster import Cluster
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection

from membase.helper.subdoc_helper import SubdocHelper
from random import randint

class SubdocSanityTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SubdocHelper(self, "default")
        self.helper.setup_cluster()
        self.cluster = Cluster()
        self.servers = self.helper.servers

    def tearDown(self):
        self.helper.cleanup_cluster()

    def test_simple_dataset_sanity(self):
        self.test_simple_dataset_get()
        self.test_deep_nested_dataset_get_dict()
        self.test_deep_nested_dataset_get_array()
        self.test_simple_dataset_dict_upsert()
        self.test_simple_dataset_dict_add()
        self.test_simple_dataset_remove()
        self.test_simple_dataset_exists()
        self.test_simple_dataset_replace()
        self.test_simple_dataset_array_push_last()
        self.test_simple_dataset_array_push_first()
        self.test_simple_dataset_counter()
        self.test_simple_dataset_array_add_unique()
        self.test_simple_dataset_counter()

    def test_simple_dataset_get(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple get sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        data_set.get_all_docs(inserted_keys, path = 'isDict')
        data_set.get_all_docs(inserted_keys, path='geometry.coordinates[0]')
        data_set.get_all_docs(inserted_keys, path='dict_value.name')
        data_set.get_all_docs(inserted_keys, path='array[0]')
        data_set.get_all_docs(inserted_keys, path='array[-1]')

        ''' This should go into ErrorTesting '''
        #self.assertFalse(data_set.get_all_docs(inserted_keys, path='array[-5]'))
        #self.assertFalse(data_set.get_all_docs(inserted_keys, path='  '))

    def test_deep_nested_dataset_get_dict(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue  get sub doc on deep nested single path on dictionaries "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys, levels = data_set.load()

        '''Top level element'''
        #data_set.get_all_docs(inserted_keys, path = 'number', check_data=levels)
        data_set.get_all_docs(inserted_keys, path = 'array')
        data_set.get_all_docs(inserted_keys, path = 'array[0]')

        '''Last element Dictionary'''
        self.log.info('Testing last element dictionary')
        data_set.get_all_docs(inserted_keys, path = self._get_path('child', levels-1))

        '''Last element Dict.Array'''
        self.log.info('Testing Dict.Array')
        data_set.get_all_docs(inserted_keys, path = self._get_path('child', levels-2)+'.array[0]')

        '''Intermediate element Dict.Array'''
        self.log.info('Testing Intermediate element Dict. Array')
        data_set.get_all_docs(inserted_keys, path = self._get_path('child', levels//2)+'.array[0]')

    def test_deep_nested_dataset_get_array(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue  get sub doc on deep nested single path on dictionaries "
                      "dataset with {0} docs".format(num_docs))

        data_set = DeeplyNestedDataSet(self.helper, num_docs)
        inserted_keys, levels = data_set.load()

        '''Top level element'''
        data_set.get_all_docs(inserted_keys, path = 'number')
        data_set.get_all_docs(inserted_keys, path = 'array')
        data_set.get_all_docs(inserted_keys, path = 'array[0]')

        '''Last element Array'''
        last_path ='child'
        for i in range(levels-1):
            last_path +='.child'
        data_set.get_all_docs(inserted_keys, path = last_path)

        '''Last element Array of Array'''
        last_path ='child'
        for i in range(levels-3):
            last_path +='.child'
        last_path +='.array[-1][-1][-1]'
        data_set.get_all_docs(inserted_keys, path = last_path)

        '''Intermediate element Array'''
        last_path ='child'
        for i in range(levels//2):
            last_path +='.child'
        last_path +='.array[0][-1]'
        data_set.get_all_docs(inserted_keys, path = last_path)

    def test_simple_dataset_dict_upsert(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple upsert dict sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(1000)

        data_set.upsert_all_docs(inserted_keys, replace_string, path='isDict')
        data_set.upsert_all_docs(inserted_keys, replace_string, path='geometry.coordinates[0]')
        data_set.upsert_all_docs(inserted_keys, replace_string, path='dict_value.name')
        data_set.upsert_all_docs(inserted_keys, "999", path='height')
        data_set.upsert_all_docs(inserted_keys, replace_string, path='array[-1]')

    def test_simple_dataset_dict_add(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple add dict sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(1000)

        #data_set.add_all_docs(inserted_keys, replace_string, path='isDict')
        #data_set.add_all_docs(inserted_keys, replace_string, path='geometry.coordinates[0]')
        data_set.add_all_docs(inserted_keys, replace_string, path='dict_value')
        #data_set.add_all_docs(inserted_keys, "999", path='height')
        #data_set.add_all_docs(inserted_keys, replace_string, path='array[-1]')

    def test_simple_dataset_remove(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple remove sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        data_set.remove_all_docs(inserted_keys, path='isDict')
        data_set.remove_all_docs(inserted_keys, path='geometry.coordinates[0]')
        data_set.remove_all_docs(inserted_keys, path='dict_value.name')
        data_set.remove_all_docs(inserted_keys, path='array[0]')
        data_set.remove_all_docs(inserted_keys, path='array[-1]')

    def test_simple_dataset_exists(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple exists sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' add test code to accept Bool values and not error out '''
        data_set.exists_all_docs(inserted_keys, path='isDict')
        data_set.exists_all_docs(inserted_keys, path='geometry.coordinates[0]')
        data_set.exists_all_docs(inserted_keys, path='dict_value.name')
        data_set.exists_all_docs(inserted_keys, path='array[0]')
        data_set.exists_all_docs(inserted_keys, path='array[-1]')

    def test_simple_dataset_replace(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple replace sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(10)

        data_set.replace_all_docs(inserted_keys, replace_string, path='isDict')
        data_set.replace_all_docs(inserted_keys, replace_string, path='geometry.coordinates[0]')
        data_set.replace_all_docs(inserted_keys, replace_string, path='dict_value.name')
        data_set.replace_all_docs(inserted_keys, "999", path='height')
        data_set.replace_all_docs(inserted_keys, replace_string, path='array[-1]')

    def test_simple_dataset_array_push_last(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple array_push_last sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(10)

        #Should be a negative testcase below.
        #data_set.array_push_last(inserted_keys, replace_string, path='isDict')
        data_set.array_push_last(inserted_keys, replace_string, path='geometry.coordinates')
        #data_set.array_push_last(inserted_keys, replace_string, path='dict_value.name')
        #data_set.array_push_last(inserted_keys, "999", path='height')
        data_set.array_push_last(inserted_keys, replace_string, path='array')

    def test_simple_dataset_array_push_first(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple array_push_first sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(10)

        #Should be a negative testcase below.
        #data_set.array_push_last(inserted_keys, replace_string, path='isDict')
        data_set.array_push_first(inserted_keys, replace_string, path='geometry.coordinates')
        #data_set.array_push_last(inserted_keys, replace_string, path='dict_value.name')
        #data_set.array_push_last(inserted_keys, "999", path='height')
        data_set.array_push_first(inserted_keys, replace_string, path='array')

    def test_simple_dataset_counter(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple counter sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(10)

        #Should be a negative testcase below.
        #data_set.array_push_last(inserted_keys, replace_string, path='isDict')
        data_set.counter_all_paths(inserted_keys, path='geometry.coordinates[0]')
        #data_set.array_push_last(inserted_keys, replace_string, path='dict_value.name')
        data_set.counter_all_paths(inserted_keys, path='height')
        #data_set.counter_all_paths(inserted_keys, path='array')


    def test_simple_dataset_array_add_unique(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple add array unique sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(10)

        #Should be a negative testcase below.
        #data_set.array_push_last(inserted_keys, replace_string, path='isDict')
        data_set.array_add_unique(inserted_keys, replace_string, path='geometry.coordinates')
        #data_set.array_push_last(inserted_keys, replace_string, path='dict_value.name')
        #data_set.counter_all_paths(inserted_keys, 1, path='height')
        #data_set.counter_all_paths(inserted_keys, replace_string, path='array')

    def test_simple_dataset_multi_lookup(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple multi lookup sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()

        ''' Randomly generate 1000 long string to replace existing path strings '''
        replace_string = self.generate_string(10)

        #Should be a negative testcase below.
        #data_set.array_push_last(inserted_keys, replace_string, path='isDict')
        data_set.multi_lookup_all_paths(inserted_keys, path='geometry.coordinates')
        #data_set.array_push_last(inserted_keys, replace_string, path='dict_value.name')
        #data_set.counter_all_paths(inserted_keys, 1, path='height')
        #data_set.counter_all_paths(inserted_keys, replace_string, path='array')


    def test_simple_dataset_multi_lookup2(self):
        pass

    def generate_string(self, range_val=100):
        long_string = ''.join(chr(97 + randint(0, 25)) for i in range(range_val))
        return '"' + long_string + '"'

    def _get_path(self, subdoc_elt=None, levels=None):
        subdoc_path = subdoc_elt
        for i in range(levels-1):
            subdoc_path +='.'+subdoc_elt
        return subdoc_path

class SimpleDataSet(SubdocSanityTests):
    def __init__(self, helper, num_docs):
        self.helper = helper
        self.num_docs = num_docs
        self.name = "simple_dataset"
        self.log = logger.Logger.get_logger()

    def load(self):
        inserted_keys = self.helper.insert_docs(self.num_docs, self.name)
        return inserted_keys

    def get_all_docs(self, inserted_keys, path):
            for in_key in inserted_keys:
                num_tries = 1
                try:
                    opaque, cas, data = self.helper.client.get_sd(in_key, path)
                except Exception as e:
                    self.helper.testcase.fail(
                        "Unable to get key {0} for path {1} after {2} tries"
                        .format(in_key, path, num_tries))

    def upsert_all_docs(self, inserted_keys, long_string, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.dict_upsert_sd(in_key, path, long_string)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to upsert key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def add_all_docs(self, inserted_keys, long_string, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.dict_add_sd(in_key, path, long_string)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to add key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def remove_all_docs(self, inserted_keys, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.delete_sd(in_key, path)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to remove value for key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def exists_all_docs(self, inserted_keys, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.exists_sd(in_key, path)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to validate value for key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def replace_all_docs(self, inserted_keys, long_string, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.replace_sd(in_key, path, long_string)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to replace for key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def array_push_last(self, inserted_keys, long_string, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.array_push_last_sd(in_key, path, long_string)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to  array push last for key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def array_push_first(self, inserted_keys, long_string, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.array_push_first_sd(in_key, path, long_string)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to  array push first for key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def counter_all_paths(self, inserted_keys, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.counter_sd(in_key, path, 10000)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to  counter incr/decr for key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def array_add_unique(self, inserted_keys, long_string, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.array_add_unique_sd(in_key, path, long_string)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to  add array_unique key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def multi_lookup_all_paths(self, inserted_keys, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.multi_lookup_sd(in_key, path)
                print(data)
            except Exception as e:
                print('[ERROR] {}'.format(e))
                self.helper.testcase.fail(
                    "Unable to  add array_unique key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

class DeeplyNestedDataSet(SubdocSanityTests):
    def __init__(self, helper, num_docs):
        self.helper = helper
        self.num_docs = num_docs
        self.name = "deeplynested_dataset"
        self.levels = 30
        self.log = logger.Logger.get_logger()

    def load(self, long_path=False):
        inserted_keys = self.helper.insert_nested_docs(self.num_docs, self.name, self.levels, long_path)
        return inserted_keys, self.levels

    def get_all_docs(self, inserted_keys, path):
        for in_key in inserted_keys:
            num_tries = 1
            try:
                opaque, cas, data = self.helper.client.get_sd(in_key, path)
                #self.log.info(data)
                #assert data == check_data
            except Exception as e:
                self.log.info(e)
                self.helper.testcase.fail(
                    "Unable to get key {0} for path {1} after {2} tries"
                    .format(in_key, path, num_tries))

    def upsert_all_docs(self):
        pass



