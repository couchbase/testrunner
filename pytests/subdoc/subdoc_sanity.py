import logger
import unittest

from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.cluster import Cluster
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection

from membase.helper.subdoc_helper import SubdocHelper


class SubdocSanityTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SubdocHelper(self, "default")
        self.helper.setup_cluster()
        self.cluster = Cluster()
        self.servers = self.helper.servers

    def tearDown(self):
        self.helper.cleanup_cluster()

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

    def test_simple_dataset_insert(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Issue simple insert sub doc single path "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        inserted_keys = data_set.load()
        # TBD

    def test_deeply_nested_dataset_get(self):
        pass

class SimpleDataSet:
    def __init__(self, helper, num_docs):
        self.helper = helper
        self.num_docs = num_docs
        self.name = "simple_dataset"

    def load(self):
        inserted_keys = self.helper.insert_docs(self.num_docs, self.name)
        return inserted_keys

    def get_all_docs(self, inserted_keys, path):
            for in_key in inserted_keys:
                num_tries = 1
                try:
                    opaque, cas, data = self.helper.client.get_in(in_key, path)
                    print data
                except Exception as e:
                    self.helper.testcase.fail(
                        "Unable to get key {0} for path {1} after {2} tries"
                        .format(in_key, path, num_tries))

    def insert_all_paths(self):
        pass

    def replace_all_paths(self):
        pass

    def append_all_paths(self):
        pass

    def prepend_all_paths(self):
        pass

    def counter_all_paths(self):
        pass

    def multi_mutation_all_paths(self):
        pass


class DeeplyNestedDataSet:
    def __init__(self, helper, num_docs):
        self.helper = helper
        self.num_docs = num_docs
        self.name = "deeplynested_dataset"

    def load(self):
        inserted_keys = self.helper.insert_docs(self.num_docs, self.name)
        return inserted_keys

    def get_all_docs(self):
            pass



