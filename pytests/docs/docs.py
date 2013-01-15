import time
import logger
from basetestcase import BaseTestCase
from couchbase.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection

class DocsTests(BaseTestCase):

    def setUp(self):
        super(DocsTests, self).setUp()

    def tearDown(self):
        super(DocsTests, self).tearDown()

    def test_docs_int_big_values(self):
        degree = self.input.param("degree", 53)
        error = self.input.param("error", False)
        number = 2**degree
        first = ['james', 'sharon']
        template = '{{ "number": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('test_docs', template, [number,], first,
                                     start=0, end=self.num_items)
        self.log.info("create %s documents..." % (self.num_items))
        try:
            self._load_all_buckets(self.master, gen_load, "create", 0)
            self._verify_stats_all_buckets(self.servers)
        except Exception as e:
            if error:
               self.log.info("Unable to create documents as expected: %s" % str(e))
            else:
                raise e
        else:
            if error:
                self.fail("Able to create documents with value: %s" % str(number))

