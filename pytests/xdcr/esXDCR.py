from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
from esbasetests import ESReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from random import randrange

import time

#Assumption that at least 2 nodes on every cluster
class ESKVTests(XDCRReplicationBaseTest, ESReplicationBaseTest):
    def setUp(self):
        super(ESKVTests, self).setUp()
        self.setup_doc_gens()

    def tearDown(self):
        super(ESKVTests, self).tearDown()

    def setup_doc_gens(self):
        # create json doc generators
        ordering = range(self._num_items/4)
        sites1 = ['google', 'bing', 'yahoo', 'wiki']
        sites2 = ['mashable', 'techcrunch', 'hackernews', 'slashdot']
        template = '{{ "ordering": {0}, "site_name": "{1}" }}'

        delete_start= int((self._num_items) * (float)(100 - self._percent_delete) / 100)
        update_end = int((self._num_items) * (float)(self._percent_update) / 100)

        self.gen_create =\
            DocumentGenerator('es_xdcr_docs', template, ordering,
                               sites1, start=0, end=self._num_items)

        self.gen_recreate =\
            DocumentGenerator('es_xdcr_docs', template, ordering,
                               sites2, start=0, end=self._num_items)

        self.gen_update =\
            DocumentGenerator('es_xdcr_docs', template, ordering,
                               sites1, start=0, end=update_end)
        self.gen_delete =\
            DocumentGenerator('es_xdcr_docs', template, ordering,
                               sites1, start=delete_start, end=self._num_items)



    def _async_modify_data(self):
        tasks = []
        """Setting up creates/updates/deletes at source nodes"""
        if self._doc_ops is not None:
            # allows multiple of them but one by one
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "create" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_recreate, "create", 0))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters.Create/Update/Delete operations are performed based on doc-ops specified by the user. """
    def load_with_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self._async_modify_data()
        self.verify_results()

    #overriding xdcr verify results method for specific es verification
    def verify_results(self, verify_src=False):
        self.verify_es_results(self, verify_src)

