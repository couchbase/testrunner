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

    def tearDown(self):
        super(ESKVTests, self).tearDown()

    def _async_modify_data(self):
        tasks = []
        """Setting up creates/updates/deletes at source nodes"""
        if self._doc_ops is not None:
            # allows multiple of them but one by one
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "create" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(elf.src_master, self.gen_create, "create", 0))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters.Create/Update/Delete operations are performed based on doc-ops specified by the user. """

    def load_with_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self._modify_src_data()
        self.verify_results()

    #overriding xdcr verify results method for specific es verification
    def verify_results(self, verify_src=False):
        self.verify_es_results(self, verify_src)

