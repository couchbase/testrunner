from .base_2i import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class SecondaryIndexingPlasmaBasicTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingPlasmaBasicTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingPlasmaBasicTests, self).tearDown()

    def test_plasma_index_with_ephemeral_bucket(self):
        if self.gsi_type == "plasma":
            try:
                self.multi_create_index(query_definitions=self.query_definitions)
            except Exception as ex:
                msg = "Error=Ephemeral Buckets Must Use MOI Storage"
                if msg not in str(ex):
                    self.log.info(str(ex))
                    raise

    def test_increase_memory_beyond_max_limit(self):
        rest = RestConnection(self.master)
        memory_quota = 5000
        content = rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=memory_quota)
        if content:
            msg = "exceeds the maximum allowed quota"
            if not msg in content:
                raise
