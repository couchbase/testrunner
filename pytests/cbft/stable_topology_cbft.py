from cbft_base import CBFTBaseTest, CBFTException
from cbft_base import NodeHelper
from cbft_base import Utility
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class StableTopCBFT(CBFTBaseTest):

    def setUp(self):
        super(StableTopCBFT, self).setUp()

    def tearDown(self):
        super(StableTopCBFT, self).tearDown()

    def simple_load(self):
        self.load_cluster()
        self.async_perform_update_delete(['salary','dept'])
        self.sleep(120)