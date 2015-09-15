from cbft_base import CBFTBaseTest, CBFTException
from cbft_base import NodeHelper
from cbft_base import Utility
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class MovingTopCBFT(CBFTBaseTest):

    def setUp(self):
        super(MovingTopCBFT, self).setUp()

    def tearDown(self):
        super(MovingTopCBFT, self).tearDown()

    def simple_load(self):
        self.load_cluster()
        self.sleep(120)