from fts_base import FTSBaseTest, FTSException
from fts_base import NodeHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class MovingTopFTS(FTSBaseTest):

    def setUp(self):
        super(MovingTopFTS, self).setUp()

    def tearDown(self):
        super(MovingTopFTS, self).tearDown()

    def simple_load(self):
        self.load_cluster()
        self.sleep(120)