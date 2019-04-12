import time, os, json

from threading import Thread
import threading
from basetestcase import BaseTestCase
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper


class AutoRetryFailedRebalance(RebalanceBaseTest):

    def setUp(self):
        super(AutoRetryFailedRebalance, self).setUp()
        self.sleep_time = self.input.param("sleep_time", 30)
        self.enabled = self.input.param("enabled", True)
        self.afterTimePeriod = self.input.param("afterTimePeriod", 300)
        self.maxAttempts = self.input.param("maxAttempts", 1)
        self.log.info("Changing the retry rebalance settings ....")
        self.change_retry_rebalance_settings(enabled=self.enabled, afterTimePeriod=self.afterTimePeriod,
                                             maxAttempts=self.maxAttempts)

    def tearDown(self):
        self.reset_retry_rebalance_settings()
        super(AutoRetryFailedRebalance, self).tearDown()

    def test_auto_retry_of_failed_rebalance(self):
        self.stop_server(self.servers[1])
        self.sleep(self.sleep_time)
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], self.servers[1:])
            self.start_server(self.servers[1])
            rebalance.result()
        except Exception:
            rest = RestConnection(self.servers[0])
            result = json.loads(rest.get_pending_rebalance_info())
            retry_after_secs = result["retry_after_secs"]
            attempts_remaining = result["attempts_remaining"]
            retry_rebalance = result["retry_rebalance"]
            while retry_rebalance == "pending" and attempts_remaining:
                # wait for the afterTimePeriod for the failed rebalance to restart
                self.sleep(retry_after_secs, message="Waiting for the afterTimePeriod to complete")
                result = rest.monitorRebalance()
                msg = "successfully rebalanced cluster {0}"
                self.log.info(msg.format(result))
                result = json.loads(rest.get_pending_rebalance_info())
                self.log.info(msg.format(result))
                retry_rebalance = result["retry_rebalance"]
                if retry_rebalance == "not_pending":
                    break
                attempts_remaining = result["attempts_remaining"]
                retry_rebalance = result["retry_rebalance"]
                retry_after_secs = result["retry_after_secs"]
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")

