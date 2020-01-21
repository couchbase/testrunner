from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from http.client import IncompleteRead
import sys
import re

class SimpleRequests(BaseTestCase):
    def setUp(self):
        super(SimpleRequests, self).setUp()
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        self.is_linux = False
        if type.lower() == 'linux':
            self.is_linux = True

    def test_simple_ui_request(self):
        rest = RestConnection(self.master)
        passed = True
        for api in ["", "versions", "pools", "pools/default", "pools/nodes", "pools/default/overviewStats",
                    "pools/default/buckets", "pools/default/buckets/@query/stats", "pools/default/nodeServices",
                    "pools/default/remoteClusters", "pools/default/serverGroups", "pools/default/certificate",
                    "pools/default/settings/memcached/global", "nodeStatuses", "logs", "settings/web",
                    "settings/alerts",
                    "settings/stats", "settings/autoFailover", "settings/maxParallelIndexers",
                    "settings/viewUpdateDaemon",
                    "settings/autoCompaction", "settings/replications", "settings/replications",
                    "settings/saslauthdAuth", "settings/audit", "internalSettings", "nodes/self/xdcrSSLPorts",
                    "indexStatus",
                    #"diag/vbuckets", MB-15080
                    "settings/indexes", "diag",
                    #"diag/ale", MB-15080
                    "pools/default/rebalanceProgress", "pools/default/tasks", "index.html", "sasl_logs",
                    "couchBase", "sampleBuckets"]:
            url = rest.baseUrl + api
            self.log.info("GET " + url)
            try:
                status, content, header = rest._http_request(url)
            except IncompleteRead as e:
                self.log.warn("size of partial responce {0} api is {1} bytes".format(api, sys.getsizeof(e.partial)))
                if api != "diag":
                    #otherwise for /diag API we should increase request time for dynamic data in _http_request
                    passed = False
                continue
            self.log.info(header)
            if not self.is_linux and api == "settings/saslauthdAuth":
                #This http API endpoint is only supported in enterprise edition running on GNU/Linux
                continue
            if not status:
                self.log.info("wrong status for {0} GET request {1}".format(url, header))
                passed = False
        self.assertTrue(passed, msg="some GET requests failed. See logs above")

        _, content, _ = rest._http_request(rest.baseUrl + "sasl_logs")
        occurrences = [m.start() for m in re.finditer('web request failed', str(content))]
        for occurrence in occurrences:
            subcontent = content[occurrence - 1000: occurrence + 1000]
            if 'path,"/diag"' in str(subcontent):
                break
            else:
                passed = False
                self.log.info(subcontent)
            self.assertTrue(passed, "some web request failed in the server logs. See logs above")
