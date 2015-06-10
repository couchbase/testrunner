from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class SimpleRequests(BaseTestCase):
    def setUp(self):
        super(SimpleRequests, self).setUp()
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        if type.lower() == 'windows':
            self.is_linux = False
        else:
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
                    "pools/default/rebalanceProgress", "pools/default/tasks", "index.html", "sasl_logs", "sasl_logs",
                    "erlwsh/", "couchBase", "sampleBuckets"]:
            url = rest.baseUrl + api
            self.log.info("GET " + url)
            status, content, header = rest._http_request(url)
            self.log.info(header)
            if self.is_linux == False and api == "settings/saslauthdAuth":
                #This http API endpoint is only supported in enterprise edition running on GNU/Linux
                continue
            if not status:
                self.log.info("wrong status for {0} GET request {1}".format(url, header))
                passed = False
        self.assertTrue(passed, msg="some GET requests failed. See logs above")

        _, content, _ = rest._http_request(rest.baseUrl + "sasl_logs")
        foundIndex = content.find("web request failed")
        if foundIndex != -1:
            self.log.info(content[foundIndex - 1000: foundIndex + 1000])
            self.assertFalse(foundIndex, "some web request failed in server logs. See logs above")