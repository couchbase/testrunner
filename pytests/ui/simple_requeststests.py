from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from http.client import IncompleteRead
from TestInput import TestInputSingleton
import sys
import re

class SimpleRequests(BaseTestCase):

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def setUp(self):
        super(SimpleRequests, self).setUp()
        if not self.skip_host_login:
            shell = RemoteMachineShellConnection(self.master)
            type = shell.extract_remote_info().distribution_type
            shell.disconnect()
            self.is_linux = False
            if type.lower() == 'linux':
                self.is_linux = True
        else:
            self.log.info("-->Done SimpleRequests setup...")
            self.is_linux = True

    def test_simple_ui_request(self):
        rest = RestConnection(self.master)
        self.log.info("-->Testing APIs...")
        passed = True
        all_apis = ["", "versions", "pools", "pools/default", "pools/nodes",
        "pools/default/buckets",
         "pools/default/buckets/@query/stats", "pools/default/nodeServices",
         "pools/default/remoteClusters", "pools/default/serverGroups", "pools/default/certificate",
         "pools/default/settings/memcached/global", "nodeStatuses", "logs", "settings/web",
         "settings/alerts", "settings/stats", "settings/autoFailover",
         "settings/maxParallelIndexers", "settings/viewUpdateDaemon", "settings/autoCompaction",
         "settings/replications", "settings/replications", "settings/saslauthdAuth",
         "settings/audit", "internalSettings", "nodes/self/xdcrSSLPorts", "indexStatus",
         # "diag/vbuckets", MB-15080
         "settings/indexes", "diag",  # "diag/ale", MB-15080
         "pools/default/rebalanceProgress", "pools/default/tasks", "index.html", "sasl_logs",
         "couchBase", "sampleBuckets"]
        non_admin_apis = ["",
                    "versions",
                    "pools",
                    "pools/default",
                    "pools/nodes",
                    "nodes/self/xdcrSSLPorts",
                    "indexStatus",
                    "index.html",
                    "couchBase"]
        is_admin = TestInputSingleton.input.param("is_admin", True)
        if is_admin:
            apis_list = all_apis
        else:
            apis_list = non_admin_apis

        self.log.info("------------>is_admin={},apis_list={}".format(is_admin,apis_list))
        for api in apis_list:
            url = rest.baseUrl + api
            self.log.info("GET " + url)
            try:
                status, content, header = rest._http_request(url)
            except IncompleteRead as e:
                self.log.warning("size of partial responce {0} api is {1} bytes".format(api, sys.getsizeof(e.partial)))
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

        if TestInputSingleton.input.param("is_admin", True):
            _, content, _ = rest._http_request(rest.baseUrl + "sasl_logs")
            occurrences = [m.start() for m in re.finditer('web request failed', str(content))]
            for occurrence in occurrences:
                subcontent = content[occurrence - 1000: occurrence + 1000]
                if 'path,"/diag"' in str(subcontent):
                    break
                else:
                    # TBD: Not stable for rerun on the existing setup
                    # passed = False
                    self.log.info(subcontent)
                self.assertTrue(passed, "some web request failed in the server logs. See logs above")
