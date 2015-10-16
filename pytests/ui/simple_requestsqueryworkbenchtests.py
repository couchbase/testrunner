from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
import re

class SimpleQueryRequests(BaseTestCase):
    def setUp(self):
        super(SimpleQueryRequests, self).setUp()
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        self.log.info(type)
        shell.disconnect()
        self.is_linux = False
        if type.lower() == 'linux':
            self.is_linux = True

    def test_simple_ui_request(self):
        rest = RestConnection(self.master)
        passed = True
        self.log.info("GET " + rest.query_baseUrl)
        status, content, header = rest._http_request(rest.query_baseUrl)
        self.log.info(header)
        if not status:
            self.log.info("wrong status for {0} GET request {1}".format(rest.query_baseUrl, header))
            passed = False
        self.assertTrue(passed, msg="some GET requests failed. See logs above")

        _, content, _ = rest._http_request(rest.query_baseUrl)
        occurrences = [m.start() for m in re.finditer('web request failed', content)]
        for occurrence in occurrences:
            subcontent = content[occurrence - 1000: occurrence + 1000]
            if 'path,"/diag"' in subcontent:
                break
            else:
                passed = False
                self.log.info(subcontent)
            self.assertTrue(passed, "some web request failed in the server logs. See logs above")
