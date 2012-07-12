import json
import urllib

from membase.api.rest_client import RestConnection
from membase.api.exception import ServerUnavailableException

class CbKarmaClient(RestConnection):
    """Performance dashboard (cbkarma) REST API"""

    def __init__(self, hostname, port='80'):
        """Create REST API client.

        Keyword arguments:
        hostname -- dashboard hostname/ip address
        port -- dashboard port
        """

        server_info = {'ip': hostname,
                       'port': port,
                       'username': '',
                       'password': ''}

        super(CbKarmaClient, self).__init__(server_info)

    def init(self):
        """Get initial test id (optional)"""

        api = self.baseUrl + 'init'
        try:
            return self._http_request(api, 'GET', timeout=30)
        except ServerUnavailableException:
            print "Dashboard is not available... bypassing."
            return (False, None)

    def update(self, id=None, build='', spec='', ini='', phase='', status=''):
        """Post test progress updates.

        Keyword arguments:
        id -- unique test id
        biuld -- build version
        spec -- configuration filename
        ini -- resource filename
        phase -- phase name
        status -- latest phase status ('started' or 'done')

        Return tuple with status and test id.
        """

        api = self.baseUrl + 'update'

        params = {'phase': phase,
                  'build': build,
                  'spec': spec,
                  'ini': ini,
                  'status': status}
        if id:
            params['id'] = id

        try:
            return self._http_request(api, 'POST', urllib.urlencode(params),
                                      timeout=30)
        except ServerUnavailableException:
            print "Dashboard is not available... bypassing."

    def histo(self, id=None, description='', attachment=''):
        """Attach latency histogram to the test"""
        api = self.baseUrl + 'histo'

        attachment = json.dumps(attachment)

        params = {'description': description,
                  'attachment': attachment}
        if id:
            params['id'] = id

        try:
            return self._http_request(api, 'POST', urllib.urlencode(params),
                                      timeout=30)
        except ServerUnavailableException:
            print "Dashboard is not available... bypassing."

    def report(self, id, filename, url):
        """Sumbit link to pdf report"""
        api = self.baseUrl + 'report'

        params = {'test_id': id, 'description': filename, 'url': url}

        try:
            return self._http_request(api, 'POST', urllib.urlencode(params),
                                      timeout=30)
        except ServerUnavailableException:
            print "Dashboard is not available... bypassing."