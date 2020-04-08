import sys
import urllib.request, urllib.parse, urllib.error
import logging
from functools import reduce

sys.path.append('lib')
from lib.membase.api.rest_client import RestConnection
from lib.membase.api.exception import ServerUnavailableException


class CbmonitorClient(RestConnection):
    """Cbmonitor rest client"""
    def __new__(self, hostname="127.0.0.1", port=80):
        """Funky method to override __new__ in RestConnection
        """
        return object.__new__(self, hostname, port)

    def __init__(self, hostname="127.0.0.1", port=80):
        """Create rest client.

        hostname: litmus dashboard hostname/ip address
        port: litmus dashboard port
        """
        self.username = ''
        self.password = ''
        self.baseUrl = 'http://%s:%s/' % (hostname, port)

    def post_results(self, build, testcase, env, metrics):
        """Post test result to litmus dashboard

        build -- build version (e.g., '2.0.0-1723-rel-enterprise')
        testcase -- testcase (e.g: 'lucky6')
        env -- test enviornment (e.g, 'terra')
        metrics: metric and value pairs, e.g: {Rebalance time, sec: 120, latency-ms: 39}
        """
        if not build or not metrics:
            logging.error("invalid param build or metrics")
            return

        if not isinstance(metrics, dict):
            logging.error("param metrics must be dict type")
            return

        logging.info("posting metrics %s for build %s to litmus dashboard"
                     % (metrics, build))
        api = self.baseUrl + 'litmus/post/'

        params = [('build', build), ('testcase', testcase), ('env', env)]
        params += reduce(lambda x, y: x + y,
                         [[('metric', k_v[0]), ('value', k_v[1])] for k_v in iter(metrics.items())])

        try:
            self._http_request(api, 'POST', urllib.parse.urlencode(params),
                               timeout=30)
        except ServerUnavailableException:
            logging.error("litmus dashboard server %s is not available"
                          % self.baseUrl)
            return
