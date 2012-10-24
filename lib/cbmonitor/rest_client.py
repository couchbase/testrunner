import urllib
import logging

from lib.membase.api.rest_client import RestConnection
from lib.membase.api.exception import ServerUnavailableException

class CbmonitorClient(RestConnection):
    """Cbmonitor rest client"""

    def __init__(self, hostname="127.0.0.1", port=80):
        """Create rest client.

        hostname: cbmonitor dashboard hostname/ip address
        port: cbmonitor dashboard port
        """
        self.username = ''
        self.password = ''
        self.baseUrl = 'http://%s:%s/' % (hostname, port)

    def post_results(self, build, metrics):
        """Post litmus test result to cbmonitor dashboard

        build: build version, e.g: 2.0.0-1723-rel-enterprise
        metrics: metric and value pairs, e.g: {reb time-s: 120, latency-ms: 39}
        """
        if not build or not metrics:
            logging.error("invalid param build or metrics")
            return

        if not isinstance(metrics, dict):
            logging.error("param metrics must be dict type")
            return

        logging.info("posting metrics %s for build %s to cbmonitor"
                     % (metrics, build))

        for metric, value in metrics.iteritems():
            params = {'build': build,
                      'metric': metric,
                      'value': value}
            try:
                self._http_request(api, 'POST', urllib.urlencode(params),
                                   timeout=30)
            except ServerUnavailableException:
                logging.error("cbmonitor server %s is not available"
                              % self.baseUrl)
                return
