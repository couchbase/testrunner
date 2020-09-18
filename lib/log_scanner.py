from remote.remote_util import RemoteMachineShellConnection
from testconstants import WIN_COUCHBASE_LOGS_PATH, LINUX_COUCHBASE_LOGS_PATH


class LogScanner(object):
    def __init__(self, server):
        self.server = server
        self.service_log_keywords_map = {
            "all": {
                "babysitter.log": ["exception occurred in runloop", "failover exited with reason"],
                "memcached.log": ["CRITICAL"],
                "all": []
            },
            "cbas": {
                "analytics_error": ["fatal", "Analytics Service is temporarily unavailable",
                                    "Failed during startup task", "HYR0", "ASX", "IllegalStateException"]
            },
            "eventing": {
                "eventing.log": ["panic", "fatal"]
            },
            "fts": {
                "fts.log": ["panic", "fatal"]
            },
            "index": {
                "indexer.log": ["panic", "fatal", "Error parsing XATTR", "zero", "protobuf.Error",
                                "Encounter planner error", "corruption"]
            },
            "kv": {
                "projector.log": ["panic", "Error parsing XATTR", "fatal"],
                "*xdcr*.log": ["panic", "fatal"],
            },
            "n1ql": {
                "query.log": ["panic", "fatal", "Encounter planner error"]
            }
        }

        self.shell = RemoteMachineShellConnection(self.server)
        self.info = self.shell.extract_remote_info().type.lower()
        self.log_path = LINUX_COUCHBASE_LOGS_PATH + '/'
        self.cmd = "zgrep "
        if self.info == "windows":
            self.log_path = WIN_COUCHBASE_LOGS_PATH
            self.cmd = "grep "

        self.services = ["all"]
        for service in self.service_log_keywords_map.keys():
            if service in self.server.services:
                self.services.append(service)

    def scan(self):
        # log_matches_map = {log: {keyword: num matches}}
        log_matches_map = {}
        try:
            for service in self.services:
                for log in self.service_log_keywords_map[service].keys():
                    for keyword in self.service_log_keywords_map[service][log]:
                        cmd = self.cmd
                        if log == "all":
                            cmd += "\"{0}\" {1}{2} -R".format(keyword, self.log_path, '*')
                        else:
                            cmd += "\"{0}\" {1}{2}".format(keyword, self.log_path, log + '*')
                        matches, err = self.shell.execute_command(cmd)
                        if len(matches):
                            print("Number of matches : " + str(len(matches)) + "\nmatches : " + str(matches))
                            if log not in log_matches_map.keys():
                                log_matches_map[log] = {}
                                log_matches_map[log][keyword] = len(matches)
                            else:
                                if keyword not in log_matches_map[log].keys():
                                    log_matches_map[log][keyword] = len(matches)
                                else:
                                    log_matches_map[log][keyword] += len(matches)
        except:
            print("WARNING: Exception in log scanner, continuing..")
        self.shell.disconnect()
        return log_matches_map
