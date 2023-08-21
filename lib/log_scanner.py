from remote.remote_util import RemoteMachineShellConnection
from testconstants import WIN_COUCHBASE_LOGS_PATH, LINUX_COUCHBASE_LOGS_PATH


class LogScanner(object):
    def __init__(self, server, exclude_keywords=None, skip_security_scan=True):
        self.server = server
        self.exclude_keywords = exclude_keywords
        self.skip_security_scan = skip_security_scan
        self.service_log_security_map = {
            "all": {
                "all": {
                        "keywords": [
                            # Authorization/password/key
                            "Basic\s[a-zA-Z]\{10,\}==",
                            "Menelaus-Auth-User:\[",
                            "BEGIN RSA PRIVATE KEY",
                            "(A3T[A-Z0-9]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}",
                            # tasklist /v
                            ".*\\(([^\\)}]|\\n)*[.*[\\\" '\\`\\(]+tasklist[\\\" '\\`\\)]+([^\\)}]|\\n)*/v.*\\)",
                            # ps -o command
                            ".*\\(([^\\)}]|\\n)*[.*[\\\" '\\`\\(]+ps[\\\" '\\`]+([^\\)}]|\\n)*command([^\\)}]|\\n)*\\)",
                        ]
                }
            }
        }

        # Structure of the map = {service:{logfile:{"keywords":[],"ignore_keywords":[]}}}
        self.service_log_keywords_map = {
            "all": {
                "babysitter.log": {
                    "keywords" : ["exception occurred in runloop", "failover exited with reason"],
                },
                "memcached.log": {
                    "keywords" : ["CRITICAL", "ERROR"],
                    "ignore_keywords" : ["XERROR"]
                },
                "sanitizers.log.*": {
                    "keywords" : ['^'],
                },
                "all": {
                    "keywords" : [],
                },
            },
            "cbas": {
                "analytics_error": {
                    "keywords" : ["Analytics Service is temporarily unavailable",
                                  "Failed during startup task", "ASX", "IllegalStateException"]
                }
            },
            "eventing": {
                "eventing.log": {
                    "keywords" : ["panic"]
                }
            },
            "fts": {
                "fts.log": {
                    "keywords" : ["panic"]
                    }
            },
            "index": {
                "indexer.log": {
                    "keywords" : ["panic in", "panic:", "Error parsing XATTR",
                                  "corruption"]
                }
            },
            "kv": {
                "projector.log": {
                    "keywords" : ["panic", "Error parsing XATTR"]
                },
                "*xdcr*.log": {
                    "keywords" : ["panic", "non-recoverable error from xmem client. response status=KEY_ENOENT",
                                  "Execution timed out",
                                  "initConnection error"]
                },
            },
            "n1ql": {
                "query.log": {
                    "keywords" : ["panic","FATAL","SEVERE","ERROR","Fatal","Error","Severe"],
                    "ignore_keywords" : ["not available"]
                }
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
        log_matches_map = {}
        try:
            for service in self.services:
                for log in self.service_log_keywords_map[service].keys():
                    keywords = self.service_log_keywords_map[service][log]["keywords"]
                    ignore_keywords = self.service_log_keywords_map[service][log]["ignore_keywords"] \
                        if "ignore_keywords" in self.service_log_keywords_map[service][log] \
                            else []
                    if not self.skip_security_scan:
                        if service in self.service_log_security_map.keys():
                            if log in self.service_log_security_map[service].keys():
                                keywords += self.service_log_security_map[service][log]["keywords"]
                                ignore_keywords += self.service_log_security_map[service][log]["ignore_keywords"] \
                                    if "ignore_keywords" in self.service_log_security_map[service][log] \
                                        else []
                    if self.exclude_keywords:
                        ignore_keywords += self.exclude_keywords
                    if ignore_keywords:
                        ignore_keywords_joined = "|".join(ignore_keywords)
                    for keyword in keywords:
                        cmd = self.cmd
                        if log == "all":
                            cmd += "\"{0}\" {1}{2}".format(keyword, self.log_path, '*')
                        else:
                            cmd += "\"{0}\" {1}{2}".format(keyword, self.log_path, log + '*')
                        if ignore_keywords:
                            cmd = f'{cmd} | {self.cmd} -Ev \"{ignore_keywords_joined}\"'
                        matches, err = self.shell.execute_command(cmd, debug=False)
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
            print("WARNING: Exception in log scanner, continuing")
        self.shell.disconnect()
        return log_matches_map