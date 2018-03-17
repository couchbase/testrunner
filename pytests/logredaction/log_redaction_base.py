from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
import json, re


class LogRedactionBase(BaseTestCase):
    def setUp(self):
        super(LogRedactionBase, self).setUp()
        self.log_redaction_level = self.input.param("redaction_level", "none")

    def tearDown(self):
        super(LogRedactionBase, self).tearDown()

    def set_redaction_level(self):
        '''
        Sets log redaction at cluster level
        :return: None
        '''
        rest_conn = RestConnection(self.master)
        if rest_conn.set_log_redaction_level(redaction_level=self.log_redaction_level):
            self.log.info("Redaction level set successfully")
        else:
            self.fail("Redaction level not set as expected")

    def start_logs_collection(self):
        '''
        Kicks off log collection at the master node for all nodes in the cluster with set redaction level
        :return: None
        '''
        shell = RemoteMachineShellConnection(self.master)
        command = "curl -X POST -u Administrator:password http://" + self.master.ip + ":8091/controller/startLogsCollection " \
                  "-d nodes=\"*\" -d logRedactionLevel=" + self.log_redaction_level
        output, error = shell.execute_command(command=command)
        shell.log_command_output(output, error)
        self.log.info("Log collection started")
        shell.disconnect()

    def monitor_logs_collection(self):
        '''
        Monitors the log collection until it completes
        :return: final json that contains path to collected log file
        '''
        shell = RemoteMachineShellConnection(self.master)
        command = "curl -X GET -u Administrator:password http://" + self.master.ip + ":8091/pools/default/tasks"
        progress = 0
        status = ""
        while progress != 100 or status == "running":
            output, error = shell.execute_command(command=command)
            tmp = output[0]
            tmp_items = json.loads(tmp)
            content = {}
            for k in tmp_items:
                if k["type"] == "clusterLogsCollection":
                    content = k
                    break
            progress = content["progress"]
            status = content["status"]
            self.log.info("waiting for collection to complete..")
            self.sleep(10)
        self.log.info("Collection completed successfully")
        shell.disconnect()
        return content

    def verify_log_files_exist(self, remotepath=None, redactFileName=None, nonredactFileName=None):
        '''
        Verifies if log files exist after collection
        :param remotepath: absolute path to log files
        :param redactFileName: redacted zip log file name
        :param nonredactFileName: non-redacted zip log file name
        :return:
        '''
        if not remotepath:
            self.fail("Remote path needed to verify if log files exist")
        shell = RemoteMachineShellConnection(self.master)
        if shell.file_exists(remotepath=remotepath, filename=nonredactFileName):
            self.log.info("Regular non-redacted log file exists as expected")
        else:
            self.fail("Regular non-redacted log file does not exist")
        if redactFileName and self.log_redaction_level == "partial":
            if shell.file_exists(remotepath=remotepath, filename=redactFileName):
                self.log.info("Redacted file exists as expected")
            else:
                self.log.info("Redacted file does not exist")
        shell.disconnect()

    def verify_log_redaction(self, remotepath=None, redactFileName=None, nonredactFileName=None, logFileName=None):
        '''
        Given a redacted file and a non-redacted file, extracts all tagged user data from both files with line
        numbers, then compares them by line number and validates the redacted content against non-redacted
        using a regex
        :param remotepath: absolute path to log files
        :param redactFileName: redacted zip log file name
        :param nonredactFileName: non-redacted zip log file name
        :param logFileName: log file being validated inside the zips
        :return:
        '''
        shell = RemoteMachineShellConnection(self.master)
        command = "zipinfo " + remotepath + nonredactFileName + " | grep " + logFileName + " | awk '{print $9}'"
        output, error = shell.execute_command(command=command)
        shell.log_command_output(output, error)
        log_file_name = output[0]

        command = "zipgrep -n -o \"<ud>.+</ud>\" " + remotepath + nonredactFileName +  " " + log_file_name + " | cut -f2 -d:"
        ln_output, _ = shell.execute_command(command=command)
        command = "zipgrep -n -o \"<ud>.+</ud>\" " + remotepath + nonredactFileName + " " + log_file_name + " | cut -f3 -d:"
        match_output, _ = shell.execute_command(command=command)
        if len(ln_output) == 0 and len(match_output) == 0:
            self.fail("No user data tags found in " + remotepath + nonredactFileName)
        nonredact_dict = dict(zip(ln_output, match_output))
        nonredact_dict_unique = {}
        for key, value in nonredact_dict.items():
            if value not in nonredact_dict_unique.values():
                nonredact_dict_unique[key] = value
        self.log.info("Line numbers and unique non-redacted tags: " + str(nonredact_dict_unique))

        command = "zipgrep -n -o \"<ud>.+</ud>\" " + remotepath + redactFileName + " " + log_file_name + " | cut -f2 -d:"
        ln_output, _ = shell.execute_command(command=command)
        command = "zipgrep -n -o \"<ud>.+</ud>\" " + remotepath + redactFileName + " " + log_file_name + " | cut -f3 -d:"
        match_output, _ = shell.execute_command(command=command)
        if len(ln_output) == 0 and len(match_output) == 0:
            self.fail("No user data tags found in " + remotepath + redactFileName)
        redact_dict = dict(zip(ln_output, match_output))
        redact_dict_unique = {}
        for key, value in redact_dict.items():
            if value not in redact_dict_unique.values():
                redact_dict_unique[key] = value
        self.log.info("Line numbers and unique redacted tags: " + str(redact_dict_unique))

        #TODO For now, we are just validating the redacted tag contents with a regex for SHA1 --> [a-f0-9]{40}
        #TODO Should replace it with hashlib function
        for key, value in nonredact_dict_unique.items():
            if key not in redact_dict_unique.keys():
                self.fail("Line: " + key + " Value: " + value + " not found in redacted file")
            else:
                redact_value = redact_dict_unique[key]
                non_redact_content = re.search("<ud>.+</ud>", value).group(0)
                redact_content = re.search("<ud>.+</ud>", redact_value).group(0)
                if non_redact_content != redact_content and re.search("[a-f0-9]{40}", redact_content):
                    self.log.info("Line: " + key + " Non-redacted content: " + non_redact_content +
                                  " hashed correctly as " + redact_content)
                else:
                    self.fail("Hashing failed for Line: " + key + " Non-redacted content: " + non_redact_content)

        shell.disconnect()
