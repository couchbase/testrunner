from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest
from lib.remote.remote_util import RemoteMachineShellConnection
import yaml
import os 

class XDCRDifferTest(XDCRNewBaseTest):
    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_master_shell = RemoteMachineShellConnection(self.src_master)
        self.xdcr_differ_yaml_conf_path = "/tmp/xdcr_differ_params.yaml"
        
        outputFileDir = self._input.param("outputFileDir", "/tmp/xdcr_differ_outputs")

        # Refer https://github.com/couchbase/xdcrDiffer/blob/main/README.md
        self.xdcr_differ_params = {
            # Source cluster details
            "sourceUrl": self._input.param("sourceUrl", f"{self.src_master.ip}:8091"),
            "sourceUsername": self._input.param("sourceUsername", self.src_master.rest_username),
            "sourcePassword": self._input.param("sourcePassword", self.src_master.rest_password),
            "sourceBucketName": self._input.param("sourceBucketName", "default"),
            "remoteClusterName": self._input.param("remoteClusterName", "remote_cluster_C1-C2"),

            # Target cluster details
            "targetUrl": self._input.param("targetUrl", ""),
            "targetUsername": self._input.param("targetUsername", ""),
            "targetPassword": self._input.param("targetPassword", ""),
            "targetBucketName": self._input.param("targetBucketName", "default"),

            # Output File names
            "outputFileDir": outputFileDir,
            "sourceFileDir": self._input.param("sourceFileDir", f"{outputFileDir}/source"),
            "targetFileDir": self._input.param("targetFileDir", f"{outputFileDir}/target"),
            "checkpointFileDir": self._input.param("checkpointFileDir", f"{outputFileDir}/checkpoint"),
            "fileDifferDir": self._input.param("fileDifferDir", f"{outputFileDir}/fileDiff"),
            "mutationDifferDir": self._input.param("mutationDifferDir", f"{outputFileDir}/mutationDiff"),

            # Checkpointing details
            "oldCheckpointFileName": self._input.param("oldCheckpointFileName", ""),
            "newCheckpointFileName": self._input.param("newCheckpointFileName", ""),
            "checkpointInterval": self._input.param("checkpointInterval", 600),

            # Differ modes of operation
            "completeByDuration": self._input.param("completeByDuration", 0),
            "completeBySeqno": self._input.param("completeBySeqno", True),
            "compareType": self._input.param("compareType", "body"),
            "runDataGeneration": self._input.param("runDataGeneration", True),
            "runFileDiffer": self._input.param("runFileDiffer", True),
            "runMutationDiffer": self._input.param("runMutationDiffer", True),
            "enforceTLS": self._input.param("enforceTLS", False),
            "clearBeforeRun": self._input.param("clearBeforeRun", "true"),

            # Other configurable parameters
            "numberOfSourceDcpClients": self._input.param("numberOfSourceDcpClients", 1),
            "numberOfWorkersPerSourceDcpClient": self._input.param("numberOfWorkersPerSourceDcpClient", 64),
            "numberOfTargetDcpClients": self._input.param("numberOfTargetDcpClients", 1),
            "numberOfWorkersPerTargetDcpClient": self._input.param("numberOfWorkersPerTargetDcpClient", 64),
            "numberOfWorkersForFileDiffer": self._input.param("numberOfWorkersForFileDiffer", 30),
            "numberOfWorkersForMutationDiffer": self._input.param("numberOfWorkersForMutationDiffer", 30),
            "numberOfBins": self._input.param("numberOfBins", 5),
            "numberOfFileDesc": self._input.param("numberOfFileDesc", 500),
            "mutationDifferBatchSize": self._input.param("mutationDifferBatchSize", 100),
            "mutationDifferTimeout": self._input.param("mutationDifferTimeout", 30),
            "sourceDcpHandlerChanSize": self._input.param("sourceDcpHandlerChanSize", 100000),
            "targetDcpHandlerChanSize": self._input.param("targetDcpHandlerChanSize", 100000),
            "bucketOpTimeout": self._input.param("bucketOpTimeout", 20),
            "maxNumOfGetStatsRetry": self._input.param("maxNumOfGetStatsRetry", 10),
            "maxNumOfSendBatchRetry": self._input.param("maxNumOfSendBatchRetry", 10),
            "getStatsRetryInterval": self._input.param("getStatsRetryInterval", 2),
            "sendBatchRetryInterval": self._input.param("sendBatchRetryInterval", 500),
            "getStatsMaxBackoff": self._input.param("getStatsMaxBackoff", 10),
            "sendBatchMaxBackoff": self._input.param("sendBatchMaxBackoff", 5),
            "delayBetweenSourceAndTarget": self._input.param("delayBetweenSourceAndTarget", 2),
            "bucketBufferCapacity": self._input.param("bucketBufferCapacity", 100000),
            "mutationDifferRetries": self._input.param("mutationDifferRetries", 0),
            "mutationDifferRetriesWaitSecs": self._input.param("mutationDifferRetriesWaitSecs", 60),
            "numOfFiltersInFilterPool": self._input.param("numOfFiltersInFilterPool", 32),
            "debugMode": self._input.param("debugMode", False),
            "setupTimeout": self._input.param("setupTimeout", 10),
            "fileContaingXattrKeysForNoComapre": self._input.param("fileContaingXattrKeysForNoComapre", ""),
        }

        self.passphrase_value = self._input.param("encryptionPassphrase", "")
        self.encryptionLogFile = self._input.param("encryptedLogFile", "/tmp/xdcr_differ_encrypted")

        # write to a yaml file in /tmp
        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path)
        self.log.info(f"XDCR Differ parameters written to {self.xdcr_differ_yaml_conf_path}")

        self.xdcr_differ_bin_path = "/opt/couchbase/bin/xdcrDiffer"
        
        prefix = ""
        if self.passphrase_value:
            prefix = f'printf "{self.passphrase_value}\\n{self.passphrase_value}\\n" | '
        self.xdcr_differ_cmd = (
            f"export CBAUTH_REVRPC_URL=\"http://{self.xdcr_differ_params['sourceUsername']}:{self.xdcr_differ_params['sourcePassword']}@{self.xdcr_differ_params['sourceUrl']}\" && "
            f"{prefix}{self.xdcr_differ_bin_path} "
        )
        if self.passphrase_value:
            self.xdcr_differ_cmd += f" -encryptionPassphrase -encryptedLogFile {self.encryptionLogFile}"
        self.xdcr_differ_cmd += " -yamlConfigFilePath {self.xdcr_differ_yaml_conf_path}"


    def verify_xdcr_differ_ran(self):
        path_checks = {
            "Output directory": self.xdcr_differ_params['outputFileDir'],
            "Source file directory": self.xdcr_differ_params['sourceFileDir'],
            "Target file directory": self.xdcr_differ_params['targetFileDir'],
            "Checkpoint file directory": self.xdcr_differ_params['checkpointFileDir'],
            "File differ directory": self.xdcr_differ_params['fileDifferDir'],
            "Mutation differ directory": self.xdcr_differ_params['mutationDifferDir'],
        }
        for dir_name, remote_path in path_checks.items():
            cmd = f'test -d "{remote_path}"'
            _, _, exit_code = self.src_master_shell.execute_command(cmd, get_exit_code=True)
            if exit_code != 0:
                self.fail(f"{dir_name} does not exist on remote: {remote_path}")


    def test_xdcr_differ(self):
        self.setup_xdcr_and_load()
        self.log.info(f"Running XDCR Differ test with command: {self.xdcr_differ_cmd}")
        o, err = self.src_master_shell.execute_command(self.xdcr_differ_cmd, timeout=1000, use_channel=True)
        self.log.info(f"Output: {o}")
        self.log.info(f"Error: {err}")
        if err:
            self.fail("XDCR Differ binary failed to run")
        if self.verify_xdcr_differ_ran():
            self.fail("XDCR Differ did not produce the expected output")
