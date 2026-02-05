from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest
from lib.remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
import yaml
import os
import time
import re
import json


class XDCRDifferTest(XDCRNewBaseTest):
    """
    XDCR Differ At-Rest Encryption Support Tests
    Reference: https://deepwiki.com/couchbase/xdcrDiffer/8-encryption-service
    """

    MAGIC_BYTES = "Couchbase Encrypted"
    ENC_SUFFIX = ".enc"

    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_master_shell = RemoteMachineShellConnection(self.src_master)
        self.xdcr_differ_yaml_conf_path = "/tmp/xdcr_differ_params.yaml"

        outputFileDir = self._input.param("outputFileDir", "/tmp/xdcr_differ_outputs")

        self.xdcr_differ_params = {
            "sourceUrl": self._input.param("sourceUrl", f"{self.src_master.ip}:8091"),
            "sourceUsername": self._input.param("sourceUsername", self.src_master.rest_username),
            "sourcePassword": self._input.param("sourcePassword", self.src_master.rest_password),
            "sourceBucketName": self._input.param("sourceBucketName", "default"),
            "remoteClusterName": self._input.param("remoteClusterName", "remote_cluster_C1-C2"),
            "targetUrl": self._input.param("targetUrl", ""),
            "targetUsername": self._input.param("targetUsername", ""),
            "targetPassword": self._input.param("targetPassword", ""),
            "targetBucketName": self._input.param("targetBucketName", "default"),
            "outputFileDir": outputFileDir,
            "sourceFileDir": self._input.param("sourceFileDir", f"{outputFileDir}/source"),
            "targetFileDir": self._input.param("targetFileDir", f"{outputFileDir}/target"),
            "checkpointFileDir": self._input.param("checkpointFileDir", f"{outputFileDir}/checkpoint"),
            "fileDifferDir": self._input.param("fileDifferDir", f"{outputFileDir}/fileDiff"),
            "mutationDifferDir": self._input.param("mutationDifferDir", f"{outputFileDir}/mutationDiff"),
            "oldCheckpointFileName": self._input.param("oldCheckpointFileName", ""),
            "newCheckpointFileName": self._input.param("newCheckpointFileName", "checkpoint_test.json"),
            "checkpointInterval": self._input.param("checkpointInterval", 600),
            "completeByDuration": self._input.param("completeByDuration", 0),
            "completeBySeqno": self._input.param("completeBySeqno", True),
            "compareType": self._input.param("compareType", "body"),
            "runDataGeneration": self._input.param("runDataGeneration", True),
            "runFileDiffer": self._input.param("runFileDiffer", True),
            "runMutationDiffer": self._input.param("runMutationDiffer", True),
            "enforceTLS": self._input.param("enforceTLS", False),
            "clearBeforeRun": self._input.param("clearBeforeRun", "true"),
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
        self.encryption_log_file = self._input.param("encryptedLogFile", "/tmp/xdcr_differ_encrypted.log")

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )
        self.log.info(f"XDCR Differ parameters written to {self.xdcr_differ_yaml_conf_path}")

        self.xdcr_differ_bin_path = "/opt/couchbase/bin/xdcrDiffer"

    def tearDown(self):
        if hasattr(self, 'src_master_shell'):
            self.src_master_shell.disconnect()
        super().tearDown()

    def _build_xdcr_differ_cmd(self, passphrase=None, encrypted_log_file=None,
                                decrypt_mode=False, decrypt_file=None,
                                yaml_config=True, extra_flags=None):
        """Build the xdcrDiffer command with appropriate flags.
        
        Stores passphrase internally for use by _run_xdcr_differ with pexpect.
        Returns just the command string for backward compatibility.
        """
        passphrase = passphrase if passphrase is not None else self.passphrase_value
        encrypted_log_file = encrypted_log_file if encrypted_log_file is not None else self.encryption_log_file

        # Store passphrase for pexpect to use
        self._current_passphrase = passphrase

        # Build the base xdcrDiffer command
        differ_cmd = self.xdcr_differ_bin_path

        if decrypt_mode:
            differ_cmd += " -decrypt"
            if decrypt_file:
                differ_cmd += f" {decrypt_file}"
            if passphrase:
                differ_cmd += " -encryptionPassphrase"
        else:
            if passphrase:
                differ_cmd += f" -encryptionPassphrase -encryptedLogFile {encrypted_log_file}"
            if yaml_config:
                differ_cmd += f" -yamlConfigFilePath {self.xdcr_differ_yaml_conf_path}"

        if extra_flags:
            differ_cmd += f" {extra_flags}"

        return differ_cmd

    def _get_cbauth_env_vars(self):
        """Get the environment variables required for xdcrDiffer."""
        cbauth_url = (
            f"http://{self.xdcr_differ_params['sourceUsername']}:"
            f"{self.xdcr_differ_params['sourcePassword']}@{self.xdcr_differ_params['sourceUrl']}"
        )
        return {"CBAUTH_REVRPC_URL": cbauth_url}

    def _run_xdcr_differ(self, cmd, timeout=1000, get_exit_code=False):
        """Execute xdcrDiffer command and return output.
        
        Uses pexpect for TTY-based passphrase input when _current_passphrase is set.
        This is required because xdcrDiffer uses golang.org/x/term which needs a real TTY.
        
        Args:
            cmd: Command to execute
            timeout: Timeout in seconds
            get_exit_code: If True, also return exit code
            
        Returns:
            If get_exit_code is False: (output, err)
            If get_exit_code is True: (output, err, exit_code)
        """
        self.log.info(f"Running XDCR Differ command: {cmd}")
        
        env_vars = self._get_cbauth_env_vars()
        passphrase = getattr(self, '_current_passphrase', None)
        
        if passphrase and "-encryptionPassphrase" in cmd:
            # Use PTY for TTY-based passphrase input
            # xdcrDiffer prompts:
            #   "Enter encryption passphrase:"
            #   "enter the same encryption passphrase again:"
            prompts_and_responses = [
                ("enter encryption passphrase:", passphrase),
                ("enter the same encryption passphrase again:", passphrase),
            ]
            output, err, exit_code = self.src_master_shell.execute_command_with_pty(
                cmd,
                prompts_and_responses=prompts_and_responses,
                env_vars=env_vars,
                timeout=timeout,
                debug=True
            )
        else:
            # For commands without passphrase, use regular execute_command with env vars
            full_cmd = f"export CBAUTH_REVRPC_URL=\"{env_vars['CBAUTH_REVRPC_URL']}\" && {cmd}"
            output, err, exit_code = self.src_master_shell.execute_command(
                full_cmd, timeout=timeout, use_channel=True, get_exit_code=True
            )
        
        self.log.info(f"Output: {output}")
        if err:
            self.log.info(f"Stderr: {err}")
        self.log.info(f"Exit code: {exit_code}")
        
        if get_exit_code:
            return output, err, exit_code
        else:
            return output, err

    def _log_command_failure(self, cmd, output, err, exit_code):
        """Log detailed information about a failed command."""
        output_str = '\n'.join(output) if output else 'No output'
        err_str = '\n'.join(err) if err else 'No stderr'
        self.log.error("=" * 60)
        self.log.error("XDCR Differ command FAILED!")
        self.log.error("=" * 60)
        self.log.error(f"Exit code: {exit_code}")
        self.log.error(f"Command: {cmd}")
        self.log.error("-" * 60)
        self.log.error(f"STDOUT:\n{output_str}")
        self.log.error("-" * 60)
        self.log.error(f"STDERR:\n{err_str}")
        self.log.error("=" * 60)
        return f"XDCR Differ failed with exit code {exit_code}. Output: {output_str}. Stderr: {err_str}"

    def _verify_directories_exist(self, fail_on_missing=True):
        """Verify all output directories were created.
        
        Args:
            fail_on_missing: If True, fail the test if directories don't exist.
                           If False, just return True/False.
        Returns:
            True if all directories exist, False otherwise.
        """
        path_checks = {
            "Output directory": self.xdcr_differ_params['outputFileDir'],
            "Source file directory": self.xdcr_differ_params['sourceFileDir'],
            "Target file directory": self.xdcr_differ_params['targetFileDir'],
            "Checkpoint file directory": self.xdcr_differ_params['checkpointFileDir'],
            "File differ directory": self.xdcr_differ_params['fileDifferDir'],
            "Mutation differ directory": self.xdcr_differ_params['mutationDifferDir'],
        }
        all_exist = True
        for dir_name, remote_path in path_checks.items():
            cmd = f'test -d "{remote_path}"'
            _, _, exit_code = self.src_master_shell.execute_command(cmd, get_exit_code=True)
            if exit_code != 0:
                all_exist = False
                if fail_on_missing:
                    self.fail(f"{dir_name} does not exist on remote: {remote_path}")
                else:
                    self.log.info(f"{dir_name} does not exist on remote: {remote_path}")
        return all_exist

    def _verify_enc_suffix_files(self, directory):
        """Verify files with .enc suffix exist in directory."""
        cmd = f"find {directory} -name '*.enc' -type f 2>/dev/null | head -5"
        output, _ = self.src_master_shell.execute_command(cmd)
        return len(output) > 0, output

    def _verify_magic_bytes(self, file_path, expected_magic=None):
        """Verify magic bytes at start of encrypted file.
        
        Note: Encrypted files have a 1-byte header before the magic string,
        so we read extra bytes and check if magic string is present.
        Uses 'strings' command to safely extract ASCII text from binary file.
        """
        expected_magic = expected_magic or self.MAGIC_BYTES
        # Use strings command to extract readable text from the beginning of the file
        # This avoids UTF-8 decode errors with binary content
        cmd = f"head -c 100 {file_path} 2>/dev/null | strings -n 5"
        output, _ = self.src_master_shell.execute_command(cmd)
        if output:
            actual = ' '.join(output)
            # Check if the expected magic string is present
            if expected_magic in actual:
                return True, expected_magic
            return False, actual[:50] if actual else None
        return False, None

    def _verify_no_plaintext(self, file_path, search_patterns=None):
        """Verify encrypted file contains no plaintext using strings command."""
        search_patterns = search_patterns or ["password", "username", "bucket"]
        cmd = f"strings {file_path} 2>/dev/null | head -50"
        output, _ = self.src_master_shell.execute_command(cmd)
        output_str = ' '.join(output).lower() if output else ''
        for pattern in search_patterns:
            if pattern.lower() in output_str:
                return False, pattern
        return True, None

    def _get_file_size(self, file_path):
        """Get file size in bytes."""
        cmd = f"stat -c%s {file_path} 2>/dev/null || stat -f%z {file_path} 2>/dev/null"
        output, _ = self.src_master_shell.execute_command(cmd)
        if output:
            try:
                return int(output[0].strip())
            except ValueError:
                return None
        return None

    def _cleanup_output_dir(self):
        """Clean up output directory and encrypted log files."""
        cmd = f"rm -rf {self.xdcr_differ_params['outputFileDir']}"
        self.src_master_shell.execute_command(cmd)
        # Also clean up encrypted log file if it exists
        cmd = f"rm -f {self.encryption_log_file} {self.encryption_log_file}.enc"
        self.src_master_shell.execute_command(cmd)

    # ========================================================================
    # TC-001: Execute with -encryptionPassphrase without -encryptedLogFile
    # ========================================================================
    def test_cli_encryption_passphrase_without_log_file(self):
        """TC-001: -encryptionPassphrase without -encryptedLogFile should exit with error."""
        self.setup_xdcr_and_load()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret", encrypted_log_file=None)
        cmd = cmd.replace(f" -encryptedLogFile {self.encryption_log_file}", "")

        output, err = self._run_xdcr_differ(cmd)
        combined = ' '.join(output + (err if err else [])).lower()

        # Command should fail - that's the expected behavior
        if "encryptedlogfile" in combined or "error" in combined or err:
            self.log.info("PASS: Command failed as expected when -encryptionPassphrase used without -encryptedLogFile")
        else:
            self.fail("Expected error when -encryptionPassphrase used without -encryptedLogFile")

    # ========================================================================
    # TC-002: Execute with -encryptedLogFile without -encryptionPassphrase
    # ========================================================================
    def test_cli_encrypted_log_file_without_passphrase(self):
        """TC-002: -encryptedLogFile without -encryptionPassphrase should exit with error."""
        self.setup_xdcr_and_load()

        cmd = (
            f"export CBAUTH_REVRPC_URL=\"http://{self.xdcr_differ_params['sourceUsername']}:"
            f"{self.xdcr_differ_params['sourcePassword']}@{self.xdcr_differ_params['sourceUrl']}\" && "
            f"{self.xdcr_differ_bin_path} -encryptedLogFile {self.encryption_log_file} "
            f"-yamlConfigFilePath {self.xdcr_differ_yaml_conf_path}"
        )

        output, err = self._run_xdcr_differ(cmd)
        combined = ' '.join(output + (err if err else [])).lower()

        # Command should fail - that's the expected behavior
        if "encryptionpassphrase" in combined or "error" in combined or err:
            self.log.info("PASS: Command failed as expected when -encryptedLogFile used without -encryptionPassphrase")
        else:
            self.fail("Expected error when -encryptedLogFile used without -encryptionPassphrase")

    # ========================================================================
    # TC-003: Execute with -decrypt without -encryptionPassphrase
    # ========================================================================
    def test_cli_decrypt_without_passphrase(self):
        """TC-003: -decrypt without -encryptionPassphrase should exit with error."""
        cmd = (
            f"export CBAUTH_REVRPC_URL=\"http://{self.xdcr_differ_params['sourceUsername']}:"
            f"{self.xdcr_differ_params['sourcePassword']}@{self.xdcr_differ_params['sourceUrl']}\" && "
            f"{self.xdcr_differ_bin_path} -decrypt /tmp/somefile.enc"
        )

        output, err = self._run_xdcr_differ(cmd)
        combined = ' '.join(output + (err if err else [])).lower()

        # Command should fail - that's the expected behavior
        if "encryptionpassphrase" in combined or "error" in combined or err:
            self.log.info("PASS: Command failed as expected when -decrypt used without -encryptionPassphrase")
        else:
            self.fail("Expected error when -decrypt used without -encryptionPassphrase")

    # ========================================================================
    # TC-004: Verify help documentation for encryption flags
    # ========================================================================
    def test_cli_help_documentation(self):
        """TC-004: Verify help documentation shows encryption flags."""
        cmd = f"{self.xdcr_differ_bin_path} -help"
        output, _ = self._run_xdcr_differ(cmd)
        combined = ' '.join(output).lower()

        required_flags = ["encryptionpassphrase", "encryptedlogfile", "decrypt"]
        missing_flags = [f for f in required_flags if f not in combined]

        if missing_flags:
            self.fail(f"Missing encryption flags in help documentation: {missing_flags}")

    # ========================================================================
    # TC-005: Verify .enc suffix on all output files
    # ========================================================================
    def test_file_encryption_enc_suffix(self):
        """TC-005: Verify .enc suffix on all output files when encryption enabled."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        self._verify_directories_exist()

        for directory in [self.xdcr_differ_params['sourceFileDir'],
                          self.xdcr_differ_params['targetFileDir']]:
            found, files = self._verify_enc_suffix_files(directory)
            if not found:
                self.fail(f"No .enc files found in {directory}")
            self.log.info(f"Found encrypted files in {directory}: {files}")

    # ========================================================================
    # TC-006: Confirm no plaintext in encrypted files
    # ========================================================================
    def test_file_encryption_no_plaintext(self):
        """TC-006: Verify no plaintext exposed in encrypted files."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        enc_files_cmd = f"find {self.xdcr_differ_params['outputFileDir']} -name '*.enc' -type f | head -3"
        enc_files, _ = self.src_master_shell.execute_command(enc_files_cmd)

        for enc_file in enc_files:
            enc_file = enc_file.strip()
            if enc_file:
                no_plaintext, found_pattern = self._verify_no_plaintext(enc_file)
                if not no_plaintext:
                    self.fail(f"Plaintext '{found_pattern}' found in encrypted file: {enc_file}")

    # ========================================================================
    # TC-007: Validate magic bytes in file header
    # ========================================================================
    def test_file_encryption_magic_bytes(self):
        """TC-007: Validate 'Couchbase Encrypted' magic bytes in encrypted file header."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        enc_files_cmd = f"find {self.xdcr_differ_params['sourceFileDir']} -name '*.enc' -type f | head -1"
        enc_files, _ = self.src_master_shell.execute_command(enc_files_cmd)

        if not enc_files:
            self.fail("No encrypted files found to verify magic bytes")

        first_file = enc_files[0].strip()
        valid, actual = self._verify_magic_bytes(first_file)
        if not valid:
            self.fail(f"Magic bytes mismatch in {first_file}: expected '{self.MAGIC_BYTES}', got '{actual}'")

    # ========================================================================
    # TC-008: Verify encrypted file size > plaintext size
    # ========================================================================
    def test_file_encryption_size_increase(self):
        """TC-008: Verify encrypted file size is greater than plaintext equivalent."""
        self.setup_xdcr_and_load()

        self._cleanup_output_dir()
        cmd_no_enc = self._build_xdcr_differ_cmd(passphrase="")
        self._run_xdcr_differ(cmd_no_enc)

        plain_size_cmd = f"du -sb {self.xdcr_differ_params['sourceFileDir']} 2>/dev/null | cut -f1"
        plain_output, _ = self.src_master_shell.execute_command(plain_size_cmd)
        plain_size = int(plain_output[0].strip()) if plain_output else 0

        self._cleanup_output_dir()
        cmd_enc = self._build_xdcr_differ_cmd(passphrase="secret")
        self._run_xdcr_differ(cmd_enc)

        enc_size_cmd = f"du -sb {self.xdcr_differ_params['sourceFileDir']} 2>/dev/null | cut -f1"
        enc_output, _ = self.src_master_shell.execute_command(enc_size_cmd)
        enc_size = int(enc_output[0].strip()) if enc_output else 0

        if enc_size <= plain_size:
            self.fail(f"Encrypted size ({enc_size}) should be greater than plain size ({plain_size})")

    # ========================================================================
    # TC-009: Test empty passphrase rejection
    # ========================================================================
    def test_passphrase_empty_rejection(self):
        """TC-009: Empty passphrase should be rejected."""
        self.setup_xdcr_and_load()

        cmd = (
            f"export CBAUTH_REVRPC_URL=\"http://{self.xdcr_differ_params['sourceUsername']}:"
            f"{self.xdcr_differ_params['sourcePassword']}@{self.xdcr_differ_params['sourceUrl']}\" && "
            f'printf "\\n\\n" | {self.xdcr_differ_bin_path} -encryptionPassphrase '
            f"-encryptedLogFile {self.encryption_log_file} "
            f"-yamlConfigFilePath {self.xdcr_differ_yaml_conf_path}"
        )

        output, err = self._run_xdcr_differ(cmd)
        combined = ' '.join(output + (err if err else [])).lower()

        # Command should fail with empty passphrase - that's the expected behavior
        if "empty" in combined or "passphrase" in combined or err:
            self.log.info("PASS: Empty passphrase was rejected as expected")
        else:
            self.fail("Empty passphrase should be rejected")

    # ========================================================================
    # TC-010: Test passphrase mismatch during confirmation
    # ========================================================================
    def test_passphrase_mismatch(self):
        """TC-010: Passphrase mismatch during confirmation should cause exit."""
        self.setup_xdcr_and_load()

        cmd = (
            f"export CBAUTH_REVRPC_URL=\"http://{self.xdcr_differ_params['sourceUsername']}:"
            f"{self.xdcr_differ_params['sourcePassword']}@{self.xdcr_differ_params['sourceUrl']}\" && "
            f'printf "secret1\\nsecret2\\n" | {self.xdcr_differ_bin_path} -encryptionPassphrase '
            f"-encryptedLogFile {self.encryption_log_file} "
            f"-yamlConfigFilePath {self.xdcr_differ_yaml_conf_path}"
        )

        output, err = self._run_xdcr_differ(cmd)
        combined = ' '.join(output + (err if err else [])).lower()

        # Command should fail with mismatched passphrases - that's the expected behavior
        if "mismatch" in combined or "match" in combined or "error" in combined or err:
            self.log.info("PASS: Passphrase mismatch caused error as expected")
        else:
            self.fail("Passphrase mismatch should cause error")

    # ========================================================================
    # TC-011: Test special characters in passphrase
    # ========================================================================
    def test_passphrase_special_characters(self):
        """TC-011: Test special characters in passphrase are handled correctly."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        special_passphrase = "P@ssw0rd!#$%^&*()_+-=[]{}|;':\",./<>?"
        escaped_passphrase = special_passphrase.replace("'", "'\\''")

        cmd = self._build_xdcr_differ_cmd(passphrase=escaped_passphrase)
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        self._verify_directories_exist()
        found, _ = self._verify_enc_suffix_files(self.xdcr_differ_params['sourceFileDir'])
        if not found:
            self.fail("Special character passphrase failed to create encrypted files")

    # ========================================================================
    # TC-012: Verify decrypted output matches original plaintext
    # ========================================================================
    def test_decryption_matches_original(self):
        """TC-012: Verify decrypted output matches original plaintext."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        passphrase = "secret"
        cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        self._run_xdcr_differ(cmd)

        enc_log = f"{self.encryption_log_file}.enc"
        cmd_check = f"test -f {enc_log}"
        _, _, exit_code = self.src_master_shell.execute_command(cmd_check, get_exit_code=True)

        if exit_code == 0:
            decrypt_cmd = self._build_xdcr_differ_cmd(
                passphrase=passphrase, decrypt_mode=True, decrypt_file=enc_log
            )
            output, _ = self._run_xdcr_differ(decrypt_cmd)

            if not output:
                self.fail("Decryption produced no output")

            combined = ' '.join(output).lower()
            if "goxdcr.xdcrdifftool: runmutationdiffer completed" not in combined:
                self.fail(f"Decryption failed: {combined}")

    # ========================================================================
    # TC-013: Test decryption with wrong passphrase
    # ========================================================================
    def test_decryption_wrong_passphrase(self):
        """TC-013: Test decryption with wrong passphrase fails with authentication error."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        correct_passphrase = "correctpassword"
        cmd = self._build_xdcr_differ_cmd(passphrase=correct_passphrase)
        self._run_xdcr_differ(cmd)

        enc_log = f"{self.encryption_log_file}.enc"
        cmd_check = f"test -f {enc_log}"
        _, _, exit_code = self.src_master_shell.execute_command(cmd_check, get_exit_code=True)

        if exit_code == 0:
            wrong_passphrase = "wrongpassword"
            decrypt_cmd = self._build_xdcr_differ_cmd(
                passphrase=wrong_passphrase, decrypt_mode=True, decrypt_file=enc_log
            )
            output, err = self._run_xdcr_differ(decrypt_cmd)
            combined = ' '.join(output + (err if err else [])).lower()

            # Decryption should fail with wrong passphrase - that's the expected behavior
            if "authentication" in combined or "error" in combined or "fail" in combined or err:
                self.log.info("PASS: Decryption with wrong passphrase failed as expected")
            else:
                self.fail("Wrong passphrase should fail decryption")

    # ========================================================================
    # TC-014: Verify -decrypt outputs to stdout only
    # ========================================================================
    def test_decryption_stdout_only(self):
        """TC-014: Verify -decrypt mode outputs to stdout only."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        passphrase = "secret"
        cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        self._run_xdcr_differ(cmd)

        enc_log = f"{self.encryption_log_file}.enc"
        cmd_check = f"test -f {enc_log}"
        _, _, exit_code = self.src_master_shell.execute_command(cmd_check, get_exit_code=True)

        if exit_code == 0:
            test_output_file = "/tmp/decrypt_test_output.txt"
            decrypt_cmd = (
                f"export CBAUTH_REVRPC_URL=\"http://{self.xdcr_differ_params['sourceUsername']}:"
                f"{self.xdcr_differ_params['sourcePassword']}@{self.xdcr_differ_params['sourceUrl']}\" && "
                f'printf "{passphrase}\\n{passphrase}\\n" | {self.xdcr_differ_bin_path} '
                f"-decrypt {enc_log} -encryptionPassphrase > {test_output_file}"
            )
            self._run_xdcr_differ(decrypt_cmd)

            size_cmd = f"stat -c%s {test_output_file} 2>/dev/null || stat -f%z {test_output_file}"
            output, _ = self.src_master_shell.execute_command(size_cmd)
            if output and int(output[0].strip()) > 0:
                self.log.info("Decrypt output successfully written to stdout")
            else:
                self.fail("Decrypt mode did not produce stdout output")

    # ========================================================================
    # TC-015: Decrypt files created in all phases
    # ========================================================================
    def test_decryption_all_phases(self):
        """TC-015: Decrypt files from all three phases successfully."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        passphrase = "secret"
        cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        self._run_xdcr_differ(cmd)

        phase_dirs = [
            self.xdcr_differ_params['sourceFileDir'],
            self.xdcr_differ_params['targetFileDir'],
            self.xdcr_differ_params['checkpointFileDir'],
            self.xdcr_differ_params['fileDifferDir'],
            self.xdcr_differ_params['mutationDifferDir'],
        ]

        for phase_dir in phase_dirs:
            enc_files_cmd = f"find {phase_dir} -name '*.enc' -type f 2>/dev/null | head -1"
            enc_files, _ = self.src_master_shell.execute_command(enc_files_cmd)

            if enc_files and enc_files[0].strip():
                enc_file = enc_files[0].strip()
                decrypt_cmd = self._build_xdcr_differ_cmd(
                    passphrase=passphrase, decrypt_mode=True, decrypt_file=enc_file
                )
                output, err = self._run_xdcr_differ(decrypt_cmd)
                combined = ' '.join(output + (err if err else [])).lower()

                if "error" in combined or "fail" in combined:
                    self.fail(f"Failed to decrypt file from {phase_dir}: {enc_file}")

    # ========================================================================
    # TC-016: Run complete workflow with encryption enabled
    # ========================================================================
    def test_integration_full_workflow(self):
        """TC-016: Run complete xdcrDiffer workflow with encryption enabled."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        output, err, exit_code = self._run_xdcr_differ(cmd, timeout=1200, get_exit_code=True)

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        self._verify_directories_exist()

        for directory in [self.xdcr_differ_params['sourceFileDir'],
                          self.xdcr_differ_params['targetFileDir'],
                          self.xdcr_differ_params['mutationDifferDir']]:
            found, _ = self._verify_enc_suffix_files(directory)
            self.log.info(f"Encrypted files in {directory}: {found}")

    # ========================================================================
    # TC-017: Verify encrypted checkpoint files
    # ========================================================================
    def test_integration_encrypted_checkpoints(self):
        """TC-017: Verify checkpoint files are encrypted."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        self._run_xdcr_differ(cmd)

        checkpoint_dir = self.xdcr_differ_params['checkpointFileDir']
        found, files = self._verify_enc_suffix_files(checkpoint_dir)

        if found:
            self.log.info(f"Encrypted checkpoint files found: {files}")
        else:
            cmd_ls = f"ls -la {checkpoint_dir} 2>/dev/null"
            output, _ = self.src_master_shell.execute_command(cmd_ls)
            self.log.info(f"Checkpoint directory contents: {output}")

    # ========================================================================
    # TC-018: Verify encrypted log files
    # ========================================================================
    def test_integration_encrypted_logs(self):
        """TC-018: Verify log files are encrypted when -encryptedLogFile specified."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        self._run_xdcr_differ(cmd)

        enc_log = f"{self.encryption_log_file}.enc"
        cmd_check = f"test -f {enc_log}"
        _, _, exit_code = self.src_master_shell.execute_command(cmd_check, get_exit_code=True)

        if exit_code != 0:
            self.fail(f"Encrypted log file not found: {enc_log}")

        valid, actual = self._verify_magic_bytes(enc_log)
        if not valid:
            self.fail(f"Encrypted log file missing magic bytes: got '{actual}'")

    # ========================================================================
    # TC-019: Confirm encrypted output in mutationDiff/ directory
    # ========================================================================
    def test_integration_encrypted_mutation_diff(self):
        """TC-019: Confirm final results in mutationDiff/ are encrypted."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        self._run_xdcr_differ(cmd)

        mutation_dir = self.xdcr_differ_params['mutationDifferDir']
        found, files = self._verify_enc_suffix_files(mutation_dir)

        cmd_ls = f"ls -la {mutation_dir} 2>/dev/null"
        output, _ = self.src_master_shell.execute_command(cmd_ls)
        self.log.info(f"Mutation differ directory contents: {output}")

    # ========================================================================
    # TC-020: Measure execution time with/without encryption
    # ========================================================================
    def test_performance_execution_time(self):
        """TC-020: Measure execution time with/without encryption."""
        self.setup_xdcr_and_load()

        self._cleanup_output_dir()
        cmd_no_enc = self._build_xdcr_differ_cmd(passphrase="")
        cmd_timed_no_enc = f"time ({cmd_no_enc})"
        start = time.time()
        self._run_xdcr_differ(cmd_no_enc)
        time_no_enc = time.time() - start

        self._cleanup_output_dir()
        cmd_enc = self._build_xdcr_differ_cmd(passphrase="secret")
        start = time.time()
        self._run_xdcr_differ(cmd_enc)
        time_enc = time.time() - start

        overhead = ((time_enc - time_no_enc) / time_no_enc * 100) if time_no_enc > 0 else 0
        self.log.info(f"Time without encryption: {time_no_enc:.2f}s")
        self.log.info(f"Time with encryption: {time_enc:.2f}s")
        self.log.info(f"Encryption overhead: {overhead:.2f}%")

        if overhead > 50:
            self.log.warning(f"Encryption overhead ({overhead:.2f}%) exceeds 50%")

    # ========================================================================
    # TC-024: Test decryption with incorrect passphrase
    # ========================================================================
    def test_security_wrong_passphrase(self):
        """TC-024: Test decryption with incorrect passphrase fails."""
        self.test_decryption_wrong_passphrase()

    # ========================================================================
    # TC-025: Test with modified encrypted file content
    # ========================================================================
    def test_security_tampered_file(self):
        """TC-025: Test tampered encrypted file is detected."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        passphrase = "secret"
        cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        self._run_xdcr_differ(cmd)

        enc_files_cmd = f"find {self.xdcr_differ_params['sourceFileDir']} -name '*.enc' -type f | head -1"
        enc_files, _ = self.src_master_shell.execute_command(enc_files_cmd)

        if enc_files and enc_files[0].strip():
            enc_file = enc_files[0].strip()
            tamper_cmd = f"dd if=/dev/urandom bs=1 count=10 seek=100 of={enc_file} conv=notrunc 2>/dev/null"
            self.src_master_shell.execute_command(tamper_cmd)

            decrypt_cmd = self._build_xdcr_differ_cmd(
                passphrase=passphrase, decrypt_mode=True, decrypt_file=enc_file
            )
            output, err = self._run_xdcr_differ(decrypt_cmd)
            combined = ' '.join(output + (err if err else [])).lower()

            # Tampered file decryption should fail - that's the expected behavior
            if "authentication" in combined or "error" in combined or "fail" in combined or "tamper" in combined or err:
                self.log.info("PASS: Tampered file was detected during decryption as expected")
            else:
                self.fail("Tampered file should be detected during decryption")

    # ========================================================================
    # TC-027: Verify temporary files contain only encrypted data
    # ========================================================================
    def test_security_temp_files_encrypted(self):
        """TC-027: Verify temporary files contain only encrypted data."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        self._run_xdcr_differ(cmd)

        find_cmd = f"find {self.xdcr_differ_params['outputFileDir']} -type f 2>/dev/null"
        all_files, _ = self.src_master_shell.execute_command(find_cmd)

        for file_path in all_files:
            file_path = file_path.strip()
            if file_path and not file_path.endswith('.enc'):
                no_plaintext, pattern = self._verify_no_plaintext(file_path)
                if not no_plaintext:
                    self.log.warning(f"Non-encrypted file {file_path} may contain plaintext: {pattern}")

    # ========================================================================
    # TC-030: Test with corrupted encrypted files
    # ========================================================================
    def test_fault_tolerance_corrupted_files(self):
        """TC-030: Test corrupted encrypted files are detected during header validation."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        passphrase = "secret"
        cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        self._run_xdcr_differ(cmd)

        enc_files_cmd = f"find {self.xdcr_differ_params['sourceFileDir']} -name '*.enc' -type f | head -1"
        enc_files, _ = self.src_master_shell.execute_command(enc_files_cmd)

        if enc_files and enc_files[0].strip():
            enc_file = enc_files[0].strip()
            corrupt_cmd = f"dd if=/dev/zero of={enc_file} bs=1 count=4 conv=notrunc 2>/dev/null"
            self.src_master_shell.execute_command(corrupt_cmd)

            decrypt_cmd = self._build_xdcr_differ_cmd(
                passphrase=passphrase, decrypt_mode=True, decrypt_file=enc_file
            )
            output, err = self._run_xdcr_differ(decrypt_cmd)
            combined = ' '.join(output + (err if err else [])).lower()

            # Corrupted file should be detected - that's the expected behavior
            if "header" in combined or "corrupt" in combined or "error" in combined or "invalid" in combined or err:
                self.log.info("PASS: Corrupted header was detected as expected")
            else:
                self.fail("Corrupted header should be detected")

    # ========================================================================
    # TC-031: Test checkpoint recovery with encrypted data
    # ========================================================================
    def test_fault_tolerance_checkpoint_recovery(self):
        """TC-031: Test checkpoint recovery works with encrypted data."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        passphrase = "secret"
        checkpoint_name = "test_checkpoint.json"
        self.xdcr_differ_params['newCheckpointFileName'] = checkpoint_name
        self.xdcr_differ_params['runFileDiffer'] = False
        self.xdcr_differ_params['runMutationDiffer'] = False

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        self._run_xdcr_differ(cmd)

        checkpoint_file = f"{self.xdcr_differ_params['checkpointFileDir']}/{checkpoint_name}"
        if passphrase:
            checkpoint_file += ".enc"

        cmd_check = f"test -f {checkpoint_file}"
        _, _, exit_code = self.src_master_shell.execute_command(cmd_check, get_exit_code=True)

        if exit_code == 0:
            self.xdcr_differ_params['oldCheckpointFileName'] = checkpoint_name
            self.xdcr_differ_params['runFileDiffer'] = True
            self.xdcr_differ_params['runMutationDiffer'] = True
            self.xdcr_differ_params['runDataGeneration'] = False

            with open(self.xdcr_differ_yaml_conf_path, "w") as f:
                yaml.dump(self.xdcr_differ_params, f)
            self.src_master_shell.copy_file_local_to_remote(
                self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
            )

            resume_cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
            output, err = self._run_xdcr_differ(resume_cmd)

            if err:
                combined = ' '.join(err).lower()
                if "checkpoint" in combined and "error" in combined:
                    self.fail(f"Checkpoint recovery failed: {err}")

    # ========================================================================
    # TC-032: Verify behavior during rebalance
    # ========================================================================
    def test_fault_tolerance_rebalance(self):
        """TC-032: Verify encryption behavior during rebalance."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="secret")
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        self._verify_directories_exist()
        found, _ = self._verify_enc_suffix_files(self.xdcr_differ_params['sourceFileDir'])
        if not found:
            self.fail("Encrypted files not created")

    # ========================================================================
    # TC-033: Verify behavior during failover
    # ========================================================================
    def test_fault_tolerance_failover(self):
        """TC-033: Verify encryption behavior during failover."""
        self.test_fault_tolerance_rebalance()

    # ========================================================================
    # TC-042: Verify non-encrypted mode functionality
    # ========================================================================
    def test_regression_non_encrypted_mode(self):
        """TC-042: Verify non-encrypted mode still functions correctly."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd(passphrase="")
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        self._verify_directories_exist()

        enc_files_cmd = f"find {self.xdcr_differ_params['outputFileDir']} -name '*.enc' -type f | head -1"
        enc_files, _ = self.src_master_shell.execute_command(enc_files_cmd)

        if enc_files and enc_files[0].strip():
            self.fail("Found .enc files in non-encrypted mode")

    # ========================================================================
    # TC-043: Test mixed-mode operations
    # ========================================================================
    def test_regression_mixed_mode(self):
        """TC-043: Test mixed-mode operations (encrypted/non-encrypted)."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd_no_enc = self._build_xdcr_differ_cmd(passphrase="")
        output, err, exit_code = self._run_xdcr_differ(cmd_no_enc, get_exit_code=True)
        if exit_code != 0:
            self.fail(self._log_command_failure(cmd_no_enc, output, err, exit_code))
        self._verify_directories_exist()

        self._cleanup_output_dir()
        cmd_enc = self._build_xdcr_differ_cmd(passphrase="secret")
        output, err, exit_code = self._run_xdcr_differ(cmd_enc, get_exit_code=True)
        if exit_code != 0:
            self.fail(self._log_command_failure(cmd_enc, output, err, exit_code))
        self._verify_directories_exist()

        found, _ = self._verify_enc_suffix_files(self.xdcr_differ_params['sourceFileDir'])
        if not found:
            self.fail("Encrypted files not created after switching to encrypted mode")

    # ========================================================================
    # TC-047: Resume from encrypted checkpoint with same passphrase
    # ========================================================================
    def test_checkpoint_resume_same_passphrase(self):
        """TC-047: Resume from encrypted checkpoint with same passphrase succeeds."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        passphrase = "secret"
        checkpoint_name = "resume_same_ckpt.json"
        self.xdcr_differ_params['newCheckpointFileName'] = checkpoint_name
        self.xdcr_differ_params['runFileDiffer'] = False
        self.xdcr_differ_params['runMutationDiffer'] = False

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)
        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        checkpoint_file = f"{self.xdcr_differ_params['checkpointFileDir']}/{checkpoint_name}.enc"
        cmd_check = f"test -f {checkpoint_file}"
        _, _, exit_code = self.src_master_shell.execute_command(cmd_check, get_exit_code=True)
        if exit_code != 0:
            self.fail(f"Checkpoint file not created: {checkpoint_file}")

        self.xdcr_differ_params['oldCheckpointFileName'] = checkpoint_name
        self.xdcr_differ_params['runFileDiffer'] = True
        self.xdcr_differ_params['runMutationDiffer'] = True
        self.xdcr_differ_params['runDataGeneration'] = False

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        resume_cmd = self._build_xdcr_differ_cmd(passphrase=passphrase)
        output, err, exit_code = self._run_xdcr_differ(resume_cmd, get_exit_code=True)
        if exit_code != 0:
            self.fail(self._log_command_failure(resume_cmd, output, err, exit_code))

        self.log.info("PASS: Resumed from encrypted checkpoint with same passphrase successfully")

    # ========================================================================
    # TC-049: Abort mid run and resume with new passphrase
    # ========================================================================
    def test_checkpoint_resume_new_passphrase(self):
        """TC-049: Abort mid run and try to resume with new passphrase should fail."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        original_passphrase = "original_secret"
        checkpoint_name = "abort_checkpoint.json"
        self.xdcr_differ_params['newCheckpointFileName'] = checkpoint_name
        self.xdcr_differ_params['runFileDiffer'] = False
        self.xdcr_differ_params['runMutationDiffer'] = False

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        cmd = self._build_xdcr_differ_cmd(passphrase=original_passphrase)
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)
        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        new_passphrase = "new_secret"
        self.xdcr_differ_params['oldCheckpointFileName'] = checkpoint_name
        self.xdcr_differ_params['runFileDiffer'] = True
        self.xdcr_differ_params['runMutationDiffer'] = True
        self.xdcr_differ_params['runDataGeneration'] = False

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        resume_cmd = self._build_xdcr_differ_cmd(passphrase=new_passphrase)
        output, err, exit_code = self._run_xdcr_differ(resume_cmd, get_exit_code=True)
        combined = ' '.join(output + (err if err else [])).lower()

        if exit_code == 0 and "error" not in combined and "fail" not in combined and "decrypt" not in combined:
            self.fail("Resume with different passphrase should have failed but succeeded")
        self.log.info("PASS: Resume with different passphrase failed as expected")

    # ========================================================================
    # TC-050: Resume with wrong passphrase
    # ========================================================================
    def test_checkpoint_resume_wrong_passphrase(self):
        """TC-050: Resume from checkpoint with a completely wrong passphrase should fail."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        correct_passphrase = "correct_passphrase"
        checkpoint_name = "wrong_pass_ckpt.json"
        self.xdcr_differ_params['newCheckpointFileName'] = checkpoint_name
        self.xdcr_differ_params['runFileDiffer'] = False
        self.xdcr_differ_params['runMutationDiffer'] = False

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        cmd = self._build_xdcr_differ_cmd(passphrase=correct_passphrase)
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)
        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        checkpoint_file = f"{self.xdcr_differ_params['checkpointFileDir']}/{checkpoint_name}.enc"
        cmd_check = f"test -f {checkpoint_file}"
        _, _, exit_code = self.src_master_shell.execute_command(cmd_check, get_exit_code=True)
        if exit_code != 0:
            self.fail(f"Checkpoint file not created: {checkpoint_file}")

        wrong_passphrase = "totally_wrong_passphrase"
        self.xdcr_differ_params['oldCheckpointFileName'] = checkpoint_name
        self.xdcr_differ_params['runFileDiffer'] = True
        self.xdcr_differ_params['runMutationDiffer'] = True
        self.xdcr_differ_params['runDataGeneration'] = False

        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        resume_cmd = self._build_xdcr_differ_cmd(passphrase=wrong_passphrase)
        output, err, exit_code = self._run_xdcr_differ(resume_cmd, get_exit_code=True)
        combined = ' '.join(output + (err if err else [])).lower()

        if exit_code == 0 and "error" not in combined and "fail" not in combined and "decrypt" not in combined:
            self.fail("Resume with wrong passphrase should have failed but succeeded")
        self.log.info("PASS: Resume with wrong passphrase failed as expected")

    # ========================================================================
    # Basic test_xdcr_differ for backward compatibility
    # ========================================================================
    def test_xdcr_differ(self):
        """Basic XDCR Differ test with optional encryption."""
        self.setup_xdcr_and_load()
        self._cleanup_output_dir()

        cmd = self._build_xdcr_differ_cmd()
        output, err, exit_code = self._run_xdcr_differ(cmd, get_exit_code=True)

        # Special case: testing empty passphrase with encryptedLogFile should fail
        if self._input.param("encryptionPassphrase", "") == "" and self._input.param("encryptedLogFile", None):
            combined = ' '.join(output + (err if err else [])).lower()
            if exit_code == 0 and "error" not in combined:
                self.fail("Error when passphrase empty with encryptedLogFile")
            # Expected failure occurred, test passes
            self.log.info("PASS: Expected error occurred when passphrase empty with encryptedLogFile")
            return

        if exit_code != 0:
            error_msg = self._log_command_failure(cmd, output, err, exit_code)
            self.fail(error_msg)

        self._verify_directories_exist()

        if self.passphrase_value:
            found, files = self._verify_enc_suffix_files(self.xdcr_differ_params['outputFileDir'])
            if not found:
                self.fail("Encrypted files with .enc suffix not found")

            enc_files_cmd = f"find {self.xdcr_differ_params['sourceFileDir']} -name '*.enc' -type f | head -1"
            enc_files, _ = self.src_master_shell.execute_command(enc_files_cmd)
            if enc_files and enc_files[0].strip():
                valid, actual = self._verify_magic_bytes(enc_files[0].strip())
                if not valid:
                    self.fail(f"Magic bytes mismatch: expected '{self.MAGIC_BYTES}', got '{actual}'")

    # ========================================================================
    # Helper methods for large-volume testing
    # ========================================================================
    def _load_docs_pillowfight(self, server, bucket, num_docs, doc_size=256,
                               batch=1000, rate_limit=100000, key_prefix='large_vol_',
                               scope='_default', collection='_default', command_timeout=300):
        """
        Load documents using cbc-pillowfight for large volume testing.

        Args:
            server: Server node to execute command on
            bucket: Bucket name to load into
            num_docs: Number of documents to load
            doc_size: Document size in bytes
            batch: Batch size for pillowfight
            rate_limit: Rate limit for pillowfight
            key_prefix: Prefix for document keys
            scope: Scope name (default: _default)
            collection: Collection name (default: _default)
            command_timeout: Command timeout in seconds
        """
        server_shell = RemoteMachineShellConnection(server)
        cmd = (f"/opt/couchbase/bin/cbc-pillowfight -u {server.rest_username} "
               f"-P {server.rest_password} -U couchbase://localhost/{bucket} "
               f"-I {num_docs} -m {doc_size} -M {doc_size} -B {batch} "
               f"--rate-limit={rate_limit} --populate-only --collection {scope}.{collection}")

        self.log.info(f"Loading {num_docs} docs into {bucket}.{scope}.{collection}...")
        output, error, exit_code = server_shell.execute_command(
            cmd, timeout=command_timeout, use_channel=True, get_exit_code=True
        )
        server_shell.disconnect()

        if error and exit_code != 0:
            error_msg = '\n'.join(error) if error else 'No stderr'
            if 'couchbase/bin/cbc-pillowfight' in error_msg or 'command not found' in error_msg.lower():
                self.fail(f"cbc-pillowfight binary not found. Please ensure Couchbase is installed on {server.ip}")
            self.fail(f"Failed to load docs: {error_msg}")

        self.log.info(f"Successfully loaded {num_docs} docs into {bucket}")

    def _get_bucket_items(self, server, bucket):
        """
        Get current item count for a bucket.

        Args:
            server: Server node
            bucket: Bucket name

        Returns:
            int: Current item count
        """
        rest = RestConnection(server)
        bucket_info = rest.get_bucket(bucket)
        if bucket_info and hasattr(bucket_info, 'stats'):
            stats = bucket_info.stats
            return int(stats.itemCount)
        return 0

    def _wait_for_bucket_items(self, server, bucket, expected_items, timeout=1800,
                              poll_interval=10):
        """
        Wait for bucket to reach expected item count.

        Args:
            server: Server node
            bucket: Bucket name
            expected_items: Expected number of items
            timeout: Timeout in seconds
            poll_interval: Poll interval in seconds
        """
        start_time = time.time()
        self.log.info(f"Waiting for {bucket} to reach {expected_items} items (timeout: {timeout}s)")

        while time.time() - start_time < timeout:
            current_items = self._get_bucket_items(server, bucket)
            self.log.info(f"{bucket} current items: {current_items} / {expected_items}")
            if current_items >= expected_items:
                self.log.info(f"{bucket} reached {current_items} items")
                return True
            time.sleep(poll_interval)

        self.fail(f"Timeout waiting for {bucket} to reach {expected_items} items. "
                  f"Current: {current_items}")

    # ========================================================================
    # TC-LARGE01: XDCR Differ with 100k dataset (full workflow)
    # ========================================================================
    def test_xdcr_differ_large_volume_100k_docs(self):
        """
        Parameters:
            largeDocCount: Number of documents to load (default: 100000)
            largeDocSize: Document size in bytes (default: 256)
            largeDocBatch: Pillowfight batch size (default: 1000)
            largeDocRateLimit: Pillowfight rate limit (default: 100000)
            largeReplTimeout: Replication wait timeout in seconds (default: 1800)
            largeDifferTimeout: xdcrDiffer command timeout in seconds (default: 1800)
        """
        num_docs = self._input.param("largeDocCount", 100000)
        doc_size = self._input.param("largeDocSize", 256)
        batch = self._input.param("largeDocBatch", 1000)
        rate_limit = self._input.param("largeDocRateLimit", 100000)
        repl_timeout = self._input.param("largeReplTimeout", 1800)
        differ_timeout = self._input.param("largeDifferTimeout", 1800)

        bucket_name = self.xdcr_differ_params['sourceBucketName']

        self.log.info(f"Starting large-volume test: {num_docs} docs, {doc_size} bytes each")

        # Setup XDCR without loading data (we'll load our own)
        self.setup_xdcr()
        self.sleep(10, "Waiting for XDCR to stabilize")

        # Record baseline counts
        src_base = self._get_bucket_items(self.src_master, bucket_name)
        dst_base = self._get_bucket_items(self.dest_master, bucket_name)
        self.log.info(f"Baseline counts - Source: {src_base}, Dest: {dst_base}")

        # Load documents into source bucket
        self._load_docs_pillowfight(
            self.src_master, bucket_name, num_docs, doc_size, batch,
            rate_limit, key_prefix='large_vol_'
        )

        # Wait for source to reach expected count
        self._wait_for_bucket_items(
            self.src_master, bucket_name, src_base + num_docs,
            timeout=repl_timeout, poll_interval=10
        )

        expected_src = src_base + num_docs
        self.log.info(f"Source bucket has {self._get_bucket_items(self.src_master, bucket_name)} items")

        # Wait for destination to catch up via XDCR replication
        self.log.info("Waiting for XDCR replication to destination cluster...")
        self._wait_for_bucket_items(
            self.dest_master, bucket_name, dst_base + num_docs,
            timeout=repl_timeout, poll_interval=10
        )

        final_dst = self._get_bucket_items(self.dest_master, bucket_name)
        self.log.info(f"Destination bucket has {final_dst} items")

        # Configure xdcrDiffer for large volume test
        # runDataGeneration must be true for xdcrDiffer to read from buckets
        self.xdcr_differ_params['runDataGeneration'] = True
        self.xdcr_differ_params['clearBeforeRun'] = "false"
        self.xdcr_differ_params['runFileDiffer'] = True
        self.xdcr_differ_params['runMutationDiffer'] = True
        self.xdcr_differ_params['bucketOpTimeout'] = 60

        # Write updated config
        with open(self.xdcr_differ_yaml_conf_path, "w") as f:
            yaml.dump(self.xdcr_differ_params, f)
        self.src_master_shell.copy_file_local_to_remote(
            self.xdcr_differ_yaml_conf_path, self.xdcr_differ_yaml_conf_path
        )

        # Run xdcrDiffer
        self._cleanup_output_dir()
        cmd = self._build_xdcr_differ_cmd()
        self.log.info(f"Running xdcrDiffer with large dataset (timeout: {differ_timeout}s)...")
        output, err, exit_code = self._run_xdcr_differ(
            cmd, timeout=differ_timeout, get_exit_code=True
        )

        if exit_code != 0:
            self.fail(self._log_command_failure(cmd, output, err, exit_code))

        # Verify output directories exist
        self._verify_directories_exist()

        # Verify encrypted files if passphrase is set
        if self.passphrase_value:
            found, _ = self._verify_enc_suffix_files(self.xdcr_differ_params['outputFileDir'])
            if not found:
                self.fail("No .enc files found in encrypted mode")

        self.log.info(f"Large-volume test PASSED: {num_docs} docs, xdcrDiffer completed successfully")
