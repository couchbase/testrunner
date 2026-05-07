import json
import logging
import time
import re as _re

from lib.couchbase_helper.encryption_at_rest_helper import EncryptionAtRestHelper
from lib.membase.helper.encryption_at_rest_helper import EncryptionUtil
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.eventing.eventing_base import EventingBaseTest

log = logging.getLogger()

EVENTING_LOG_ROOT = "/opt/couchbase/var/lib/couchbase/data/@eventing"

class EventingEncryptionAtRest(EventingBaseTest):
    def setUp(self):
        self.created_secret_ids = []
        self.log_encryption_enabled = False
        super(EventingEncryptionAtRest, self).setUp()
        handler_code = self.input.param("handler_code", "logger")
        if handler_code == "logger":
            self.handler_code = "handler_code/logger.js"
        elif handler_code == "heavy_logger":
            self.handler_code = "handler_code/heavy_logger.js"

        self.app_log_max_size = self.input.param("app_log_max_size", 3768300)
        self.rotation_wait_timeout = self.input.param("rotation_wait_timeout", 180)
        self.logsize = self.input.param("size", None)
        self.aggregate = self.input.param("aggregate", False)

        self.ear_helper = EncryptionAtRestHelper(self.log)
        self.encryption_util.bypass_encryption_restrictions(server=self.master)

    def tearDown(self):
        try:
            if self.log_encryption_enabled:
                self._set_log_encryption_method("disabled")
        except Exception as e:
            self.log.warning("Failed to disable log encryption in tearDown: {}".format(e))
        for secret_id in self.created_secret_ids:
            try:
                self.rest.delete_secret(secret_id)
            except Exception as e:
                self.log.warning("Failed to delete secret {}: {}".format(secret_id, e))
        super(EventingEncryptionAtRest, self).tearDown()


    # -------------------------- Helper Functions --------------------------

    def _create_and_deploy_function(self):
        self.load_data_to_collection(self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['settings']['app_log_max_size'] = self.app_log_max_size
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", self.num_docs)
        return body

    def _trigger_more_mutations(self):
        self.load_data_to_collection(self.num_docs, "src_bucket._default._default")
        self.verify_doc_count_collections("dst_bucket._default._default", self.num_docs)

    def _eventing_log_dir(self, bucket_name, scope_name):
        bucket_uuid = "b_" + self.rest.fetch_bucket_uuid(bucket_name)
        manifest = self.rest.get_bucket_manifest(bucket_name)
        scope_id = None
        for scope in manifest["scopes"]:
            if scope["name"] == scope_name:
                scope_id = "s_" + scope["uid"]
                break
        if scope_id is None:
            raise Exception("Scope '{}' not found in bucket '{}' manifest".format(
                scope_name, bucket_name))
        return "{}/{}/{}".format(EVENTING_LOG_ROOT, bucket_uuid, scope_id)

    def _get_log_dir(self):
        if self.global_function_scope:
            return EVENTING_LOG_ROOT
        return self._eventing_log_dir(self.src_bucket_name, "_default")

    def _list_log_files(self, node, log_dir):
        shell = RemoteMachineShellConnection(node)
        cmd = "ls -1t {}/*.log* 2>/dev/null".format(log_dir)
        output, _ = shell.execute_command(cmd)
        shell.disconnect()
        return [path.strip() for path in output if path.strip()]

    def _file_is_encrypted(self, node, file_path):
        is_encrypted, details = self.ear_helper.verify_file_encryption_magic_bytes(
            node, file_path)
        self.log.info("Encryption check on {}: encrypted={}, details={}".format(
            file_path, is_encrypted, details))
        return is_encrypted

    def _create_log_encryption_secret(self):
        params = EncryptionUtil.create_secret_params(
            name=EncryptionUtil.generate_random_name("EventingLogSecret"),
            usage=["log-encryption"],
        )
        status, response = self.rest.create_secret(params)
        if not status:
            raise Exception(
                "Failed to create log-encryption secret. Response: {}".format(response))
        response_dict = json.loads(response)
        secret_id = response_dict.get("id")
        if secret_id is None:
            raise Exception(
                "Log-encryption secret created but no id returned: {}".format(response))
        self.log.info("Created log-encryption secret with id: {}".format(secret_id))
        self.created_secret_ids.append(secret_id)
        return secret_id

    def _set_log_encryption_method(self, method, key_id=None):
        params = {"log.encryptionMethod": method}
        if method == "encryptionKey":
            if key_id is None:
                raise Exception("encryptionKey method requires a key_id")
            params["log.encryptionKeyId"] = key_id
        status, response = self.rest.configure_encryption_at_rest(params)
        if not status:
            raise Exception("Failed to set log encryption to {}: {}".format(
                method, response))
        self.log_encryption_enabled = method != "disabled"
        self.log.info("Configured log encryption: method={} key_id={}".format(
            method, key_id))

    def _wait_for_rotation(self, node, log_dir, baseline_count, timeout):
        deadline = time.time() + timeout
        files = []
        while time.time() < deadline:
            files = self._list_log_files(node, log_dir)
            if len(files) > baseline_count:
                return files
            self.sleep(5, "Waiting for eventing log rotation in {}".format(log_dir))
        raise Exception(
            "Log rotation did not occur in {} within {}s "
            "(baseline {} files, current {} files)".format(
                log_dir, timeout, baseline_count, len(files)))

    def _find_state_change_pair(self, node, files_newest_first,
                                expected_old_encrypted, expected_new_encrypted):
        """
        Eventing Log Rotation convention:
        base.log is always the active (newest) file
        base.log.1 is the previous, base.log.2 older, etc.
        Chronological order = descending suffix (no suffix treated as 0)
        Returns (older_path, newer_path) or None
        """

        def _suffix(path):
            m = _re.search(r'\.(\d+)$', path)
            return int(m.group(1)) if m else 0

        chronological = sorted(files_newest_first, key=_suffix, reverse=True)
        states = [(path, self._file_is_encrypted(node, path)) for path in chronological]
        for (older, older_state), (newer, newer_state) in zip(states, states[1:]):
            if older_state == expected_old_encrypted and newer_state == expected_new_encrypted:
                return older, newer
        return None

    def _assert_file_is_readable_plaintext(self, node, file_path):
        header_read, decoded, printable = self.ear_helper.get_file_header_text(
            node, file_path, bytes_to_read=512)
        self.assertTrue(header_read, "Could not read header for {}".format(file_path))
        self.assertNotIn(
            EncryptionAtRestHelper.ENCRYPTION_MAGIC_BYTES, decoded,
            "File {} contains encryption magic bytes; expected plaintext".format(
                file_path))
        printable_chars = sum(1 for c in printable if c != '.')
        ratio = printable_chars / max(len(printable), 1)
        self.assertGreater(
            ratio, 0.5,
            "File {} header does not look like plaintext (printable ratio {:.2f})".format(
                file_path, ratio))

    def _assert_app_log_non_empty(self, applogs):
        if isinstance(applogs, Exception):
            raise applogs
        content = applogs.decode('utf-8', errors='replace') if isinstance(applogs, bytes) else applogs
        self.assertGreater(
            len(content.strip()), 0,
            "/getAppLog returned an empty response")
        self.log.info("/getAppLog response length: {} bytes".format(len(content)))


    # ---------------------- Encryption at Rest Tests ----------------------

    def test_log_encryption_sanity(self):
        """
        Sanity test:
        - Deploy a function, make mutations
        - Verify the latest log file does NOT carry the encryption signature
        - Create a log-encryption key and enable log encryption
        - Make more mutations
        - Verify the latest log file now DOES carry the signature
        """
        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for app-log writes to settle before snapshot")

        files_before = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(
            len(files_before), 0, "No eventing log files found in {} before state change".format(log_dir))

        latest_before = files_before[0]
        self.assertFalse(
            self._file_is_encrypted(eventing_node, latest_before),
            "Latest log {} is unexpectedly encrypted before enabling encryption".format(
                latest_before))

        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)

        self._trigger_more_mutations()
        files_after = self._wait_for_rotation(
            eventing_node, log_dir, baseline_count=len(files_before),
            timeout=self.rotation_wait_timeout)

        latest_after = files_after[0]
        self.assertTrue(
            self._file_is_encrypted(eventing_node, latest_after),
            "Latest log {} is not encrypted after enabling log encryption".format(
                latest_after))
        self.undeploy_and_delete_function(body)


    # --------------- Encryption at Rest State Change Tests ----------------

    def test_state_change_disabled_to_enabled(self):
        """
        State-change test (disabled -> enabled):
        - Verify rotation produces two consecutive files with opposite
          encryption state (older unencrypted, newer encrypted)
        - Verify all unencrypted files written before the state-change
          remain readable plaintext
        - Verify the new latest file is encrypted
        """
        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for app-log writes to settle before snapshot")

        files_before = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(
            len(files_before), 0,
            "No eventing log files found in {} before state change".format(log_dir))
        for path in files_before:
            self.assertFalse(
                self._file_is_encrypted(eventing_node, path),
                "Pre-state file {} unexpectedly encrypted".format(path))

        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        self._trigger_more_mutations()

        files_after = self._wait_for_rotation(
            eventing_node, log_dir, baseline_count=len(files_before),
            timeout=self.rotation_wait_timeout)
        latest_after = files_after[0]
        self.assertTrue(
            self._file_is_encrypted(eventing_node, latest_after),
            "Latest log {} is not encrypted after enabling log encryption".format(
                latest_after))

        rotation_pair = self._find_state_change_pair(
            eventing_node, files_after,
            expected_old_encrypted=False, expected_new_encrypted=True)
        self.assertIsNotNone(
            rotation_pair,
            "Did not find a consecutive (unencrypted -> encrypted) rotation "
            "pair after enabling log encryption. Files (newest first): {}".format(
                files_after))
        self.log.info("State-change pair (unencrypted -> encrypted): {}".format(
            rotation_pair))

        pre_state_files = [f for f in files_after if _re.search(r'\.\d+$', f)]
        for path in pre_state_files:
            self.assertFalse(
                self._file_is_encrypted(eventing_node, path),
                "Pre-state file {} became encrypted after state change".format(path))
            self._assert_file_is_readable_plaintext(eventing_node, path)
        self.undeploy_and_delete_function(body)

    def test_state_change_enabled_to_disabled(self):
        """
        Functional state-change test (enabled -> disabled):
        - Encryption is enabled before the function is deployed so the
          first log files written are encrypted
        - Disable log encryption
        - Make more mutations
        - Verify rotation produces two consecutive files with opposite state
          (older encrypted, newer unencrypted)
        - Verify all pre-state files remain encrypted
        - Verify the new latest file is unencrypted
        """
        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)

        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for app-log writes to settle before snapshot")

        files_before = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(
            len(files_before), 0,
            "No eventing log files found in {} before state change".format(log_dir))
        for path in files_before:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-state file {} unexpectedly unencrypted".format(path))

        self._set_log_encryption_method("disabled")
        self._trigger_more_mutations()

        files_after = self._wait_for_rotation(
            eventing_node, log_dir, baseline_count=len(files_before),
            timeout=self.rotation_wait_timeout)
        latest_after = files_after[0]
        self.assertFalse(
            self._file_is_encrypted(eventing_node, latest_after),
            "Latest log {} is encrypted after disabling log encryption".format(
                latest_after))

        rotation_pair = self._find_state_change_pair(
            eventing_node, files_after,
            expected_old_encrypted=True, expected_new_encrypted=False)
        self.assertIsNotNone(
            rotation_pair,
            "Did not find a consecutive (encrypted -> unencrypted) rotation "
            "pair after disabling log encryption. Files (newest first): {}".format(
                files_after))
        self.log.info("State-change pair (encrypted -> unencrypted): {}".format(
            rotation_pair))

        pre_state_files = [f for f in files_after if _re.search(r'\.\d+$', f)]
        for path in pre_state_files:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-state file {} became unencrypted after state change".format(path))
        self.undeploy_and_delete_function(body)


    # --------------------- /getAppLog Endpoint Tests ----------------------

    def test_get_app_log_with_encryption(self):
        """
        /getAppLog endpoint test when Log Encryption is enabled
        Test for single-node, multi-node
        Test with 'size' and 'aggregate' params
        """
        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        body = self._create_and_deploy_function()
        applogs = self.get_app_logs(self.function_name, size=self.logsize, aggregate=self.aggregate)
        self._assert_app_log_non_empty(applogs)
        self.undeploy_and_delete_function(body)

    def test_get_app_log_state_change(self):
        """
        /getAppLog during a state change where a mixture of encrypted
        and unencrypted log files exists on disk:
        - Deploy a function and let plaintext logs accumulate
        - Enable encryption and trigger rotation so the active log is
          now encrypted while older rotated files remain plaintext
        - Call /getAppLog with a size spanning both file types
        - Test for single-node, multi-node
        - Test with 'size' and 'aggregate' params
        """
        body = self._create_and_deploy_function()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        log_dir = self._get_log_dir()
        self.sleep(15, "Wait for initial app-log writes to settle")

        files_before = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(
            len(files_before), 0, "No eventing log files found before state change in {}".format(log_dir))

        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        self._trigger_more_mutations()
        self._wait_for_rotation(eventing_node, log_dir, baseline_count=len(files_before),
                                timeout=self.rotation_wait_timeout)

        applogs = self.get_app_logs(self.function_name, size=self.logsize, aggregate=self.aggregate)
        self._assert_app_log_non_empty(applogs)
        self.undeploy_and_delete_function(body)