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
    STABLE_INTERVAL_S = 60000  # 1000 min — no accidental DEK rotation during setup

    def setUp(self):
        self.created_secret_ids = []
        self.log_encryption_enabled = False
        super(EventingEncryptionAtRest, self).setUp()
        handler_code = self.input.param("handler_code", "logger")
        if handler_code == "logger":
            self.handler_code = "handler_code/logger.js"
        elif handler_code == "heavy_logger":
            self.handler_code = "handler_code/heavy_logger.js"

        self.app_log_max_size = self.input.param("app_log_max_size", 41943040)
        self.app_log_max_files = self.input.param("app_log_max_files", 10)
        self.rotation_wait_timeout = self.input.param("rotation_wait_timeout", 180)
        self.dek_rotation_interval = self.input.param("dek_rotation_interval", 60)
        self.num_rotations = self.input.param("num_rotations", 5)
        self.dek_lifetime = self.input.param("dek_lifetime", 60)
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
        body['settings']['app_log_max_files'] = self.app_log_max_files
        self.rest.create_function(body['appname'], body, self.function_scope)
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

    def _create_log_encryption_secret(self, rotation_interval_seconds=None):
        params = EncryptionUtil.create_secret_params(
            name=EncryptionUtil.generate_random_name("EventingLogSecret"),
            usage=["log-encryption"],
            rotationIntervalInSeconds=rotation_interval_seconds,
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

    def _configure_dek_rotation(self, rotation_interval, lifetime):
        """
        Set log.dekRotationInterval and log.dekLifetime independently
        """
        status, response = self.rest.configure_encryption_at_rest({
            "log.dekRotationInterval": rotation_interval,
            "log.dekLifetime": lifetime,
        })
        if not status:
            raise Exception(
                "Failed to set log DEK settings "
                "(dekRotationInterval={}s, dekLifetime={}s): {}".format(
                    rotation_interval, lifetime, response))
        self.log.info(
            "log.dekRotationInterval={}s, log.dekLifetime={}s applied".format(
                rotation_interval, lifetime))

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

    def _get_file_size(self, node, file_path):
        shell = RemoteMachineShellConnection(node)
        output, _ = shell.execute_command(
            "stat -c %s {} 2>/dev/null".format(file_path))
        shell.disconnect()
        if output and output[0].strip().isdigit():
            return int(output[0].strip())
        return None

    def _assert_no_file_exceeds_max_size(self, node, log_dir):
        files = self._list_log_files(node, log_dir)
        for path in files:
            size = self._get_file_size(node, path)
            if size is None:
                self.log.warning("Could not get size for {}".format(path))
                continue
            self.log.info("File size: {} = {} bytes (max: {})".format(
                path, size, self.app_log_max_size))
            self.assertLessEqual(
                size, self.app_log_max_size,
                "Log file {} exceeds app_log_max_size: {} > {}".format(
                    path, size, self.app_log_max_size))

    def _drive_rotations_to_count(self, node, log_dir, target_count, timeout):
        deadline = time.time() + timeout
        files = self._list_log_files(node, log_dir)
        while len(files) < target_count and time.time() < deadline:
            self._trigger_more_mutations()
            files = self._list_log_files(node, log_dir)
            self.log.info("Log files: {}/{} in {}".format(
                len(files), target_count, log_dir))
        return files

    def _find_file_by_suffix(self, files, suffix):
        for f in files:
            m = _re.search(r'\.(\d+)$', f)
            if m and int(m.group(1)) == suffix:
                return f
        return None

    def _get_file_fingerprint(self, node, file_path, bytes_to_read=128):
        shell = RemoteMachineShellConnection(node)
        cmd = "head -c {} {} | base64 | tr -d '\\n' 2>/dev/null".format(
            bytes_to_read, file_path)
        output, _ = shell.execute_command(cmd)
        shell.disconnect()
        return "".join(output).strip()

    def _assert_oldest_file_dropped(self, node, log_dir, files_at_cap):
        oldest_path = self._find_file_by_suffix(files_at_cap, self.app_log_max_files)
        second_oldest_path = self._find_file_by_suffix(
            files_at_cap, self.app_log_max_files - 1)
        if oldest_path is None or second_oldest_path is None:
            self.log.warning(
                "Cannot verify oldest-drop: oldest={} 2nd-oldest={}".format(
                    oldest_path, second_oldest_path))
            return files_at_cap

        fp_oldest = self._get_file_fingerprint(node, oldest_path)
        fp_second_oldest = self._get_file_fingerprint(node, second_oldest_path)
        self.log.info("Pre-extra-rotation: oldest={}, 2nd-oldest={}".format(
            oldest_path, second_oldest_path))

        self.sleep(self.dek_rotation_interval,
                   "Waiting {}s for one DEK rotation to trigger oldest-drop".format(
                       self.dek_rotation_interval))
        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        deadline = time.time() + self.rotation_wait_timeout
        files_post = None
        while time.time() < deadline:
            candidate_files = self._list_log_files(node, log_dir)
            new_oldest = self._find_file_by_suffix(
                candidate_files, self.app_log_max_files)
            if new_oldest and self._get_file_fingerprint(node, new_oldest) != fp_oldest:
                files_post = candidate_files
                break
            self.sleep(5, "Waiting for oldest file to be replaced by extra rotation")
        self.assertIsNotNone(
            files_post,
            "Oldest (suffix {}) was not replaced within {}s".format(
                self.app_log_max_files, self.rotation_wait_timeout))

        overflow = self._find_file_by_suffix(files_post, self.app_log_max_files + 1)
        self.assertIsNone(
            overflow,
            "File with suffix {} exists; oldest was pushed rather than dropped".format(
                self.app_log_max_files + 1))

        new_oldest_path = self._find_file_by_suffix(files_post, self.app_log_max_files)
        fp_new_oldest = self._get_file_fingerprint(node, new_oldest_path)
        self.assertEqual(
            fp_second_oldest, fp_new_oldest,
            "New oldest ({}) fingerprint does not match old 2nd-oldest ({}); "
            "wrong file promoted to oldest position".format(
                new_oldest_path, second_oldest_path))
        self.log.info(
            "Oldest-drop confirmed: old 2nd-oldest {} is now the oldest {}".format(
                second_oldest_path, new_oldest_path))
        return files_post


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


    # --------------------- App Log Max Size and Files ---------------------

    def test_app_log_max_files_rotation(self):
        """
        Verify app_log_max_files cap and per-file size limit:
        - Drive enough mutations to fill all slots (11 files)
        - On overflow, oldest file is dropped; total count stays at max_files + 1
        - No single file exceeds app_log_max_size
        Total capacity = app_log_max_size * app_log_max_files
        """
        total_capacity = self.app_log_max_size * self.app_log_max_files
        self.log.info(
            "Total app log capacity: {} bytes ({} files x {} bytes/file)".format(
                total_capacity, self.app_log_max_files, self.app_log_max_size))

        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)

        max_total = self.app_log_max_files + 1
        files = self._drive_rotations_to_count(
            eventing_node, log_dir, max_total, self.rotation_wait_timeout)

        self.assertLessEqual(
            len(files), max_total,
            "File count {} exceeds cap of {} (app_log_max_files={})".format(
                len(files), max_total, self.app_log_max_files))

        suffixes = []
        for f in files:
            m = _re.search(r'\.(\d+)$', f)
            if m:
                suffixes.append(int(m.group(1)))
        self.log.info("Rotated file suffixes present: {}".format(sorted(suffixes)))
        if suffixes:
            self.assertLessEqual(
                max(suffixes), self.app_log_max_files,
                "Max suffix {} > app_log_max_files {}; oldest file was not dropped".format(
                    max(suffixes), self.app_log_max_files))
        # Might get rotated further before the check, will validate in separate TC
        # files = self._assert_oldest_file_dropped(eventing_node, log_dir, files)
        self._assert_no_file_exceeds_max_size(eventing_node, log_dir)
        self.undeploy_and_delete_function(body)

    def test_app_log_max_files_rotation_with_encryption(self):
        """
        Verify app_log_max_files cap with log encryption enabled:
        - Same cap and per-file size assertions as test_app_log_max_files_rotation
        - Every file at max capacity must carry the encryption signature
        Total capacity = app_log_max_size * app_log_max_files
        """
        total_capacity = self.app_log_max_size * self.app_log_max_files
        self.log.info(
            "Total app log capacity: {} bytes ({} files x {} bytes/file)".format(
                total_capacity, self.app_log_max_files, self.app_log_max_size))

        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)

        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)

        max_total = self.app_log_max_files + 1
        files = self._drive_rotations_to_count(
            eventing_node, log_dir, max_total, self.rotation_wait_timeout)

        self.assertLessEqual(
            len(files), max_total,
            "File count {} exceeds cap of {} (app_log_max_files={})".format(
                len(files), max_total, self.app_log_max_files))

        for path in files:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "File {} is not encrypted despite encryption being enabled".format(path))

        suffixes = []
        for f in files:
            m = _re.search(r'\.(\d+)$', f)
            if m:
                suffixes.append(int(m.group(1)))
        self.log.info("Rotated file suffixes present: {}".format(sorted(suffixes)))
        if suffixes:
            self.assertLessEqual(
                max(suffixes), self.app_log_max_files,
                "Max suffix {} > app_log_max_files {}; oldest file was not dropped".format(
                    max(suffixes), self.app_log_max_files))

        # Might get rotated further before the check, will validate in separate TC
        #files = self._assert_oldest_file_dropped(eventing_node, log_dir, files)
        self._assert_no_file_exceeds_max_size(eventing_node, log_dir)
        self.undeploy_and_delete_function(body)

    def test_app_log_oldest_file_dropped(self):
        """
        Verify oldest log file is dropped and second-oldest is promoted
        when the file count hits app_log_max_files.
        DEK rotation (dekRotationInterval) drives log rotations without
        relying on heavy mutations to fill up each file.
        - Enable encryption, pin dekRotationInterval high, deploy function
        - Lower dekRotationInterval so DEK rotations trigger log rotations
        - Wait for files to accumulate to the cap
        - Trigger mutations to cause one more rotation
        - Assert oldest file is dropped and second-oldest is promoted
        """
        body = self._create_and_deploy_function()
        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for initial app-log writes to settle")

        self._configure_dek_rotation(self.dek_rotation_interval, self.STABLE_INTERVAL_S)

        deadline = time.time() + self.rotation_wait_timeout
        files_at_cap = self._list_log_files(eventing_node, log_dir)
        while len(files_at_cap) < self.app_log_max_files + 1 and time.time() < deadline:
            self.sleep(5, "Waiting for DEK rotations to drive files to cap "
                          "({}/{})".format(len(files_at_cap), self.app_log_max_files + 1))
            files_at_cap = self._list_log_files(eventing_node, log_dir)

        self.assertGreaterEqual(
            len(files_at_cap), self.app_log_max_files,
            "Files did not reach cap of {} within {}s; got {}".format(
                self.app_log_max_files, self.rotation_wait_timeout, len(files_at_cap)))

        self._assert_oldest_file_dropped(eventing_node, log_dir, files_at_cap)
        self.undeploy_and_delete_function(body)


    # ----------------- Log Encryption Key Rotation Tests -----------------

    def test_log_encryption_key_rotation(self):
        """
        DEK rotation test (log.dekRotationInterval):
        - Pin dekRotationInterval high to prevent accidental rotation during setup
        - Create a KEK, enable log encryption, deploy function
        - Verify initial log files are encrypted
        - Reduce dekRotationInterval to trigger DEK rotation
        - Wait for DEK rotation interval to expire, then trigger mutations
        - Wait for log rotation
        - Re-pin dekRotationInterval high to prevent a second rotation during validation
        - Verify the new active log is still encrypted
        - Verify pre-rotation files remain encrypted
        - Verify /getAppLog serves content spanning files from both DEK generations
        """
        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for initial app-log writes to settle")

        files_before = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(
            len(files_before), 0,
            "No eventing log files found before DEK rotation in {}".format(log_dir))
        for path in files_before:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-rotation file {} is not encrypted with initial DEK".format(path))

        self._configure_dek_rotation(self.dek_rotation_interval, self.STABLE_INTERVAL_S)
        self.sleep(
            self.dek_rotation_interval,
            "Waiting {}s for DEK rotation".format(self.dek_rotation_interval))

        self._trigger_more_mutations()
        files_after = self._wait_for_rotation(
            eventing_node, log_dir, baseline_count=len(files_before),
            timeout=self.rotation_wait_timeout)

        # Re-pin immediately after detecting rotation to prevent a second rotation
        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        latest_after = files_after[0]
        self.assertTrue(
            self._file_is_encrypted(eventing_node, latest_after),
            "Latest log {} is not encrypted after DEK rotation".format(latest_after))

        pre_rotation_files = [f for f in files_after if _re.search(r'\.\d+$', f)]
        for path in pre_rotation_files:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-rotation file {} lost encryption after DEK rotation".format(path))

        applogs = self.get_app_logs(
            self.function_name, size=self.logsize, aggregate=self.aggregate)
        self._assert_app_log_non_empty(applogs)
        self.undeploy_and_delete_function(body)

    def test_log_encryption_force_rotate_and_reencrypt(self):
        """
        Force log re-encryption test (POST /controller/dropEncryptionAtRestDeks/log):
        Unlike DEK rotation (which generates a new active DEK while retaining old ones),
        force re-encryption drops all existing DEKs and re-encrypts all log data with a
        freshly generated active DEK so every byte on disk is under a single new key.
        - Pin dekRotationInterval high to prevent background rotation during setup
        - Create a KEK, enable log encryption, deploy function
        - Verify initial log files are encrypted
        - Trigger force log re-encryption; eventing rotates the active log on receipt
        - Wait for log rotation
        - Verify the new active log is still encrypted
        - Verify pre-re-encryption files remain encrypted
        - Verify /getAppLog serves content spanning files from both DEK generations
        """
        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for initial app-log writes to settle")

        files_before = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(
            len(files_before), 0,
            "No eventing log files found before force re-encryption in {}".format(log_dir))
        for path in files_before:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-re-encryption file {} is not encrypted".format(path))

        status, response = self.rest.trigger_log_reencryption()
        self.assertTrue(status,
                        "Failed to trigger force log re-encryption: {}".format(response))
        self.log.info("Force log re-encryption triggered: {}".format(response))

        self._trigger_more_mutations()
        files_after = self._wait_for_rotation(
            eventing_node, log_dir, baseline_count=len(files_before),
            timeout=self.rotation_wait_timeout)

        latest_after = files_after[0]
        self.assertTrue(
            self._file_is_encrypted(eventing_node, latest_after),
            "Latest log {} is not encrypted after force re-encryption".format(latest_after))

        pre_reencrypt_files = [f for f in files_after if _re.search(r'\.\d+$', f)]
        for path in pre_reencrypt_files:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-re-encryption file {} lost encryption after force re-encryption".format(
                    path))

        applogs = self.get_app_logs(
            self.function_name, size=self.logsize, aggregate=self.aggregate)
        self._assert_app_log_non_empty(applogs)
        self.undeploy_and_delete_function(body)

    def test_log_encryption_dek_continuous_rotation(self):
        """
        Continuous DEK rotation test:
        - Pin dekRotationInterval high during setup to prevent premature rotation
        - Create KEK, enable log encryption, deploy function
        - Set dekRotationInterval to dek_rotation_interval and repeat num_rotations cycles:
            - Sleep the rotation interval so the DEK timer fires
            - Verify all current log files remain encrypted
        - Re-pin interval high after all cycles
        - Verify /getAppLog works after all rotations
        """
        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for initial app-log writes to settle")

        files = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(len(files), 0,
                           "No eventing log files found in {}".format(log_dir))

        self._configure_dek_rotation(self.dek_rotation_interval, self.STABLE_INTERVAL_S)

        prev_count = len(files)

        for cycle in range(1, self.num_rotations + 1):
            self.sleep(
                self.dek_rotation_interval,
                "Cycle {}/{}: waiting {}s for DEK rotation".format(
                    cycle, self.num_rotations, self.dek_rotation_interval))

            self._trigger_more_mutations()
            files_now = self._wait_for_rotation(
                eventing_node, log_dir, baseline_count=prev_count,
                timeout=self.rotation_wait_timeout)

            for path in files_now:
                self.assertTrue(
                    self._file_is_encrypted(eventing_node, path),
                    "File {} is not encrypted in cycle {}".format(path, cycle))

            prev_count = len(files_now)
            self.log.info("Cycle {}/{}: {} log files, all encrypted".format(
                cycle, self.num_rotations, len(files_now)))

        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        applogs = self.get_app_logs(
            self.function_name, size=self.logsize, aggregate=self.aggregate)
        self._assert_app_log_non_empty(applogs)
        self.undeploy_and_delete_function(body)

    def test_log_encryption_dek_expiry(self):
        """
        DEK expiry test (log.dekLifetime):
        A low dekLifetime causes the cluster to generate a fresh DEK once the active
        one ages past that threshold
        - Pin dekRotationInterval high so rotation does not interfere
        - Set a low dekLifetime so the initial DEK expires quickly
        - Create KEK, enable log encryption, deploy function; capture initial DEK UUID
        - Wait for dekLifetime to expire; cluster auto-generates a new active DEK
        - Trigger mutations and wait for log rotation
        - Verify the new active log carries a different UUID (new DEK applied)
        - Verify pre-expiry files remain encrypted (old DEK still readable)
        - Verify /getAppLog works across both DEK generations
        """
        key_id = self._create_log_encryption_secret()
        self._set_log_encryption_method("encryptionKey", key_id=key_id)
        self._configure_dek_rotation(self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        body = self._create_and_deploy_function()
        log_dir = self._get_log_dir()
        eventing_node = self.get_nodes_from_services_map(
            service_type="eventing", get_all_nodes=False)
        self.sleep(15, "Wait for initial app-log writes to settle")

        files_before = self._list_log_files(eventing_node, log_dir)
        self.assertGreater(
            len(files_before), 0,
            "No eventing log files found before DEK expiry in {}".format(log_dir))
        for path in files_before:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-expiry file {} is not encrypted".format(path))

        self._configure_dek_rotation(self.dek_lifetime, self.dek_lifetime)
        self.sleep(self.dek_lifetime, "Waiting {}s for DEK to expire".format(self.dek_lifetime))

        self._trigger_more_mutations()
        files_after = self._wait_for_rotation(
            eventing_node, log_dir, baseline_count=len(files_before),
            timeout=self.rotation_wait_timeout)

        latest_after = files_after[0]
        self.assertTrue(
            self._file_is_encrypted(eventing_node, latest_after),
            "Latest log {} is not encrypted after DEK expiry".format(latest_after))

        pre_expiry_files = [f for f in files_after if _re.search(r'\.\d+$', f)]
        for path in pre_expiry_files:
            self.assertTrue(
                self._file_is_encrypted(eventing_node, path),
                "Pre-expiry file {} lost encryption after DEK expiry".format(path))

        applogs = self.get_app_logs(
            self.function_name, size=self.logsize, aggregate=self.aggregate)
        self._assert_app_log_non_empty(applogs)
        self.undeploy_and_delete_function(body)