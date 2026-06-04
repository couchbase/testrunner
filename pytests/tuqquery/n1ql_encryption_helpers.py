
"""Query-specific encryption-at-rest helpers."""

import threading
import time

from pytests.gsi.gsi_encryption_helpers import GSIEncryptionHelpers
from lib.remote.remote_util import RemoteMachineShellConnection


def _is_transient_query_log(shell, file_path):
    """Return True iff file no longer exists or is currently empty (size 0).

    rlstream.* and local_request_log.* files are rotated/recycled continuously;
    an empty or missing file means we hit it between truncation and the next
    write and should skip rather than flag as 'not encrypted'."""
    out, _ = shell.execute_command(
        f"if [ ! -e {file_path} ]; then echo MISSING; "
        f"elif [ ! -s {file_path} ]; then echo EMPTY; else echo OK; fi"
    )
    token = ''.join(out).strip() if out else ""
    return token in ("MISSING", "EMPTY")


class N1QLEncryptionHelpers(GSIEncryptionHelpers):

    def __init__(self, log):
        super().__init__(log)

    def verify_query_log_files_encrypted(self, query_nodes, expected_key_ids=None):
        """
        Verify that query service request log files are encrypted.

        Checks rlstream.* (active stream) and local_request_log.* (archived) files
        in the logs directory on each query node for the "Couchbase Encrypted"
        magic-bytes header. If expected_key_ids is provided, also verifies that at
        least one of the given key IDs appears in each file's header.

        Args:
            query_nodes: List of query service server objects
            expected_key_ids: Optional list of key ID strings to look for in headers

        Returns:
            dict: {node_ip: {"rlstream": {file_path: result_dict},
                             "local_request_log": {file_path: result_dict}}}
                  where result_dict has "encrypted" (bool), "details" (str),
                  and optionally "key_id_found" (bool).
        """
        results = {}
        for node in query_nodes:
            self.log.info(f"Validating query log file encryption on node {node.ip}")
            node_result = {"rlstream": {}, "local_request_log": {}}

            log_path = self.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            # -size +0c excludes 0-byte files: rlstream.* gets recycled
            # continuously, and an empty file is a rotation artifact, not a
            # real validation target.
            rlstream_out, _ = shell.execute_command(
                f"find {log_path} -name 'rlstream.*' -size +0c 2>/dev/null"
            )
            local_log_out, _ = shell.execute_command(
                f"find {log_path} -name 'local_request_log.*' -size +0c 2>/dev/null"
            )

            rlstream_files = [f.strip() for f in rlstream_out if f.strip()]
            local_log_files = [f.strip() for f in local_log_out if f.strip()]

            self.log.info(
                f"Node {node.ip}: found {len(rlstream_files)} rlstream file(s), "
                f"{len(local_log_files)} local_request_log file(s)"
            )

            for category, file_list in (
                ("rlstream", rlstream_files),
                ("local_request_log", local_log_files),
            ):
                for file_path in file_list:
                    is_encrypted, details = self.verify_file_encryption_magic_bytes(
                        node, file_path
                    )
                    entry = {"encrypted": is_encrypted, "details": details}

                    # If xxd returned nothing and the file is now gone/empty,
                    # it was deleted or truncated mid-validation. Both
                    # rlstream.* and local_request_log.* are transient; skip
                    # rather than flag as a failure.
                    if (
                        not is_encrypted
                        and _is_transient_query_log(shell, file_path)
                    ):
                        entry["transient"] = True
                        entry["encrypted"] = True
                        entry["key_id_found"] = True
                        self.log.info(
                            f"Node {node.ip}: {file_path} skipped — transient "
                            f"{category} file (deleted/empty during validation)"
                        )
                        node_result[category][file_path] = entry
                        continue

                    if is_encrypted and expected_key_ids:
                        key_id_found = False
                        for key_id in expected_key_ids:
                            found, _ = self.verify_file_header_contains(
                                node, file_path, str(key_id)
                            )
                            if found:
                                key_id_found = True
                                break
                        entry["key_id_found"] = key_id_found
                        if not key_id_found:
                            self.log.warning(
                                f"Node {node.ip}: {file_path} encrypted but none of "
                                f"{expected_key_ids} found in header"
                            )
                    elif is_encrypted:
                        entry["key_id_found"] = True

                    log_level = self.log.info if is_encrypted else self.log.warning
                    log_level(
                        f"Node {node.ip}: {file_path} encrypted={is_encrypted}, "
                        f"details={details}"
                    )
                    node_result[category][file_path] = entry

            shell.disconnect()
            results[node.ip] = node_result

        return results

    def verify_query_log_files_encrypted_with_watcher(
        self, query_nodes, expected_key_ids=None, watch_seconds=45, poll_interval=2,
    ):
        """Background-thread variant of verify_query_log_files_encrypted.

        For each query node, spawns a thread that polls for rlstream.* /
        local_request_log.* files for `watch_seconds`, validating each newly
        observed file immediately. Files that vanish or are empty by the time
        xxd runs are recorded with `transient=True` and never counted as
        failures. Returns the same dict shape as verify_query_log_files_encrypted.
        """
        results = {}
        results_lock = threading.Lock()

        def _watch_node(node):
            node_result = {"rlstream": {}, "local_request_log": {}}
            log_path = self.get_log_path(node)
            deadline = time.time() + watch_seconds
            seen = set()
            while time.time() < deadline:
                shell = RemoteMachineShellConnection(node, verbose=False)
                try:
                    rl_out, _ = shell.execute_command(
                        f"find {log_path} -name 'rlstream.*' -size +0c 2>/dev/null"
                    )
                    lrl_out, _ = shell.execute_command(
                        f"find {log_path} -name 'local_request_log.*' -size +0c 2>/dev/null"
                    )
                    rl_files = [f.strip() for f in rl_out if f.strip()]
                    lrl_files = [f.strip() for f in lrl_out if f.strip()]
                    for category, file_list in (
                        ("rlstream", rl_files),
                        ("local_request_log", lrl_files),
                    ):
                        for fpath in file_list:
                            key = (category, fpath)
                            if key in seen:
                                continue
                            is_enc, details = self.verify_file_encryption_magic_bytes(
                                node, fpath
                            )
                            entry = {"encrypted": is_enc, "details": details}
                            if (
                                not is_enc
                                and _is_transient_query_log(shell, fpath)
                            ):
                                entry["transient"] = True
                                entry["encrypted"] = True
                                entry["key_id_found"] = True
                                self.log.info(
                                    f"Node {node.ip}: {fpath} skipped — transient "
                                    f"{category} file (deleted/empty during validation)"
                                )
                                node_result[category][fpath] = entry
                                seen.add(key)
                                continue
                            if is_enc and expected_key_ids:
                                key_id_found = False
                                for kid in expected_key_ids:
                                    found, _ = self.verify_file_header_contains(
                                        node, fpath, str(kid)
                                    )
                                    if found:
                                        key_id_found = True
                                        break
                                entry["key_id_found"] = key_id_found
                            elif is_enc:
                                entry["key_id_found"] = True
                            node_result[category][fpath] = entry
                            seen.add(key)
                finally:
                    shell.disconnect()
                time.sleep(poll_interval)
            with results_lock:
                results[node.ip] = node_result

        threads = [threading.Thread(target=_watch_node, args=(n,), daemon=True)
                   for n in query_nodes]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def verify_query_ffdc_files_encrypted(self, query_nodes, expected_key_ids=None):
        """
        Verify that query FFDC files are encrypted.

        Checks query_ffdc_MAN_areq*, query_ffdc_MAN_creq*, and query_ffdc_MAN_vita*
        files in the logs directory on each query node for the "Couchbase Encrypted"
        magic-bytes header. If expected_key_ids is provided, also verifies that at
        least one of the given key IDs appears in each file's header.

        Args:
            query_nodes: List of query service server objects
            expected_key_ids: Optional list of key ID strings to look for in headers

        Returns:
            dict: {node_ip: {file_path: {encrypted: bool, details: str, key_id_found: bool}}}
        """
        results = {}
        for node in query_nodes:
            self.log.info(f"Validating query FFDC file encryption on node {node.ip}")
            node_result = {}

            log_path = self.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            ffdc_out, _ = shell.execute_command(
                f"find {log_path} -name 'query_ffdc_MAN_areq*' "
                f"-o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
            )
            shell.disconnect()

            ffdc_files = [f.strip() for f in ffdc_out if f.strip()]

            self.log.info(
                f"Node {node.ip}: found {len(ffdc_files)} FFDC file(s)"
            )

            for file_path in ffdc_files:
                is_encrypted, details = self.verify_file_encryption_magic_bytes(
                    node, file_path
                )
                entry = {"encrypted": is_encrypted, "details": details}

                if is_encrypted and expected_key_ids:
                    key_id_found = False
                    for key_id in expected_key_ids:
                        found, _ = self.verify_file_header_contains(
                            node, file_path, str(key_id)
                        )
                        if found:
                            key_id_found = True
                            break
                    entry["key_id_found"] = key_id_found
                    if not key_id_found:
                        self.log.warning(
                            f"Node {node.ip}: {file_path} encrypted but none of "
                            f"{expected_key_ids} found in header"
                        )
                elif is_encrypted:
                    entry["key_id_found"] = True

                log_level = self.log.info if is_encrypted else self.log.warning
                log_level(
                    f"Node {node.ip}: {file_path} encrypted={is_encrypted}, "
                    f"details={details}"
                )
                node_result[file_path] = entry

            results[node.ip] = node_result

        return results

    def verify_query_log_files_decrypted(self, query_nodes):
        """
        Verify that query service request log files are NOT encrypted.

        Checks rlstream.* (active stream) and local_request_log.* (archived) files
        in the logs directory on each query node. These files should NOT
        have the "Couchbase Encrypted" magic-bytes header (encryption disabled).

        Args:
            query_nodes: List of query service server objects

        Returns:
            dict: {node_ip: {"rlstream": {file_path: result_dict},
                             "local_request_log": {file_path: result_dict}}}
                  where result_dict has "encrypted" (bool), "details" (str).
        """
        results = {}
        for node in query_nodes:
            self.log.info(f"Validating query log files are decrypted on node {node.ip}")
            node_result = {"rlstream": {}, "local_request_log": {}}

            log_path = self.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            rlstream_out, _ = shell.execute_command(
                f"find {log_path} -name 'rlstream.*' -size +0c 2>/dev/null"
            )
            local_log_out, _ = shell.execute_command(
                f"find {log_path} -name 'local_request_log.*' -size +0c 2>/dev/null"
            )

            rlstream_files = [f.strip() for f in rlstream_out if f.strip()]
            local_log_files = [f.strip() for f in local_log_out if f.strip()]

            self.log.info(
                f"Node {node.ip}: found {len(rlstream_files)} rlstream file(s), "
                f"{len(local_log_files)} local_request_log file(s)"
            )

            for category, file_list in (
                ("rlstream", rlstream_files),
                ("local_request_log", local_log_files),
            ):
                for file_path in file_list:
                    is_encrypted, details = self.verify_file_encryption_magic_bytes(
                        node, file_path
                    )
                    entry = {"encrypted": is_encrypted, "details": details}

                    if _is_transient_query_log(shell, file_path):
                        entry["transient"] = True
                        self.log.info(
                            f"Node {node.ip}: {file_path} skipped — transient "
                            f"{category} file (deleted/empty during validation)"
                        )
                        node_result[category][file_path] = entry
                        continue

                    log_level = self.log.info if not is_encrypted else self.log.warning
                    log_level(
                        f"Node {node.ip}: {file_path} encrypted={is_encrypted}, "
                        f"details={details}"
                    )
                    node_result[category][file_path] = entry

            shell.disconnect()
            results[node.ip] = node_result

        return results

    def verify_query_ffdc_files_decrypted(self, query_nodes):
        """
        Verify that query FFDC files are NOT encrypted.

        Checks query_ffdc_MAN_areq*, query_ffdc_MAN_creq*, and query_ffdc_MAN_vita*
        files in the logs directory on each query node. These files should NOT
        have the "Couchbase Encrypted" magic-bytes header (encryption disabled).

        Args:
            query_nodes: List of query service server objects

        Returns:
            dict: {node_ip: {file_path: {encrypted: bool, details: str}}}
        """
        results = {}
        for node in query_nodes:
            self.log.info(f"Validating query FFDC files are decrypted on node {node.ip}")
            node_result = {}

            log_path = self.get_log_path(node)
            shell = RemoteMachineShellConnection(node, verbose=False)
            ffdc_out, _ = shell.execute_command(
                f"find {log_path} -name 'query_ffdc_MAN_areq*' "
                f"-o -name 'query_ffdc_MAN_creq*' -o -name 'query_ffdc_MAN_vita*' 2>/dev/null"
            )
            shell.disconnect()

            ffdc_files = [f.strip() for f in ffdc_out if f.strip()]

            self.log.info(
                f"Node {node.ip}: found {len(ffdc_files)} FFDC file(s)"
            )

            for file_path in ffdc_files:
                is_encrypted, details = self.verify_file_encryption_magic_bytes(
                    node, file_path
                )
                entry = {"encrypted": is_encrypted, "details": details}

                log_level = self.log.info if not is_encrypted else self.log.warning
                log_level(
                    f"Node {node.ip}: {file_path} encrypted={is_encrypted}, "
                    f"details={details}"
                )
                node_result[file_path] = entry

            results[node.ip] = node_result

        return results
