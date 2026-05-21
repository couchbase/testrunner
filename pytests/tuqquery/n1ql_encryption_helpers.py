
"""Query-specific encryption-at-rest helpers."""

from pytests.gsi.gsi_encryption_helpers import GSIEncryptionHelpers
from lib.remote.remote_util import RemoteMachineShellConnection


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
            rlstream_out, _ = shell.execute_command(
                f"find {log_path} -name 'rlstream.*' 2>/dev/null"
            )
            local_log_out, _ = shell.execute_command(
                f"find {log_path} -name 'local_request_log.*' 2>/dev/null"
            )
            shell.disconnect()

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

            results[node.ip] = node_result

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
                f"find {log_path} -name 'rlstream.*' 2>/dev/null"
            )
            local_log_out, _ = shell.execute_command(
                f"find {log_path} -name 'local_request_log.*' 2>/dev/null"
            )
            shell.disconnect()

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

                    log_level = self.log.info if not is_encrypted else self.log.warning
                    log_level(
                        f"Node {node.ip}: {file_path} encrypted={is_encrypted}, "
                        f"details={details}"
                    )
                    node_result[category][file_path] = entry

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
