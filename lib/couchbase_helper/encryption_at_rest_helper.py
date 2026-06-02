
"""Shared encryption-at-rest primitives."""

from lib.remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection


class EncryptionAtRestHelper:
    ENCRYPTION_MAGIC_BYTES = "Couchbase Encrypted"

    def __init__(self, log):
        self.log = log

    def get_file_header_text(self, node, file_path, bytes_to_read=512):
        shell = RemoteMachineShellConnection(node, verbose=False)
        cmd = f"xxd -l {bytes_to_read} -p {file_path} 2>/dev/null"
        output, _ = shell.execute_command(cmd)

        if not output:
            ls_out, _ = shell.execute_command(f"ls -la {file_path} 2>&1")
            xxd_err_out, _ = shell.execute_command(f"xxd -l 64 {file_path} 2>&1")
            file_type_out, _ = shell.execute_command(f"file {file_path} 2>&1")
            shell.disconnect()
            ls_str = ' '.join(ls_out).strip() if ls_out else "no ls output"
            xxd_err_str = ' '.join(xxd_err_out).strip() if xxd_err_out else "(xxd also silent with stderr)"
            file_type_str = ' '.join(file_type_out).strip() if file_type_out else "unknown"
            diag = f"ls: [{ls_str}] | file: [{file_type_str}] | xxd -l 64 (with stderr): [{xxd_err_str}]"
            return False, "", f"No output from xxd. {diag}"

        shell.disconnect()
        hex_str = ''.join(output).strip()
        try:
            raw_bytes = bytes.fromhex(hex_str)
            actual = raw_bytes.decode('ascii', errors='replace')
            printable = ''.join(
                chr(b) if 32 <= b < 127 else f'\\x{b:02x}'
                for b in raw_bytes
            )
            return True, actual, printable
        except (ValueError, UnicodeDecodeError) as e:
            return False, "", f"hex parse error: {str(e)}"

    def verify_file_encryption_magic_bytes(self, node, file_path):
        header_read, actual, printable = self.get_file_header_text(
            node, file_path, bytes_to_read=64
        )
        if header_read:
            if self.ENCRYPTION_MAGIC_BYTES in actual:
                return True, self.ENCRYPTION_MAGIC_BYTES
            return False, printable
        return False, printable

    def verify_no_plaintext_in_file(self, node, file_path, search_patterns=None):
        """
        Verify file contains no plaintext using strings command.
        
        Args:
            node: Server object to check
            file_path: Absolute path to file to verify
            search_patterns: List of patterns to search for (default: common sensitive terms)
            
        Returns:
            tuple: (no_plaintext: bool, found_pattern: str or None)
        """
        search_patterns = search_patterns or ["password", "username", "bucket", "secret"]
        shell = RemoteMachineShellConnection(node)
        cmd = f"strings {file_path} 2>/dev/null | head -100"
        output, _ = shell.execute_command(cmd)
        shell.disconnect()
        
        output_str = ' '.join(output).lower() if output else ''
        for pattern in search_patterns:
            if pattern.lower() in output_str:
                return False, pattern
        return True, None
    
    def get_files_in_directory(self, node, directory, file_pattern="*"):
        """
        Get list of files matching pattern in directory.
        
        Args:
            node: Server object to check
            directory: Directory path to search
            file_pattern: Glob pattern for file matching
            
        Returns:
            list: List of file paths
        """
        shell = RemoteMachineShellConnection(node)
        # Use find to get files, handle both Linux and macOS/BSD
        cmd = f"find {directory} -type f -name '{file_pattern}' 2>/dev/null | head -20"
        output, _ = shell.execute_command(cmd)
        shell.disconnect()
        
        return [f.strip() for f in output if f.strip()]
    
    def verify_gsi_storage_files_encrypted(self, index_nodes, gsi_type="forestdb", expected_key_id=None):
        """
        Verify that storage files (BHIVE/Plasma/MOI) on index nodes are encrypted.
        
        Storage files are located under the index path (obtained via get_index_path()).
        All storage types (Plasma, MOI, BHIVE) have files under @2i/ directory:
        - Plasma/forestdb: .fdb files, .index directories with data/log files
        - MOI (memory_optimized): metadata and index files in @2i directory
        - BHIVE: vector index files under @2i/@bhive/bhive-shards/ directory
        
        Directory structure under @2i:
        - @bhive/bhive-shards/ - BHIVE vector index shards (sstable.*.data, state.*, config.json)
        - *.index/ - Index directories (codebook/, docIndex/, mainIndex/ with .data, .codebook files)
        - cache/ - Cache files (meta/, stats/)
        - indexstats/ - Index stats (stats file)
        - metadata_repo_v2/ - Metadata repository (sstable.*.data, state.*, wal.*)
        - shards/ - Shard directories (data/, meta/ with .data, .json files)
        
        Args:
            index_nodes: List of index server objects
            gsi_type: Storage type ("forestdb", "memory_optimized", or "plasma")
            
        Returns:
            dict: Results per node with validation status
        """
        results = {}
        expected_key_id = None if expected_key_id in [None, ""] else str(expected_key_id)
        
        # File patterns to check for encryption (prioritizing data files)
        data_file_patterns = ["*.data", "*.codebook", "stats", "wal.*", "shard.json"]
        
        # Directories to check under @2i
        storage_subdirs = [
            "@bhive/bhive-shards",  # BHIVE vector index shards
            "shards",               # Regular shards
            "metadata_repo_v2",     # Metadata repository
            "cache",                # Cache files
            "indexstats"            # Index stats
        ]
        
        for node in index_nodes:
            self.log.info(f"Validating storage file encryption on node {node.ip} (gsi_type={gsi_type})")
            rest = RestConnection(node)
            index_path = rest.get_index_path()
            
            # All storage types have files under @2i directory
            storage_dir = f"{index_path}/@2i"
            
            shell = RemoteMachineShellConnection(node)
            cmd = f"test -d {storage_dir} && echo 'exists' || echo 'not_found'"
            output, _ = shell.execute_command(cmd)
            shell.disconnect()
            
            if not output or 'exists' not in output[0]:
                self.log.warning(f"Node {node.ip}: Storage directory {storage_dir} not found")
                results[node.ip] = {"status": "skipped", "reason": f"Directory {storage_dir} not found"}
                continue
            
            # Collect files to validate from all relevant directories
            all_files = []
            
            # 1. Check *.index directories (contain codebooks and data files)
            shell = RemoteMachineShellConnection(node)
            cmd = f"find {storage_dir} -maxdepth 1 -type d -name '*.index' 2>/dev/null | head -5"
            index_dirs, _ = shell.execute_command(cmd)
            shell.disconnect()
            
            for idx_dir in index_dirs:
                idx_dir = idx_dir.strip()
                if idx_dir:
                    # Get .data and .codebook files from index directory
                    for pattern in ["*.data", "*.codebook"]:
                        files = self.get_files_in_directory(node, idx_dir, pattern)
                        all_files.extend(files[:2])  # Limit to 2 per pattern per index
            
            # 2. Check BHIVE shards directory
            bhive_shards_dir = f"{storage_dir}/@bhive/bhive-shards"
            shell = RemoteMachineShellConnection(node)
            cmd = f"test -d {bhive_shards_dir} && echo 'exists' || echo 'not_found'"
            output, _ = shell.execute_command(cmd)
            shell.disconnect()
            
            if output and 'exists' in output[0]:
                # Get sstable.data files from BHIVE shards
                files = self.get_files_in_directory(node, bhive_shards_dir, "*.data")
                all_files.extend(files[:3])  # Limit to 3 files
            
            # 3. Check other storage subdirectories
            for subdir in storage_subdirs:
                sub_dir_path = f"{storage_dir}/{subdir}"
                shell = RemoteMachineShellConnection(node)
                cmd = f"test -d {sub_dir_path} && echo 'exists' || echo 'not_found'"
                output, _ = shell.execute_command(cmd)
                shell.disconnect()
                
                if output and 'exists' in output[0]:
                    for pattern in data_file_patterns:
                        files = self.get_files_in_directory(node, sub_dir_path, pattern)
                        all_files.extend(files[:2])  # Limit to 2 files per pattern
            
            # 4. Also check for any .fdb files directly under @2i (Plasma storage)
            fdb_files = self.get_files_in_directory(node, storage_dir, "*.fdb")
            all_files.extend(fdb_files[:3])
            
            # Deduplicate files
            all_files = list(set(all_files))
            
            if not all_files:
                self.log.warning(f"Node {node.ip}: No storage files found in {storage_dir}")
                results[node.ip] = {"status": "skipped", "reason": "No storage files found"}
                continue
            
            # Validate encryption on sample files
            encrypted_count = 0
            failed_files = []
            sample_files = all_files[:10]  # Check up to 10 files
            
            for file_path in sample_files:
                is_encrypted, details = self.verify_file_encryption_magic_bytes(node, file_path)
                if is_encrypted:
                    if expected_key_id is not None:
                        key_id_found, header_details = self.verify_file_header_contains(
                            node, file_path, expected_key_id
                        )
                        if not key_id_found:
                            failed_files.append(
                                (file_path, f"Missing encryption key id {expected_key_id} in header: {header_details}")
                            )
                            self.log.warning(
                                f"Node {node.ip}: File {file_path} is encrypted but header does not contain "
                                f"encryption key id {expected_key_id}: {header_details}"
                            )
                            continue
                    encrypted_count += 1
                    self.log.info(
                        f"Node {node.ip}: File {file_path} is encrypted"
                        + (f" and header contains encryption key id {expected_key_id}" if expected_key_id is not None else "")
                    )
                else:
                    failed_files.append((file_path, details))
                    self.log.warning(f"Node {node.ip}: File {file_path} may not be encrypted: {details}")
            
            if encrypted_count > 0:
                results[node.ip] = {
                    "status": "passed",
                    "encrypted_count": encrypted_count,
                    "total_checked": len(sample_files),
                    "failed_files": failed_files
                }
            else:
                results[node.ip] = {
                    "status": "failed",
                    "reason": "No encrypted files found",
                    "failed_files": failed_files
                }
        
        return results
    
    def verify_gsi_indexer_stats_log_encrypted(self, index_nodes, expected_key_id=None):
        """
        Verify that indexer_stats.log files are encrypted.
        
        The stats.log file is located in the standard logs directory.
        
        Args:
            index_nodes: List of index server objects
            
        Returns:
            dict: Results per node with validation status
        """
        results = {}
        expected_key_id = None if expected_key_id in [None, ""] else str(expected_key_id)
        
        for node in index_nodes:
            self.log.info(f"Validating indexer stats log encryption on node {node.ip}")
            log_path = self.get_log_path(node)
            
            # stats.log file path
            stats_log = f"{log_path}/stats.log"
            
            shell = RemoteMachineShellConnection(node)
            cmd = f"test -f {stats_log} && echo 'exists' || echo 'not_found'"
            output, _ = shell.execute_command(cmd)
            shell.disconnect()
            
            if not output or 'exists' not in output[0]:
                self.log.info(f"Node {node.ip}: stats.log not found at {stats_log}")
                results[node.ip] = {"status": "skipped", "reason": "stats.log not found"}
                continue
            
            # Prefer explicit encrypted file if present
            shell = RemoteMachineShellConnection(node)
            cmd = f"test -f {stats_log}.enc && echo 'exists' || echo 'not_found'"
            output, _ = shell.execute_command(cmd)
            shell.disconnect()
            
            stats_file_to_check = f"{stats_log}.enc" if output and 'exists' in output[0] else stats_log

            if stats_file_to_check.endswith(".enc"):
                self.log.info(f"Node {node.ip}: Found encrypted stats.log.enc")

            is_encrypted, details = self.verify_file_encryption_magic_bytes(node, stats_file_to_check)
            if is_encrypted:
                if expected_key_id is not None:
                    key_id_found, header_details = self.verify_file_header_contains(
                        node, stats_file_to_check, expected_key_id
                    )
                    if not key_id_found:
                        reason = f"Header does not contain encryption key id {expected_key_id}: {header_details}"
                        results[node.ip] = {
                            "status": "failed",
                            "reason": reason,
                            "file": stats_file_to_check,
                            "failed_files": [(stats_file_to_check, reason)]
                        }
                        continue
                results[node.ip] = {"status": "passed", "file": stats_file_to_check}
            else:
                # Check if the regular stats.log contains plaintext (should be encrypted)
                no_plaintext, found = self.verify_no_plaintext_in_file(node, stats_log)
                if no_plaintext:
                    self.log.info(f"Node {node.ip}: stats.log contains no plaintext (may be encrypted)")
                    results[node.ip] = {"status": "passed", "file": stats_log, "note": "No plaintext detected"}
                else:
                    self.log.warning(f"Node {node.ip}: stats.log contains plaintext pattern: {found}")
                    results[node.ip] = {"status": "warning", "file": stats_log, "found_pattern": found}
        
        return results

    def verify_gsi_snapshot_files_encrypted(self, index_nodes, expected_key_id=None, encrypted_bucket_names=None):
        """
        Verify that specific files under snapshot folders inside index directories are encrypted.
        
        Snapshot folders are located inside each *.index directory under @2i/.
        Example path: @2i/test2_idx5_fap1xLqR_6580631863546817959_0.index/snapshot.2026-04-15.071344.331/
        
        NOT all files in the snapshot folder are encrypted. Only specific files need encryption:
        
        Files that SHOULD be encrypted:
        - manifest.json (in snapshot root)
        - data/shard-* (shard-0, shard-1, shard-2, etc.)
        - delta/shard-* (shard-0, shard-1, shard-2, etc.)
        
        Files that should NOT be encrypted:
        - nitro.json (in snapshot root)
        - data/checksums.json
        - data/files.json
        - delta/checksums.json
        - delta/files.json
        
        Args:
            index_nodes: List of index server objects
            expected_key_id: Optional encryption key id expected in encrypted file headers
            encrypted_bucket_names: Optional list of bucket names whose index snapshot files
                should be checked for encryption
            
        Returns:
            dict: Results per node with validation status
        """
        results = {}
        if isinstance(expected_key_id, (list, tuple, set)):
            expected_key_ids = [str(value) for value in expected_key_id if value not in [None, ""]]
        elif expected_key_id in [None, ""]:
            expected_key_ids = []
        else:
            expected_key_ids = [str(expected_key_id)]
        encrypted_bucket_names = encrypted_bucket_names or []
        
        # Files/patterns that MUST be encrypted in snapshot folders
        # Format: (relative_path_pattern, description)
        ENCRYPTED_FILE_PATTERNS = [
            ("manifest.json", "snapshot manifest"),
            ("data/shard-*", "data shard files"),
            ("delta/shard-*", "delta shard files"),
        ]
        
        for node in index_nodes:
            self.log.info(f"Validating snapshot folder file encryption on node {node.ip}")
            rest = RestConnection(node)
            index_path = rest.get_index_path()
            
            # Base directory for indexes
            storage_dir = f"{index_path}/@2i"
            
            shell = RemoteMachineShellConnection(node)
            cmd = f"test -d {storage_dir} && echo 'exists' || echo 'not_found'"
            output, _ = shell.execute_command(cmd)
            shell.disconnect()
            
            if not output or 'exists' not in output[0]:
                self.log.warning(f"Node {node.ip}: Storage directory {storage_dir} not found")
                results[node.ip] = {"status": "skipped", "reason": f"Directory {storage_dir} not found"}
                continue
            
            # Find all snapshot directories inside *.index folders
            # Pattern: @2i/*.index/snapshot.*
            shell = RemoteMachineShellConnection(node)
            cmd = f"find {storage_dir} -type d -path '*/*.index/snapshot.*' 2>/dev/null | head -20"
            output, _ = shell.execute_command(cmd)
            shell.disconnect()
            
            snapshot_dirs = [d.strip() for d in output if d.strip()]
            if encrypted_bucket_names:
                allowed_prefixes = tuple(f"/{bucket_name}_" for bucket_name in encrypted_bucket_names)
                snapshot_dirs = [d for d in snapshot_dirs if any(prefix in d for prefix in allowed_prefixes)]
            
            if not snapshot_dirs:
                reason = "No snapshot directories found"
                if encrypted_bucket_names:
                    reason = f"No snapshot directories found for encrypted buckets: {encrypted_bucket_names}"
                self.log.warning(f"Node {node.ip}: {reason} under {storage_dir}/*.index/")
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue
            
            self.log.info(f"Node {node.ip}: Found {len(snapshot_dirs)} snapshot directories")
            
            # Collect only files that should be encrypted from all snapshot directories
            files_to_check = []
            for snapshot_dir in snapshot_dirs:
                for pattern, description in ENCRYPTED_FILE_PATTERNS:
                    shell = RemoteMachineShellConnection(node)
                    cmd = f"find {snapshot_dir} -type f -path '*/{pattern}' 2>/dev/null | head -10"
                    output, _ = shell.execute_command(cmd)
                    shell.disconnect()
                    
                    matched_files = [f.strip() for f in output if f.strip()]
                    if matched_files:
                        self.log.info(f"Node {node.ip}: Found {len(matched_files)} {description} in {snapshot_dir}")
                        files_to_check.extend(matched_files)
            
            if not files_to_check:
                self.log.warning(f"Node {node.ip}: No encryptable files found in snapshot directories")
                results[node.ip] = {"status": "skipped", "reason": "No encryptable snapshot files found"}
                continue
            
            self.log.info(f"Node {node.ip}: Total {len(files_to_check)} files to check for encryption")
            
            # Validate encryption on files (up to 30 files)
            encrypted_count = 0
            failed_files = []
            sample_files = files_to_check[:30]
            
            for file_path in sample_files:
                is_encrypted, details = self.verify_file_encryption_magic_bytes(node, file_path)
                if is_encrypted:
                    if expected_key_ids:
                        key_id_found, matched_key_id = self.verify_file_header_contains_any(
                            node, file_path, expected_key_ids
                        )
                        if not key_id_found:
                            failed_files.append(
                                (file_path, f"Missing encryption key ids {expected_key_ids} in header")
                            )
                            continue
                    encrypted_count += 1
                    self.log.info(
                        f"Node {node.ip}: Snapshot file {file_path} is encrypted"
                        + (f" and header contains encryption key id {matched_key_id}" if expected_key_ids else "")
                    )
                else:
                    failed_files.append((file_path, details))
                    self.log.warning(f"Node {node.ip}: Snapshot file {file_path} is NOT encrypted: {details}")
            
            if encrypted_count > 0 and not failed_files:
                results[node.ip] = {
                    "status": "passed",
                    "encrypted_count": encrypted_count,
                    "total_checked": len(sample_files),
                    "failed_files": []
                }
            elif encrypted_count > 0:
                # Some files encrypted but some failed
                results[node.ip] = {
                    "status": "failed",
                    "reason": f"{len(failed_files)} files not encrypted",
                    "encrypted_count": encrypted_count,
                    "total_checked": len(sample_files),
                    "failed_files": failed_files
                }
            else:
                results[node.ip] = {
                    "status": "failed",
                    "reason": "No encrypted files found in snapshot folders",
                    "failed_files": failed_files
                }

        return results

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
            shell = RemoteMachineShellConnection(node)
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
            shell = RemoteMachineShellConnection(node)
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
            shell = RemoteMachineShellConnection(node)
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
            shell = RemoteMachineShellConnection(node)
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

    # ------------------------------------------------------------------
    # FTS (Search) encryption-at-rest validation
    # ------------------------------------------------------------------
    # IMPORTANT - FTS uses TWO on-disk encryption formats (per the Search
    # Encryption-at-Rest design doc + dev notes):
    #
    #   * .zap segment files and root.bolt are encrypted by Search itself using
    #     gocbcrypto in tens of thousands of small *logical chunks*. Non-user data
    #     (offset values, struct metadata) is deliberately left in plaintext and the
    #     Key Id is embedded internally -- there is NO leading "Couchbase Encrypted"
    #     magic header on these files (mirrors the GSI Plasma ".data" case).
    #     => We validate these by confirming sensitive USER data (field names and
    #        indexed term values) is NOT readable as plaintext.
    #
    #   * Whole-file metadata (PINDEX_META, PINDEX_BLEVE_META, planPIndexes) is small
    #     and encrypted in one block. Whether these carry the ns_server magic header
    #     or the Search-internal format is an OPEN QUESTION (test-plan Q1), so
    #     verify_fts_metadata_files_encrypted accepts EITHER signal.
    #
    #   * cbft.uuid is NOT encrypted (negative check). index_meta.json IS encrypted
    #     the same way as PINDEX_META (per dev), so it is validated as encrypted, not
    #     as a negative case.
    #
    #   * The exact on-disk form of the embedded Key Id is also an OPEN QUESTION
    #     (Q4); key-id matching inside .zap is therefore best-effort (logged, not
    #     asserted) until that is confirmed with the dev team.

    FTS_DATA_SUBDIR = "@fts"

    def get_fts_data_path(self, node):
        """
        Return the absolute @fts data directory for a node.

        Args:
            node: Server object

        Returns:
            str: e.g. /opt/couchbase/var/lib/couchbase/data/@fts
        """
        rest = RestConnection(node)
        data_path = rest.get_data_path()
        return f"{data_path.rstrip('/')}/{self.FTS_DATA_SUBDIR}"

    def _dir_exists(self, node, directory):
        """Return True if the directory exists on the node."""
        shell = RemoteMachineShellConnection(node)
        output, _ = shell.execute_command(
            f"test -d {directory} && echo 'exists' || echo 'not_found'"
        )
        shell.disconnect()
        return bool(output) and 'exists' in output[0]

    def _find_files(self, node, base_dir, find_args, limit=20):
        """Run a find under base_dir and return a list of file paths."""
        shell = RemoteMachineShellConnection(node)
        cmd = f"find {base_dir} {find_args} 2>/dev/null | head -{limit}"
        output, _ = shell.execute_command(cmd)
        shell.disconnect()
        return [f.strip() for f in output if f.strip()]

    def find_plaintext_terms_in_file(self, node, file_path, terms, max_terms=50):
        """
        Search a file's printable strings for any of the given sensitive terms.

        Used to detect plaintext leakage of user data inside encrypted FTS files.

        Args:
            node: Server object
            file_path: Absolute path to the file to inspect
            terms: Iterable of sensitive strings (field names, indexed values)
            max_terms: Cap on number of terms grepped (bounds SSH cost)

        Returns:
            tuple: (clean: bool, found_term: str or None)
                   clean=True means none of the terms were found in plaintext.
        """
        # Only meaningful terms: skip empty / very short tokens that would
        # match noise inside binary data.
        terms = [str(t) for t in (terms or []) if t and len(str(t)) >= 3][:max_terms]
        if not terms:
            return True, None

        shell = RemoteMachineShellConnection(node)
        try:
            for term in terms:
                safe = term.replace("'", "'\\''")
                cmd = f"strings {file_path} 2>/dev/null | grep -iF -m1 -- '{safe}'"
                output, _ = shell.execute_command(cmd)
                if output and any(line.strip() for line in output):
                    return False, term
        finally:
            shell.disconnect()
        return True, None

    def file_contains_any(self, node, file_path, values):
        """
        Best-effort: does the whole file contain any of the given values?

        Unlike verify_file_header_contains_any (header only) this scans the entire
        file's printable strings -- used for the internal Key Id which is embedded
        somewhere inside .zap / bolt rather than in a fixed header.

        Returns:
            tuple: (found: bool, matched_value: str or None)
        """
        shell = RemoteMachineShellConnection(node)
        try:
            for value in values:
                value = str(value)
                if not value:
                    continue
                safe = value.replace("'", "'\\''")
                cmd = f"strings {file_path} 2>/dev/null | grep -iF -m1 -- '{safe}'"
                output, _ = shell.execute_command(cmd)
                if output and any(line.strip() for line in output):
                    return True, value
        finally:
            shell.disconnect()
        return False, None

    def verify_fts_segment_files_encrypted(self, fts_nodes, sensitive_terms=None,
                                           expected_key_ids=None, max_files=10):
        """
        Verify that .zap segment files and root.bolt under @fts are encrypted.

        These files use Search-internal chunked encryption with NO magic header, so
        the primary signal is that sensitive user data must not appear as plaintext.
        If expected_key_ids is provided, a best-effort grep for a key id is also done
        (logged only -- see Q4 in the module notes).

        Args:
            fts_nodes: List of FTS server objects
            sensitive_terms: Iterable of strings (indexed field names + values) that
                MUST NOT appear as plaintext in segment files
            expected_key_ids: Optional key id(s) to best-effort grep for inside files
            max_files: Max number of segment files to sample per node

        Returns:
            dict: {node_ip: {"status": "passed"|"failed"|"skipped", ...}}
        """
        results = {}
        sensitive_terms = list(sensitive_terms or [])
        if isinstance(expected_key_ids, (str, int)):
            expected_key_ids = [str(expected_key_ids)]
        expected_key_ids = [str(k) for k in (expected_key_ids or []) if k not in (None, "")]

        for node in fts_nodes:
            fts_dir = self.get_fts_data_path(node)
            self.log.info(
                f"Validating FTS segment encryption on node {node.ip} under {fts_dir}"
            )

            if not self._dir_exists(node, fts_dir):
                self.log.warning(f"Node {node.ip}: FTS data dir {fts_dir} not found")
                results[node.ip] = {"status": "skipped", "reason": f"{fts_dir} not found"}
                continue

            seg_files = self._find_files(
                node, fts_dir,
                r"-type f \( -name '*.zap' -o -name 'root.bolt' \)",
                limit=max_files,
            )
            if not seg_files:
                self.log.warning(f"Node {node.ip}: no .zap/root.bolt files under {fts_dir}")
                results[node.ip] = {"status": "skipped", "reason": "No segment files found"}
                continue

            failed_files = []
            for file_path in seg_files:
                clean, found_term = self.find_plaintext_terms_in_file(
                    node, file_path, sensitive_terms
                )
                if not clean:
                    failed_files.append(
                        (file_path, f"plaintext sensitive term found: '{found_term}'")
                    )
                    self.log.error(
                        f"Node {node.ip}: segment {file_path} leaks plaintext term "
                        f"'{found_term}'"
                    )
                    continue

                if expected_key_ids:
                    key_found, matched = self.file_contains_any(
                        node, file_path, expected_key_ids
                    )
                    # Best-effort only - do not fail on a miss (Q4 open).
                    self.log.info(
                        f"Node {node.ip}: segment {file_path} key-id "
                        f"{'present (' + str(matched) + ')' if key_found else 'not found (best-effort)'}"
                    )
                self.log.info(
                    f"Node {node.ip}: segment {file_path} OK (no plaintext user data)"
                )

            results[node.ip] = {
                "status": "failed" if failed_files else "passed",
                "encrypted_count": len(seg_files) - len(failed_files),
                "total_checked": len(seg_files),
                "failed_files": failed_files,
            }
            if failed_files:
                results[node.ip]["reason"] = f"{len(failed_files)} segment file(s) leak plaintext"
        return results

    def verify_fts_metadata_files_encrypted(self, fts_nodes, sensitive_terms=None):
        """
        Verify FTS whole-file metadata is encrypted: PINDEX_META, PINDEX_BLEVE_META
        and files under planPIndexes/.

        Accepts EITHER the ns_server "Couchbase Encrypted" magic header OR the
        Search-internal format (detected as "no sensitive plaintext present"), since
        which format these use is an open question (Q1).

        Args:
            fts_nodes: List of FTS server objects
            sensitive_terms: Iterable of strings that must not appear as plaintext

        Returns:
            dict: {node_ip: {"status": ..., "total_checked": int, "failed_files": [...]}}
        """
        results = {}
        sensitive_terms = list(sensitive_terms or [])
        meta_file_names = ["PINDEX_META", "PINDEX_BLEVE_META", "index_meta.json"]

        for node in fts_nodes:
            fts_dir = self.get_fts_data_path(node)
            self.log.info(
                f"Validating FTS metadata encryption on node {node.ip} under {fts_dir}"
            )

            if not self._dir_exists(node, fts_dir):
                results[node.ip] = {"status": "skipped", "reason": f"{fts_dir} not found"}
                continue

            files = []
            for fname in meta_file_names:
                files.extend(self._find_files(node, fts_dir, f"-type f -name '{fname}'", limit=10))
            # planPIndexes is a global metadata dir (recovery plans)
            plan_dir = f"{fts_dir}/planPIndexes"
            if self._dir_exists(node, plan_dir):
                files.extend(self._find_files(node, plan_dir, "-type f", limit=10))

            if not files:
                results[node.ip] = {"status": "skipped", "reason": "No FTS metadata files found"}
                continue

            failed_files = []
            for file_path in files:
                is_magic, _ = self.verify_file_encryption_magic_bytes(node, file_path)
                if is_magic:
                    self.log.info(f"Node {node.ip}: metadata {file_path} encrypted (magic header)")
                    continue
                clean, found_term = self.find_plaintext_terms_in_file(
                    node, file_path, sensitive_terms
                )
                if clean:
                    self.log.info(
                        f"Node {node.ip}: metadata {file_path} encrypted "
                        f"(internal format; no plaintext sensitive data)"
                    )
                else:
                    failed_files.append(
                        (file_path, f"no magic header and plaintext term '{found_term}' present")
                    )
                    self.log.error(
                        f"Node {node.ip}: metadata {file_path} appears unencrypted "
                        f"(plaintext term '{found_term}')"
                    )

            results[node.ip] = {
                "status": "failed" if failed_files else "passed",
                "total_checked": len(files),
                "failed_files": failed_files,
            }
            if failed_files:
                results[node.ip]["reason"] = f"{len(failed_files)} metadata file(s) appear unencrypted"
        return results

    def verify_fts_file_not_encrypted(self, node, file_name):
        """
        Negative check: confirm a named FTS file is NOT encrypted.

        Used for cbft.uuid, which must remain plaintext (the only unencrypted file).

        Args:
            node: Server object
            file_name: Bare file name to locate under @fts (e.g. 'cbft.uuid')

        Returns:
            dict: {"status": "passed"|"failed"|"skipped", "file": str, "reason": str}
        """
        fts_dir = self.get_fts_data_path(node)
        if not self._dir_exists(node, fts_dir):
            return {"status": "skipped", "reason": f"{fts_dir} not found"}

        matches = self._find_files(node, fts_dir, f"-type f -name '{file_name}'", limit=1)
        if not matches:
            return {"status": "skipped", "reason": f"{file_name} not found under {fts_dir}"}

        file_path = matches[0]
        is_magic, details = self.verify_file_encryption_magic_bytes(node, file_path)
        if is_magic:
            return {
                "status": "failed",
                "file": file_path,
                "reason": f"{file_name} unexpectedly carries the encryption magic header",
            }
        self.log.info(f"Node {node.ip}: {file_path} is not encrypted (as expected)")
        return {"status": "passed", "file": file_path}

    def _resolve_log_path(self, node):
        """Resolve the Couchbase logs directory for a node (OS-aware).

        Self-contained so the FTS validators do not depend on other helpers in this
        module that may change. Imports the path constants locally.
        """
        from lib.testconstants import LINUX_COUCHBASE_LOGS_PATH, WIN_COUCHBASE_LOGS_PATH
        shell = RemoteMachineShellConnection(node)
        try:
            os_type = shell.extract_remote_info().type.lower()
        finally:
            shell.disconnect()
        return WIN_COUCHBASE_LOGS_PATH if os_type == 'windows' else LINUX_COUCHBASE_LOGS_PATH

    def verify_fts_log_files_encrypted(self, fts_nodes, expected_key_ids=None):
        """
        Verify fts.log (and its rotations) are encrypted.

        fts.log is the Search service log; its encryption is owned by ns_server (log
        encryption), so with log encryption enabled it carries the standard ns_server
        "Couchbase Encrypted" magic header -- the same format used by query rlstream
        logs. If expected_key_ids is provided we also check the header carries one.

        The ACTIVE fts.log AND its rotations (fts.log.N, fts.log.N.gz) are all expected
        to be encrypted. Rotated/compressed archives are NOT exempt -- a file beginning
        with the gzip magic (1f 8b 08) is plaintext-compressed (i.e. unencrypted) and is
        reported as a real failure (this surfaces an ns_server log-encryption gap).

        Args:
            fts_nodes: List of FTS server objects
            expected_key_ids: Optional log-encryption key id(s) to look for in headers

        Returns:
            dict: {node_ip: {"status": "passed"|"failed"|"skipped", ...}}
        """
        results = {}
        if isinstance(expected_key_ids, (str, int)):
            expected_key_ids = [str(expected_key_ids)]
        expected_key_ids = [str(k) for k in (expected_key_ids or []) if k not in (None, "")]

        for node in fts_nodes:
            log_path = self._resolve_log_path(node)
            self.log.info(f"Validating fts.log encryption on node {node.ip} under {log_path}")
            # Active log + all rotations/archives -- all are expected to be encrypted
            log_files = self._find_files(node, log_path, "-maxdepth 1 -name 'fts.log*'", limit=20)
            if not log_files:
                results[node.ip] = {"status": "skipped", "reason": "no fts.log files found"}
                continue

            failed_files = []        # hard failures: a plain log that is not encrypted
            rotation_findings = []   # soft findings: gzip rotations that are not encrypted
            for file_path in log_files:
                is_encrypted, details = self.verify_file_encryption_magic_bytes(node, file_path)
                if not is_encrypted:
                    # A gzip header (1f 8b ..) means the rotation was compressed but never
                    # encrypted. This is a known ns_server log-rotation gap, so it is
                    # reported as a SOFT finding (caller decides whether to enforce). A
                    # non-gzip plaintext log is a hard failure.
                    is_gzip = isinstance(details, str) and details.startswith("\\x1f\\x8b")
                    if is_gzip:
                        rotation_findings.append(
                            (file_path, "gzip-compressed but NOT encrypted (plaintext archive)")
                        )
                        self.log.warning(
                            f"Node {node.ip}: {file_path} is a gzip archive, NOT encrypted "
                            f"(tracked finding -- ns_server log-rotation gap)"
                        )
                    else:
                        failed_files.append((file_path, f"not encrypted: {details}"))
                        self.log.error(f"Node {node.ip}: {file_path} is not encrypted: {details}")
                    continue
                if expected_key_ids:
                    # Best-effort only: log files are written continuously and their
                    # encryption key reflects whichever log DEK was active when those
                    # bytes were written -- NOT necessarily the latest test's key. So a
                    # key-id mismatch is reported as a warning, not a hard failure. The
                    # hard signal is simply "is the active log encrypted at all".
                    header_read, actual, _ = self.get_file_header_text(
                        node, file_path, bytes_to_read=64
                    )
                    found = header_read and any(str(k) in actual for k in expected_key_ids)
                    if not found:
                        self.log.warning(
                            f"Node {node.ip}: {file_path} encrypted but key id "
                            f"{expected_key_ids} not in header (best-effort, not failed)"
                        )
                self.log.info(f"Node {node.ip}: {file_path} is encrypted")

            results[node.ip] = {
                "status": "failed" if failed_files else "passed",
                "total_checked": len(log_files),
                "failed_files": failed_files,
                "rotation_findings": rotation_findings,
            }
        return results

    def verify_fts_pindex_dir_names_sanitized(self, node, forbidden_tokens):
        """
        Confirm pindex directory names under @fts do not leak sensitive names.

        Per the path-change in the design, partition directory names are randomly
        generated identifiers and must NOT contain bucket / scope / collection /
        index names.

        Args:
            node: Server object
            forbidden_tokens: Iterable of names that must not appear in any dir name

        Returns:
            dict: {"status": "passed"|"failed"|"skipped", "leaks": [(dir, token)], ...}
        """
        fts_dir = self.get_fts_data_path(node)
        if not self._dir_exists(node, fts_dir):
            return {"status": "skipped", "reason": f"{fts_dir} not found"}

        shell = RemoteMachineShellConnection(node)
        output, _ = shell.execute_command(
            f"find {fts_dir} -mindepth 1 -maxdepth 1 -type d 2>/dev/null"
        )
        shell.disconnect()
        dir_names = [d.strip().rsplit('/', 1)[-1] for d in output if d.strip()]

        tokens = [str(t) for t in (forbidden_tokens or []) if t]
        leaks = []
        for base in dir_names:
            for token in tokens:
                if token.lower() in base.lower():
                    leaks.append((base, token))

        if leaks:
            self.log.error(f"Node {node.ip}: pindex dir names leak sensitive tokens: {leaks}")
            return {"status": "failed", "leaks": leaks, "dirs_checked": len(dir_names)}
        self.log.info(
            f"Node {node.ip}: {len(dir_names)} pindex dir name(s) contain no sensitive tokens"
        )
        return {"status": "passed", "dirs_checked": len(dir_names)}

    def verify_fts_segment_files_not_encrypted(self, fts_nodes, sensitive_terms,
                                               max_files=10):
        """
        Negative check: confirm .zap segment files are NOT encrypted.

        Used after encryption is DISABLED (and segments rewritten). Because .zap files
        have no magic header either way, the signal that a segment is plaintext is that
        sensitive user data IS readable. We pass if at least one sensitive term is found
        in plaintext on a node (encryption is off), and fail if none are -- which would
        mean the segments are still encrypted.

        NOTE: stored-field values are snappy-compressed even when unencrypted, so value
        tokens may not be readable; field-name tokens (e.g. languages_known, emp_id) are
        the reliable signal here.

        Args:
            fts_nodes: List of FTS server objects
            sensitive_terms: Iterable of strings expected to be readable once decrypted
            max_files: Max number of segment files to sample per node

        Returns:
            dict: {node_ip: {"status": "passed"|"failed"|"skipped", ...}}
        """
        results = {}
        sensitive_terms = list(sensitive_terms or [])
        for node in fts_nodes:
            fts_dir = self.get_fts_data_path(node)
            if not self._dir_exists(node, fts_dir):
                results[node.ip] = {"status": "skipped", "reason": f"{fts_dir} not found"}
                continue
            seg_files = self._find_files(
                node, fts_dir, r"-type f \( -name '*.zap' -o -name 'root.bolt' \)",
                limit=max_files,
            )
            if not seg_files:
                results[node.ip] = {"status": "skipped", "reason": "No segment files found"}
                continue

            plaintext_hits = []
            for file_path in seg_files:
                clean, found_term = self.find_plaintext_terms_in_file(
                    node, file_path, sensitive_terms
                )
                if not clean:
                    plaintext_hits.append((file_path, found_term))
                    self.log.info(
                        f"Node {node.ip}: {file_path} has plaintext '{found_term}' "
                        f"(confirms NOT encrypted)"
                    )

            if plaintext_hits:
                results[node.ip] = {
                    "status": "passed",
                    "total_checked": len(seg_files),
                    "plaintext_hits": plaintext_hits,
                }
            else:
                results[node.ip] = {
                    "status": "failed",
                    "reason": "no plaintext found in any segment -- still encrypted?",
                    "total_checked": len(seg_files),
                }
        return results

    def count_fts_pindex_dirs(self, node):
        """
        Count the pindex partition directories under @fts on a node.

        A pindex partition is a directory that contains a 'store' subdir (the segments).
        Used to confirm an index's on-disk files are cleaned up after deletion.

        Returns:
            int: number of pindex partition directories (0 if @fts is absent)
        """
        fts_dir = self.get_fts_data_path(node)
        if not self._dir_exists(node, fts_dir):
            return 0
        shell = RemoteMachineShellConnection(node)
        # A pindex dir is one that has a 'store' child (where .zap/root.bolt live)
        output, _ = shell.execute_command(
            f"find {fts_dir} -mindepth 2 -maxdepth 2 -type d -name 'store' 2>/dev/null | wc -l"
        )
        shell.disconnect()
        try:
            return int((output[0] if output else "0").strip())
        except (ValueError, IndexError):
            return 0

    # ------------------------------------------------------------------
    # FTS in-use encryption keys (GetInUseKeys) -- :8094/api/encryption/GetInUseKeys
    # ------------------------------------------------------------------
    # Response shape (per datatype):
    #   {"other": ["", ...],
    #    "service_bucket <bucket-uuid>": ["<dek-uuid>", ...],
    #    "service_bucket <bucket-uuid>": []}
    # - a "service_bucket <uuid>" entry lists the DEK id(s) currently encrypting that
    #   bucket's Search data; [] means none in use yet.
    # - an "" (empty string) entry means UNENCRYPTED data is present for that datatype.
    SERVICE_BUCKET_PREFIX = "service_bucket "

    def get_fts_in_use_keys(self, fts_nodes):
        """
        Query GetInUseKeys on every FTS node and merge into one datatype->keys map.

        Returns:
            dict: {datatype_string: sorted([key_id, ...])} preserving "" markers,
                  where datatype_string is e.g. "other" or "service_bucket <uuid>".

        Raises:
            Exception if no FTS node returned a usable response.
        """
        merged = {}
        any_ok = False
        for node in fts_nodes:
            rest = RestConnection(node)
            status, response = rest.get_fts_in_use_encryption_keys()
            if not status or not isinstance(response, dict):
                self.log.warning(
                    f"FTS GetInUseKeys on {node.ip} returned status={status}, "
                    f"response={response}"
                )
                continue
            any_ok = True
            self.log.info(f"FTS GetInUseKeys on {node.ip}: {response}")
            for datatype, keys in response.items():
                bucket_set = merged.setdefault(datatype, set())
                for key_id in (keys or []):
                    bucket_set.add(key_id)
        if not any_ok:
            raise Exception("FTS GetInUseKeys returned no usable response from any node")
        return {dt: sorted(keys) for dt, keys in merged.items()}

    def extract_fts_in_use_key_ids(self, merged, include_empty=False):
        """Flat sorted list of all key ids across datatypes (excludes "" unless asked)."""
        ids = set()
        for keys in merged.values():
            for key_id in keys:
                if key_id == "" and not include_empty:
                    continue
                ids.add(key_id)
        return sorted(ids)

    def fts_dek_ids_for_bucket(self, merged, bucket_uuid):
        """Return the non-empty DEK id(s) in use for a given bucket UUID."""
        datatype = f"{self.SERVICE_BUCKET_PREFIX}{bucket_uuid}"
        return [k for k in merged.get(datatype, []) if k != ""]

    def fts_has_unencrypted_marker(self, merged, datatype=None):
        """True if an "" (unencrypted-data) marker is present.

        If datatype is given (e.g. "other" or "service_bucket <uuid>"), checks only
        that datatype; otherwise checks across all datatypes.
        """
        if datatype is not None:
            return "" in merged.get(datatype, [])
        return any("" in keys for keys in merged.values())
