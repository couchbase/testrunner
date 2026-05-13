"""
encryption_at_rest_helper.py: Helper utilities for Encryption at Rest validation

Provides common methods for validating file encryption on disk for Couchbase indexes.
Used by GSI encryption at rest tests and can be reused by other components.

File types validated:
- Storage files (BHIVE/Plasma/MOI) under @2i directory
- Index data files (*.data, *.codebook, *.fdb)
- Log files (stats.log)
- Metadata files (config.json, shard.json, wal.*)

__author__ = "Pavan PB"
__maintainer__ = "Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"
__created_on__ = "16/04/26"
"""

from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import LINUX_COUCHBASE_LOGS_PATH, WIN_COUCHBASE_LOGS_PATH
from membase.api.rest_client import RestConnection


class EncryptionAtRestHelper:
    """
    Helper class for validating encryption at rest on Couchbase index files.
    
    Provides methods to:
    - Verify encryption magic bytes in file headers
    - Check for plaintext exposure in files
    - Validate storage file encryption across different storage types
    - Validate log file encryption
    """
    
    # Encryption magic bytes header for Couchbase encrypted files
    ENCRYPTION_MAGIC_BYTES = "Couchbase Encrypted"
    
    def __init__(self, log):
        """
        Initialize the helper with a logger.
        
        Args:
            log: Logger instance for logging messages
        """
        self.log = log
    
    def get_log_path(self, node):
        """
        Get the log path for a node based on OS type.
        
        Args:
            node: Server object
            
        Returns:
            str: Log directory path
        """
        shell = RemoteMachineShellConnection(node)
        os_type = shell.extract_remote_info().type.lower()
        shell.disconnect()
        
        if os_type == 'windows':
            return WIN_COUCHBASE_LOGS_PATH
        return LINUX_COUCHBASE_LOGS_PATH
    
    def verify_file_encryption_magic_bytes(self, node, file_path):
        """
        Verify magic bytes at start of encrypted file.
        
        Encrypted files have a leading null byte before the magic string
        "Couchbase Encrypted". Uses xxd to safely read binary header as hex,
        then converts to ASCII for comparison.
        
        Args:
            node: Server object to check
            file_path: Absolute path to file to verify
            
        Returns:
            tuple: (is_encrypted: bool, details: str)
        """
        header_read, actual, printable = self.get_file_header_text(
            node, file_path, bytes_to_read=64
        )
        if header_read:
            if self.ENCRYPTION_MAGIC_BYTES in actual:
                return True, self.ENCRYPTION_MAGIC_BYTES
            return False, printable
        return False, printable

    def get_file_header_text(self, node, file_path, bytes_to_read=512):
        """
        Read a file header and return decoded and printable variants.

        Args:
            node: Server object to check
            file_path: Absolute path to file to inspect
            bytes_to_read: Number of header bytes to read

        Returns:
            tuple: (header_read: bool, decoded_header: str, printable_header: str)
        """
        shell = RemoteMachineShellConnection(node)
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
            # Build printable from raw bytes: keep printable ASCII as-is,
            # represent everything else as \xNN so the output is always readable
            # (avoids Unicode replacement chars and control characters in log lines)
            printable = ''.join(
                chr(b) if 32 <= b < 127 else f'\\x{b:02x}'
                for b in raw_bytes
            )
            return True, actual, printable
        except (ValueError, UnicodeDecodeError) as e:
            return False, "", f"hex parse error: {str(e)}"

    def verify_file_header_contains(self, node, file_path, expected_text, bytes_to_read=512):
        """
        Verify that the expected text is present in the file header.

        Args:
            node: Server object to check
            file_path: Absolute path to file to inspect
            expected_text: Text expected in the file header
            bytes_to_read: Number of header bytes to inspect

        Returns:
            tuple: (contains_text: bool, details: str)
        """
        header_read, actual, printable = self.get_file_header_text(
            node, file_path, bytes_to_read=bytes_to_read
        )
        if not header_read:
            return False, printable
        expected_text = str(expected_text)
        if expected_text in actual:
            return True, expected_text
        return False, printable

    def verify_file_header_contains_any(self, node, file_path, expected_values, bytes_to_read=512):
        """
        Verify that any expected value is present in the file header.

        Args:
            node: Server object to check
            file_path: Absolute path to file to inspect
            expected_values: Iterable of texts expected in the file header
            bytes_to_read: Number of header bytes to inspect

        Returns:
            tuple: (contains_any: bool, matched_value/details: str)
        """
        header_read, actual, printable = self.get_file_header_text(
            node, file_path, bytes_to_read=bytes_to_read
        )
        if not header_read:
            return False, printable

        for expected_value in expected_values:
            expected_value = str(expected_value)
            if expected_value and expected_value in actual:
                return True, expected_value
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
