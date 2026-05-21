
"""GSI-focused encryption-at-rest helpers extracted from the shared EAR facade."""

import json
import re
import shlex

from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import LINUX_COUCHBASE_LOGS_PATH, WIN_COUCHBASE_LOGS_PATH
from membase.api.rest_client import RestConnection


class GSIEncryptionHelpers:
    ENCRYPTION_MAGIC_BYTES = "Couchbase Encrypted"
    SNAPSHOT_PLAINTEXT_FILES = {
        "nitro.json",
        "data/checksums.json",
        "data/files.json",
        "delta/checksums.json",
        "delta/files.json",
    }
    _SNAPSHOT_PLAINTEXT_BASENAMES = frozenset(
        p.split("/")[-1] for p in SNAPSHOT_PLAINTEXT_FILES
    )
    SNAPSHOT_ENCRYPTED_PATTERNS = (
        "manifest.json",
        "data/shard-*",
        "delta/shard-*",
    )
    _LEAK_MIN_TOKEN_LEN = 6
    _LEAK_MAX_TOKENS = 25
    LEAKAGE_CATEGORIES = (
        "snapshot",
        "data",
        "error",
        "codebook",
        "request_handler_cache",
        "stats_logs",
    )
    STORAGE_STORE_NAMES = ("MainStore", "BackStore")
    STORAGE_CRYPT_COUNTERS = (
        "lss_blk_written_crypt",
        "recovery_lss_blk_written_crypt",
        "lss_blk_read_bs_crypt",
        "recovery_lss_blk_read_bs_crypt",
    )
    _IDENT_RE = re.compile(r"`([^`]+)`|([A-Za-z_][A-Za-z0-9_]*)")
    _NAME_BLOCKLIST = {"", "default", "_default", "meta", "id", "self"}


    def __init__(self, log):
        self.log = log

    def get_log_path(self, node):
        """
        Get the log path for a node based on OS type.
        
        Args:
            node: Server object
            
        Returns:
            str: Log directory path
        """
        shell = RemoteMachineShellConnection(node, verbose=False)
        os_type = shell.extract_remote_info().type.lower()
        shell.disconnect()
        
        if os_type == 'windows':
            return WIN_COUCHBASE_LOGS_PATH
        return LINUX_COUCHBASE_LOGS_PATH

    def _log_directory_tree(self, node, root_dir, maxdepth=4, cap=60):
        """Log the filesystem tree under root_dir for diagnostics."""
        shell = RemoteMachineShellConnection(node, verbose=False)
        out, _ = shell.execute_command(
            f"find {root_dir} -maxdepth {maxdepth} 2>/dev/null | head -{cap}"
        )
        shell.disconnect()
        lines = [l.strip() for l in out if l.strip()]
        self.log.info(
            f"Node {node.ip}: [{root_dir}] tree "
            f"({len(lines)} entries, maxdepth={maxdepth}, capped at {cap}):\n"
            + "\n".join(lines)
        )

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
        shell = RemoteMachineShellConnection(node, verbose=False)
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
        shell = RemoteMachineShellConnection(node, verbose=False)
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
            
            shell = RemoteMachineShellConnection(node, verbose=False)
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
            shell = RemoteMachineShellConnection(node, verbose=False)
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
            shell = RemoteMachineShellConnection(node, verbose=False)
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
                shell = RemoteMachineShellConnection(node, verbose=False)
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
    @staticmethod

    def _snapshot_relative(file_path, snapshot_dir):
        """Return the path of file_path relative to snapshot_dir, slash-style."""
        prefix = snapshot_dir.rstrip("/") + "/"
        return file_path[len(prefix):] if file_path.startswith(prefix) else file_path
    @staticmethod

    def _matches_snapshot_allowlist(rel_path):
        """True if rel_path matches any SNAPSHOT_ENCRYPTED_PATTERNS glob."""
        from fnmatch import fnmatch
        return any(fnmatch(rel_path, p)
                   for p in EncryptionAtRestHelper.SNAPSHOT_ENCRYPTED_PATTERNS)

    def verify_gsi_snapshot_files_encrypted(self, index_nodes, expected_key_id=None,
                                            encrypted_bucket_names=None,
                                            bucket_key_map=None,
                                            metadata_inst_ids=None,
                                            per_node_bucket_key_map=None):
        """
        Verify every file in every snapshot folder is encrypted (with the right key).

        Classifies every file under ``snapshot.<ts>/`` into one of three sets:
          * **allowlist** — ``SNAPSHOT_ENCRYPTED_PATTERNS`` (``manifest.json``,
            ``data/shard-*``, ``delta/shard-*``). Canonical files that must be
            present and encrypted.
          * **plaintext** — ``SNAPSHOT_PLAINTEXT_FILES``. Skipped (must NOT be
            encrypted by design).
          * **extras** — any other regular file in the folder (e.g. temp /
            staging files written during snapshot creation). Must also be
            encrypted; reported under ``extras_checked`` / ``extras_failed``
            so a regression here is visible in the result dict.

        For each index node:
          * find ``@2i/*.index/snapshot.<ts>/`` directories,
          * list ALL regular files under each,
          * classify per the rules above,
          * header-check the allowlist and extras for the ``Couchbase
            Encrypted`` magic and an acceptable key ID.

        Key-ID validation uses ``bucket_key_map`` when provided — each file is
        attributed to its bucket from the index-dir prefix
        (``/<bucket>_..._.index/``) and only that bucket's keys are accepted.
        When ``bucket_key_map`` is empty/None, falls back to the flat
        ``expected_key_id`` list.

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Flat key ID(s) used when bucket_key_map is absent.
            encrypted_bucket_names: Optional bucket-name filter.
            bucket_key_map: Optional ``{bucket: [key_ids]}`` produced by
                :meth:`build_bucket_key_map`.

        Returns:
            dict: ``{node_ip: {"status": str, "encrypted_count": int,
                                "total_checked": int, "failed_files": list}}``
        """
        results = {}
        expected_key_ids = self._normalize_key_ids(expected_key_id)
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(f"Validating snapshot folder file encryption on node {node.ip}")
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"

            shell = RemoteMachineShellConnection(node, verbose=False)
            snap_files_out, _ = shell.execute_command(
                f"find {storage_dir} -path '*snapshot*' 2>/dev/null"
            )
            shell.disconnect()
            snap_files = [l.strip() for l in snap_files_out if l.strip()]
            self.log.info(
                f"Node {node.ip}: snapshot paths under {storage_dir} "
                f"({len(snap_files)} entries):\n" + "\n".join(snap_files)
            )

            shell = RemoteMachineShellConnection(node, verbose=False)
            exists_out, _ = shell.execute_command(
                f"test -d {storage_dir} && echo 'exists' || echo 'not_found'"
            )
            shell.disconnect()
            if not exists_out or 'exists' not in exists_out[0]:
                self.log.warning(f"Node {node.ip}: {storage_dir} not found")
                results[node.ip] = {"status": "skipped",
                                    "reason": f"{storage_dir} not found"}
                continue

            shell = RemoteMachineShellConnection(node, verbose=False)
            output, _ = shell.execute_command(
                f"find {storage_dir} -type d -path '*/*.index/snapshot.*' "
                f"2>/dev/null | head -20"
            )
            shell.disconnect()

            snapshot_dirs = [d.strip() for d in output if d.strip()]
            if encrypted_bucket_names and bucket_key_map:
                snapshot_dirs = self._bucket_uuid_prefix_filter(
                    snapshot_dirs, bucket_key_map
                )

            # Exclude snapshot directories belonging to internal (metadata)
            # indexes created by Couchbase (_system scope / _query collection).
            if metadata_inst_ids:
                snapshot_dirs, excluded = self._filter_metadata_index_paths(
                    snapshot_dirs, metadata_inst_ids
                )
                if excluded:
                    self.log.info(
                        f"Node {node.ip}: excluded {excluded} snapshot dir(s) "
                        "belonging to internal metadata indexes"
                    )

            if not snapshot_dirs:
                reason = ("no snapshot directories found"
                          + (f" for buckets {encrypted_bucket_names}"
                             if encrypted_bucket_names else ""))
                self.log.warning(f"Node {node.ip}: {reason}")
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue

            self.log.debug(
                f"Node {node.ip}: found {len(snapshot_dirs)} snapshot directories"
            )

            # Classify every file under each snapshot dir into one of three
            # sets: allowlist (canonical, must be encrypted), plaintext
            # (skipped), or extras (anything else, also must be encrypted).
            # Plaintext files (checksums.json, files.json, nitro.json) are
            # excluded at the find(1) level so they never reach _check().
            # A post-loop relative-path check is kept as a belt-and-braces
            # fallback, but the find exclusion is the primary guard — it avoids
            # false warnings when path normalisation differs between the two
            # separate shell calls used to discover snapshot dirs vs. their
            # contents.
            plaintext_excl = " ".join(
                f"! -name '{n}'"
                for n in self._SNAPSHOT_PLAINTEXT_BASENAMES
            )
            allowlist_files = []
            extra_files = []
            skipped_plaintext = 0
            for snapshot_dir in snapshot_dirs:
                shell = RemoteMachineShellConnection(node, verbose=False)
                file_out, _ = shell.execute_command(
                    f"find {snapshot_dir} -type f {plaintext_excl} 2>/dev/null"
                )
                shell.disconnect()

                for line in file_out:
                    file_path = line.strip()
                    if not file_path:
                        continue
                    rel = self._snapshot_relative(file_path, snapshot_dir)
                    if rel in self.SNAPSHOT_PLAINTEXT_FILES:
                        # Belt-and-braces: should already be excluded by find.
                        skipped_plaintext += 1
                        continue
                    if self._matches_snapshot_allowlist(rel):
                        allowlist_files.append(file_path)
                    else:
                        extra_files.append(file_path)

            if not allowlist_files and not extra_files:
                self.log.warning(
                    f"Node {node.ip}: no encryptable files found "
                    f"(skipped {skipped_plaintext} plaintext entries)"
                )
                results[node.ip] = {"status": "skipped",
                                    "reason": "no encryptable snapshot files"}
                continue

            self.log.debug(
                f"Node {node.ip}: snapshot file classification — "
                f"allowlist={len(allowlist_files)}, extras={len(extra_files)}, "
                f"plaintext_excluded={skipped_plaintext}"
            )
            if extra_files:
                self.log.debug(
                    f"Node {node.ip}: extra (non-allowlist) snapshot files "
                    f"will also be checked for encryption: "
                    f"{extra_files[:5]}{'...' if len(extra_files) > 5 else ''}"
                )

            # Check allowlist first, then extras. Both must be encrypted; the
            # result dict tracks them separately so a regression in extras
            # (e.g. a new temp-file type appearing unencrypted) is obvious.
            def _check(files, label):
                encrypted = 0
                failed = []
                for file_path in files:
                    allowed_keys = self._allowed_keys_for_path(
                        file_path, effective_bkm, expected_key_ids
                    )
                    is_encrypted, details = self.verify_file_encryption_magic_bytes(
                        node, file_path
                    )
                    if not is_encrypted:
                        failed.append((file_path, details))
                        self.log.warning(
                            f"Node {node.ip}: [{label}] {file_path} NOT encrypted: {details}"
                        )
                        continue
                    if allowed_keys:
                        found, matched = self.verify_file_header_contains_any(
                            node, file_path, allowed_keys
                        )
                        if not found:
                            failed.append(
                                (file_path,
                                 f"header missing any of allowed key ids {allowed_keys}")
                            )
                            self.log.warning(
                                f"Node {node.ip}: [{label}] {file_path} no allowed "
                                f"key id from {allowed_keys}"
                            )
                            continue
                        self.log.debug(
                            f"Node {node.ip}: [{label}] {file_path} encrypted with key {matched}"
                        )
                    else:
                        self.log.debug(f"Node {node.ip}: [{label}] {file_path} encrypted")
                    encrypted += 1
                return encrypted, failed

            allow_sample = allowlist_files[:50]
            extra_sample = extra_files[:50]
            allow_passed, allow_failed = _check(allow_sample, "allowlist")
            extra_passed, extra_failed = _check(extra_sample, "extras")

            failed_files = allow_failed + extra_failed
            total_passed = allow_passed + extra_passed
            status = "failed" if failed_files else ("passed" if total_passed else "failed")

            results[node.ip] = {
                "status": status,
                "encrypted_count": total_passed,
                "total_checked": len(allow_sample) + len(extra_sample),
                "allowlist_checked": len(allow_sample),
                "allowlist_passed": allow_passed,
                "extras_checked": len(extra_sample),
                "extras_passed": extra_passed,
                "extras_total_found": len(extra_files),
                "failed_files": failed_files,
            }

        return results
    @staticmethod

    def _normalize_key_ids(expected_key_id):
        """
        Coerce a key-ID input into a clean list of strings.

        Accepts None, "", a single value, or any iterable; drops blanks and
        stringifies the survivors. Lets every verifier accept the same
        ``expected_key_id`` argument shape without per-caller branching.

        Args:
            expected_key_id: None, str, int, or iterable of those.

        Returns:
            list[str]: De-blanked, stringified key IDs (possibly empty).
        """
        if expected_key_id in (None, ""):
            return []
        if isinstance(expected_key_id, (list, tuple, set)):
            return [str(v) for v in expected_key_id if v not in (None, "")]
        return [str(expected_key_id)]

    def build_bucket_key_map(self, rest, bucket_names):
        """
        Build ``{bucket_name: {"uuid": str, "key_ids": [str]}}`` from REST.

        Combines two sources on the same indexer node:
          1. ``GET /pools/default/buckets/<bucket>`` → bucket UUID
             (via :meth:`RestConnection.get_bucket_json`).
          2. ``GET <indexer>:9102/encryption/GetInUseKeys`` → the keys-in-use
             dict, whose per-bucket entries are keyed
             ``"{service_bucket <uuid>}"`` with value = list of key IDs
             currently used by that bucket.

        The bucket UUID is what actually appears on disk: index directories
        are now named ``@2i/<bucket-uuid>_<index-def-id>_<partition-id>.index``
        (and the BHIVE variant ``@2i/@bhive/<bucket-uuid>_..._.index``). The
        UUID is therefore the only reliable way to attribute a file path to a
        bucket; the bucket *name* never appears in any on-disk path.

        Buckets without a matching keysInUse entry resolve to an empty
        ``key_ids`` list; buckets whose ``get_bucket_json`` fails resolve to
        ``uuid=""`` and an empty ``key_ids`` list (caller can detect via the
        empty UUID).

        Args:
            rest: ``RestConnection`` to ANY index node — keysInUse and
                bucket info are both cluster-visible from any node.
            bucket_names: List of bucket names to resolve.

        Returns:
            dict: ``{bucket_name: {"uuid": str, "key_ids": list[str]}}``.
                Empty dict when ``bucket_names`` is empty or the keysInUse
                request fails.
        """
        if not bucket_names:
            return {}

        try:
            status, keys_resp = rest.get_indexer_in_use_encryption_keys()
        except Exception as exc:
            self.log.warning(f"GetInUseKeys request failed: {exc}")
            return {}
        if not status or not isinstance(keys_resp, dict):
            self.log.warning(f"GetInUseKeys returned non-dict: {keys_resp!r}")
            return {}

        # Build {bucket_uuid: [key_ids]} from the response.
        uuid_to_keys = {}
        for raw_key, key_ids in keys_resp.items():
            match = self._BUCKET_KEY_TAG_RE.match(raw_key.strip())
            if not match:
                continue
            uuid_to_keys[match.group(1).lower()] = [
                str(k) for k in (key_ids or []) if k
            ]

        bucket_key_map = {}
        for bucket in bucket_names:
            try:
                bucket_json = rest.get_bucket_json(bucket=bucket)
            except Exception as exc:
                self.log.warning(f"get_bucket_json({bucket}) failed: {exc}")
                bucket_key_map[bucket] = {"uuid": "", "key_ids": []}
                continue
            uuid = (bucket_json.get("uuid") or "").lower()
            keys = uuid_to_keys.get(uuid, [])
            if not keys:
                self.log.info(
                    f"Bucket {bucket} (uuid={uuid}) has no in-use indexer keys"
                )
            bucket_key_map[bucket] = {"uuid": uuid, "key_ids": keys}

        self.log.debug(f"Bucket->{{uuid,key_ids}} map built: {bucket_key_map}")
        return bucket_key_map

    def build_per_node_bucket_key_map(self, index_nodes, bucket_names):
        """Build a per-node bucket key map by querying each node separately.

        Each indexer node has its own set of encryption keys. When validating
        files on node X, key-ID resolution must use keys from node X's own
        ``GetInUseKeys`` endpoint, not another node's.

        Args:
            index_nodes: List of index server objects.
            bucket_names: List of bucket names to resolve.

        Returns:
            dict: ``{node_ip: {bucket_name: {"uuid": str, "key_ids": [str]}}}``
        """
        per_node = {}
        for node in index_nodes:
            rest = RestConnection(node)
            try:
                per_node[node.ip] = self.build_bucket_key_map(rest, bucket_names)
            except Exception as exc:
                self.log.warning(
                    f"build_bucket_key_map for node {node.ip} failed: {exc!r} "
                    "— using empty key map for this node"
                )
                per_node[node.ip] = {}
        return per_node
    @staticmethod

    def _bucket_for_path(file_path, bucket_key_map):
        """
        Return the bucket name that owns ``file_path``, or None.

        Index directories are named ``@2i/<bucket-uuid>_<def-id>_<partition>.index``
        (and ``@2i/@bhive/<bucket-uuid>_..._.index`` for BHIVE), so we match
        on the substring ``/<uuid>_`` anchored on the slash before and the
        underscore after. Empty UUIDs (``get_bucket_json`` failed) are skipped.

        Args:
            file_path: Absolute on-disk path.
            bucket_key_map: Output of :meth:`build_bucket_key_map`
                — ``{name: {"uuid": str, "key_ids": [str]}}``.

        Returns:
            str | None: Matching bucket name, or None if no UUID matched.
        """
        if not bucket_key_map:
            return None
        for name, info in bucket_key_map.items():
            uuid = (info or {}).get("uuid")
            if uuid and f"/{uuid}_" in file_path:
                return name
        return None

    def _allowed_keys_for_path(self, file_path, bucket_key_map, fallback_keys):
        """
        Resolve the list of acceptable key IDs for a given file path.

        Walks: bucket-map lookup (via UUID prefix) → flat fallback. Returns
        ``[]`` if neither source yields keys, which tells the caller to skip
        the key-ID check (the encryption header check still runs).

        Callers pass the node-specific ``bucket_key_map`` (built from that
        node's own ``GetInUseKeys`` endpoint via
        :meth:`build_per_node_bucket_key_map`) so the keys resolved here
        are always the correct keys for the node whose files are being checked.

        Args:
            file_path: Absolute on-disk path.
            bucket_key_map: ``{name: {"uuid", "key_ids"}}``; may be empty.
                Should be the node-specific map, not the shared cluster map.
            fallback_keys: Flat list of key IDs to use when the map doesn't
                yield anything for this file's bucket.

        Returns:
            list[str]: Allowed key IDs (possibly empty).
        """
        if bucket_key_map:
            bucket = self._bucket_for_path(file_path, bucket_key_map)
            if bucket:
                keys = bucket_key_map[bucket].get("key_ids") or []
                if keys:
                    return keys
        return list(fallback_keys or [])
    @staticmethod

    def _bucket_uuid_prefix_filter(paths, bucket_key_map):
        """
        Keep only paths that belong to one of the bucket UUIDs in the map.

        Used to skip work for buckets whose indexes happen to live in @2i but
        aren't part of the encrypted set under test. Matches on ``/<uuid>_``
        the same way :meth:`_bucket_for_path` does. Buckets with an empty
        UUID are ignored.

        Args:
            paths: Iterable of absolute on-disk paths.
            bucket_key_map: Output of :meth:`build_bucket_key_map`.

        Returns:
            list[str]: Filtered paths (input order preserved). Falls back to
                the input unchanged when the map is empty (no filter).
        """
        if not bucket_key_map:
            return list(paths)
        prefixes = tuple(
            f"/{info['uuid']}_"
            for info in bucket_key_map.values()
            if info and info.get("uuid")
        )
        if not prefixes:
            return list(paths)
        return [p for p in paths if any(prefix in p for prefix in prefixes)]

    def _build_metadata_inst_id_set(self, rest):
        """Return the set of instId strings for metadata (internal) indexes.

        Queries the indexer ``/getIndexStatus`` endpoint and collects the
        ``instId`` of every index whose ``scope`` is ``_system`` or whose
        ``collection`` is ``_query``.  These directories are created
        automatically by Couchbase (e.g. system-scoped query-metadata indexes)
        and must be excluded from encryption validation so the test only
        validates indexes it created itself.

        File-path format: ``<BucketUUID>_<instId>_<PartitionID>.index``
        Exclusion pattern: ``_<instId>_`` is present in the path.

        Args:
            rest: :class:`RestConnection` pointed at an index node (port 9102).

        Returns:
            set[str]: instId strings for metadata indexes.  Returns the empty
                set on failure so callers silently skip the filtering.
        """
        api = rest.index_baseUrl + 'getIndexStatus'
        status, content, _ = rest.urllib_request(api, timeout=60)
        if not status:
            self.log.warning(
                "getIndexStatus request failed — "
                "metadata index exclusion list will be empty"
            )
            return set()
        try:
            parsed = (json.loads(content)
                      if isinstance(content, (str, bytes)) else content)
        except (TypeError, ValueError) as exc:
            self.log.warning(
                f"getIndexStatus parse failed: {exc} — "
                "metadata index exclusion list will be empty"
            )
            return set()

        inst_ids = set()
        for entry in parsed.get("status", []):
            if (entry.get("scope") == "_system"
                    or entry.get("collection") == "_query"):
                inst_id = entry.get("instId")
                if inst_id is not None:
                    inst_ids.add(str(inst_id))

        self.log.info(
            f"Metadata index exclusion: {len(inst_ids)} internal instId(s) "
            "will be skipped in encryption validation"
        )
        self.log.debug(f"Excluded metadata instIds: {inst_ids}")
        return inst_ids
    @staticmethod

    def _filter_metadata_index_paths(paths, metadata_inst_ids):
        """Remove paths whose ``.index`` directory belongs to a metadata index.

        A path is considered to belong to a metadata index when the substring
        ``_<instId>_`` is present anywhere in the path.  This reliably
        identifies the instId segment in the ``<BucketUUID>_<instId>_<PartID>``
        directory name pattern without requiring regex parsing.

        Args:
            paths: Iterable of absolute on-disk path strings.
            metadata_inst_ids: Set of instId strings returned by
                :meth:`_build_metadata_inst_id_set`.

        Returns:
            tuple[list[str], int]: (filtered_paths, excluded_count)
        """
        if not metadata_inst_ids:
            return list(paths), 0
        markers = {f"_{inst_id}_" for inst_id in metadata_inst_ids}
        filtered = []
        excluded = 0
        for path in paths:
            if any(marker in path for marker in markers):
                excluded += 1
            else:
                filtered.append(path)
        return filtered, excluded

    def verify_file_contains_key_id(self, node, file_path, key_ids):
        """
        Check whether any of the given key IDs appears anywhere in the file body.

        Uses `grep -aF` so binary content is treated as text with a fixed-string
        match. Intended for files whose header does NOT carry the key ID (Plasma
        and BHIVE .data files).

        Returns:
            tuple: (found: bool, matched_key_id: str|None, details: str)
        """
        key_ids = [str(k) for k in key_ids if k not in (None, "")]
        if not key_ids:
            return False, None, "no key ids supplied"

        shell = RemoteMachineShellConnection(node, verbose=False)
        try:
            for key_id in key_ids:
                cmd = f"grep -aFl -e {shlex.quote(key_id)} {shlex.quote(file_path)} 2>/dev/null"
                output, _ = shell.execute_command(cmd)
                if output and any(line.strip() for line in output):
                    return True, key_id, f"matched {key_id}"
            return False, None, f"none of {key_ids} present in file body"
        finally:
            shell.disconnect()

    def verify_gsi_data_files_key_id(self, index_nodes, expected_key_id=None,
                                     encrypted_bucket_names=None,
                                     bucket_key_map=None,
                                     metadata_inst_ids=None,
                                     per_node_bucket_key_map=None):
        """
        Verify Plasma/BHIVE ``.data`` files contain an expected key ID.

        These files do NOT carry the encryption magic header — the key ID is
        embedded inside the file body and is the only signal that encryption
        is in effect. For each index node, walks
        ``@2i/*.index/{docIndex,mainIndex,keyindex}/`` and grep-checks every
        ``*.data`` file (capped at 30 per node to keep run time bounded).

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Single key ID, list of key IDs, or None. When
                empty, the verifier short-circuits to ``status=skipped``
                because there is nothing meaningful to assert.
            encrypted_bucket_names: Optional list of bucket names to restrict
                the sweep to. Useful when only some buckets in the cluster
                have encryption enabled.

        Returns:
            dict: ``{node_ip: {"status": "passed"|"failed"|"skipped",
                                "passed_count": int,
                                "total_checked": int,
                                "failed_files": list[(path, details)]}}``
        """
        results = {}
        fallback_keys = self._normalize_key_ids(expected_key_id)
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        has_map_keys = any(
            info.get("key_ids") for info in bucket_key_map.values()
        )
        if not fallback_keys and not has_map_keys:
            self.log.info("verify_gsi_data_files_key_id: no key ids known, skipping")
            return {node.ip: {"status": "skipped",
                              "reason": "no key ids supplied"}
                    for node in index_nodes}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(f"Validating Plasma/BHIVE data files on node {node.ip}")
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"

            shell = RemoteMachineShellConnection(node, verbose=False)
            # Include top-level *.data and recovery/*.data for each sub-index.
            # Excludes *.json (config.json, lss.json, etc.) automatically since
            # the pattern requires the .data suffix.
            #
            # TODO: header.data is excluded below because in the current build
            #   it is written once at index-creation time and is NOT re-encrypted
            #   when bucket encryption is enabled on an already-existing index.
            #   As a result the key ID is absent from its body even on an
            #   otherwise correctly encrypted index.  Remove the exclusion once
            #   the indexer team confirms/fixes the expected behaviour for
            #   header.data (re-written on encrypt, or exempt from key-ID check).
            cmd = (
                f"find {storage_dir} -type f "
                f"! -name 'header.data' "
                f"\\( "
                f"-path '*/*.index/docIndex/*.data' -o "
                f"-path '*/*.index/docIndex/recovery/*.data' -o "
                f"-path '*/*.index/mainIndex/*.data' -o "
                f"-path '*/*.index/mainIndex/recovery/*.data' -o "
                f"-path '*/*.index/keyindex/*.data' "
                f"\\) 2>/dev/null"
            )
            output, _ = shell.execute_command(cmd)
            shell.disconnect()

            data_files = [f.strip() for f in output if f.strip()]
            if encrypted_bucket_names and bucket_key_map:
                data_files = self._bucket_uuid_prefix_filter(
                    data_files, bucket_key_map
                )

            # Exclude files belonging to internal metadata indexes.
            if metadata_inst_ids:
                data_files, excluded = self._filter_metadata_index_paths(
                    data_files, metadata_inst_ids
                )
                if excluded:
                    self.log.info(
                        f"Node {node.ip}: excluded {excluded} data file(s) "
                        "belonging to internal metadata indexes"
                    )

            if not data_files:
                reason = "no docIndex/mainIndex/keyindex .data files found"
                if encrypted_bucket_names:
                    reason += f" for buckets {encrypted_bucket_names}"
                self.log.warning(f"Node {node.ip}: {reason}")
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue

            sample_files = data_files[:30]
            passed_count = 0
            failed_files = []
            for file_path in sample_files:
                allowed_keys = self._allowed_keys_for_path(
                    file_path, effective_bkm, fallback_keys
                )
                if not allowed_keys:
                    failed_files.append(
                        (file_path, "no key ids available for this bucket")
                    )
                    continue
                found, matched, details = self.verify_file_contains_key_id(
                    node, file_path, allowed_keys
                )
                if found:
                    passed_count += 1
                    self.log.debug(
                        f"Node {node.ip}: {file_path} contains key id {matched}"
                    )
                else:
                    failed_files.append((file_path, details))
                    self.log.warning(
                        f"Node {node.ip}: {file_path} missing key id: {details}"
                    )

            if passed_count and not failed_files:
                status = "passed"
            elif not passed_count and not failed_files:
                status = "skipped"
            else:
                status = "failed"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": len(sample_files),
                "failed_files": failed_files,
            }
        return results

    def verify_gsi_plasma_shard_data_encrypted(self, index_nodes,
                                               expected_key_id=None,
                                               encrypted_bucket_names=None,
                                               bucket_key_map=None,
                                               per_node_bucket_key_map=None):
        """Verify Plasma shared-shard LSS ``.data`` files contain an expected key ID.

        Plasma stores shared shard data under ``@2i/shards/<shard_id>/data/``.
        These files do NOT carry the encryption magic header — the key ID is
        embedded inside the LSS block headers in the file body.

        Covered paths:
          shards/<shard_id>/data/header.data
          shards/<shard_id>/data/log.*.data
          shards/<shard_id>/data/recovery/header.data
          shards/<shard_id>/data/recovery/log.*.data

        config.json and shard.json are excluded automatically (not *.data).

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Single key ID, list of key IDs, or None. When
                empty, the verifier short-circuits to ``status=skipped``.
            encrypted_bucket_names: Optional list of bucket names to restrict
                the sweep to.
            bucket_key_map: Pre-built per-bucket key map from
                ``build_bucket_key_map``.

        Returns:
            dict: ``{node_ip: {"status": "passed"|"failed"|"skipped",
                                "passed_count": int,
                                "total_checked": int,
                                "failed_files": list[(path, details)]}}``
        """
        results = {}
        fallback_keys = self._normalize_key_ids(expected_key_id)
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        has_map_keys = any(
            info.get("key_ids") for info in bucket_key_map.values()
        )
        if not fallback_keys and not has_map_keys:
            self.log.info(
                "verify_gsi_plasma_shard_data_encrypted: no key ids known — "
                f"fallback_keys={fallback_keys!r}, "
                f"bucket_key_map buckets={list(bucket_key_map.keys())!r}, "
                f"per_node_bucket_key_map nodes={list((per_node_bucket_key_map or {}).keys())!r} — "
                "skipping"
            )
            return {node.ip: {"status": "skipped",
                              "reason": "no key ids supplied"}
                    for node in index_nodes}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(
                f"Validating Plasma shard data files on node {node.ip} — "
                f"fallback_keys={fallback_keys!r}, "
                f"effective_bkm buckets={list(effective_bkm.keys())!r}"
            )
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"
            shards_dir = f"{storage_dir}/shards"

            self._log_directory_tree(node, shards_dir)

            shell = RemoteMachineShellConnection(node, verbose=False)
            dir_check, _ = shell.execute_command(
                f"test -d {shards_dir} && echo exists || echo not_found"
            )
            shell.disconnect()
            dir_exists = dir_check and "exists" in dir_check[0]
            self.log.info(
                f"Node {node.ip}: {shards_dir} "
                f"{'EXISTS' if dir_exists else 'NOT FOUND'}"
            )

            if not dir_exists:
                reason = f"{shards_dir} directory not present on disk"
                self.log.info(f"Node {node.ip}: {reason} — skipping")
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue

            shell = RemoteMachineShellConnection(node, verbose=False)
            cmd = (
                # TODO: header.data excluded — see verify_gsi_data_files_key_id
                #   for the full rationale.  Reinstate once confirmed with the
                #   indexer team that shard header.data is re-written on encrypt.
                f"find {shards_dir} -type f "
                f"-name 'log.*.data' "
                f"2>/dev/null"
            )
            output, _ = shell.execute_command(cmd)
            shell.disconnect()

            data_files = [f.strip() for f in output if f.strip()]
            if not data_files:
                reason = "no shard data files found under @2i/shards"
                self.log.info(f"Node {node.ip}: {reason}")
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue

            self.log.info(
                f"Node {node.ip}: found {len(data_files)} log.*.data file(s) "
                f"under {shards_dir} — checking up to 30"
            )

            sample_files = data_files[:30]
            passed_count = 0
            failed_files = []
            for file_path in sample_files:
                allowed_keys = self._allowed_keys_for_path(
                    file_path, effective_bkm, fallback_keys
                )
                if not allowed_keys:
                    failed_files.append(
                        (file_path, "no key ids available for this bucket")
                    )
                    continue
                found, matched, details = self.verify_file_contains_key_id(
                    node, file_path, allowed_keys
                )
                if found:
                    passed_count += 1
                    self.log.debug(
                        f"Node {node.ip}: {file_path} contains key id {matched}"
                    )
                else:
                    failed_files.append((file_path, details))
                    self.log.warning(
                        f"Node {node.ip}: {file_path} missing key id: {details}"
                    )

            if passed_count and not failed_files:
                status = "passed"
            elif not passed_count and not failed_files:
                status = "skipped"
            else:
                status = "failed"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": len(sample_files),
                "failed_files": failed_files,
            }
        return results

    def verify_gsi_bhive_pindex_lss_encrypted(self, index_nodes,
                                              expected_key_id=None,
                                              encrypted_bucket_names=None,
                                              bucket_key_map=None,
                                              metadata_inst_ids=None,
                                              per_node_bucket_key_map=None):
        """Verify BHive pindex LSS ``.data`` files contain an expected key ID.

        BHive vector-index (pindex) data is stored in LSS format under
        ``@2i/@bhive/<hash>.index/mainIndex/lss.{N}/``.  The key ID is
        embedded in LSS block headers in the file body, not in an external
        magic header.

        Covered paths:
          @bhive/<hash>.index/mainIndex/lss.{N}/header.data
          @bhive/<hash>.index/mainIndex/lss.{N}/log.*.data

        EXCLUDED (never encrypted, plaintext by design):
          lss.{N}/recovery/chkp.{N}/header.data
          lss.{N}/recovery/chkp.{N}/log.*.data
          recovery/rp.N  (not .data)
          config.json    (not .data)

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Single key ID, list of key IDs, or None. When
                empty, the verifier short-circuits to ``status=skipped``.
            encrypted_bucket_names: Optional bucket-name filter.
            bucket_key_map: Pre-built per-bucket key map.

        Returns:
            dict: ``{node_ip: {"status": "passed"|"failed"|"skipped",
                                "passed_count": int,
                                "total_checked": int,
                                "failed_files": list[(path, details)]}}``
        """
        results = {}
        fallback_keys = self._normalize_key_ids(expected_key_id)
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        has_map_keys = any(
            info.get("key_ids") for info in bucket_key_map.values()
        )
        if not fallback_keys and not has_map_keys:
            self.log.info(
                "verify_gsi_bhive_pindex_lss_encrypted: no key ids known, skipping"
            )
            return {node.ip: {"status": "skipped",
                              "reason": "no key ids supplied"}
                    for node in index_nodes}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(
                f"Validating BHive pindex LSS data files on node {node.ip}"
            )
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"
            self._log_directory_tree(node, f"{storage_dir}/@bhive")

            shell = RemoteMachineShellConnection(node, verbose=False)
            # Match *.data files inside mainIndex/lss.*/ but exclude
            # recovery checkpoint subdirectories (chkp.*).
            # TODO: header.data excluded — see verify_gsi_data_files_key_id
            #   for the full rationale.  Reinstate once confirmed with the
            #   indexer team that BHive pindex lss header.data is re-written
            #   on encrypt.
            cmd = (
                f"find {storage_dir}/@bhive -type f -name '*.data' "
                f"! -name 'header.data' "
                f"-path '*/mainIndex/lss.*/*' "
                f"! -path '*/lss.*/recovery/chkp.*' "
                f"2>/dev/null"
            )
            output, _ = shell.execute_command(cmd)
            shell.disconnect()

            data_files = [f.strip() for f in output if f.strip()]
            if encrypted_bucket_names and bucket_key_map:
                data_files = self._bucket_uuid_prefix_filter(
                    data_files, bucket_key_map
                )

            # Exclude files belonging to internal metadata indexes.
            if metadata_inst_ids:
                data_files, excluded = self._filter_metadata_index_paths(
                    data_files, metadata_inst_ids
                )
                if excluded:
                    self.log.info(
                        f"Node {node.ip}: excluded {excluded} BHive pindex LSS "
                        "file(s) belonging to internal metadata indexes"
                    )

            if not data_files:
                reason = "no BHive pindex LSS .data files found under @2i/@bhive"
                self.log.info(f"Node {node.ip}: {reason} — skipping")
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue

            sample_files = data_files[:30]
            passed_count = 0
            failed_files = []
            for file_path in sample_files:
                allowed_keys = self._allowed_keys_for_path(
                    file_path, effective_bkm, fallback_keys
                )
                if not allowed_keys:
                    failed_files.append(
                        (file_path, "no key ids available for this bucket")
                    )
                    continue
                found, matched, details = self.verify_file_contains_key_id(
                    node, file_path, allowed_keys
                )
                if found:
                    passed_count += 1
                    self.log.debug(
                        f"Node {node.ip}: {file_path} contains key id {matched}"
                    )
                else:
                    failed_files.append((file_path, details))
                    self.log.warning(
                        f"Node {node.ip}: {file_path} missing key id: {details}"
                    )

            if passed_count and not failed_files:
                status = "passed"
            elif not passed_count and not failed_files:
                status = "skipped"
            else:
                status = "failed"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": len(sample_files),
                "failed_files": failed_files,
            }
        return results

    def verify_gsi_bhive_vindex_magma_encrypted(self, index_nodes,
                                                expected_key_id=None,
                                                encrypted_bucket_names=None,
                                                bucket_key_map=None,
                                                per_node_bucket_key_map=None):
        """Verify BHive vindex Magma KVStore SST files contain an expected key ID.

        BHive stores scalar-component (vindex) data in Magma KVStore format
        under ``@2i/@bhive/bhive-shards/<shard_id>/kvstore-{N}/``.
        The key ID is embedded in the SST file body using per-kvstore key
        derivation (KBKDF, AES-256-GCM).

        Covered paths:
          @bhive/bhive-shards/<shard_id>/kvstore-{N}/rev-{ver}/{keyIndex,seqIndex}/
            sstable.N.data

        Excluded automatically (not sstable.*.data):
          config.json, state.N, file.lock
          localIndex/ (no .data files under this sub-directory)

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Single key ID, list of key IDs, or None. When
                empty, the verifier short-circuits to ``status=skipped``.
            encrypted_bucket_names: Optional bucket-name filter.
            bucket_key_map: Pre-built per-bucket key map.

        Returns:
            dict: ``{node_ip: {"status": "passed"|"failed"|"skipped",
                                "passed_count": int,
                                "total_checked": int,
                                "failed_files": list[(path, details)]}}``
        """
        results = {}
        fallback_keys = self._normalize_key_ids(expected_key_id)
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        has_map_keys = any(
            info.get("key_ids") for info in bucket_key_map.values()
        )
        if not fallback_keys and not has_map_keys:
            self.log.info(
                "verify_gsi_bhive_vindex_magma_encrypted: no key ids known, skipping"
            )
            return {node.ip: {"status": "skipped",
                              "reason": "no key ids supplied"}
                    for node in index_nodes}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(
                f"Validating BHive vindex Magma SST files on node {node.ip}"
            )
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"
            self._log_directory_tree(node, f"{storage_dir}/@bhive/bhive-shards")

            shell = RemoteMachineShellConnection(node, verbose=False)
            cmd = (
                f"find {storage_dir}/@bhive/bhive-shards -type f "
                f"-name 'sstable.*.data' "
                f"2>/dev/null"
            )
            output, _ = shell.execute_command(cmd)
            shell.disconnect()

            data_files = [f.strip() for f in output if f.strip()]
            if not data_files:
                reason = (
                    "no BHive vindex Magma sstable.*.data files found "
                    "under @2i/@bhive/bhive-shards"
                )
                self.log.info(f"Node {node.ip}: {reason} — skipping")
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue

            sample_files = data_files[:30]
            passed_count = 0
            failed_files = []
            for file_path in sample_files:
                allowed_keys = self._allowed_keys_for_path(
                    file_path, effective_bkm, fallback_keys
                )
                if not allowed_keys:
                    failed_files.append(
                        (file_path, "no key ids available for this bucket")
                    )
                    continue
                found, matched, details = self.verify_file_contains_key_id(
                    node, file_path, allowed_keys
                )
                if found:
                    passed_count += 1
                    self.log.debug(
                        f"Node {node.ip}: {file_path} contains key id {matched}"
                    )
                else:
                    failed_files.append((file_path, details))
                    self.log.warning(
                        f"Node {node.ip}: {file_path} missing key id: {details}"
                    )

            if passed_count and not failed_files:
                status = "passed"
            elif not passed_count and not failed_files:
                status = "skipped"
            else:
                status = "failed"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": len(sample_files),
                "failed_files": failed_files,
            }
        return results

    def verify_gsi_error_files_encrypted(self, index_nodes, expected_key_id=None,
                                         bucket_key_map=None,
                                         encrypted_bucket_names=None,
                                         per_node_bucket_key_map=None):
        """
        Verify ``.error`` files under @2i carry the encryption header.

        ``.error`` files are only written when the indexer detects corruption,
        so on a healthy cluster the sweep is expected to find nothing — that
        path returns ``status=skipped`` per node. When files are present, each
        is checked for the ``Couchbase Encrypted`` magic header and (if
        provided) the expected key ID in the header. Capped at 20 files/node.

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Optional key ID (or list) expected in the
                encrypted header. When None/empty, only the magic header is
                asserted.

        Returns:
            dict: ``{node_ip: {"status": "passed"|"failed"|"skipped",
                                "passed_count": int,
                                "total_checked": int,
                                "failed_files": list[(path, details)]}}``
        """
        results = {}
        fallback_keys = self._normalize_key_ids(expected_key_id)
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(f"Validating .error files on node {node.ip}")
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"

            shell = RemoteMachineShellConnection(node, verbose=False)
            output, _ = shell.execute_command(
                f"find {storage_dir} -type f -name '*.error' 2>/dev/null"
            )
            shell.disconnect()

            error_files = [f.strip() for f in output if f.strip()]
            if encrypted_bucket_names and bucket_key_map:
                error_files = self._bucket_uuid_prefix_filter(
                    error_files, bucket_key_map
                )
            if not error_files:
                results[node.ip] = {"status": "skipped",
                                    "reason": "no .error files found"}
                continue

            passed_count = 0
            failed_files = []
            for file_path in error_files[:20]:
                allowed_keys = self._allowed_keys_for_path(
                    file_path, effective_bkm, fallback_keys
                )
                is_encrypted, details = self.verify_file_encryption_magic_bytes(
                    node, file_path
                )
                if not is_encrypted:
                    failed_files.append((file_path, details))
                    continue
                if allowed_keys:
                    found, matched = self.verify_file_header_contains_any(
                        node, file_path, allowed_keys
                    )
                    if not found:
                        failed_files.append(
                            (file_path, f"key id {allowed_keys} not in header")
                        )
                        continue
                passed_count += 1

            status = "passed" if passed_count and not failed_files else "failed"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": min(len(error_files), 20),
                "failed_files": failed_files,
            }
        return results

    def _verify_stats_log(self, node, log_name, expected_key_ids):
        """
        Header-check a single named stats log file in the node's log dir.

        Shared implementation for ``indexer_stats.log`` and
        ``projector_stats.log``. If a ``<log_name>.enc`` sibling exists it is
        preferred over the plain log (Couchbase writes the encrypted variant
        alongside the legacy filename in some builds).

        Args:
            node: Server object to check.
            log_name: Bare filename, e.g. ``"indexer_stats.log"``.
            expected_key_ids: Normalized list of key IDs (may be empty).

        Returns:
            dict: ``{"status": "passed"|"failed"|"skipped",
                      "file": str,
                      "reason": str,            # on failure/skip
                      "failed_files": list}``   # on failure
        """
        log_path = self.get_log_path(node)
        stats_log = f"{log_path}/{log_name}"

        shell = RemoteMachineShellConnection(node, verbose=False)
        exists_out, _ = shell.execute_command(
            f"test -f {stats_log} && echo 'exists' || echo 'not_found'"
        )
        enc_exists_out, _ = shell.execute_command(
            f"test -f {stats_log}.enc && echo 'exists' || echo 'not_found'"
        )
        shell.disconnect()

        if not exists_out or 'exists' not in exists_out[0]:
            return {"status": "skipped", "reason": f"{log_name} not found"}

        target = (f"{stats_log}.enc"
                  if enc_exists_out and 'exists' in enc_exists_out[0]
                  else stats_log)

        is_encrypted, details = self.verify_file_encryption_magic_bytes(node, target)
        if not is_encrypted:
            return {"status": "failed", "file": target, "reason": details,
                    "failed_files": [(target, details)]}

        if expected_key_ids:
            found, matched = self.verify_file_header_contains_any(
                node, target, expected_key_ids
            )
            if not found:
                reason = f"key id {expected_key_ids} not in header: {matched}"
                return {"status": "failed", "file": target, "reason": reason,
                        "failed_files": [(target, reason)]}
        return {"status": "passed", "file": target}

    def verify_gsi_indexer_stats_log_encrypted(self, index_nodes,
                                                  expected_key_id=None):
        """
        Verify ``indexer_stats.log`` is encrypted on every index node.

        Thin wrapper over ``_verify_stats_log`` that shares logic with the
        projector verifier and uses the normalized key-ID list (supports
        single value or list).

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Optional key ID (or list) expected in the header.

        Returns:
            dict: ``{node_ip: <_verify_stats_log result>}``
        """
        expected_key_ids = self._normalize_key_ids(expected_key_id)
        return {node.ip: self._verify_stats_log(node, "indexer_stats.log",
                                                expected_key_ids)
                for node in index_nodes}

    def verify_gsi_projector_stats_log_encrypted(self, index_nodes,
                                                 expected_key_id=None):
        """
        Verify ``projector_stats.log`` is encrypted on every index node.

        Mirrors :meth:`verify_gsi_indexer_stats_log_encrypted` for the projector
        log; both share ``_verify_stats_log``.

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Optional key ID (or list) expected in the header.

        Returns:
            dict: ``{node_ip: <_verify_stats_log result>}``
        """
        expected_key_ids = self._normalize_key_ids(expected_key_id)
        return {node.ip: self._verify_stats_log(node, "projector_stats.log",
                                                expected_key_ids)
                for node in index_nodes}

    def verify_gsi_request_handler_cache_encrypted(self, index_nodes,
                                                   expected_key_id=None):
        """
        Verify request-handler cache files are encrypted.

        The request-handler cache persists short-lived metadata and stats
        snapshots under ``@2i/cache/stats/`` and ``@2i/cache/meta/``; every
        regular file in those two directories must carry the encryption
        magic header (and the expected key ID, if supplied). Capped at 30
        files per node.

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Optional key ID (or list) expected in the
                header.

        Returns:
            dict: ``{node_ip: {"status": "passed"|"failed"|"skipped",
                                "passed_count": int,
                                "total_checked": int,
                                "failed_files": list[(path, details)]}}``
        """
        results = {}
        expected_key_ids = self._normalize_key_ids(expected_key_id)

        for node in index_nodes:
            self.log.info(f"Validating request-handler cache files on node {node.ip}")
            rest = RestConnection(node)
            cache_dir = f"{rest.get_index_path()}/@2i/cache"
            self._log_directory_tree(node, cache_dir)

            shell = RemoteMachineShellConnection(node, verbose=False)
            exists_out, _ = shell.execute_command(
                f"test -d {cache_dir} && echo 'exists' || echo 'not_found'"
            )
            if not exists_out or 'exists' not in exists_out[0]:
                shell.disconnect()
                results[node.ip] = {"status": "skipped",
                                    "reason": f"{cache_dir} not found"}
                continue

            stats_out, _ = shell.execute_command(
                f"find {cache_dir}/stats -type f 2>/dev/null"
            )
            meta_out, _ = shell.execute_command(
                f"find {cache_dir}/meta -type f 2>/dev/null"
            )
            shell.disconnect()

            files = [f.strip() for f in (stats_out + meta_out) if f.strip()]
            if not files:
                results[node.ip] = {"status": "skipped",
                                    "reason": "no cache files found"}
                continue

            passed_count = 0
            failed_files = []
            for file_path in files[:30]:
                is_encrypted, details = self.verify_file_encryption_magic_bytes(
                    node, file_path
                )
                if not is_encrypted:
                    failed_files.append((file_path, details))
                    continue
                if expected_key_ids:
                    found, matched = self.verify_file_header_contains_any(
                        node, file_path, expected_key_ids
                    )
                    if not found:
                        failed_files.append(
                            (file_path, f"key id {expected_key_ids} not in header")
                        )
                        continue
                passed_count += 1

            status = "passed" if passed_count and not failed_files else "failed"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": min(len(files), 30),
                "failed_files": failed_files,
            }
        return results

    def _extract_string_values(self, obj, sink):
        """Recursively pull printable string values out of a JSON-y object."""
        if isinstance(obj, str):
            sink.append(obj)
        elif isinstance(obj, dict):
            for value in obj.values():
                self._extract_string_values(value, sink)
        elif isinstance(obj, list):
            for value in obj:
                self._extract_string_values(value, sink)

    def collect_plaintext_samples_via_n1ql(self, query_node, bucket_names,
                                           sample_size=10):
        """
        Pull a sample of plaintext field values to grep snapshot files for.

        Runs ``SELECT * FROM <bucket> LIMIT <sample_size>`` on each bucket
        and recursively extracts string values from the result rows. Tokens
        shorter than :attr:`_LEAK_MIN_TOKEN_LEN` are dropped (too noisy),
        and the result is capped at :attr:`_LEAK_MAX_TOKENS` distinct
        substrings.

        Args:
            query_node: Server object for a node running the query service.
            bucket_names: List of bucket names to sample.
            sample_size: ``LIMIT`` for each per-bucket SELECT.

        Returns:
            list[str]: Sorted, deduplicated plaintext substrings safe to grep
                for. Empty list if no buckets supplied or all queries failed.
        """
        if not bucket_names:
            return []
        rest = RestConnection(query_node)
        sink = []
        for bucket in bucket_names:
            query = f"SELECT * FROM `{bucket}` LIMIT {int(sample_size)}"
            try:
                result = rest.query_tool(query, timeout=60, verbose=False)
            except Exception as exc:
                self.log.warning(
                    f"N1QL sample for bucket {bucket} failed: {exc}"
                )
                continue
            for row in (result or {}).get("results", []):
                self._extract_string_values(row, sink)

        # Strip whitespace, drop tokens too short to be meaningful, de-dup.
        candidates = {
            s.strip() for s in sink
            if isinstance(s, str) and len(s.strip()) >= self._LEAK_MIN_TOKEN_LEN
        }
        # Drop obviously generic tokens that would over-match.
        candidates -= {"results", "default", "true", "false", "null"}
        tokens = sorted(candidates)[:self._LEAK_MAX_TOKENS]
        self.log.debug(
            f"plaintext samples for leakage scan ({len(tokens)} tokens): "
            f"{tokens[:5]}{'...' if len(tokens) > 5 else ''}"
        )
        return tokens

    def _discover_encrypted_files(self, node, category, storage_dir, log_path,
                                  encrypted_bucket_names, bucket_key_map):
        """
        Enumerate the must-be-encrypted files on ``node`` for one category.

        Centralizes the find-pattern dispatch so the leakage scanner reuses
        the exact same file set the per-category encryption verifiers do.
        Returns absolute paths; the caller is responsible for grep.
        """
        shell = RemoteMachineShellConnection(node, verbose=False)
        try:
            if category == "snapshot":
                out, _ = shell.execute_command(
                    f"find {storage_dir} -type d -path '*/*.index/snapshot.*' "
                    f"2>/dev/null"
                )
                snapshot_dirs = [d.strip() for d in out if d.strip()]
                if encrypted_bucket_names and bucket_key_map:
                    snapshot_dirs = self._bucket_uuid_prefix_filter(
                        snapshot_dirs, bucket_key_map
                    )
                files = []
                for snapshot_dir in snapshot_dirs:
                    file_out, _ = shell.execute_command(
                        f"find {snapshot_dir} -type f 2>/dev/null"
                    )
                    for line in file_out:
                        fp = line.strip()
                        if not fp:
                            continue
                        rel = self._snapshot_relative(fp, snapshot_dir)
                        if rel in self.SNAPSHOT_PLAINTEXT_FILES:
                            continue
                        files.append(fp)
                return files

            if category == "data":
                out, _ = shell.execute_command(
                    f"find {storage_dir} -type f \\( "
                    f"-path '*/*.index/docIndex/*.data' -o "
                    f"-path '*/*.index/mainIndex/*.data' -o "
                    f"-path '*/*.index/keyindex/*.data' "
                    f"\\) 2>/dev/null"
                )
                files = [f.strip() for f in out if f.strip()]
                if encrypted_bucket_names and bucket_key_map:
                    files = self._bucket_uuid_prefix_filter(files, bucket_key_map)
                return files

            if category == "error":
                out, _ = shell.execute_command(
                    f"find {storage_dir} -type f -name '*.error' 2>/dev/null"
                )
                files = [f.strip() for f in out if f.strip()]
                if encrypted_bucket_names and bucket_key_map:
                    files = self._bucket_uuid_prefix_filter(files, bucket_key_map)
                return files

            if category == "codebook":
                out, _ = shell.execute_command(
                    f"find {storage_dir} -type f -path '*/*.index/codebook/*' "
                    f"2>/dev/null"
                )
                files = [f.strip() for f in out if f.strip()]
                if encrypted_bucket_names and bucket_key_map:
                    files = self._bucket_uuid_prefix_filter(files, bucket_key_map)
                return files

            if category == "request_handler_cache":
                cache_dir = f"{storage_dir}/cache"
                stats_out, _ = shell.execute_command(
                    f"find {cache_dir}/stats -type f 2>/dev/null"
                )
                meta_out, _ = shell.execute_command(
                    f"find {cache_dir}/meta -type f 2>/dev/null"
                )
                return [f.strip() for f in (stats_out + meta_out) if f.strip()]

            if category == "stats_logs":
                files = []
                for log_name in ("indexer_stats.log", "projector_stats.log"):
                    for suffix in ("", ".enc"):
                        target = f"{log_path}/{log_name}{suffix}"
                        exists_out, _ = shell.execute_command(
                            f"test -f {target} && echo 'exists' || echo 'no'"
                        )
                        if exists_out and 'exists' in exists_out[0]:
                            files.append(target)
                return files

            self.log.warning(f"unknown leakage category {category!r}")
            return []
        finally:
            shell.disconnect()

    def verify_no_plaintext_in_encrypted_files(self, index_nodes,
                                               plaintext_samples,
                                               categories=None,
                                               encrypted_bucket_names=None,
                                               bucket_key_map=None,
                                               per_category_limit=50):
        """
        Grep every must-be-encrypted file (across categories) for plaintext index values.

        For each requested category, enumerates the same files the matching
        encryption verifier would check, then runs ``grep -aFl <token>`` for
        every plaintext sample. Any hit means the file is encrypted on the
        surface (or claims to be) but leaks clear-text index data, which is
        a security regression even when the magic header is present.

        Categories (see :attr:`LEAKAGE_CATEGORIES` for the canonical set):
          * ``snapshot``               — ``@2i/*.index/snapshot.*/`` (allowlist
                                          + extras, minus plaintext-by-design)
          * ``data``                   — ``@2i/*.index/{docIndex,mainIndex,
                                          keyindex}/*.data``
          * ``error``                  — ``@2i/**/*.error``
          * ``codebook``               — ``@2i/*.index/codebook/*``
          * ``request_handler_cache``  — ``@2i/cache/{stats,meta}/*``
          * ``stats_logs``             — ``indexer_stats.log`` and
                                          ``projector_stats.log`` (+ ``.enc``)

        Args:
            index_nodes: List of index server objects.
            plaintext_samples: Iterable of strings to grep for. Get this via
                :meth:`collect_plaintext_samples_via_n1ql` or curate manually.
            categories: Iterable of category names to scan. ``None`` →
                :attr:`LEAKAGE_CATEGORIES` (all).
            encrypted_bucket_names: Optional bucket-name filter (applied via
                UUID prefix to path-based categories).
            bucket_key_map: ``{name: {"uuid", "key_ids"}}`` from
                :meth:`build_bucket_key_map`.
            per_category_limit: Cap on files scanned per category per node.

        Returns:
            dict: ``{node_ip: {
                        "status": "passed"|"failed"|"skipped",
                        "by_category": {
                            cat: {"files_checked": int,
                                  "leak_count": int,
                                  "leaks": list[(path, matched_token)]}
                        },
                        "total_leaks": int,
                    }}``
        """
        results = {}
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}
        tokens = [t for t in (plaintext_samples or []) if t]
        cats = list(categories) if categories else list(self.LEAKAGE_CATEGORIES)

        if not tokens:
            self.log.info("verify_no_plaintext_in_encrypted_files: "
                          "no plaintext samples supplied, skipping")
            return {node.ip: {"status": "skipped",
                              "reason": "no plaintext samples",
                              "by_category": {}, "total_leaks": 0}
                    for node in index_nodes}

        for node in index_nodes:
            self.log.info(
                f"Validating encrypted files for plaintext leakage on {node.ip} "
                f"(categories: {cats})"
            )
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"
            log_path = self.get_log_path(node)

            node_total_leaks = 0
            by_category = {}
            for category in cats:
                files = self._discover_encrypted_files(
                    node, category, storage_dir, log_path,
                    encrypted_bucket_names, bucket_key_map,
                )
                if not files:
                    by_category[category] = {
                        "status": "skipped",
                        "reason": "no files found",
                        "files_checked": 0,
                        "leak_count": 0,
                        "leaks": [],
                    }
                    continue

                sample_files = files[:per_category_limit]
                leaks = []
                for file_path in sample_files:
                    shell = RemoteMachineShellConnection(node, verbose=False)
                    try:
                        for token in tokens:
                            # Single-quoted shell arg; escape any embedded
                            # single quotes in the token itself.
                            safe = token.replace("'", "'\"'\"'")
                            cmd = f"grep -aFl -e '{safe}' {file_path} 2>/dev/null"
                            out, _ = shell.execute_command(cmd)
                            if out and any(line.strip() for line in out):
                                leaks.append((file_path, token))
                                self.log.error(
                                    f"Node {node.ip}: PLAINTEXT LEAK [{category}] "
                                    f"— {file_path} contains {token!r}"
                                )
                                break  # one leak per file is enough
                    finally:
                        shell.disconnect()

                by_category[category] = {
                    "status": "failed" if leaks else "passed",
                    "files_checked": len(sample_files),
                    "leak_count": len(leaks),
                    "leaks": leaks[:50],
                }
                node_total_leaks += len(leaks)

            results[node.ip] = {
                "status": "failed" if node_total_leaks else "passed",
                "by_category": by_category,
                "total_leaks": node_total_leaks,
            }
        return results

    def verify_no_plaintext_in_snapshot_files(self, index_nodes,
                                              plaintext_samples,
                                              encrypted_bucket_names=None,
                                              bucket_key_map=None):
        """
        Backwards-compat wrapper — scans only the ``snapshot`` category.

        Equivalent to calling :meth:`verify_no_plaintext_in_encrypted_files`
        with ``categories=("snapshot",)``.
        """
        return self.verify_no_plaintext_in_encrypted_files(
            index_nodes, plaintext_samples,
            categories=("snapshot",),
            encrypted_bucket_names=encrypted_bucket_names,
            bucket_key_map=bucket_key_map,
        )

    def verify_gsi_codebook_encrypted(self, index_nodes, expected_key_id=None,
                                      encrypted_bucket_names=None,
                                      bucket_key_map=None,
                                      per_node_bucket_key_map=None):
        """
        Header-check every file under ``@2i/*.index/codebook/`` per index node.

        Composite and BHIVE indexes persist their learned codebooks in a
        ``codebook/`` subfolder next to ``docIndex``/``mainIndex``. Per the
        spec these files must carry the ``Couchbase Encrypted`` magic header
        (and an acceptable key ID). Filename-leak is covered separately by
        the naming-leak check.

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Flat key ID(s) used as the fallback when
                bucket_key_map is empty or the file's bucket can't be resolved.
            encrypted_bucket_names: Optional bucket-name filter; combined with
                ``bucket_key_map`` to filter codebook files by their UUID
                prefix.
            bucket_key_map: ``{name: {"uuid", "key_ids"}}`` from
                :meth:`build_bucket_key_map`.

        Returns:
            dict: ``{node_ip: {"status": "passed"|"failed"|"skipped",
                                "passed_count": int,
                                "total_checked": int,
                                "failed_files": list[(path, details)]}}``
        """
        results = {}
        fallback_keys = self._normalize_key_ids(expected_key_id)
        encrypted_bucket_names = list(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(f"Validating codebook files on node {node.ip}")
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"

            shell = RemoteMachineShellConnection(node, verbose=False)
            output, _ = shell.execute_command(
                f"find {storage_dir} -type f -path '*/*.index/codebook/*' "
                f"2>/dev/null"
            )
            shell.disconnect()

            codebook_files = [f.strip() for f in output if f.strip()]
            if encrypted_bucket_names and bucket_key_map:
                codebook_files = self._bucket_uuid_prefix_filter(
                    codebook_files, bucket_key_map
                )

            if not codebook_files:
                results[node.ip] = {"status": "skipped",
                                    "reason": "no codebook files found"}
                continue

            passed_count = 0
            failed_files = []
            for file_path in codebook_files[:30]:
                allowed_keys = self._allowed_keys_for_path(
                    file_path, effective_bkm, fallback_keys
                )
                is_encrypted, details = self.verify_file_encryption_magic_bytes(
                    node, file_path
                )
                if not is_encrypted:
                    failed_files.append((file_path, details))
                    continue
                if allowed_keys:
                    found, matched = self.verify_file_header_contains_any(
                        node, file_path, allowed_keys
                    )
                    if not found:
                        failed_files.append(
                            (file_path,
                             f"header missing any of allowed key ids {allowed_keys}")
                        )
                        continue
                passed_count += 1

            status = "passed" if passed_count and not failed_files else "failed"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": min(len(codebook_files), 30),
                "failed_files": failed_files,
            }
        return results

    def verify_gsi_metadata_repo_encrypted(self, index_nodes, expected_key_id=None):
        """
        Placeholder for ``@2i/metadata_repo_v2/*.wal`` and ``*.sstable`` checks.

        The spec for these files is still being finalized — it is not yet
        confirmed whether they carry the encryption header or only embed the
        key ID in the body. Returning a structured ``status=todo`` result
        keeps the category visible in the orchestrator output without failing
        any test; replace the body with the real check once the spec lands.

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Currently unused (preserved for forward
                compatibility with the eventual implementation).

        Returns:
            dict: ``{node_ip: {"status": "todo", "reason": str}}``
        """
        _ = expected_key_id
        self.log.info(
            "TODO: metadata_repo_v2 *.wal / *.sstable encryption check not yet "
            "implemented (spec pending)."
        )
        return {node.ip: {"status": "todo",
                          "reason": "metadata_repo encryption spec pending"}
                for node in index_nodes}

    def verify_storage_stats_rest(self, index_nodes, expected_key_id=None,
                                  encrypted_bucket_names=None,
                                  bucket_key_map=None,
                                  per_node_bucket_key_map=None):
        """
        Verify per-store encryption state via the indexer ``/stats/storage``.

        ``/stats/storage`` returns, per index, a ``Stats`` block that contains
        a ``MainStore`` substructure and (usually) a ``BackStore``
        substructure. The encryption fields live INSIDE those stores, not at
        ``Stats`` top level. This verifier walks each store independently:

          * ``encryption_status == None`` → store skipped entirely.
            MOI (in-memory) stores do not populate this field because there
            is no persistent LSS to report on. A node where every store
            returns ``None`` gets ``status=skipped`` with an explanatory
            reason rather than a failure.
          * ``encryption_status == "encrypted"`` → pass
          * ``encryption_status == "partially_encrypted"`` → soft fail
            (logged + tracked in ``soft_failed``, does not flip overall status)
          * any other value (e.g. ``"unencrypted"``) on a bucket that is
            supposed to be encrypted → hard fail
          * ``encryption_key_ids`` (when present) must be a subset of the
            allowed keys (per-bucket map preferred; flat fallback otherwise).
            Any stray key ID is a hard fail.
          * ``num_encryption_key_ids != len(encryption_key_ids)`` → sanity
            log only.

        Args:
            index_nodes: List of index server objects (one REST call per node).
            expected_key_id: Single key ID or list of allowed key IDs (flat
                fallback when bucket_key_map doesn't supply keys for a bucket).
            encrypted_bucket_names: Optional bucket-name filter.
            bucket_key_map: ``{name: {"uuid", "key_ids"}}`` from
                :meth:`build_bucket_key_map`.

        Returns:
            dict: ``{node_ip: {"status": ..., "checked_stores": int,
                                "passed_count": int,
                                "soft_failed": list[(label, reason)],
                                "failed_stores": list[(label, reason)]}}``
                where ``label`` is ``"<bucket>:<index>/<MainStore|BackStore>"``.
        """
        results = {}
        fallback_keys = set(self._normalize_key_ids(expected_key_id))
        encrypted_bucket_names = set(encrypted_bucket_names or [])
        bucket_key_map = bucket_key_map or {}

        for node in index_nodes:
            # Use the key map built from THIS node's own GetInUseKeys so that
            # per-node encryption keys are correctly attributed.
            effective_bkm = (
                (per_node_bucket_key_map or {}).get(node.ip)
                or bucket_key_map
            )
            self.log.info(f"Validating /stats/storage encryption on node {node.ip}")
            rest = RestConnection(node)
            try:
                raw_stats = rest.get_index_storage_stats()
            except Exception as exc:
                self.log.warning(
                    f"Node {node.ip}: /stats/storage request failed: {exc}"
                )
                results[node.ip] = {"status": "skipped",
                                    "reason": f"REST call failed: {exc}"}
                continue

            checked_stores = 0
            null_stores = 0   # stores that returned encryption_status=None (MOI)
            passed_count = 0
            soft_failed = []
            failed_stores = []

            for bucket, indexes in raw_stats.items():
                if encrypted_bucket_names and bucket not in encrypted_bucket_names:
                    continue
                for index_name, stats in indexes.items():
                    for store_name in self.STORAGE_STORE_NAMES:
                        store = stats.get(store_name)
                        if not isinstance(store, dict):
                            continue
                        label = f"{bucket}:{index_name}/{store_name}"

                        status_val = store.get("encryption_status")

                        # MOI (in-memory) stores do not populate
                        # encryption_status — the field is null/absent because
                        # there is no persistent LSS to report on.  Treat these
                        # stores as not applicable and skip them entirely so
                        # they do not inflate failed_stores.
                        if status_val is None:
                            self.log.debug(
                                f"Node {node.ip}: {label} "
                                "encryption_status=None — MOI or field not "
                                "populated, skipping store"
                            )
                            null_stores += 1
                            continue

                        checked_stores += 1
                        key_ids = store.get("encryption_key_ids") or []
                        num_key_ids = store.get("num_encryption_key_ids")

                        if num_key_ids is not None and num_key_ids != len(key_ids):
                            self.log.info(
                                f"Node {node.ip}: {label} "
                                f"num_encryption_key_ids={num_key_ids} but "
                                f"encryption_key_ids has {len(key_ids)} entries"
                            )

                        store_ok = True
                        if status_val == "encrypted":
                            pass
                        elif status_val == "partially_encrypted":
                            reason = "encryption_status=partially_encrypted"
                            soft_failed.append((label, reason))
                            self.log.warning(f"Node {node.ip}: {label} {reason}")
                        else:
                            reason = f"encryption_status={status_val!r}"
                            failed_stores.append((label, reason))
                            self.log.error(f"Node {node.ip}: {label} {reason}")
                            store_ok = False

                        # Prefer per-bucket allowed keys; fall back to flat.
                        per_bucket = (
                            (effective_bkm.get(bucket) or {}).get("key_ids") or []
                        )
                        allowed = set(per_bucket) or fallback_keys
                        if allowed and key_ids:
                            stray = [k for k in key_ids if k not in allowed]
                            if stray:
                                reason = (f"stray key ids {stray} not in "
                                          f"allowed {sorted(allowed)}")
                                failed_stores.append((label, reason))
                                self.log.error(f"Node {node.ip}: {label} {reason}")
                                store_ok = False

                        if store_ok:
                            passed_count += 1

            if checked_stores == 0:
                if null_stores:
                    reason = (
                        f"all {null_stores} store(s) reported "
                        "encryption_status=None — MOI stores do not "
                        "populate this field"
                    )
                elif encrypted_bucket_names:
                    reason = (f"no stores matched filter "
                              f"{sorted(encrypted_bucket_names)}")
                else:
                    reason = "no MainStore/BackStore in /stats/storage"
                results[node.ip] = {"status": "skipped", "reason": reason}
                continue

            results[node.ip] = {
                "status": "failed" if failed_stores else "passed",
                "checked_stores": checked_stores,
                "passed_count": passed_count,
                "soft_failed": soft_failed,
                "failed_stores": failed_stores,
            }
        return results

    def snapshot_storage_stats_counters(self, index_nodes):
        """
        Capture per-store crypt-byte counters at a point in time.

        Reads ``/stats/storage`` on every index node, picks out the four
        encryption-throughput counters listed in :attr:`STORAGE_CRYPT_COUNTERS`
        from every ``MainStore`` / ``BackStore`` substructure, and packs them
        into a flat snapshot dict suitable for diffing later via
        :meth:`assert_storage_stats_counters_delta`.

        Args:
            index_nodes: List of index server objects.

        Returns:
            dict: ``{node_ip: {label: {counter_field: int, ...}, ...}}``
                where ``label = "<bucket>:<index>/<MainStore|BackStore>"``.
                Missing counters default to 0 so the diff is well-defined.
                Nodes whose REST call fails resolve to ``{}``.
        """
        snapshot = {}
        for node in index_nodes:
            rest = RestConnection(node)
            try:
                raw = rest.get_index_storage_stats()
            except Exception as exc:
                self.log.warning(
                    f"Node {node.ip}: /stats/storage snapshot failed: {exc}"
                )
                snapshot[node.ip] = {}
                continue
            node_snap = {}
            for bucket, indexes in raw.items():
                for index_name, stats in indexes.items():
                    for store_name in self.STORAGE_STORE_NAMES:
                        store = stats.get(store_name)
                        if not isinstance(store, dict):
                            continue
                        label = f"{bucket}:{index_name}/{store_name}"
                        node_snap[label] = {
                            field: int(store.get(field) or 0)
                            for field in self.STORAGE_CRYPT_COUNTERS
                        }
            snapshot[node.ip] = node_snap
        return snapshot

    def assert_storage_stats_counters_delta(self, before, after, mode):
        """
        Diff two ``snapshot_storage_stats_counters`` snapshots and assert.

        Args:
            before: Snapshot from before the workload window.
            after: Snapshot from after the workload window.
            mode: ``"must_increase"`` (encryption enabled / actively writing)
                or ``"must_not_increase"`` (encryption disabled).

        Returns:
            dict: ``{"status": "passed"|"failed", "mode": mode,
                      "violations": list[(label, field, before, after, delta)],
                      "deltas": {label: {field: delta}}}``

        Notes:
            * A store/label that disappeared between snapshots is recorded as a
              warning but not a violation (index drop is legitimate).
            * A store/label that appeared in ``after`` but not ``before`` is
              diffed against an implicit 0 baseline — fine for ``must_increase``
              (counter went from 0 → N), but flagged for ``must_not_increase``
              if N > 0.
        """
        if mode not in ("must_increase", "must_not_increase"):
            raise ValueError(f"invalid mode {mode!r}; "
                             "expected 'must_increase' or 'must_not_increase'")

        violations = []
        deltas = {}
        for node_ip, after_node in after.items():
            before_node = before.get(node_ip, {})
            for label, after_fields in after_node.items():
                before_fields = before_node.get(label, {})
                deltas.setdefault(node_ip, {})[label] = {}
                for field in self.STORAGE_CRYPT_COUNTERS:
                    a = int(after_fields.get(field) or 0)
                    b = int(before_fields.get(field) or 0)
                    delta = a - b
                    deltas[node_ip][label][field] = delta
                    if mode == "must_increase" and delta <= 0:
                        violations.append((node_ip, label, field, b, a, delta))
                    elif mode == "must_not_increase" and delta > 0:
                        violations.append((node_ip, label, field, b, a, delta))

            # Stores that vanished after — log but don't fail
            for label in before_node:
                if label not in after_node:
                    self.log.info(
                        f"Node {node_ip}: store {label} disappeared between "
                        f"snapshots (likely index drop) — skipped"
                    )

        if violations:
            self.log.error(
                f"crypt-counter delta violations ({mode}): "
                f"{len(violations)} (showing 5): {violations[:5]}"
            )
        return {
            "status": "failed" if violations else "passed",
            "mode": mode,
            "violations": violations[:50],
            "deltas": deltas,
        }

    def _collect_forbidden_names(self, rest):
        """
        Discover names that must not appear in any on-disk path: bucket, scope,
        collection, index, and per-field names parsed from secExprs.

        Uses the raw /indexStatus REST endpoint (the parsed map omits
        scope/collection/secExprs).
        """
        api = rest.baseUrl + 'indexStatus'
        status, content, _ = rest.urllib_request(api, timeout=60)
        if not status:
            self.log.warning("indexStatus request failed; "
                             "naming-leak forbidden list will be empty")
            return []

        try:
            parsed = (json.loads(content)
                      if isinstance(content, (str, bytes)) else content)
        except (TypeError, ValueError) as e:
            self.log.warning(f"indexStatus parse failed: {e}")
            return []

        names = set()
        for entry in parsed.get("indexes", []):
            for key in ("bucket", "scope", "collection", "index"):
                value = entry.get(key)
                if value:
                    names.add(value)
            for expr in entry.get("secExprs", []) or []:
                for backticked, bare in self._IDENT_RE.findall(str(expr)):
                    names.add(backticked or bare)

        filtered = sorted({
            n.lower() for n in names
            if n and n.lower() not in self._NAME_BLOCKLIST
            and len(n) >= 3 and not n.isdigit()
        })
        self.log.info(f"naming-leak: forbidden substrings = {filtered}")
        return filtered

    def verify_no_sensitive_names_in_paths(self, index_nodes, forbidden_substrings):
        """
        Walk @2i and the log directory on each node; flag any path component
        whose lowercased basename contains a forbidden substring.

        Returns:
            dict: {node_ip: {"status": str, "violations": list[(path, needle)]}}
        """
        results = {}
        needles = [n.lower() for n in (forbidden_substrings or [])
                   if n and len(n) >= 3 and not n.isdigit()]

        if not needles:
            self.log.info("naming-leak: no forbidden substrings to check")
            return {node.ip: {"status": "skipped",
                              "reason": "no forbidden substrings"}
                    for node in index_nodes}

        for node in index_nodes:
            rest = RestConnection(node)
            storage_dir = f"{rest.get_index_path()}/@2i"
            log_dir = self.get_log_path(node)

            shell = RemoteMachineShellConnection(node, verbose=False)
            idx_out, _ = shell.execute_command(f"find {storage_dir} 2>/dev/null")
            log_out, _ = shell.execute_command(
                f"find {log_dir} -maxdepth 2 2>/dev/null"
            )
            shell.disconnect()

            all_paths = [p.strip() for p in (idx_out + log_out) if p.strip()]
            violations = []
            for path in all_paths:
                # Check each path component, not the full path, so a leak in
                # the index dir name (e.g. ".../<bucket>_idx5_...index") is
                # caught while harmless parent dirs aren't repeated.
                for component in path.split("/"):
                    lc = component.lower()
                    matched = next((n for n in needles if n in lc), None)
                    if matched:
                        violations.append((path, matched))
                        break

            results[node.ip] = {
                "status": "failed" if violations else "passed",
                "violations": violations[:50],
                "total_paths_checked": len(all_paths),
            }
            if violations:
                self.log.warning(
                    f"Node {node.ip}: {len(violations)} naming-leak violations "
                    f"(showing up to 5): {violations[:5]}"
                )
        return results
