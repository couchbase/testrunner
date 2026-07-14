
"""GSI-focused encryption-at-rest helpers extracted from the shared EAR facade."""

import json
import random
import re
import shlex
import string
import time

from lib.membase.helper.encryption_at_rest_helper import EncryptionUtil
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
    _BUCKET_KEY_TAG_RE = re.compile(
        r"^\{service_bucket\s+"
        r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
        r"\}$"
    )


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

        Uses `LC_ALL=C grep -aF` so binary content is treated as raw single
        bytes with a fixed-string match. Intended for files whose header does
        NOT carry the key ID (Plasma and BHIVE .data files).

        LC_ALL=C is required: under a UTF-8 (or other multibyte) locale, grep
        decodes each newline-delimited "line" as multibyte text and can silently
        skip regions containing invalid byte sequences — which encrypted block
        bodies are full of — missing the ASCII key ID even when it is present.
        LC_ALL=C forces byte-wise matching and eliminates those false negatives.

        Returns:
            tuple: (found: bool, matched_key_id: str|None, details: str)
        """
        key_ids = [str(k) for k in key_ids if k not in (None, "")]
        if not key_ids:
            return False, None, "no key ids supplied"

        shell = RemoteMachineShellConnection(node, verbose=False)
        try:
            for key_id in key_ids:
                cmd = f"LC_ALL=C grep -aFl -e {shlex.quote(key_id)} {shlex.quote(file_path)} 2>/dev/null"
                output, _ = shell.execute_command(cmd)
                if output and any(line.strip() for line in output):
                    return True, key_id, f"matched {key_id}"
            return False, None, f"none of {key_ids} present in file body"
        finally:
            shell.disconnect()

    def _remote_file_size(self, node, file_path):
        """Return the size of a remote file in bytes, or -1 if it can't be read."""
        shell = RemoteMachineShellConnection(node, verbose=False)
        try:
            cmd = f"stat -c %s {shlex.quote(file_path)} 2>/dev/null"
            output, _ = shell.execute_command(cmd)
            for line in output:
                line = line.strip()
                if line.isdigit():
                    return int(line)
            return -1
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
            # Zero-length segment files genuinely cannot carry a key ID and are
            # tracked separately rather than counted as encryption failures.
            empty_files = []
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
                    continue

                # grep found no key ID. Distinguish a genuinely empty segment
                # (nothing to encrypt yet) from a non-empty file that is missing
                # its key ID (a real encryption gap). Annotate the byte size so
                # triage can spot suspiciously small / header-only segments.
                size = self._remote_file_size(node, file_path)
                if size == 0:
                    empty_files.append(
                        (file_path, "zero-length segment (no data to encrypt)")
                    )
                    self.log.info(
                        f"Node {node.ip}: {file_path} is zero-length; "
                        "no key id expected"
                    )
                else:
                    failed_files.append(
                        (file_path, f"{details} (file_size={size} bytes)")
                    )
                    self.log.warning(
                        f"Node {node.ip}: {file_path} missing key id "
                        f"(file_size={size} bytes): {details}"
                    )

            if failed_files:
                status = "failed"
            elif passed_count:
                status = "passed"
            else:
                status = "skipped"
            results[node.ip] = {
                "status": status,
                "passed_count": passed_count,
                "total_checked": len(sample_files),
                "failed_files": failed_files,
                "empty_files": empty_files,
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
                            cmd = f"LC_ALL=C grep -aFl -e '{safe}' {file_path} 2>/dev/null"
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

    @staticmethod
    def _parse_size_path(entries):
        parsed = []
        for line in entries:
            size_str, _, fpath = line.partition(' ')
            try:
                size = int(size_str)
            except ValueError:
                size = -1  # unknown — treat as non-empty to be safe
            parsed.append((fpath, size))
        return parsed

    def verify_gsi_metadata_repo_encrypted(self, index_nodes, expected_key_id=None):
        """
        Verify that metadata_repo_v2 wal and sstable files contain the expected key ID.

        Searches for any of the expected key IDs in:
          - ``@2i/metadata_repo_v2/wal/wal.*``  (WAL files)
          - ``@2i/metadata_repo_v2/kvstore-*/rev-*/*/sstable.*.data``  (SSTable files)

        Delegates to ``verify_file_contains_key_id``, which greps the full file
        body (binary-as-text) for each candidate key ID in turn using
        ``LC_ALL=C`` so invalid multibyte sequences in the encrypted binary
        body don't cause false negatives. A match on any key indicates the
        file is encrypted with that key.

        Args:
            index_nodes: List of index server objects.
            expected_key_id: Key ID string/int, or an iterable of those (e.g. the
                key ID list returned by ``_get_indexer_in_use_key_ids``). Required.

        Empty (0-byte) files — e.g. a WAL segment caught mid-rotation — are
        skipped rather than verified: an empty file has no body for `xxd` to
        find a magic header in, so treating it like any other file would
        always misclassify it as PLAINTEXT regardless of whether encryption
        is actually working.

        Returns:
            dict: ``{node_ip: {"status": "pass"/"fail"/"skipped",
                               "wal_files_checked": int,
                               "sstable_files_checked": int,
                               "skipped_empty_files": list[str],
                               "failed_files": list[tuple[str, str]]}}``
                  (path, classification)
        """
        if expected_key_id is None:
            raise ValueError("verify_gsi_metadata_repo_encrypted: expected_key_id is required")

        key_ids = self._normalize_key_ids(expected_key_id)
        if not key_ids:
            raise ValueError(
                "verify_gsi_metadata_repo_encrypted: expected_key_id normalized to an empty list"
            )
        results = {}

        for node in index_nodes:
            self.log.info(f"[metadata_repo] Checking node {node.ip} for key_ids={key_ids}")
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                rest = RestConnection(node)
                storage_dir = f"{rest.get_index_path()}/@2i"
                metadata_dir = f"{storage_dir}/metadata_repo_v2"

                # Find WAL files (with size, so empty/mid-rotation segments
                # can be skipped below instead of misclassified as PLAINTEXT).
                wal_cmd = (
                    f"find {metadata_dir}/wal -maxdepth 1 -type f "
                    f"-printf '%s %p\\n' 2>/dev/null"
                )
                wal_out, _ = shell.execute_command(wal_cmd)
                wal_entries = [f.strip() for f in wal_out if f.strip()]

                # Find SSTable files (with size)
                sst_cmd = (
                    f"find {metadata_dir} -type f -name 'sstable.*.data' "
                    f"-printf '%s %p\\n' 2>/dev/null"
                )
                sst_out, _ = shell.execute_command(sst_cmd)
                sst_entries = [f.strip() for f in sst_out if f.strip()]

                wal_parsed = self._parse_size_path(wal_entries)
                sst_parsed = self._parse_size_path(sst_entries)
                wal_files = [p for p, _ in wal_parsed]
                sst_files = [p for p, _ in sst_parsed]
                all_parsed = wal_parsed + sst_parsed

                if not all_parsed:
                    self.log.warning(
                        f"[metadata_repo] Node {node.ip}: no wal/sstable files found under {metadata_dir}"
                    )
                    results[node.ip] = {
                        "status": "skipped",
                        "reason": f"no wal/sstable files found under {metadata_dir}",
                        "wal_files_checked": 0,
                        "sstable_files_checked": 0,
                        "skipped_empty_files": [],
                        "failed_files": [],
                    }
                    continue

                failed_files = []
                skipped_empty_files = []
                for fpath, size in all_parsed:
                    if size == 0:
                        skipped_empty_files.append(fpath)
                        self.log.info(
                            f"[metadata_repo] Node {node.ip}: skipping empty (0-byte) "
                            f"file {fpath} — no content to verify encryption on "
                            f"(e.g. a WAL segment caught mid-rotation)"
                        )
                        continue
                    # Check every candidate key id (not just a single stringified
                    # value) — same pattern as verify_gsi_data_files_key_id.
                    found, matched_key, _details = self.verify_file_contains_key_id(
                        node, fpath, key_ids
                    )
                    if not found:
                        # A missing expected-key match doesn't tell us WHY it's
                        # missing — the file could be genuinely plaintext, or
                        # it could still be encrypted under a previous/stale
                        # key (e.g. a superseded sstable left behind by a
                        # compaction that rewrote the "live" data but didn't
                        # touch every old file). Disambiguate by checking for
                        # the encryption magic header independently of key ID.
                        is_encrypted, magic_details = self.verify_file_encryption_magic_bytes(
                            node, fpath
                        )
                        if is_encrypted:
                            classification = (
                                f"STALE_KEY (magic header present, but expected "
                                f"key(s) {key_ids} not found — likely encrypted "
                                f"under a previous key)"
                            )
                        else:
                            classification = (
                                f"PLAINTEXT (no encryption magic header found: "
                                f"{magic_details})"
                            )
                        failed_files.append((fpath, classification))
                        self.log.warning(
                            f"[metadata_repo] Node {node.ip}: key(s) {key_ids} "
                            f"NOT found in {fpath} — {classification}"
                        )
                    else:
                        self.log.debug(
                            f"[metadata_repo] Node {node.ip}: key '{matched_key}' found in {fpath}"
                        )

                if failed_files:
                    status = "fail"
                elif len(skipped_empty_files) == len(all_parsed):
                    # Every file found was empty — nothing was actually
                    # verified, so don't report a "pass" that implies
                    # encryption was confirmed.
                    status = "skipped"
                else:
                    status = "pass"
                self.log.info(
                    f"[metadata_repo] Node {node.ip}: {status} — "
                    f"wal={len(wal_files)}, sstable={len(sst_files)}, "
                    f"skipped_empty={len(skipped_empty_files)}, failed={len(failed_files)}"
                )
                results[node.ip] = {
                    "status": status,
                    "wal_files_checked": len(wal_files),
                    "sstable_files_checked": len(sst_files),
                    "skipped_empty_files": skipped_empty_files,
                    "failed_files": failed_files,
                }
            finally:
                shell.disconnect()

        return results

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

    def _install_tools(self):
        """
        Ensure xxd is installed on every cluster node. Called once from suite_setUp.

        xxd ships in vim-common on Debian/Ubuntu and in vim-common (or vim-enhanced)
        on RHEL/CentOS/Fedora. The check is skipped for nodes where xxd is already
        present so repeated suite runs are fast.
        """
        for node in self.servers:
            shell = RemoteMachineShellConnection(node, verbose=False)
            try:
                info = shell.extract_remote_info()
                distro = (info.distribution_type or "").lower()

                out, _ = shell.execute_command("which xxd 2>/dev/null")
                if out and out[0].strip():
                    self.log.info(f"xxd already present on {node.ip}: {out[0].strip()}")
                    continue

                self.log.info(f"Installing xxd on {node.ip} (distro={distro})")
                if "ubuntu" in distro or "debian" in distro:
                    install_out, _ = shell.execute_command(
                        "apt-get install -y xxd 2>&1"
                    )
                elif any(d in distro for d in ("centos", "rhel", "fedora", "amazon")):
                    install_out, _ = shell.execute_command(
                        "yum install -y vim-common 2>&1 || dnf install -y vim-common 2>&1"
                    )
                else:
                    install_out, _ = shell.execute_command(
                        "apt-get install -y xxd 2>&1 || yum install -y vim-common 2>&1"
                    )
                self.log.info(f"Install output on {node.ip}: {' '.join(install_out or [])}")

                verify_out, _ = shell.execute_command("which xxd 2>/dev/null")
                if verify_out and verify_out[0].strip():
                    self.log.info(f"xxd installed successfully on {node.ip}: {verify_out[0].strip()}")
                else:
                    self.log.warning(f"xxd installation may have failed on {node.ip}")
            finally:
                shell.disconnect()


# ---------------------------------------------------------------------------
# EAR test-suite base mixin
# ---------------------------------------------------------------------------

# Maps self.gsi_type strings to the three canonical engine names accepted by
# validate_engine_encryption().  The dispatcher only speaks 'moi', 'plasma',
# 'bhive'; any other string raises ValueError there.
_GSI_TYPE_TO_ENGINE = {
    "memory_optimized": "moi",
    "moi": "moi",
    "plasma": "plasma",
    "forestdb": "plasma",   # legacy alias used in conf files
    "bhive": "bhive",
}


class GSIEncryptionAtRestBase:
    """
    Mixin providing shared helper methods for GSI Encryption at Rest test suites.

    Consumed by GSIEncryptionAtRestRebalance and GSIEncryptionAtRestStaticTopology, both of
    which also inherit BaseSecondaryIndexingTests so that self.* attributes such
    as self.rest, self.master, self.cluster, etc. are available at runtime.
    """

    UUID_PATTERN = re.compile(
        r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    )

    def validate_bucket_encryption(self, bucket_name):
        bucket_info = self.rest.get_bucket_json(bucket_name)
        encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
        self.log.info(f"Bucket '{bucket_name}' encryptionAtRestKeyId: {encryption_key_id}")

        if encryption_key_id is None or encryption_key_id == -1:
            raise Exception(f"Bucket '{bucket_name}' does not have encryption at rest enabled")

        if hasattr(self, 'encryption_at_rest_id') and self.encryption_at_rest_id is not None and encryption_key_id != self.encryption_at_rest_id:
            raise Exception(f"Bucket encryption key id {encryption_key_id} does not match expected {self.encryption_at_rest_id}")

        self.log.info(f"Bucket '{bucket_name}' encryption at rest validation passed")

    def prepare_namespaces(self, bucket_name):
        self.namespaces = []

        if self.use_default_collection_only:
            namespace = f"default:{bucket_name}._default._default"
            self.namespaces.append(namespace)
            self.log.info(f"Using default collection namespace: {namespace}")
        else:
            self.prepare_collection_for_indexing(
                num_scopes=self.num_scopes,
                num_collections=self.num_collections,
                num_of_docs_per_collection=self.num_of_docs_per_collection,
                json_template=self.json_template,
                bucket_name=bucket_name
            )

        return self.namespaces

    def build_deferred_indexes(self, namespace, deferred_definitions):
        if deferred_definitions:
            build_query = self.gsi_util_obj.get_build_indexes_query(
                definition_list=deferred_definitions,
                namespace=namespace
            )
            self.log.info(f"Building deferred indexes: {build_query}")
            self.run_cbq_query(query=build_query)

    def capture_scan_stats(self):
        stats_before = {}
        stats_map = self.get_index_stats(perNode=True)

        for node in stats_map:
            for keyspace in stats_map[node]:
                for index_name in stats_map[node][keyspace]:
                    if index_name not in stats_before:
                        stats_before[index_name] = 0
                    if 'num_requests' in stats_map[node][keyspace][index_name]:
                        stats_before[index_name] += stats_map[node][keyspace][index_name]['num_requests']

        return stats_before

    def run_and_validate_scans(self, select_queries, expected_doc_count):
        self.log.info(f"Running {len(select_queries)} scan queries for validation")

        for query in select_queries:
            self.log.info(f"Running scan query: {query[:100]}...")
            result = self.run_cbq_query(query=query, scan_consistency='request_plus')

            self.assertIn('results', result, f"No results returned for scan query: {query[:100]}")

            result_count = len(result.get('results', []))
            if result_count > 0:
                self.log.info(f"Scan query returned {result_count} results")
            else:
                self.log.info(f"Scan query completed (no results returned)")

    def validate_scan_stats_increment(self, stats_before):
        stats_after = self.get_index_stats(perNode=True)

        stats_after_agg = {}
        for node in stats_after:
            for keyspace in stats_after[node]:
                for index_name in stats_after[node][keyspace]:
                    if index_name not in stats_after_agg:
                        stats_after_agg[index_name] = 0
                    if 'num_requests' in stats_after[node][keyspace][index_name]:
                        stats_after_agg[index_name] += stats_after[node][keyspace][index_name]['num_requests']

        total_before = sum(stats_before.values())
        total_after = sum(stats_after_agg.values())

        self.assertGreater(total_after, total_before,
            f"Total num_requests did not increase after scans (before={total_before}, after={total_after})")

        self.log.info(f"Total num_requests: before={total_before}, after={total_after}")
        self.log.info(f"Scan activity validated across {len(stats_after_agg)} indexes")

    def create_bucket_with_encryption(self, bucket_name, enable_encryption=True):
        self.bucket_params = self._create_bucket_params(
            server=self.master,
            size=self.bucket_size,
            replicas=self.num_replicas,
            bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy,
            lww=self.lww
        )
        self.cluster.create_standard_bucket(name=bucket_name, port=11222, bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()

        if enable_encryption:
            if hasattr(self, 'encryption_at_rest_id') and self.encryption_at_rest_id is not None:
                self.log.info(f"Enabling encryption at rest on bucket '{bucket_name}' with secret id {self.encryption_at_rest_id}")
                status, response = self.rest.enable_bucket_encryption(bucket_name, self.encryption_at_rest_id)
                if not status:
                    raise Exception(f"Failed to enable encryption at rest on bucket {bucket_name}: {response}")
                self.log.info(f"Encryption at rest enabled successfully on bucket '{bucket_name}'")
            else:
                raise Exception("encryption_at_rest_id not available. Ensure enable_encryption_at_rest=True is set in test params")
        else:
            self.log.info(f"Created bucket '{bucket_name}' without encryption at rest")

        return bucket_name

    def create_new_encryption_key(self):
        params = EncryptionUtil.create_secret_params(
            name=EncryptionUtil.generate_random_name("RotationKey"),
            rotationIntervalInSeconds=self.secret_rotation_interval if hasattr(self, 'secret_rotation_interval') else 60
        )
        self.log.info(f"Creating new encryption key with params: {params}")
        status, response = self.rest.create_secret(params)
        if not status:
            raise Exception(f"Failed to create new encryption key: {response}")

        response_dict = json.loads(response)
        new_key_id = response_dict.get('id')
        if new_key_id is None:
            raise Exception(f"New encryption key created but no ID returned: {response}")

        self.log.info(f"Created new encryption key with ID: {new_key_id}")
        return new_key_id

    def set_bucket_dek_rotation_config(self, bucket_name, new_key_id, dek_rotation_interval=120, dek_lifetime=120):
        self.log.info(f"Rotating bucket '{bucket_name}' encryption to key ID {new_key_id}")

        self.log.info(f"Setting DEK rotation interval to {dek_rotation_interval} seconds")
        status, response = self.rest.set_bucket_dek_rotation_interval(bucket_name, dek_rotation_interval)
        if not status:
            raise Exception(f"Failed to set DEK rotation interval: {response}")

        self.log.info(f"Setting DEK lifetime to {dek_lifetime} seconds")
        status, response = self.rest.set_bucket_dek_lifetime(bucket_name, dek_lifetime)
        if not status:
            raise Exception(f"Failed to set DEK lifetime: {response}")

        self.log.info(f"Bucket '{bucket_name}' DEK rotation configured with interval={dek_rotation_interval}s, lifetime={dek_lifetime}s")

    def extract_header_key_id_candidates(self, value, candidates):
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                key_lower = nested_key.lower()
                if isinstance(nested_value, (str, int)):
                    value_str = str(nested_value)
                    if key_lower.endswith("id") or self.UUID_PATTERN.match(value_str):
                        candidates.add(value_str)
                self.extract_header_key_id_candidates(nested_value, candidates)
        elif isinstance(value, list):
            for item in value:
                self.extract_header_key_id_candidates(item, candidates)
        elif isinstance(value, str) and self.UUID_PATTERN.match(value):
            candidates.add(value)

    def get_expected_header_key_ids(self, expected_key_id):
        if expected_key_id in [None, ""]:
            return []

        candidates = {str(expected_key_id)}
        try:
            status, response = self.rest.get_specific_secret(expected_key_id)
            if not status:
                self.log.warning(
                    f"Failed to fetch secret details for key ID {expected_key_id}; "
                    f"falling back to direct key ID matching"
                )
                return list(candidates)

            if isinstance(response, bytes):
                response = response.decode("utf-8", errors="replace")
            response_dict = json.loads(response) if isinstance(response, str) else response
            self.extract_header_key_id_candidates(response_dict, candidates)
        except Exception as err:
            self.log.warning(
                f"Unable to resolve header key identifiers for key ID {expected_key_id}: {err}"
            )

        return sorted(candidates)

    def extract_in_use_key_ids(self, value, key_ids):
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                if nested_key.lower().endswith("keyid") and nested_value not in [None, ""]:
                    key_ids.add(str(nested_value))
                self.extract_in_use_key_ids(nested_value, key_ids)
        elif isinstance(value, list):
            for item in value:
                self.extract_in_use_key_ids(item, key_ids)
        elif isinstance(value, (str, int)) and value not in [None, ""]:
            value_str = str(value)
            if self.UUID_PATTERN.match(value_str):
                key_ids.add(value_str)

    def get_indexer_in_use_key_ids(self, index_nodes):
        key_ids = set()
        for index_node in index_nodes:
            rest = RestConnection(index_node)
            status, response = rest.get_indexer_in_use_encryption_keys()
            if not status:
                raise Exception(
                    f"Failed to fetch in-use encryption keys from index node {index_node.ip}: {response}"
                )
            self.extract_in_use_key_ids(response, key_ids)

        if not key_ids:
            raise Exception("Indexer GetInUseKeys returned no key IDs")

        resolved_key_ids = sorted(key_ids)
        self.log.info(f"Resolved in-use indexer key IDs: {resolved_key_ids}")
        return resolved_key_ids

    def get_new_indexer_in_use_key_ids(self, index_nodes, previous_key_ids):
        current_key_ids = set(self.get_indexer_in_use_key_ids(index_nodes))
        previous_key_ids = {str(key_id) for key_id in previous_key_ids}
        new_key_ids = sorted(current_key_ids.difference(previous_key_ids))
        self.log.info(
            f"Current in-use indexer key IDs: {sorted(current_key_ids)}, "
            f"previous key IDs: {sorted(previous_key_ids)}, new key IDs: {new_key_ids}"
        )
        return new_key_ids

    def poll_for_new_indexer_key_ids(self, index_nodes, baseline_key_ids, timeout=300, label=""):
        baseline = {str(k) for k in baseline_key_ids}
        deadline = time.time() + timeout
        while time.time() < deadline:
            new_ids = self.get_new_indexer_in_use_key_ids(index_nodes, baseline_key_ids)
            if new_ids:
                self.log.info(
                    f"{label}New indexer key IDs detected: {new_ids} "
                    f"(baseline was {sorted(baseline)})"
                )
                return new_ids
            self.sleep(15, f"{label}Waiting for new indexer key IDs (baseline: {sorted(baseline)})")
        self.fail(
            f"{label}No new indexer key IDs appeared within {timeout} s. "
            f"Baseline: {sorted(baseline)}"
        )

    def verify_prerequisites(self, min_index_nodes=3, check_encryption=True, step_prefix="[STEP 1]"):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.log.info(f"{step_prefix} Found {len(index_nodes)} index nodes: {[n.ip for n in index_nodes]}")
        self.assertGreaterEqual(len(index_nodes), min_index_nodes,
            f"Need at least {min_index_nodes} index nodes, found {len(index_nodes)}")
        if check_encryption:
            self.assertTrue(self.enable_encryption_at_rest,
                "Encryption at rest not enabled. Set enable_encryption_at_rest=True")
            self.assertIsNotNone(self.encryption_at_rest_id, "encryption_at_rest_id is None")
            self.log.info(f"{step_prefix} Encryption key ID: {self.encryption_at_rest_id}")
        self.log.info(f"{step_prefix} PASSED - Prerequisites verified")
        return index_nodes

    def set_indexer_snapshot_settings(self, index_nodes, step_prefix="[STEP 2]"):
        for index_node in index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 60000})
            rest.set_index_settings({"indexer.settings.persisted_snapshot_init_build.moi.interval": 60000})
        self.log.info(f"{step_prefix} PASSED - Indexer snapshot settings configured")

    def create_indexes_on_namespace(self, namespace, index_nodes, prefix_str,
                                      step_prefix="[STEP]", wait_for_ready=False,
                                      defer_build=None, randomise_replica_count=True):
        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in index_nodes]
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        _defer = self.defer_build if defer_build is None else defer_build
        prefix = prefix_str + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
        query_definitions = self.gsi_util_obj.get_index_definition_list(
            dataset=self.json_template, prefix=prefix, skip_primary=False
        )
        select_queries = set(self.gsi_util_obj.get_select_queries(
            definition_list=query_definitions, namespace=namespace
        ))
        create_kwargs = dict(
            definition_list=query_definitions, namespace=namespace,
            num_replica=self.num_index_replica, deploy_node_info=deploy_nodes,
            defer_build=_defer
        )
        if randomise_replica_count:
            create_kwargs['randomise_replica_count'] = True
        queries = self.gsi_util_obj.get_create_index_list(**create_kwargs)
        self.gsi_util_obj.create_gsi_indexes(
            create_queries=queries, database=namespace, query_node=query_node
        )
        if wait_for_ready:
            self.wait_until_indexes_online(timeout=1200, defer_build=_defer)
        suffix = " in Ready state" if wait_for_ready else ""
        self.log.info(f"{step_prefix} PASSED - Created {len(queries)} indexes{suffix}")
        return select_queries

    def restart_index_nodes(self, index_nodes):
        """Rolling restart of all indexer nodes one at a time."""
        for node in index_nodes:
            self.log.info(f"[RESTART] Stopping indexer on node {node.ip}...")
            remote = RemoteMachineShellConnection(node)
            remote.stop_server()
            self.sleep(20, f"Waiting after stopping {node.ip}")
            self.log.info(f"[RESTART] Starting indexer on node {node.ip}...")
            remote.start_server()
            remote.disconnect()
            self.sleep(20, f"Waiting for {node.ip} to come back up")
        self.log.info("[RESTART] All index nodes restarted. Waiting for indexes to be online...")
        self.wait_until_indexes_online(timeout=1200)
        self.log.info("[RESTART] All indexes back online after restart")

    def run_scan_validation(self, select_queries, validate_item_count=False, expected_doc_count=None):
        if expected_doc_count is None:
            expected_doc_count = self.num_of_docs_per_collection
        stats_before = self.capture_scan_stats()
        self.run_and_validate_scans(select_queries, expected_doc_count=expected_doc_count)
        self.validate_scan_stats_increment(stats_before)
        if validate_item_count:
            self.item_count_related_validations()

    def validate_mixed_bucket_encrypted_keys(self, index_nodes, encrypted_bucket_names, timeout=300, step_prefix=""):
        """
        Validate in-use encryption keys for a mixed-bucket setup (some encrypted, some not).

        Replaces normal file-level validation when encrypt_all_buckets=False and
        bucket_count >= 2. The unencrypted bucket's service_bucket entries will
        naturally have no keys, so the standard _wait_for_empty_service_bucket_keys_cleared
        check would never converge. Instead this method:

          1. Resolves the UUID for each encrypted bucket via build_bucket_key_map.
          2. Polls get_indexer_in_use_encryption_keys on all index nodes for up to
             `timeout` seconds (default 300 s / 5 min).
          3. On every poll, checks only the {service_bucket <uuid>} entries that
             belong to an encrypted bucket.
          4. If any such entry has an empty key ID, keeps waiting.
          5. Once all encrypted-bucket service_bucket entries have non-empty key IDs
             on every index node, returns (validation passed).
          6. If the deadline is reached without convergence, calls self.fail() so the
             test is marked as a hard failure.
        """
        self.log.info(
            f"{step_prefix}Mixed-bucket validation: polling in-use keys for encrypted "
            f"buckets {encrypted_bucket_names} (timeout={timeout}s)..."
        )

        # Step 1 — resolve encrypted bucket UUIDs from the first index node.
        bucket_key_map = self._build_bucket_key_map_safe(index_nodes, encrypted_bucket_names)
        encrypted_uuids = set()
        for bucket_name, info in bucket_key_map.items():
            uuid = info.get("uuid", "")
            if uuid:
                encrypted_uuids.add(uuid.lower())
            else:
                self.log.warning(
                    f"{step_prefix}Could not resolve UUID for encrypted bucket '{bucket_name}' — "
                    f"it will be skipped in key validation"
                )

        if not encrypted_uuids:
            self.fail(
                f"{step_prefix}No UUIDs resolved for encrypted buckets {encrypted_bucket_names}; "
                f"cannot validate in-use keys"
            )

        self.log.info(f"{step_prefix}Resolved encrypted bucket UUIDs: {sorted(encrypted_uuids)}")

        # Step 2-5 — poll until all encrypted-bucket service_bucket entries are non-empty.
        deadline = time.time() + timeout
        poll_interval = 15
        while time.time() < deadline:
            found_empty = False
            for index_node in index_nodes:
                rest = RestConnection(index_node)
                status, response = rest.get_indexer_in_use_encryption_keys()
                if not status:
                    self.log.warning(
                        f"{step_prefix}Could not fetch in-use keys from {index_node.ip}: {response}; retrying..."
                    )
                    found_empty = True
                    break
                for bucket_key, key_list in response.items():
                    if not bucket_key.startswith("{service_bucket"):
                        continue
                    # Extract the UUID from the "{service_bucket <uuid>}" tag.
                    inner = bucket_key.strip("{} ")
                    parts = inner.split()
                    if len(parts) < 2:
                        continue
                    entry_uuid = parts[1].lower()
                    if entry_uuid not in encrypted_uuids:
                        continue
                    if any(k == "" for k in key_list):
                        self.log.info(
                            f"{step_prefix}Encrypted bucket UUID {entry_uuid} still has an empty "
                            f"key in {bucket_key} on {index_node.ip} — waiting for DEK to populate..."
                        )
                        found_empty = True
                        break
                if found_empty:
                    break

            if not found_empty:
                self.log.info(
                    f"{step_prefix}All encrypted-bucket service_bucket entries have non-empty "
                    f"key IDs on all index nodes — validation passed"
                )
                return

            time.sleep(poll_interval)

        # Step 6 — hard failure after timeout.
        self.fail(
            f"{step_prefix}Encrypted-bucket service_bucket entries still had empty key IDs after "
            f"{timeout}s. Encrypted bucket UUIDs checked: {sorted(encrypted_uuids)}"
        )

    def get_bucket_name(self, bucket):
        """
        Return a bucket name from REST bucket objects, dicts, or strings.
        """
        if isinstance(bucket, str):
            return bucket
        if isinstance(bucket, dict):
            return bucket.get("name") or bucket.get("bucket") or bucket.get("bucketName")
        return getattr(bucket, "name", None) or getattr(bucket, "bucket", None) or str(bucket)

    def is_bucket_encryption_enabled(self, bucket_name):
        """
        Check whether bucket encryption is currently enabled.
        """
        bucket_info = self.rest.get_bucket_json(bucket_name)
        encryption_key_id = bucket_info.get('encryptionAtRestKeyId', None)
        return encryption_key_id not in [None, -1, "-1"]

    def get_current_encrypted_bucket_names(self, bucket_names=None):
        """
        Return only buckets that currently have encryption enabled.
        """
        if bucket_names is None:
            bucket_names = [self.get_bucket_name(bucket) for bucket in self.rest.get_buckets()]

        encrypted_bucket_names = []
        for bucket_name in bucket_names:
            if not bucket_name:
                continue
            if self.is_bucket_encryption_enabled(bucket_name):
                encrypted_bucket_names.append(bucket_name)
            else:
                self.log.info(
                    f"Skipping bucket '{bucket_name}' for file encryption validation; encryption is disabled"
                )

        return encrypted_bucket_names

    def validate_file_encryption_with_key(self, index_nodes, expected_key_id=None, step_prefix="",
                                            encrypted_bucket_names=None, raise_on_failure=False):
        """
        Validate file encryption on disk and optionally check for expected key IDs in headers.

        Args:
            index_nodes: List of index nodes to validate
            expected_key_id: Optional expected encryption key ID(s) in file headers.
                             Can be a single key ID, list/tuple/set of key IDs, or None.
            step_prefix: Prefix for logging steps (e.g., "[STEP 10] ")
            encrypted_bucket_names: Optional list of bucket names that should be encrypted.
                                    Used for more targeted validation logging.
            raise_on_failure: If True, raises an exception on validation failure.
                              If False, returns False on failure. Default: False.

        Returns:
            bool: True if validation passed, False otherwise

        Raises:
            Exception: If raise_on_failure=True and validation fails
        """
        encrypted_bucket_names = self.get_current_encrypted_bucket_names(encrypted_bucket_names)
        if not encrypted_bucket_names:
            self.log.info(f"{step_prefix}No encryption-enabled buckets found; skipping file encryption validation")
            return True

        if expected_key_id is None:
            expected_header_key_ids = self.get_indexer_in_use_key_ids(index_nodes)
            self.log.info(
                f"{step_prefix}Validating file encryption on disk with in-use key IDs "
                f"{expected_header_key_ids}..."
            )
        elif isinstance(expected_key_id, (list, tuple, set)):
            expected_header_key_ids = [str(key_id) for key_id in expected_key_id if key_id not in [None, ""]]
            self.log.info(
                f"{step_prefix}Validating file encryption with expected key IDs {expected_header_key_ids}..."
            )
        else:
            expected_header_key_ids = self.get_expected_header_key_ids(expected_key_id)
            self.log.info(
                f"{step_prefix}Validating file encryption with expected key ID {expected_key_id} "
                f"using header key identifiers {expected_header_key_ids}..."
            )

        self.log.info(f"{step_prefix}Encrypted buckets to validate: {encrypted_bucket_names}")

        engine = _GSI_TYPE_TO_ENGINE.get(self.gsi_type)
        if engine is None:
            raise ValueError(
                f"Unsupported gsi_type: {self.gsi_type!r}. "
                f"Expected one of {list(_GSI_TYPE_TO_ENGINE)}"
            )

        encryption_results = self.validate_engine_encryption(
            engine, index_nodes,
            expected_key_id=expected_header_key_ids,
            encrypted_bucket_names=encrypted_bucket_names,
        )

        all_failed_files = []
        validation_passed = True
        _SKIP_CATEGORIES = {"_overall", "_helper_error", "TODO"}

        for category, results in encryption_results.items():
            if category in _SKIP_CATEGORIES:
                continue
            if isinstance(results, dict):
                for node, result in results.items():
                    if not isinstance(result, dict):
                        continue
                    status = result.get("status", "unknown")
                    if status == "passed":
                        failed_files = result.get("failed_files", [])
                        if failed_files:
                            self.log.warning(
                                f"{step_prefix}{category} on {node}: PASSED with "
                                f"{len(failed_files)} unencrypted files"
                            )
                            for file_path, details in failed_files:
                                self.log.error(
                                    f"{step_prefix}UNENCRYPTED FILE on {node}: {file_path} - {details}"
                                )
                                all_failed_files.append((node, file_path, details))
                        else:
                            self.log.info(
                                f"{step_prefix}{category} on {node}: PASSED - All files encrypted"
                            )
                    elif status == "failed":
                        validation_passed = False
                        self.log.error(
                            f"{step_prefix}{category} on {node}: FAILED - "
                            f"{result.get('reason', 'unknown')}"
                        )
                        failed_files = result.get("failed_files", [])
                        if failed_files:
                            self.log.error(
                                f"{step_prefix}=== UNENCRYPTED FILES on {node} "
                                f"({len(failed_files)} files) ==="
                            )
                            for file_path, details in failed_files:
                                self.log.error(f"{step_prefix}UNENCRYPTED FILE: {file_path}")
                                self.log.error(f"{step_prefix}  Details: {details}")
                                all_failed_files.append((node, file_path, details))
                    elif status == "skipped":
                        self.log.warning(
                            f"{step_prefix}{category} on {node}: SKIPPED - "
                            f"{result.get('reason', 'unknown')}"
                        )

        if all_failed_files:
            self.log.error("=" * 60)
            self.log.error(
                f"{step_prefix}SUMMARY: Found {len(all_failed_files)} UNENCRYPTED FILES:"
            )
            for node, file_path, details in all_failed_files:
                self.log.error(f"{step_prefix}  Node: {node}, File: {file_path}")
            self.log.error("=" * 60)
            if raise_on_failure:
                raise Exception(
                    f"File encryption validation failed: {len(all_failed_files)} files are not encrypted"
                )
            return False

        if not validation_passed:
            if raise_on_failure:
                raise Exception("File encryption validation reported one or more failures")
            return False

        self.log.info(f"{step_prefix}File encryption validation completed successfully")
        return True

    def validate_files_not_encrypted(self, index_nodes, bucket_name, timeout=300, poll_interval=15, step_prefix=""):
        """
        Poll index nodes until all index files for bucket_name are confirmed NOT encrypted.

        Resolves the bucket UUID, then on each poll SSHes to every index node,
        finds all files under @2i whose path contains the bucket UUID, and checks
        each file's magic bytes via verify_file_encryption_magic_bytes. Polling
        continues while any file still carries the encryption header. After timeout
        seconds without full convergence, self.fail() is called.

        Args:
            index_nodes: List of index nodes to check.
            bucket_name: Bucket whose index files must be plaintext.
            timeout: Max seconds to wait (default 300 / 5 min).
            poll_interval: Seconds between polls (default 15).
            step_prefix: Log prefix string.
        """
        self.log.info(
            f"{step_prefix}Polling until all index files for bucket '{bucket_name}' "
            f"are decrypted (timeout={timeout}s)..."
        )

        bucket_key_map = self._build_bucket_key_map_safe(index_nodes, [bucket_name])
        bucket_uuid = (bucket_key_map.get(bucket_name) or {}).get("uuid", "")
        if not bucket_uuid:
            self.fail(
                f"{step_prefix}Could not resolve UUID for bucket '{bucket_name}' — "
                f"cannot locate index files on disk"
            )

        self.log.info(f"{step_prefix}Bucket '{bucket_name}' UUID: {bucket_uuid}")
        deadline = time.time() + timeout

        while time.time() < deadline:
            still_encrypted = []
            for node in index_nodes:
                rest = RestConnection(node)
                index_path = rest.get_index_path()
                storage_dir = f"{index_path}/@2i"
                shell = RemoteMachineShellConnection(node, verbose=False)
                try:
                    out, _ = shell.execute_command(
                        f"find {storage_dir} -type f 2>/dev/null | grep -i '{bucket_uuid}'"
                    )
                finally:
                    shell.disconnect()

                files = [l.strip() for l in out if l.strip()]
                if not files:
                    self.log.info(
                        f"{step_prefix}{node.ip}: no index files found for UUID {bucket_uuid} — "
                        f"treating as decrypted"
                    )
                    continue

                for fpath in files:
                    is_enc, details = self.gsi_encryption_helper.verify_file_encryption_magic_bytes(
                        node, fpath
                    )
                    if is_enc:
                        still_encrypted.append((node.ip, fpath))
                        self.log.info(f"{step_prefix}{node.ip}: file still encrypted: {fpath}")

            if not still_encrypted:
                self.log.info(
                    f"{step_prefix}All index files for bucket '{bucket_name}' "
                    f"(UUID={bucket_uuid}) are plaintext — force decryption complete"
                )
                return

            self.log.info(
                f"{step_prefix}{len(still_encrypted)} file(s) still encrypted, "
                f"waiting {poll_interval}s before retry..."
            )
            time.sleep(poll_interval)

        self.fail(
            f"{step_prefix}Index files for bucket '{bucket_name}' (UUID={bucket_uuid}) "
            f"were still encrypted after {timeout}s. Force decryption did not complete in time."
        )
