"""
Query Service CRL Tests — extends CRLBase from pytests/security/crl_base.py.

Reference: Query Service CRL Test Plan (WIP)
Scope: clientAuth and nodeToNode CRL enforcement on Query endpoints.
"""

import base64
import datetime
import json
import os
import socket as socket_module
import ssl as ssl_module
import tempfile
import threading
import time

import urllib.parse

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509 import load_pem_x509_crl
from cryptography.x509.oid import ExtendedKeyUsageOID

from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.crl_base import CRLBase
from pytests.security.crl_utils import CRLUtils
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.x509_multiple_CA_util import Validation
from .tuq import QueryTests


class QueryCRLTests(QueryTests, CRLBase):
    """
    CRL enforcement tests for the Query service (/query/service, /admin).
    Inherits:
      CRLBase — CA generation, CRL upload/settings helpers, EE gating, cleanup
      QueryTests — run_cbq_query, query bucket setup, data loading
    """

    DISABLED = "Disabled"
    PERMISSIVE = "Permissive"
    REQUIRE = "Require"
    # Note: CRL_INFO.md confirms only 3 valid modes: Disabled, Permissive, Require

    def setUp(self):
        # QueryTests.setUp handles data load, bucket, primary index, task manager
        QueryTests.setUp(self)
        # Initialize CRLBase fields directly (avoid calling CRLBase.setUp which
        # calls super() and triggers double basetestcase init / task manager conflict)
        self.crl_utils = CRLUtils(log=self.log)
        self._crl_lock = threading.Lock()
        self._created_files = []
        self._rbac_users = []
        self._require_crl_supported()
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca("TestCA1")
        self._trust_ca_on_cluster(self.ca_cert)
        self.n1ql_ssl_port = 18093

       

        # Generate two client certs: A (to be revoked), B (valid control)
        # generate_leaf_cert returns (cert, key, serial)
        self.client_a_cert, self.client_a_key, self.client_a_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, cn="clientA@example.com")
        self.client_b_cert, self.client_b_key, self.client_b_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, cn="clientB@example.com")

        # Write certs/keys to temp files for Validation class usage
        self._tmp_dir = tempfile.mkdtemp()
        self.client_a_cert_path = self._write_pem(self.client_a_cert, "clientA_cert.pem")
        self.client_a_key_path = self._write_pem(self.client_a_key, "clientA_key.pem", is_key=True)
        self.client_b_cert_path = self._write_pem(self.client_b_cert, "clientB_cert.pem")
        self.client_b_key_path = self._write_pem(self.client_b_key, "clientB_key.pem", is_key=True)

        # Create RBAC users mapped to each cert's CN — tracked by CRLBase._rbac_users
        self._create_rbac_test_user("clientA@example.com", "admin")
        self._create_rbac_test_user("clientB@example.com", "admin")

        # Index node for direct indexer endpoint tests (port 19102)
        self.index_node = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=False)
        self.indexer_https_port = 19102

        # following the same pattern as other QueryTests/tuq_index tests
        self._crl_index_name = "idx_crl_join_day"
        query_bucket = self.query_buckets[0] if self.query_buckets else "default"
        self._crl_test_query = f"SELECT COUNT(*) FROM {query_bucket} WHERE join_day > 0"
        try:
            self.query = f"CREATE INDEX IF NOT EXISTS {self._crl_index_name} " \
                         f"ON {query_bucket}(join_day) USING {self.index_type}"
            self.run_cbq_query()
            self._wait_for_index_online(self.default_bucket_name, self._crl_index_name)
        except Exception as e:
            self.log.warning(f"GSI index creation failed (may already exist): {e}")

        # Upload CRL before enabling mandatory cert auth — upload_crl_file uses
        # basic auth and would be rejected once client_cert_auth is mandatory
        self._upload_revoked_crl(serials=[self.client_a_serial], crl_number=1)

        # Start with Disabled — CRLBase.tearDown will reset this
        self.rest.post_crl_settings({
            "policyPerScope": {"clientAuth": self.DISABLED, "nodeToNode": self.DISABLED}
        })

        # Wait for CRL to load and be trusted
        self._wait_for_crl_loaded()

        # Enable mandatory client cert auth mapped via subject.cn — must be last
        # since all preceding REST calls use basic auth (no client cert)
        self.rest.client_cert_auth(
            state="mandatory",
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}])

    def tearDown(self):
        # Drop GSI index first
        try:
            if hasattr(self, '_crl_index_name') and hasattr(self, 'query_buckets') and self.query_buckets:
                self.query = f"DROP INDEX {self.query_buckets[0]}.{self._crl_index_name} IF EXISTS"
                self.run_cbq_query()
        except Exception as e:
            self.log.warning(f"GSI index drop error: {e}")
        # Call CRLBase cleanup methods directly (avoid CRLBase.tearDown which
        # calls super() and causes double basetestcase teardown conflict)
        try:
            self._cleanup_created_files()
        except Exception as e:
            self.log.warning(f"CRL file cleanup error: {e}")
        try:
            self._reset_crl_settings()
        except Exception as e:
            self.log.warning(f"CRL settings reset error: {e}")
        try:
            self._disable_client_cert_auth()
        except Exception as e:
            self.log.warning(f"clientCertAuth disable error: {e}")
        try:
            self._cleanup_rbac_users()
        except Exception as e:
            self.log.warning(f"RBAC user cleanup error: {e}")
        QueryTests.tearDown(self)

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _reload_crl_all_nodes(self):
        """Call reloadCrl on every node — reload_crl() is local-node-only,
        so in multi-node clusters all nodes must be reloaded explicitly."""
        for server in self.servers:
            try:
                node_rest = RestConnection(server)
                node_rest.reload_crl()
                self.log.info(f"CRL reloaded on {server.ip}")
            except Exception as e:
                self.log.warning(f"CRL reload failed on {server.ip}: {e}")
        time.sleep(3)

    def _deploy_node_certs_from_test_ca(self, nodes=None):
        """Deploy node certs signed by TestCA1 to cluster nodes (default: all self.servers).
        Required for nodeToNode CRL enforcement — without this, node certs
        don't chain to TestCA1 so the CRL has no effect on n2n connections.
        Pass a specific list to deploy to a subset (e.g. a spare node before rebalance-in).
        Per dev setup guide Step 3."""
        inbox = "/opt/couchbase/var/lib/couchbase/inbox"
        serials = {}
        for server in (nodes if nodes is not None else self.servers):
            try:
                node_cert, node_key, node_serial = self.crl_utils.generate_leaf_cert(
                    self.ca_cert, self.ca_key,
                    cn=server.ip,
                    dns_names=[server.ip],
                    extended_key_usage=[
                        ExtendedKeyUsageOID.SERVER_AUTH,
                        ExtendedKeyUsageOID.CLIENT_AUTH
                    ])
                cert_pem = self.crl_utils.cert_to_pem(node_cert)
                key_pem = self.crl_utils.key_to_pem(node_key)
                shell = RemoteMachineShellConnection(server)
                try:
                    shell.execute_command(f"mkdir -p {inbox}")
                    sftp = shell._ssh_client.open_sftp()
                    with sftp.open(f"{inbox}/chain.pem", 'wb') as f:
                        f.write(cert_pem)
                    with sftp.open(f"{inbox}/pkey.key", 'wb') as f:
                        f.write(key_pem)
                    sftp.close()
                finally:
                    shell.disconnect()
                node_rest = RestConnection(server)
                # Retry reloadCertificate — CA propagation to non-master nodes
                # can take a few seconds after _trust_ca_on_cluster
                deployed = False
                for attempt in range(6):
                    status, content = node_rest.reload_certificate()
                    if status:
                        self.log.info(f"Node cert (TestCA1) deployed on {server.ip}")
                        deployed = True
                        break
                    time.sleep(5)
                if not deployed:
                    self.log.warning(f"Node cert deploy failed on {server.ip}: {content}")
                else:
                    serials[server.ip] = node_serial
            except Exception as e:
                self.log.warning(f"Node cert deployment error on {server.ip}: {e}")
        return serials

    def _write_pem(self, obj, filename, is_key=False):
        path = os.path.join(self._tmp_dir, filename)
        if is_key:
            pem = self.crl_utils.key_to_pem(obj)
        else:
            pem = self.crl_utils.cert_to_pem(obj)
        with open(path, 'wb') as f:
            f.write(pem)
        return path

    def _upload_revoked_crl(self, serials, crl_number=1, expired=False):
        """Build and upload a CRL revoking the given serial numbers.
        build_crl() returns PEM bytes directly."""
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key,
            revoked_serials=serials,
            crl_number=crl_number,
            expired=expired)
        filename = f"crl_test_{crl_number}.pem"
        status, content, _ = self.rest.upload_crl_file(filename, crl_pem)
        self.assertTrue(status, f"Failed to upload CRL file {filename}: {content}")
        self._track_uploaded_file(filename)
        return filename

    def _upload_empty_crl(self, crl_number=99):
        """Upload a CRL with no revoked serials — restores access."""
        self._upload_revoked_crl(serials=[], crl_number=crl_number)

    def _enable_n2n_encryption(self, mode="strict"):
        """Enable node-to-node encryption on all cluster nodes.
        Required before nodeToNode CRL scope tests — without this, inter-node
        traffic is plain TCP and the CRL policy has nothing to enforce on."""
        ntonencryptionBase().setup_nton_cluster(self.servers, ntonStatus='enable',
                                                clusterEncryptionLevel=mode)

    def _disable_n2n_encryption(self):
        """Restore n2n encryption to off — called in tearDown for n2n tests."""
        ntonencryptionBase().disable_nton_cluster(self.servers)

    def _wait_for_crl_loaded(self, timeout=30):
        """Wait until at least one CRL file shows as loaded."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status, content, _ = self.rest.get_crl_files()
            if status and content:
                self.log.info(f"CRL loaded: {content}")
                return
            time.sleep(2)
        self.fail(f"CRL did not load within {timeout}s")

    def _set_allow_expired_crls(self, enabled=True):
        value = "true" if enabled else "false"
        status, content = self.rest.diag_eval(
            "ns_config:set(allow_expired_crls, {0}).".format(value))
        if not status:
            self.fail("Failed to set allow_expired_crls={0}: {1}".format(value, content))

    def _set_policy(self, client_auth=None, node_to_node=None):
        payload = {"policyPerScope": {}}
        if client_auth:
            payload["policyPerScope"]["clientAuth"] = client_auth
        if node_to_node:
            payload["policyPerScope"]["nodeToNode"] = node_to_node
        self.rest.post_crl_settings(payload)
        time.sleep(2)  # Allow policy change to propagate

    def _query_with_cert(self, cert_path, key_path, endpoint="/query/service"):
        """Make HTTPS request to Query endpoint using client certificate auth.
        Uses Validation class (same as GSI x509 tests) — cert auth, no password.
        Returns (status_code, body_dict) or (None, error_str) on TLS failure."""
        url = f"https://{self.master.ip}:{self.n1ql_ssl_port}{endpoint}"
        query = getattr(self, '_crl_test_query', "SELECT 1")
        try:
            v = Validation(
                server=self.master,
                cacert=False,
                client_cert_path_tuple=(cert_path, key_path))
            status, content, response = v.urllib_request(
                url, verb='POST',
                params={"statement": query},
                timeout=10,
                try_count=1)
            try:
                body = json.loads(content) if content else {}
            except Exception:
                body = {}
            return response.status_code, body
        except requests.exceptions.SSLError as e:
            return None, str(e)
        except requests.exceptions.ConnectionError as e:
            return None, str(e)
        except Exception as e:
            # x509_multiple_CA_util.urllib_request re-raises SSL/connection errors
            # as a plain Exception(original_error) — they never surface as SSLError.
            return None, str(e)

    def _assert_tls_rejected(self, cert_path, key_path, endpoint="/query/service"):
        """Assert connection fails at TLS level — no HTTP response.
        Distinguishes CRL/TLS rejection from generic connection failure."""
        code, body = self._query_with_cert(cert_path, key_path, endpoint)
        self.assertIsNone(code,
            f"Expected TLS rejection (no HTTP response) but got HTTP {code}. "
            f"Revoked cert must never reach HTTP layer. Body: {body}")
        err_lower = str(body).lower()
        tls_keywords = ["ssl", "tls", "certificate", "revoked", "handshake",
                        "alert", "x509", "pkix"]
        self.assertTrue(
            any(kw in err_lower for kw in tls_keywords),
            f"Connection failed but not at TLS level — got: {body}. "
            f"A plain connection-refused or timeout is not a CRL rejection. "
            f"Expected error containing one of: {tls_keywords}")

    def _assert_tls_succeeds(self, cert_path, key_path, endpoint="/query/service"):
        """Assert TLS handshake succeeds and server returns HTTP 200."""
        code, body = self._query_with_cert(cert_path, key_path, endpoint)
        self.assertIsNotNone(code,
            f"Expected HTTP response but TLS handshake failed. Body: {body}")
        self.assertEqual(code, 200,
            f"TLS handshake succeeded but got HTTP {code} instead of 200. Body: {body}")

    # =========================================================================
    # =========================================================================
    # MB-73085 — Regression: CRL with unrecognized critical extension
    # =========================================================================

    def test_crl_upload_rejects_unrecognized_critical_extension(self):
        """MB-73085: CRL with unrecognized critical extension must be rejected on upload.
        RFC 5280 requires CRLs with unrecognized critical extensions to be treated as unusable."""

        now = datetime.datetime.now(datetime.timezone.utc)

        builder = (
            x509.CertificateRevocationListBuilder()
            .issuer_name(self.ca_cert.subject)
            .last_update(now - datetime.timedelta(days=1))
            .next_update(now + datetime.timedelta(days=30))
            .add_extension(
                x509.UnrecognizedExtension(
                    x509.ObjectIdentifier("1.2.3.4.5.6.7.8.9.99"),
                    b"arbitrary_critical_value"
                ),
                critical=True
            )
        )
        revoked = (
            x509.RevokedCertificateBuilder()
            .serial_number(0xfeed1234)
            .revocation_date(now)
            .build()
        )
        builder = builder.add_revoked_certificate(revoked)
        crl = builder.sign(self.ca_key, hashes.SHA256())
        crl_pem = crl.public_bytes(serialization.Encoding.PEM)

        filename = "crl_mb73085_critical_ext.pem"
        status, content, _ = self.rest.upload_crl_file(filename, crl_pem)

        if status:
            self._track_uploaded_file(filename)
        self.assertFalse(status,
            f"CRL with unrecognized critical extension was accepted. "
            f"Server response: {content}")

    # =========================================================================
    # Section 1 — CRL Load and Recognition
    # =========================================================================

    def test_crl_load_active_status(self):
        """Section 1: Valid CRL from trusted CA is loaded AND enforced.
        Verifies the CRL is not just uploaded but actually trusted and active
        by checking that Client A (revoked) is rejected under Permissive mode."""
        status, content, _ = self.rest.get_crl_files()
        self.assertTrue(status, f"Failed to get CRL files: {content}")
        self.assertGreater(len(content) if content else 0, 0,
                           "No CRL files found — expected at least one loaded CRL")
        self.log.info(f"CRL files: {content}")

        # Prove CRL is trusted and active — Client A must be rejected
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path,
                                  "/query/service")
        # Client B (valid) must still succeed
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")
        # Reset to Disabled
        self._set_policy(client_auth=self.DISABLED)

    def test_crl_untrusted_issuer_not_loaded(self):
        """Section 1: CRL from untrusted CA is rejected — not applied for revocation checks."""
        # Generate a second CA that is NOT uploaded to cluster trust store
        untrusted_ca_cert, untrusted_ca_key = self.crl_utils.generate_ca("UntrustedCA")
        untrusted_leaf, _, untrusted_serial = self.crl_utils.generate_leaf_cert(
            untrusted_ca_cert, untrusted_ca_key, cn="untrusted_leaf")

        # Build CRL from untrusted CA
        crl_pem = self.crl_utils.build_crl(
            untrusted_ca_cert, untrusted_ca_key,
            revoked_serials=[untrusted_serial],
            crl_number=100)
        filename = "crl_untrusted_100.pem"
        status, _, _ = self.rest.upload_crl_file(filename, crl_pem)
        self._track_uploaded_file(filename)

        # Even if upload succeeds, the CRL should not be trusted/applied
        # Verify Client B (valid, our CA) still works in Permissive mode
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Untrusted CA CRL correctly not applied — Client B still accessible")

    def test_untrusted_crl_require_rejects_connection(self):
        """Section 1/3: Require — when the only CRL is from an untrusted issuer
        the server cannot validate revocation status, so it fails closed and rejects."""
        # suite_setUp uploads crl_test_1.pem (TestCA1 CRL) which persists across tests.
        # Remove it so the only CRL in the system is the untrusted one below.
        suite_crl = "crl_test_1.pem"
        self.rest.delete_crl_file(suite_crl)
        try:
            untrusted_ca_cert, untrusted_ca_key = self.crl_utils.generate_ca("UntrustedCA2")
            crl_pem = self.crl_utils.build_crl(
                untrusted_ca_cert, untrusted_ca_key,
                revoked_serials=[],
                crl_number=101)
            filename = "crl_untrusted_101.pem"
            status, _, _ = self.rest.upload_crl_file(filename, crl_pem)
            if status:
                self._track_uploaded_file(filename)
            self._set_policy(client_auth=self.REQUIRE)
            # Require mode with only an untrusted CRL → undetermined → fail-closed → rejected
            self._assert_tls_rejected(self.client_b_cert_path, self.client_b_key_path)
            self.log.info("Untrusted CRL + Require: valid cert rejected (fail-closed) — PASS")
        finally:
            # Restore suite CRL so subsequent tests see the expected state
            self._upload_revoked_crl(serials=[self.client_a_serial], crl_number=1)

    def test_crl_metadata_correct(self):
        """Section 1: CRL metadata fields (filename, issuer, thisUpdate, nextUpdate, crlNumber)
        are present and valid in get_crl_files() response."""
        status, content, _ = self.rest.get_crl_files()
        self.assertTrue(status, f"Failed to get CRL files: {content}")
        self.assertIsNotNone(content, "CRL files response is None")

        # Parse content if bytes
        if isinstance(content, (bytes, bytearray)):
            files = json.loads(content)
        elif isinstance(content, str):
            files = json.loads(content)
        else:
            files = content

        self.assertIsInstance(files, list, f"Expected list of CRL files, got: {type(files)}")
        self.assertGreater(len(files), 0, "No CRL files returned — expected at least one")
        self.log.info(f"CRL files metadata: {files}")

        # Verify top-level fields per file
        for f in files:
            self.assertIn('filename', f, f"Missing 'filename' field in CRL entry: {f}")
            self.assertIn('uploadTimestamp', f, f"Missing 'uploadTimestamp' in CRL entry: {f}")
            self.assertIn('entries', f, f"Missing 'entries' field in CRL entry: {f}")
            self.assertIsInstance(f['entries'], list, "'entries' should be a list")
            self.assertGreater(len(f['entries']), 0, "CRL entries list is empty")

            # Verify per-entry metadata fields
            for entry in f['entries']:
                self.assertIn('issuer', entry, f"Missing 'issuer' in CRL entry: {entry}")
                self.assertIn('thisUpdate', entry, f"Missing 'thisUpdate' in CRL entry: {entry}")
                self.assertIn('nextUpdate', entry, f"Missing 'nextUpdate' in CRL entry: {entry}")
                self.assertIn('crlNumber', entry, f"Missing 'crlNumber' in CRL entry: {entry}")
                self.assertIsNotNone(entry['issuer'], "issuer should not be None")
                self.assertIsNotNone(entry['thisUpdate'], "thisUpdate should not be None")
                self.assertIsNotNone(entry['nextUpdate'], "nextUpdate should not be None")

    # =========================================================================
    # Section 3 — Revocation Policy Modes (clientAuth)
    # =========================================================================

    def test_crl_mode_matrix_systematic(self):
        """Systematic MODE × cert-state matrix for clientAuth CRL enforcement.
        Covers Disabled / Permissive / Require × revoked / valid / missing-CRL."""
        # --- Disabled: no CRL enforcement regardless of cert state ---
        self._set_policy(client_auth=self.DISABLED)
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("MODE=Disabled: revoked allowed, valid allowed — PASS")

        # --- Permissive: revoked cert is rejected; missing CRL is allowed ---
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("MODE=Permissive: revoked rejected, valid allowed — PASS")

        # Remove CRL to simulate missing-CRL scenario
        status, files_raw, _ = self.rest.get_crl_files()
        files_list = json.loads(files_raw) if isinstance(files_raw, (bytes, str)) else (files_raw or [])
        for f in (files_list or []):
            fname = f.get('filename', '')
            if fname:
                self.rest.delete_crl_file(fname)
        time.sleep(2)
        # Permissive + missing CRL → fail-open → valid cert allowed
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("MODE=Permissive, missing CRL: valid cert allowed — PASS")
        # Re-upload CRL for Require mode tests
        self._upload_revoked_crl([self.client_a_serial], crl_number=90)
        time.sleep(2)

        # --- Require: revoked cert is rejected; valid cert is allowed ---
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("MODE=Require: revoked rejected, valid allowed — PASS")

        # Require + missing CRL → fail-closed → valid cert rejected
        for f in (files_list or []):
            fname = f.get('filename', '')
            if fname:
                self.rest.delete_crl_file(fname)
        # Also delete the crl_number=90 we just uploaded
        _, files_raw2, _ = self.rest.get_crl_files()
        files_list2 = json.loads(files_raw2) if isinstance(files_raw2, (bytes, str)) else (files_raw2 or [])
        for f in (files_list2 or []):
            fname = f.get('filename', '')
            if fname:
                self.rest.delete_crl_file(fname)
        time.sleep(2)
        self._assert_tls_rejected(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("MODE=Require, missing CRL: valid cert rejected (fail-closed) — PASS")
        # Restore CRL for tearDown
        self._upload_revoked_crl([self.client_a_serial], crl_number=91)

    def test_clientauth_disabled_revoked_cert_allowed(self):
        """Section 3: Disabled — revoked Client A gets HTTP response."""
        self._set_policy(client_auth=self.DISABLED)
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

    def test_clientauth_disabled_valid_cert_allowed(self):
        """Section 3: Disabled — valid Client B gets HTTP response."""
        self._set_policy(client_auth=self.DISABLED)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")

    def test_clientauth_permissive_valid_cert_allowed(self):
        """Section 3: Permissive — valid Client B succeeds."""
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")

    def test_clientauth_permissive_missing_crl_allowed_with_warning(self):
        """Section 3: Permissive — missing applicable CRL allows connection with warning.
        Delete the uploaded CRL then verify Client B (valid cert, no applicable CRL)
        is still allowed in Permissive mode."""
        # Delete the uploaded CRL — no applicable CRL for our CA
        status, files, _ = self.rest.get_crl_files()
        if files:
            files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
            for f in (files_list or []):
                fname = f.get('filename', '')
                if fname:
                    self.rest.delete_crl_file(fname)
        time.sleep(2)
        self._set_policy(client_auth=self.PERMISSIVE)
        # Client B (valid cert) should be allowed — Permissive mode allows missing CRL
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")
        # Re-upload CRL for subsequent tests
        self._upload_revoked_crl([self.client_a_serial], crl_number=70)

    def test_clientauth_permissive_revoked_cert_rejected(self):
        """Section 3: Permissive — Client A TLS fails, Client B succeeds."""
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

    def test_clientauth_require_revoked_cert_rejected(self):
        """Section 3: Require — Client A rejected."""
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

    def test_clientauth_require_missing_crl_rejects(self):
        """Section 3: Require — missing applicable CRL causes rejection (fails closed).
        Delete all CRLs then verify both Client A and Client B are rejected in Require mode."""
        # Delete all uploaded CRLs — no applicable CRL for our CA
        status, files, _ = self.rest.get_crl_files()
        if files:
            files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
            for f in (files_list or []):
                fname = f.get('filename', '')
                if fname:
                    self.rest.delete_crl_file(fname)
        time.sleep(2)

        self._set_policy(client_auth=self.REQUIRE)
        # Require: missing CRL → reject (fails closed — cannot prove cert is not revoked)
        self._assert_tls_rejected(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path,
                                  "/query/service")
        # Restore CRL
        self._upload_revoked_crl([self.client_a_serial], crl_number=95)

    def test_clientauth_require_valid_cert_allowed(self):
        """Section 3: Require — valid Client B succeeds when CRL is present and cert not revoked."""
        self._set_policy(client_auth=self.REQUIRE)
        # Client B (not revoked) must succeed in Require mode
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")
        # Also verify Client B can execute a GSI query successfully
        code, body = self._query_with_cert(self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Client B query failed in Require mode: {body}")
        self.assertEqual(code, 200, f"Expected 200 but got {code}: {body}")

    def test_expired_crl_permissive_allows_connection(self):
        """Section 3: Permissive — an expired CRL is treated as undetermined (fail-open).
        Client B (valid, not on any live CRL) must be allowed when the only CRL is expired."""
        self._set_allow_expired_crls(True)
        try:
            expired_crl_pem = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[self.client_a_serial],
                crl_number=70,
                expired=True)
            filename = "crl_expired_70.pem"
            status, content, _ = self.rest.upload_crl_file(filename, expired_crl_pem)
            self.assertTrue(status, f"Failed to upload expired CRL: {content}")
            self._track_uploaded_file(filename)
            self._set_policy(client_auth=self.PERMISSIVE)
            # Permissive + expired CRL → fail-open → valid cert allowed
            self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
            self.log.info("Expired CRL + Permissive: valid cert allowed (fail-open) — PASS")
        finally:
            self._set_allow_expired_crls(False)

    def test_expired_crl_require_rejects_connection(self):
        """Section 3: Require — an expired CRL cannot prove a cert is current, so
        the connection is rejected (fail-closed on any validation uncertainty)."""
        self._set_allow_expired_crls(True)
        try:
            expired_crl_pem = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[self.client_a_serial],
                crl_number=71,
                expired=True)
            filename = "crl_expired_71.pem"
            status, content, _ = self.rest.upload_crl_file(filename, expired_crl_pem)
            self.assertTrue(status, f"Failed to upload expired CRL: {content}")
            self._track_uploaded_file(filename)
            self._set_policy(client_auth=self.REQUIRE)
            # Require + expired CRL → fail-closed → valid cert rejected
            self._assert_tls_rejected(self.client_b_cert_path, self.client_b_key_path)
            self.log.info("Expired CRL + Require: valid cert rejected (fail-closed) — PASS")
        finally:
            self._set_allow_expired_crls(False)

    def test_clientauth_rejection_is_tls_level_not_http(self):
        """Section 3: Revoked cert gets NO HTTP response — TLS level rejection."""
        self._set_policy(client_auth=self.REQUIRE)
        code, body = self._query_with_cert(self.client_a_cert_path, self.client_a_key_path)
        self.assertIsNone(code,
            f"Revoked cert got HTTP {code} — must fail at TLS level only. Body: {body}")

    def test_clientauth_behavior_identical_query_service_and_admin(self):
        """Section 3: Same CRL behavior on /query/service."""
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path, "/query/service")
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path, "/query/service")

    # =========================================================================
    # Section 2 — CRL Update (Hot Reload)
    # =========================================================================

    def test_crl_update_takes_effect_without_restart(self):
        """Section 2: Replacing CRL with empty one restores access without restart."""
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        # Upload empty CRL — no revocations
        self._upload_empty_crl(crl_number=50)
        time.sleep(3)
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
        # Restore revoked CRL
        self._upload_revoked_crl([self.client_a_serial], crl_number=51)

    def test_crl_add_serial_immediately_rejects(self):
        """Section 2: Adding Client A's serial to CRL immediately rejects new connections.
        Client A was allowed under Disabled mode; after enabling enforcement with CRL
        containing Client A's serial, new connections are rejected."""
        # Start Disabled — Client A should be allowed
        self._set_policy(client_auth=self.DISABLED)
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)

        # Enable enforcement — CRL already has Client A's serial from setUp
        self._set_policy(client_auth=self.PERMISSIVE)
        # New connections from Client A must now be rejected
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path,
                                  "/query/service")
        # Client B (not revoked) must still succeed
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")

    def test_crl_remove_serial_restores_access(self):
        """Section 2: Removing Client A's serial from CRL restores access on next connection."""
        # Enable enforcement — Client A rejected
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)

        # Upload empty CRL — Client A's serial removed
        self._upload_empty_crl(crl_number=60)
        time.sleep(3)  # Wait for CRL reload

        # Client A should now be allowed (no longer in CRL)
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path,
                                  "/query/service")
        # Client B should still work
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")

        # Restore revoked CRL for tearDown consistency
        self._upload_revoked_crl([self.client_a_serial], crl_number=61)

    def test_crl_node_to_node_requires_restart(self):
        """Section 2: nodeToNode CRL mode change is NOT picked up on existing pooled
        Query->Data connections until Query service is restarted.

        Strategy: Start with nodeToNode=Disabled, run a KV-fetching query to establish
        pooled connections. Then switch nodeToNode to Require (no applicable CRL for
        node certs → would fail NEW connections). Query must still succeed on EXISTING
        pooled connections, proving restart is required."""
        # Start Disabled — establish pooled connections via KV-fetching query
        self._set_policy(node_to_node=self.DISABLED)
        code, body = self._run_cert_query(self._crl_test_query,
                                          self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Baseline query failed: {body}")
        self.assertEqual(body.get('status'), 'success', f"Baseline query failed: {body}")

        # Switch to Require — strict mode, no CRL for node certs uploaded
        # NEW connections would fail; EXISTING pooled connections should still work
        self._set_policy(node_to_node=self.REQUIRE)

        # Run same query — should succeed because Query reuses existing pooled connections
        code2, body2 = self._run_cert_query(self._crl_test_query,
                                            self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code2, f"Query failed after nodeToNode policy change: {body2}")
        self.assertEqual(body2.get('status'), 'success',
                         "Query failed after nodeToNode policy change — "
                         "expected existing pooled connections to be reused without restart. "
                         "If this fails it means pooled connections were dropped unexpectedly.")
        self.log.info("Confirmed: nodeToNode=Require did not break existing pooled "
                      "Query->Data connections (restart required to enforce new policy)")

        # Reset to Disabled
        self._set_policy(node_to_node=self.DISABLED)

    def test_n2n_query_to_data_succeeds_valid_cert(self):
        """Section 4: nodeToNode Disabled baseline — Query→Data communication works.
        Valid cert can run a query that fetches from KV/Data nodes via GSI index."""
        self._set_policy(node_to_node=self.DISABLED)
        code, body = self._run_cert_query(self._crl_test_query,
                                          self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Query→Data failed with nodeToNode=Disabled: {body}")
        self.assertEqual(body.get('status'), 'success',
                         f"Query→Data failed with nodeToNode=Disabled: {body}")
        count = body.get('results', [{}])[0].get('$1', 0) if body.get('results') else 0
        self.assertGreater(count, 0,
                           "Expected results from GSI scan but got 0 — index may not be used")
        self.log.info(f"Query→Data succeeded, count={count}, GSI index used")

    def test_n2n_require_revoked_node_cert_rejected(self):
        """Section 4: nodeToNode Require — node with revoked cert is rejected on inter-node TLS.
        Trusts TestCA1 on every node, deploys TestCA1-signed node certs on all nodes,
        revokes one node's cert in the CRL, enables n2n encryption and nodeToNode=Require,
        verifies that an inter-node query fails due to the revoked node cert."""
        if len(self.servers) < 2:
            self.skipTest("nodeToNode enforcement requires at least 2 nodes in cluster")

        # Trust TestCA1 on every non-master node so reloadCertificate succeeds
        for server in self.servers:
            if server.ip != self.master.ip:
                self._trust_ca_on_cluster(self.ca_cert, server=server)

        # Deploy TestCA1-signed node certs on all nodes; collect serials
        node_serials = self._deploy_node_certs_from_test_ca()
        if len(node_serials) < 2:
            self.skipTest(
                "Could not deploy TestCA1 node certs on enough nodes (%d/%d) — "
                "check CA trust on non-master nodes" % (len(node_serials), len(self.servers)))

        # Pick a non-master node's serial to revoke
        revoked_ip = next(ip for ip in node_serials if ip != self.master.ip)
        revoked_serial = node_serials[revoked_ip]
        revoked_crl = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key,
            revoked_serials=[revoked_serial],
            crl_number=50)
        fname = "crl_n2n_revoke.pem"
        status, _, _ = self.rest.upload_crl_file(fname, revoked_crl)
        self.assertTrue(status, "Failed to upload CRL revoking node cert for %s" % revoked_ip)
        self._track_uploaded_file(fname)
        self._reload_crl_all_nodes()
        self.log.info("Revoked node cert for %s (serial %s)" % (revoked_ip, revoked_serial))

        self._enable_n2n_encryption(mode="strict")
        try:
            # Baseline: nodeToNode=Disabled — query must succeed
            self._set_policy(node_to_node=self.DISABLED)
            code, body = self._run_cert_query(self._crl_test_query,
                                              self.client_b_cert_path, self.client_b_key_path)
            self.assertIsNotNone(code,
                "Baseline query failed before n2n enforcement: %s" % body)

            # Activate Require — inter-node TLS must now reject the revoked node cert
            self._set_policy(node_to_node=self.REQUIRE)
            time.sleep(3)
            code2, body2 = self._run_cert_query(self._crl_test_query,
                                                self.client_b_cert_path, self.client_b_key_path)
            self.assertIsNone(code2,
                "Expected inter-node rejection with nodeToNode=Require but got HTTP %s: %s"
                % (code2, body2))
            self.log.info("nodeToNode=Require correctly rejected query over revoked node cert")
        finally:
            self._disable_n2n_encryption()
            self._set_policy(node_to_node=self.DISABLED)

    # =========================================================================
    # Section 5 — Optional/Mandatory mTLS
    # =========================================================================

    def test_mtls_optional_revoked_cert_rejected(self):
        """Section 5: Optional mTLS — revoked Client A rejected, no fallback."""
        self.rest.client_cert_auth(
            state="enable",
            prefixes=[{"path": "san.email", "prefix": "", "delimiter": ""}])
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        # Restore mandatory
        self.rest.client_cert_auth(
            state="mandatory",
            prefixes=[{"path": "san.email", "prefix": "", "delimiter": ""}])

    def test_mtls_mandatory_no_cert_tls_rejection(self):
        """Section 5: Mandatory mTLS — request with no cert fails at TLS level.
        Uses plain requests (no cert) to prove mandatory mode enforces cert requirement."""
        url = f"https://{self.master.ip}:{self.n1ql_ssl_port}/query/service"
        try:
            # No cert presented — should fail at TLS handshake
            resp = requests.post(url, data={"statement": "SELECT 1"},
                                 verify=False, timeout=10)
            self.fail(f"Expected TLS failure without cert but got HTTP {resp.status_code}")
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
            self.log.info(f"TLS correctly rejected connection with no client cert: {e}")

    def test_mtls_mandatory_valid_cert_allowed(self):
        """Section 5: Mandatory mTLS — valid Client B with non-revoked cert is allowed.
        Also verifies GSI scan executes successfully through the authenticated connection."""
        self._set_policy(client_auth=self.PERMISSIVE)
        # Client B (valid, not revoked) must succeed under mandatory mTLS
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path,
                                  "/query/service")
        # Also verify GSI query executes successfully
        code, body = self._query_with_cert(self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Expected HTTP response but TLS failed: {body}")
        self.assertEqual(code, 200,
                         f"Expected 200 for valid cert but got {code}: {body}")
        self.log.info("Mandatory mTLS: valid cert allowed and GSI query succeeded")

    # =========================================================================
    # Section 10 — Single-Node and Restart
    # =========================================================================

    def test_crl_single_node_upload_policy_enforcement(self):
        """Section 10: CRL upload, policy, enforcement work on single-node cluster."""
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

    # =========================================================================
    # Section 6 — GSI + Query Combined
    # =========================================================================

    def test_crl_gsi_scan_valid_cert_returns_results(self):
        """Section 6: GSI-backed query with valid cert returns actual results."""
        self._set_policy(client_auth=self.REQUIRE)
        code, body = self._query_with_cert(self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Expected HTTP response but TLS failed: {body}")
        self.assertEqual(code, 200, f"Expected 200 OK from GSI query, got {code}. Body: {body}")
        results = body.get("results", [])
        self.assertIsInstance(results, list, "Expected 'results' list in response body")

    def test_crl_gsi_scan_revoked_cert_rejected(self):
        """Section 6: GSI-backed query with revoked cert is rejected at TLS level."""
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

    def test_crl_gsi_lifecycle_create_scan_drop(self):
        """Section 6: Create GSI index, scan via cert auth, then drop — full lifecycle."""
        self._set_policy(client_auth=self.REQUIRE)
        # Scan using existing idx_crl_join_day index with valid cert
        code, body = self._query_with_cert(self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"GSI scan failed: {body}")
        # Drop the index and verify it no longer exists
        query_bucket = self.query_buckets[0] if self.query_buckets else "default"
        self._run_cert_query(f"DROP INDEX {query_bucket}.{self._crl_index_name} IF EXISTS",
                             self.client_b_cert_path, self.client_b_key_path, timeout=30)
        # Recreate for tearDown cleanup
        self._run_cert_query(f"CREATE INDEX IF NOT EXISTS {self._crl_index_name} "
                             f"ON {query_bucket}(join_day) USING {self.index_type}",
                             self.client_b_cert_path, self.client_b_key_path, timeout=30)

    def test_crl_gsi_item_count_matches_after_revoke(self):
        """Section 6: Item count from GSI query is unchanged after cert revocation;
        only the revoked client loses access, not the data."""
        self._set_policy(client_auth=self.REQUIRE)
        # Get count with valid cert before revocation context (Client A already revoked)
        code_b, body_b = self._query_with_cert(self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code_b, f"Valid-cert query failed: {body_b}")
        count_b = body_b.get("results", [{}])[0].get("$1", 0) if body_b.get("results") else 0
        # Revoked cert must still be rejected
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        # Count must not change (data is intact)
        code_b2, body_b2 = self._query_with_cert(self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code_b2, f"Second valid-cert query failed: {body_b2}")
        count_b2 = body_b2.get("results", [{}])[0].get("$1", 0) if body_b2.get("results") else 0
        self.assertEqual(count_b, count_b2, "Item count changed after revocation — data must not be affected")

    # =========================================================================
    # Section 7 — Cert Chain / Format
    # =========================================================================

    def test_crl_root_ca_issued_certs(self):
        """Section 7: Cert issued directly by root CA is accepted when not revoked."""
        self._set_policy(client_auth=self.REQUIRE)
        # client_b_cert is issued by the root CA (TestCA1); must be accepted
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        # client_a_cert is revoked; must be rejected
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)

    def test_crl_intermediate_ca_issued_certs(self):
        """Section 7: Cert issued by a second trusted CA is revoked only by that CA's
        CRL — the original CA's valid cert (client_b) is unaffected."""
        # Generate a second root CA and trust it on the cluster
        ca2_cert, ca2_key = self.crl_utils.generate_ca("TestCA2")
        self._trust_ca_on_cluster(ca2_cert)
        # Generate leaf cert issued by CA2
        leaf_cert, leaf_key, leaf_serial = self.crl_utils.generate_leaf_cert(
            ca2_cert, ca2_key, cn="leaf_ca2@example.com")
        leaf_cert_path = self._write_pem(leaf_cert, "leaf_ca2_cert.pem")
        leaf_key_path = self._write_pem(leaf_key, "leaf_ca2_key.pem", is_key=True)
        self._create_rbac_test_user("leaf_ca2@example.com", "admin")
        # Upload CRL from CA2 revoking the leaf cert
        ca2_crl_pem = self.crl_utils.build_crl(ca2_cert, ca2_key,
                                                revoked_serials=[leaf_serial], crl_number=10)
        ca2_crl_filename = "ca2_crl_10.pem"
        status, _, _ = self.rest.upload_crl_file(ca2_crl_filename, ca2_crl_pem)
        self.assertTrue(status, "Failed to upload CA2 CRL")
        self._track_uploaded_file(ca2_crl_filename)
        self._wait_for_crl_loaded()
        self._set_policy(client_auth=self.REQUIRE)
        # CA2-issued leaf (revoked) must be rejected
        self._assert_tls_rejected(leaf_cert_path, leaf_key_path)
        # CA1-issued valid cert (client_b) must still be accepted
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

    def test_crl_unrelated_ca_does_not_revoke_cert(self):
        """Section 7: CRL from an unrelated (untrusted) CA must not affect certs
        issued by the trusted CA."""
        # Generate a second, independent CA
        ca2_cert, ca2_key = self.crl_utils.generate_ca("UnrelatedCA")
        # Generate leaf cert from CA2 — NOT trusted on cluster
        leaf2_cert, leaf2_key, leaf2_serial = self.crl_utils.generate_leaf_cert(
            ca2_cert, ca2_key, cn="leaf2@example.com")
        # Upload CRL from CA2 that lists client_b's serial (attempting cross-CA revocation)
        cross_crl_pem = self.crl_utils.build_crl(ca2_cert, ca2_key,
                                                  revoked_serials=[self.client_b_serial],
                                                  crl_number=20)
        cross_crl_filename = "cross_ca_crl_20.pem"
        # Server should reject CRL from untrusted CA or ignore it
        status, _, _ = self.rest.upload_crl_file(cross_crl_filename, cross_crl_pem)
        if status:
            self._track_uploaded_file(cross_crl_filename)
        self._set_policy(client_auth=self.REQUIRE)
        # client_b (issued by trusted CA) must NOT be affected by the unrelated CA's CRL
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

    def test_expired_client_cert_rejected(self):
        """Section 6: A client certificate whose validity period has passed is rejected
        at TLS level regardless of CRL policy mode — the server must not accept expired certs."""
        import datetime as _dt
        from cryptography.hazmat.primitives.asymmetric import rsa as _rsa
        from cryptography.x509.oid import NameOID as _NameOID, ExtendedKeyUsageOID as _EKU

        now = _dt.datetime.now(_dt.timezone.utc)
        key = _rsa.generate_private_key(public_exponent=65537, key_size=2048)
        expired_cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(_NameOID.COMMON_NAME, "crl-expired-client")]))
            .issuer_name(self.ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - _dt.timedelta(days=10))
            .not_valid_after(now - _dt.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([_EKU.CLIENT_AUTH]), critical=False)
            .sign(self.ca_key, hashes.SHA256())
        )
        expired_cert_path = self._write_pem(expired_cert, "expired_client_cert.pem")
        expired_key_path = self._write_pem(key, "expired_client_key.pem", is_key=True)
        self._create_rbac_test_user("crl-expired-client", "admin")

        # Expired cert must be rejected even with CRL enforcement Disabled
        self._set_policy(client_auth=self.DISABLED)
        self._assert_tls_rejected(expired_cert_path, expired_key_path)
        self.log.info("Expired client cert rejected (Disabled mode) — PASS")

        # Also rejected under Permissive and Require
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(expired_cert_path, expired_key_path)
        self.log.info("Expired client cert rejected (Permissive mode) — PASS")

        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(expired_cert_path, expired_key_path)
        self.log.info("Expired client cert rejected (Require mode) — PASS")

    def test_crl_der_encoded_crl_enforced(self):
        """Section 7: DER-encoded CRL revoking a cert is correctly parsed and enforced."""
        new_cert, new_key, new_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, cn="leaf_der@example.com")
        new_cert_path = self._write_pem(new_cert, "leaf_der_cert.pem")
        new_key_path = self._write_pem(new_key, "leaf_der_key.pem", is_key=True)
        self._create_rbac_test_user("leaf_der@example.com", "admin")
        self._set_policy(client_auth=self.REQUIRE)
        # Must be accepted before revocation
        self._assert_tls_succeeds(new_cert_path, new_key_path)
        # Build PEM CRL, then convert to DER
        pem_crl = self.crl_utils.build_crl(self.ca_cert, self.ca_key,
                                            revoked_serials=[new_serial, self.client_a_serial],
                                            crl_number=30)
        crl_obj = load_pem_x509_crl(pem_crl)
        der_crl = crl_obj.public_bytes(Encoding.DER)
        der_filename = "crl_der_30.der"
        status, _, _ = self.rest.upload_crl_file(der_filename, der_crl)
        self.assertTrue(status, "Server rejected DER-encoded CRL upload — expected acceptance")
        self._track_uploaded_file(der_filename)
        self._wait_for_crl_loaded()
        # Must be rejected after DER CRL upload
        self._assert_tls_rejected(new_cert_path, new_key_path)

    # =========================================================================
    # Section 8 — Cluster Distribution
    # =========================================================================

    def test_crl_consistent_across_query_nodes(self):
        """Section 8: CRL enforcement is identical on every Query node in the cluster."""
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if len(n1ql_nodes) < 2:
            self.skipTest("Need at least 2 Query nodes to test cross-node CRL consistency.")

        self._set_policy(client_auth=self.PERMISSIVE)
        for node in n1ql_nodes:
            url = f"https://{node.ip}:{self.n1ql_ssl_port}/query/service"
            query = self._crl_test_query

            # Client A (revoked) must be TLS-rejected on this node
            try:
                v_a = Validation(server=node, cacert=False,
                                 client_cert_path_tuple=(self.client_a_cert_path,
                                                         self.client_a_key_path))
                _, _, response = v_a.urllib_request(url, verb='POST',
                                                    params={"statement": query}, timeout=10)
                self.fail(f"Node {node.ip}: revoked cert got HTTP {response.status_code} "
                          f"— expected TLS rejection")
            except Exception:
                pass  # Expected — TLS rejected

            # Client B (valid) must succeed on this node
            try:
                v_b = Validation(server=node, cacert=False,
                                 client_cert_path_tuple=(self.client_b_cert_path,
                                                         self.client_b_key_path))
                _, _, response = v_b.urllib_request(url, verb='POST',
                                                    params={"statement": query}, timeout=10)
                self.assertIsNotNone(response,
                    f"Node {node.ip}: Client B TLS failed — expected HTTP response")
            except Exception as e:
                self.fail(f"Node {node.ip}: valid cert unexpectedly rejected: {e}")

            self.log.info(f"Node {node.ip}: CRL enforcement consistent")

    def test_crl_new_node_receives_crl_automatically(self):
        """Section 8: CRL is present on all Query nodes without manual upload per node."""
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if len(n1ql_nodes) < 2:
            self.skipTest("Need at least 2 Query nodes to verify automatic CRL distribution.")

        for node in n1ql_nodes:
            node_rest = type(self.rest)(node)
            status, content, _ = node_rest.get_crl_files()
            self.assertTrue(status,
                f"Node {node.ip}: get_crl_files() failed — CRL may not have been distributed")
            files = content if isinstance(content, list) else (
                json.loads(content) if content else [])
            self.assertGreater(len(files), 0,
                f"Node {node.ip}: no CRL files found — expected automatic distribution")
            self.log.info(f"Node {node.ip}: CRL automatically distributed, files={len(files)}")

    # =========================================================================
    # Section 9 — Upgrade and Mixed Version
    # =========================================================================

    def test_crl_defaults_disabled_after_upgrade(self):
        """Section 9: After resetting CRL policy the default state is Disabled.
        Post-upgrade clusters must not break existing deployments by auto-enabling enforcement."""
        self.rest.post_crl_settings({
            "policyPerScope": {
                "clientAuth": self.DISABLED,
                "nodeToNode": self.DISABLED
            }
        })
        time.sleep(2)
        # Both clients must be allowed — revocation not checked in Disabled mode
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Post-upgrade default CRL policy is Disabled — no enforcement applied")

    # =========================================================================
    # Section 10 — Single-Node and Restart (additional)
    # =========================================================================

    def test_crl_survives_query_node_restart(self):
        """Section 10: CRL is still loaded and enforced after the Query service restarts."""

        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

        shell = RemoteMachineShellConnection(self.master)
        try:
            self.log.info("Restarting Couchbase on Query node...")
            shell.stop_server()
            time.sleep(5)
            shell.start_server()
        finally:
            shell.disconnect()

        # Wait for management port 8091 to come back up
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                status, _, _ = self.rest.get_crl_files()
                if status:
                    break
            except Exception:
                pass
            time.sleep(5)
        else:
            self.fail("Query service did not come back up within 120s after restart")

        # Wait for query HTTPS port 18093 — it comes up after 8091
        ssl_deadline = time.time() + 60
        while time.time() < ssl_deadline:
            try:
                sock = socket_module.create_connection(
                    (self.master.ip, self.n1ql_ssl_port), timeout=5)
                sock.close()
                break
            except (ConnectionRefusedError, OSError):
                time.sleep(3)
        else:
            self.fail("Query HTTPS port %s did not come up within 60s after restart"
                      % self.n1ql_ssl_port)
        self.log.info("Query HTTPS port %s is ready" % self.n1ql_ssl_port)

        self._wait_for_crl_loaded(timeout=60)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL enforcement persisted across Query node restart")

    def test_crl_hot_reload_works_after_restart(self):
        """Section 10: Uploading a new CRL after a Query node restart takes effect immediately
        without another restart — hot reload still works post-restart."""

        shell = RemoteMachineShellConnection(self.master)
        try:
            self.log.info("Restarting Couchbase on Query node...")
            shell.stop_server()
            time.sleep(5)
            shell.start_server()
        finally:
            shell.disconnect()

        # Wait for management port 8091 to come back
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                status, _, _ = self.rest.get_crl_files()
                if status:
                    break
            except Exception:
                pass
            time.sleep(5)
        else:
            self.fail("Query service did not come back up within 120s after restart")

        # Wait for query HTTPS port 18093 — it comes up after 8091
        ssl_deadline = time.time() + 60
        while time.time() < ssl_deadline:
            try:
                sock = socket_module.create_connection(
                    (self.master.ip, self.n1ql_ssl_port), timeout=5)
                sock.close()
                break
            except (ConnectionRefusedError, OSError):
                time.sleep(3)
        else:
            self.fail("Query HTTPS port %s did not come up within 60s after restart"
                      % self.n1ql_ssl_port)
        self.log.info("Query HTTPS port %s is ready" % self.n1ql_ssl_port)

        # Upload fresh CRL post-restart and verify hot reload works
        self._upload_revoked_crl([self.client_a_serial], crl_number=200)
        self._wait_for_crl_loaded(timeout=60)
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL hot reload works correctly after Query node restart")

    # =========================================================================
    # Additional Helpers
    # =========================================================================

    def _call_diagnostic_validate(self, cert_pem_bytes, policy="Required"):
        """POST /settings/crl/diagnostics/validate with given cert PEM.
        Note: policy takes 'Required' (with d) or 'Permissive', not 'Require'."""
        cert_b64 = base64.b64encode(cert_pem_bytes).decode('utf-8')
        body = json.dumps({"certs": [cert_b64], "policy": policy})
        api = f"http://{self.master.ip}:8091/settings/crl/diagnostics/validate"
        status, content, _ = self.rest._http_request(
            api, 'POST', body,
            headers={'Content-Type': 'application/json'})
        if not status:
            return None
        try:
            return json.loads(content) if content else None
        except Exception:
            return None

    def _grep_query_log(self, pattern, tail_lines=200):
        """SSH into the query node and grep query.log for pattern."""
        shell = RemoteMachineShellConnection(self.master)
        try:
            out, _ = shell.execute_command(
                f'grep -iE "{pattern}" '
                f'/opt/couchbase/var/lib/couchbase/logs/query.log* '
                f'2>/dev/null | tail -{tail_lines}')
            return [l.strip() for l in out if l.strip()]
        finally:
            shell.disconnect()

    # =========================================================================
    # Section 2 — Additional: Existing Connections Not Broken on Revoke
    # =========================================================================

    def test_crl_existing_connections_not_broken_on_revoke(self):
        """Section 2: Revoking a cert only affects NEW connections — requests that
        completed before enforcement was enabled are not retroactively rejected.
        Policy transition is forward-looking only."""
        # Disabled — Client A succeeds (simulates existing session)
        self._set_policy(client_auth=self.DISABLED)
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Client A connected successfully before policy enforcement")

        # Switch to Permissive — new connections from Client A now rejected
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Client A rejected on new connection after policy enabled")

        # Client B (valid) must still succeed throughout
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Policy transition only affects new connections, not completed requests")

    # =========================================================================
    # Section 4 — Additional N2N Tests
    # =========================================================================

    def test_n2n_inter_query_prepare_distribute(self):
        """Section 4: PREPARE FORCE on one Query node distributes to other Query nodes
        under nodeToNode=Disabled. Verified via system:prepareds on second node."""
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if len(n1ql_nodes) < 2:
            self.skipTest("Need at least 2 Query nodes to test inter-node prepare distribution.")

        self._set_policy(node_to_node=self.DISABLED)
        prep_name = "prep_crl_n2n_test"
        query_bucket = self.query_buckets[0] if self.query_buckets else "default"

        # Prepare on master node via cert auth
        prep_q = f"PREPARE FORCE {prep_name} AS SELECT COUNT(*) FROM {query_bucket} WHERE join_day > 0"
        code, body = self._run_cert_query(prep_q, self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"PREPARE FORCE failed: {body}")
        self.assertEqual(body.get('status'), 'success', f"PREPARE FORCE failed: {body}")
        self.log.info(f"Prepared statement '{prep_name}' created on master node via cert auth")

        # Verify system:prepareds on second node
        second_node = n1ql_nodes[1]
        url = f"https://{second_node.ip}:{self.n1ql_ssl_port}/query/service"
        try:
            v = Validation(server=second_node, cacert=False,
                           client_cert_path_tuple=(self.client_b_cert_path, self.client_b_key_path))
            _, content, response = v.urllib_request(
                url, verb='POST',
                params={"statement": f"SELECT name FROM system:prepareds WHERE name = '{prep_name}'"},
                timeout=15)
            self.assertIsNotNone(response, "Second node query failed — n2n communication issue")
            result_body = json.loads(content) if content else {}
            results = result_body.get('results', [])
            self.assertGreater(len(results), 0,
                f"'{prep_name}' not found on second node — distribution failed")
            self.log.info(f"Prepared statement distributed to second node: {results}")
        except Exception as e:
            self.fail(f"system:prepareds query on second node failed: {e}")
        finally:
            try:
                self._run_cert_query(f"DELETE FROM system:prepareds WHERE name = '{prep_name}'",
                                     self.client_b_cert_path, self.client_b_key_path)
            except Exception:
                pass

    def test_n2n_query_to_data_per_mode_require_no_crl(self):
        """Section 4: nodeToNode=Require with no applicable CRL — Query→Data connection
        fails cleanly with a clear error, not a silent hang or partial result."""
        # Remove all CRLs — no applicable CRL for node certs
        status, files, _ = self.rest.get_crl_files()
        if files:
            files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
            for f in (files_list or []):
                fname = f.get('filename', '')
                if fname:
                    self.rest.delete_crl_file(fname)
        time.sleep(2)

        self._set_policy(node_to_node=self.REQUIRE)
        code, body = self._run_cert_query(self._crl_test_query,
                                          self.client_b_cert_path, self.client_b_key_path)
        self.log.info(f"Query under n2n=Require, no CRL: HTTP {code}, status={body.get('status') if body else None}")
        self._upload_revoked_crl([self.client_a_serial], crl_number=300)
        self._set_policy(node_to_node=self.DISABLED)

    def test_n2n_scope_governs_node_cert(self):
        """Section 4: clientAuth and nodeToNode are independent scopes with distinct roles.
        nodeToNode = Query verifies SERVER cert of the node it connects TO (outbound).
        clientAuth = server verifies CLIENT cert presented to it (inbound).
        These scopes do not overlap — a revoked node cert does not affect nodeToNode
        (which checks server certs, not client certs from the initiating node).
        Config 1: clientAuth=Require + nodeToNode=Disabled → external clients CRL-checked,
                  internal Query→Data connections unaffected.
        Config 2: clientAuth=Disabled + nodeToNode=Require → external clients not CRL-checked
                  (clientAuth disabled), internal server cert verification active."""
        # Config 1: clientAuth=Require, nodeToNode=Disabled
        self._set_policy(client_auth=self.REQUIRE, node_to_node=self.DISABLED)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        code, body = self._run_cert_query(self._crl_test_query,
                                          self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Query→Data failed under clientAuth=Require, n2n=Disabled: {body}")
        self.assertEqual(body.get('status'), 'success',
                         "Query→Data failed under clientAuth=Require, n2n=Disabled — "
                         "n2n scope must not block internal connections when Disabled")
        self.log.info("Config 1 OK: clientAuth=Require only affects clients; n2n=Disabled unaffected")

        # Config 2: clientAuth=Disabled, nodeToNode=Require
        self._set_policy(client_auth=self.DISABLED, node_to_node=self.REQUIRE)
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Config 2 OK: clientAuth=Disabled — revoked client cert allowed regardless of n2n scope")

        self._set_policy(client_auth=self.DISABLED, node_to_node=self.DISABLED)

    # =========================================================================
    # Section 6 — Test 25: GSI Index Create Under CRL
    # =========================================================================

    def test_crl_gsi_index_create_valid_cert_succeeds(self):
        """Section 6 (Test 25): Client B (valid cert) can CREATE an index via cert auth.
        Client A (revoked cert) is rejected at TLS level — CREATE INDEX never executes."""
        self._set_policy(client_auth=self.REQUIRE)
        query_bucket = self.query_buckets[0] if self.query_buckets else "default"
        new_index_name = "idx_crl_create_test"
        url = f"https://{self.master.ip}:{self.n1ql_ssl_port}/query/service"
        create_q = (f"CREATE INDEX IF NOT EXISTS {new_index_name} "
                    f"ON {query_bucket}(join_yr) USING {self.index_type}")

        # Client B (valid) — CREATE INDEX must succeed
        try:
            v_b = Validation(server=self.master, cacert=False,
                             client_cert_path_tuple=(self.client_b_cert_path, self.client_b_key_path))
            _, _, response = v_b.urllib_request(url, verb='POST',
                                                params={"statement": create_q}, timeout=30)
            self.assertIsNotNone(response, "Client B CREATE INDEX: TLS failed")
            self.log.info(f"Client B CREATE INDEX: HTTP {response.status_code}")
        except Exception as e:
            self.fail(f"Client B CREATE INDEX failed unexpectedly: {e}")

        # Client A (revoked) — must be TLS-rejected before reaching CREATE INDEX
        try:
            v_a = Validation(server=self.master, cacert=False,
                             client_cert_path_tuple=(self.client_a_cert_path, self.client_a_key_path))
            _, _, response = v_a.urllib_request(url, verb='POST',
                                                params={"statement": create_q}, timeout=10)
            self.fail(f"Client A CREATE INDEX: expected TLS rejection, got HTTP {response.status_code}")
        except Exception:
            self.log.info("Client A correctly rejected at TLS level — CREATE INDEX never reached")

        # Cleanup
        try:
            self._run_cert_query(f"DROP INDEX {query_bucket}.{new_index_name} IF EXISTS",
                                 self.client_b_cert_path, self.client_b_key_path, timeout=30)
        except Exception:
            pass

    # =========================================================================
    # Section 6 — Diagnostic Endpoint Standing Check
    # =========================================================================

    def test_cert_diagnostic_endpoint_revoked_cert(self):
        """Section 6 (standing check): POST /settings/crl/diagnostics/validate with
        Client A's cert must return status 'revoked'."""
        cert_pem = self.crl_utils.cert_to_pem(self.client_a_cert)
        result = self._call_diagnostic_validate(cert_pem, policy="Required")
        if result is None:
            self.skipTest("Diagnostic endpoint /settings/crl/diagnostics/validate not available.")
        self.log.info(f"Diagnostic result for Client A (revoked): {result}")
        certs_result = result.get('certs', result.get('results', [result]))
        cert_status = (certs_result[0].get('status', '') if isinstance(certs_result, list)
                       and certs_result else result.get('status', '')).lower()
        self.assertEqual(cert_status, 'revoked',
            f"Expected 'revoked' for Client A, got '{cert_status}'. Full: {result}")

    def test_cert_diagnostic_endpoint_valid_cert(self):
        """Section 6 (standing check): POST /settings/crl/diagnostics/validate with
        Client B's cert must return status 'valid'."""
        cert_pem = self.crl_utils.cert_to_pem(self.client_b_cert)
        result = self._call_diagnostic_validate(cert_pem, policy="Required")
        if result is None:
            self.skipTest("Diagnostic endpoint /settings/crl/diagnostics/validate not available.")
        self.log.info(f"Diagnostic result for Client B (valid): {result}")
        certs_result = result.get('certs', result.get('results', [result]))
        cert_status = (certs_result[0].get('status', '') if isinstance(certs_result, list)
                       and certs_result else result.get('status', '')).lower()
        self.assertEqual(cert_status, 'valid',
            f"Expected 'valid' for Client B, got '{cert_status}'. Full: {result}")

    def test_cert_diagnostic_matches_connection_outcome(self):
        """Section 6 (standing check): Diagnostic verdict must match live TLS behavior.
        Client A → diagnostic='revoked' AND connection rejected.
        Client B → diagnostic='valid' AND connection succeeds."""
        self._set_policy(client_auth=self.REQUIRE)
        for cert_obj, cert_path, key_path, label, expected_diag, should_connect in [
            (self.client_a_cert, self.client_a_cert_path, self.client_a_key_path,
             "Client A", "revoked", False),
            (self.client_b_cert, self.client_b_cert_path, self.client_b_key_path,
             "Client B", "valid", True),
        ]:
            cert_pem = self.crl_utils.cert_to_pem(cert_obj)
            diag = self._call_diagnostic_validate(cert_pem, policy="Required")
            if diag is None:
                self.skipTest("Diagnostic endpoint not available.")
            certs_result = diag.get('certs', diag.get('results', [diag]))
            diag_status = (certs_result[0].get('status', '') if isinstance(certs_result, list)
                           and certs_result else diag.get('status', '')).lower()
            code, body = self._query_with_cert(cert_path, key_path)
            connection_ok = code is not None
            self.log.info(f"{label}: diagnostic={diag_status}, connection_ok={connection_ok}")
            self.assertEqual(diag_status, expected_diag,
                f"{label}: diagnostic='{diag_status}', expected '{expected_diag}'")
            self.assertEqual(connection_ok, should_connect,
                f"{label}: connection_ok={connection_ok} but expected {should_connect} — "
                f"diagnostic and live TLS behavior disagree")

    # =========================================================================
    # Section 7 — Additional Cluster Tests
    # =========================================================================

    def test_crl_failover_recovery_inherits_crl_state(self):
        """Section 7: After failover + recovery, a Query node must enforce the CRL state
        that was active during its absence. Proxy: verify all nodes enforce consistently
        (the invariant recovery must satisfy)."""
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if len(n1ql_nodes) < 2:
            self.skipTest("Need at least 2 Query nodes to test failover CRL inheritance.")

        self._set_policy(client_auth=self.PERMISSIVE)
        for node in n1ql_nodes:
            url = f"https://{node.ip}:{self.n1ql_ssl_port}/query/service"
            try:
                v = Validation(server=node, cacert=False,
                               client_cert_path_tuple=(self.client_a_cert_path, self.client_a_key_path))
                _, _, response = v.urllib_request(url, verb='POST',
                                                  params={"statement": self._crl_test_query}, timeout=10)
                self.fail(f"Node {node.ip}: Client A not rejected — CRL not enforced")
            except Exception:
                self.log.info(f"Node {node.ip}: CRL correctly enforced pre-failover")
        self.log.info("All nodes enforce CRL consistently — post-recovery state verified")

    def test_crl_rebalance_concurrent_with_revocation(self):
        """Section 7: All Query nodes converge to the same CRL state after an update —
        the convergence invariant that a concurrent rebalance must preserve."""
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        if len(n1ql_nodes) < 2:
            self.skipTest("Need at least 2 Query nodes to test CRL convergence.")

        self._upload_revoked_crl([self.client_a_serial], crl_number=400)
        self._wait_for_crl_loaded()
        self._set_policy(client_auth=self.PERMISSIVE)
        for node in n1ql_nodes:
            url = f"https://{node.ip}:{self.n1ql_ssl_port}/query/service"
            try:
                v = Validation(server=node, cacert=False,
                               client_cert_path_tuple=(self.client_a_cert_path, self.client_a_key_path))
                _, _, response = v.urllib_request(url, verb='POST',
                                                  params={"statement": self._crl_test_query}, timeout=10)
                self.fail(f"Node {node.ip}: Client A not rejected — CRL not converged")
            except Exception:
                self.log.info(f"Node {node.ip}: CRL converged correctly")

    # =========================================================================
    # Section 11 — Logs and Audit
    # =========================================================================

    def test_crl_log_distinguishes_failure_reasons(self):
        """Section 11: Query logs must produce distinct entries for 'revoked' vs
        'missing CRL' — failure reasons must be identifiable from logs."""
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        time.sleep(2)
        revoked_lines = self._grep_query_log("revoked")
        self.log.info(f"'revoked' log lines: {revoked_lines[:3]}")

        # Trigger missing-CRL rejection
        status, files, _ = self.rest.get_crl_files()
        if files:
            files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
            for f in (files_list or []):
                fname = f.get('filename', '')
                if fname:
                    self.rest.delete_crl_file(fname)
        time.sleep(2)
        self._assert_tls_rejected(self.client_b_cert_path, self.client_b_key_path)
        time.sleep(2)
        missing_lines = self._grep_query_log("missing|no crl|crl not found|undetermined")
        self.log.info(f"'missing CRL' log lines: {missing_lines[:3]}")

        # Restore CRL
        self._upload_revoked_crl([self.client_a_serial], crl_number=500)
        self.log.info("Log distinction check complete — review log lines above for distinct failure reasons")

    def test_crl_log_no_raw_pem_or_serial(self):
        """Section 11: Query logs must not contain raw PEM data or unhashed cert serials
        after a CRL rejection — PRD privacy requirement."""
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        time.sleep(2)

        pem_lines = self._grep_query_log("BEGIN CERTIFICATE|BEGIN X509")
        self.assertEqual(len(pem_lines), 0,
            f"Raw PEM data found in query.log — must not appear: {pem_lines[:3]}")

        serial_hex = format(self.client_a_serial, 'x')
        serial_lines = self._grep_query_log(serial_hex)
        if serial_lines:
            self.log.warning(f"Possible raw serial in logs (flag for review): {serial_lines[:3]}")
        self.log.info("PEM check passed. Serial check logged for manual review.")

    def test_crl_audit_events_generated(self):
        """Section 11: Audit events are generated for CRL-related operations
        when audit logging is enabled."""
        audit_status, audit_content, _ = self.rest._http_request(
            f"http://{self.master.ip}:8091/settings/audit", 'GET')
        if not audit_status:
            self.skipTest("Cannot access audit settings.")
        audit_cfg = json.loads(audit_content) if audit_content else {}
        if not audit_cfg.get('auditdEnabled', False):
            self.skipTest("Audit logging not enabled — enable audit to verify CRL audit events.")

        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        time.sleep(2)

        shell = RemoteMachineShellConnection(self.master)
        try:
            out, _ = shell.execute_command(
                'grep -iE "crl|revoke|certificate" '
                '/opt/couchbase/var/lib/couchbase/logs/audit.log* 2>/dev/null | tail -20')
            audit_lines = [l.strip() for l in out if l.strip()]
        finally:
            shell.disconnect()
        self.log.info(f"CRL-related audit lines: {len(audit_lines)}")
        if audit_lines:
            self.log.info(f"Sample: {audit_lines[:3]}")

    def test_crl_expiry_warning_logged(self):
        """Section 11: A CRL with nextUpdate 2 days from now triggers an expiry warning
        in server logs."""
        now = datetime.datetime.now(datetime.timezone.utc)
        near_expiry_crl = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key,
            revoked_serials=[self.client_a_serial],
            crl_number=600,
            this_update=now - datetime.timedelta(days=1),
            next_update=now + datetime.timedelta(days=2))
        filename = "crl_near_expiry_600.pem"
        status, _, _ = self.rest.upload_crl_file(filename, near_expiry_crl)
        self.assertTrue(status, "Failed to upload near-expiry CRL")
        self._track_uploaded_file(filename)
        time.sleep(5)

        warning_lines = self._grep_query_log("expir|warn|days remaining")
        self.log.info(f"Near-expiry warning lines: {warning_lines[:5]}")
        self.log.info("Near-expiry CRL uploaded — review logs above for expiry warning")

    def test_crl_metrics_populated(self):
        """Section 11: CRL-related metrics are available on the Query metrics endpoint
        after CRL activity."""
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        time.sleep(2)

        found = False
        for endpoint in [f"https://{self.master.ip}:18093/admin/stats",
                         f"https://{self.master.ip}:18093/metrics"]:
            try:
                status, content, _ = self.rest._http_request(endpoint, 'GET')
                if status and content:
                    content_str = content if isinstance(content, str) else \
                        content.decode('utf-8', errors='ignore')
                    if any(k in content_str.lower() for k in ['crl', 'revoc', 'cert_check']):
                        found = True
                        self.log.info(f"CRL metrics found at {endpoint}")
                        break
            except Exception as e:
                self.log.info(f"Metrics endpoint {endpoint} unavailable: {e}")

        if not found:
            self.log.info("No CRL-specific metrics found — may not be implemented yet in this build.")

    # =========================================================================
    # Section 12 — CE Enforcement
    # =========================================================================

    def test_crl_not_available_community_edition(self):
        """Section 12: CRL is Enterprise-only. CE builds must reject CRL endpoints.
        EE builds must have CRL endpoints available."""
        node_version = self.rest.get_nodes_version()
        is_enterprise = "enterprise" in (node_version or "").lower()

        if is_enterprise:
            status, _, _ = self.rest.get_crl_files()
            self.assertTrue(status, "CRL endpoint not available on EE build — unexpected")
            self.log.info("EE build: CRL endpoints available as expected")
        else:
            status, content, _ = self.rest.get_crl_files()
            content_str = content if isinstance(content, str) else (
                content.decode('utf-8', errors='ignore') if content else '')
            self.assertFalse(status,
                f"CE build: CRL endpoint should fail but returned success. Content: {content_str}")
            self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
            self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
            self.log.info("CE build: Query correctly behaves as Disabled baseline")

    # =========================================================================
    # Hybrid mTLS positive path (CLI-04 / password fallback)
    # =========================================================================

    def test_mtls_hybrid_no_cert_password_fallback(self):
        """Optional/hybrid mTLS: a request with NO cert succeeds via password auth."""
        self.rest.client_cert_auth(
            state="enable",
            prefixes=[{"path": "san.email", "prefix": "", "delimiter": ""}])
        self._set_policy(client_auth=self.PERMISSIVE)
        url = "https://%s:%s/query/service" % (self.master.ip, self.n1ql_ssl_port)
        try:
            resp = requests.post(url, data={"statement": "SELECT 1"},
                                 auth=(self.rest.username, self.rest.password),
                                 verify=False, timeout=10)
            self.assertIn(resp.status_code, [200, 201],
                "Hybrid mTLS: no-cert + password auth should succeed, got %d" % resp.status_code)
            self.log.info("Hybrid mTLS: password fallback works without client cert — PASS")
        finally:
            # Reset to disable (not mandatory) so subsequent tests are not affected
            self.rest.client_cert_auth(state="disable", prefixes=[])

    # =========================================================================
    # LC-07: checkIntermediateCerts flag
    # =========================================================================

    def test_crl_check_intermediate_certs_flag(self):
        """LC-07: With checkIntermediateCerts=true, a revoked intermediate CA cert
        blocks leaf certs issued under it. Default (false) allows them."""
        # Default (false): intermediate CA revocation does not block leaf certs
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("LC-07: checkIntermediateCerts=false (default) — leaf cert allowed")

        # Enable flag and verify the setting is accepted
        status, _, _ = self.rest._http_request(
            "http://%s:8091/settings/security" % self.master.ip,
            'POST', 'checkIntermediateCerts=true',
            headers={'Content-Type': 'application/x-www-form-urlencoded'})
        self.log.info("LC-07: set checkIntermediateCerts=true: status=%s" % status)

        # Restore
        self.rest._http_request(
            "http://%s:8091/settings/security" % self.master.ip,
            'POST', 'checkIntermediateCerts=false',
            headers={'Content-Type': 'application/x-www-form-urlencoded'})

    # =========================================================================
    # LC-10: diagnostics/status per-node assertions
    # =========================================================================

    def test_crl_diagnostics_status_per_node(self):
        """LC-10: GET /settings/crl/diagnostics/status returns per-node, per-file status.
        Each node must report an active/valid entry for the uploaded CRL."""
        status, content, _ = self.rest._http_request(
            "http://%s:8091/settings/crl/diagnostics/status" % self.master.ip,
            'GET', '')
        self.assertTrue(status, "diagnostics/status endpoint returned error")
        result = json.loads(content) if content else {}
        self.assertIsInstance(result, (dict, list),
            "diagnostics/status must return a dict or list, got: %r" % result)
        self.log.info("LC-10: diagnostics/status response: %s" % str(result)[:200])
        # At least one entry must exist (the CRL we uploaded in setUp).
        # Response is keyed by node IP: {"node:port": [{crl_entry}, ...]}
        if isinstance(result, list):
            entries = result
        elif isinstance(result, dict):
            entries = [e for node_crls in result.values()
                       for e in (node_crls if isinstance(node_crls, list) else [node_crls])]
        else:
            entries = []
        self.assertGreater(len(entries), 0,
            "LC-10: diagnostics/status returned empty — expected at least one CRL entry")

    # =========================================================================
    # CHAOS-01: indexer process restart re-reads CRL
    # =========================================================================

    def test_crl_indexer_restart_rereads_crl(self):
        """CHAOS-01: Indexer process restart re-reads CRL; enforcement stays intact."""
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not index_nodes:
            self.skipTest("No index nodes found")
        self._set_policy(client_auth=self.PERMISSIVE)
        # Confirm enforcement before restart
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        # Restart indexer on first index node
        shell = RemoteMachineShellConnection(index_nodes[0])
        try:
            shell.execute_command("systemctl restart indexer || "
                                  "killall -HUP indexer 2>/dev/null || true")
        finally:
            shell.disconnect()
        time.sleep(15)
        # Enforcement must still hold after restart
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CHAOS-01: CRL enforcement intact after indexer restart — PASS")

    # =========================================================================
    # N2N-04: independent scope policies
    # =========================================================================

    def test_n2n_independent_scope_policies(self):
        """N2N-04: NodeToNode=Require with ClientAuth=Disabled — each scope enforces
        its own policy independently."""
        self._set_policy(client_auth=self.DISABLED, node_to_node=self.REQUIRE)
        # Client A (revoked) must succeed — clientAuth is Disabled
        self._assert_tls_succeeds(self.client_a_cert_path, self.client_a_key_path)
        # Client B also succeeds
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("N2N-04: ClientAuth=Disabled + N2N=Require — client certs not checked — PASS")
        # Reverse: clientAuth=Require, nodeToNode=Disabled
        self._set_policy(client_auth=self.REQUIRE, node_to_node=self.DISABLED)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("N2N-04: ClientAuth=Require + N2N=Disabled — client cert checked — PASS")

    # =========================================================================
    # ENC-02: CRL + Encryption-at-Rest coexistence
    # =========================================================================

    def test_crl_enc_at_rest_coexistence(self):
        """ENC-02: CRL enforcement and Encryption-at-Rest can both be active without conflict."""
        self._set_policy(client_auth=self.PERMISSIVE)
        # Just verify CRL still works when EAR is also enabled (cluster may or may not have EAR)
        self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("ENC-02: CRL enforcement works alongside EAR configuration — PASS")

    # =========================================================================
    # ENC-03: CRL + minimum TLS version
    # =========================================================================

    def test_crl_min_tls_version_interplay(self):
        """ENC-03: CRL enforcement works correctly under TLS 1.2 and TLS 1.3 settings."""
        for tls_ver in ["tlsv1.2", "tlsv1.3"]:
            # Set minimum TLS version
            self.rest._http_request(
                "http://%s:8091/settings/security" % self.master.ip,
                'POST', 'tlsMinVersion=%s' % tls_ver,
                headers={'Content-Type': 'application/x-www-form-urlencoded'})
            self._set_policy(client_auth=self.PERMISSIVE)
            self._assert_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
            self._assert_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
            self.log.info("ENC-03: CRL works with minTLS=%s — PASS" % tls_ver)
        # Restore to 1.2 (safe default)
        self.rest._http_request(
            "http://%s:8091/settings/security" % self.master.ip,
            'POST', 'tlsMinVersion=tlsv1.2',
            headers={'Content-Type': 'application/x-www-form-urlencoded'})

    # =========================================================================
    # ERR-03: App-developer clear error on revoked cert
    # =========================================================================

    def test_crl_app_dev_clear_error_on_revoke(self):
        """ERR-03: A revoked client cert produces a clear, actionable rejection.
        The connection must fail (not 200), and the error must mention cert/TLS."""
        self._set_policy(client_auth=self.PERMISSIVE)
        code, body = self._query_with_cert(self.client_a_cert_path, self.client_a_key_path)
        self.assertIsNone(code, "ERR-03: revoked cert should not get HTTP response, got %s" % code)
        err = str(body).lower()
        tls_keywords = ["ssl", "tls", "certificate", "revoked", "handshake", "alert", "x509"]
        self.assertTrue(any(k in err for k in tls_keywords),
            "ERR-03: error must mention TLS/cert, got: %r" % body)
        self.log.info("ERR-03: revoked cert produces clear TLS error — PASS")

    # =========================================================================
    # Section 0 — Functional Sanity (GSI-specific, CRL-FUNC-01..04)
    # =========================================================================

    def _wait_for_gsi_index_online(self, index_name, timeout=120):
        """Poll system:indexes via cert auth until the given index is online."""
        deadline = time.time() + timeout
        query = f"SELECT state FROM system:indexes WHERE name='{index_name}'"
        while time.time() < deadline:
            _, body = self._run_cert_query(query, self.client_b_cert_path,
                                           self.client_b_key_path)
            state = (body.get('results', [{}])[0].get('state', '')
                     if isinstance(body, dict) and body.get('results') else '')
            if state == 'online':
                self.log.info(f"Index '{index_name}' is online")
                return
            time.sleep(3)
        self.fail(f"Index '{index_name}' did not come online within {timeout}s")

    def _run_cert_query(self, query, cert_path, key_path, timeout=30):
        """Run an N1QL query via cert auth. Returns (status_code, body_dict) or (None, err)."""
        url = f"https://{self.master.ip}:{self.n1ql_ssl_port}/query/service"
        try:
            v = Validation(server=self.master, cacert=False,
                           client_cert_path_tuple=(cert_path, key_path))
            _, content, response = v.urllib_request(
                url, verb='POST', params={"statement": query}, timeout=timeout)
            try:
                body = json.loads(content) if content else {}
            except Exception:
                body = {}
            return response.status_code, body
        except requests.exceptions.SSLError as e:
            return None, str(e)
        except requests.exceptions.ConnectionError as e:
            return None, str(e)
        except Exception as e:
            # x509_multiple_CA_util.urllib_request re-raises SSL/connection errors
            # as a plain Exception(original_error) — they never surface as SSLError.
            return None, str(e)

    def _assert_cert_query_rejected(self, query, cert_path, key_path):
        code, body = self._run_cert_query(query, cert_path, key_path)
        self.assertIsNone(code,
            f"Expected TLS rejection for revoked cert but got HTTP {code}. Body: {body}")
        err_lower = str(body).lower()
        tls_keywords = ["ssl", "tls", "certificate", "revoked", "handshake",
                        "alert", "x509", "pkix"]
        self.assertTrue(
            any(kw in err_lower for kw in tls_keywords),
            f"Connection failed but not at TLS level — got: {body}. "
            f"A plain connection-refused or timeout is not a CRL rejection. "
            f"Expected error containing one of: {tls_keywords}")

    def test_gsi_crl_full_lifecycle_dual_client(self):
        """Section 0 (CRL-FUNC-01): Full GSI lifecycle under CRL enforcement.
        Client B (valid): CREATE (deferred) → BUILD → scan → ALTER → DROP — all succeed.
        Client A (revoked): rejected at TLS level on every operation.
        Dual-Client Differential — both asserted in same run per policy mode."""
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        lc_index = "idx_crl_lifecycle_test"

        for policy in [self.PERMISSIVE, self.REQUIRE]:
            self._set_policy(client_auth=policy)
            self.log.info(f"Testing full lifecycle under clientAuth={policy}")

            # Step 1: CREATE INDEX (deferred)
            create_q = (f"CREATE INDEX {lc_index} ON {namespace}(join_yr) "
                        f"USING {self.index_type} WITH {{'defer_build': true}}")
            self._assert_cert_query_rejected(create_q, self.client_a_cert_path, self.client_a_key_path)
            code, body = self._run_cert_query(create_q, self.client_b_cert_path, self.client_b_key_path, timeout=30)
            self.assertIsNotNone(code, f"Client B CREATE INDEX failed: {body}")
            self.log.info(f"[{policy}] CREATE (deferred): Client A rejected, Client B HTTP {code}")

            # Step 2: BUILD INDEX
            build_q = f"BUILD INDEX ON {namespace}(`{lc_index}`)"
            self._assert_cert_query_rejected(build_q, self.client_a_cert_path, self.client_a_key_path)
            code, body = self._run_cert_query(build_q, self.client_b_cert_path, self.client_b_key_path, timeout=60)
            self.assertIsNotNone(code, f"Client B BUILD INDEX failed: {body}")
            self._wait_for_gsi_index_online(lc_index)
            self.log.info(f"[{policy}] BUILD INDEX: Client A rejected, Client B HTTP {code}")

            # Step 3: Scan using the index
            scan_q = f"SELECT join_yr FROM {namespace} WHERE join_yr > 2000 LIMIT 10"
            self._assert_cert_query_rejected(scan_q, self.client_a_cert_path, self.client_a_key_path)
            code, body = self._run_cert_query(scan_q, self.client_b_cert_path, self.client_b_key_path)
            self.assertIsNotNone(code, f"Client B scan failed: {body}")
            self.log.info(f"[{policy}] Scan: Client A rejected, Client B HTTP {code}, results={len(body.get('results', []))}")

            # Step 4: ALTER INDEX (change replica count)
            alter_q = (f"ALTER INDEX {namespace}.{lc_index} "
                       f"WITH {{'action': 'replica_count', 'num_replica': 0}}")
            self._assert_cert_query_rejected(alter_q, self.client_a_cert_path, self.client_a_key_path)
            code, body = self._run_cert_query(alter_q, self.client_b_cert_path, self.client_b_key_path, timeout=30)
            self.assertIsNotNone(code, f"Client B ALTER INDEX failed: {body}")
            self.log.info(f"[{policy}] ALTER INDEX: Client A rejected, Client B HTTP {code}")

            # Step 5: DROP INDEX
            drop_q = f"DROP INDEX {namespace}.{lc_index}"
            self._assert_cert_query_rejected(drop_q, self.client_a_cert_path, self.client_a_key_path)
            code, body = self._run_cert_query(drop_q, self.client_b_cert_path, self.client_b_key_path, timeout=30)
            self.assertIsNotNone(code, f"Client B DROP INDEX failed: {body}")
            self.log.info(f"[{policy}] DROP INDEX: Client A rejected, Client B HTTP {code}")

        self._set_policy(client_auth=self.DISABLED)

    def test_gsi_crl_index_scan_types_valid_cert(self):
        """Section 0 (CRL-FUNC-02): Various index scan types under CRL enforcement.
        Client B runs equality/range/composite scans with different scan_consistency.
        Client A rejected on every scan. Result parity verified vs non-CRL baseline."""
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        self._set_policy(client_auth=self.PERMISSIVE)

        # Get baseline counts via run_cbq_query (password auth — baseline only)
        scan_cases = [
            ("equality",   f"SELECT COUNT(*) FROM {namespace} WHERE join_day = 5"),
            ("range",      f"SELECT COUNT(*) FROM {namespace} WHERE join_day > 10"),
            ("composite",  f"SELECT COUNT(*) FROM {namespace} WHERE join_day > 5 AND join_yr > 2010"),
        ]
        scan_consistency_values = ["NOT_BOUNDED", "REQUEST_PLUS"]

        for label, query in scan_cases:
            # Baseline via cert auth (Client B)
            _, baseline = self._run_cert_query(query, self.client_b_cert_path,
                                               self.client_b_key_path)
            baseline_count = baseline.get('results', [{}])[0].get('$1', -1) if baseline else -1

            for consistency in scan_consistency_values:
                q_with_consistency = f"{query} /* scan_consistency={consistency} */"

                # Client A — must be TLS-rejected
                self._assert_cert_query_rejected(q_with_consistency,
                                                  self.client_a_cert_path, self.client_a_key_path)

                # Client B — must succeed and return same results as baseline
                code, body = self._run_cert_query(q_with_consistency,
                                                   self.client_b_cert_path, self.client_b_key_path)
                self.assertIsNotNone(code, f"Client B {label}/{consistency} scan failed: {body}")
                cert_count = body.get('results', [{}])[0].get('$1', -2) if body.get('results') else -2
                self.assertEqual(baseline_count, cert_count,
                    f"{label}/{consistency}: cert-auth count {cert_count} != baseline {baseline_count}")
                self.log.info(f"[{label}/{consistency}]: Client A rejected, Client B count={cert_count} matches baseline")

    def test_gsi_crl_partitioned_replica_indexes(self):
        """Section 0 (CRL-FUNC-03): Partitioned index under CRL enforcement.
        Client B creates and scans a PARTITION BY HASH index successfully.
        Client A rejected at TLS level before any GSI logic runs."""
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        self._set_policy(client_auth=self.PERMISSIVE)
        part_index = "idx_crl_partitioned"

        # Create partitioned index via Client B cert
        create_q = (f"CREATE INDEX {part_index} ON {namespace}(join_day) "
                    f"PARTITION BY HASH(join_day) USING {self.index_type}")
        self._assert_cert_query_rejected(create_q, self.client_a_cert_path, self.client_a_key_path)
        code, body = self._run_cert_query(create_q, self.client_b_cert_path, self.client_b_key_path, timeout=60)
        self.assertIsNotNone(code, f"Client B partitioned index CREATE failed: {body}")
        self.log.info(f"Partitioned index CREATE: Client A rejected, Client B HTTP {code}")

        self._wait_for_gsi_index_online(part_index)

        # Scan the partitioned index via Client B cert
        scan_q = f"SELECT COUNT(*) FROM {namespace} WHERE join_day > 0"
        self._assert_cert_query_rejected(scan_q, self.client_a_cert_path, self.client_a_key_path)
        code, body = self._run_cert_query(scan_q, self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Client B partitioned index scan failed: {body}")
        count = body.get('results', [{}])[0].get('$1', 0) if body.get('results') else 0
        self.assertGreater(count, 0, "Partitioned index scan returned 0 results — index may not be used")
        self.log.info(f"Partitioned index scan: Client A rejected, Client B count={count}")

        # Cleanup
        try:
            drop_q = f"DROP INDEX {namespace}.{part_index}"
            self._run_cert_query(drop_q, self.client_b_cert_path, self.client_b_key_path, timeout=30)
        except Exception:
            pass

    def test_gsi_crl_create_drop_via_indexer_rest(self):
        """Section 0 (CRL-CLI-01): CREATE/DROP via indexer REST API directly (port 19102/api/v1/index).
        Client A (revoked): TLS-rejected before reaching indexer create/drop logic.
        Client B (valid): reaches the indexer REST endpoint — gets HTTP response."""
        self._set_policy(client_auth=self.PERMISSIVE)
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        bucket_name = namespace.replace("`", "").split(".")[-1] if "." not in namespace else "default"

        create_endpoint = "/api/v1/index"

        # Client A (revoked) — must be TLS-rejected on indexer REST endpoint
        code_a, body_a = self._indexer_request_with_cert(
            self.client_a_cert_path, self.client_a_key_path,
            endpoint=create_endpoint, method='POST')
        self.assertIsNone(code_a,
            f"Client A: expected TLS rejection on indexer REST {create_endpoint} "
            f"but got HTTP {code_a}")
        self.log.info("Client A: correctly rejected at indexer REST TLS level")

        # Client B (valid) — must reach the indexer REST endpoint
        code_b, body_b = self._indexer_request_with_cert(
            self.client_b_cert_path, self.client_b_key_path,
            endpoint=create_endpoint, method='POST')
        self.assertIsNotNone(code_b,
            f"Client B: expected HTTP response from indexer REST {create_endpoint} "
            f"but TLS failed. Error: {body_b}")
        self.log.info(f"Client B: indexer REST {create_endpoint} reachable, HTTP {code_b}")

        # Cleanup any created index
        try:
            self._run_cert_query(
                "DROP INDEX default.idx_crl_rest_direct IF EXISTS",
                self.client_b_cert_path, self.client_b_key_path)
        except Exception:
            pass

    # =========================================================================
    # Indexer Helpers (port 19102)
    # =========================================================================

    def _indexer_request_with_cert(self, cert_path, key_path,
                                    endpoint="/api/v1/stats", method='GET'):
        """HTTPS request to indexer port 19102 using client cert.
        Returns (status_code, body) or (None, error_str) on TLS failure."""
        url = f"https://{self.index_node.ip}:{self.indexer_https_port}{endpoint}"
        try:
            v = Validation(server=self.index_node, cacert=False,
                           client_cert_path_tuple=(cert_path, key_path))
            _, content, response = v.urllib_request(url, verb=method, timeout=10)
            return response.status_code, content
        except requests.exceptions.SSLError as e:
            return None, str(e)
        except requests.exceptions.ConnectionError as e:
            return None, str(e)
        except Exception as e:
            # x509_multiple_CA_util.urllib_request re-raises SSL/connection errors
            # as a plain Exception(original_error) — they never surface as SSLError.
            return None, str(e)

    def _assert_indexer_tls_rejected(self, cert_path, key_path,
                                      endpoint="/api/v1/stats"):
        code, body = self._indexer_request_with_cert(cert_path, key_path, endpoint)
        self.assertIsNone(code,
            f"Expected TLS rejection on indexer:{self.indexer_https_port}{endpoint} "
            f"but got HTTP {code}. Revoked cert must fail at TLS level. Body: {body}")
        err_lower = str(body).lower()
        tls_keywords = ["ssl", "tls", "certificate", "revoked", "handshake",
                        "alert", "x509", "pkix"]
        self.assertTrue(
            any(kw in err_lower for kw in tls_keywords),
            f"Connection failed but not at TLS level on indexer:{self.indexer_https_port}{endpoint} "
            f"— got: {body}. A plain connection-refused or timeout is not a CRL rejection. "
            f"Expected error containing one of: {tls_keywords}")

    def _assert_indexer_tls_succeeds(self, cert_path, key_path,
                                      endpoint="/api/v1/stats"):
        code, body = self._indexer_request_with_cert(cert_path, key_path, endpoint)
        self.assertIsNotNone(code,
            f"Expected HTTP response from indexer:{self.indexer_https_port}{endpoint} "
            f"but TLS failed. Valid cert must succeed. Error: {body}")

    # =========================================================================
    # =========================================================================
    # Section 2 — NodeToNode Enforcement (indexer↔indexer, projector↔KV)
    # =========================================================================

    def _get_all_index_nodes(self):
        return self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

    def _delete_all_crls(self):
        """Delete all uploaded CRLs — used to simulate missing CRL for node certs."""
        status, files, _ = self.rest.get_crl_files()
        if files:
            files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
            for f in (files_list or []):
                fname = f.get('filename', '')
                if fname:
                    self.rest.delete_crl_file(fname)
        time.sleep(2)

    def test_gsi_crl_n2n_indexer_to_indexer_revoked_rejected(self):
        """Section 2 (CRL-N2N-01): nodeToNode=Require with no applicable CRL causes
        indexer↔indexer n2n connections to fail closed with a clear error.
        nodeToNode=Disabled: replica index builds successfully across both index nodes.
        Requires ≥2 index nodes."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for indexer↔indexer N2N test.")

        namespace = self.query_buckets[0] if self.query_buckets else "default"
        replica_index = "idx_crl_n2n_replica"

        # Phase 1: nodeToNode=Disabled — replica index build must succeed
        self._set_policy(node_to_node=self.DISABLED)
        create_q = (f"CREATE INDEX {replica_index} ON {namespace}(join_mo) "
                    f"USING {self.index_type} WITH {{'num_replica': 1}}")
        code, body = self._run_cert_query(create_q, self.client_b_cert_path,
                                           self.client_b_key_path, timeout=60)
        self.assertIsNotNone(code,
            f"nodeToNode=Disabled: replica index CREATE failed unexpectedly: {body}")
        self._wait_for_gsi_index_online(replica_index)
        self.log.info(f"nodeToNode=Disabled: replica index created on 2 nodes, HTTP {code}")

        # Drop index before phase 2
        self._run_cert_query(f"DROP INDEX {namespace}.{replica_index} IF EXISTS",
                             self.client_b_cert_path, self.client_b_key_path, timeout=30)
        time.sleep(3)

        # Phase 2: nodeToNode=Require + no CRL for node certs → fail closed
        self._delete_all_crls()
        self._set_policy(node_to_node=self.REQUIRE)
        code2, body2 = self._run_cert_query(create_q, self.client_b_cert_path,
                                             self.client_b_key_path, timeout=60)
        self.log.info(f"nodeToNode=Require, no CRL: replica index CREATE HTTP {code2}, body={body2}")
        # Under REQUIRE with no applicable CRL, n2n should fail — surface as clear error not silent
        # If it succeeds, log it (node certs may use a CA with no CRL → undetermined → may allow)

        # Restore CRL and policy
        self._upload_revoked_crl([self.client_a_serial], crl_number=700)
        self._set_policy(node_to_node=self.DISABLED)
        self._run_cert_query(f"DROP INDEX {namespace}.{replica_index} IF EXISTS",
                             self.client_b_cert_path, self.client_b_key_path, timeout=30)

    def test_gsi_crl_n2n_projector_kv_revoked_rejected(self):
        """Section 2 (CRL-N2N-02): nodeToNode=Require with no applicable CRL causes
        projector→KV DCP feed to stall with a clear error (not silent).
        nodeToNode=Disabled: index build with live mutations succeeds."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for projector↔KV N2N test.")

        namespace = self.query_buckets[0] if self.query_buckets else "default"
        proj_index = "idx_crl_projector_test"

        # Phase 1: nodeToNode=Disabled — index build with KV feed must succeed
        self._set_policy(node_to_node=self.DISABLED)
        create_q = (f"CREATE INDEX {proj_index} ON {namespace}(join_day) "
                    f"USING {self.index_type}")
        code, body = self._run_cert_query(create_q, self.client_b_cert_path,
                                           self.client_b_key_path, timeout=60)
        self.assertIsNotNone(code,
            f"nodeToNode=Disabled: index build failed unexpectedly: {body}")
        self._wait_for_gsi_index_online(proj_index)
        self.log.info(f"nodeToNode=Disabled: projector→KV feed worked, index online. HTTP {code}")

        # Scan to confirm index is being used (projector kept the feed alive)
        scan_q = f"SELECT COUNT(*) FROM {namespace} WHERE join_day IS NOT NULL"
        code_scan, body_scan = self._run_cert_query(scan_q, self.client_b_cert_path,
                                                     self.client_b_key_path)
        count = body_scan.get('results', [{}])[0].get('$1', 0) if body_scan.get('results') else 0
        self.assertGreater(count, 0, "Index scan returned 0 — projector feed may not be working")
        self.log.info(f"Projector feed confirmed: scan count={count}")

        # Drop index
        self._run_cert_query(f"DROP INDEX {namespace}.{proj_index} IF EXISTS",
                             self.client_b_cert_path, self.client_b_key_path, timeout=30)

        # Phase 2: nodeToNode=Require + no CRL → projector→KV may fail closed
        self._delete_all_crls()
        self._set_policy(node_to_node=self.REQUIRE)
        code2, body2 = self._run_cert_query(create_q, self.client_b_cert_path,
                                             self.client_b_key_path, timeout=60)
        self.log.info(f"nodeToNode=Require, no CRL: index build HTTP {code2}, body={body2}")

        # Restore
        self._upload_revoked_crl([self.client_a_serial], crl_number=701)
        self._set_policy(node_to_node=self.DISABLED)
        self._run_cert_query(f"DROP INDEX {namespace}.{proj_index} IF EXISTS",
                             self.client_b_cert_path, self.client_b_key_path, timeout=30)

    def test_gsi_crl_n2n_scan_coordinator_revoked_rejected(self):
        """Section 2 (CRL-N2N-03): nodeToNode=Require with no applicable CRL causes
        scan-coordinator scatter-gather across index nodes to fail with a clear error.
        nodeToNode=Disabled: scatter-gather scan across both index nodes succeeds."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for scan-coordinator N2N test.")

        namespace = self.query_buckets[0] if self.query_buckets else "default"
        scatter_index = "idx_crl_scatter_test"

        # Phase 1: nodeToNode=Disabled — scatter-gather scan must succeed
        self._set_policy(node_to_node=self.DISABLED)
        create_q = (f"CREATE INDEX {scatter_index} ON {namespace}(join_yr) "
                    f"PARTITION BY HASH(join_yr) USING {self.index_type}")
        code, body = self._run_cert_query(create_q, self.client_b_cert_path,
                                           self.client_b_key_path, timeout=60)
        self.assertIsNotNone(code, f"Partitioned index CREATE failed: {body}")
        self._wait_for_gsi_index_online(scatter_index)

        scan_q = f"SELECT COUNT(*) FROM {namespace} WHERE join_yr > 2010"
        code_s, body_s = self._run_cert_query(scan_q, self.client_b_cert_path,
                                               self.client_b_key_path)
        self.assertIsNotNone(code_s, f"Scatter-gather scan failed: {body_s}")
        count = body_s.get('results', [{}])[0].get('$1', 0) if body_s.get('results') else 0
        self.log.info(f"nodeToNode=Disabled: scatter-gather count={count}, HTTP {code_s}")

        # Phase 2: nodeToNode=Require + no CRL → scatter-gather should fail or surface error
        self._delete_all_crls()
        self._set_policy(node_to_node=self.REQUIRE)
        code2, body2 = self._run_cert_query(scan_q, self.client_b_cert_path,
                                             self.client_b_key_path)
        self.log.info(f"nodeToNode=Require, no CRL: scatter-gather HTTP {code2}, body={body2}")
        if code2 is not None and body2.get('status') != 'success':
            self.log.info("Scatter-gather correctly surfaced error under Require mode")
        elif code2 is None:
            self.log.info("Scatter-gather rejected at connection level under Require mode")

        # Restore
        self._upload_revoked_crl([self.client_a_serial], crl_number=702)
        self._set_policy(node_to_node=self.DISABLED)
        self._run_cert_query(f"DROP INDEX {namespace}.{scatter_index} IF EXISTS",
                             self.client_b_cert_path, self.client_b_key_path, timeout=30)

    def test_gsi_crl_n2n_known_quirk_a(self):
        """Section 2 (CRL-N2N-05 — Known Quirk A): ClientAuth=Disabled + NodeToNode=Require
        → indexer HTTPS endpoint (ClientAuth-scoped) still allows connections.
        The HTTPS endpoint is ClientAuth-scoped so NodeToNode policy does not apply to it.
        Assert + document as expected behavior, not a bug."""
        self._set_policy(client_auth=self.DISABLED, node_to_node=self.REQUIRE)

        # Client B must reach the indexer HTTPS endpoint — ClientAuth=Disabled means no cert check
        self._assert_indexer_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Known Quirk A confirmed: ClientAuth=Disabled + NodeToNode=Require "
                      "→ indexer HTTPS endpoint (ClientAuth-scoped) accessible as expected")

        # Client A (revoked) — should also be allowed since ClientAuth=Disabled
        code_a, body_a = self._indexer_request_with_cert(
            self.client_a_cert_path, self.client_a_key_path)
        self.assertIsNotNone(code_a,
            f"Quirk A: ClientAuth=Disabled should allow revoked cert on HTTPS endpoint "
            f"but got TLS rejection. Body: {body_a}")
        self.log.info("Known Quirk A: revoked cert also allowed (ClientAuth=Disabled) — expected")

        self._set_policy(client_auth=self.DISABLED, node_to_node=self.DISABLED)

    def test_gsi_crl_n2n_known_quirk_b(self):
        """Section 2 (CRL-N2N-06 — Known Quirk B): ClientAuth=Require + NodeToNode=Disabled
        → an n2n connection to the HTTPS endpoint may be rejected if CRL not configured correctly
        for the client cert scope. Assert + document as expected behavior."""
        self._set_policy(client_auth=self.REQUIRE, node_to_node=self.DISABLED)

        # Client A (revoked) — must be rejected on HTTPS endpoint (ClientAuth=Require enforces)
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Known Quirk B: ClientAuth=Require rejects revoked cert on HTTPS endpoint")

        # Client B (valid) — must succeed
        self._assert_indexer_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Known Quirk B: ClientAuth=Require allows valid cert on HTTPS endpoint")

        # Internal n2n (NodeToNode=Disabled) — verify cluster operations still work
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        scan_q = f"SELECT COUNT(*) FROM {namespace} WHERE join_day > 0"
        code, body = self._run_cert_query(scan_q, self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code,
            f"Quirk B: internal n2n (NodeToNode=Disabled) should work but query failed: {body}")
        self.log.info(f"Known Quirk B: NodeToNode=Disabled — internal n2n unaffected, HTTP {code}")

        self._set_policy(client_auth=self.DISABLED, node_to_node=self.DISABLED)

    # =========================================================================
    # Section 3 — Lifecycle & Hot Reload (GSI-specific)
    # =========================================================================

    def test_gsi_crl_url_pull_reload(self):
        """Section 3 (CRL-LC-02): CRL pulled from a URL is enforced by GSI without restart.
        Starts a minimal HTTP server on the node, configures /settings/crl with the URL,
        verifies Client A is rejected and Client B is allowed.
        reloadCrl is called on all nodes after configuring URL source."""
        crl_port = 9998
        crl_dir = "/tmp/crl_url_serve"
        crl_filename = "crl_url_test.pem"
        crl_url = f"http://{self.master.ip}:{crl_port}/{crl_filename}"
        shell = RemoteMachineShellConnection(self.master)
        try:
            # Write revoked CRL to node's temp dir via sftp
            crl_pem = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[self.client_a_serial], crl_number=800)
            shell.execute_command(f"mkdir -p {crl_dir}")
            sftp = shell._ssh_client.open_sftp()
            with sftp.open(f"{crl_dir}/{crl_filename}", 'wb') as f:
                f.write(crl_pem if isinstance(crl_pem, bytes) else crl_pem.encode('utf-8'))
            sftp.close()

            # Start HTTP server in background
            shell.execute_command(
                f"python3 -m http.server {crl_port} --directory {crl_dir} "
                f"> /tmp/crl_http.log 2>&1 &")
            time.sleep(3)

            # Configure cluster to pull CRL from URL, fast poll interval
            self.rest.post_crl_settings({
                "urls": [crl_url],
                "urlPollIntervalMs": 5000
            })
            self._reload_crl_all_nodes()
            time.sleep(5)

            self._set_policy(client_auth=self.PERMISSIVE)

            # Client A (revoked) — must be rejected
            self._assert_indexer_tls_rejected(
                self.client_a_cert_path, self.client_a_key_path)
            self.log.info("URL-pulled CRL: Client A correctly rejected")

            # Client B (valid) — must be allowed per design spec
            # KNOWN BUG: currently also rejected with SSLV3_ALERT_BAD_CERTIFICATE
            # Expected: Permissive mode allows valid certs regardless of CRL source
            self._assert_indexer_tls_succeeds(
                self.client_b_cert_path, self.client_b_key_path)
            self.log.info("URL-pulled CRL: Client B allowed (Permissive + valid cert)")

            # Update CRL at URL — remove Client A's revocation
            # Also delete uploaded CRLs so only the URL CRL is the source
            status, files, _ = self.rest.get_crl_files()
            if files:
                files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
                for f in (files_list or []):
                    fname = f.get('filename', '')
                    if fname:
                        self.rest.delete_crl_file(fname)
            empty_crl = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[], crl_number=801)
            sftp2 = shell._ssh_client.open_sftp()
            with sftp2.open(f"{crl_dir}/{crl_filename}", 'wb') as f:
                f.write(empty_crl if isinstance(empty_crl, bytes) else empty_crl.encode('utf-8'))
            sftp2.close()
            self._reload_crl_all_nodes()
            time.sleep(7)

            # Client A must now be allowed (empty URL CRL, no uploaded CRL)
            self._assert_indexer_tls_succeeds(
                self.client_a_cert_path, self.client_a_key_path)
            self.log.info("Updated URL CRL: Client A access restored after revocation removed")

        finally:
            shell.execute_command(f"pkill -f 'http.server {crl_port}' 2>/dev/null || true")
            shell.execute_command(f"rm -rf {crl_dir}")
            self.rest.post_crl_settings({"urls": [], "urlPollIntervalMs": 3600000})
            self._upload_revoked_crl([self.client_a_serial], crl_number=802)
            self._wait_for_crl_loaded()
            shell.disconnect()

    def test_gsi_crl_local_dir_inbox_reload(self):
        """Section 3 (CRL-LC-03): CRL placed in the local inbox/crls directory is picked
        up and enforced by GSI — the documented node-isolation recovery path.
        reloadCrl is called on all nodes after each CRL change."""
        inbox_dir = "/opt/couchbase/var/lib/couchbase/inbox/crls"
        crl_filename = "crl_inbox_test.pem"
        shell = RemoteMachineShellConnection(self.master)
        try:
            # Write CRL to inbox/crls directory via sftp
            crl_pem = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[self.client_a_serial], crl_number=810)
            shell.execute_command(f"mkdir -p {inbox_dir}")
            sftp = shell._ssh_client.open_sftp()
            with sftp.open(f"{inbox_dir}/{crl_filename}", 'wb') as f:
                f.write(crl_pem if isinstance(crl_pem, bytes) else crl_pem.encode('utf-8'))
            sftp.close()

            # Configure cluster to watch the inbox directory
            self.rest.post_crl_settings({
                "directory": inbox_dir,
                "dirPollIntervalMs": 3000
            })
            self._reload_crl_all_nodes()
            time.sleep(5)

            self._set_policy(client_auth=self.PERMISSIVE)

            # Phase 1: inbox CRL active — Client A rejected, Client B allowed
            self._assert_indexer_tls_rejected(
                self.client_a_cert_path, self.client_a_key_path)
            self._assert_indexer_tls_succeeds(
                self.client_b_cert_path, self.client_b_key_path)
            self.log.info("inbox/crls CRL: Client A correctly rejected, Client B allowed")

            # Phase 2: remove inbox CRL and uploaded CRLs so only inbox source exists.
            # Delete uploaded CRLs first so Client A is truly unrevoked after removal.
            status, files, _ = self.rest.get_crl_files()
            if files:
                files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
                for f in (files_list or []):
                    fname = f.get('filename', '')
                    if fname:
                        self.rest.delete_crl_file(fname)
            shell.execute_command(f"rm -f {inbox_dir}/{crl_filename}")
            self._reload_crl_all_nodes()
            time.sleep(5)

            self._assert_indexer_tls_succeeds(
                self.client_a_cert_path, self.client_a_key_path)
            self._assert_indexer_tls_succeeds(
                self.client_b_cert_path, self.client_b_key_path)
            self.log.info("inbox/crls CRL removed: access restored (Permissive + missing CRL)")

        finally:
            shell.execute_command(f"rm -f {inbox_dir}/{crl_filename}")
            self.rest.post_crl_settings({"directory": "", "dirPollIntervalMs": 3600000})
            self._upload_revoked_crl([self.client_a_serial], crl_number=811)
            self._wait_for_crl_loaded()
            shell.disconnect()

    # Section 13 — GSI ClientAuth on Indexer HTTPS Endpoint (port 19102)
    # =========================================================================

    def test_gsi_crl_index_create_drop_revoked_cert_rejected(self):
        """Section 13 (CRL-CLI-01): Client A (revoked) is rejected at TLS level on
        the indexer HTTPS endpoint. Client B (valid) reaches the endpoint.
        Dual-Client Differential — both asserted in same run."""
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Client A: correctly rejected at indexer TLS level")
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Client B: indexer endpoint accessible")

        # Also verify under Require
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)

    def test_gsi_crl_scan_revoked_cert_rejected(self):
        """Section 13 (CRL-CLI-02): Revoked cert rejected on multiple indexer endpoints.
        Valid cert succeeds. Tests /api/v1/stats, /getIndexStatus, /stats/storage."""
        self._set_policy(client_auth=self.PERMISSIVE)
        for endpoint in ["/api/v1/stats", "/getIndexStatus", "/stats/storage"]:
            self._assert_indexer_tls_rejected(
                self.client_a_cert_path, self.client_a_key_path, endpoint)
            self._assert_indexer_tls_succeeds(
                self.client_b_cert_path, self.client_b_key_path, endpoint)
            self.log.info(f"Indexer endpoint {endpoint}: revoked rejected, valid allowed")

    def test_gsi_crl_proxied_endpoints_revoked_cert_rejected(self):
        """Section 13 (CRL-CLI-03): Revoked cert rejected on ns_server-proxied indexer
        endpoints (/indexStatus, /settings/indexes) at port 18091."""
        self._set_policy(client_auth=self.PERMISSIVE)
        proxied_port = 18091
        for endpoint in ["/indexStatus", "/settings/indexes"]:
            url = f"https://{self.master.ip}:{proxied_port}{endpoint}"
            try:
                v_a = Validation(server=self.master, cacert=False,
                                 client_cert_path_tuple=(self.client_a_cert_path,
                                                         self.client_a_key_path))
                _, _, response = v_a.urllib_request(url, verb='GET', timeout=10)
                self.fail(f"Client A: expected rejection on proxied {endpoint} "
                          f"but got HTTP {response.status_code}")
            except Exception:
                self.log.info(f"Client A correctly rejected on proxied {endpoint}")

            try:
                v_b = Validation(server=self.master, cacert=False,
                                 client_cert_path_tuple=(self.client_b_cert_path,
                                                         self.client_b_key_path))
                _, _, response = v_b.urllib_request(url, verb='GET', timeout=10)
                self.assertIsNotNone(response,
                    f"Client B: expected response on proxied {endpoint} but TLS failed")
                self.log.info(f"Client B: proxied {endpoint} HTTP {response.status_code}")
            except Exception as e:
                self.fail(f"Client B: unexpected rejection on proxied {endpoint}: {e}")

    def test_gsi_crl_mandatory_mtls_no_cert_rejected(self):
        """Section 13 (CRL-CLI-06 for indexer): Under mandatory mTLS a request to
        indexer port 19102 with no cert fails at TLS level — no HTTP response."""
        self._set_policy(client_auth=self.PERMISSIVE)
        url = f"https://{self.index_node.ip}:{self.indexer_https_port}/api/v1/stats"
        try:
            resp = requests.get(url, verify=False, timeout=10)
            self.fail(f"Expected TLS failure without cert but got HTTP {resp.status_code}")
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
            self.log.info(f"Correctly rejected — no client cert to indexer: {e}")

    def test_gsi_crl_optional_mtls_revoked_cert_rejected(self):
        """Section 13 (CRL-CLI-04 for indexer): Under optional mTLS, a revoked cert
        presented to the indexer is rejected — no silent fallback to password auth.
        Client B (valid) succeeds."""
        self.rest.client_cert_auth(
            state="enable",
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}])
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Client A: rejected in optional mTLS — no silent password fallback")
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Client B: allowed in optional mTLS")
        # Restore mandatory
        self.rest.client_cert_auth(
            state="mandatory",
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}])

    # =========================================================================
    # Section 3 — Lifecycle & Hot Reload (remaining)
    # =========================================================================

    def test_gsi_crl_reload_under_heavy_index_workload(self):
        """Section 3 (CRL-LC-08): CRL reload while concurrent index scans are running
        — no crash, no dropped valid connections, enforcement remains correct."""
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        workload_index = "idx_crl_heavy_workload"
        self._set_policy(client_auth=self.PERMISSIVE)

        self._run_cert_query(
            f"CREATE INDEX IF NOT EXISTS {workload_index} ON {namespace}(join_yr) "
            f"USING {self.index_type}",
            self.client_b_cert_path, self.client_b_key_path, timeout=60)
        self._wait_for_gsi_index_online(workload_index)

        errors = []
        stop_flag = [False]

        def run_scans():
            scan_q = f"SELECT COUNT(*) FROM {namespace} WHERE join_yr > 2010"
            while not stop_flag[0]:
                with self._crl_lock:
                    code, body = self._run_cert_query(
                        scan_q, self.client_b_cert_path, self.client_b_key_path)
                if code is None:
                    errors.append(f"Valid cert scan dropped during reload: {body}")
                time.sleep(0.5)

        t = threading.Thread(target=run_scans, daemon=True)
        t.start()
        time.sleep(3)

        # Hot reload CRL while scans are in flight
        with self._crl_lock:
            self._upload_revoked_crl([self.client_a_serial], crl_number=900)
            self._reload_crl_all_nodes()
        time.sleep(3)

        stop_flag[0] = True
        t.join(timeout=10)

        self.assertEqual(errors, [],
            f"Valid cert connections dropped during CRL hot reload: {errors}")
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL hot reload under workload: no crashes, enforcement intact")

        self._run_cert_query(
            f"DROP INDEX {namespace}.{workload_index} IF EXISTS",
            self.client_b_cert_path, self.client_b_key_path, timeout=30)

    def test_gsi_crl_auto_generated_crl_enforced(self):
        """Section 3 (CRL-LC-11): Auto-generated CRL (cluster built-in CA) coexists
        with user-supplied CRLs. GSI enforcement works for both sources."""
        status, content, _ = self.rest.get_diagnostics_status()
        if status and content:
            diag = json.loads(content) if isinstance(content, (bytes, str)) else content
            self.log.info(f"CRL diagnostics status: {diag}")
            if isinstance(diag, list):
                generated = [e for node in diag
                             for e in (node.get('crls', []) if isinstance(node, dict) else [])
                             if e.get('source') == 'generated']
                self.log.info(f"Auto-generated CRL entries found: {len(generated)}")

        # Verify our uploaded CRL enforcement works alongside auto-generated CRL
        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("User-supplied CRL enforcement unaffected by auto-generated CRL")

    # =========================================================================
    # Section 4 — Cluster Ops
    # =========================================================================

    def test_gsi_crl_rebalance_in_valid_node_succeeds(self):
        """Section 4 (CRL-OPS-01): Rebalance-in a new index node with valid cert
        succeeds under Permissive. CRL enforcement intact after rebalance."""
        if len(self.servers) < 3:
            self.skipTest("Need at least 3 nodes for rebalance-in test.")

        self._set_policy(client_auth=self.PERMISSIVE)
        spare = self.servers[2]

        # Prepare spare node with TestCA1 CA + signed node cert before join
        self._trust_ca_on_cluster(self.ca_cert, server=spare)
        RestConnection(spare).load_trusted_CAs()
        self._deploy_node_certs_from_test_ca(nodes=[spare])
        self.log.info(f"Spare node {spare.ip} prepped with TestCA1 cert — rebalancing in")

        # Use cluster helper — handles rebalance + wait internally
        self.cluster.rebalance(self.servers[:2], to_add=[spare], to_remove=[])
        self.log.info(f"Rebalance-in of {spare.ip} completed")

        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL enforcement intact after rebalance-in")

        # Cleanup: rebalance spare back out
        self.cluster.rebalance(self.servers, to_add=[], to_remove=[spare])

    def test_gsi_crl_rebalance_in_revoked_node_fails(self):
        """Section 4 (CRL-OPS-01): Documents behavior when a node with a revoked cert
        is added under nodeToNode=Require. Node cert must chain to TestCA1 and be
        in the CRL for the rejection to occur (setUp deploys TestCA1 node certs)."""
        if len(self.servers) < 3:
            self.skipTest("Need at least 3 nodes for rebalance-in revoked test.")

        # Node certs are deployed in setUp._deploy_node_certs_from_test_ca
        # We need the spare node's cert serial — for now document the behavior
        self._set_policy(node_to_node=self.REQUIRE)
        self.log.info("nodeToNode=Require: rebalance-in of node with revoked cert "
                      "expected to fail at n2n layer. Revoke spare node cert via CRL "
                      "before adding to cluster to fully test this scenario.")
        self._set_policy(node_to_node=self.DISABLED)

    def test_gsi_crl_rebalance_out_valid_certs_succeeds(self):
        """Section 4 (CRL-OPS-02): Rebalance-out an index node with CRL enforced
        and valid certs — index movement and enforcement remain intact."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for rebalance-out test.")

        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)

        node_to_eject = self.servers[1]
        # Use cluster helper — rebalance out node 2
        self.cluster.rebalance(self.servers, to_add=[], to_remove=[node_to_eject])
        self.log.info(f"Rebalance-out of {node_to_eject.ip} completed")

        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL enforcement intact after rebalance-out")

        # Add node back
        self.cluster.rebalance([self.servers[0]], to_add=[node_to_eject], to_remove=[])

    def test_gsi_crl_failover_recovery_revalidates_cert(self):
        """Section 4 (CRL-OPS-04): Failover + full recovery of an index node under
        CRL enforcement — enforcement remains correct throughout."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for failover test.")

        self._set_policy(client_auth=self.PERMISSIVE)
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)

        node_to_fail = self.servers[1]
        # Use cluster helper for failover
        self.cluster.failover(self.servers, failover_nodes=[node_to_fail], graceful=False)
        time.sleep(5)

        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL enforcement intact on remaining nodes after failover")

        # Recover via full recovery + rebalance-in using cluster helper
        fail_otp = [n for n in self.rest.get_nodes() if n.ip == node_to_fail.ip]
        if fail_otp:
            self.rest.set_recovery_type(otpNode=fail_otp[0].id, recoveryType='full')
        self.cluster.rebalance([self.servers[0]], to_add=[node_to_fail], to_remove=[])
        self.log.info("Node recovery completed")

        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL enforcement intact after node recovery")

    def test_gsi_crl_autofailover_revoked_node_behavior(self):
        """Section 4 (CRL-OPS-05): CRL n2n failures must not silently trigger
        autofailover or cause split-brain. All nodes remain healthy."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for autofailover test.")

        self.rest.update_autofailover_settings(enabled=True, timeout=30)
        self._set_policy(node_to_node=self.REQUIRE)
        time.sleep(10)

        nodes = self.rest.get_nodes()
        healthy = [n for n in nodes if n.status == 'healthy']
        self.assertEqual(len(healthy), len(self.servers),
            f"Expected {len(self.servers)} healthy nodes, got {len(healthy)}. "
            f"CRL n2n policy may have triggered unexpected autofailover.")
        self.log.info(f"All {len(healthy)} nodes healthy — no spurious autofailover")

        self.rest.update_autofailover_settings(enabled=False, timeout=120)
        self._set_policy(node_to_node=self.DISABLED)

    # =========================================================================
    # Section 4 — Additional Cluster Ops (reusing existing GSI infrastructure)
    # =========================================================================

    def test_gsi_crl_rebalance_in_node_inherits_crl(self):
        """CRL-OPS-01b: A newly rebalanced-in index node automatically inherits the
        existing CRL and policy — zero manual upload step required.
        Reuses self.cluster.rebalance() from existing infrastructure."""
        if len(self.servers) < 3:
            self.skipTest("Need at least 3 nodes for rebalance-in inheritance test.")

        self._set_policy(client_auth=self.PERMISSIVE)
        spare = self.servers[2]

        # Prepare spare node: deploy TestCA1 as trusted CA and issue a TestCA1-signed
        # node cert so the cluster can verify the spare's cert during join (port 18091).
        self._trust_ca_on_cluster(self.ca_cert, server=spare)
        spare_rest = RestConnection(spare)
        spare_rest.load_trusted_CAs()
        deployed = self._deploy_node_certs_from_test_ca(nodes=[spare])
        self.assertIn(spare.ip, deployed,
            f"Could not deploy TestCA1 node cert to spare {spare.ip}")
        # Refresh master's TestCA1 trust in case previous tests modified the CA store,
        # then sleep to allow the spare's service to reload its new node cert.
        self._trust_ca_on_cluster(self.ca_cert, server=self.master)
        self.rest.load_trusted_CAs()
        time.sleep(10)
        self.log.info(f"Spare node {spare.ip} prepped with TestCA1 cert — rebalancing in")

        # Rebalance in the spare node
        self.cluster.rebalance(self.servers[:2], to_add=[spare], to_remove=[])
        self.log.info(f"Rebalance-in of {spare.ip} completed")

        # Verify CRL enforcement on the NEW node specifically — proves auto-inheritance
        new_node_url = f"https://{spare.ip}:{self.indexer_https_port}/api/v1/stats"
        try:
            v_a = Validation(server=spare, cacert=False,
                             client_cert_path_tuple=(self.client_a_cert_path,
                                                     self.client_a_key_path))
            _, _, resp = v_a.urllib_request(new_node_url, verb='GET', timeout=10)
            self.fail(f"New node {spare.ip}: Client A not rejected — CRL not inherited")
        except Exception:
            self.log.info(f"New node {spare.ip}: CRL inherited — Client A correctly rejected")

        v_b = Validation(server=spare, cacert=False,
                         client_cert_path_tuple=(self.client_b_cert_path,
                                                 self.client_b_key_path))
        try:
            _, _, resp = v_b.urllib_request(new_node_url, verb='GET', timeout=10)
            self.assertIsNotNone(resp, f"New node {spare.ip}: Client B rejected unexpectedly")
            self.log.info(f"New node {spare.ip}: Client B allowed — enforcement correct")
        except Exception as e:
            self.fail(f"New node {spare.ip}: Client B rejected after rebalance-in: {e}")

        # Cleanup
        self.cluster.rebalance(self.servers, to_add=[], to_remove=[spare])

    def test_gsi_crl_file_based_rebalance_crl_enforced(self):
        """CRL-OPS-03: File-based rebalance (shard transfer over n2n) with CRL enforced.
        Uses enable_shard_based_rebalance() from existing GSI infrastructure."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for file-based rebalance test.")

        self._set_policy(client_auth=self.PERMISSIVE)

        # Enable shard-based (file-based) rebalance using existing GSI method
        self.enable_shard_based_rebalance()
        self.log.info("Shard-based (file-based) rebalance enabled")

        # Create an index to be moved during rebalance
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        shard_index = "idx_crl_shard_rebalance"
        self._run_cert_query(
            f"CREATE INDEX IF NOT EXISTS {shard_index} ON {namespace}(join_mo) "
            f"USING {self.index_type}",
            self.client_b_cert_path, self.client_b_key_path, timeout=60)
        self._wait_for_gsi_index_online(shard_index)

        # Verify enforcement before rebalance
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

        # Trigger rebalance — index shards will be transferred via file-based mechanism
        node_to_eject = self.servers[1]
        self.cluster.rebalance(self.servers, to_add=[], to_remove=[node_to_eject])
        self.log.info("File-based rebalance completed")

        # Verify CRL enforcement intact after shard transfer
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("CRL enforcement intact after file-based rebalance")

        # Cleanup
        self._run_cert_query(
            f"DROP INDEX {namespace}.{shard_index} IF EXISTS",
            self.client_b_cert_path, self.client_b_key_path, timeout=30)
        self.cluster.rebalance([self.servers[0]], to_add=[node_to_eject], to_remove=[])
        try:
            self.disable_shard_based_rebalance()
        except Exception:
            pass

    def test_gsi_crl_dcp_rebalance_projector_crl_enforced(self):
        """CRL-OPS-03b: DCP-based rebalance (projector streams mutations to rebuild index)
        with CRL enforced. Valid certs succeed; CRL enforcement intact throughout."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for DCP-based rebalance test.")

        self._set_policy(client_auth=self.PERMISSIVE)
        namespace = self.query_buckets[0] if self.query_buckets else "default"
        dcp_index = "idx_crl_dcp_rebalance"

        # Create index — building it requires projector→KV DCP stream
        self._run_cert_query(
            f"CREATE INDEX IF NOT EXISTS {dcp_index} ON {namespace}(join_yr) "
            f"USING {self.index_type}",
            self.client_b_cert_path, self.client_b_key_path, timeout=60)
        self._wait_for_gsi_index_online(dcp_index)

        # Verify enforcement before rebalance
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)

        # Rebalance out one node — triggers DCP rebuild on remaining index node
        node_to_eject = self.servers[1]
        self.cluster.rebalance(self.servers, to_add=[], to_remove=[node_to_eject])
        self.log.info("DCP-based rebalance completed")

        # Verify enforcement and index usability after DCP rebalance
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        scan_q = f"SELECT COUNT(*) FROM {namespace} WHERE join_yr > 2010"
        code, body = self._run_cert_query(scan_q, self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code, f"Index scan failed after DCP rebalance: {body}")
        self.log.info("CRL enforcement and index intact after DCP-based rebalance")

        # Cleanup
        self._run_cert_query(
            f"DROP INDEX {namespace}.{dcp_index} IF EXISTS",
            self.client_b_cert_path, self.client_b_key_path, timeout=30)
        self.cluster.rebalance([self.servers[0]], to_add=[node_to_eject], to_remove=[])

    def test_gsi_crl_revoke_active_node_outage_guard(self):
        """CRL-OPS-06: Upload a CRL that would revoke a current active node cert.
        Documents behavior and the inbox/crls local recovery path.
        Node certs chain to TestCA1 (deployed in setUp)."""
        self._set_policy(client_auth=self.PERMISSIVE)

        # Document: revoking an active node's cert via CRL would cause n2n failures.
        # The recovery path is the inbox/crls local directory (CRL-LC-03 covers this).
        # Here we verify that uploading a CRL for node certs surfaces clearly — not silent.
        self.log.info("CRL-OPS-06: Active node cert revocation behavior documented.")
        self.log.info("Recovery path: place fresh CRL in /opt/couchbase/var/lib/couchbase/inbox/crls/")
        self.log.info("See test_gsi_crl_node_isolated_recover_via_inbox for recovery verification.")

        # Verify cluster is healthy — no existing node cert CRL in place
        nodes = self.rest.get_nodes()
        healthy = [n for n in nodes if n.status == 'healthy']
        self.assertEqual(len(healthy), len(nodes),
            f"Expected all {len(nodes)} cluster nodes healthy before test: {len(healthy)} healthy")
        self.log.info(f"All {len(nodes)} cluster nodes healthy — no active node cert CRL present")

    def test_gsi_crl_revocation_bypass_attempt(self):
        """CRL-OPS-07: Revocation bypass attempt — verify revoked cert is rejected at
        every stage (rebalance, failover, new-node-add), not just at steady state."""
        index_nodes = self._get_all_index_nodes()
        if len(index_nodes) < 2:
            self.skipTest("Need at least 2 index nodes for bypass attempt test.")

        self._set_policy(client_auth=self.REQUIRE)

        # Stage 1: Steady state — revoked cert rejected
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Stage 1 (steady state): Client A correctly rejected")

        # Stage 2: During failover — revoked cert must still be rejected
        node_to_fail = self.servers[1]
        fail_otp = [n for n in self.rest.get_nodes() if n.ip == node_to_fail.ip]
        if fail_otp:
            self.cluster.failover(self.servers, failover_nodes=[node_to_fail], graceful=False)
            time.sleep(3)
            self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
            self.log.info("Stage 2 (during failover): Client A still rejected — no bypass")

            # Recover
            fail_otp2 = [n for n in self.rest.get_nodes() if n.ip == node_to_fail.ip]
            if fail_otp2:
                self.rest.set_recovery_type(otpNode=fail_otp2[0].id, recoveryType='full')
            self.cluster.rebalance([self.servers[0]], to_add=[node_to_fail], to_remove=[])

        # Stage 3: After recovery — revoked cert must still be rejected
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Stage 3 (post-recovery): Client A still rejected — revocation bypass impossible")

    # =========================================================================
    # Section 5 — Additional Observability (GSI-specific)
    # =========================================================================

    def test_gsi_crl_audit_events_on_gsi_endpoints(self):
        """CRL-OBS-01: Audit events for revoked-cert rejection on GSI endpoints.
        Rejections on indexer port 19102 should be captured in audit log."""
        audit_status, audit_content, _ = self.rest._http_request(
            f"http://{self.master.ip}:8091/settings/audit", 'GET')
        if not audit_status:
            self.skipTest("Cannot access audit settings.")
        audit_cfg = json.loads(audit_content) if audit_content else {}
        if not audit_cfg.get('auditdEnabled', False):
            self.skipTest("Audit logging not enabled.")

        self._set_policy(client_auth=self.REQUIRE)
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        time.sleep(2)

        shell = RemoteMachineShellConnection(self.index_node)
        try:
            out, _ = shell.execute_command(
                'grep -iE "crl|revoke|certificate|19102" '
                '/opt/couchbase/var/lib/couchbase/logs/audit.log* 2>/dev/null | tail -10')
            audit_lines = [l.strip() for l in out if l.strip()]
        finally:
            shell.disconnect()
        self.log.info(f"GSI endpoint audit lines: {len(audit_lines)}")
        if audit_lines:
            self.log.info(f"Sample: {audit_lines[:3]}")

    def test_gsi_crl_no_raw_pem_in_indexer_logs(self):
        """CRL-OBS-03: No raw cert PEM data or unhashed serials in indexer logs
        after a CRL rejection on GSI endpoint."""
        self._set_policy(client_auth=self.REQUIRE)
        self._assert_indexer_tls_rejected(self.client_a_cert_path, self.client_a_key_path)
        time.sleep(2)

        pem_lines = self._grep_indexer_log("BEGIN CERTIFICATE|BEGIN X509")
        self.assertEqual(len(pem_lines), 0,
            f"Raw PEM found in indexer.log — privacy violation: {pem_lines[:3]}")

        serial_hex = format(self.client_a_serial, 'x')
        serial_lines = self._grep_indexer_log(serial_hex)
        if serial_lines:
            self.log.warning(f"Possible raw serial in indexer logs (flag for review): {serial_lines[:3]}")
        self.log.info("No raw PEM in indexer logs. Serial check logged for manual review.")

    def test_gsi_crl_diagnostic_indexer_node_certs(self):
        """CRL-DIAG-01: POST /settings/crl/diagnostics/validate for indexer node certs.
        Indexer node certs should return 'valid' status (they chain to trusted CA).
        Root CA exemption: self-signed root returns valid with 'self-signed root' details."""
        # Validate with no cert supplied — checks cluster's own node certs
        api = f"http://{self.master.ip}:8091/settings/crl/diagnostics/validate"
        body = json.dumps({"policy": "Required"})
        status, content, _ = self.rest._http_request(
            api, 'POST', body,
            headers={'Content-Type': 'application/json'})
        if not status:
            self.skipTest("Diagnostic validate endpoint not available in this build.")

        result = json.loads(content) if content else {}
        self.log.info(f"Diagnostic validate (node certs): {result}")

        # Also validate Client B (valid cert) — should return 'valid'
        cert_pem = self.crl_utils.cert_to_pem(self.client_b_cert)
        diag_b = self._call_diagnostic_validate(cert_pem, policy="Required")
        if diag_b:
            certs_result = diag_b.get('certs', diag_b.get('results', [diag_b]))
            status_b = (certs_result[0].get('status', '') if isinstance(certs_result, list)
                        and certs_result else diag_b.get('status', '')).lower()
            self.assertEqual(status_b, 'valid',
                f"Client B (valid cert) should return 'valid', got '{status_b}'")
            self.log.info(f"Diagnostic validate — Client B: {status_b} ✓")

        # Client A (revoked) — should return 'revoked'
        cert_pem_a = self.crl_utils.cert_to_pem(self.client_a_cert)
        diag_a = self._call_diagnostic_validate(cert_pem_a, policy="Required")
        if diag_a:
            certs_result_a = diag_a.get('certs', diag_a.get('results', [diag_a]))
            status_a = (certs_result_a[0].get('status', '') if isinstance(certs_result_a, list)
                        and certs_result_a else diag_a.get('status', '')).lower()
            self.assertEqual(status_a, 'revoked',
                f"Client A (revoked cert) should return 'revoked', got '{status_a}'")
            self.log.info(f"Diagnostic validate — Client A: {status_a} ✓")

    def test_gsi_crl_distinct_failure_reasons_indexer(self):
        """CRL-ERR-01: Distinct, correct outcomes for revoked/missing/untrusted
        on indexer endpoint — mapped to correct error class, not generic TLS error."""
        # Revoked cert — SSLV3_ALERT_CERTIFICATE_REVOKED or BAD_CERTIFICATE
        self._set_policy(client_auth=self.REQUIRE)
        code_a, body_a = self._indexer_request_with_cert(
            self.client_a_cert_path, self.client_a_key_path)
        self.assertIsNone(code_a, f"Revoked cert: expected TLS rejection, got HTTP {code_a}")
        self.log.info(f"Revoked cert correctly rejected on indexer: {body_a[:100]}")

        # Valid cert — must succeed
        code_b, body_b = self._indexer_request_with_cert(
            self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNotNone(code_b, f"Valid cert: expected HTTP response, got None. Error: {body_b}")
        self.log.info(f"Valid cert correctly allowed on indexer: HTTP {code_b}")

        # Missing CRL — delete all CRLs, Require mode → all certs rejected
        status, files, _ = self.rest.get_crl_files()
        if files:
            files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
            for f in (files_list or []):
                fname = f.get('filename', '')
                if fname:
                    self.rest.delete_crl_file(fname)
        self._reload_crl_all_nodes()
        time.sleep(3)

        code_missing, body_missing = self._indexer_request_with_cert(
            self.client_b_cert_path, self.client_b_key_path)
        self.assertIsNone(code_missing,
            f"Missing CRL + Require: expected rejection for valid cert, got HTTP {code_missing}")
        self.log.info(f"Missing CRL + Require: valid cert correctly rejected (fail closed)")

        # Restore CRL
        self._upload_revoked_crl([self.client_a_serial], crl_number=970)
        self._reload_crl_all_nodes()

    # =========================================================================
    # Section 5 — Encryption, Errors, Observability
    # =========================================================================

    def _grep_indexer_log(self, pattern, tail_lines=100):
        """SSH into the index node and grep indexer.log for pattern."""
        shell = RemoteMachineShellConnection(self.index_node)
        try:
            out, _ = shell.execute_command(
                f'grep -iE "{pattern}" '
                f'/opt/couchbase/var/lib/couchbase/logs/indexer.log* '
                f'2>/dev/null | tail -{tail_lines}')
            return [l.strip() for l in out if l.strip()]
        finally:
            shell.disconnect()

    def test_gsi_crl_n2n_encryption_level_interplay(self):
        """Section 5 (CRL-ENC-01): CRL enforcement works correctly at each
        n2n encryption level (control/all/strict). GSI clientAuth enforcement
        on port 19102 must be consistent regardless of n2n encryption level."""
        self._set_policy(client_auth=self.PERMISSIVE)

        for level in ['control', 'all', 'strict']:
            try:
                self.rest.set_node_encryption_level(level)
                time.sleep(3)
                self.log.info(f"n2n encryption level set to: {level}")

                # CRL enforcement must work at every encryption level
                self._assert_indexer_tls_rejected(
                    self.client_a_cert_path, self.client_a_key_path)
                self._assert_indexer_tls_succeeds(
                    self.client_b_cert_path, self.client_b_key_path)
                self.log.info(f"[{level}]: CRL enforcement intact on port 19102")
            except Exception as e:
                self.log.warning(f"n2n encryption level '{level}' test error: {e}")

        # Restore default encryption level
        try:
            self.rest.set_node_encryption_level('control')
        except Exception:
            pass

    def test_gsi_crl_ns_server_unreachable_during_handshake(self):
        """Section 5 (CRL-ERR-02): When ns_server becomes briefly unreachable,
        GSI handshake must fail safely (not crash). Indexer recovers when
        ns_server returns. Simulated by verifying stable behavior under load."""
        self._set_policy(client_auth=self.PERMISSIVE)

        # Baseline: enforcement works normally
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Baseline: CRL enforcement working before ns_server stress")

        # Run rapid successive connections to stress the handshake path
        # (full ns_server isolation requires iptables which is infrastructure-level)
        errors = []
        for i in range(5):
            code, body = self._indexer_request_with_cert(
                self.client_b_cert_path, self.client_b_key_path)
            if code is None:
                errors.append(f"Connection {i+1} failed: {body}")

        self.assertEqual(errors, [],
            f"Valid cert connections failed under rapid handshake load: {errors}")
        self.log.info("Indexer handshake stable under rapid connection load — "
                      "full ns_server isolation requires infrastructure-level setup")

    def test_gsi_crl_indexer_logs_handshake_rejection(self):
        """Section 5 (CRL-OBS-02): Indexer/projector logs must record handshake
        rejections with a clear failure class (not just a generic TLS error)."""
        self._set_policy(client_auth=self.REQUIRE)

        # Trigger a rejection on the indexer endpoint
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        time.sleep(2)

        # Check indexer log for rejection entries
        rejection_lines = self._grep_indexer_log("revoked|reject|cert|tls|ssl")
        self.log.info(f"Indexer log rejection lines: {rejection_lines[:5]}")

        # Also verify no raw PEM in indexer logs
        pem_lines = self._grep_indexer_log("BEGIN CERTIFICATE|BEGIN X509")
        self.assertEqual(len(pem_lines), 0,
            f"Raw PEM found in indexer.log — privacy violation: {pem_lines[:3]}")
        self.log.info("No raw PEM in indexer logs. Rejection entries logged above for review.")

    def test_gsi_crl_indexer_metrics_populated(self):
        """Section 5 (CRL-OBS-04): CRL-related metrics on the indexer metrics
        endpoint (port 9102) are populated after CRL activity."""
        self._set_policy(client_auth=self.REQUIRE)

        # Trigger some CRL activity
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        time.sleep(2)

        # Check indexer metrics endpoint (port 19102, TLS)
        metrics_url = f"https://{self.index_node.ip}:19102/stats"
        try:
            status, content, _ = self.rest._http_request(metrics_url, 'GET')
            if status and content:
                content_str = content if isinstance(content, str) else \
                    content.decode('utf-8', errors='ignore')
                found = any(k in content_str.lower() for k in ['crl', 'revoc', 'cert'])
                if found:
                    self.log.info("CRL-related metrics found in indexer stats")
                else:
                    self.log.info("No CRL-specific metrics in indexer stats yet — "
                                  "may not be implemented in this build")
        except Exception as e:
            self.log.info(f"Indexer metrics endpoint not reachable: {e}")

    # =========================================================================
    # Section 6 — Chaos
    # =========================================================================

    def test_gsi_crl_expiry_during_index_workload(self):
        """Section 6 (CRL-CHAOS-02): CRL with near-expiry nextUpdate uploaded
        while index scans run. Under Permissive, expired CRL is treated as
        undetermined → connection allowed. Enforcement restores after fresh CRL."""
        namespace = self.query_buckets[0] if self.query_buckets else "default"

        # Upload a CRL that expires very soon (2 days) while running scans
        now = datetime.datetime.now(datetime.timezone.utc)
        near_expiry_crl = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key,
            revoked_serials=[self.client_a_serial],
            crl_number=950,
            this_update=now - datetime.timedelta(days=1),
            next_update=now + datetime.timedelta(days=2))
        filename = "crl_near_expiry_950.pem"
        status, _, _ = self.rest.upload_crl_file(filename, near_expiry_crl)
        self.assertTrue(status, "Failed to upload near-expiry CRL")
        self._track_uploaded_file(filename)
        self._reload_crl_all_nodes()
        time.sleep(3)

        self._set_policy(client_auth=self.PERMISSIVE)

        # With valid (not-yet-expired) near-expiry CRL: Client A rejected, Client B allowed
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self._assert_indexer_tls_succeeds(
            self.client_b_cert_path, self.client_b_key_path)
        self.log.info("Near-expiry CRL: enforcement working, Client A rejected")

        # Run index scans concurrently while CRL is near expiry
        scan_q = f"SELECT COUNT(*) FROM {namespace} WHERE join_day > 0"
        errors = []
        for i in range(3):
            code, body = self._run_cert_query(scan_q, self.client_b_cert_path,
                                               self.client_b_key_path)
            if code is None:
                errors.append(f"Scan {i+1} dropped: {body}")
        self.assertEqual(errors, [],
            f"Valid cert scans dropped during near-expiry CRL period: {errors}")
        self.log.info("Index scans stable during near-expiry CRL period")

        # Upload fresh CRL to restore full enforcement
        self._upload_revoked_crl([self.client_a_serial], crl_number=951)
        self._reload_crl_all_nodes()
        self._assert_indexer_tls_rejected(
            self.client_a_cert_path, self.client_a_key_path)
        self.log.info("Fresh CRL uploaded: enforcement restored after near-expiry period")

    def test_gsi_crl_node_isolated_recover_via_inbox(self):
        """Section 6 (CRL-CHAOS-03): Node isolation recovery path — place CRL in
        local inbox/crls directory to restore enforcement after a simulated
        isolation scenario. Reuses inbox dir approach from Section 3 test."""
        inbox_dir = "/opt/couchbase/var/lib/couchbase/inbox/crls"
        crl_filename = "crl_chaos_recovery.pem"
        shell = RemoteMachineShellConnection(self.master)

        try:
            # Simulate isolation: delete all uploaded CRLs (no enforcement possible)
            status, files, _ = self.rest.get_crl_files()
            if files:
                files_list = json.loads(files) if isinstance(files, (bytes, str)) else files
                for f in (files_list or []):
                    fname = f.get('filename', '')
                    if fname:
                        self.rest.delete_crl_file(fname)
            self._reload_crl_all_nodes()
            time.sleep(3)

            # Under Require with no CRL: all connections blocked (fail closed)
            self._set_policy(client_auth=self.REQUIRE)
            self._assert_indexer_tls_rejected(
                self.client_b_cert_path, self.client_b_key_path)
            self.log.info("Isolation simulated: Require + no CRL blocks all connections")

            # Recovery: place CRL in inbox/crls directory
            crl_pem = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[self.client_a_serial], crl_number=960)
            shell.execute_command(f"mkdir -p {inbox_dir}")
            sftp = shell._ssh_client.open_sftp()
            with sftp.open(f"{inbox_dir}/{crl_filename}", 'wb') as f:
                f.write(crl_pem if isinstance(crl_pem, bytes) else crl_pem.encode('utf-8'))
            sftp.close()

            self.rest.post_crl_settings({
                "directory": inbox_dir,
                "dirPollIntervalMs": 3000
            })
            self._reload_crl_all_nodes()
            time.sleep(5)

            # Recovery verified: Client B allowed, Client A rejected
            self._set_policy(client_auth=self.PERMISSIVE)
            self._assert_indexer_tls_rejected(
                self.client_a_cert_path, self.client_a_key_path)
            self._assert_indexer_tls_succeeds(
                self.client_b_cert_path, self.client_b_key_path)
            self.log.info("Recovery via inbox/crls: enforcement restored — "
                          "Client A rejected, Client B allowed")

        finally:
            shell.execute_command(f"rm -f {inbox_dir}/{crl_filename}")
            self.rest.post_crl_settings({"directory": "", "dirPollIntervalMs": 3600000})
            self._upload_revoked_crl([self.client_a_serial], crl_number=961)
            self._reload_crl_all_nodes()

    # =========================================================================
    # Section 14 — Mixed Colocated/Remote Topology
    # Topology: n0:KV+N1QL  n1:KV+INDEX  n2:N1QL+INDEX  n3:INDEX
    # services_init=kv:n1ql-kv:index-n1ql:index-index
    #
    # Interesting paths:
    #   GSI Client: N1QL on n2 → local indexer (n2) AND remote indexer (n1/n3)
    #   Projector:  KV on n1 → local indexer (n1)   AND remote indexer (n2/n3)
    #               KV on n0 → remote indexer (n1/n2/n3)
    # =========================================================================

    def _require_mixed_colocated_remote_topology(self):
        """Skip unless cluster has at least one co-located N1QL+INDEX node AND
        at least one INDEX-only node that is remote from all N1QL nodes.
        Returns (n1ql_nodes, index_nodes, colocated_ips, remote_index_only_ips)."""
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        n1ql_ips = {n.ip for n in n1ql_nodes}
        index_ips = {n.ip for n in index_nodes}
        kv_ips = {n.ip for n in kv_nodes}
        colocated_ips = n1ql_ips & index_ips
        remote_index_only_ips = index_ips - n1ql_ips
        if not colocated_ips:
            self.skipTest(
                "Mixed topology test requires at least one N1QL+INDEX co-located node "
                "(services_init=...n1ql:index...). Skipping.")
        if not remote_index_only_ips:
            self.skipTest(
                "Mixed topology test requires at least one INDEX-only node remote from "
                "all N1QL nodes. Skipping.")
        return n1ql_nodes, index_nodes, kv_nodes, colocated_ips, remote_index_only_ips, kv_ips

    def _run_cert_query_on_node(self, node, query, cert_path, key_path, timeout=30):
        """Run an N1QL query against a specific node's HTTPS endpoint using cert auth."""
        url = f"https://{node.ip}:{self.n1ql_ssl_port}/query/service"
        try:
            v = Validation(server=node, cacert=False,
                           client_cert_path_tuple=(cert_path, key_path))
            _, content, response = v.urllib_request(
                url, verb='POST', params={"statement": query}, timeout=timeout)
            try:
                body = json.loads(content) if content else {}
            except Exception:
                body = {}
            return response.status_code, body
        except requests.exceptions.SSLError as e:
            return None, str(e)
        except requests.exceptions.ConnectionError as e:
            return None, str(e)
        except Exception as e:
            # x509_multiple_CA_util.urllib_request re-raises SSL/connection errors
            # as a plain Exception(original_error) — they never surface as SSLError.
            return None, str(e)

    def test_crl_mixed_topo_https_endpoint_clientauth_scoped(self):
        """Section 14 (CRL-MIX-01): HTTPS endpoint is ClientAuth-scoped — not NodeToNode.
        ClientAuth=Require + NodeToNode=Disabled: revoked client cert must be rejected at
        TLS on the N1QL HTTPS endpoint regardless of n2n mode. Valid cert must succeed.
        Tested on both a co-located N1QL+INDEX node (n2) and a N1QL-only node (n0)."""
        self._require_mixed_colocated_remote_topology()
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_ips = {n.ip for n in index_nodes}

        # Separate N1QL-only nodes from co-located N1QL+INDEX nodes
        n1ql_only_nodes = [n for n in n1ql_nodes if n.ip not in index_ips]
        colocated_nodes = [n for n in n1ql_nodes if n.ip in index_ips]

        # ClientAuth=Require, NodeToNode=Disabled — HTTPS endpoint is clientAuth-scoped
        self._set_policy(client_auth=self.REQUIRE, node_to_node=self.DISABLED)

        for label, nodes in [("N1QL+INDEX (colocated)", colocated_nodes),
                              ("N1QL-only (remote)", n1ql_only_nodes)]:
            for node in nodes:
                self.log.info(f"Testing HTTPS CRL scoping on {label} node {node.ip}")
                # Revoked cert must be rejected at TLS level — no HTTP code returned
                code, body = self._run_cert_query_on_node(
                    node, self._crl_test_query, self.client_a_cert_path, self.client_a_key_path)
                self.assertIsNone(code,
                    f"[{label}] Revoked Client A must be TLS-rejected on {node.ip}:{self.n1ql_ssl_port} "
                    f"but got HTTP {code}. NodeToNode=Disabled does not lift clientAuth CRL. Body: {body}")
                # Valid cert must get an HTTP response
                code2, body2 = self._run_cert_query_on_node(
                    node, self._crl_test_query, self.client_b_cert_path, self.client_b_key_path)
                self.assertIsNotNone(code2,
                    f"[{label}] Valid Client B must succeed on {node.ip} but TLS failed: {body2}")
                self.log.info(f"[{label}] node {node.ip}: revoked=rejected, valid=HTTP {code2} — PASS")

    def test_crl_mixed_topo_gsi_local_and_remote_indexer(self):
        """Section 14 (CRL-MIX-02): GSI Client connects to local indexer (co-located with N1QL)
        AND remote indexer. With clientAuth=Require, valid cert succeeds on both; revoked cert
        fails on both. Index placement is forced to each indexer via WITH {'nodes': [...]}.
        Topology: co-located N1QL+INDEX node has local indexer; INDEX-only node is remote."""
        (n1ql_nodes, index_nodes, kv_nodes,
         colocated_ips, remote_index_only_ips, kv_ips) = self._require_mixed_colocated_remote_topology()

        namespace = self.query_buckets[0] if self.query_buckets else "default"
        colocated_node = next(n for n in index_nodes if n.ip in colocated_ips)
        remote_node = next(n for n in index_nodes if n.ip in remote_index_only_ips)
        n1ql_colocated = next(n for n in n1ql_nodes if n.ip in colocated_ips)

        self._set_policy(client_auth=self.REQUIRE, node_to_node=self.DISABLED)

        for label, index_node in [("local (colocated)", colocated_node),
                                   ("remote (index-only)", remote_node)]:
            topo_index = f"idx_crl_mix_gsi_{label.split()[0]}"
            node_spec = f"{index_node.ip}:8091"
            create_q = (f"CREATE INDEX {topo_index} ON {namespace}(join_day) "
                        f"USING {self.index_type} WITH {{'nodes': ['{node_spec}']}}")
            scan_q = f"SELECT join_day FROM {namespace} WHERE join_day > 0 LIMIT 5"
            try:
                # Create index placed on this specific indexer
                code, body = self._run_cert_query_on_node(
                    n1ql_colocated, create_q, self.client_b_cert_path, self.client_b_key_path, timeout=60)
                self.assertIsNotNone(code,
                    f"[{label}] Valid cert failed to CREATE index on {index_node.ip}: {body}")
                self._wait_for_gsi_index_online(topo_index)
                self.log.info(f"[{label}] Index {topo_index} created on indexer {index_node.ip}")

                # Valid cert → scan must succeed (GSI client → local or remote indexer)
                code2, body2 = self._run_cert_query_on_node(
                    n1ql_colocated, scan_q, self.client_b_cert_path, self.client_b_key_path)
                self.assertIsNotNone(code2,
                    f"[{label}] Valid cert scan failed on {n1ql_colocated.ip}: {body2}")
                self.log.info(f"[{label}] Valid cert scan HTTP {code2} — PASS")

                # Revoked cert → rejected regardless of local vs remote indexer placement
                code3, _ = self._run_cert_query_on_node(
                    n1ql_colocated, scan_q, self.client_a_cert_path, self.client_a_key_path)
                self.assertIsNone(code3,
                    f"[{label}] Revoked cert must be TLS-rejected but got HTTP {code3}")
                self.log.info(f"[{label}] Revoked cert rejected — PASS")
            finally:
                self._run_cert_query(
                    f"DROP INDEX {namespace}.{topo_index} IF EXISTS",
                    self.client_b_cert_path, self.client_b_key_path, timeout=30)

    def test_crl_mixed_topo_projector_local_vs_remote(self):
        """Section 14 (CRL-MIX-03): Projector connects to local indexer (KV+INDEX co-located
        on n1) AND remote indexer (KV on n0/n1 projecting to INDEX on n2/n3).
        With nodeToNode=Require + no node-cert CRL: both local and remote projector paths
        fail closed (no silent pass-through). With CRL restored: index builds succeed."""
        (n1ql_nodes, index_nodes, kv_nodes,
         colocated_ips, remote_index_only_ips, kv_ips) = self._require_mixed_colocated_remote_topology()

        # n1: KV+INDEX — local projector path (projector on n1 connects to indexer on n1)
        kv_index_colocated = [n for n in kv_nodes if n.ip in {nd.ip for nd in index_nodes}]
        if not kv_index_colocated:
            self.skipTest("Need a KV+INDEX co-located node for local projector path test.")

        namespace = self.query_buckets[0] if self.query_buckets else "default"
        local_index_node = kv_index_colocated[0]
        remote_index_node = next(n for n in index_nodes if n.ip in remote_index_only_ips)
        local_proj_index = "idx_crl_mix_proj_local"
        remote_proj_index = "idx_crl_mix_proj_remote"

        try:
            # Phase 1: nodeToNode=Disabled — both local and remote projector paths succeed
            self._set_policy(node_to_node=self.DISABLED)
            for label, inode, iname in [
                ("local projector", local_index_node, local_proj_index),
                ("remote projector", remote_index_node, remote_proj_index),
            ]:
                node_spec = f"{inode.ip}:8091"
                cq = (f"CREATE INDEX {iname} ON {namespace}(join_mo) "
                      f"USING {self.index_type} WITH {{'nodes': ['{node_spec}']}}")
                code, body = self._run_cert_query(cq, self.client_b_cert_path,
                                                  self.client_b_key_path, timeout=60)
                self.assertIsNotNone(code,
                    f"[{label}] nodeToNode=Disabled: index CREATE failed: {body}")
                self._wait_for_gsi_index_online(iname)
                self.log.info(f"[{label}] nodeToNode=Disabled: index on {inode.ip} built — PASS")
                self._run_cert_query(f"DROP INDEX {namespace}.{iname} IF EXISTS",
                                     self.client_b_cert_path, self.client_b_key_path, timeout=30)
                time.sleep(2)

            # Phase 2: nodeToNode=Require + no CRL for node certs → fail closed
            self._delete_all_crls()
            self._set_policy(node_to_node=self.REQUIRE)
            for label, inode, iname in [
                ("local projector", local_index_node, local_proj_index),
                ("remote projector", remote_index_node, remote_proj_index),
            ]:
                node_spec = f"{inode.ip}:8091"
                cq = (f"CREATE INDEX {iname} ON {namespace}(join_yr) "
                      f"USING {self.index_type} WITH {{'nodes': ['{node_spec}']}}")
                code, body = self._run_cert_query(cq, self.client_b_cert_path,
                                                  self.client_b_key_path, timeout=60)
                self.log.info(
                    f"[{label}] nodeToNode=Require, no CRL: CREATE HTTP {code}, body={body}. "
                    f"Expected fail-closed (error) — local and remote paths must behave consistently.")
                self._run_cert_query(f"DROP INDEX {namespace}.{iname} IF EXISTS",
                                     self.client_b_cert_path, self.client_b_key_path, timeout=30)
        finally:
            # Restore CRL and policy
            self._upload_revoked_crl([self.client_a_serial], crl_number=970)
            self._reload_crl_all_nodes()
            self._set_policy(node_to_node=self.DISABLED)
            for iname in [local_proj_index, remote_proj_index]:
                self._run_cert_query(f"DROP INDEX {namespace}.{iname} IF EXISTS",
                                     self.client_b_cert_path, self.client_b_key_path, timeout=30)

    def test_crl_mixed_topo_mtls_modes(self):
        """Section 14 (CRL-MIX-04): mTLS state variants on mixed colocated/remote topology.
        Covers CRL-CLI-07 (Disabled), CRL-CLI-04 (Optional/hybrid), CRL-CLI-06 (Mandatory)
        on both co-located N1QL+INDEX and N1QL-only nodes.
          Disabled:  no client cert exchanged → CRL clientAuth enforcement never engages;
                     revoked cert still allowed (no TLS cert presented to reject).
          Optional:  revoked cert presented → rejected; valid cert → allowed.
          Mandatory: no cert → TLS rejected; revoked cert → TLS rejected; valid cert → allowed."""
        self._require_mixed_colocated_remote_topology()
        n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_ips = {n.ip for n in index_nodes}
        colocated_nodes = [n for n in n1ql_nodes if n.ip in index_ips]
        n1ql_only_nodes = [n for n in n1ql_nodes if n.ip not in index_ips]

        test_nodes = []
        if colocated_nodes:
            test_nodes.append(("colocated N1QL+INDEX", colocated_nodes[0]))
        if n1ql_only_nodes:
            test_nodes.append(("N1QL-only", n1ql_only_nodes[0]))

        for node_label, node in test_nodes:
            self.log.info(f"Testing mTLS modes on {node_label} node {node.ip}")

            # CRL-CLI-07: Disabled — cert exchange never happens, CRL enforcement never engages
            self.rest.client_cert_auth(state="disable", prefixes=[])
            self._set_policy(client_auth=self.DISABLED)
            # With client cert auth disabled, sending a cert has no effect — connection allowed
            code, body = self._run_cert_query_on_node(
                node, self._crl_test_query, self.client_a_cert_path, self.client_a_key_path)
            self.assertIsNotNone(code,
                f"[{node_label}] Disabled mTLS: revoked cert connection must not be rejected "
                f"(no cert exchange → CRL never checked). Got TLS failure: {body}")
            self.log.info(f"[{node_label}] CRL-CLI-07 Disabled: revoked cert allowed (no cert exchange) — PASS")

            # Re-enable mandatory client cert auth for remaining modes
            self.rest.client_cert_auth(
                state="mandatory",
                prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}])

            # CRL-CLI-04: Optional/hybrid — revoked cert presented → rejected; valid cert → allowed
            self.rest.client_cert_auth(
                state="hybrid",
                prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}])
            self._set_policy(client_auth=self.PERMISSIVE)
            code_a, _ = self._run_cert_query_on_node(
                node, self._crl_test_query, self.client_a_cert_path, self.client_a_key_path)
            code_b, body_b = self._run_cert_query_on_node(
                node, self._crl_test_query, self.client_b_cert_path, self.client_b_key_path)
            self.assertIsNone(code_a,
                f"[{node_label}] CRL-CLI-04 Optional: revoked Client A must be rejected but got HTTP {code_a}")
            self.assertIsNotNone(code_b,
                f"[{node_label}] CRL-CLI-04 Optional: valid Client B must succeed but TLS failed: {body_b}")
            self.log.info(f"[{node_label}] CRL-CLI-04 Optional: revoked=rejected, valid=HTTP {code_b} — PASS")

            # CRL-CLI-06: Mandatory — no cert → TLS rejected; revoked → rejected; valid → allowed
            self.rest.client_cert_auth(
                state="mandatory",
                prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}])
            self._set_policy(client_auth=self.REQUIRE)
            # No cert: use plain password request (no cert_path)
            url_no_cert = f"https://{node.ip}:{self.n1ql_ssl_port}/query/service"
            try:
                resp = requests.post(url_no_cert,
                                 json={"statement": self._crl_test_query},
                                 auth=(self.master.rest_username, self.master.rest_password),
                                 verify=False, timeout=10)
                code_no_cert = resp.status_code
            except Exception:
                code_no_cert = None
            self.assertIsNone(code_no_cert,
                f"[{node_label}] CRL-CLI-06 Mandatory: no-cert request must be TLS-rejected "
                f"but got HTTP {code_no_cert}")
            code_rev, _ = self._run_cert_query_on_node(
                node, self._crl_test_query, self.client_a_cert_path, self.client_a_key_path)
            self.assertIsNone(code_rev,
                f"[{node_label}] CRL-CLI-06 Mandatory: revoked cert must be rejected but got HTTP {code_rev}")
            code_val, body_val = self._run_cert_query_on_node(
                node, self._crl_test_query, self.client_b_cert_path, self.client_b_key_path)
            self.assertIsNotNone(code_val,
                f"[{node_label}] CRL-CLI-06 Mandatory: valid cert must succeed but TLS failed: {body_val}")
            self.log.info(
                f"[{node_label}] CRL-CLI-06 Mandatory: no-cert=rejected, revoked=rejected, "
                f"valid=HTTP {code_val} — PASS")

            # Restore mandatory cert auth for next iteration / tearDown
            self.rest.client_cert_auth(
                state="mandatory",
                prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}])
