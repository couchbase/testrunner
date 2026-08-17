"""Base class for FTS CRL (Certificate Revocation List) test automation."""

import base64
import collections
import copy
import datetime
import json
import os
import re
import tempfile
import threading
import time
import uuid

import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization

from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from pytests.security.crl_utils import CRLUtils
from pytests.security.rbac_base import RbacBase
from pytests.security.x509_multiple_CA_util import x509main

from .fts_base import FTSBaseTest, NodeHelper

try:  # keep `verify=False` from flooding test output
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except Exception:
    pass

OUTCOME_ALLOWED = "allowed"
OUTCOME_REJECTED_TLS = "rejected_tls"        # TLS alert — the expected path
OUTCOME_REJECTED_AUTH = "rejected_auth"      # HTTP 401 — PRD Phase-1 fallback
OUTCOME_REJECTED_RBAC = "rejected_rbac"      # HTTP 403 — must NOT be revocation
OUTCOME_UNREACHABLE = "unreachable"          # infra fault — must NOT be revocation
OUTCOME_OTHER = "other"

REVOCATION_OUTCOMES = frozenset([OUTCOME_REJECTED_TLS, OUTCOME_REJECTED_AUTH])

# A revoked certificate is refused during the TLS handshake, and the peer's
# alert text is what distinguishes that from the network simply being broken.
# Without this, a connection reset from flaky infrastructure would be recorded
# as "revocation enforced" and the suite's central assertion would pass for the
# wrong reason.
TLS_REJECTION_MARKERS = (
    "certificate revoked",
    "certificate_revoked",
    "sslv3 alert",
    "tlsv1 alert",
    "alert certificate",
    "alert unknown ca",
    "alert handshake failure",
    "handshake failure",
    "bad certificate",
    "certificate unknown",
    "unknown ca",
    "decrypt error",
    "peer did not return a certificate",
    "certificate required",
    "no certificate returned",
)

# Transport faults that say nothing about certificates.
CONNECTIVITY_MARKERS = (
    "connection refused",
    "connection aborted",
    "name or service not known",
    "temporary failure in name resolution",
    "no route to host",
    "network is unreachable",
    "timed out",
    "max retries exceeded",
)

IDENTITY_CN_PREFIX = "ftscrl"
DEFAULT_CRL_FILE = "fts_crl.pem"

DIAG_EVAL_BLOCKED = "API is accessible from localhost only"

SENSITIVE_LOG_PATTERNS = [
    "BEGIN CERTIFICATE",
    "BEGIN RSA PRIVATE KEY",
    "BEGIN PRIVATE KEY",
    "BEGIN EC PRIVATE KEY",
    "BEGIN X509 CRL",
]


Enforcement = collections.namedtuple(
    "Enforcement", ["index", "valid", "revoked", "query"])


class ClientIdentity(object):
    """A client certificate plus the RBAC user its CN maps to."""

    def __init__(self, cn, cert, key, serial, cert_path, key_path, ca_path,
                 username, password, ca_cert=None, ca_key=None):
        self.cn = cn
        self.cert = cert
        self.key = key
        self.serial = serial
        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_path = ca_path
        self.username = username
        self.password = password
        self.ca_cert = ca_cert
        self.ca_key = ca_key

    @property
    def cert_tuple(self):
        return self.cert_path, self.key_path

    def __repr__(self):
        return "<ClientIdentity cn={0} serial={1}>".format(self.cn, self.serial)


class FTSCallResult(object):
    """Classified result of one FTS request."""

    def __init__(self, outcome, status_code=None, content=None, exception=None,
                 url=None):
        self.outcome = outcome
        self.status_code = status_code
        self.content = content
        self.exception = exception
        self.url = url

    @property
    def allowed(self):
        return self.outcome == OUTCOME_ALLOWED

    @property
    def revoked(self):
        return self.outcome in REVOCATION_OUTCOMES

    def total_hits(self):
        if isinstance(self.content, dict):
            return self.content.get("total_hits")
        return None

    def error_text(self):
        if isinstance(self.content, dict):
            return str(self.content.get("error") or self.content)
        if self.exception is not None:
            return str(self.exception)
        return str(self.content)

    def __repr__(self):
        return "<FTSCallResult {0} http={1} url={2}>".format(
            self.outcome, self.status_code, self.url)


class FTSCRLBase(FTSBaseTest):
    """Base class for FTS CRL tests."""

    def setUp(self):
        super(FTSCRLBase, self).setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.master)

        self.crl_client_auth_mode = self._input.param("crl_client_auth", "Disabled")
        self.crl_node_to_node_mode = self._input.param("crl_node_to_node", "Disabled")
        self.client_cert_state = self._input.param("client_cert_state", "enable")
        self.crl_dir_poll_interval_ms = self._input.param(
            "crl_dir_poll_interval_ms", 1000)
        self.crl_enforcement_timeout = self._input.param("crl_enforcement_timeout", 60)

        self._require_crl_supported()

        self._crl_files = []
        self._rbac_users = []
        self._identities = []
        self._allow_expired_crls_set = False

        self.fts_nodes = self._cb_cluster.get_fts_nodes()
        if not self.fts_nodes:
            self.fail("FTS CRL tests need at least one node running the fts service.")

        self._prepare_nodes_for_crl_tests()

        self._run_id = uuid.uuid4().hex[:8]
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca(
            self.ca_cn("FTSCRLTestCA"))
        self.trust_ca_on_all_nodes(self.ca_cert)
        self.ca_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(self.ca_cert), suffix="-ca.pem")

        self.set_crl_settings({"dirPollIntervalMs": self.crl_dir_poll_interval_ms})

        self.log.info("FTS CRL setUp complete: fts_nodes={0}".format(
            [node.ip for node in self.fts_nodes]))

    def tearDown(self):
        try:
            for step, action in (
                ("clientCertAuth disable", self._disable_client_cert_auth),
                ("CRL settings reset", self._reset_crl_settings),
                ("CRL file cleanup", self._cleanup_crl_files),
                ("allow_expired_crls reset", self._reset_allow_expired_crls),
                ("RBAC user cleanup", self._cleanup_rbac_users),
                ("trusted CA cleanup", self._cleanup_trusted_cas),
            ):
                try:
                    action()
                except Exception as exc:
                    self.log.warning("{0} error: {1}".format(step, exc))
        finally:
            super(FTSCRLBase, self).tearDown()

    def _require_crl_supported(self):
        """Fail fast unless the cluster can run CRL tests."""
        if not self.rest.is_enterprise_edition():
            self.fail("CRL support requires an Enterprise Edition cluster.")

        status, content, _ = self.rest.get_crl_settings()
        if not status:
            self.fail(
                "GET /settings/crl unavailable — CRL tests need a Totoro "
                "(8.1.0+) EE cluster. Response: {0}".format(
                    self.crl_utils.parse_content(content)))

    def _prepare_nodes_for_crl_tests(self, servers=None):
        """Enable the node-local endpoints this suite depends on."""
        servers = servers or self._input.servers
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                try:
                    output, _ = shell.enable_diag_eval_on_non_local_hosts()
                    self.log.info("Enabled non-local diag/eval on {0}: "
                                  "{1}".format(server.ip, str(output)[:120]))
                except Exception as exc:
                    self.log.warning(
                        "Could not enable non-local diag/eval on {0}: {1}. "
                        "Tests needing allow_expired_crls or fts.log access may "
                        "fail.".format(server.ip, exc))
                try:
                    shell.non_local_CA_upload(allow=True)
                    self.log.info("Enabled allowNonLocalCACertUpload on "
                                  "{0}".format(server.ip))
                except Exception as exc:
                    self.log.warning(
                        "Could not enable allowNonLocalCACertUpload on {0}: "
                        "{1}".format(server.ip, exc))
            finally:
                shell.disconnect()

    def ca_cn(self, label):
        """A CA common name unique to this test.

        CRLs are matched to certificates by *issuer name*. With a constant CN, a
        CRL left behind by an earlier test is matched against this test's CA —
        same name, different key — so the signature check fails and, under
        Require, everything downstream fails closed. That is indistinguishable
        from a product bug, so every CA this suite mints carries a unique CN.
        """
        return "{0}_{1}".format(label, self._run_id)

    @staticmethod
    def ca_filename(ca_cert):
        """On-node PEM filename derived from the CA's (unique) common name."""
        attrs = ca_cert.subject.get_attributes_for_oid(
            x509.oid.NameOID.COMMON_NAME)
        cn = attrs[0].value if attrs else "ca"
        return "fts_crl_{0}.pem".format(re.sub(r"[^A-Za-z0-9_.-]", "_", cn))

    def trust_ca_on_all_nodes(self, ca_cert, servers=None, filename=None):
        """Trust `ca_cert` on every node."""
        servers = servers or self._input.servers
        filename = filename or self.ca_filename(ca_cert)
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)
        for server in servers:
            ca_dir = self._place_ca_on_node(server, pem_bytes, filename)
            status, content = RestConnection(server).load_trusted_CAs()
            if not status:
                self._fail_ca_load(server, ca_dir, content)
        self._verify_ca_trusted(ca_cert, servers[0])
        self.log.info("Trusted CA {0} on {1} node(s)".format(
            filename, len(servers)))

    def _fail_ca_load(self, server, ca_dir, content):
        """Fail a loadTrustedCAs error, naming both paths involved."""
        message = str(content)
        match = re.search(r"from\s+(/\S+?)[\.\"']", message)
        server_dir = match.group(1) if match else None

        detail = ""
        if server_dir and server_dir.rstrip("/") != ca_dir.rstrip("/"):
            detail = (
                " PATH MISMATCH: the CA was written to {0} but ns_server read "
                "{1}. The datadir resolution is wrong — note that the bucket "
                "data path (/nodes/self storage) is NOT the config datadir "
                "ns_server uses for inbox.".format(ca_dir, server_dir)
            )
        elif server_dir:
            detail = (
                " Paths agree ({0}), so this is not a path-resolution problem — "
                "check that the file is readable by the couchbase user and is a "
                "valid PEM.".format(server_dir)
            )
        self.fail("Failed to load trusted CAs on {0}: {1}.{2}".format(
            server.ip, message, detail))

    def resolve_config_datadir(self, server):
        """Return ns_server's config datadir — the parent of `inbox`.

        NOT RestConnection.get_data_path(), which is the *bucket* data dir
        (<datadir>/data) and puts the CA where ns_server never reads it.
        """
        from_eval = self._datadir_via_diag_eval(server)
        from_settings = self._datadir_via_crl_settings()

        if from_eval and from_settings and from_eval != from_settings:
            self.log.warning(
                "Config datadir disagreement on {0}: diag/eval says {1!r}, "
                "CRL settings imply {2!r}. Using diag/eval.".format(
                    server.ip, from_eval, from_settings))

        datadir = from_eval or from_settings
        if not datadir:
            self.fail(
                "Could not resolve ns_server's config datadir on {0}. Tried "
                "diag/eval path_config_datadir (restricted to localhost?) and "
                "the 'directory' field of /settings/crl. CRL tests need it to "
                "place CA files where ns_server will read them.".format(
                    server.ip))
        self.log.info("Config datadir on {0}: {1}".format(server.ip, datadir))
        return datadir

    def _datadir_via_diag_eval(self, server):
        try:
            _, raw = RestConnection(server).diag_eval(
                "filename:absname(element(2, application:get_env("
                "ns_server,path_config_datadir))).")
        except Exception as exc:
            self.log.warning("diag/eval datadir lookup failed on {0}: "
                             "{1}".format(server.ip, exc))
            return None
        value = str(raw or "").strip().strip('"')
        if DIAG_EVAL_BLOCKED in value:
            self.log.warning(
                "diag/eval is restricted to localhost on {0}; falling back to "
                "the CRL settings directory for the datadir.".format(server.ip))
            return None
        if not value.startswith("/"):
            self.log.warning("diag/eval returned a non-absolute datadir on "
                             "{0}: {1!r}".format(server.ip, value))
            return None
        return value.rstrip("/")

    def _datadir_via_crl_settings(self):
        """Derive the datadir from the CRL directory ns_server reports."""
        suffix = "/{0}/crls".format(x509main.CHAINFILEPATH)
        try:
            directory = str(self.get_crl_settings().get("directory") or "").strip()
        except Exception as exc:
            self.log.warning("Could not read CRL settings for the datadir: "
                             "{0}".format(exc))
            return None
        directory = directory.rstrip("/")
        if directory.startswith("/") and directory.endswith(suffix):
            return directory[: -len(suffix)]
        self.log.warning(
            "CRL settings directory {0!r} does not match the expected "
            "<datadir>{1} shape; cannot derive the datadir from it.".format(
                directory, suffix))
        return None

    def _place_ca_on_node(self, server, pem_bytes, filename):
        """Write the CA PEM into the node's real inbox/CA directory."""
        datadir = self.resolve_config_datadir(server)
        inbox_dir = "{0}/{1}".format(datadir, x509main.CHAINFILEPATH)
        ca_dir = "{0}/{1}".format(inbox_dir, x509main.TRUSTEDCAPATH)

        self._copy_file_to_node_dir(
            server, pem_bytes, ca_dir, filename,
            make_dir=True, chown_dir=inbox_dir)
        return ca_dir

    def _copy_file_to_node_dir(self, server, content_bytes, dest_dir, filename,
                               make_dir=False, chown_dir=None):
        """Copy bytes to `dest_dir/filename` on `server`, verifying it landed.

        copy_file_local_to_remote() logs sftp failures instead of raising, so
        an unwritable destination is otherwise invisible until much later.
        """
        local_path = self._write_temp_pem(content_bytes, suffix="-upload.pem")
        dest_path = "{0}/{1}".format(dest_dir.rstrip("/"), filename)
        shell = RemoteMachineShellConnection(server)
        try:
            if make_dir:
                shell.execute_command("mkdir -p {0}".format(dest_dir))
            shell.copy_file_local_to_remote(local_path, dest_path)
            if chown_dir:
                shell.execute_command("chown -R couchbase {0}".format(chown_dir))

            out, err = shell.execute_command("ls -l {0}".format(dest_dir))
            listing = "\n".join(out) if isinstance(out, list) else str(out or "")
            if filename not in listing:
                self.fail(
                    "File {0} was not written to {1} on {2}. "
                    "copy_file_local_to_remote() swallows sftp errors, so the "
                    "likely cause is that the SSH user cannot write to that "
                    "path, or the directory does not exist. "
                    "Directory listing:\n{3}\nstderr: {4}".format(
                        filename, dest_dir, server.ip, listing or "(empty)", err))
            self.log.info("Placed {0}:{1}".format(server.ip, dest_path))
            return dest_path
        finally:
            shell.disconnect()

    def place_crl_in_local_dir(self, pem_bytes, filename="fts_crl_localdir.pem",
                               servers=None, reload_now=True):
        """Drop a CRL straight into the node's configured CRL directory."""
        servers = servers or self._input.servers
        settings = self.get_crl_settings()
        crl_dir = str(settings.get("directory") or "").strip()
        if not crl_dir.startswith("/"):
            self.fail(
                "CRL settings 'directory' is not an absolute path ({0!r}); "
                "cannot place a CRL locally. Full settings: {1}".format(
                    crl_dir, settings))

        for server in servers:
            self._copy_file_to_node_dir(
                server, pem_bytes, crl_dir, filename,
                make_dir=True, chown_dir=crl_dir)
        if reload_now:
            self.reload_crl_all_nodes()
        return crl_dir

    def cbft_pids(self):
        """cbft PID per FTS node, for no-restart assertions."""
        pids = {}
        for node in self.fts_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                out, _ = shell.execute_command("pgrep -f cbft | head -1")
                text = (out[0] if isinstance(out, list) and out
                        else (out or "")).strip()
                pids[node.ip] = text or None
            except Exception as exc:
                self.log.warning("Could not read cbft pid on {0}: {1}".format(
                    node.ip, exc))
                pids[node.ip] = None
            finally:
                shell.disconnect()
        return pids

    def assert_cbft_not_restarted(self, pids_before, context=""):
        """Fail if cbft's PID changed since `pids_before`."""
        pids_after = self.cbft_pids()
        if pids_before and pids_after and pids_before != pids_after:
            self.fail("{0}: cbft restarted — before={1} after={2}".format(
                context or "cbft restart check", pids_before, pids_after))
        self.log.info("{0}: cbft PIDs unchanged {1}".format(
            context or "cbft restart check", pids_after))
        return pids_after

    def restart_cbft(self, node=None):
        """Kill cbft; the babysitter restarts it."""
        node = node or self.fts_nodes[0]
        self.log.info("Killing cbft on {0}".format(node.ip))
        NodeHelper.kill_cbft_process(node)
        return node

    def restart_couchbase_on_node(self, node=None, wait_time=180):
        """Full couchbase-server restart on one node, then wait for it up."""
        node = node or self.fts_nodes[0]
        self.log.info("Restarting couchbase-server on {0}".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        try:
            shell.restart_couchbase()
        finally:
            shell.disconnect()
        NodeHelper.wait_service_started(node, wait_time=wait_time)
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self)
        return node

    def reboot_node(self, node=None):
        """Reboot a node and wait for couchbase to come back."""
        node = node or self.fts_nodes[0]
        self.log.info("Rebooting {0}".format(node.ip))
        NodeHelper.reboot_server(node, self)
        return node

    def wait_for_fts_ready(self, identity, index, query=None, timeout=300,
                           interval=5, context="fts ready"):
        """Poll until `identity` can query again — i.e. cbft is serving."""
        query = query if query is not None else self.default_query()
        return self.wait_until_allowed(
            lambda ident: self.fts_query(ident, index, query), identity,
            timeout=timeout, interval=interval, context=context)

    def assert_never_allowed_until(self, revoked_identity, valid_identity, index,
                                   query=None, timeout=300, interval=2,
                                   context="startup window"):
        """Assert a revoked identity is never allowed while the service returns."""
        query = query if query is not None else self.default_query()
        deadline = time.time() + timeout
        attempts = 0
        while time.time() < deadline:
            attempts += 1
            revoked_result = self.fts_query(revoked_identity, index, query)
            if revoked_result.allowed:
                self.fail(
                    "{0}: revoked identity was ALLOWED during the recovery "
                    "window (attempt {1}) — enforcement must be active before "
                    "cbft serves traffic. {2}".format(
                        context, attempts, revoked_result))
            valid_result = self.fts_query(valid_identity, index, query)
            if valid_result.allowed:
                self.log.info(
                    "{0}: service serving again after {1} probe(s); revoked "
                    "identity never allowed (last outcome {2})".format(
                        context, attempts, revoked_result.outcome))
                return valid_result
            time.sleep(interval)
        self.fail("{0}: service did not resume serving within {1}s "
                  "({2} probes)".format(context, timeout, attempts))

    def _verify_ca_trusted(self, ca_cert, server):
        """Confirm the cluster actually trusts the CA we just loaded."""
        common_names = [
            attribute.value for attribute in
            ca_cert.subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)
        ]
        if not common_names:
            return
        expected = common_names[0]
        try:
            trusted = RestConnection(server).get_trusted_CAs()
        except Exception as exc:
            self.log.warning(
                "Could not verify trusted CAs on {0}: {1}".format(server.ip, exc))
            return
        subjects = [str(entry.get("subject", "")) for entry in trusted or []]
        if not any(expected in subject for subject in subjects):
            self.fail(
                "CA {0!r} is not in the cluster's trusted CA list after "
                "loadTrustedCAs. Trusted subjects: {1}".format(
                    expected, subjects))
        self.log.info("Verified CA {0!r} is trusted by the cluster".format(
            expected))

    def generate_intermediate_ca(self, root_cert, root_key, cn,
                                 key_algorithm="rsa2048", valid_days=1825):
        """Generate an intermediate CA signed by `root_cert`/`root_key`."""
        key = CRLUtils._generate_private_key(key_algorithm)
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name(
                [x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, cn)]))
            .issuer_name(root_cert.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=valid_days))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0),
                           critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=False, content_commitment=False,
                    key_encipherment=False, data_encipherment=False,
                    key_agreement=False, key_cert_sign=True, crl_sign=True,
                    encipher_only=False, decipher_only=False),
                critical=True)
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                critical=False)
            .sign(root_key, hashes.SHA256())
        )
        return cert, key

    def make_untrusted_ca(self, cn="FTSCRLUntrustedCA"):
        """A CA the cluster does NOT trust — for negative upload cases."""
        return self.crl_utils.generate_ca(self.ca_cn(cn))

    @staticmethod
    def tamper_crl_pem(pem_bytes):
        """Return a structurally valid CRL PEM whose signature is broken."""
        body = b"".join(
            line for line in pem_bytes.splitlines()
            if b"-----" not in line)
        der = bytearray(base64.b64decode(body))
        for offset in range(1, min(9, len(der))):
            der[-offset] ^= 0xFF
        encoded = base64.b64encode(bytes(der))
        lines = [encoded[i:i + 64] for i in range(0, len(encoded), 64)]
        return b"-----BEGIN X509 CRL-----\n" + b"\n".join(lines) + \
               b"\n-----END X509 CRL-----\n"

    def _x509_fixtures(self):
        """x509main instance, or fail if the conf did not ask for it."""
        x509_util = getattr(CbServer, "x509", None)
        if not x509_util:
            self.fail(
                "Node-certificate revocation needs x509main's node certs — "
                "pass multiple_ca=True in the conf so FTSBaseTest generates "
                "them. Couchbase's built-in self-signed node certs cannot be "
                "revoked, since this suite has no access to that CA key.")
        return x509_util

    def _x509_issuer_dir(self, entry):
        """Directory of the intermediate that signed a node/client cert.

        Both x509main maps store {signed_by, path} where path is
        <root_ca_dir>/<something>/, so the issuing intermediate sits beside it
        as <root_ca_dir>/<signed_by>/ holding int.pem and int.key.
        """
        leaf_dir = entry["path"].rstrip("/")
        return os.path.join(os.path.dirname(leaf_dir), entry["signed_by"])

    def _read_x509_file(self, path):
        """Read a file from x509main's cert tree (local slave, else over SSH)."""
        try:
            with open(path, "rb") as handle:
                return handle.read()
        except IOError:
            pass
        shell = RemoteMachineShellConnection(self._x509_fixtures().slave_host)
        try:
            out, err = shell.execute_command("cat {0}".format(path))
            data = "\n".join(out) if isinstance(out, list) else (out or "")
            if not data.strip():
                self.fail("Could not read {0} from the x509 cert tree: "
                          "{1}".format(path, err))
            return data.encode() if isinstance(data, str) else data
        finally:
            shell.disconnect()

    def node_cert_entry(self, node):
        """x509main's node_ca_map entry for `node`."""
        x509_util = self._x509_fixtures()
        entry = x509_util.node_ca_map.get(str(node.ip))
        if not entry:
            self.fail("No node cert for {0} in the x509 manifest; have "
                      "{1}".format(node.ip, list(x509_util.node_ca_map)))
        return entry

    def node_cert_serial(self, node):
        """Serial of `node`'s own TLS certificate."""
        entry = self.node_cert_entry(node)
        leaf = os.path.join(entry["path"].rstrip("/"),
                            "{0}.pem".format(node.ip))
        return x509.load_pem_x509_certificate(
            self._read_x509_file(leaf)).serial_number

    def node_issuer_ca(self, node):
        """(cert, key) of the intermediate that signed `node`'s cert."""
        int_dir = self._x509_issuer_dir(self.node_cert_entry(node))
        cert = x509.load_pem_x509_certificate(
            self._read_x509_file(os.path.join(int_dir, "int.pem")))
        key = serialization.load_pem_private_key(
            self._read_x509_file(os.path.join(int_dir, "int.key")),
            password=None)
        return cert, key

    def publish_node_crls(self, revoked_nodes=(), servers=None, crl_number=1):
        """One CRL per issuing intermediate across the cluster's node certs.

        Under nodeToNode=Require every node cert needs an applicable CRL or the
        cluster fails closed for reasons unrelated to the revocation under
        test. Nodes may be signed by different intermediates, so this groups by
        issuer and uploads one CRL each, revoking only the requested nodes.
        """
        servers = servers or self._input.servers
        revoked_ips = {node.ip for node in revoked_nodes}
        by_issuer = {}
        for server in servers:
            entry = self.node_cert_entry(server)
            bucket = by_issuer.setdefault(
                entry["signed_by"], {"node": server, "serials": []})
            if server.ip in revoked_ips:
                bucket["serials"].append(self.node_cert_serial(server))

        for issuer, info in by_issuer.items():
            cert, key = self.node_issuer_ca(info["node"])
            pem = self.crl_utils.build_crl(
                cert, key, revoked_serials=info["serials"],
                crl_number=crl_number)
            self.upload_crl(
                "fts_crl_node_{0}.pem".format(
                    re.sub(r"[^A-Za-z0-9_.-]", "_", issuer)), pem)
            self.log.info("Node CRL for issuer {0}: revoking {1}".format(
                issuer, info["serials"] or "nothing"))
        self.reload_crl_all_nodes()
        return by_issuer

    def revoke_node_cert(self, node, crl_number=2, servers=None):
        """Revoke `node`'s own TLS certificate cluster-wide."""
        serial = self.node_cert_serial(node)
        self.publish_node_crls(revoked_nodes=[node], servers=servers,
                               crl_number=crl_number)
        self.log.info("Revoked node cert for {0} (serial {1})".format(
            node.ip, hex(serial)))
        return serial

    def restore_node_certs(self, crl_number=3, servers=None):
        """Re-publish node CRLs revoking nothing."""
        return self.publish_node_crls(revoked_nodes=(), servers=servers,
                                      crl_number=crl_number)

    def create_client_identity(self, name, roles="admin", key_algorithm="rsa2048",
                               ca_cert=None, ca_key=None, chain_pem=None):
        """Create a client cert whose CN maps to a freshly created RBAC user."""
        ca_cert = ca_cert if ca_cert is not None else self.ca_cert
        ca_key = ca_key if ca_key is not None else self.ca_key
        cn = "{0}-{1}".format(IDENTITY_CN_PREFIX, name)

        cert, key, serial = self.crl_utils.generate_leaf_cert(
            ca_cert, ca_key, cn, key_algorithm=key_algorithm)
        username, password = self.create_rbac_user(cn, roles)

        cert_pem = self.crl_utils.cert_to_pem(cert)
        if chain_pem:
            cert_pem = cert_pem + chain_pem

        identity = ClientIdentity(
            cn=cn, cert=cert, key=key, serial=serial,
            cert_path=self._write_temp_pem(cert_pem, suffix="-cert.pem"),
            key_path=self._write_temp_pem(
                self.crl_utils.key_to_pem(key), suffix="-key.pem"),
            ca_path=self._write_temp_pem(
                self.crl_utils.cert_to_pem(ca_cert), suffix="-ca.pem"),
            username=username, password=password,
            ca_cert=ca_cert, ca_key=ca_key,
        )
        self._identities.append(identity)
        self.log.info("Created client identity {0}".format(identity))
        return identity

    def create_dual_identities(self, roles="admin", **kwargs):
        """Create the (valid, to-be-revoked) identity pair for plan §0.4."""
        valid = self.create_client_identity("valid", roles=roles, **kwargs)
        revoked = self.create_client_identity("revoked", roles=roles, **kwargs)
        return valid, revoked

    def create_identities(self, count, prefix="bulk", roles="admin", **kwargs):
        """N identities, for the concurrency case (plan FTS-CLI-09)."""
        return [
            self.create_client_identity(
                "{0}{1}".format(prefix, index), roles=roles, **kwargs)
            for index in range(count)
        ]

    def enable_client_cert_auth(self, state=None):
        """Enable client-cert auth with a subject.cn -> username mapping."""
        state = state if state is not None else self.client_cert_state
        prefixes = [{"path": "subject.cn", "prefix": "", "delimiter": ""}]
        status, content = self.rest.client_cert_auth(state=state, prefixes=prefixes)
        if not status:
            self.fail("Failed to set clientCertAuth state={0}: {1}".format(
                state, content))
        self.log.info("clientCertAuth state={0}".format(state))

    def set_crl_policy(self, client_auth=None, node_to_node=None):
        """Set policyPerScope. Partial updates leave the other scope untouched."""
        policy = {}
        if client_auth is not None:
            policy["clientAuth"] = client_auth
        if node_to_node is not None:
            policy["nodeToNode"] = node_to_node
        if not policy:
            return None
        content = self.set_crl_settings({"policyPerScope": policy})
        self.log.info("CRL policyPerScope set to {0}".format(policy))
        return content

    def get_crl_settings(self):
        status, content, _ = self.rest.get_crl_settings()
        parsed = self.crl_utils.parse_content(content)
        if not status:
            self.fail("GET /settings/crl failed: {0}".format(parsed))
        return parsed

    def set_crl_settings(self, settings):
        """POST /settings/crl, returning the full merged config."""
        status, content, _ = self.rest.post_crl_settings(settings)
        parsed = self.crl_utils.parse_content(content)
        if not status:
            self.fail("POST /settings/crl {0} failed: {1}".format(settings, parsed))
        return parsed

    def set_crl_settings_expecting_failure(self, settings):
        """POST /settings/crl expecting a 400. Returns the parsed error body."""
        status, content, _ = self.rest.post_crl_settings(settings)
        parsed = self.crl_utils.parse_content(content)
        if status:
            self.fail("POST /settings/crl {0} unexpectedly succeeded: {1}".format(
                settings, parsed))
        return parsed

    def upload_crl(self, filename, pem_bytes, timeout=300):
        """Upload a CRL and register it for teardown deletion."""
        status, content, _ = self.rest.upload_crl_file(
            filename, pem_bytes, timeout=timeout)
        parsed = self.crl_utils.parse_content(content)
        if not status:
            self.fail("CRL upload of {0} failed: {1}".format(filename, parsed))
        self.track_crl_file(filename)
        return parsed

    def upload_crl_expecting_failure(self, filename, pem_bytes, timeout=120):
        """Upload a CRL expecting rejection. Returns the parsed error body."""
        status, content, _ = self.rest.upload_crl_file(
            filename, pem_bytes, timeout=timeout)
        parsed = self.crl_utils.parse_content(content)
        if status:
            self.track_crl_file(filename)
            self.fail("CRL upload of {0} unexpectedly succeeded: {1}".format(
                filename, parsed))
        self.log.info("CRL upload of {0} rejected as expected: {1}".format(
            filename, parsed))
        return parsed

    def publish_crl(self, serials, filename=DEFAULT_CRL_FILE, crl_number=1,
                    reload_now=True, ca_cert=None, ca_key=None, **crl_kwargs):
        """Build a CRL revoking `serials`, upload it, and reload every node."""
        ca_cert = ca_cert if ca_cert is not None else self.ca_cert
        ca_key = ca_key if ca_key is not None else self.ca_key
        pem = self.crl_utils.build_crl(
            ca_cert, ca_key, revoked_serials=self._to_serials(serials),
            crl_number=crl_number, **crl_kwargs)
        content = self.upload_crl(filename, pem)
        if reload_now:
            self.reload_crl_all_nodes()
        return content

    @staticmethod
    def _to_serials(serials):
        """Normalise int / ClientIdentity / list-of-either into a serial list."""
        if serials is None:
            return []
        if not isinstance(serials, (list, tuple, set)):
            serials = [serials]
        return [
            item.serial if isinstance(item, ClientIdentity) else item
            for item in serials
        ]

    def revoke(self, identities, filename=DEFAULT_CRL_FILE, crl_number=2,
               **kwargs):
        """Revoke identities and make it effective on every node."""
        return self.publish_crl(identities, filename=filename,
                                crl_number=crl_number, **kwargs)

    def reload_crl_all_nodes(self, servers=None):
        """Force an immediate CRL reload on every node."""
        servers = servers or self._input.servers
        results = {}
        for server in servers:
            status, content, _ = RestConnection(server).reload_crl()
            results[server.ip] = self.crl_utils.parse_content(content)
            if not status:
                self.log.warning("reloadCrl failed on {0}: {1}".format(
                    server.ip, results[server.ip]))
        return results

    def track_crl_file(self, filename):
        if filename not in self._crl_files:
            self._crl_files.append(filename)

    def list_crl_files(self):
        status, content, _ = self.rest.get_crl_files()
        parsed = self.crl_utils.parse_content(content)
        if not status:
            self.fail("GET /settings/crl/files failed: {0}".format(parsed))
        return parsed

    def delete_crl(self, filename, reload_now=True):
        status, content, _ = self.rest.delete_crl_file(filename)
        if filename in self._crl_files:
            self._crl_files.remove(filename)
        if status and reload_now:
            self.reload_crl_all_nodes()
        return status, self.crl_utils.parse_content(content)

    def set_allow_expired_crls(self, enabled=True):
        """Toggle the server-side allowance for expired CRLs."""
        value = "true" if enabled else "false"
        status, content = self.rest.diag_eval(
            "ns_config:set(allow_expired_crls, {0}).".format(value))
        if not status or DIAG_EVAL_BLOCKED in str(content):
            self.fail(
                "Failed to set allow_expired_crls={0}: {1}. If the response is "
                "{2!r}, /diag/eval is restricted to localhost on this cluster — "
                "_prepare_nodes_for_crl_tests() tries to lift that at setUp, so "
                "check its warnings.".format(
                    value, content, DIAG_EVAL_BLOCKED))
        self._allow_expired_crls_set = enabled
        self.log.info("allow_expired_crls set to {0}".format(value))

    def diagnostics_status(self, nodes=None, expect_success=True):
        status, content, _ = self.rest.get_diagnostics_status(nodes=nodes)
        parsed = self.crl_utils.parse_content(content)
        if expect_success and not status:
            self.fail("diagnostics/status failed: {0}".format(parsed))
        return status, parsed

    def diagnostics_validate(self, certs=None, policy="Require",
                             expect_success=True):
        """POST /settings/crl/diagnostics/validate."""
        status, content, _ = self.rest.post_diagnostics_validate(
            policy=policy, certs=certs)
        parsed = self.crl_utils.parse_content(content)
        if expect_success and not status:
            self.fail("diagnostics/validate(policy={0}) failed: {1}".format(
                policy, parsed))
        return status, parsed

    @staticmethod
    def node_error(node_entry):
        """Return a node's error string from diagnostics/status, or None."""
        if isinstance(node_entry, dict) and "crlFiles" not in node_entry:
            error = node_entry.get("error")
            if error:
                return str(error)
        return None

    @staticmethod
    def crl_files(node_entry):
        """Per-file status dicts for one node of a diagnostics/status response.

        Live shape is {"crlFiles": [...], "pollDirectory": {...}}; CRL_INFO.md
        §5 documents a bare array. Both are accepted.
        """
        if isinstance(node_entry, dict):
            return node_entry.get("crlFiles") or []
        if isinstance(node_entry, list):  # the shape CRL_INFO.md documents
            return node_entry
        return []

    @staticmethod
    def poll_directory(node_entry):
        """The pollDirectory block for one node, or {} if absent."""
        if isinstance(node_entry, dict):
            return node_entry.get("pollDirectory") or {}
        return {}

    def crl_sources(self, status_body):
        """Distinct `source` values across every node in a status response."""
        sources = set()
        for node_entry in (status_body or {}).values():
            for entry in self.crl_files(node_entry):
                sources.add(entry.get("source"))
        return sources

    @staticmethod
    def diag_results(body):
        """Per-cert result objects from a diagnostics/validate response.

        Live key is "results"; CRL_INFO.md §4/§7 say "statuses". Both work.
        """
        if not isinstance(body, dict):
            return []
        results = body.get("results")
        if results is None:
            results = body.get("statuses")  # the shape CRL_INFO.md documents
        return results or []

    def diag_statuses(self, body):
        return [entry.get("status") for entry in self.diag_results(body)]

    def diag_details(self, body):
        return [str(entry.get("details") or "")
                for entry in self.diag_results(body)]

    def resolve_diagnostics_policy_spelling(self):
        """Return whichever of "Require"/"Required" the server accepts."""
        accepted = []
        for spelling in ("Require", "Required"):
            status, _ = self.diagnostics_validate(
                policy=spelling, expect_success=False)
            self.log.info("diagnostics/validate policy={0!r} accepted={1}".format(
                spelling, status))
            if status:
                accepted.append(spelling)
        if not accepted:
            self.fail("diagnostics/validate rejected both 'Require' and "
                      "'Required' — the policy parameter contract is unknown.")
        return accepted

    def fts_api(self, identity, path, method="GET", body=None, node=None,
                timeout=60, port=None, basic_auth=None, verify_server=False,
                extra_headers=None):
        """Call an FTS endpoint, optionally presenting a client certificate.

        verify=False: the node cert is cluster-CA signed, not ours — passing
        our CA would SSLError on every call and read as revocation.
        """
        node = node or self.fts_nodes[0]
        port = port or CbServer.ssl_fts_port
        url = "https://{0}:{1}{2}".format(node.ip, port, path)

        kwargs = {"timeout": timeout, "verify": False}
        if verify_server and identity is not None:
            kwargs["verify"] = identity.ca_path
        if identity is not None:
            kwargs["cert"] = identity.cert_tuple
        if basic_auth is not None:
            kwargs["auth"] = basic_auth
        headers = dict(extra_headers or {})
        if body is not None:
            kwargs["data"] = json.dumps(body)
            headers.setdefault("Content-Type", "application/json")
        if headers:
            kwargs["headers"] = headers

        try:
            response = requests.request(method, url, **kwargs)
        except requests.exceptions.RequestException as exc:
            return FTSCallResult(self.classify_transport_error(exc),
                                 exception=exc, url=url)

        content = self.crl_utils.parse_content(response.content)
        if response.status_code == 401:
            outcome = OUTCOME_REJECTED_AUTH
        elif response.status_code == 403:
            outcome = OUTCOME_REJECTED_RBAC
        elif response.ok:
            outcome = OUTCOME_ALLOWED
        else:
            outcome = OUTCOME_OTHER
        return FTSCallResult(outcome, status_code=response.status_code,
                             content=content, url=url)

    def index_bucket_name(self, index):
        """Source bucket name for an index, preferring the public attribute.

        `source_bucket` is a Bucket object, so `.name` is the readable form;
        `_source_name` is the private fallback. Note `index.bucket` does not
        exist on FTSIndex — do not reach for it.
        """
        bucket = getattr(index, "source_bucket", None)
        name = getattr(bucket, "name", None)
        return name or getattr(index, "_source_name", None)

    @staticmethod
    def classify_transport_error(exc):
        """Decide whether a transport failure was a cert rejection or infra.

        Only a positively identified TLS alert counts as revocation. An
        SSLError is by definition a TLS-layer failure and, since these requests
        pass verify=False, it can only originate from the peer refusing our
        client certificate. A bare ConnectionError is ambiguous — a revoked-cert
        alert can surface as a reset, but so can a refused port or a dropped
        network — so it only counts as a rejection when the message names a TLS
        alert. Everything else is reported as unreachable and fails loudly
        rather than masquerading as enforcement.
        """
        text = " ".join(filter(None, [str(exc), repr(getattr(exc, "args", ""))])).lower()
        if any(marker in text for marker in TLS_REJECTION_MARKERS):
            return OUTCOME_REJECTED_TLS
        if isinstance(exc, requests.exceptions.SSLError):
            return OUTCOME_REJECTED_TLS
        if any(marker in text for marker in CONNECTIVITY_MARKERS):
            return OUTCOME_UNREACHABLE
        if isinstance(exc, requests.exceptions.ConnectionError):
            # A reset with no TLS evidence. Could be either, so refuse to guess.
            return OUTCOME_UNREACHABLE
        return OUTCOME_UNREACHABLE

    def query_path_for_index(self, index):
        """Build the query path for an index, scoped or flat."""
        scope = getattr(index, "scope", None)
        bucket = self.index_bucket_name(index)
        if scope and scope != "_default" and bucket:
            return "/api/bucket/{0}/scope/{1}/index/{2}/query".format(
                bucket, scope, index.name)
        return "/api/index/{0}/query".format(index.name)

    def fts_query(self, identity, index, query, size=10, node=None,
                  timeout=60, **kwargs):
        """Run a search query as `identity`. Accepts an FTSIndex or a name."""
        if isinstance(index, str):
            path = "/api/index/{0}/query".format(index)
        else:
            path = self.query_path_for_index(index)
        body = {"query": query, "size": size}
        return self.fts_api(identity, path, method="POST", body=body,
                            node=node, timeout=timeout, **kwargs)

    def fts_list_indexes(self, identity, node=None, **kwargs):
        return self.fts_api(identity, "/api/index", node=node, **kwargs)

    def fts_stats(self, identity, node=None, **kwargs):
        return self.fts_api(identity, "/api/nsstats", node=node, **kwargs)

    def fts_index_definition(self, identity, index_name, node=None, **kwargs):
        return self.fts_api(
            identity, "/api/index/{0}".format(index_name), node=node, **kwargs)

    def default_query(self):
        """A query the default test index reliably matches.

        Overridable via conf: every enforcement test leans on this one shape, so
        a schema change would otherwise fail them all with no clear signal.
        """
        return {"match": self._input.param("crl_query_match", "emp"),
                "field": self._input.param("crl_query_field", "type")}

    def query_op(self, index, query=None, **kwargs):
        """Return a one-arg callable suitable for the dual-client helpers."""
        query = query if query is not None else self.default_query()
        return lambda identity: self.fts_query(
            identity, index, query, **kwargs)

    def setup_enforcement(self, mode="Require", revoke=True, node_to_node=None,
                          index_name="fts_crl_index", load=True,
                          client_cert_state=None, baseline_crl=True,
                          crl_number=1, roles="admin"):
        """Build the fixture nearly every enforcement test needs."""
        index = self.create_and_load_test_index(
            index_name=index_name, load=load)
        valid, revoked = self.create_dual_identities(roles=roles)
        self.enable_client_cert_auth(state=client_cert_state)
        if baseline_crl:
            self.publish_crl([], crl_number=crl_number)
        self.set_crl_policy(client_auth=mode, node_to_node=node_to_node)
        if revoke:
            self.revoke(revoked, crl_number=crl_number + 1)
        return Enforcement(index, valid, revoked, self.default_query())

    @staticmethod
    def clone_index_definition(index, new_name):
        """Copy an existing index definition under a new name."""
        definition = copy.deepcopy(index.index_definition)
        definition["name"] = new_name
        definition.pop("uuid", None)
        definition.pop("sourceUUID", None)
        return definition

    def assert_revoked(self, result, context=""):
        """Assert a request was rejected *because of revocation*.

        Accepts a TLS alert or a 401; 403 fails here, since that is FTS RBAC
        answering and means revocation did not precede it (plan FTS-CLI-08).
        """
        label = context or "revoked identity"
        if result.outcome == OUTCOME_REJECTED_RBAC:
            self.fail(
                "{0}: got HTTP 403 (RBAC) instead of a revocation rejection — "
                "revocation must be enforced before identity mapping and "
                "RBAC. {1}".format(label, result))
        if result.outcome == OUTCOME_UNREACHABLE:
            self.fail(
                "{0}: the request failed at the transport layer with no TLS "
                "alert, so this is an infrastructure fault and NOT evidence of "
                "revocation. Treating it as enforcement would pass this test "
                "for the wrong reason. Underlying error: {1}".format(
                    label, result.error_text()[:300]))
        if not result.revoked:
            self.fail("{0}: expected revocation rejection, got {1} "
                      "(content={2})".format(label, result.outcome, result.content))
        self.log.info("{0}: revocation enforced via {1} ({2})".format(
            label, result.outcome, result.error_text()[:160]))
        return result

    def assert_allowed(self, result, expected_hits=None, context=""):
        """Assert a request succeeded — and, for queries, returned real results."""
        label = context or "valid identity"
        if not result.allowed:
            self.fail("{0}: expected success, got {1} (http={2}, content={3})".format(
                label, result.outcome, result.status_code, result.content))
        if expected_hits is not None:
            hits = result.total_hits()
            if hits != expected_hits:
                self.fail("{0}: expected {1} hits, got {2}".format(
                    label, expected_hits, hits))
        self.log.info("{0}: allowed{1}".format(
            label, "" if expected_hits is None else
            " ({0} hits)".format(expected_hits)))
        return result

    def assert_dual_client(self, description, operation, valid, revoked,
                           expect_revoked_rejected=True, expected_hits=None):
        """Drive the Dual-Client Differential Pattern (plan §0.4)."""
        self.log.info("=== dual-client: {0} (expect_revoked_rejected={1}) ===".format(
            description, expect_revoked_rejected))

        valid_result = operation(valid)
        self.assert_allowed(valid_result, expected_hits=expected_hits,
                            context="{0} [Client-Valid]".format(description))

        revoked_result = operation(revoked)
        if expect_revoked_rejected:
            self.assert_revoked(
                revoked_result,
                context="{0} [Client-Revoked]".format(description))
        else:
            self.assert_allowed(
                revoked_result, expected_hits=expected_hits,
                context="{0} [Client-Revoked, policy Disabled]".format(description))
        return valid_result, revoked_result

    def assert_crl_consistent_across_nodes(self, servers=None):
        """Assert every node reports the same CRL files and cache status."""
        servers = servers or self._input.servers
        _, parsed = self.diagnostics_status()

        fingerprints = {}
        for host, node_entry in (parsed or {}).items():
            error = self.node_error(node_entry)
            if error:
                self.fail("Node {0} reported an error in diagnostics/status: "
                          "{1}".format(host, error))
            fingerprints[host] = sorted(
                (entry.get("filename"), entry.get("cacheStatus"))
                for entry in self.crl_files(node_entry))

        distinct = {json.dumps(value) for value in fingerprints.values()}
        if len(distinct) > 1:
            self.fail("CRL state inconsistent across nodes: {0}".format(
                json.dumps(fingerprints, indent=2)))
        self.log.info("CRL state consistent across {0} node(s): {1}".format(
            len(fingerprints), fingerprints))
        return parsed

    def expected_doc_count(self):
        """How many docs the test index should hold.

        FTSIndex.get_src_bucket_doc_count() was observed returning 0 with 1000
        docs indexed; the `items` conf param is deterministic.
        """
        return self._num_items

    def assert_index_complete(self, index, timeout=180,
                              context="index convergence"):
        """Assert the index holds exactly the expected doc count."""
        return self.assert_index_doc_count(
            index, self.expected_doc_count(), timeout=timeout, context=context)

    def assert_index_doc_count(self, index, expected, timeout=180,
                               context="index convergence"):
        """Assert the index converges to exactly `expected` docs."""
        deadline = time.time() + timeout
        actual = None
        while time.time() < deadline:
            actual = index.get_indexed_doc_count()
            if actual == expected:
                self.log.info("{0}: converged to {1} docs".format(context, actual))
                return actual
            time.sleep(5)
        self.fail("{0}: expected {1} indexed docs, got {2} after {3}s".format(
            context, expected, actual, timeout))

    def wait_for_outcome(self, operation, identity, predicate, timeout=None,
                         interval=3, context=""):
        """Poll `operation(identity)` until `predicate(result)` holds."""
        timeout = timeout or self.crl_enforcement_timeout
        deadline = time.time() + timeout
        result = None
        while time.time() < deadline:
            result = operation(identity)
            if predicate(result):
                return result
            time.sleep(interval)
        self.fail("{0}: condition not met within {1}s, last result {2}".format(
            context or "wait_for_outcome", timeout, result))

    def wait_until_revoked(self, operation, identity, **kwargs):
        return self.wait_for_outcome(
            operation, identity, lambda res: res.revoked,
            context=kwargs.pop("context", "wait for revocation"), **kwargs)

    def wait_until_allowed(self, operation, identity, **kwargs):
        return self.wait_for_outcome(
            operation, identity, lambda res: res.allowed,
            context=kwargs.pop("context", "wait for access"), **kwargs)

    def _fts_log_path(self, node):
        """Resolve fts.log per node."""
        log_dir = NodeHelper.get_log_dir(node).strip().strip('"')
        if not log_dir.startswith("/"):
            self.fail(
                "Could not resolve the log directory on {0} (got {1!r}). "
                "/diag/eval is likely restricted to localhost — see the "
                "warnings from _prepare_nodes_for_crl_tests().".format(
                    node.ip, log_dir))
        return "{0}/fts.log".format(log_dir)

    def fts_log_baselines(self):
        """Line counts per FTS node, so later reads only see new output."""
        baselines = {}
        for node in self.fts_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                out, _ = shell.execute_command(
                    "wc -l < {0}".format(self._fts_log_path(node)))
                text = (out[0] if isinstance(out, list) and out else (out or "")).strip()
                baselines[node.ip] = int(text) if str(text).isdigit() else 0
            except Exception as exc:
                self.log.warning("Could not baseline fts.log on {0}: {1}".format(
                    node.ip, exc))
                baselines[node.ip] = 0
            finally:
                shell.disconnect()
        return baselines

    def fts_log_since(self, baselines):
        """Aggregate new fts.log output across FTS nodes since `baselines`."""
        chunks = []
        for node in self.fts_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                out, _ = shell.execute_command(
                    "tail -n +{0} {1}".format(
                        baselines.get(node.ip, 0) + 1, self._fts_log_path(node)))
                chunks.append("\n".join(out) if isinstance(out, list) else (out or ""))
            except Exception as exc:
                self.log.warning("Could not read fts.log on {0}: {1}".format(
                    node.ip, exc))
            finally:
                shell.disconnect()
        return "\n".join(chunks)

    def assert_no_sensitive_material(self, log_text, identities=(), context="fts.log"):
        """Assert logs leak neither PEM blocks nor raw certificate serials."""
        found = [pattern for pattern in SENSITIVE_LOG_PATTERNS
                 if pattern in log_text]
        if found:
            self.fail("{0} contains sensitive material: {1}".format(context, found))

        leaked = []
        for identity in identities:
            for rendering in (str(identity.serial), format(identity.serial, "x"),
                              format(identity.serial, "X")):
                if len(rendering) > 8 and re.search(
                        r"\b{0}\b".format(re.escape(rendering)), log_text):
                    leaked.append((identity.cn, rendering))
        if leaked:
            self.fail("{0} contains raw certificate serials (must be hashed): "
                      "{1}".format(context, leaked))
        self.log.info("{0}: no sensitive material found".format(context))

    def create_and_load_test_index(self, index_name="fts_crl_index",
                                   bucket_name="default", load=True):
        """Create a default FTS index, load docs and wait for indexing."""
        collection_index, index_type, index_scope, index_collections = \
            self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name(bucket_name),
            index_name=index_name,
            collection_index=collection_index, _type=index_type,
            scope=index_scope, collections=index_collections)
        if load:
            self.load_data()
            self.wait_for_indexing_complete()
        return index

    def run_concurrently(self, operation, identities):
        """Run `operation(identity)` for each identity on its own thread."""
        results = {}
        lock = threading.Lock()

        def worker(identity):
            try:
                result = operation(identity)
            except Exception as exc:  # pragma: no cover - defensive
                result = FTSCallResult(OUTCOME_OTHER, exception=exc)
            with lock:
                results[identity] = result

        threads = [threading.Thread(target=worker, args=(identity,))
                   for identity in identities]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results

    def _write_temp_pem(self, pem_bytes, suffix=".pem"):
        """Write PEM bytes to a temp file removed at test end."""
        handle, path = tempfile.mkstemp(suffix=suffix, prefix="ftscrl-")
        with os.fdopen(handle, "wb") as file_obj:
            file_obj.write(pem_bytes)
        self.addCleanup(self._safe_remove, path)
        return path

    @staticmethod
    def _safe_remove(path):
        try:
            os.remove(path)
        except OSError:
            pass

    def create_rbac_user(self, username, roles="admin",
                         password="Couchbase@1234"):
        user = [{"id": username, "password": password, "name": username}]
        RbacBase().create_user_source(user, "builtin", self.master)
        RbacBase().add_user_role(
            [{"id": username, "name": username, "roles": roles}],
            self.rest, "builtin")
        self._rbac_users.append(username)
        return username, password

    def _cleanup_crl_files(self):
        for filename in list(self._crl_files):
            status, _, _ = self.rest.delete_crl_file(filename)
            if not status:
                self.log.warning(
                    "Failed to delete CRL file {0} in teardown".format(filename))
        self._crl_files = []

    def _reset_crl_settings(self):
        self.rest.post_crl_settings(
            {"policyPerScope": {"clientAuth": "Disabled", "nodeToNode": "Disabled"}})

    def _reset_allow_expired_crls(self):
        if self._allow_expired_crls_set:
            self.rest.diag_eval("ns_config:set(allow_expired_crls, false).")
            self._allow_expired_crls_set = False

    def _disable_client_cert_auth(self):
        self.rest.client_cert_auth(state="disable", prefixes=[])

    def _cleanup_trusted_cas(self):
        """Delete the CAs this test added to the cluster's trust store.

        Matched by the unique run id in their CN, so only our own are touched —
        never a node cert's issuer. Left to accumulate, they would pile up
        across a long conf run and give the cluster several same-purpose CAs to
        consider during issuer matching.
        """
        run_id = getattr(self, "_run_id", None)
        if not run_id:
            return
        try:
            trusted = self.rest.get_trusted_CAs()
        except Exception as exc:
            self.log.warning("Could not list trusted CAs: {0}".format(exc))
            return
        for entry in trusted or []:
            subject = str(entry.get("subject", ""))
            if run_id not in subject:
                continue
            ca_id = entry.get("id")
            if ca_id is None:
                continue
            try:
                self.rest.delete_trusted_CA(ca_id)
                self.log.info("Deleted trusted CA {0} ({1})".format(
                    ca_id, subject))
            except Exception as exc:
                self.log.warning("Could not delete trusted CA {0}: {1}".format(
                    ca_id, exc))

    def _cleanup_rbac_users(self):
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self.log.warning("Failed to delete RBAC user {0}: {1}".format(
                    username, exc))
        self._rbac_users = []
