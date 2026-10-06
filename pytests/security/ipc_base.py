import os
import tempfile

import requests

from basetestcase import BaseTestCase
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.crl_utils import CRLUtils
from pytests.security.rbac_base import RbacBase
from pytests.security.x509main import x509main
from TestInput import TestInputSingleton


class IPCBase(BaseTestCase):
    """
    Base class for Internal-identity Password Check (IPC) tests — MB-73874.

    The feature under test: a Couchbase node recognises its own internal client
    certificate purely by a SAN rfc822Name of the form
    <name>@internal.couchbase.com, and historically accepted that certificate as
    proof of identity on its own, granting administrator rights with no password
    and no RBAC evaluation. Any certificate carrying that SAN, signed by ANY CA
    the cluster trusts, was therefore highly privileged — which is the bypass a
    customer demonstrated by adding the reserved SAN email to an unrelated
    certificate that merely shared their corporate root CA.

    The cluster setting 'internalIdentityPasswordCheckUnderMtls' (ns_config key
    internal_identity_password_check_under_mtls, served from /internalSettings)
    removes that: the certificate is still presented and chain-validated as part
    of the TLS connection, but it no longer establishes identity. The request
    must carry its own credentials, and the identity comes from those — exactly
    as if no certificate had been presented.

    The default is NOT stored in config; it is derived at read time from
    cluster_compat_mode:is_cluster_85(), so it reports false on an 8.0 cluster
    and true once compat reaches 8.5. An explicitly set value always wins. This
    base class therefore always reads the current value in setUp and restores it
    in tearDown rather than assuming either state.

    Extends the portable `BaseTestCase` alias, matching CRLBase's convention.
    """

    # The reserved domain ns_server matches on. Deliberately a constant here
    # rather than a literal at each call site — a typo in it would make an
    # "exploit blocked" assertion pass for entirely the wrong reason (the cert
    # simply would not be an internal cert at all).
    INTERNAL_CERT_DOMAIN = "internal.couchbase.com"

    IPC_SETTING = "internalIdentityPasswordCheckUnderMtls"

    MGMT_SSL_PORT = 18091
    KV_SSL_PORT = 11207

    def setUp(self):
        self._self_heal_stuck_client_cert_auth()
        super(IPCBase, self).setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.master)

        self._require_ipc_supported()
        self._self_heal_stuck_trusted_cas()

        # Resources created during a test — cleaned up in tearDown.
        self._rbac_users = []
        self._temp_pem_files = []

        # Remember what the cluster reported before we touched anything, so
        # tearDown can put it back whichever way the test drove it.
        self._original_ipc_state = self.get_ipc_setting()
        self.log.info("{0} at setUp: {1}".format(
            self.IPC_SETTING, self._original_ipc_state))

        # The cluster's own trusted CA for this run. Tests that need to prove
        # the cluster boundary generate a second, unrelated CA themselves.
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca("IPCTestCA")
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        try:
            if hasattr(self, "rest"):
                try:
                    self._restore_ipc_setting()
                except Exception as exc:
                    self.log.warning("{0} restore error: {1}".format(
                        self.IPC_SETTING, exc))
                try:
                    self._disable_client_cert_auth()
                except Exception as exc:
                    self.log.warning(
                        "clientCertAuth disable error: {0}".format(exc))
                try:
                    self._cleanup_rbac_users()
                except Exception as exc:
                    self.log.warning(
                        "RBAC user cleanup error: {0}".format(exc))
                try:
                    self._cleanup_temp_pem_files()
                except Exception as exc:
                    self.log.warning(
                        "Temp PEM file cleanup error: {0}".format(exc))
                try:
                    self._cleanup_trusted_cas()
                except Exception as exc:
                    self.log.warning(
                        "Trusted CA cleanup error: {0}".format(exc))
        finally:
            super(IPCBase, self).tearDown()

    # ── The setting under test ───────────────────────────────────────────────

    def get_ipc_setting(self):
        """
        Current value of internalIdentityPasswordCheckUnderMtls.

        Returns the server's own answer rather than a cached expectation: the
        default is version-derived, not written to config, so GET is the only
        honest source of truth for what the cluster is actually enforcing.
        """
        try:
            return self.rest.get_internalSettings(self.IPC_SETTING)
        except KeyError:
            self.fail(
                "/internalSettings has no '{0}' key — this build predates "
                "MB-73874.".format(self.IPC_SETTING)
            )

    def set_ipc_setting(self, enabled):
        """Enable/disable the check and confirm the cluster agrees it took."""
        status = self.rest.set_internalSetting(self.IPC_SETTING, enabled)
        if not status:
            self.fail("POST /internalSettings {0}={1} failed".format(
                self.IPC_SETTING, enabled))
        actual = self.get_ipc_setting()
        if actual != enabled:
            self.fail(
                "Set {0}={1} but the cluster reports {2} — the setting did "
                "not take effect.".format(self.IPC_SETTING, enabled, actual)
            )
        self.log.info("{0} set to {1}".format(self.IPC_SETTING, enabled))

    def _restore_ipc_setting(self):
        """
        Put the setting back only if a test actually changed it.

        Writing it back unconditionally would be worse than a no-op: an explicit
        write pins the key in config, and from then on the version-derived
        default no longer applies — so a cluster reused by a later test would
        silently stop tracking its own compat version.
        """
        current = self.get_ipc_setting()
        if current != self._original_ipc_state:
            self.log.info("Restoring {0} to {1}".format(
                self.IPC_SETTING, self._original_ipc_state))
            self.set_ipc_setting(self._original_ipc_state)

    # ── Certificate fixtures ─────────────────────────────────────────────────

    def generate_internal_client_cert(self, ca_cert, ca_key, name="internal",
                                      cn=None):
        """
        A client certificate carrying the reserved internal SAN.

        Args:
            ca_cert, ca_key: the issuing CA. Pass the cluster's trusted CA for a
                genuine internal certificate, or an unrelated CA to forge one.
            name: local part of the SAN email, i.e. the '<name>' that ns_server
                maps to the identity '@<name>'.
            cn: subject CN. Defaults to `name`. The whole point of the forged
                case is that this can be anything at all — ns_server keys off
                the SAN, never the subject.

        Returns:
            (cert_path, key_path) — temp PEM files, tracked for teardown.
        """
        cert, key, _serial = self.crl_utils.generate_leaf_cert(
            ca_cert, ca_key, cn or name,
            email_sans=["{0}@{1}".format(name, self.INTERNAL_CERT_DOMAIN)],
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        return cert_path, key_path

    def generate_forged_internal_cert(self, ca_cert=None, ca_key=None,
                                      cn="ads-dashboard.example.com"):
        """
        The customer's bypass, reproduced.

        A leaf sharing nothing with this cluster except a trust anchor — an
        unrelated subject, issued for an unrelated purpose — with the reserved
        internal SAN injected into it. That injection is the entire attack: the
        SAN value is identical in every Couchbase installation and is published
        in the documentation, so obtaining a certificate carrying it requires
        nothing more than asking a trusted CA for one extra SAN entry.

        Defaults to this cluster's trusted CA so the certificate reaches the
        application layer; pass a different CA to test the cluster boundary.
        """
        return self.generate_internal_client_cert(
            ca_cert if ca_cert is not None else self.ca_cert,
            ca_key if ca_key is not None else self.ca_key,
            name="internal", cn=cn,
        )

    # ── mTLS probes ──────────────────────────────────────────────────────────

    def mtls_request(self, cert_path, key_path, path="/pools/default",
                     server=None, port=None, auth=None, timeout=30):
        """
        One HTTPS request presenting `cert_path`/`key_path`.

        Leave `auth` as None to present the certificate ALONE. That is the case
        the whole feature is about, and it only means anything if no
        Authorization header goes with it — a request carrying Basic auth is
        authenticated by password, so it would report success regardless of what
        the certificate did or did not establish.

        Server verification is skipped (ca_cert_path=None): these tests trust a
        CA for the CLIENT certificate only and leave the node presenting its own
        out-of-the-box certificate, which cannot be verified locally.
        """
        server = server or self.master
        return self.crl_utils.perform_mtls_handshake(
            server.ip, port or self.MGMT_SSL_PORT, cert_path, key_path,
            ca_cert_path=None, path=path, timeout=timeout, auth=auth,
        )

    def whoami_via_mtls(self, cert_path, key_path, server=None, auth=None,
                        timeout=30):
        """
        Identity the server resolved for this certificate, via GET /whoami.

        Note /whoami does NOT reject an unidentified caller — it answers 200
        with {"roles": [], "id": "", "domain": "anonymous"}. That is the signal
        to assert on when a certificate is expected to confer no identity.
        Use assert_no_identity_from_cert rather than reading this directly.
        """
        server = server or self.master
        return self.crl_utils.get_identity_via_mtls(
            server.ip, self.MGMT_SSL_PORT, cert_path, key_path,
            ca_cert_path=None, timeout=timeout, auth=auth,
        )

    def assert_no_identity_from_cert(self, cert_path, key_path, server=None,
                                     path="/pools/default"):
        """
        Assert the certificate reaches the application layer and names nobody.

        Three things are checked, and all three matter:

        1. The TLS handshake SUCCEEDS. An SSLError would also make the request
           "fail", but for an entirely different reason — the certificate is
           meant to stay perfectly valid for the connection and merely stop
           conferring an identity. A TLS-layer rejection would leave the actual
           behaviour under test unverified, so it is an explicit failure here.
        2. A permissioned endpoint returns 401 — the caller got nothing.
        3. /whoami reports the anonymous identity. This is the positive form of
           the same fact and the more precise one: it shows the request was
           processed and resolved to no user, rather than merely being refused
           for some unrelated reason.
        """
        try:
            resp = self.mtls_request(cert_path, key_path, path=path,
                                     server=server)
        except requests.exceptions.SSLError as exc:
            self.fail(
                "Certificate was rejected at the TLS layer ({0}). Expected the "
                "handshake to succeed and {1} to return 401 — the certificate "
                "is meant to stay valid for the connection and merely stop "
                "conferring an identity.".format(exc, path)
            )
        self.assertEqual(
            resp.status_code, 401,
            "Certificate alone should not authenticate against {0} when {1} is "
            "enabled, got HTTP {2}: {3}".format(
                path, self.IPC_SETTING, resp.status_code, resp.text[:300])
        )

        identity = self.whoami_via_mtls(cert_path, key_path, server=server)
        self.assertEqual(
            identity.get("domain"), "anonymous",
            "Certificate alone should resolve to the anonymous identity when "
            "{0} is enabled, got {1}".format(self.IPC_SETTING, identity)
        )
        self.assertFalse(
            identity.get("roles"),
            "Anonymous caller should hold no roles, got {0}".format(identity)
        )
        return resp

    # ── EE gating ────────────────────────────────────────────────────────────

    def _require_ipc_supported(self):
        """Client certificate authentication is Enterprise-only, so the whole
        suite is."""
        if not self.rest.is_enterprise_edition():
            self.fail(
                "The internal identity password check requires an Enterprise "
                "Edition cluster."
            )

    # ── Self-healing preconditions ───────────────────────────────────────────

    def _self_heal_stuck_client_cert_auth(self):
        """
        Reset clientCertAuth to 'disable' if an aborted run left it 'mandatory'.

        Left mandatory, every later HTTPS call — including the framework's own
        setUp — fails the TLS handshake with "certificate required", so the
        suite cannot even reach the point of reporting a real failure. Uses the
        plain HTTP port to get underneath the TLS layer.

        Runs before super().setUp(), so self.master and self.log do not exist
        yet; uses TestInputSingleton and print(). Best-effort: a genuinely down
        node should surface during the real setUp, not here.
        """
        server = TestInputSingleton.input.servers[0]
        base_url = "http://{0}:8091".format(server.ip)
        auth = (server.rest_username, server.rest_password)

        try:
            resp = requests.get(
                "{0}/settings/clientCertAuth".format(base_url),
                auth=auth, timeout=30,
            )
            resp.raise_for_status()

            if resp.json().get("state") == "mandatory":
                print("[IPCBase] {0} was stuck with "
                      "clientCertAuth='mandatory'. Resetting to 'disable' via "
                      "HTTP before setUp().".format(server.ip))
                reset = requests.post(
                    "{0}/settings/clientCertAuth".format(base_url),
                    auth=auth, timeout=30,
                    headers={"Content-Type": "application/json"},
                    json={"state": "disable", "prefixes": []},
                )
                reset.raise_for_status()
        except requests.exceptions.RequestException:
            pass

    def _self_heal_stuck_trusted_cas(self):
        """
        Untrust leftover CAs from a previous run before this test trusts its own.

        Tries to delete every CA rather than guessing which one is the node's
        own: CA ids are a plain counter, and a freshly provisioned node has
        already rotated past id 0 by the time node-init finishes. The server
        refuses to delete a CA that is actually in use by a node's current
        certificate, so attempting all of them removes exactly the orphans and
        leaves the real one alone. Best-effort: logs, never raises.
        """
        try:
            removed = 0
            for entry in self.rest.get_trusted_CAs():
                ca_id = entry.get("id")
                try:
                    del_status, _, _ = self.rest.delete_trusted_CA(ca_id)
                    if del_status:
                        removed += 1
                except Exception as exc:
                    # One failed delete must not abandon the rest of the pass,
                    # or a single hiccup carries every other stale CA into the
                    # next test too.
                    self.log.warning(
                        "Trusted CA self-heal: delete of id={0} failed, "
                        "continuing with the rest: {1}".format(ca_id, exc)
                    )
            if removed > 0:
                self.log.warning(
                    "{0} had {1} stale trusted CA(s) from a previous run — "
                    "untrusted them before this test starts.".format(
                        self.master.ip, removed)
                )
        except Exception as exc:
            self.log.warning("Trusted CA self-heal error: {0}".format(exc))

        shell = RemoteMachineShellConnection(self.master)
        try:
            shell.execute_command("rm -f '{0}'/*".format(self._ca_dir()))
        except Exception as exc:
            self.log.warning(
                "Trusted CA inbox/CA cleanup error: {0}".format(exc))
        finally:
            shell.disconnect()

    # ── CA trust / cleanup ───────────────────────────────────────────────────

    def _ca_dir(self, server=None):
        """The node's inbox/CA directory, resolved the way x509main already
        resolves install paths (OS-detected, or the node's real configured data
        directory via diag/eval on Linux) rather than guessing."""
        server = server or self.master
        install_path = x509main(host=server).install_path
        return "{0}{1}/CA".format(install_path, x509main.CHAINFILEPATH)

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        """Write the CA's PEM into the node's inbox/CA folder and tell the
        cluster to load it."""
        server = server or self.master
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)
        ca_dir = self._ca_dir(server)

        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command("mkdir -p '{0}'".format(ca_dir))
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".pem", mode="wb"
            ) as tmp_file:
                tmp_file.write(pem_bytes)
                local_path = tmp_file.name
            try:
                shell.copy_file_local_to_remote(
                    local_path, "{0}/ipc_test_ca.pem".format(ca_dir)
                )
            finally:
                os.remove(local_path)
        finally:
            shell.disconnect()

        status, content = self.rest.load_trusted_CAs()
        if not status:
            self.fail("Failed to load trusted CAs on {0}: {1}".format(
                server.ip, content))

    def _cleanup_trusted_cas(self):
        """Untrust every CA this run added. The server refuses to delete a CA
        still in use by a node's certificate, so the node's own survives."""
        try:
            for entry in self.rest.get_trusted_CAs():
                ca_id = entry.get("id")
                subject = str(entry.get("subject", ""))
                if "IPCTestCA" not in subject:
                    continue
                try:
                    self.rest.delete_trusted_CA(ca_id)
                except Exception as exc:
                    self.log.warning(
                        "Failed to delete trusted CA id={0}: {1}".format(
                            ca_id, exc))
        except Exception as exc:
            self.log.warning("Trusted CA listing failed: {0}".format(exc))

        shell = RemoteMachineShellConnection(self.master)
        try:
            shell.execute_command(
                "rm -f '{0}/ipc_test_ca.pem'".format(self._ca_dir()))
        finally:
            shell.disconnect()

    # ── Client cert auth ─────────────────────────────────────────────────────

    def _enable_client_cert_auth(self, state="enable", prefixes=None):
        if prefixes is None:
            prefixes = [{"path": "subject.cn", "prefix": "", "delimiter": ""}]
        status, content = self.rest.client_cert_auth(
            state=state, prefixes=prefixes)
        if not status:
            self.fail("Failed to set clientCertAuth={0}: {1}".format(
                state, content))

    def _disable_client_cert_auth(self):
        self.rest.client_cert_auth(state="disable", prefixes=[])

    # ── Temp PEM files ───────────────────────────────────────────────────────

    def _write_temp_pem(self, pem_bytes, suffix=".pem"):
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix, mode="wb"
        ) as tmp_file:
            tmp_file.write(pem_bytes)
            path = tmp_file.name
        self._temp_pem_files.append(path)
        return path

    def _cleanup_temp_pem_files(self):
        for path in self._temp_pem_files:
            try:
                os.remove(path)
            except OSError:
                pass
        self._temp_pem_files = []

    # ── RBAC helpers ─────────────────────────────────────────────────────────

    def _create_rbac_test_user(self, username, role, password="Couchbase@1234"):
        user = [{'id': username, 'password': password, 'name': 'Some Name'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': username, 'name': 'Some Name', 'roles': role}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')
        self._rbac_users.append(username)
        return username, password

    def _cleanup_rbac_users(self):
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self.log.warning("Failed to delete RBAC user {0}: {1}".format(
                    username, exc))
        self._rbac_users = []
