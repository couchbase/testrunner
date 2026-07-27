import os
import tempfile

from basetestcase import BaseTestCase
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.crl_utils import CRLUtils
from pytests.security.rbac_base import RbacBase
from pytests.security.x509main import x509main


class CRLBase(BaseTestCase):
    """
    Base class for CRL (Certificate Revocation List) tests against Couchbase
    Server Enterprise. See CRL_INFO.md for the full REST/API reference.

    Extends the portable `BaseTestCase` alias (not `OnPremBaseTestCase`
    directly), matching MultipleCA's convention, so CRL tests also work
    unmodified against Capella runs.
    """

    def setUp(self):
        super(CRLBase, self).setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.master)

        self._require_crl_supported()

        # Uploaded CRL filenames created during a test — cleaned up in tearDown
        self._created_files = []
        # RBAC users created during a test — cleaned up in tearDown
        self._rbac_users = []

        self.ca_cert, self.ca_key = self.crl_utils.generate_ca("TestCA1")
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        try:
            if hasattr(self, "rest"):
                try:
                    self._cleanup_created_files()
                except Exception as exc:
                    self.log.warning("CRL file cleanup error: {0}".format(exc))
                try:
                    self._reset_crl_settings()
                except Exception as exc:
                    self.log.warning("CRL settings reset error: {0}".format(exc))
                try:
                    self._disable_client_cert_auth()
                except Exception as exc:
                    self.log.warning("clientCertAuth disable error: {0}".format(exc))
                try:
                    self._cleanup_rbac_users()
                except Exception as exc:
                    self.log.warning("RBAC user cleanup error: {0}".format(exc))
        finally:
            super(CRLBase, self).tearDown()

    # ── EE gating ────────────────────────────────────────────────────────────

    def _require_crl_supported(self):
        """Fail immediately if the cluster can't run CRL tests — Enterprise
        Edition only. No compat-version check — this suite assumes it always
        runs against Totoro+ (8.1+) clusters."""
        if not self.rest.is_enterprise_edition():
            self.fail("CRL support requires an Enterprise Edition cluster.")

    # ── CA trust setup ───────────────────────────────────────────────────────

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        """
        Write ca_cert's PEM into the node's real inbox/CA folder and instruct
        the cluster to load it (POST /node/controller/loadTrustedCAs).

        Resolves the install path via x509main(host=server).install_path —
        the same resolution x509main._get_install_path() already uses
        (OS-detected WININSTALLPATH/MACINSTALLPATH, or the node's actual
        configured data directory via a diag/eval call for Linux — see
        x509main.get_data_path()) rather than guessing at shell attributes.
        """
        server = server or self.master
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)
        install_path = x509main(host=server).install_path
        ca_dir = "{0}{1}/CA".format(install_path, x509main.CHAINFILEPATH)

        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command("mkdir -p {0}".format(ca_dir))
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".pem", mode="wb"
            ) as tmp_file:
                tmp_file.write(pem_bytes)
                local_path = tmp_file.name
            try:
                shell.copy_file_local_to_remote(
                    local_path, "{0}/crl_test_ca.pem".format(ca_dir)
                )
            finally:
                os.remove(local_path)
        finally:
            shell.disconnect()

        status, content = self.rest.load_trusted_CAs()
        if not status:
            self.fail("Failed to load trusted CAs on {0}: {1}".format(server.ip, content))

    # ── Cleanup helpers ──────────────────────────────────────────────────────

    def _track_uploaded_file(self, filename):
        self._created_files.append(filename)

    def _cleanup_created_files(self):
        for filename in self._created_files:
            status, _, _ = self.rest.delete_crl_file(filename)
            if not status:
                self.log.warning("Failed to delete CRL file {0} in teardown".format(filename))
        self._created_files = []

    def _reset_crl_settings(self):
        self.rest.post_crl_settings(
            {"policyPerScope": {"clientAuth": "Disabled", "nodeToNode": "Disabled"}}
        )

    def _disable_client_cert_auth(self):
        self.rest.client_cert_auth(state="disable", prefixes=[])

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
                self.log.warning("Failed to delete RBAC user {0}: {1}".format(username, exc))
        self._rbac_users = []
