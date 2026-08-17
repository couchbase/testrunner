"""Run existing FTS suites as CRL enforcement tests.

`CRLEnforcementMixin` puts an existing suite behind a CRL-checked client
certificate. FTSBaseTest builds the x509 fixtures when the conf passes
`multiple_ca=True,use_client_certs=True`, and
`on_prem_rest_client.urllib_request` then routes every FTS index CRUD/query call
over that certificate with the Authorization header removed — so revoking the
certificate turns an inherited test body into a revocation test with no new test
code. The bridge below is the only thing in the repo that can revoke it: it
finds the intermediate CA that signed the cert and signs a CRL with it.

The subclasses at the bottom are declarations only; coverage is chosen by conf
lines naming existing test methods. `crl_enabled=False` runs the same line with
enforcement off, for an A/B baseline.
"""

import os

import requests
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.crl_utils import CRLUtils

from .fts_crl_base import FTSCRLBase, OUTCOME_REJECTED_TLS

from .fts_vector_search import VectorSearch
from .index_management_api import IndexManagementAPI
from .moving_topology_fts import MovingTopFTS
from .stable_topology_fts import StableTopFTS

#: The client CA `urllib_request` presents certs from (hardcoded there).
DEFAULT_CLIENT_INT_CA = "iclient1_clientroot"
CRL_FILE = "fts_crl_x509.pem"


class CRLEnforcementMixin(object):
    """Applies a CRL policy to an inherited FTS suite and can revoke its cert."""

    def setUp(self):
        super(CRLEnforcementMixin, self).setUp()
        self.crl_client_auth = self._input.param("crl_client_auth", "Require")
        self.crl_node_to_node = self._input.param("crl_node_to_node", None)
        self._crl_number = 0
        self._crl_active = False
        if self._input.param("crl_enabled", True):
            self.enable_crl_enforcement()

    def tearDown(self):
        try:
            if getattr(self, "_crl_active", False):
                self.disable_crl_enforcement()
        except Exception as exc:
            self.log.warning("CRL teardown error: {0}".format(exc))
        finally:
            super(CRLEnforcementMixin, self).tearDown()

    # ── x509main -> CRLUtils bridge ──────────────────────────────────────────

    def _x509(self):
        if not getattr(CbServer, "x509", None):
            self.fail("CRLEnforcementMixin needs the x509 fixtures — pass "
                      "multiple_ca=True,use_client_certs=True in the conf.")
        return CbServer.x509

    def _read_x509_file(self, path):
        """Read a file from x509main's cert tree (local slave, else over SSH)."""
        try:
            with open(path, "rb") as handle:
                return handle.read()
        except IOError:
            pass
        shell = RemoteMachineShellConnection(self._x509().slave_host)
        try:
            out, err = shell.execute_command("cat {0}".format(path))
            data = "\n".join(out) if isinstance(out, list) else (out or "")
            if not data.strip():
                self.fail("Could not read {0} from the x509 cert tree on {1}: "
                          "{2}".format(path, self._x509().slave_host.ip, err))
            return data.encode() if isinstance(data, str) else data
        finally:
            shell.disconnect()

    def x509_client_paths(self, int_ca_name=DEFAULT_CLIENT_INT_CA):
        """(leaf_pem_path, issuing_intermediate_dir) for the presented client cert."""
        x509_util = self._x509()
        entry = x509_util.client_ca_map.get("client_" + int_ca_name)
        if not entry:
            self.fail("No client CA 'client_{0}' in the x509 manifest; have "
                      "{1}".format(int_ca_name,
                                   list(x509_util.client_ca_map)))
        client_dir = entry["path"].rstrip("/")
        # client_dir is <root_ca_dir>/client_<int>, so the issuing intermediate
        # lives beside it as <root_ca_dir>/<signed_by>/ holding int.pem/int.key.
        int_dir = os.path.join(os.path.dirname(client_dir), entry["signed_by"])
        leaf = os.path.join(client_dir, "{0}.pem".format(x509_util.client_ip))
        return leaf, int_dir

    def x509_client_ca(self, int_ca_name=DEFAULT_CLIENT_INT_CA):
        """(cert, key) of the intermediate that signed the client certificate."""
        _, int_dir = self.x509_client_paths(int_ca_name)
        cert = x509.load_pem_x509_certificate(
            self._read_x509_file(os.path.join(int_dir, "int.pem")))
        key = serialization.load_pem_private_key(
            self._read_x509_file(os.path.join(int_dir, "int.key")), password=None)
        return cert, key

    def x509_client_serial(self, int_ca_name=DEFAULT_CLIENT_INT_CA):
        """Serial of the client leaf cert that urllib_request presents."""
        leaf, _ = self.x509_client_paths(int_ca_name)
        return x509.load_pem_x509_certificate(
            self._read_x509_file(leaf)).serial_number

    # ── Policy control ───────────────────────────────────────────────────────

    def publish_x509_crl(self, revoked_serials=()):
        """Upload a CRL signed by the client cert's issuer, then reload all nodes."""
        cert, key = self.x509_client_ca()
        self._crl_number += 1
        pem = CRLUtils.build_crl(cert, key, revoked_serials=list(revoked_serials),
                                 crl_number=self._crl_number)
        rest = RestConnection(self.master)
        status, content, _ = rest.upload_crl_file(CRL_FILE, pem)
        if not status:
            self.fail("CRL upload failed: {0}".format(
                CRLUtils.parse_content(content)))
        for server in self._input.servers:
            RestConnection(server).reload_crl()
        self.log.info("Published CRL #{0} revoking {1}".format(
            self._crl_number, list(revoked_serials) or "nothing"))

    def set_crl_policy(self, client_auth=None, node_to_node=None):
        policy = {}
        if client_auth:
            policy["clientAuth"] = client_auth
        if node_to_node:
            policy["nodeToNode"] = node_to_node
        if not policy:
            return
        status, content, _ = RestConnection(self.master).post_crl_settings(
            {"policyPerScope": policy})
        if not status:
            self.fail("Setting CRL policy {0} failed: {1}".format(
                policy, CRLUtils.parse_content(content)))
        self.log.info("CRL policyPerScope={0}".format(policy))

    def enable_crl_enforcement(self):
        """Baseline CRL revoking nothing, then switch the policy on."""
        self.publish_x509_crl()
        self.set_crl_policy(client_auth=self.crl_client_auth,
                            node_to_node=self.crl_node_to_node)
        self._crl_active = True
        self.log.info("CRL enforcement active (clientAuth={0}) — the inherited "
                      "suite now runs over a CRL-checked client "
                      "cert".format(self.crl_client_auth))

    def disable_crl_enforcement(self):
        rest = RestConnection(self.master)
        self.set_crl_policy(client_auth="Disabled", node_to_node="Disabled")
        rest.delete_crl_file(CRL_FILE)
        self._crl_active = False

    def revoke_client_cert(self):
        """Revoke the certificate the inherited suite authenticates with."""
        serial = self.x509_client_serial()
        self.publish_x509_crl([serial])
        return serial

    def restore_client_cert(self):
        self.publish_x509_crl()


class CRLVectorSearch(CRLEnforcementMixin, VectorSearch):
    """VectorSearch, run with a CRL-checked client certificate."""

    def test_crl_vector_search_revocation(self):
        """Plan FTS-IDX-07 — kNN works under Require, then is refused once revoked."""
        containers = self._cb_cluster._setup_bucket_structure(
            cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(
            containers, dataset=self.vector_dataset)

        idx = [("i1", "b1.s1.c1")]
        index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=idx,
            vector_fields=self._build_vector_fields(),
            create_vector_index=True,
            extra_fields=[{"sno": "number"}])
        index[0]['dataset'] = bucketvsdataset['bucket_name']
        index[0]['similarity'] = self.similarity
        index_obj = next(item for item in index
                         if item['name'] == "i1")['index_obj']

        queries = self.get_query_vectors(self.vector_dataset)
        vector = queries[0].tolist()

        def knn():
            return self._vector_hits(self.run_vector_query(
                vector=vector, index=index_obj, validate_result_count=False))

        # Under Require with a valid cert the kNN query must behave normally.
        hits = knn()
        if hits is None or hits < 0:
            self.fail("kNN query failed under CRL Require with a valid "
                      "certificate: hits={0}".format(hits))
        self.log.info("kNN under Require with a valid cert: {0} hits".format(hits))

        # Revoke the certificate the suite authenticates with; the same query
        # must now be refused rather than returning results.
        self.revoke_client_cert()
        try:
            revoked_hits = knn()
        except requests.exceptions.RequestException as exc:
            # Only a positively identified TLS alert proves the certificate was
            # refused. A bare connection failure could equally be flaky infra,
            # and accepting it here would pass this test without the product
            # having enforced anything.
            outcome = FTSCRLBase.classify_transport_error(exc)
            if outcome != OUTCOME_REJECTED_TLS:
                self.fail(
                    "kNN with a revoked cert failed at the transport layer but "
                    "with no TLS alert, so this is an infrastructure fault, not "
                    "evidence of revocation: {0}: {1}".format(
                        type(exc).__name__, str(exc)[:300]))
            self.log.info("Revoked cert refused by a TLS alert as expected: "
                          "{0}".format(str(exc)[:160]))
        else:
            if revoked_hits is not None and revoked_hits > 0:
                self.fail(
                    "kNN query returned {0} hits with a REVOKED client "
                    "certificate — vector search is bypassing CRL "
                    "enforcement.".format(revoked_hits))
            self.log.info("Revoked cert yielded no results (hits={0})".format(
                revoked_hits))

        # Restore and confirm recovery, so a false positive above is ruled out.
        self.restore_client_cert()
        hits = knn()
        if hits is None or hits < 0:
            self.fail("kNN query did not recover after un-revoking the "
                      "certificate: hits={0}".format(hits))
        self.log.info("kNN recovered after un-revocation: {0} hits".format(hits))

    @staticmethod
    def _vector_hits(result):
        """hits out of run_vector_query's variable-arity return.

        Normal and failure paths return (n1ql_hits, hits, ...) so hits is index
        1; the doc-filter path returns (hits, hits_d, ...) with hits first.
        """
        if not isinstance(result, (list, tuple)):
            return None
        if len(result) == 6:
            return result[0]
        return result[1] if len(result) >= 2 else None


class CRLStableTopFTS(CRLEnforcementMixin, StableTopFTS):
    """StableTopFTS (114 tests) under CRL enforcement."""


class CRLMovingTopFTS(CRLEnforcementMixin, MovingTopFTS):
    """MovingTopFTS (24 tests) under CRL enforcement — rebalance and failover."""


class CRLIndexManagementAPI(CRLEnforcementMixin, IndexManagementAPI):
    """IndexManagementAPI (10 tests) under CRL enforcement — control endpoints."""
