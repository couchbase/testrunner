"""Base class for FTS Internal-identity Password Check (IPC) tests — MB-73874.

IPCMixin (pytests/security/ipc_base.py) owns everything that is not FTS
specific: the setting, the genuine/forged internal certificates, CA trust,
client cert auth and RBAC users, and their cleanup. This class adds only what
the Search service needs on top — requests against the FTS TLS port (18094),
an endpoint sweep, and polling helpers for runtime toggles.

ns_server's /whoami cannot say who *cbft* resolved a caller to, so on the FTS
port identity is proven by RBAC instead: an fts_searcher may query but not
create an index or read /api/cfg.
"""

import collections
import time

import requests

from lib.Cb_constants.CBServer import CbServer
from pytests.security.ipc_base import IPCMixin

from .fts_base import FTSBaseTest, LazyIndexUpdateHelper
from .fts_crl_base import (
    FTSCRLBase, FTSCallResult, OUTCOME_ALLOWED, OUTCOME_OTHER,
    OUTCOME_REJECTED_AUTH, OUTCOME_REJECTED_RBAC, OUTCOME_REJECTED_TLS,
)

# cbft answers BOTH a failed authentication and an RBAC denial with HTTP 403;
# only the body tells them apart. Every authentication failure names the
# cbauth.AuthWebCreds call that failed; an RBAC denial comes after it succeeded.
# Seen on 8.5.0-1392:
#   403 "rest_auth: cbauth.AuthWebCreds, err: Authentication failure"
#       (wrong or unknown credentials)
#   403 "rest_list: filteredListIndex, cbauth.AuthWebCreds, err: no web
#        credentials found in request" (certificate alone, setting on)
CBFT_AUTH_FAILURE = "cbauth.AuthWebCreds"

PROBE_INDEX = "fts_ipc_probe"
PROBE_INDEX_BODY = {"type": "fulltext-index", "sourceType": "gocbcore",
                    "sourceName": "default", "params": {}}

# What must never appear in a response to, or a log line about, a refused
# caller. The internal domain itself would tell an attacker their SAN was
# recognised and is merely being gated.
LEAK_MARKERS = (
    "internal.couchbase.com",
    "BEGIN CERTIFICATE",
    "PRIVATE KEY",
)


class FTSIPCBase(IPCMixin, FTSBaseTest):
    """FTSBaseTest with the IPC fixtures, aimed at the FTS TLS port."""

    FTS_SSL_PORT = CbServer.ssl_fts_port

    # The certificate alone must leave the caller UNAUTHENTICATED. An RBAC
    # denial would mean the certificate still resolved to some identity, just
    # one without permission here — so it does not count, and neither does
    # 200 or a 400/404 that shows the request got past auth.
    CERT_ONLY_REJECTED = (OUTCOME_REJECTED_AUTH,)

    # Borrowed from the CRL suite, not copied: they need only FTSBaseTest
    # helpers plus self.fts_nodes / self.log, which this class provides.
    create_and_load_test_index = FTSCRLBase.create_and_load_test_index
    index_bucket_name = FTSCRLBase.index_bucket_name
    query_path_for_index = FTSCRLBase.query_path_for_index
    cbft_pids = FTSCRLBase.cbft_pids
    assert_cbft_not_restarted = FTSCRLBase.assert_cbft_not_restarted

    def setUp(self):
        self._self_heal_stuck_client_cert_auth()
        super(FTSIPCBase, self).setUp()
        self._ipc_setup()

        self.client_cert_state = self._input.param("client_cert_state", "enable")
        self.ipc_toggle_timeout = self._input.param("ipc_toggle_timeout", 60)

        self.fts_nodes = self._cb_cluster.get_fts_nodes()
        if not self.fts_nodes:
            self.fail("FTS IPC tests need at least one node running the fts "
                      "service.")
        self.log.info("FTS IPC setUp complete: fts_nodes={0}".format(
            [node.ip for node in self.fts_nodes]))

    def tearDown(self):
        try:
            self._ipc_teardown()
        finally:
            super(FTSIPCBase, self).tearDown()

    def refresh_fts_nodes(self):
        """Re-read the FTS nodes after a topology change."""
        self.fts_nodes = self._cb_cluster.get_fts_nodes()
        return self.fts_nodes

    # ── Requests on the FTS port ─────────────────────────────────────────────

    def fts_ipc_request(self, cert_path, key_path, path, method="GET",
                        body=None, node=None, auth=None, session=None,
                        timeout=60):
        """One request to 18094 presenting `cert_path`/`key_path`.

        Leave `auth` as None to present the certificate ALONE — see
        IPCMixin.mtls_request for why that is the case that matters. Pass a
        requests.Session to reuse one keep-alive connection across calls.
        verify=False for the same reason as mtls_request: the node serves its
        own certificate, not one from this test's CA.
        """
        node = node or self.fts_nodes[0]
        url = "https://{0}:{1}{2}".format(node.ip, self.FTS_SSL_PORT, path)
        kwargs = {"timeout": timeout, "verify": False,
                  "cert": (cert_path, key_path)}
        if auth is not None:
            kwargs["auth"] = auth
        if body is not None:
            kwargs["json"] = body

        try:
            response = (session or requests).request(method, url, **kwargs)
        except requests.exceptions.RequestException as exc:
            return FTSCallResult(FTSCRLBase.classify_transport_error(exc),
                                 exception=exc, url=url)

        content = self.crl_utils.parse_content(response.content)
        if response.status_code == 401 or (
                response.status_code == 403
                and CBFT_AUTH_FAILURE in response.text):
            outcome = OUTCOME_REJECTED_AUTH
        elif response.status_code == 403:
            outcome = OUTCOME_REJECTED_RBAC
        elif response.ok:
            outcome = OUTCOME_ALLOWED
        else:
            outcome = OUTCOME_OTHER
        return FTSCallResult(outcome, status_code=response.status_code,
                             content=content, url=url)

    def default_query(self):
        return {"match": self._input.param("ipc_query_match", "emp"),
                "field": self._input.param("ipc_query_field", "type")}

    def fts_endpoints(self, index, mutating=True):
        """name -> (method, path, body) for the sweep.

        Mutating calls target PROBE_INDEX rather than the real index, so a
        request wrongly let through creates or deletes nothing the rest of the
        test relies on — and still fails the sweep, because the outcome is not
        a 401/403. PROBE_INDEX must exist while they run (see
        ensure_probe_index): cbft resolves the index before authenticating,
        so a DELETE of a missing index answers 400 "index not found" and never
        reaches the auth check.
        """
        endpoints = collections.OrderedDict([
            ("list indexes", ("GET", "/api/index", None)),
            ("index definition",
             ("GET", "/api/index/{0}".format(index.name), None)),
            ("index count",
             ("GET", "/api/index/{0}/count".format(index.name), None)),
            ("search query",
             ("POST", self.query_path_for_index(index),
              {"query": self.default_query(), "size": 10})),
            ("node stats", ("GET", "/api/nsstats", None)),
            ("api stats", ("GET", "/api/stats", None)),
            ("api cfg", ("GET", "/api/cfg", None)),
            ("manager meta", ("GET", "/api/managerMeta", None)),
        ])
        if mutating:
            endpoints["ingest control"] = (
                "POST",
                "/api/index/{0}/ingestControl/resume".format(index.name), None)
            endpoints["index create"] = (
                "PUT", "/api/index/{0}".format(PROBE_INDEX), PROBE_INDEX_BODY)
            endpoints["index delete"] = (
                "DELETE", "/api/index/{0}".format(PROBE_INDEX), None)
        return endpoints

    def admin_auth(self):
        return (self.master.rest_username, self.master.rest_password)

    def ensure_probe_index(self, cert_path, key_path, node=None):
        """Create PROBE_INDEX with administrator credentials (no-op if it is
        already there). The certificate is presented too, so this also works
        under clientCertAuth=mandatory."""
        result = self.fts_ipc_request(cert_path, key_path,
                                      "/api/index/{0}".format(PROBE_INDEX),
                                      method="PUT", body=PROBE_INDEX_BODY,
                                      node=node, auth=self.admin_auth())
        if not result.allowed and "exists" not in result.error_text():
            self.fail("Could not create probe index {0} for the sweep: {1}: "
                      "{2}".format(PROBE_INDEX, result,
                                   result.error_text()[:200]))

    def drop_probe_index(self, cert_path, key_path, node=None):
        """Best-effort delete of PROBE_INDEX with administrator credentials."""
        result = self.fts_ipc_request(cert_path, key_path,
                                      "/api/index/{0}".format(PROBE_INDEX),
                                      method="DELETE", node=node,
                                      auth=self.admin_auth())
        if not result.allowed:
            self.log.warning("could not drop probe index {0}: {1}".format(
                PROBE_INDEX, result))

    # ── Assertions ───────────────────────────────────────────────────────────

    @staticmethod
    def leaked_markers(content):
        text = str(content or "")
        return [marker for marker in LEAK_MARKERS if marker in text]

    def assert_fts_cert_only_rejected(self, cert_path, key_path, index,
                                      nodes=None, mutating=True, context=""):
        """Every sweep endpoint on every FTS node refuses the cert alone.

        As in IPCMixin.assert_no_identity_from_cert, a TLS-layer rejection is a
        failure: the certificate must stay valid for the connection and merely
        stop conferring identity. Refusals must also not leak the internal
        identity or certificate material. All failures are collected first so
        one run shows the whole picture.
        """
        failures = []
        for node in nodes or self.fts_nodes:
            if mutating:
                # Re-created per node: a wrongly allowed DELETE on the
                # previous node would otherwise leave nothing to delete here.
                self.ensure_probe_index(cert_path, key_path, node=node)
            for name, (method, path, body) in self.fts_endpoints(
                    index, mutating).items():
                result = self.fts_ipc_request(cert_path, key_path, path,
                                              method=method, body=body,
                                              node=node)
                where = "{0} ({1} {2}) on {3}".format(name, method, path,
                                                      node.ip)
                if result.outcome == OUTCOME_REJECTED_TLS:
                    failures.append(
                        "{0}: rejected at the TLS layer ({1}); expected the "
                        "handshake to succeed and the request to be "
                        "refused".format(where, result.error_text()[:200]))
                elif result.outcome not in self.CERT_ONLY_REJECTED:
                    failures.append("{0}: expected an authentication failure "
                                    "for the certificate alone, got {1}: "
                                    "{2}".format(
                                        where, result,
                                        result.error_text()[:200]))
                else:
                    leaked = self.leaked_markers(result.content)
                    if leaked:
                        failures.append("{0}: refusal leaks {1}".format(
                            where, leaked))
                    self.log.info("{0}: refused with HTTP {1}: {2}".format(
                        where, result.status_code,
                        result.error_text()[:120]))
        if mutating:
            self.drop_probe_index(cert_path, key_path)
        if failures:
            self.fail("{0}: the certificate alone was not cleanly refused by "
                      "FTS with {1} enabled:\n  {2}".format(
                          context or "FTS sweep", self.IPC_SETTING,
                          "\n  ".join(failures)))

    def assert_fts_cert_only_allowed(self, cert_path, key_path, index,
                                     nodes=None, context=""):
        """The cert alone lists indexes and queries on every FTS node.

        Only meaningful with the setting disabled — it is the "before"
        evidence that the certificate really is recognised as internal.
        """
        for node in nodes or self.fts_nodes:
            for method, path, body in (
                    ("GET", "/api/index", None),
                    ("POST", self.query_path_for_index(index),
                     {"query": self.default_query(), "size": 10})):
                result = self.fts_ipc_request(cert_path, key_path, path,
                                              method=method, body=body,
                                              node=node)
                if not result.allowed:
                    self.fail(
                        "{0}: with {1} disabled the internal certificate alone "
                        "should reach {2} {3} on {4}, got {5}: {6}. If this "
                        "stops reproducing, the enforcement assertions prove "
                        "nothing.".format(
                            context or "FTS baseline", self.IPC_SETTING,
                            method, path, node.ip, result,
                            result.error_text()[:200]))

    def wait_for_fts_cert_only(self, cert_path, key_path, expect_rejected,
                               nodes=None, session=None, timeout=None,
                               interval=1):
        """Poll GET /api/index with the cert alone on each node until it gives
        the expected answer. Returns the seconds taken.

        A runtime toggle is pushed to cbft through cbauth, so a short lag is
        allowed; one that outlasts `timeout` fails, naming the nodes still
        answering the old way.
        """
        timeout = timeout or self.ipc_toggle_timeout
        pending = list(nodes or self.fts_nodes)
        start = time.time()
        last = {}
        while pending and time.time() - start < timeout:
            for node in list(pending):
                result = self.fts_ipc_request(cert_path, key_path,
                                              "/api/index", node=node,
                                              session=session)
                last[node.ip] = result
                if expect_rejected and result.outcome in self.CERT_ONLY_REJECTED:
                    pending.remove(node)
                elif not expect_rejected and result.allowed:
                    pending.remove(node)
            if pending:
                time.sleep(interval)
        elapsed = time.time() - start
        if pending:
            self.fail("Certificate-only access on FTS did not become {0} "
                      "within {1}s on {2}; last results: {3}".format(
                          "refused" if expect_rejected else "allowed",
                          timeout, [node.ip for node in pending],
                          {ip: last.get(ip) for ip in
                           [node.ip for node in pending]}))
        self.log.info("Certificate-only access on FTS became {0} on all "
                      "nodes after {1:.1f}s".format(
                          "refused" if expect_rejected else "allowed",
                          elapsed))
        return elapsed

    # ── fts.log ──────────────────────────────────────────────────────────────

    def fts_log_baselines(self):
        return LazyIndexUpdateHelper(self).fts_log_baselines()

    def fts_log_since(self, baselines):
        return LazyIndexUpdateHelper(self).fts_log_since(baselines)
