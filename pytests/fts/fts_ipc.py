import time

import requests

from .fts_crl_base import (
    FTSCRLBase, OUTCOME_REJECTED_AUTH, OUTCOME_REJECTED_RBAC,
)
from .fts_ipc_base import FTSIPCBase, PROBE_INDEX, PROBE_INDEX_BODY


class FTSIPC(FTSIPCBase):
    """
    Internal-identity Password Check under mTLS (MB-73874), on the Search
    service.

    These tests cover what only the FTS port can show: that cbft, resolving
    identity through cbauth, refuses a certificate carrying the internal SAN
    once the setting is on, and that credentials alone decide who the caller
    is. Whether FTS's own internal traffic keeps working with the setting on is
    covered by re-running the existing suites — see fts_ipc_reuse.py.
    """

    # restart_couchbase_on_node waits on ns_server; restart_cbft lets the
    # babysitter bring cbft back. Both are the CRL suite's, unchanged.
    restart_cbft = FTSCRLBase.restart_cbft
    restart_couchbase_on_node = FTSCRLBase.restart_couchbase_on_node

    def _admin_auth(self):
        return self.master.rest_username, self.master.rest_password

    def _query(self, cert_path, key_path, index, node=None, auth=None,
               session=None):
        return self.fts_ipc_request(
            cert_path, key_path, self.query_path_for_index(index),
            method="POST", body={"query": self.default_query(), "size": 10},
            node=node, auth=auth, session=session)

    def _wait_until_fts_serves(self, cert_path, key_path, index, node,
                               timeout=300, interval=5):
        """Poll until cert + admin credentials can query `node` again."""
        start = time.time()
        result = None
        while time.time() - start < timeout:
            result = self._query(cert_path, key_path, index, node=node,
                                 auth=self._admin_auth())
            if result.allowed:
                return
            time.sleep(interval)
        self.fail("FTS on {0} did not serve queries within {1}s of the "
                  "restart; last result {2}".format(node.ip, timeout, result))

    # ── P0 ───────────────────────────────────────────────────────────────────

    def test_fts_forged_cert_blocked(self):
        """
        The customer's bypass, against the Search service.

        Test plan: Customer Exploit 1 & 4, Cross Functional 5 (FTS).

        Steps:
            1. Create and load an index; enable client cert auth in the state
               under test (client_cert_state).
            2. Forge a certificate: unrelated subject, internal SAN injected.
            3. IPC enabled: the certificate alone is refused on every FTS node.
            4. IPC disabled: the same certificate lists indexes and queries —
               the "before" evidence that it is recognised as internal.
            5. IPC re-enabled: refused again, so the setting is what decides.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_forged_internal_cert()

        self.log.info("Step 3: IPC enabled — forged certificate must be "
                      "refused by FTS")
        self.set_ipc_setting(True)
        self.wait_for_fts_cert_only(cert_path, key_path, expect_rejected=True)
        self.assert_fts_cert_only_rejected(cert_path, key_path, index,
                                           mutating=False, context="Step 3")

        self.log.info("Step 4: IPC disabled — forged certificate must "
                      "reproduce the bypass on FTS")
        self.set_ipc_setting(False)
        self.wait_for_fts_cert_only(cert_path, key_path, expect_rejected=False)
        self.assert_fts_cert_only_allowed(cert_path, key_path, index,
                                          context="Step 4")

        self.log.info("Step 5: IPC re-enabled — refused again")
        self.set_ipc_setting(True)
        self.wait_for_fts_cert_only(cert_path, key_path, expect_rejected=True)
        self.assert_fts_cert_only_rejected(cert_path, key_path, index,
                                           mutating=False, context="Step 5")

    def test_fts_genuine_cert_needs_credentials(self):
        """
        With IPC enabled, credentials alone decide identity on FTS, and grant
        exactly that account's permissions.

        Test plan: Core 1–4, Customer Exploit 2.

        Steps:
            1. Genuine internal certificate from the cluster's CA; IPC on.
            2. Certificate alone: refused.
            3. Certificate + administrator: may create an index.
            4. Certificate + fts_searcher[default]: may query but NOT create an
               index (403). This is the discriminating case — an administrator
               would have been allowed regardless of what the certificate did.
            5. Certificate + wrong password, and + unknown user: refused.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")
        self.set_ipc_setting(True)
        self.wait_for_fts_cert_only(cert_path, key_path, expect_rejected=True)

        self.log.info("Step 2: certificate alone")
        self.assert_fts_cert_only_rejected(cert_path, key_path, index,
                                           mutating=False, context="Step 2")

        self.log.info("Step 3: certificate + administrator")
        result = self.fts_ipc_request(
            cert_path, key_path, "/api/index/{0}".format(PROBE_INDEX),
            method="PUT", body=PROBE_INDEX_BODY, auth=self._admin_auth())
        self.assertTrue(result.allowed,
                        "Administrator credentials over an internal "
                        "certificate should create an index, got {0}: "
                        "{1}".format(result, result.error_text()[:300]))
        self.fts_ipc_request(
            cert_path, key_path, "/api/index/{0}".format(PROBE_INDEX),
            method="DELETE", auth=self._admin_auth())

        self.log.info("Step 4: certificate + fts_searcher")
        searcher = self._create_rbac_test_user("ipc_fts_searcher",
                                               "fts_searcher[default]")
        result = self._query(cert_path, key_path, index, auth=searcher)
        self.assertTrue(result.allowed,
                        "fts_searcher over an internal certificate should be "
                        "able to query, got {0}: {1}".format(
                            result, result.error_text()[:300]))
        result = self.fts_ipc_request(
            cert_path, key_path, "/api/index/{0}".format(PROBE_INDEX),
            method="PUT", body=PROBE_INDEX_BODY, auth=searcher)
        self.assertEqual(
            result.outcome, OUTCOME_REJECTED_RBAC,
            "fts_searcher over an internal certificate must be denied index "
            "creation by RBAC (authenticated, then 403) — the certificate "
            "confers no privilege. Got {0}: {1}".format(
                result, result.error_text()[:300]))

        self.log.info("Step 5: certificate + bad credentials")
        for label, auth in (
                ("wrong password", (searcher[0], "not-the-password")),
                ("unknown user", ("ipc_no_such_user", "Couchbase@1234"))):
            result = self._query(cert_path, key_path, index, auth=auth)
            self.assertEqual(
                result.outcome, OUTCOME_REJECTED_AUTH,
                "Internal certificate + {0} should fail authentication, got "
                "{1}: {2}".format(label, result, result.error_text()[:300]))

    def test_fts_endpoint_sweep_cert_only(self):
        """
        Every FTS endpoint, on every FTS node, refuses both the forged and the
        genuine internal certificate alone, without leaking anything.

        Test plan: Customer Exploit 4, Core 1, Error Messages.

        The sweep first runs with administrator credentials as a positive
        control: a wrong path or a down node would otherwise also "refuse" the
        certificate, and the test would pass for the wrong reason.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        forged = self.generate_forged_internal_cert()
        genuine = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")
        self.set_ipc_setting(True)

        self.log.info("Positive control: read endpoints with credentials")
        for node in self.fts_nodes:
            for name, (method, path, body) in self.fts_endpoints(
                    index, mutating=False).items():
                result = self.fts_ipc_request(
                    genuine[0], genuine[1], path, method=method, body=body,
                    node=node, auth=self._admin_auth())
                self.assertTrue(
                    result.allowed,
                    "Positive control: {0} ({1} {2}) on {3} should succeed "
                    "with administrator credentials, got {4}: {5}".format(
                        name, method, path, node.ip, result,
                        result.error_text()[:200]))

        for label, (cert_path, key_path) in (("forged", forged),
                                             ("genuine", genuine)):
            self.wait_for_fts_cert_only(cert_path, key_path,
                                        expect_rejected=True)
            self.assert_fts_cert_only_rejected(
                cert_path, key_path, index,
                context="{0} certificate".format(label))

        # cbft resolves the index before authenticating, so an unauthenticated
        # caller can tell a missing index (400) from an existing one (403).
        # Logged, not asserted, until the Search team says whether that is
        # intended.
        result = self.fts_ipc_request(forged[0], forged[1],
                                      "/api/index/fts_ipc_no_such_index",
                                      method="DELETE")
        self.log.info("[observation] cert-only DELETE of a missing index: "
                      "HTTP {0}: {1}".format(result.status_code,
                                             result.error_text()[:120]))

    def test_fts_runtime_toggle_no_restart(self):
        """
        Toggling the setting takes effect on FTS without restarting cbft, and
        repeatedly, with no stuck state.

        Test plan: Runtime Toggle, Setting Write.

        Records how long each direction takes to reach every FTS node — a
        long lag would point at caching in the cbauth path.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_forged_internal_cert()
        cycles = self._input.param("ipc_toggle_cycles", 3)
        pids_before = self.cbft_pids()

        latencies = []
        for cycle in range(1, cycles + 1):
            self.set_ipc_setting(True)
            enabled_after = self.wait_for_fts_cert_only(
                cert_path, key_path, expect_rejected=True)
            self.set_ipc_setting(False)
            disabled_after = self.wait_for_fts_cert_only(
                cert_path, key_path, expect_rejected=False)
            latencies.append((round(enabled_after, 1),
                              round(disabled_after, 1)))
            self.log.info("Cycle {0}: enable reached FTS in {1:.1f}s, disable "
                          "in {2:.1f}s".format(cycle, enabled_after,
                                                disabled_after))

        self.set_ipc_setting(True)
        self.wait_for_fts_cert_only(cert_path, key_path, expect_rejected=True)
        self.assert_fts_cert_only_rejected(cert_path, key_path, index,
                                           mutating=False,
                                           context="after toggling")
        self.assert_cbft_not_restarted(pids_before, context="IPC toggling")
        self.log.info("Toggle latencies (enable, disable) per cycle: "
                      "{0}".format(latencies))

    # ── P1 ───────────────────────────────────────────────────────────────────

    def test_fts_keepalive_identity_per_request(self):
        """
        Identity is evaluated per request, not per connection.

        Test plan: Security 1 (keep-alive).

        Over one keep-alive session: certificate + credentials succeeds, the
        next request with the certificate alone is refused, and credentials
        work again after that.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")
        self.set_ipc_setting(True)
        self.wait_for_fts_cert_only(cert_path, key_path, expect_rejected=True)

        session = requests.Session()
        try:
            for step, auth, expect_allowed in (
                    ("with credentials", self._admin_auth(), True),
                    ("certificate alone", None, False),
                    ("with credentials again", self._admin_auth(), True)):
                result = self._query(cert_path, key_path, index, auth=auth,
                                     session=session)
                if expect_allowed:
                    self.assertTrue(result.allowed,
                                    "Keep-alive request {0} should succeed, "
                                    "got {1}".format(step, result))
                else:
                    self.assertIn(
                        result.outcome, self.CERT_ONLY_REJECTED,
                        "Keep-alive request {0} must be refused even though "
                        "the previous request on the connection carried "
                        "credentials, got {1}".format(step, result))
        finally:
            session.close()

    def test_fts_open_connection_loses_rights_on_enable(self):
        """
        A connection opened while the setting was off does not keep
        certificate-derived rights once it is turned on.

        Test plan: Customer Exploit 4 (open TLS connection), Runtime Toggle.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_forged_internal_cert()
        self.set_ipc_setting(False)
        node = self.fts_nodes[0]

        session = requests.Session()
        try:
            result = self._query(cert_path, key_path, index, node=node,
                                 session=session)
            self.assertTrue(result.allowed,
                            "With IPC disabled the forged certificate should "
                            "query FTS, got {0} — without that baseline the "
                            "test proves nothing".format(result))

            self.set_ipc_setting(True)
            self.wait_for_fts_cert_only(cert_path, key_path,
                                        expect_rejected=True, nodes=[node],
                                        session=session)
        finally:
            session.close()

    def test_fts_ui_proxy_cert_only(self):
        """
        ns_server's proxy to FTS (/_p/fts/..., used by the UI) refuses the
        certificate alone.

        Test plan: Core 7, Cross Functional 1.
        """
        self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_forged_internal_cert()
        path = "/_p/fts/api/index"

        self.set_ipc_setting(False)
        resp = self.mtls_request(cert_path, key_path, path=path)
        self.assertEqual(resp.status_code, 200,
                         "With IPC disabled the forged certificate should "
                         "reach {0}, got HTTP {1}: {2}".format(
                             path, resp.status_code, resp.text[:300]))

        self.set_ipc_setting(True)
        resp = self.mtls_request(cert_path, key_path, path=path)
        self.assertEqual(resp.status_code, 401,
                         "With IPC enabled the forged certificate alone must "
                         "not reach {0}, got HTTP {1}: {2}".format(
                             path, resp.status_code, resp.text[:300]))
        self.assertFalse(self.leaked_markers(resp.text),
                         "Refusal leaks {0}".format(
                             self.leaked_markers(resp.text)))

        resp = self.mtls_request(cert_path, key_path, path=path,
                                 auth=self._admin_auth())
        self.assertEqual(resp.status_code, 200,
                         "Certificate + administrator credentials should "
                         "reach {0}, got HTTP {1}: {2}".format(
                             path, resp.status_code, resp.text[:300]))

    def test_fts_internal_name_variants(self):
        """
        Internal certificates naming any internal identity — internal, fts,
        cbq-engine, prometheus... (internal_name) — are refused by FTS on
        their own.

        Test plan: Certificate Edge Cases 1.
        """
        name = self._input.param("internal_name", "internal")
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name=name, cn="ipc-{0}".format(name))
        self.set_ipc_setting(True)
        self.wait_for_fts_cert_only(cert_path, key_path, expect_rejected=True)
        self.assert_fts_cert_only_rejected(
            cert_path, key_path, index,
            context="{0}@{1}".format(name, self.INTERNAL_CERT_DOMAIN))

    def test_fts_rebalance_in_node_enforces(self):
        """
        An FTS node rebalanced in while the setting is on enforces it at once,
        and serves its partitions to credentialed callers.

        Test plan: Cluster Distribution 1–2, Dynamic Service Topology.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_forged_internal_cert()
        self.set_ipc_setting(True)

        before = {node.ip for node in self.fts_nodes}
        self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])
        new_nodes = [node for node in self.refresh_fts_nodes()
                     if node.ip not in before]
        self.assertTrue(new_nodes, "Rebalance-in added no FTS node")
        self.wait_for_indexing_complete()

        self.assert_fts_cert_only_rejected(cert_path, key_path, index,
                                           nodes=new_nodes, mutating=False,
                                           context="rebalanced-in node")
        for node in new_nodes:
            result = self._query(cert_path, key_path, index, node=node,
                                 auth=self._admin_auth())
            self.assertTrue(result.allowed,
                            "Credentialed query on rebalanced-in node {0} "
                            "failed: {1}: {2}".format(
                                node.ip, result, result.error_text()[:300]))

    def test_fts_restart_keeps_enforcement(self):
        """
        After cbft (restart_mode=cbft) or the whole node
        (restart_mode=couchbase) restarts, FTS re-authenticates, serves
        credentialed queries, and still refuses the certificate alone.

        Test plan: Service Restart, Node Restart.
        """
        mode = self._input.param("restart_mode", "cbft")
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        cert_path, key_path = self.generate_forged_internal_cert()
        self.set_ipc_setting(True)
        node = self.fts_nodes[0]

        if mode == "couchbase":
            self.restart_couchbase_on_node(node)
        else:
            self.restart_cbft(node)
        self._wait_until_fts_serves(cert_path, key_path, index, node)

        self.assertTrue(self.get_ipc_setting(),
                        "{0} did not survive the {1} restart".format(
                            self.IPC_SETTING, mode))
        self.assert_fts_cert_only_rejected(
            cert_path, key_path, index, nodes=[node], mutating=False,
            context="after {0} restart".format(mode))

    def test_fts_logs_do_not_leak_on_rejection(self):
        """
        fts.log carries no certificate or key material after certificate-only
        rejections.

        Test plan: Logs, Cross Functional 5 ("failures are logged clearly").

        What cbft logs about a refusal is not yet specified, so matching lines
        are logged for review rather than asserted on.
        """
        index = self.create_and_load_test_index()
        self._enable_client_cert_auth(state=self.client_cert_state)
        forged = self.generate_forged_internal_cert()
        self.set_ipc_setting(True)
        self.wait_for_fts_cert_only(forged[0], forged[1],
                                    expect_rejected=True)

        baselines = self.fts_log_baselines()
        self.assert_fts_cert_only_rejected(forged[0], forged[1], index,
                                           mutating=False)
        log_text = self.fts_log_since(baselines)

        leaked = [marker for marker in ("BEGIN CERTIFICATE", "PRIVATE KEY")
                  if marker in log_text]
        self.assertFalse(leaked,
                         "fts.log contains certificate/key material after "
                         "certificate-only rejections: {0}".format(leaked))
        auth_lines = [line for line in log_text.splitlines()
                      if "auth" in line.lower()]
        self.log.info("[observation] fts.log auth lines since the probes "
                      "({0}):\n{1}".format(len(auth_lines),
                                           "\n".join(auth_lines[:20])))
