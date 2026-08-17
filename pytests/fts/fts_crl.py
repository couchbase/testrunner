"""FTS CRL (Certificate Revocation List) tests.

Covers the whole plan: functional sanity and clientAuth enforcement (§1, §2),
the policy-mode matrix (§4A), CRL lifecycle and hot reload (§5), index
operations (§6), failure semantics (§10), chaos and resilience (§12) and the
diagnostic endpoint (§13A).

cbft implements no CRL logic — it consumes cbauth.CRLsValidate from the
VerifyPeerCertificate callback of its tls.Config, so rejection lands at the TLS
handshake (a 401 is the PRD's Phase-1 fallback), the check is a live
per-handshake call to ns_server, and existing connections survive until they
reconnect. Every discrimination test uses the Dual-Client Differential Pattern:
a valid and a revoked identity from one CA, same state, one invocation, both
outcomes asserted together.
"""

import datetime
import time

import requests
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from lib.Cb_constants.CBServer import CbServer
from pytests.security.crl_utils import (
    CACHE_STATUS_VALUES, DIAGNOSTIC_STATUS_VALUES, KEY_ALGORITHMS,
    RELOAD_RESULT_VALUES,
)

from .fts_crl_base import (
    ClientIdentity, DEFAULT_CRL_FILE, FTSCRLBase, OUTCOME_REJECTED_RBAC,
)


class FTSCRL(FTSCRLBase):
    """Enforcement, lifecycle, index operations, diagnostics and chaos."""

    def test_crl_smoke_client_cert_revocation(self):
        """Plan FTS-FUNC-06 / FTS-CLI-02 — the Phase 0 unblock check."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("smoke")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")

        query = self.default_query()
        operation = self.query_op(index, query)

        baseline = self.assert_allowed(
            operation(identity), context="baseline before revocation")
        expected_hits = baseline.total_hits()

        self.revoke(identity, crl_number=2)
        rejected = self.wait_until_revoked(
            operation, identity, context="after revocation")
        self.assert_revoked(rejected, context="after revocation")
        self.log.info(
            "REJECTION CONTRACT: revoked FTS client cert surfaced as {0}. "
            "TLS-layer rejection is expected; 401 is the PRD Phase-1 "
            "fallback.".format(rejected.outcome))

        self.publish_crl([], crl_number=3)
        restored = self.wait_until_allowed(
            operation, identity, context="after access restored")
        self.assert_allowed(restored, expected_hits=expected_hits,
                            context="after access restored")

    def test_crl_index_lifecycle_dual_client(self):
        """Plan FTS-FUNC-01 — full index lifecycle via the dual-client pattern."""
        mode = self.crl_client_auth_mode
        source = self.create_and_load_test_index(index_name="fts_crl_source")
        valid, revoked = self.create_dual_identities()
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth=mode)
        self.revoke(revoked, crl_number=2)

        expect_rejected = (mode != "Disabled")
        query = self.default_query()

        def create_index(identity):
            new_name = "lifecycle_{0}".format(identity.cn.replace("-", "_"))
            body = self.clone_index_definition(source, new_name)
            return self.fts_api(
                identity, "/api/index/{0}".format(new_name),
                method="PUT", body=body)

        def read_definition(identity):
            return self.fts_index_definition(identity, source.name)

        def run_query(identity):
            return self.fts_query(identity, source, query)

        def drop_index(identity):
            new_name = "lifecycle_{0}".format(identity.cn.replace("-", "_"))
            return self.fts_api(
                identity, "/api/index/{0}".format(new_name), method="DELETE")

        for description, operation in (
            ("index create", create_index),
            ("index definition read", read_definition),
            ("search query", run_query),
            ("index drop", drop_index),
        ):
            self.assert_dual_client(
                "{0} (mode={1})".format(description, mode), operation,
                valid, revoked, expect_revoked_rejected=expect_rejected)

    def test_crl_endpoint_sweep(self):
        """Plan FTS-CLI-01/03, FTS-FUNC-04, FTS-1501 — bypass sweep."""
        index, valid, revoked, query = self.setup_enforcement()

        endpoints = {
            "list indexes": lambda ident: self.fts_list_indexes(ident),
            "index definition": lambda ident: self.fts_index_definition(
                ident, index.name),
            "search query": self.query_op(index, query),
            "node stats": lambda ident: self.fts_stats(ident),
            "api cfg": lambda ident: self.fts_api(ident, "/api/cfg"),
            "api stats": lambda ident: self.fts_api(ident, "/api/stats"),
            "manager meta": lambda ident: self.fts_api(
                ident, "/api/managerMeta"),
            "index count": lambda ident: self.fts_api(
                ident, "/api/index/{0}/count".format(index.name)),
            # A control endpoint, so the sweep covers a mutating POST too.
            # `resume` is the default state, so this is non-destructive.
            "ingest control": lambda ident: self.fts_api(
                ident, "/api/index/{0}/ingestControl/resume".format(index.name),
                method="POST"),
        }
        for name, operation in endpoints.items():
            self.assert_dual_client(name, operation, valid, revoked)

    def test_crl_enforcement_precedes_rbac(self):
        """Plan FTS-CLI-08 — revocation is enforced before identity and RBAC."""
        index = self.create_and_load_test_index()
        low_priv = self.create_client_identity(
            "lowpriv", roles="fts_searcher[default]")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")

        create_body = {"type": "fulltext-index", "sourceType": "couchbase",
                       "sourceName": "default", "params": {}}
        create_path = "/api/index/fts_crl_rbac_probe"
        query = self.default_query()

        # This 403 is the discriminator, not a nicety. With a *privileged*
        # identity both orderings look identical — RBAC-first would pass, then
        # revocation would refuse — so the test would prove refusal but not
        # ordering. It only discriminates because RBAC denies this identity
        # while un-revoked. If that baseline does not hold, the test is not
        # testing what it claims, so fail rather than warn.
        rbac_result = self.fts_api(low_priv, create_path, method="PUT",
                                   body=create_body)
        if rbac_result.outcome != OUTCOME_REJECTED_RBAC:
            self.fail(
                "Expected HTTP 403 for an under-privileged index create, got "
                "{0}. Without that baseline this test cannot distinguish "
                "revocation-before-RBAC from RBAC-before-revocation — both "
                "orderings would refuse a revoked cert. Fix the role/endpoint "
                "pairing so RBAC denies this identity while it is "
                "valid.".format(rbac_result))
        self.assert_allowed(self.fts_query(low_priv, index, query),
                            context="fts_searcher query before revocation")

        self.revoke(low_priv, crl_number=2)
        self.assert_revoked(
            self.wait_until_revoked(
                self.query_op(index, query), low_priv,
                context="fts_searcher query after revocation"),
            context="fts_searcher query after revocation")
        self.assert_revoked(
            self.fts_api(low_priv, create_path, method="PUT", body=create_body),
            context="under-privileged index create after revocation")

    def test_crl_no_user_mapping_and_missing_resource(self):
        """Plan FTS-CLI-08 (tail) — revocation outranks other failure classes."""
        index = self.create_and_load_test_index()
        self.enable_client_cert_auth()

        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "ftscrl-nouser")
        orphan = ClientIdentity(
            cn="ftscrl-nouser", cert=cert, key=key, serial=serial,
            cert_path=self._write_temp_pem(
                self.crl_utils.cert_to_pem(cert), suffix="-cert.pem"),
            key_path=self._write_temp_pem(
                self.crl_utils.key_to_pem(key), suffix="-key.pem"),
            ca_path=self.ca_path, username=None, password=None,
            ca_cert=self.ca_cert, ca_key=self.ca_key)

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")

        pre = self.fts_query(orphan, index, self.default_query())
        self.log.info("Unmapped-CN cert before revocation: {0}".format(pre))

        self.revoke(orphan, crl_number=2)
        self.assert_revoked(
            self.wait_until_revoked(
                lambda ident: self.fts_query(ident, index, self.default_query()),
                orphan, context="unmapped CN after revocation"),
            context="unmapped CN after revocation")

        self.assert_revoked(
            self.fts_query(orphan, "no_such_index_ftscrl", self.default_query()),
            context="revoked cert against nonexistent index")

    def test_crl_optional_mtls_no_password_fallback(self):
        """Plan FTS-CLI-04 / FTS-CLI-04b — the optional-mTLS bypass risk."""
        index, valid, revoked, query = self.setup_enforcement(
            client_cert_state="enable")

        admin_auth = (self.master.rest_username, self.master.rest_password)

        self.assert_allowed(
            self.fts_query(None, index, query, basic_auth=admin_auth),
            context="no client cert, password auth")

        self.assert_allowed(self.fts_query(valid, index, query),
                            context="valid cert, optional mTLS")
        self.assert_revoked(self.fts_query(revoked, index, query),
                            context="revoked cert, optional mTLS")

        self.assert_revoked(
            self.fts_query(revoked, index, query, basic_auth=admin_auth),
            context="revoked cert WITH valid Authorization header")

    def test_crl_mandatory_mtls(self):
        """Plan FTS-CLI-06 — mandatory mTLS."""
        index, valid, revoked, query = self.setup_enforcement(
            client_cert_state="mandatory")

        admin_auth = (self.master.rest_username, self.master.rest_password)

        no_cert = self.fts_query(None, index, query, basic_auth=admin_auth)
        if no_cert.allowed:
            self.fail("Mandatory mTLS accepted a request with no client "
                      "certificate: {0}".format(no_cert))
        self.log.info("Mandatory mTLS rejected the no-cert request via {0}".format(
            no_cert.outcome))

        self.assert_allowed(self.fts_query(valid, index, query),
                            context="valid cert, mandatory mTLS")
        self.assert_revoked(self.fts_query(revoked, index, query),
                            context="revoked cert, mandatory mTLS")

    def test_crl_disabled_mtls_never_engages(self):
        """Plan FTS-CLI-07 — with mTLS disabled, CRL clientAuth never engages."""
        index = self.create_and_load_test_index()
        revoked = self.create_client_identity("nomtls")
        self.rest.client_cert_auth(state="disable", prefixes=[])

        self.publish_crl(revoked, crl_number=1)
        self.set_crl_policy(client_auth="Require")

        query = self.default_query()
        admin_auth = (self.master.rest_username, self.master.rest_password)
        self.assert_allowed(
            self.fts_query(None, index, query, basic_auth=admin_auth),
            context="password auth with mTLS disabled and clientAuth=Require")
        self.log.info(
            "mTLS disabled: no cert exchange, so CRL clientAuth enforcement "
            "correctly never engaged.")

    def test_crl_concurrent_mixed_identities(self):
        """Plan FTS-CLI-09 — mixed valid/revoked identities under concurrency."""
        index = self.create_and_load_test_index()
        count = self._input.param("identity_count", 4)
        identities = self.create_identities(count * 2)
        valid_set, revoked_set = identities[:count], identities[count:]
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")

        query = self.default_query()
        expected_hits = self.assert_allowed(
            self.fts_query(valid_set[0], index, query),
            context="baseline before revocation").total_hits()

        self.revoke(revoked_set, crl_number=2)
        self.wait_until_revoked(
            self.query_op(index, query), revoked_set[0],
            context="first revoked identity")

        results = self.run_concurrently(
            self.query_op(index, query), identities)

        failures = []
        for identity in valid_set:
            result = results[identity]
            if not result.allowed or result.total_hits() != expected_hits:
                failures.append((identity.cn, "expected {0} hits, got {1}".format(
                    expected_hits, result)))
        for identity in revoked_set:
            result = results[identity]
            if not result.revoked:
                failures.append((identity.cn,
                                 "expected rejection, got {0}".format(result)))
        if failures:
            self.fail("Concurrent mixed-identity failures: {0}".format(failures))
        self.log.info(
            "{0} valid and {1} revoked identities behaved correctly "
            "concurrently".format(len(valid_set), len(revoked_set)))

    def test_crl_existing_connection_survives_revocation(self):
        """Plan FTS-CLI-05 — documented v1 behaviour for live sessions."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("session")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")

        node = self.fts_nodes[0]
        url = "https://{0}:{1}{2}".format(
            node.ip, CbServer.ssl_fts_port, self.query_path_for_index(index))
        body = {"query": self.default_query(), "size": 10}

        session = requests.Session()
        session.cert = identity.cert_tuple
        session.verify = False
        try:
            first = session.post(url, json=body, timeout=60)
            if not first.ok:
                self.fail("Keep-alive session failed before revocation: "
                          "{0} {1}".format(first.status_code, first.content))

            self.revoke(identity, crl_number=2)
            self.assert_revoked(
                self.wait_until_revoked(
                    lambda ident: self.fts_query(ident, index, self.default_query()),
                    identity, context="new connection after revocation"),
                context="new connection after revocation")

            try:
                second = session.post(url, json=body, timeout=60)
                reused_ok = second.ok
            except requests.exceptions.RequestException as exc:
                reused_ok = False
                self.log.info("Established session raised after revocation: "
                              "{0}".format(exc))
            self.log.info(
                "V1 BEHAVIOUR: established FTS connection {0} after revocation "
                "(plan FTS-CLI-05 documents survival; active-session "
                "termination is a future phase).".format(
                    "survived" if reused_ok else "was terminated"))
        finally:
            session.close()

    def test_crl_policy_mode_matrix(self):
        """Plan FTS-MODE-01..05 / §4A — one matrix cell per invocation."""
        mode = self.crl_client_auth_mode
        crl_state = self._input.param("crl_state", "revoked")

        index = self.create_and_load_test_index()
        valid, revoked = self.create_dual_identities()
        self.enable_client_cert_auth()

        applicable = self._setup_crl_state(crl_state, revoked)
        self.set_crl_policy(client_auth=mode)

        query = self.default_query()
        operation = self.query_op(index, query)

        if mode == "Disabled":
            expect_rejected = False
        elif crl_state == "revoked":
            expect_rejected = True
        elif crl_state == "valid":
            expect_rejected = False
        else:  # missing / expired / invalid -> "undetermined"
            expect_rejected = (mode == "Require")

        self.log.info("Policy matrix cell: mode={0} crl_state={1} -> "
                      "expect_revoked_rejected={2} (applicable CRL={3})".format(
                          mode, crl_state, expect_rejected, applicable))

        if not applicable and mode == "Require":
            for identity, label in ((valid, "valid"), (revoked, "revoked")):
                self.assert_revoked(
                    operation(identity),
                    context="{0} identity, Require + crl_state={1}".format(
                        label, crl_state))
            return

        self.assert_dual_client(
            "policy matrix mode={0} crl_state={1}".format(mode, crl_state),
            operation, valid, revoked, expect_revoked_rejected=expect_rejected)

    def _setup_crl_state(self, crl_state, revoked_identity):
        """Put the cluster into the requested CRL state."""
        if crl_state == "missing":
            return False

        if crl_state == "valid":
            self.publish_crl([], crl_number=1)
            return True

        if crl_state == "revoked":
            self.publish_crl(revoked_identity, crl_number=1)
            return True

        if crl_state == "expired":
            self.set_allow_expired_crls(True)
            self.publish_crl(revoked_identity, crl_number=1, expired=True)
            return False

        if crl_state == "invalid":
            pem = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[revoked_identity.serial], crl_number=1)
            tampered = self.tamper_crl_pem(pem)
            status, _, _ = self.rest.upload_crl_file(DEFAULT_CRL_FILE, tampered)
            if status:
                self.track_crl_file(DEFAULT_CRL_FILE)
                self.reload_crl_all_nodes()
                self.log.info("Tampered CRL was accepted at upload; relying on "
                              "load-time validation to mark it invalid.")
            else:
                self.log.info("Tampered CRL rejected at upload, as expected — "
                              "cluster has no applicable CRL.")
            return False

        self.fail("Unknown crl_state: {0}".format(crl_state))

    def test_crl_policy_transitions(self):
        """Plan FTS-MODE-06 — live policy transitions with no cbft restart."""
        index = self.create_and_load_test_index()
        valid, revoked = self.create_dual_identities()
        self.enable_client_cert_auth()

        self.publish_crl(revoked, crl_number=1)
        query = self.default_query()
        operation = self.query_op(index, query)

        self.set_crl_policy(client_auth="Disabled")
        self.assert_allowed(
            self.wait_until_allowed(operation, revoked,
                                    context="revoked cert while Disabled"),
            context="revoked cert while Disabled")

        self.set_crl_policy(client_auth="Require")
        self.assert_revoked(
            self.wait_until_revoked(operation, revoked,
                                    context="after Disabled -> Require"),
            context="after Disabled -> Require")
        self.assert_allowed(operation(valid),
                            context="valid cert under Require")

        self.set_crl_policy(client_auth="Disabled")
        self.assert_allowed(
            self.wait_until_allowed(operation, revoked,
                                    context="after Require -> Disabled"),
            context="after Require -> Disabled")

    def test_crl_scope_independence(self):
        """Plan FTS-MODE-07 / FTS-N2N-04 — the two scopes are independent."""
        index, valid, revoked, query = self.setup_enforcement(node_to_node="Disabled")

        expected = self.assert_allowed(
            self.fts_query(valid, index, query),
            context="valid cert, clientAuth=Require").total_hits()
        self.assert_revoked(self.fts_query(revoked, index, query),
                            context="revoked cert, clientAuth=Require")

        self.wait_for_indexing_complete()
        self.assert_index_complete(
            index, context="index health with clientAuth=Require, nodeToNode=Disabled")
        self.assert_allowed(self.fts_query(valid, index, query),
                            expected_hits=expected,
                            context="post-check distributed query")

    def test_crl_error_classification(self):
        """Plan FTS-ERR-01/03 — distinct, diagnosable failure classes."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("errclass")
        self.enable_client_cert_auth()
        self.set_crl_policy(client_auth="Require")

        query = self.default_query()
        operation = self.query_op(index, query)
        observations = {}

        self.publish_crl(identity, crl_number=1)
        observations["revoked"] = self.wait_until_revoked(
            operation, identity, context="revoked")

        self.delete_crl(DEFAULT_CRL_FILE)
        observations["missing_crl"] = self.wait_until_revoked(
            operation, identity, context="missing CRL")

        self.set_allow_expired_crls(True)
        self.publish_crl([], filename="fts_crl_expired.pem", crl_number=2,
                         expired=True)
        observations["expired_crl"] = self.wait_until_revoked(
            operation, identity, context="expired CRL")
        self.delete_crl("fts_crl_expired.pem")
        self.set_allow_expired_crls(False)

        untrusted_cert, untrusted_key = self.make_untrusted_ca()
        untrusted_pem = self.crl_utils.build_crl(
            untrusted_cert, untrusted_key, revoked_serials=[], crl_number=1)
        upload_error = self.upload_crl_expecting_failure(
            "fts_crl_untrusted.pem", untrusted_pem)
        observations["untrusted_issuer_upload"] = upload_error

        for name, value in observations.items():
            if hasattr(value, "outcome"):
                self.log.info("FAILURE CLASS {0}: outcome={1} error={2}".format(
                    name, value.outcome, value.error_text()[:200]))
            else:
                self.log.info("FAILURE CLASS {0}: {1}".format(name, value))

        for name in ("revoked", "missing_crl", "expired_crl"):
            self.assert_revoked(observations[name],
                                context="failure class {0}".format(name))

    def test_crl_logs_classify_and_do_not_leak(self):
        """Plan FTS-OBS-02/03 — cbft logs are useful and non-sensitive."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("logscan")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")
        self.assert_allowed(self.fts_query(identity, index, self.default_query()),
                            context="baseline before log capture")

        baselines = self.fts_log_baselines()
        self.revoke(identity, crl_number=2)
        self.wait_until_revoked(
            lambda ident: self.fts_query(ident, index, self.default_query()),
            identity, context="revocation before log read")
        for _ in range(3):
            self.fts_query(identity, index, self.default_query())

        log_text = self.fts_log_since(baselines)
        self.log.info("Captured {0} chars of new fts.log output".format(
            len(log_text)))

        self.assert_no_sensitive_material(
            log_text, identities=[identity], context="fts.log after revocation")

        hits = [term for term in ("crl", "revoked", "revocation", "certificate")
                if term in log_text.lower()]
        self.log.info("fts.log revocation-related terms present: {0}".format(
            hits or "none — logging may need a dev follow-up"))

    # ──────────────────────────────────────────────────────────────────
    # Lifecycle, diagnostics and index operations
    # ──────────────────────────────────────────────────────────────────

    def test_crl_hot_reload_no_restart(self):
        """Plan FTS-LC-01/04 — revocation applies with no cbft restart."""
        index, valid, revoked, query = self.setup_enforcement(
            revoke=False)
        operation = self.query_op(index, query)

        pids_before = self.cbft_pids()
        self.assert_allowed(operation(revoked), context="before revocation")

        self.revoke(revoked, crl_number=2)
        self.assert_revoked(
            self.wait_until_revoked(operation, revoked,
                                    context="after hot reload"),
            context="after hot reload")
        self.assert_allowed(operation(valid),
                            context="valid identity unaffected by reload")

        self.assert_cbft_not_restarted(pids_before, context="CRL hot reload")

    def test_crl_update_add_and_remove_serial(self):
        """Plan FTS-LC-04 — adding then removing a serial flips access both ways."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("update")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()
        operation = self.query_op(index, query)

        expected = self.assert_allowed(
            operation(identity), context="initial access").total_hits()

        self.revoke(identity, crl_number=2)
        self.assert_revoked(
            self.wait_until_revoked(operation, identity, context="serial added"),
            context="serial added")

        self.publish_crl([], crl_number=3)
        restored = self.wait_until_allowed(
            operation, identity, context="serial removed")
        self.assert_allowed(restored, expected_hits=expected,
                            context="serial removed — no stale revoked state")

    def test_crl_delete_only_applicable_crl_fails_closed(self):
        """Plan FTS-LC-05 — the sharpened delete edge case."""
        index = self.create_and_load_test_index()
        valid, revoked = self.create_dual_identities()
        self.enable_client_cert_auth()

        self.publish_crl(revoked, crl_number=1)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()

        self.assert_revoked(self.fts_query(revoked, index, query),
                            context="revoked before delete")
        self.assert_allowed(self.fts_query(valid, index, query),
                            context="valid before delete")

        self.delete_crl(DEFAULT_CRL_FILE)

        self.assert_revoked(
            self.wait_until_revoked(
                self.query_op(index, query), valid,
                context="valid identity after deleting the only CRL"),
            context="valid identity after deleting the only CRL")
        self.assert_revoked(self.fts_query(revoked, index, query),
                            context="revoked identity after deleting the only CRL")

        self.set_crl_policy(client_auth="Permissive")
        self.assert_allowed(
            self.wait_until_allowed(
                self.query_op(index, query), valid,
                context="Permissive with no applicable CRL"),
            context="Permissive with no applicable CRL")

    def test_crl_metadata_and_listing(self):
        """Plan FTS-LC-06 — CRL metadata surfaces correctly."""
        identity = self.create_client_identity("metadata")

        pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[identity.serial],
            crl_number=7)
        self.upload_crl("fts_crl_pem.pem", pem)

        der = x509.load_pem_x509_crl(pem).public_bytes(serialization.Encoding.DER)
        self.upload_crl("fts_crl_der.crl", der)

        no_number = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[], crl_number=None)
        self.upload_crl("fts_crl_nonumber.pem", no_number)

        files = self.list_crl_files()
        by_name = {entry.get("filename"): entry for entry in files or []}
        self.log.info("CRL listing: {0}".format(by_name.keys()))

        for filename in ("fts_crl_pem.pem", "fts_crl_der.crl",
                         "fts_crl_nonumber.pem"):
            if filename not in by_name:
                self.fail("Uploaded CRL {0} missing from the listing: {1}".format(
                    filename, list(by_name)))
            entry = by_name[filename]
            for field in ("checksum", "uploadTimestamp", "entries"):
                if field not in entry:
                    self.fail("CRL listing entry for {0} lacks {1!r}: {2}".format(
                        filename, field, entry))
            for crl_entry in entry.get("entries") or []:
                for field in ("issuer", "thisUpdate", "nextUpdate", "crlNumber"):
                    if field not in crl_entry:
                        self.fail("CRL entry for {0} lacks {1!r}: {2}".format(
                            filename, field, crl_entry))

        for crl_entry in by_name["fts_crl_nonumber.pem"].get("entries") or []:
            if crl_entry.get("crlNumber") is not None:
                self.fail("Expected crlNumber=null for a CRL with no CRLNumber "
                          "extension, got {0!r}".format(crl_entry.get("crlNumber")))
        self.log.info("crlNumber correctly null when the extension is absent")

    def test_crl_untrusted_and_tampered_rejected(self):
        """Plan FTS-LC-07 tail / security — bad CRLs never take effect."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("badcrl")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()
        expected = self.assert_allowed(
            self.fts_query(identity, index, query),
            context="baseline before bad CRLs").total_hits()

        untrusted_cert, untrusted_key = self.make_untrusted_ca()
        untrusted_pem = self.crl_utils.build_crl(
            untrusted_cert, untrusted_key, revoked_serials=[identity.serial],
            crl_number=1)
        self.upload_crl_expecting_failure("fts_crl_untrusted.pem", untrusted_pem)

        good = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[identity.serial],
            crl_number=5)
        tampered = self.tamper_crl_pem(good)
        status, _, _ = self.rest.upload_crl_file("fts_crl_tampered.pem", tampered)
        if status:
            self.track_crl_file("fts_crl_tampered.pem")
            self.log.info("Tampered CRL accepted at upload; it must still not "
                          "revoke the identity.")
            self.reload_crl_all_nodes()
        else:
            self.log.info("Tampered CRL rejected at upload, as expected.")

        self.assert_allowed(self.fts_query(identity, index, query),
                            expected_hits=expected,
                            context="access unaffected by untrusted/tampered CRLs")

    def test_crl_intermediate_chain_and_check_flag(self):
        """Plan FTS-LC-07 — checkIntermediateCerts semantics."""
        index = self.create_and_load_test_index()

        intermediate_cert, intermediate_key = self.generate_intermediate_ca(
            self.ca_cert, self.ca_key, self.ca_cn("FTSCRLIntermediateCA"))
        intermediate_pem = self.crl_utils.cert_to_pem(intermediate_cert)
        self.trust_ca_on_all_nodes(intermediate_cert)

        leaf = self.create_client_identity(
            "interleaf", ca_cert=intermediate_cert, ca_key=intermediate_key,
            chain_pem=intermediate_pem)
        self.enable_client_cert_auth()

        self.publish_crl([], filename="fts_crl_inter.pem", crl_number=1,
                         ca_cert=intermediate_cert, ca_key=intermediate_key)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()
        operation = self.query_op(index, query)

        expected = self.assert_allowed(
            self.wait_until_allowed(operation, leaf,
                                    context="leaf via intermediate, baseline"),
            context="leaf via intermediate, baseline").total_hits()

        intermediate_serial = intermediate_cert.serial_number
        self.publish_crl(intermediate_serial, filename="fts_crl_root.pem",
                         crl_number=1)

        self.set_crl_settings({"checkIntermediateCerts": False})
        self.reload_crl_all_nodes()
        self.assert_allowed(
            self.wait_until_allowed(
                operation, leaf,
                context="revoked intermediate, checkIntermediateCerts=False"),
            expected_hits=expected,
            context="revoked intermediate, checkIntermediateCerts=False")

        self.set_crl_settings({"checkIntermediateCerts": True})
        self.reload_crl_all_nodes()
        self.assert_revoked(
            self.wait_until_revoked(
                operation, leaf,
                context="revoked intermediate, checkIntermediateCerts=True"),
            context="revoked intermediate, checkIntermediateCerts=True")

        self.set_crl_settings({"checkIntermediateCerts": False})
        self.publish_crl([], filename="fts_crl_root.pem", crl_number=2)
        self.wait_until_allowed(operation, leaf, context="leaf restored")
        self.publish_crl(leaf, filename="fts_crl_inter.pem", crl_number=2,
                         ca_cert=intermediate_cert, ca_key=intermediate_key)
        self.assert_revoked(
            self.wait_until_revoked(operation, leaf, context="leaf revoked"),
            context="leaf revoked by its own issuer's CRL")

    def test_crl_multi_ca_no_cross_revocation(self):
        """Plan FTS-LC-07 / FTS-705 — CRLs are matched by issuer, not applied globally."""
        index = self.create_and_load_test_index()

        ca_b_cert, ca_b_key = self.crl_utils.generate_ca(self.ca_cn("FTSCRLTestCA-B"))
        self.trust_ca_on_all_nodes(ca_b_cert)

        identity_a = self.create_client_identity("caA")
        identity_b = self.create_client_identity(
            "caB", ca_cert=ca_b_cert, ca_key=ca_b_key)
        self.enable_client_cert_auth()

        self.publish_crl([], filename="fts_crl_ca_a.pem", crl_number=1)
        self.publish_crl([], filename="fts_crl_ca_b.pem", crl_number=1,
                         ca_cert=ca_b_cert, ca_key=ca_b_key)
        self.set_crl_policy(client_auth="Require")

        query = self.default_query()
        expected_a = self.assert_allowed(
            self.wait_until_allowed(self.query_op(index, query), identity_a,
                                    context="CA-A identity baseline"),
            context="CA-A identity baseline").total_hits()
        expected_b = self.assert_allowed(
            self.wait_until_allowed(self.query_op(index, query), identity_b,
                                    context="CA-B identity baseline"),
            context="CA-B identity baseline").total_hits()

        self.publish_crl(identity_a, filename="fts_crl_ca_a.pem", crl_number=2)
        self.assert_revoked(
            self.wait_until_revoked(
                self.query_op(index, query), identity_a,
                context="CA-A identity revoked"),
            context="CA-A identity revoked")
        self.assert_allowed(self.fts_query(identity_b, index, query),
                            expected_hits=expected_b,
                            context="CA-B identity unaffected by CA-A's CRL")

        self.publish_crl(identity_b, filename="fts_crl_ca_b.pem", crl_number=2,
                         ca_cert=ca_b_cert, ca_key=ca_b_key)
        self.assert_revoked(
            self.wait_until_revoked(
                self.query_op(index, query), identity_b,
                context="CA-B identity revoked"),
            context="CA-B identity revoked")

        self.publish_crl([], filename="fts_crl_ca_a.pem", crl_number=3)
        self.assert_allowed(
            self.wait_until_allowed(
                self.query_op(index, query), identity_a,
                context="CA-A identity restored"),
            expected_hits=expected_a, context="CA-A identity restored")

    def test_crl_key_algorithm_coverage(self):
        """Plan FTS-711 — RSA-2048 and ECDSA-P256 enforce identically."""
        index = self.create_and_load_test_index()
        self.enable_client_cert_auth()
        query = self.default_query()

        for algorithm in sorted(KEY_ALGORITHMS):
            ca_cert, ca_key = self.crl_utils.generate_ca(
                self.ca_cn("FTSCRLTestCA-{0}".format(algorithm)),
                key_algorithm=algorithm)
            self.trust_ca_on_all_nodes(ca_cert)

            valid = self.create_client_identity(
                "valid-{0}".format(algorithm), key_algorithm=algorithm,
                ca_cert=ca_cert, ca_key=ca_key)
            revoked = self.create_client_identity(
                "revoked-{0}".format(algorithm), key_algorithm=algorithm,
                ca_cert=ca_cert, ca_key=ca_key)

            crl_file = "fts_crl_{0}.pem".format(algorithm)
            self.publish_crl([], filename=crl_file, crl_number=1,
                             ca_cert=ca_cert, ca_key=ca_key)
            self.set_crl_policy(client_auth="Require")
            self.publish_crl(revoked, filename=crl_file, crl_number=2,
                             ca_cert=ca_cert, ca_key=ca_key)

            self.assert_dual_client(
                "key algorithm {0}".format(algorithm),
                self.query_op(index, query),
                valid, revoked)

    def test_crl_dir_poll_picks_up_changes(self):
        """Plan FTS-LC-03/405 — the directory poller applies changes unaided."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("dirpoll")
        self.enable_client_cert_auth()

        poll_ms = 2000
        self.set_crl_settings({"dirPollIntervalMs": poll_ms})
        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()
        operation = self.query_op(index, query)
        self.assert_allowed(operation(identity), context="before revocation")

        self.publish_crl(identity, crl_number=2, reload_now=False)
        self.assert_revoked(
            self.wait_until_revoked(
                operation, identity,
                timeout=max(self.crl_enforcement_timeout, (poll_ms // 1000) * 6),
                context="revocation via directory poll only"),
            context="revocation via directory poll only")

    def test_crl_settings_round_trip_and_validation(self):
        """Plan FTS-LC / §7 — settings round-trip and reject bad input."""
        original = self.get_crl_settings()
        self.log.info("Initial CRL settings: {0}".format(original))

        merged = self.set_crl_settings({
            "checkIntermediateCerts": True,
            "dirPollIntervalMs": 5000,
        })
        self.crl_utils.assert_settings_equal(
            merged, {"checkIntermediateCerts": True, "dirPollIntervalMs": 5000})

        self.set_crl_policy(client_auth="Permissive")
        after = self.set_crl_policy(node_to_node="Permissive")
        policy = after.get("policyPerScope", {})
        if policy.get("clientAuth") != "Permissive":
            self.fail("Partial policy update clobbered clientAuth: {0}".format(policy))

        for bad in ({"dirPollIntervalMs": 10},
                    {"dirPollIntervalMs": 99999999},
                    {"urlPollIntervalMs": 10},
                    {"policyPerScope": {"clientAuth": "Strict"}},
                    {"policyPerScope": {"bogusScope": "Require"}},
                    {"totallyUnknownField": True}):
            error = self.set_crl_settings_expecting_failure(bad)
            self.log.info("Rejected {0} -> {1}".format(bad, error))

        self.log.info(
            "Confirmed 'Strict' is rejected — only Disabled/Permissive/Require "
            "exist (plan §15).")

        self.set_crl_settings({
            "checkIntermediateCerts": original.get("checkIntermediateCerts", False),
            "dirPollIntervalMs": original.get("dirPollIntervalMs", 60000),
        })

    def test_crl_reload_is_node_local_then_uniform(self):
        """Plan FTS-LC-12 — reloadCrl is node-local; all-node reload is uniform."""
        if len(self.fts_nodes) < 2:
            self.skipTest("Needs >= 2 FTS nodes; got {0}".format(len(self.fts_nodes)))

        index, valid, revoked, query = self.setup_enforcement(
            revoke=False)

        self.publish_crl(revoked, crl_number=2, reload_now=False)
        self.reload_crl_all_nodes(servers=[self.fts_nodes[0]])

        per_node = {}
        for node in self.fts_nodes:
            result = self.fts_query(revoked, index, query, node=node)
            per_node[node.ip] = result.outcome
        self.log.info("After single-node reload, per-node outcomes: {0}".format(
            per_node))

        self.reload_crl_all_nodes()
        self.assert_crl_consistent_across_nodes()
        for node in self.fts_nodes:
            self.assert_dual_client(
                "query on node {0} after all-node reload".format(node.ip),
                self.query_op(index, query, node=node),
                valid, revoked)

    def test_crl_multi_node_consistency(self):
        """Plan FTS-OPS-09 — no node-dependent bypass."""
        if len(self.fts_nodes) < 2:
            self.skipTest("Needs >= 2 FTS nodes; got {0}".format(len(self.fts_nodes)))

        index, valid, revoked, query = self.setup_enforcement()
        self.assert_crl_consistent_across_nodes()

        for node in self.fts_nodes:
            self.assert_dual_client(
                "query on node {0}".format(node.ip),
                self.query_op(index, query, node=node),
                valid, revoked)

    def test_crl_diagnostics_policy_spelling(self):
        """Plan FTS-DIAG-05 / OQ-E — settle "Require" vs "Required"."""
        self.publish_crl([], crl_number=1)
        accepted = self.resolve_diagnostics_policy_spelling()
        self.log.info(
            "OQ-E RESOLVED: diagnostics/validate accepts {0}. "
            "RestConnection.post_diagnostics_validate currently sends "
            "'Require'.".format(accepted))
        if "Require" not in accepted:
            self.fail(
                "diagnostics/validate rejects 'Require' but accepts {0} — "
                "RestConnection.post_diagnostics_validate needs fixing for all "
                "component suites.".format(accepted))

        status, content = self.diagnostics_validate(
            policy="Disabled", expect_success=False)
        if status:
            self.log.warning("diagnostics/validate accepted policy=Disabled, "
                             "which CRL_INFO.md documents as rejected: "
                             "{0}".format(content))

    def test_crl_diagnostics_supplied_certs(self):
        """Plan FTS-DIAG-01/03 — supplied-cert statuses and the root exemption."""
        identity = self.create_client_identity("diag")
        self.publish_crl(identity, crl_number=1)

        cert_pem = self.crl_utils.cert_to_pem(identity.cert).decode()
        root_pem = self.crl_utils.cert_to_pem(self.ca_cert).decode()

        _, revoked_body = self.diagnostics_validate(certs=[cert_pem])
        statuses = self.diag_statuses(revoked_body)
        self.log.info("Revoked cert diagnostics: {0}".format(revoked_body))
        for status_value in statuses:
            if status_value not in DIAGNOSTIC_STATUS_VALUES:
                self.fail("Undocumented diagnostics status {0!r}; expected one "
                          "of {1}".format(status_value, DIAGNOSTIC_STATUS_VALUES))
        if "revoked" not in statuses:
            self.fail("Expected 'revoked' for a revoked cert, got {0}".format(
                statuses))

        _, root_body = self.diagnostics_validate(certs=[root_pem])
        self.log.info("Self-signed root diagnostics: {0}".format(root_body))
        root_statuses = self.diag_statuses(root_body)
        if root_statuses and set(root_statuses) != {"valid"}:
            self.fail("Self-signed root should be 'valid' (not CRL-checked), "
                      "got {0}".format(root_statuses))
        details = " ".join(self.diag_details(root_body)).lower()
        if "self-signed" not in details:
            self.log.warning(
                "Self-signed root detail text did not mention 'self-signed': "
                "{0!r}. Details are free-form, so this is informational.".format(
                    details))

        _, bad_body = self.diagnostics_validate(
            certs=["-----BEGIN CERTIFICATE-----\nnot-a-cert\n"
                   "-----END CERTIFICATE-----\n"],
            expect_success=False)
        self.log.info("Malformed chain diagnostics: {0}".format(bad_body))

    def test_crl_diagnostics_agrees_with_runtime(self):
        """Plan FTS-DIAG-04 — diagnostics and runtime share one evaluator."""
        index = self.create_and_load_test_index()
        valid, revoked = self.create_dual_identities()
        self.enable_client_cert_auth()
        self.publish_crl(revoked, crl_number=1)

        query = self.default_query()
        mismatches = []
        for mode in ("Permissive", "Require"):
            self.set_crl_policy(client_auth=mode)
            for identity, label in ((valid, "valid"), (revoked, "revoked")):
                cert_pem = self.crl_utils.cert_to_pem(identity.cert).decode()
                _, body = self.diagnostics_validate(certs=[cert_pem], policy=mode)
                statuses = self.diag_statuses(body)
                runtime = self.fts_query(identity, index, query)

                diag_says_revoked = "revoked" in statuses
                runtime_says_revoked = runtime.revoked
                self.log.info(
                    "mode={0} identity={1}: diagnostics={2} runtime={3}".format(
                        mode, label, statuses, runtime.outcome))
                if diag_says_revoked != runtime_says_revoked:
                    mismatches.append({
                        "mode": mode, "identity": label,
                        "diagnostics": statuses,
                        "runtime": runtime.outcome,
                    })
        if mismatches:
            self.fail("diagnostics/validate disagrees with runtime FTS "
                      "enforcement: {0}".format(mismatches))

    def test_crl_diagnostics_status_per_node(self):
        """Plan FTS-LC-10 / FTS-DIAG-06 — per-node, per-file status reporting."""
        identity = self.create_client_identity("diagstatus")
        self.publish_crl(identity, crl_number=1)

        _, body = self.diagnostics_status()
        self.log.info("diagnostics/status: {0}".format(body))
        if not body:
            self.fail("diagnostics/status returned nothing after a CRL upload")

        for host, node_entry in body.items():
            error = self.node_error(node_entry)
            if error:
                self.fail("Node {0} errored unexpectedly: {1}".format(host, error))
            for entry in self.crl_files(node_entry):
                cache_status = entry.get("cacheStatus")
                if cache_status not in CACHE_STATUS_VALUES:
                    self.fail("Undocumented cacheStatus {0!r} on {1}; expected "
                              "one of {2}".format(cache_status, host,
                                                  CACHE_STATUS_VALUES))
                reload_result = (entry.get("lastReload") or {}).get("result")
                if reload_result is not None and \
                        reload_result not in RELOAD_RESULT_VALUES:
                    self.fail("Undocumented lastReload.result {0!r} on {1}; "
                              "expected one of {2}".format(
                                  reload_result, host, RELOAD_RESULT_VALUES))
            poll = self.poll_directory(node_entry)
            if poll:
                self.log.info("{0} pollDirectory: directory={1} status={2} "
                              "errors={3}".format(
                                  host, poll.get("directory"),
                                  poll.get("status"), poll.get("errors")))
        self.log.info("Observed CRL sources: {0}".format(self.crl_sources(body)))

        status, error = self.diagnostics_status(
            nodes=["definitely-not-a-node:8091"], expect_success=False)
        if status:
            self.log.warning("diagnostics/status accepted an unknown hostname: "
                             "{0}".format(error))
        else:
            self.log.info("Unknown hostname correctly rejected: {0}".format(error))

    def test_crl_generated_source_enforced(self):
        """Plan FTS-LC-11 — the auto-generated CRL for the built-in CA."""
        _, body = self.diagnostics_status()
        sources = self.crl_sources(body)
        self.log.info("CRL sources present: {0}".format(sources))

        if "generated" not in sources:
            self.skipTest(
                "No 'generated' CRL source present on this cluster — the "
                "built-in-CA CRL may not be auto-generated in this build. "
                "Sources seen: {0}".format(sources))

        checked = 0
        for node_entry in (body or {}).values():
            for entry in self.crl_files(node_entry):
                if entry.get("source") == "generated":
                    self.crl_utils.assert_diagnostics_entry(
                        entry, expected_source="generated")
                    checked += 1
        if not checked:
            self.fail("'generated' appeared in the source set but no entry "
                      "could be read back: {0}".format(body))
        self.log.info("Validated {0} generated-source CRL entr(ies)".format(
            checked))

    def test_crl_revocation_mid_build(self):
        """Plan FTS-IDX-02 — revoking a client cert mid-build is harmless."""
        valid, revoked = self.create_dual_identities()
        self.enable_client_cert_auth()
        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")

        index = self.create_and_load_test_index(index_name="fts_crl_midbuild",
                                                load=False)
        self.load_data()
        self.revoke(revoked, crl_number=2)
        self.wait_for_indexing_complete()

        expected_docs = self.expected_doc_count()
        self.assert_index_doc_count(
            index, expected_docs, context="build across a mid-flight revocation")

        query = self.default_query()
        self.assert_dual_client(
            "query after mid-build revocation",
            self.query_op(index, query), valid, revoked)

    def test_crl_alias_enforcement(self):
        """Plan FTS-IDX-04 — aliases enforce identically to direct queries."""
        index, valid, revoked, query = self.setup_enforcement()

        alias_name = "fts_crl_alias"
        alias_body = {
            "type": "fulltext-alias",
            "sourceType": "nil",
            "params": {"targets": {index.name: {}}},
        }
        created = self.fts_api(valid, "/api/index/{0}".format(alias_name),
                               method="PUT", body=alias_body)
        self.assert_allowed(created, context="alias creation with a valid cert")

        direct = self.assert_allowed(
            self.fts_query(valid, index, query), context="direct query")
        self.assert_dual_client(
            "query through alias",
            self.query_op(alias_name, query),
            valid, revoked, expected_hits=direct.total_hits())

        self.fts_api(valid, "/api/index/{0}".format(alias_name), method="DELETE")

    def test_crl_index_convergence_after_incident(self):
        """Plan FTS-IDX-11 — the data-integrity guard."""
        index = self.create_and_load_test_index(index_name="fts_crl_converge")
        identity = self.create_client_identity("converge")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()
        operation = self.query_op(index, query)
        self.assert_allowed(operation(identity), context="before the incident")

        self.delete_crl(DEFAULT_CRL_FILE)
        self.wait_until_revoked(operation, identity, context="during the incident")

        self.load_data()

        self.publish_crl([], crl_number=2)
        self.assert_allowed(
            self.wait_until_allowed(operation, identity,
                                    context="after the incident"),
            context="after the incident")

        self.wait_for_indexing_complete()
        expected_docs = self.expected_doc_count()
        self.assert_index_doc_count(
            index, expected_docs,
            context="index convergence after a revocation incident")

        final = self.assert_allowed(
            self.fts_query(identity, index, {"match_all": {}}, size=0),
            context="match_all after convergence")
        self.log.info("match_all total_hits after convergence: {0} "
                      "(source bucket docs: {1})".format(
                          final.total_hits(), expected_docs))

    # ──────────────────────────────────────────────────────────────────
    # nodeToNode scope (requires node-to-node encryption)
    # ──────────────────────────────────────────────────────────────────

    def test_crl_node_to_node_with_encryption(self):
        """Plan FTS-N2N-01/09 — nodeToNode enforcement with n2n encryption on.

        Pass ntonencrypt=enable and ntonencrypt_level=control|all|strict; the
        FTS base enables it during setUp. With valid node certs the cluster must
        stay healthy: DCP feeds up, the index complete, distributed queries
        correct. This is the positive half — revoking a live node certificate is
        deliberately left out until dev confirms the blast radius (plan OQ-D),
        since it can isolate a node.
        """
        level = self._input.param("ntonencrypt_level", "control")
        index, valid, revoked, query = self.setup_enforcement(
            node_to_node=self.crl_node_to_node_mode)

        self.log.info("nodeToNode={0} with n2n encryption level={1}".format(
            self.crl_node_to_node_mode, level))

        # The data path must be unaffected by nodeToNode enforcement.
        self.wait_for_indexing_complete()
        self.assert_index_complete(
            index, context="index under nodeToNode={0}, n2n={1}".format(
                self.crl_node_to_node_mode, level))

        # Distributed queries must be correct on every FTS node.
        expected = self.assert_allowed(
            self.fts_query(valid, index, query),
            context="baseline under nodeToNode enforcement").total_hits()
        for node in self.fts_nodes:
            self.assert_allowed(
                self.fts_query(valid, index, query, node=node),
                expected_hits=expected,
                context="query on {0} under nodeToNode enforcement".format(node.ip))

        # clientAuth still discriminates independently of nodeToNode.
        self.assert_revoked(self.fts_query(revoked, index, query),
                            context="clientAuth still enforced alongside nodeToNode")
        self.assert_crl_consistent_across_nodes()

    def test_crl_known_scope_quirks(self):
        """Plan §0.2 — the two documented clientAuth/nodeToNode quirks.

        Quirk A: clientAuth=Disabled + nodeToNode=Require — FTS-to-FTS over the
        clientAuth-scoped 18094 endpoint still succeeds.
        Quirk B: clientAuth=Require + nodeToNode=Disabled — that same hop can be
        refused, which for FTS degrades distributed queries rather than only an
        admin path.

        Both are assert-and-document, not bugs. Records the observed behaviour
        so a change in either direction is visible.
        """
        index, valid, revoked, query = self.setup_enforcement(
            mode="Disabled", node_to_node="Require")

        self.wait_for_indexing_complete()
        quirk_a = self.fts_query(valid, index, query)
        self.log.info(
            "QUIRK A (clientAuth=Disabled, nodeToNode=Require): query outcome "
            "{0} — the 18094 endpoint is clientAuth-scoped, so this is expected "
            "to succeed.".format(quirk_a.outcome))
        self.assert_index_complete(index, context="index under quirk A")

        self.set_crl_policy(client_auth="Require", node_to_node="Disabled")
        quirk_b_valid = self.fts_query(valid, index, query)
        quirk_b_revoked = self.fts_query(revoked, index, query)
        self.log.info(
            "QUIRK B (clientAuth=Require, nodeToNode=Disabled): valid={0} "
            "revoked={1}".format(quirk_b_valid.outcome, quirk_b_revoked.outcome))

        # Whatever the quirks do to internal hops, clientAuth must still
        # discriminate — that is the part which is not allowed to regress.
        self.assert_allowed(quirk_b_valid, context="quirk B, valid identity")
        self.assert_revoked(quirk_b_revoked, context="quirk B, revoked identity")


    # ──────────────────────────────────────────────────────────────────
    # Node-certificate revocation — FTS as the revoked provider (MB-73610)
    # ──────────────────────────────────────────────────────────────────

    def test_crl_query_to_fts_blocked_when_fts_node_cert_revoked(self):
        """MB-73610 analogue — a consumer must not reach an FTS node whose cert is revoked.

        MB-73610 revoked a dedicated FTS node's certificate under
        nodeToNode=Require and found Eventing's couchbase.searchQuery() kept
        succeeding. This is the same shape with Query as the consumer: N1QL
        SEARCH() reaches FTS over an internal, nodeToNode-scoped connection, so
        once the FTS node's own certificate is revoked that hop must fail.

        Deliberately drives the consumer, not the FTS REST port. A direct
        client-cert query would exercise clientAuth; the property here is that
        a *peer service* stops trusting a revoked node.
        """
        if not self._cb_cluster.get_random_n1ql_node():
            self.skipTest("Needs a node running the n1ql service")

        index = self.create_and_load_test_index(index_name="fts_crl_n1ql")
        fts_node = self.fts_nodes[0]
        search_query = (
            'SELECT COUNT(1) AS hits FROM `{0}` AS t WHERE SEARCH(t, '
            '{{"query": {{"match": "emp", "field": "type"}}}}, '
            '{{"index": "{1}"}})'.format(index._source_name, index.name))

        # Baseline over a healthy nodeToNode path.
        self.publish_node_crls(crl_number=1)
        self.set_crl_policy(client_auth="Disabled", node_to_node="Require")
        baseline = self._cb_cluster.run_n1ql_query(search_query)
        baseline_hits = self._n1ql_hits(baseline)
        if baseline_hits is None:
            self.fail("SEARCH() baseline did not return a hit count before any "
                      "revocation: {0}".format(str(baseline)[:400]))
        self.log.info("SEARCH() baseline under nodeToNode=Require: {0} "
                      "hits".format(baseline_hits))

        # Revoke the FTS node's own certificate.
        self.revoke_node_cert(fts_node, crl_number=2)

        after = self._run_n1ql_expecting_failure(search_query)
        if after is not None:
            self.fail(
                "SEARCH() still returned {0} hits after the FTS node's "
                "certificate ({1}) was revoked under nodeToNode=Require. A "
                "peer service must not reach a node whose certificate is "
                "revoked — this is the MB-73610 failure mode.".format(
                    after, fts_node.ip))
        self.log.info("SEARCH() failed after the FTS node cert was revoked, "
                      "as required")

        # Restore, and confirm the failure was the revocation and not a
        # cluster left permanently broken by the test.
        self.restore_node_certs(crl_number=3)
        restored = self._cb_cluster.run_n1ql_query(search_query)
        restored_hits = self._n1ql_hits(restored)
        self.assertEqual(
            restored_hits, baseline_hits,
            "SEARCH() did not recover after un-revoking the FTS node cert: "
            "expected {0} hits, got {1}".format(baseline_hits, restored_hits))
        self.log.info("SEARCH() recovered after un-revocation")

    def test_crl_scatter_gather_with_revoked_fts_node(self):
        """Plan FTS-N2N-03 — a refused participant must never silently truncate results.

        With a partitioned index spread across FTS nodes, revoking one node's
        certificate under nodeToNode=Require makes it unreachable to its peers.
        The surviving node must fail the query or flag it as partial. Quietly
        returning the hits it can still reach is a correctness bug, not a
        security one, and is the outcome this test exists to catch.
        """
        if len(self.fts_nodes) < 2:
            self.skipTest("Needs >= 2 FTS nodes; got {0}".format(
                len(self.fts_nodes)))

        collection_index, index_type, index_scope, index_collections = \
            self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name("default"),
            index_name="fts_crl_scatter",
            collection_index=collection_index, _type=index_type,
            scope=index_scope, collections=index_collections,
            plan_params={"maxPartitionsPerPIndex": 171})
        self.load_data()
        self.wait_for_indexing_complete()

        identity = self.create_client_identity("scatter")
        self.enable_client_cert_auth()
        self.publish_crl([], crl_number=1)
        self.publish_node_crls(crl_number=1)
        self.set_crl_policy(client_auth="Require", node_to_node="Require")

        query = self.default_query()
        survivor, victim = self.fts_nodes[0], self.fts_nodes[1]
        baseline = self.assert_allowed(
            self.wait_until_allowed(
                self.query_op(index, query, node=survivor), identity,
                context="scatter-gather baseline"),
            context="scatter-gather baseline")
        baseline_hits = baseline.total_hits()
        self.log.info("Scatter-gather baseline from {0}: {1} hits".format(
            survivor.ip, baseline_hits))

        self.revoke_node_cert(victim, crl_number=2)

        result = self.fts_query(identity, index, query, node=survivor)
        hits = result.total_hits()
        self.log.info(
            "After revoking {0}'s node cert, a query served by {1} returned "
            "outcome={2} hits={3} (baseline {4})".format(
                victim.ip, survivor.ip, result.outcome, hits, baseline_hits))

        if result.allowed and hits is not None and hits < baseline_hits:
            self.fail(
                "SILENT TRUNCATION: the query succeeded with {0} hits after "
                "{1}'s certificate was revoked, down from {2}. A scatter-gather "
                "participant that cannot be reached must fail the query or "
                "flag partial results, never quietly return fewer "
                "rows.".format(hits, victim.ip, baseline_hits))
        self.log.info("No silent truncation: outcome={0}".format(result.outcome))

        self.restore_node_certs(crl_number=3)
        self.assert_allowed(
            self.wait_until_allowed(
                self.query_op(index, query, node=survivor), identity,
                context="scatter-gather after restore"),
            expected_hits=baseline_hits,
            context="scatter-gather after restore")

    @staticmethod
    def _n1ql_hits(result):
        """hits from a SEARCH() COUNT query, or None if it did not return one."""
        if not isinstance(result, dict):
            return None
        if result.get("status") != "success":
            return None
        rows = result.get("results") or []
        if not rows or not isinstance(rows[0], dict):
            return None
        return rows[0].get("hits")

    def _run_n1ql_expecting_failure(self, query):
        """Run a N1QL query that should fail; return hits if it wrongly succeeded."""
        try:
            result = self._cb_cluster.run_n1ql_query(query)
        except Exception as exc:
            self.log.info("SEARCH() raised as expected: {0}: {1}".format(
                type(exc).__name__, str(exc)[:200]))
            return None
        hits = self._n1ql_hits(result)
        if hits is None:
            self.log.info("SEARCH() returned no usable result, as expected: "
                          "{0}".format(str(result)[:300]))
        return hits

    # ──────────────────────────────────────────────────────────────────
    # Chaos and resilience
    # ──────────────────────────────────────────────────────────────────

    def test_crl_cbft_restart_reenforces(self):
        """Plan FTS-CHAOS-01 — cbft re-reads CRL state on restart."""
        index, valid, revoked, query = self.setup_enforcement()

        self.assert_allowed(self.fts_query(valid, index, query),
                            context="valid before cbft restart")
        self.assert_revoked(self.fts_query(revoked, index, query),
                            context="revoked before cbft restart")

        node = self.restart_cbft(self.fts_nodes[0])

        self.assert_never_allowed_until(
            revoked, valid, index, query=query,
            context="cbft restart recovery window on {0}".format(node.ip))

        self.assert_dual_client(
            "query after cbft restart",
            self.query_op(index, query), valid, revoked)

        self.wait_for_indexing_complete()
        self.assert_index_complete(
            index, context="index after cbft restart")

    def test_crl_hot_reload_still_works_after_cbft_restart(self):
        """Plan FTS-CHAOS-01 / FTS-LC-09 — reload path survives a restart."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("afterrestart")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()
        operation = self.query_op(index, query)

        self.restart_cbft(self.fts_nodes[0])
        self.wait_for_fts_ready(identity, index, query,
                                context="cbft back up before reload test")

        self.revoke(identity, crl_number=2)
        self.assert_revoked(
            self.wait_until_revoked(operation, identity,
                                    context="revocation after cbft restart"),
            context="revocation after cbft restart")

        self.publish_crl([], crl_number=3)
        self.assert_allowed(
            self.wait_until_allowed(operation, identity,
                                    context="restored after cbft restart"),
            context="restored after cbft restart")

    def test_crl_revoked_session_does_not_survive_cbft_restart(self):
        """Plan FTS-CLI-05 (tail) — a live revoked session dies with the service."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("chaossession")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")

        node = self.fts_nodes[0]
        url = "https://{0}:{1}{2}".format(
            node.ip, CbServer.ssl_fts_port, self.query_path_for_index(index))
        body = {"query": self.default_query(), "size": 10}

        session = requests.Session()
        session.cert = identity.cert_tuple
        session.verify = False
        try:
            first = session.post(url, json=body, timeout=60)
            if not first.ok:
                self.fail("Keep-alive session failed before revocation: "
                          "{0} {1}".format(first.status_code, first.content))

            self.revoke(identity, crl_number=2)
            self.restart_cbft(node)

            probe = self.create_client_identity("chaosprobe")
            self.wait_for_fts_ready(probe, index, context="cbft back up")

            try:
                response = session.post(url, json=body, timeout=60)
                if response.ok:
                    self.fail(
                        "A revoked identity's session succeeded after a cbft "
                        "restart (http {0}) — the reconnect must be rejected, "
                        "since the original handshake no longer "
                        "exists.".format(response.status_code))
                self.log.info(
                    "Revoked session reconnect rejected with http {0}".format(
                        response.status_code))
            except requests.exceptions.RequestException as exc:
                self.log.info(
                    "Revoked session reconnect failed at the transport layer "
                    "as expected: {0}".format(type(exc).__name__))
        finally:
            session.close()

    def test_crl_expires_during_workload_under_require(self):
        """Plan FTS-CHAOS-02 — a CRL expiring mid-workload under Require."""
        lifetime_secs = self._input.param("crl_lifetime_secs", 90)

        index = self.create_and_load_test_index()
        identity = self.create_client_identity("expiry")
        self.enable_client_cert_auth()

        now = datetime.datetime.now(datetime.timezone.utc)
        short_lived = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[],
            this_update=now - datetime.timedelta(minutes=1),
            next_update=now + datetime.timedelta(seconds=lifetime_secs),
            crl_number=1)
        self.upload_crl(DEFAULT_CRL_FILE, short_lived)
        self.reload_crl_all_nodes()
        self.set_crl_policy(client_auth="Require")

        query = self.default_query()
        operation = self.query_op(index, query)
        expected_hits = self.assert_allowed(
            operation(identity),
            context="valid identity while the CRL is current").total_hits()

        self.log.info("Querying until the CRL expires (~{0}s)".format(
            lifetime_secs))
        deadline = time.time() + lifetime_secs + 120
        became_rejected = False
        while time.time() < deadline:
            result = operation(identity)
            if result.revoked:
                became_rejected = True
                self.log.info(
                    "CRL expiry took effect: new connections now rejected "
                    "via {0}".format(result.outcome))
                break
            if not result.allowed:
                self.fail("Unexpected outcome while the CRL was still current: "
                          "{0}".format(result))
            time.sleep(5)

        if not became_rejected:
            self.fail(
                "CRL with nextUpdate {0}s in the future never caused rejection "
                "under Require. An expired CRL cannot establish that a cert is "
                "unrevoked, so Require must fail closed (plan §4A). Note plan "
                "OQ-C: expired is reported as 'undetermined', and this is the "
                "mode where that must still reject.".format(lifetime_secs))

        self.publish_crl([], crl_number=2)
        self.assert_allowed(
            self.wait_until_allowed(operation, identity,
                                    context="after a fresh CRL is uploaded"),
            expected_hits=expected_hits,
            context="after a fresh CRL is uploaded")

        self.wait_for_indexing_complete()
        self.assert_index_complete(
            index, context="index after CRL expiry and recovery")

    def test_crl_local_dir_recovery_from_lockout(self):
        """Plan FTS-CHAOS-03 / FTS-LC-03 — the inbox/crls escape hatch."""
        index = self.create_and_load_test_index()
        identity = self.create_client_identity("lockout")
        self.enable_client_cert_auth()

        self.publish_crl([], crl_number=1)
        self.set_crl_policy(client_auth="Require")
        query = self.default_query()
        operation = self.query_op(index, query)
        expected_hits = self.assert_allowed(
            operation(identity), context="before lockout").total_hits()

        self.delete_crl(DEFAULT_CRL_FILE)
        self.assert_revoked(
            self.wait_until_revoked(operation, identity,
                                    context="locked out with no applicable CRL"),
            context="locked out with no applicable CRL")

        recovery_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[], crl_number=10)
        crl_dir = self.place_crl_in_local_dir(
            recovery_pem, filename="fts_crl_recovery.pem")
        self.log.info("Dropped a recovery CRL into {0}".format(crl_dir))

        self.assert_allowed(
            self.wait_until_allowed(operation, identity,
                                    context="recovered via the local CRL dir"),
            expected_hits=expected_hits,
            context="recovered via the local CRL dir")

        _, status_body = self.diagnostics_status()
        self.log.info("CRL sources after local-dir recovery: {0}".format(
            self.crl_sources(status_body)))
        for host, node_entry in (status_body or {}).items():
            poll = self.poll_directory(node_entry)
            if poll:
                self.log.info("{0} pollDirectory after recovery: status={1} "
                              "errors={2}".format(
                                  host, poll.get("status"), poll.get("errors")))

        self.wait_for_indexing_complete()
        self.assert_index_complete(
            index, context="index after local-dir recovery")

        revoked_after = self.create_client_identity("lockout-revoked")
        self.place_crl_in_local_dir(
            self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=[revoked_after.serial], crl_number=11),
            filename="fts_crl_recovery.pem")
        self.assert_revoked(
            self.wait_until_revoked(operation, revoked_after,
                                    context="revocation via local-dir CRL"),
            context="revocation via local-dir CRL")

    def test_crl_survives_couchbase_restart(self):
        """Plan FTS-LC-09 / FTS-CHAOS-01 — full couchbase-server restart."""
        index, valid, revoked, query = self.setup_enforcement()

        files_before = {entry.get("filename"): entry.get("checksum")
                        for entry in self.list_crl_files() or []}
        policy_before = self.get_crl_settings().get("policyPerScope")
        self.log.info("Before restart: files={0} policy={1}".format(
            files_before, policy_before))

        node = self.restart_couchbase_on_node(self.fts_nodes[0])

        query = self.default_query()
        self.assert_never_allowed_until(
            revoked, valid, index, query=query,
            context="couchbase restart recovery window on {0}".format(node.ip))

        files_after = {entry.get("filename"): entry.get("checksum")
                       for entry in self.list_crl_files() or []}
        policy_after = self.get_crl_settings().get("policyPerScope")
        if files_before != files_after:
            self.fail("CRL files changed across restart: before={0} "
                      "after={1}".format(files_before, files_after))
        if policy_before != policy_after:
            self.fail("CRL policy changed across restart: before={0} "
                      "after={1}".format(policy_before, policy_after))

        self.assert_crl_consistent_across_nodes()
        self.assert_dual_client(
            "query after couchbase restart",
            self.query_op(index, query), valid, revoked)

        self.wait_for_indexing_complete()
        self.assert_index_complete(
            index, context="index after couchbase restart")

    def test_crl_survives_node_reboot(self):
        """Plan FTS-CHAOS-04 — node reboot under enforcement."""
        index, valid, revoked, query = self.setup_enforcement()

        files_before = {entry.get("filename"): entry.get("checksum")
                        for entry in self.list_crl_files() or []}
        policy_before = self.get_crl_settings().get("policyPerScope")

        node = self.reboot_node(self.fts_nodes[0])

        query = self.default_query()
        self.assert_never_allowed_until(
            revoked, valid, index, query=query, timeout=600,
            context="reboot recovery window on {0}".format(node.ip))

        files_after = {entry.get("filename"): entry.get("checksum")
                       for entry in self.list_crl_files() or []}
        policy_after = self.get_crl_settings().get("policyPerScope")
        if files_before != files_after:
            self.fail("CRL files changed across reboot: before={0} "
                      "after={1}".format(files_before, files_after))
        if policy_before != policy_after:
            self.fail("CRL policy changed across reboot: before={0} "
                      "after={1}".format(policy_before, policy_after))

        self.assert_dual_client(
            "query after node reboot",
            self.query_op(index, query), valid, revoked)

        self.wait_for_indexing_complete()
        self.assert_index_complete(index, context="index after node reboot")

    def test_crl_malformed_upload_barrage_keeps_fts_serving(self):
        """Plan FTS-CHAOS-05 — a barrage of bad CRLs must not destabilise cbft."""
        index, valid, revoked, query = self.setup_enforcement()

        expected_hits = self.assert_allowed(
            self.fts_query(valid, index, query),
            context="baseline before the barrage").total_hits()
        pids_before = self.cbft_pids()

        good_crl = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[], crl_number=1)
        cert_pem = self.crl_utils.cert_to_pem(self.ca_cert)

        payloads = {
            "empty": b"",
            "whitespace": b"   \n\t\n",
            "random bytes": bytes(range(256)) * 8,
            "truncated pem": good_crl[:len(good_crl) // 3],
            "pem headers only": b"-----BEGIN X509 CRL-----\n-----END X509 CRL-----\n",
            "certificate not crl": cert_pem,
            "tampered signature": self.tamper_crl_pem(good_crl),
            "json body": b'{"not": "a crl"}',
            "null bytes": b"\x00" * 4096,
            "huge garbage": b"A" * (2 * 1024 * 1024),
        }

        accepted = []
        for name, payload in payloads.items():
            filename = "fts_crl_bad_{0}.pem".format(
                name.replace(" ", "_"))
            status, content, _ = self.rest.upload_crl_file(filename, payload)
            parsed = self.crl_utils.parse_content(content)
            if status:
                self.track_crl_file(filename)
                accepted.append((name, parsed))
                self.log.warning("Malformed payload {0!r} was ACCEPTED: "
                                 "{1}".format(name, parsed))
            else:
                self.log.info("Rejected {0!r}: {1}".format(
                    name, str(parsed)[:160]))

        self.assert_cbft_not_restarted(
            pids_before, context="malformed-CRL barrage")

        self.reload_crl_all_nodes()
        self.assert_dual_client(
            "query after the malformed-CRL barrage",
            self.query_op(index, query),
            valid, revoked, expected_hits=expected_hits)

        if accepted:
            self.log.warning(
                "These malformed payloads were accepted at upload and rely on "
                "load-time validation instead: {0}".format(
                    [name for name, _ in accepted]))

    def test_crl_rotation_soak(self):
        """Plan §12 (soak) — repeated CRL rotation under continuous query load."""
        duration = self._input.param("soak_duration_secs", 300)

        index, valid, revoked, query = self.setup_enforcement(
            revoke=False)

        expected_hits = self.assert_allowed(
            self.fts_query(valid, index, query),
            context="soak baseline").total_hits()

        deadline = time.time() + duration
        crl_number = 1
        rotations = 0
        while time.time() < deadline:
            crl_number += 1
            rotations += 1
            revoking = (rotations % 2 == 1)

            self.publish_crl(revoked if revoking else [],
                             crl_number=crl_number)

            operation = self.query_op(index, query)
            if revoking:
                self.assert_revoked(
                    self.wait_until_revoked(
                        operation, revoked,
                        context="soak rotation {0} (revoking)".format(rotations)),
                    context="soak rotation {0} (revoking)".format(rotations))
            else:
                self.assert_allowed(
                    self.wait_until_allowed(
                        operation, revoked,
                        context="soak rotation {0} (clearing)".format(rotations)),
                    expected_hits=expected_hits,
                    context="soak rotation {0} (clearing)".format(rotations))

            self.assert_allowed(
                self.fts_query(valid, index, query),
                expected_hits=expected_hits,
                context="soak rotation {0} (valid identity)".format(rotations))

        self.log.info("Soak completed {0} CRL rotations over {1}s".format(
            rotations, duration))
        if rotations < 2:
            self.fail("Soak completed only {0} rotation(s) in {1}s — increase "
                      "soak_duration_secs for this to be meaningful".format(
                          rotations, duration))

        self.assert_crl_consistent_across_nodes()
        self.wait_for_indexing_complete()
        self.assert_index_complete(
            index, context="index after the rotation soak")
