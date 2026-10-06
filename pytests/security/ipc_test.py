from pytests.security.ipc_base import IPCBase


class IPCTest(IPCBase):
    """
    Internal-identity Password Check under mTLS — MB-73874.

    See IPCBase for what the feature does and why. These tests drive it from the
    attacker's side: a certificate carrying the reserved internal SAN, and the
    question of whether presenting it alone is enough to become a cluster
    administrator.
    """

    def test_forged_internal_cert_blocked_when_ipc_enabled(self):
        """
        Core exploit and fix validation.

        A certificate that shares nothing with this cluster except a trust
        anchor — unrelated subject, issued for an unrelated purpose — with the
        reserved SAN rfc822Name internal@internal.couchbase.com injected into
        it. That injection is the whole attack: the SAN value is identical in
        every Couchbase installation and is published in the documentation, so
        obtaining such a certificate takes nothing more than asking a trusted CA
        for one extra SAN entry.

        Steps:
            1. Enable client certificate authentication.
            2. Forge the certificate described above.
            3. IPC enabled: the forged certificate alone is refused, as an
               application-layer 401 with the anonymous identity — not a TLS
               rejection. That distinction is the point of the test.
            4. IPC disabled: the same certificate authenticates as the internal
               identity, reproducing the bypass. This is the "before" half of
               the evidence and must fail loudly if it ever stops reproducing,
               because that would mean the test is no longer exercising the
               thing it claims to.
            5. IPC re-enabled: refused again, proving the setting is what
               decides and that the result is not an artefact of ordering.
        """
        self.log.info("Step 1: enabling client certificate authentication")
        self._enable_client_cert_auth(state="enable")

        self.log.info("Step 2: forging a certificate with an unrelated subject "
                      "and the reserved internal SAN injected")
        cert_path, key_path = self.generate_forged_internal_cert()

        self.log.info("Step 3: IPC enabled — the forged certificate must be "
                      "refused")
        self.set_ipc_setting(True)
        self.assert_no_identity_from_cert(cert_path, key_path)
        self.log.info("Step 3 passed: forged certificate established no "
                      "identity")

        self.log.info("Step 4: IPC disabled — the same certificate must "
                      "reproduce the original bypass")
        self.set_ipc_setting(False)
        identity = self.whoami_via_mtls(cert_path, key_path)
        self.log.info("Step 4: /whoami resolved the forged certificate to "
                      "{0}".format(identity))
        resolved_id = identity.get("id", "")
        self.assertTrue(
            resolved_id.startswith("@"),
            "With IPC disabled the forged certificate should resolve to an "
            "internal '@' identity, got {0}. If this stops reproducing, the "
            "certificate is no longer being recognised as an internal one and "
            "step 3 proves nothing.".format(identity)
        )
        resp = self.mtls_request(cert_path, key_path, path="/pools/default")
        self.assertEqual(
            resp.status_code, 200,
            "With IPC disabled the forged certificate should reach "
            "/pools/default as an administrator, got HTTP {0}: {1}".format(
                resp.status_code, resp.text[:300])
        )
        self.log.info("Step 4 passed: bypass reproduced — the certificate "
                      "alone granted administrator access as {0}".format(
                          resolved_id))

        self.log.info("Step 5: IPC re-enabled — must be refused again")
        self.set_ipc_setting(True)
        self.assert_no_identity_from_cert(cert_path, key_path)

        self.log.info("Test passed: the forged internal certificate grants "
                      "administrator access only while the internal identity "
                      "password check is disabled")

    def test_genuine_internal_cert_needs_credentials_when_ipc_enabled(self):
        """
        The other half of the contract: with IPC enabled the certificate stops
        conferring identity, and the request's own credentials decide it
        instead — granting exactly that account's permissions, no more.

        Uses a genuine internal certificate issued by the cluster's own trusted
        CA, so this is about the supported path rather than the attack.

        Steps:
            1. Enable client certificate authentication.
            2. Issue a genuine internal certificate from the cluster's CA.
            3. IPC enabled, certificate alone: refused.
            4. Same certificate plus administrator credentials: authenticates as
               the administrator account, NOT as the internal identity. The
               certificate must contribute nothing to who the caller is.
            5. Same certificate plus a least-privileged account's credentials:
               authenticates as that account and gets no more than its own
               permissions. This is the case that would actually catch a
               residual escalation — step 4 cannot, because an administrator
               would have reached those endpoints regardless.
        """
        self.log.info("Step 1: enabling client certificate authentication")
        self._enable_client_cert_auth(state="enable")

        self.log.info("Step 2: issuing a genuine internal certificate")
        cert_path, key_path = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal"
        )

        self.log.info("Step 3: IPC enabled — certificate alone must be refused")
        self.set_ipc_setting(True)
        self.assert_no_identity_from_cert(cert_path, key_path)

        self.log.info("Step 4: certificate plus administrator credentials")
        admin_auth = (self.master.rest_username, self.master.rest_password)
        identity = self.whoami_via_mtls(cert_path, key_path, auth=admin_auth)
        self.log.info("Step 4: /whoami resolved to {0}".format(identity))
        self.assertEqual(
            identity.get("id"), self.master.rest_username,
            "With IPC enabled the identity must come from the supplied "
            "credentials, not from the certificate. Expected {0}, got "
            "{1}".format(self.master.rest_username, identity)
        )
        self.assertFalse(
            str(identity.get("id", "")).startswith("@"),
            "Identity resolved to an internal '@' account despite credentials "
            "being supplied — the certificate is still conferring identity: "
            "{0}".format(identity)
        )

        self.log.info("Step 5: certificate plus a least-privileged account")
        username, password = self._create_rbac_test_user(
            "ipc_ro_user", "ro_admin"
        )
        identity = self.whoami_via_mtls(
            cert_path, key_path, auth=(username, password)
        )
        self.log.info("Step 5: /whoami resolved to {0}".format(identity))
        self.assertEqual(
            identity.get("id"), username,
            "Expected the read-only account {0} to be the resolved identity, "
            "got {1}".format(username, identity)
        )
        resp = self.mtls_request(
            cert_path, key_path, path="/settings/clientCertAuth",
            auth=(username, password),
        )
        self.assertIn(
            resp.status_code, (401, 403),
            "A read-only account presenting an internal certificate must not "
            "reach an administrative endpoint — the certificate confers no "
            "privilege. Got HTTP {0}: {1}".format(
                resp.status_code, resp.text[:300])
        )

        self.log.info("Test passed: with IPC enabled the certificate "
                      "establishes no identity and confers no privilege; "
                      "credentials alone decide both")
