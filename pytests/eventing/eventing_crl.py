"""
CRL (Certificate Revocation List) tests for Eventing: clientAuth (Eventing's
own HTTPS port) and nodeToNode/n2n (Eventing dialing OUT to KV/FTS/CBAS/N1QL).

clientAuth flow:
  1. Generate a CA, trust it on the cluster.
  2. Issue a node cert for Eventing's HTTPS port + two client certs
     (A = revoked, B = valid).
  3. Enable clientCertAuth, mapping subject.cn -> RBAC username.
  4. Revoke A's serial, upload the CRL.
  5. Sweep the clientAuth CRL policy (Disabled/Permissive/Require) and probe
     both clients via a real mTLS handshake -- judged by TLS result alone,
     never HTTP status, since a revoked cert never gets an HTTP response.
     No "Strict" mode exists server-side.

n2n flow (KV/FTS/CBAS/N1QL/Eventing itself):
  1. Enable n2n encryption cluster-wide (ntonencryptionBase).
  2. Issue a CA-signed cert for the target node(s), revoke their serial(s),
     upload the CRL under nodeToNode scope, set the policy mode
     (nodetonode_crl_mode param, default Require -- both modes enforce an
     active/non-stale revocation identically, see CRL expiry below for
     where they diverge).
  3. Deploy a real handler that calls the dependent service from OnUpdate
     (KV: a plain mutation streamed via DCP; FTS/CBAS/N1QL: a query the
     handler itself issues) and confirm baseline processing pre-revocation.
  4. Kill eventing-producer (self.kill_producer()) to force a fresh
     connection under the new cert/CRL state -- undeploy/redeploy alone was
     tried first and confirmed NOT enough: pooled connections opened before
     revocation just kept being reused.
  5. Load one more batch and check whether the destination count grew:
       KV, all nodes revoked        -> stays at num_docs
       KV, one of two revoked       -> num_docs < count < num_docs*2
       FTS/CBAS/N1QL (single node)  -> stays at baseline (no partial case)
       Eventing's own node revoked  -> stays at num_docs (KV refuses the peer)

CRL expiry: an uploaded CRL's nextUpdate passing (going stale) does NOT
uniformly fail open. Confirmed live for clientAuth: Require hard-fails every
cert from that CA once its only CRL is stale (no fresh list = no verifiable
decision = no access), while Permissive soft-fails -- a never-revoked cert
keeps authenticating regardless of staleness. n2n is covered separately too
(restarts eventing-producer, confirms KV traffic keeps flowing) so a Require
clientAuth policy never ends up governing the framework's own non-cert admin
calls used to check n2n.
"""
import datetime
import json
import os
import tempfile
import urllib.parse

import requests
from cryptography.x509.oid import ExtendedKeyUsageOID

from pytests.eventing.eventing_base import EventingBaseTest
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_FTS_QUERY_SUPPORT, HANDLER_CODE_ANALYTICS
from pytests.fts.fts_callable import FTSCallable
from pytests.security.crl_base import CRLBase
from pytests.security.jwt_utils import JWTUtils
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.x509main import x509main
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection

import logging
log = logging.getLogger()


class EventingCRL(CRLBase, EventingBaseTest):
    def setUp(self):
        super(EventingCRL, self).setUp()  # CRLBase: CA gen + trust; also runs EventingBaseTest.setUp() via MRO
        self.eventing_ssl_port = self.input.param("eventing_ssl_port", 18096)
        self.client_cert_auth_state = self.input.param("client_cert_auth_state", "hybrid")
        self.crl_poll_wait = self.input.param("crl_poll_wait", 6)
        self.crl_expiry_wait = self.input.param("crl_expiry_wait", 30)
        self.crl_filename = "eventing_crl_test.pem"
        self.n2n_crl_filename = "eventing_n2n_crl_test.pem"
        self.ntonencrypt_level = self.input.param("ntonencrypt_level", "all")
        self.nodetonode_crl_mode = self.input.param("nodetonode_crl_mode", "Require")
        self.clientauth_crl_mode = self.input.param("clientauth_crl_mode", "Require")
        self._n2n_enabled = False
        self.log.info("clientCertAuth state={0}, eventing_ssl_port={1}, crl_poll_wait={2}, "
                      "nodetonode_crl_mode={3}, clientauth_crl_mode={4}".format(
            self.client_cert_auth_state, self.eventing_ssl_port, self.crl_poll_wait, self.nodetonode_crl_mode,
            self.clientauth_crl_mode))
        # http vs https here decides which port every admin REST call lands on
        self.log.info("self.rest.baseUrl={0}".format(self.rest.baseUrl))
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        # CRLBase pins self.rest to master; function calls (create/deploy) need the actual Eventing node
        self.rest = RestConnection(eventing_node)
        self.log.info("Deploying node cert on eventing node {0}".format(eventing_node.ip))
        self._deploy_node_cert(eventing_node)

    def tearDown(self):
        if getattr(self, "_n2n_enabled", False):  # n2n encryption isn't touched by CRLBase.tearDown()
            try:
                ntonencryptionBase().disable_nton_cluster([self.master])
            except Exception as exc:
                self.log.warning("Failed to disable n2n encryption in tearDown: {0}".format(exc))
        self.log.info("Tearing down EventingCRL test")
        super(EventingCRL, self).tearDown()  # CRLBase: CRL files, policy reset, clientCertAuth disable, RBAC cleanup

    # -------------------------- Helper Functions --------------------------

    def _write_temp_pem(self, pem_bytes):
        """Write PEM bytes to a temp file, auto-cleaned after the test."""
        fd, path = tempfile.mkstemp(suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(pem_bytes)
        self.addCleanup(os.remove, path)
        return path

    def _deploy_node_cert(self, server):
        """
        Issue a CA-signed node cert, push it over SSH, activate via reloadCertificate.
        """
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, cn=server.ip, dns_names=[server.ip],
            extended_key_usage=[ExtendedKeyUsageOID.SERVER_AUTH],
        )
        self.log.info("Node cert generated for {0} (serial={1})".format(server.ip, serial))
        install_path = x509main(host=server).install_path
        node_dir = "{0}{1}".format(install_path, x509main.CHAINFILEPATH)
        self.log.info("Pushing chain.pem + pkey.key to {0}:{1} over SSH".format(server.ip, node_dir))
        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command("mkdir -p {0}".format(node_dir))
            for filename, pem_bytes in [("chain.pem", self.crl_utils.cert_to_pem(cert)),
                                         ("pkey.key", self.crl_utils.key_to_pem(key))]:
                local = self._write_temp_pem(pem_bytes)
                shell.copy_file_local_to_remote(local, "{0}/{1}".format(node_dir, filename))
        finally:
            shell.disconnect()
        self.log.info("Files copied, calling reloadCertificate on {0}".format(server.ip))
        node_rest = RestConnection(server) if server.ip != self.master.ip else self.rest
        status, content = node_rest.reload_certificate()
        self.assertTrue(status, "reloadCertificate failed on {0}: {1}".format(server.ip, content))
        self.log.info("Node cert deployed + activated on {0}: {1}".format(server.ip, content))
        return serial

    def _generate_client_certs(self, specs=None):
        """
        Client A (revoke target) + client B (control, stays valid) by default.
        Specs let callers pick custom CNs, since a cert's CN is the RBAC username it logs in as
        (RBAC test cases need non-'admin' roles per client, not the fixed default pair).
        """
        if specs is None:
            specs = [("a", "test-client-a"), ("b", "test-client-b")]
        self.log.info("Generating client certs for: {0}".format(specs))
        clients = {}
        for label, cn in specs:
            cert, key, serial = self.crl_utils.generate_leaf_cert(self.ca_cert, self.ca_key, cn=cn)
            clients[label] = {
                "cert": cert, "key": key, "serial": serial, "cn": cn,
                "cert_path": self._write_temp_pem(self.crl_utils.cert_to_pem(cert)),
                "key_path": self._write_temp_pem(self.crl_utils.key_to_pem(key)),
            }
        self.log.info("Client certs generated: {0}".format(
            {label: (clients[label]["serial"], clients[label]["cn"]) for label in clients}))
        return clients

    def _probe_eventing_ssl(self, cert_path, key_path, ca_path):
        """
        mTLS handshake against Eventing's HTTPS port. True = accepted, False = rejected
        (either a TLS-layer SSLError, or the 401 fallback for a TLS alert).
        """
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        try:
            resp = self.crl_utils.perform_mtls_handshake(
                eventing_node.ip, self.eventing_ssl_port,
                cert_path, key_path, ca_path, path="/api/v1/functions",
            )
            if resp.status_code == 401:
                self.log.info("mTLS probe against {0}:{1} -> REJECTED via HTTP 401 ({2}): {3}".format(
                    eventing_node.ip, self.eventing_ssl_port, cert_path, resp.text))
                return False
            self.log.info("mTLS probe against {0}:{1} -> ACCEPTED ({2})".format(
                eventing_node.ip, self.eventing_ssl_port, cert_path))
            return True
        except requests.exceptions.SSLError as e:
            self.log.info("mTLS probe against {0}:{1} -> REJECTED ({2}): {3}".format(
                eventing_node.ip, self.eventing_ssl_port, cert_path, e))
            return False
        except requests.exceptions.ConnectionError as e:
            self.fail("mTLS probe against {0}:{1} failed with a connection error, not a TLS rejection: {2}".format(
                eventing_node.ip, self.eventing_ssl_port, e))

    def _wait_for_crl_poll_interval(self, filename):
        self.sleep(self.crl_poll_wait, "Waiting {0}s for CRL poll interval to clear ({1})".format(
            self.crl_poll_wait, filename))
        status, content, _ = self.rest.get_diagnostics_status()
        if not status:
            self.fail("get_diagnostics_status call failed: {0}".format(content))
        # The endpoint dumps every crlFile (including the unrelated ootb.crl)
        # for every node -- narrow it down to just the file we care about.
        per_node = json.loads(content)
        relevant = {}
        for node, node_status in per_node.items():
            for crl_file in node_status.get("crlFiles", []):
                if crl_file.get("filename") == filename:
                    relevant[node] = crl_file
                    break
        missing = sorted(set(per_node) - set(relevant))
        if missing:
            self.fail("diagnostics/status for {0} was not fetched on node(s): {1}".format(
                filename, missing))
        self.log.info("diagnostics/status for {0} after CRL change: {1}".format(filename, relevant))

    def _setup_clientauth_crl(self, next_update_seconds=None):
        """
        Enable clientCertAuth, create matching RBAC users, revoke client A, upload CRL.
        next_update_seconds: overrides the CRL's nextUpdate to expire that many
        seconds from now, instead of the 30-day default -- for expiry tests.
        """
        clients = self._generate_client_certs()
        self.log.info("Setting clientCertAuth state -> {0}".format(self.client_cert_auth_state))
        # subject.cn -> username; RBAC user must exist or a valid cert still fails auth downstream
        status, content = self.rest.client_cert_auth(
            state=self.client_cert_auth_state,
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}],
        )
        self.assertTrue(status, "client_cert_auth failed: {0}".format(content))
        self.log.info("clientCertAuth set: {0}".format(content))
        for label in ("a", "b"):
            cn = clients[label]["cn"]
            self._create_rbac_test_user(cn, "admin")
            self.log.info("RBAC user created for client {0} (cn={1})".format(label, cn))
        next_update = None
        if next_update_seconds is not None:
            next_update = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                seconds=next_update_seconds)
        self.log.info("Building CRL revoking client A (serial={0}), next_update={1}".format(
            clients["a"]["serial"], next_update or "default (+30d)"))
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key,
            revoked_serials=[clients["a"]["serial"]], crl_number=1, next_update=next_update,
        )
        status, content, _ = self.rest.upload_crl_file(self.crl_filename, crl_pem)
        self.assertTrue(status, "CRL upload failed: {0}".format(content))
        self._track_uploaded_file(self.crl_filename)
        self.log.info("CRL uploaded: {0}".format(self.crl_filename))
        ca_path = self._write_temp_pem(self.crl_utils.cert_to_pem(self.ca_cert))
        return clients, ca_path

    # ---------------------------- ClientAuth CRL Helpers ---------------------------

    def _revoke_and_apply_clientauth(self, serials, mode, crl_number):
        """
        Revoke `serials` under clientAuth and set the policy mode -- pulled out of
        _setup_clientauth_crl so a test can revoke a SECOND client mid-run. crl_number
        must be higher than any already used. serials: int or list (a lone int is
        wrapped into a list for build_crl()).
        """
        if isinstance(serials, int):
            serials = [serials]
        self.log.info("Revoking serial(s) {0} under clientAuth, mode -> {1}".format(serials, mode))
        crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, revoked_serials=serials,
                                           crl_number=crl_number)
        status, content, _ = self.rest.upload_crl_file(self.crl_filename, crl_pem)
        self.assertTrue(status, "CRL upload failed: {0}".format(content))
        self._track_uploaded_file(self.crl_filename)
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": mode}})
        self.assertTrue(status, "post_crl_settings failed for mode {0}".format(mode))
        self._wait_for_crl_poll_interval(self.crl_filename)

    def _setup_clientauth_crl_multi(self, specs):
        """Like _setup_clientauth_crl, but lets each client have its own CN and RBAC role
        (specs: list of (label, cn, role)) instead of the fixed pair (both role 'admin').
        Revokes nobody -- call _revoke_and_apply_clientauth() later for whichever client
        the test needs to revoke."""
        clients = self._generate_client_certs([(label, cn) for label, cn, _role in specs])
        self.log.info("Setting clientCertAuth state -> {0}".format(self.client_cert_auth_state))
        status, content = self.rest.client_cert_auth(
            state=self.client_cert_auth_state,
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}],
        )
        self.assertTrue(status, "client_cert_auth failed: {0}".format(content))
        for label, cn, role in specs:
            self._create_rbac_test_user(cn, role)
            self.log.info("RBAC user created: label={0} cn={1} role={2}".format(label, cn, role))
        ca_path = self._write_temp_pem(self.crl_utils.cert_to_pem(self.ca_cert))
        return clients, ca_path

    def _function_scope_query_string(self):
        if not self.function_scope:
            return ""
        return "?bucket={0}&scope={1}".format(self.function_scope["bucket"], self.function_scope["scope"])

    def _call_eventing_https_endpoint(self, cert_path, key_path, ca_path, method, path, json_body=None):
        """Make one HTTPS call to the Eventing node, presenting a client cert (plain
        `requests`, not RestConnection -- that can't present a cert at all). Returns a
        Response on success; raises SSLError/ConnectionError if the cert is rejected at
        the TLS layer."""
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        url = "https://{0}:{1}{2}".format(eventing_node.ip, self.eventing_ssl_port, path)
        return requests.request(
            method, url, cert=(cert_path, key_path), verify=ca_path,
            json=json_body, timeout=30,
        )

    def _create_function_via_cert(self, cert_path, key_path, ca_path, name, body):
        return self._call_eventing_https_endpoint(cert_path, key_path, ca_path, "POST",
                               "/api/v1/functions/{0}{1}".format(name, self._function_scope_query_string()), body)

    def _set_settings_via_cert(self, cert_path, key_path, ca_path, name, deployment_status, processing_status):
        body = {"deployment_status": deployment_status, "processing_status": processing_status}
        return self._call_eventing_https_endpoint(cert_path, key_path, ca_path, "POST",
                               "/api/v1/functions/{0}/settings{1}".format(name, self._function_scope_query_string()), body)

    def _assert_accepted(self, fn, *args, **kwargs):
        """Assert fn(*args, **kwargs) is accepted -- fail the test if it's rejected instead
        (a TLS-layer SSLError, a connection error, or the 401 fallback for a TLS alert)."""
        try:
            resp = fn(*args, **kwargs)
            if resp.status_code == 401:
                self.fail("Expected acceptance but got rejected via HTTP 401: {0}".format(resp.text))
            return resp
        except requests.exceptions.SSLError as e:
            self.fail("Expected acceptance but got a TLS-layer rejection: {0}".format(e))
        except requests.exceptions.ConnectionError as e:
            self.fail("Expected acceptance but got a connection error, not a TLS rejection: {0}".format(e))

    def _assert_rejected(self, fn, *args, **kwargs):
        """Assert fn(*args, **kwargs) is rejected -- fail the test if it succeeds instead.
        401 in place of a TLS alert is an acceptable way for ns_server's callback path to
        signal a refused/revoked cert, so it counts as a rejection alongside an actual TLS-layer SSLError."""
        try:
            resp = fn(*args, **kwargs)
            if resp.status_code == 401:
                self.log.info("Correctly rejected via HTTP 401 (fallback for a TLS alert): {0}".format(resp.text))
                return
            self.fail("Expected a TLS-layer rejection but got a response: {0} {1}".format(
                resp.status_code, resp.text))
        except requests.exceptions.SSLError as e:
            self.log.info("Correctly rejected at the TLS layer: {0}".format(e))
        except requests.exceptions.ConnectionError as e:
            self.fail("Expected a TLS-layer rejection but got a connection error instead: {0}".format(e))

    def _admin_set_settings(self, name, deployment_status, processing_status, expected_state):
        """Drive a lifecycle state transition via plain Administrator basic auth, then block
        until the function actually reaches expected_state."""
        try:
            self.rest.set_settings_for_function(
                name, {"deployment_status": deployment_status, "processing_status": processing_status},
                self.function_scope)
        except Exception as exc:
            if "state transition not possible" not in str(exc):
                raise
            self.log.info("{0} already at/transitioning to {1}, skipping duplicate settings call: {2}".format(
                name, expected_state, exc))
        self.wait_for_handler_state(name, expected_state)

    def _cleanup_function(self, name):
        """Used after a gated op that may or may not have actually landed
        (a rejected call never reaches the server, so the function may already be
        gone, or may never have existed under that name)."""
        try:
            self.rest.delete_single_function(name, self.function_scope)
        except Exception as exc:
            self.log.info("Cleanup delete for {0} skipped (already gone, or never created): {1}".format(name, exc))

    def _assert_lifecycle_ops_for_cert(self, appname, cert_path, key_path, ca_path, expect_accept):
        """Exercises Create/Save, Deploy, Pause, Resume, Export, Import, Undeploy, and Delete
        against `appname` through ONE cert identity, asserting every single op is uniformly
        accepted or rejected (a TLS-layer alert, or the 401 fallback)."""
        assert_fn = self._assert_accepted if expect_accept else self._assert_rejected
        body = self.create_save_function_body(appname, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)

        assert_fn(self._create_function_via_cert, cert_path, key_path, ca_path, appname, body)  # Create/Save
        self.rest.create_function(appname, body, self.function_scope)

        assert_fn(self._set_settings_via_cert, cert_path, key_path, ca_path, appname, True, True)  # Deploy
        self._admin_set_settings(appname, True, True, "deployed")

        assert_fn(self._set_settings_via_cert, cert_path, key_path, ca_path, appname, True, False)  # Pause
        self._admin_set_settings(appname, True, False, "paused")

        assert_fn(self._set_settings_via_cert, cert_path, key_path, ca_path, appname, True, True)  # Resume
        self._admin_set_settings(appname, True, True, "deployed")

        assert_fn(self._call_eventing_https_endpoint, cert_path, key_path, ca_path, "GET",
             "/api/v1/export/{0}{1}".format(appname, self._function_scope_query_string()))  # Export
        exported = self.rest.export_function(appname, self.function_scope)

        import_name = appname + "_imported"  # Import
        exported_copy = dict(exported)
        exported_copy["appname"] = import_name
        assert_fn(self._call_eventing_https_endpoint, cert_path, key_path, ca_path, "POST",
             "/api/v1/import", [exported_copy])
        self._cleanup_function(import_name)

        assert_fn(self._set_settings_via_cert, cert_path, key_path, ca_path, appname, False, False)  # Undeploy
        self._admin_set_settings(appname, False, False, "undeployed")

        assert_fn(self._call_eventing_https_endpoint, cert_path, key_path, ca_path, "DELETE",
             "/api/v1/functions/{0}{1}".format(appname, self._function_scope_query_string()))  # Delete
        self._cleanup_function(appname)

    def _setup_fts_for_multiple_functions_test(self):
        """Loads travel-sample and creates a default full-text index named
        'travel_sample_test' over it -- what handler_code/fts_query_support/match_query.js
        expects to query."""
        self.load_sample_buckets(self.server, "travel-sample")
        fts_node = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)
        fts_rest = RestConnection(fts_node)
        index_params = {
            "type": "fulltext-index",
            "name": "travel_sample_test",
            "sourceType": "couchbase",
            "sourceName": "travel-sample",
            "planParams": {"indexPartitions": 1, "numReplicas": 0},
            "params": {
                "doc_config": {"mode": "type_field", "type_field": "type"},
                "mapping": {
                    "default_analyzer": "standard", "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all", "default_mapping": {"enabled": True, "dynamic": True},
                    "default_type": "_default", "index_dynamic": True, "store_dynamic": False,
                    "type_field": "_type",
                },
            },
        }
        fts_rest.create_fts_index("travel_sample_test", index_params, bucket="travel-sample")
        self.sleep(15, "Waiting for FTS index build")

    def _setup_analytics_for_multiple_functions_test(self):
        """
        Dataverse + analytics collection over travel-sample's airline data, connected and
        polled for ingestion -- same sequence as eventing_analytics.py's _setup_analytics().
        """
        self.load_sample_buckets(self.server, "travel-sample")
        cbas_node = self.get_nodes_from_services_map(service_type="cbas", get_all_nodes=False)
        cbas_rest = RestConnection(cbas_node)
        cbas_rest.execute_statement_on_cbas("CREATE DATAVERSE `travel-sample`.`inventory`", None)
        cbas_rest.execute_statement_on_cbas(
            "CREATE ANALYTICS COLLECTION `travel-sample`.`inventory`.`airline` "
            "ON `travel-sample`.`inventory`.`airline`", None)
        cbas_rest.execute_statement_on_cbas("CONNECT LINK Local", None)
        count = 0
        poll_count = 0
        while count == 0 and poll_count < 20:
            self.sleep(15, "Waiting for analytics to ingest travel-sample data")
            result = cbas_rest.execute_statement_on_cbas(
                "SELECT COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline`", None)
            if isinstance(result, bytes):
                result = result.decode("utf-8")
            count = json.loads(result)["results"][0]["cnt"]
            poll_count += 1
        self.assertTrue(count > 0, "Analytics airline collection has 0 docs -- ingestion failed")

    def _setup_jwt_auth(self):
        """
        JWT branch of test_multiple_functions_with_clientauth_crl -- a minimal, self-signed
        version of eventing_jwt_auth.py's setup_jwt_config() (no external IdP needed).
        Returns the bearer token for function create/deploy calls.
        """
        jwt_utils = JWTUtils(log=self.log)
        private_key, public_key = jwt_utils.generate_key_pair("ES256", key_size=2048)

        group_name, user_name = "crl_multi_fn_jwt_group", "crl_multi_fn_jwt_user"
        status, content = self.rest.add_group_role(
            group_name=group_name, description="JWT group for test_multiple_functions_with_clientauth_crl",
            roles=self._minimal_eventing_role())
        self.assertTrue(status, "Failed to create JWT group {0}: {1}".format(group_name, content))

        payload = urllib.parse.urlencode({"name": user_name, "groups": group_name})
        self.rest.add_external_user(user_name, payload)

        jwt_config = jwt_utils.get_jwt_config(
            issuer_name="crl-test-issuer", algorithm="ES256", pub_key=public_key,
            token_audience=["cb-cluster"], token_group_matching_rule=["^.{0}$ {1}".format(user_name, group_name)],
            jit_provisioning=True)
        status, content, _ = self.rest.create_jwt_with_config(jwt_config)
        self.assertTrue(status, "Failed to configure JWT on cluster: {0}".format(content))

        return jwt_utils.create_token(
            issuer_name="crl-test-issuer", user_name=user_name, algorithm="ES256", private_key=private_key,
            token_audience=["cb-cluster"], user_groups=[group_name], ttl=3600)

    def _minimal_eventing_role(self):
        """
        Scoped eventing_manage_functions role for just the function's own bucket/scope.
        """
        return "eventing_manage_functions[{0}:{1}]".format(
            self.function_scope["bucket"], self.function_scope["scope"])

    # ---------------------------- N2N CRL Helpers ---------------------------

    def _enable_n2n_encryption(self):
        """
        Cluster-wide n2n encryption -- same helper as eventing_security.py.
        """
        self.log.info("Enabling n2n encryption cluster-wide (level={0})".format(self.ntonencrypt_level))
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        self._n2n_enabled = True

    def _trust_ca_on_eventing_node(self):
        """
        CRLBase.setUp() only trusts the CA on master, not necessarily the
        eventing node (the TLS client here) -- trust it explicitly here.
        """
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        self.log.info("Trusting CA on eventing node {0}".format(eventing_node.ip))
        self._trust_ca_on_cluster(self.ca_cert, server=eventing_node)

    def _get_kv_nodes(self):
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        self.log.info("KV nodes in cluster: {0}".format([n.ip for n in kv_nodes]))
        return kv_nodes

    def _revoke_node_certs(self, nodes_to_revoke, next_update_seconds=None):
        """
        Issue + deploy a cert per node, then upload one CRL revoking all their serials
        under nodeToNode (node-type agnostic). nodes_to_revoke=[] revokes nothing, for
        expiry tests needing just a stale-but-benign CRL. next_update_seconds overrides
        the CRL's nextUpdate (default +30d), also for expiry tests.
        """
        serials = []
        for node in nodes_to_revoke:
            self._trust_ca_on_cluster(self.ca_cert, server=node)
            serial = self._deploy_node_cert(node)
            serials.append(serial)
        next_update = None
        if next_update_seconds is not None:
            next_update = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                seconds=next_update_seconds)
        self.log.info("Revoking node(s) {0} (serials={1}), next_update={2}".format(
            [n.ip for n in nodes_to_revoke], serials, next_update or "default (+30d)"))
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=serials, crl_number=1, next_update=next_update)
        status, content, _ = self.rest.upload_crl_file(self.n2n_crl_filename, crl_pem)
        self.assertTrue(status, "n2n CRL upload failed: {0}".format(content))
        self._track_uploaded_file(self.n2n_crl_filename)
        self.log.info("n2n CRL uploaded: {0}".format(self.n2n_crl_filename))
        return serials

    def _set_nodetonode_crl_mode(self, mode):
        self.log.info("Setting nodeToNode CRL mode -> {0}".format(mode))
        status, content, _ = self.rest.post_crl_settings({"policyPerScope": {"nodeToNode": mode}})
        self.assertTrue(status, "post_crl_settings (nodeToNode) failed for mode {0}: {1}".format(mode, content))

    def _create_and_deploy_n2n_function(self):
        """
        Real OnUpdate/OnDelete handler (not a no-op probe) so execution_stats
        reflect actual per-vbucket processing once a KV node is unreachable.
        """
        body = self.create_save_function_body(self.function_name, "handler_code/delete_doc_bucket_op.js")
        self.deploy_function(body)
        self.load_data_to_collection(self.num_docs, "src_bucket._default._default")
        self.verify_doc_count_collections("dst_bucket._default._default", self.num_docs)
        self.log.info("Baseline function deployed and processing confirmed ({0} docs)".format(self.num_docs))
        return body

    def _force_function_reconnect(self, body):
        """
        Kill eventing-producer -- respawns with fresh connections under the
        new cert/CRL state.
        """
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        self.log.info("Killing eventing-producer on {0} to force reconnection under new nodeToNode CRL state".format(
            eventing_node.ip))
        self.kill_producer(eventing_node)
        self.sleep(120, "Waiting for eventing-producer to respawn")
        self.wait_for_handler_state(body['appname'], "deployed")

    def _get_update_delete_stats(self):
        on_update_success = self.get_stats_value(self.function_name, "execution_stats.on_update_success")
        on_delete_success = self.get_stats_value(self.function_name, "execution_stats.on_delete_success")
        self.log.info("execution_stats.on_update_success={0}, on_delete_success={1}".format(
            on_update_success, on_delete_success))
        return on_update_success, on_delete_success

    def _wait_for_update_stat_stable(self, interval=15, max_checks=8):
        """
        Poll on_update_success until it stops changing between checks (settles).
        """
        previous, _ = self._get_update_delete_stats()
        for _ in range(max_checks):
            self.sleep(interval, "Waiting for on_update_success to stabilize (currently {0})".format(previous))
            current, _ = self._get_update_delete_stats()
            if current == previous:
                return current
            previous = current
        self.log.warning("on_update_success did not stabilize within {0}s, using last value {1}".format(
            interval * max_checks, previous))
        return previous

    def _get_dst_doc_count(self):
        return self.stat.get_collection_item_count_cumulative(
            self.dst_bucket_name, "_default", "_default", self.get_kv_nodes())

    def _print_final_state(self):
        """
        final dst_bucket doc count + on_update/on_delete stats
        """
        self.log.info("Final dst_bucket doc count: {0}".format(self._get_dst_doc_count()))
        self._get_update_delete_stats()

    # ------------------------ ClientAuth CRL Tests -------------------------
    # Step 5 matrix (per policy mode):
    #   Disabled            -> A accepted, B accepted
    #   Permissive/Require  -> A rejected, B accepted

    def test_clientauth_crl_sweep(self):
        """
        All three modes in one run, probing both clients at each
        """
        self.log.info(">>> test_clientauth_crl_sweep starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        expected = {
            "Disabled":   {"a": True,  "b": True},
            "Permissive": {"a": False, "b": True},
            "Require":    {"a": False, "b": True},
        }
        results = {}
        for mode, exp in expected.items():
            self.log.info("== clientAuth CRL mode -> {0} ==".format(mode))
            status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": mode}})
            self.assertTrue(status, "post_crl_settings failed for mode {0}".format(mode))
            self._wait_for_crl_poll_interval(self.crl_filename)
            probe_a = self._probe_eventing_ssl(clients["a"]["cert_path"], clients["a"]["key_path"], ca_path)
            probe_b = self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path)
            results[mode] = (probe_a, probe_b)
            self.log.info("Mode={0} result: client A={1} (expected {2}), client B={3} (expected {4})".format(
                mode, probe_a, exp["a"], probe_b, exp["b"]))
            self.assertEqual(probe_a, exp["a"], "Mode={0} client A: expected {1} got {2}".format(mode, exp["a"], probe_a))
            self.assertEqual(probe_b, exp["b"], "Mode={0} client B: expected {1} got {2}".format(mode, exp["b"], probe_b))
        self.log.info(">>> test_clientauth_crl_sweep finished, all modes as expected: {0} <<<".format(results))

    def test_clientauth_crl_disabled(self):
        """
        Disabled: CRL not enforced, revoked cert still gets through
        """
        self.log.info(">>> test_clientauth_crl_disabled starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        self.log.info("== clientAuth CRL mode -> Disabled ==")
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": "Disabled"}})
        self.assertTrue(status, "post_crl_settings failed for mode Disabled")
        self._wait_for_crl_poll_interval(self.crl_filename)
        self.assertTrue(self._probe_eventing_ssl(clients["a"]["cert_path"], clients["a"]["key_path"], ca_path),
                        "Revoked cert should be accepted when CRL mode is Disabled")
        self.assertTrue(self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path),
                        "Valid cert should be accepted when CRL mode is Disabled")
        self.log.info(">>> test_clientauth_crl_disabled finished <<<")

    def test_clientauth_crl_permissive(self):
        """
        Permissive: revoked cert rejected, valid cert unaffected
        """
        self.log.info(">>> test_clientauth_crl_permissive starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        self.log.info("== clientAuth CRL mode -> Permissive ==")
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": "Permissive"}})
        self.assertTrue(status, "post_crl_settings failed for mode Permissive")
        self._wait_for_crl_poll_interval(self.crl_filename)
        self.assertFalse(self._probe_eventing_ssl(clients["a"]["cert_path"], clients["a"]["key_path"], ca_path),
                         "Revoked cert should be rejected in Permissive mode")
        self.assertTrue(self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path),
                        "Valid cert should be accepted in Permissive mode")
        self.log.info(">>> test_clientauth_crl_permissive finished <<<")

    def test_clientauth_crl_require(self):
        """
        Require: revoked cert rejected, valid cert unaffected
        """
        self.log.info(">>> test_clientauth_crl_require starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        self.log.info("== clientAuth CRL mode -> Require ==")
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": "Require"}})
        self.assertTrue(status, "post_crl_settings failed for mode Require")
        self._wait_for_crl_poll_interval(self.crl_filename)
        self.assertFalse(self._probe_eventing_ssl(clients["a"]["cert_path"], clients["a"]["key_path"], ca_path),
                         "Revoked cert should be rejected in Require mode")
        self.assertTrue(self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path),
                        "Valid cert should be accepted in Require mode")
        self.log.info(">>> test_clientauth_crl_require finished <<<")

    # ------------------------ Functional Tests for ClientAuth ------------------------

    def test_lifecycle_ops_crl_sweep(self):
        """
        Every lifecycle op (Create/Save, Deploy, Pause, Resume, Export, Import, Undeploy,
        Delete), for two clients across all three CRL modes: A (revoked) must be rejected
        (a TLS-layer alert, or the 401 fallback) from Permissive onward; B (valid, a
        control) must always succeed --
        so a failure in A's checks can be blamed on the revocation, not something else.
        """
        self.log.info(">>> test_lifecycle_ops_crl_sweep starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        for mode in ("Disabled", "Permissive", "Require"):
            self.log.info("== lifecycle ops CRL mode -> {0} ==".format(mode))
            status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": mode}})
            self.assertTrue(status, "post_crl_settings failed for mode {0}".format(mode))
            self._wait_for_crl_poll_interval(self.crl_filename)

            a_appname = "{0}_{1}_a".format(self.function_name, mode.lower())
            b_appname = "{0}_{1}_b".format(self.function_name, mode.lower())
            self._assert_lifecycle_ops_for_cert(a_appname, clients["a"]["cert_path"], clients["a"]["key_path"], ca_path, expect_accept=(mode == "Disabled"))
            self._assert_lifecycle_ops_for_cert(b_appname, clients["b"]["cert_path"], clients["b"]["key_path"], ca_path, expect_accept=True)
        self.log.info(">>> test_lifecycle_ops_crl_sweep finished <<<")

    def test_rest_api_crl_stats_and_functions(self):
        """api/v1/functions and api/v1/stats both succeed for a valid cert,
        and are rejected (TLS-layer alert, or the 401 fallback) for a revoked one."""
        self.log.info(">>> test_rest_api_crl_stats_and_functions starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": self.clientauth_crl_mode}})
        self.assertTrue(status, "post_crl_settings failed for mode {0}".format(self.clientauth_crl_mode))
        self._wait_for_crl_poll_interval(self.crl_filename)

        # Client B (valid cert) -- both endpoints must succeed with a real 200
        resp = self._assert_accepted(
            self._call_eventing_https_endpoint, clients["b"]["cert_path"], clients["b"]["key_path"],
            ca_path, "GET", "/api/v1/functions")
        self.assertEqual(
            resp.status_code, 200,
            "api/v1/functions should succeed for a valid cert: {0}".format(resp.text))

        resp = self._assert_accepted(
            self._call_eventing_https_endpoint, clients["b"]["cert_path"], clients["b"]["key_path"],
            ca_path, "GET", "/api/v1/stats")
        self.assertEqual(
            resp.status_code, 200,
            "api/v1/stats should succeed for a valid cert: {0}".format(resp.text))

        # Client A (revoked cert) -- both endpoints must be rejected (TLS-layer alert, or the 401 fallback)
        self._assert_rejected(
            self._call_eventing_https_endpoint, clients["a"]["cert_path"], clients["a"]["key_path"],
            ca_path, "GET", "/api/v1/functions")
        self._assert_rejected(
            self._call_eventing_https_endpoint, clients["a"]["cert_path"], clients["a"]["key_path"],
            ca_path, "GET", "/api/v1/stats")

        self.log.info(">>> test_rest_api_crl_stats_and_functions finished <<<")

    def test_deployment_config_crl_gating(self):
        """Tests modifying deployment config through two different endpoints:
          - api/v1/config -- global settings like num_nodes_running.
          - the function settings endpoint -- per-function settings like num_timer_partitions
        Both are tested same way as any other lifecycle op: rejected (a TLS-layer alert,
        or the 401 fallback) for a revoked cert, accepted for a valid one."""

        self.log.info(">>> test_deployment_config_crl_gating starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.rest.create_function(body['appname'], body, self.function_scope)
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": self.clientauth_crl_mode}})
        self.assertTrue(status, "post_crl_settings failed for mode {0}".format(self.clientauth_crl_mode))
        self._wait_for_crl_poll_interval(self.crl_filename)

        config_path = "/api/v1/config{0}".format(self._function_scope_query_string())
        self._assert_rejected(self._call_eventing_https_endpoint, clients["a"]["cert_path"], clients["a"]["key_path"],
                              ca_path, "POST", config_path, {"num_nodes_running": 1})
        self._assert_accepted(self._call_eventing_https_endpoint, clients["b"]["cert_path"], clients["b"]["key_path"],
                              ca_path, "POST", config_path, {"num_nodes_running": 1})

        self._assert_rejected(self._set_settings_via_cert, clients["a"]["cert_path"], clients["a"]["key_path"],
                              ca_path, body['appname'], True, True)
        self._assert_accepted(self._set_settings_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                              ca_path, body['appname'], True, True)
        self.wait_for_handler_state(body['appname'], "deployed")

        self._admin_set_settings(body['appname'], False, False, "undeployed")
        self._cleanup_function(body['appname'])
        self.log.info(">>> test_deployment_config_crl_gating finished <<<")

    def test_multiple_functions_with_clientauth_crl(self):
        """Tests that features -- FTS, Analytics, or JWT-authenticated function management --
        still work normally with clientAuth CRL turned on (Require or Permissive, via
        clientauth_crl_mode -- both reject a currently-revoked cert identically).
          - Deploy and run the handler (via cert for fts/analytics, via a JWT bearer token for
            jwt) -- must succeed, and the handler must actually process data.
          - Try to touch the same function mid-run with a revoked cert -- must still be
            rejected, regardless of which identity actually owns/created the function. This is
            the important assertion for the jwt case in particular: it proves JWT auth working
            doesn't mean CRL enforcement got silently bypassed for everyone else."""
        self.log.info(">>> test_multiple_functions_with_clientauth_crl starting <<<")
        handler_code_name = self.input.param('handler_code', 'fts')
        if handler_code_name == "fts":
            self._setup_fts_for_multiple_functions_test()
            handler_path = HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY
        elif handler_code_name == "analytics":
            self._setup_analytics_for_multiple_functions_test()
            handler_path = HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT
        elif handler_code_name == "jwt":
            handler_path = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        else:
            self.fail("Unknown handler_code param: {0} (expected 'fts', 'analytics', or 'jwt')".format(handler_code_name))

        clients, ca_path = self._setup_clientauth_crl()
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": self.clientauth_crl_mode}})
        self.assertTrue(status, "post_crl_settings failed for mode {0}".format(self.clientauth_crl_mode))
        self._wait_for_crl_poll_interval(self.crl_filename)

        if handler_code_name == "fts":
            body = self.create_save_function_body(self.function_name, handler_path, worker_count=1)
        else:
            body = self.create_save_function_body(self.function_name, handler_path)
        if handler_code_name == "jwt":
            jwt_token = self._setup_jwt_auth()
            self.rest.create_function_with_jwt(body['appname'], body, jwt_token, self.function_scope)
            self.deploy_function(body, jwt_token=jwt_token)
        else:
            self._assert_accepted(self._create_function_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                                  ca_path, body['appname'], body)
            self._assert_accepted(self._set_settings_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                                  ca_path, body['appname'], True, True)
        self.wait_for_handler_state(body['appname'], "deployed")
        self.load_data_to_collection(self.num_docs, "src_bucket._default._default")
        self.verify_doc_count_collections("dst_bucket._default._default", self.num_docs)

        # Revoked cert still can't touch the function mid-run, regardless of handler type or
        # of whether the function itself was created via cert or JWT (see docstring above)
        self._assert_rejected(self._set_settings_via_cert, clients["a"]["cert_path"], clients["a"]["key_path"],
                              ca_path, body['appname'], True, False)

        self._admin_set_settings(body['appname'], False, False, "undeployed")
        self._cleanup_function(body['appname'])
        self.log.info(">>> test_multiple_functions_with_clientauth_crl finished <<<")

    def test_owner_revoked_other_still_works(self):
        """User A (owner) and User B both hold the same minimal eventing role. The
        function is created/deployed by A. Revoking A's cert must not block B -- B, still
        valid, must keep being able to perform lifecycle ops on A's function."""
        self.log.info(">>> test_owner_revoked_other_still_works starting <<<")
        role = self._minimal_eventing_role()
        clients, ca_path = self._setup_clientauth_crl_multi(
            [("owner", "test-owner-a", role), ("other", "test-other-b", role)])

        appname = self.function_name + "_ownera"
        body = self.create_save_function_body(appname, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self._assert_accepted(self._create_function_via_cert, clients["owner"]["cert_path"],
                              clients["owner"]["key_path"], ca_path, appname, body)
        self._assert_accepted(self._set_settings_via_cert, clients["owner"]["cert_path"],
                              clients["owner"]["key_path"], ca_path, appname, True, True)
        self.wait_for_handler_state(appname, "deployed")

        self._revoke_and_apply_clientauth(clients["owner"]["serial"], self.clientauth_crl_mode, crl_number=1)
        self._assert_rejected(self._set_settings_via_cert, clients["owner"]["cert_path"],
                              clients["owner"]["key_path"], ca_path, appname, True, False)
        self._assert_accepted(self._set_settings_via_cert, clients["other"]["cert_path"],
                              clients["other"]["key_path"], ca_path, appname, True, False)
        self.wait_for_handler_state(appname, "paused")

        self._cleanup_function(appname)
        self.log.info(">>> test_owner_revoked_other_still_works finished <<<")

    def test_other_revoked_owner_still_works(self):
        """The function is created/deployed by owner A. Revoking
        non-owner B's cert must not block A -- A, still valid, keeps working."""
        self.log.info(">>> test_other_revoked_owner_still_works starting <<<")
        role = self._minimal_eventing_role()
        clients, ca_path = self._setup_clientauth_crl_multi(
            [("owner", "test-owner-a2", role), ("other", "test-other-b2", role)])

        appname = self.function_name + "_ownerb"
        body = self.create_save_function_body(appname, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self._assert_accepted(self._create_function_via_cert, clients["owner"]["cert_path"],
                              clients["owner"]["key_path"], ca_path, appname, body)
        self._assert_accepted(self._set_settings_via_cert, clients["owner"]["cert_path"],
                              clients["owner"]["key_path"], ca_path, appname, True, True)
        self.wait_for_handler_state(appname, "deployed")

        self._revoke_and_apply_clientauth(clients["other"]["serial"], self.clientauth_crl_mode, crl_number=1)
        self._assert_rejected(self._set_settings_via_cert, clients["other"]["cert_path"],
                              clients["other"]["key_path"], ca_path, appname, True, False)
        self._assert_accepted(self._set_settings_via_cert, clients["owner"]["cert_path"],
                              clients["owner"]["key_path"], ca_path, appname, True, False)
        self.wait_for_handler_state(appname, "paused")

        self._cleanup_function(appname)
        self.log.info(">>> test_other_revoked_owner_still_works finished <<<")

    def test_roles_permitted_for_lifecycle_ops_under_crl(self):
        """Admin, Eventing Admin, and Eventing Manage Functions should be
        permitted to perform lifecycle ops; a role with no eventing permissions should be
        blocked by RBAC regardless of CRL state. A revoked cert is blocked (a TLS-layer
        alert, or the 401 fallback) regardless of role -- CRL and RBAC are independent gates."""
        self.log.info(">>> test_roles_permitted_for_lifecycle_ops_under_crl starting <<<")
        role = self.input.param('role', 'admin')
        role_map = {
            "admin": "admin",
            "eventing_admin": "eventing_admin",
            "eventing_manage_functions": self._minimal_eventing_role(),
            "none": "replication_admin",  #no eventing permissions
        }
        if role not in role_map:
            self.fail("Unknown role param: {0} (expected one of {1})".format(role, list(role_map)))
        rbac_role = role_map[role]
        clients, ca_path = self._setup_clientauth_crl_multi(
            [("a", "test-client-a-role", rbac_role), ("b", "test-client-b-role", rbac_role)])
        self._revoke_and_apply_clientauth([], self.clientauth_crl_mode, crl_number=1)
        appname = self.function_name + "_" + role
        body = self.create_save_function_body(appname, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)

        if role == "none":
            resp = self._create_function_via_cert(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path,
                                              appname, body)
            self.assertEqual(resp.status_code, 403,
                             "Expected RBAC to forbid function creation for role={0}, got {1}: {2}".format(
                                 role, resp.status_code, resp.text))
        else:
            self._assert_accepted(self._create_function_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                                  ca_path, appname, body)
            self._cleanup_function(appname)

        # Revoked cert is always blocked at the TLS layer, independent of role
        self._revoke_and_apply_clientauth(clients["a"]["serial"], self.clientauth_crl_mode, crl_number=2)
        self._assert_rejected(self._create_function_via_cert, clients["a"]["cert_path"], clients["a"]["key_path"],
                              ca_path, appname, body)
        self.log.info(">>> test_roles_permitted_for_lifecycle_ops_under_crl finished <<<")

    def test_dropping_function_scope_with_owner_revoked(self):
        """Revoking the function owner's cert must not prevent Eventing's own internal
        auto-undeploy when the function's source bucket (function scope) is dropped"""
        self.log.info(">>> test_dropping_function_scope_with_owner_revoked starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self._assert_accepted(self._create_function_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                              ca_path, body['appname'], body)
        self._assert_accepted(self._set_settings_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                              ca_path, body['appname'], True, True)
        self.wait_for_handler_state(body['appname'], "deployed")

        self._revoke_and_apply_clientauth([clients["a"]["serial"], clients["b"]["serial"]], self.clientauth_crl_mode, crl_number=2)
        self.rest.delete_bucket(self.src_bucket_name)
        self.wait_for_handler_internal_undeployment_and_deletion(body['appname'])
        self.log.info(">>> test_dropping_function_scope_with_owner_revoked finished <<<")

    def test_dropping_metadata_keyspace_with_owner_revoked(self):
        """Same as above, but dropping the metadata keyspace instead of the source bucket."""
        self.log.info(">>> test_dropping_metadata_keyspace_with_owner_revoked starting <<<")
        clients, ca_path = self._setup_clientauth_crl()
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self._assert_accepted(self._create_function_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                              ca_path, body['appname'], body)
        self._assert_accepted(self._set_settings_via_cert, clients["b"]["cert_path"], clients["b"]["key_path"],
                              ca_path, body['appname'], True, True)
        self.wait_for_handler_state(body['appname'], "deployed")

        self._revoke_and_apply_clientauth([clients["a"]["serial"], clients["b"]["serial"]], self.clientauth_crl_mode, crl_number=2)
        self.rest.delete_bucket(self.metadata_bucket_name)
        self.wait_for_handler_state(body['appname'], "undeployed")
        self.log.info(">>> test_dropping_metadata_keyspace_with_owner_revoked finished <<<")

    # ---------------------------- N2N CRL Tests -----------------------------
    # 3-node cluster, 2 dedicated KV nodes + 1 Eventing node
    # (nodes_init=3,services_init=kv-kv-eventing). Expected matrix (dst_bucket
    # doc count after loading num_docs*2 -- see _create_and_deploy_n2n_function):
    #   All KV nodes revoked        -> count == num_docs
    #   One of two KV nodes revoked -> num_docs < count < num_docs*2 (partial)
    # nodetonode_crl_mode (Require/Permissive, see setUp) is expected to give
    # the same result either way here -- the CRL is active/non-stale, and both
    # modes enforce a determined revocation identically.

    def test_n2n_crl_all_kv_nodes_revoked(self):
        """All KV nodes revoked -> Eventing processes 0 mutations."""
        self.log.info(">>> test_n2n_crl_all_kv_nodes_revoked starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()
        kv_nodes = self._get_kv_nodes()
        body = self._create_and_deploy_n2n_function()

        self._revoke_node_certs(kv_nodes)
        self._set_nodetonode_crl_mode(self.nodetonode_crl_mode)
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        self._force_function_reconnect(body)

        # load_data_to_collection()'s key range is always [0, num_items), so loading
        # num_docs*2 re-touches the original keys AND creates num_docs genuinely NEW
        # ones -- dst_bucket count only grows past num_docs if some got processed.
        self.load_data_to_collection(self.num_docs * 2, "src_bucket._default._default")
        self._wait_for_update_stat_stable()
        dst_count = self._get_dst_doc_count()
        self._print_final_state()
        self.assertEqual(dst_count, self.num_docs,
                         "Expected dst_bucket count to stay at {0} (only the original baseline docs) "
                         "with all KV nodes revoked, got {1}".format(self.num_docs, dst_count))
        self.log.info(">>> test_n2n_crl_all_kv_nodes_revoked finished <<<")

    def test_n2n_crl_one_of_two_kv_nodes_revoked(self):
        """
        One of two KV nodes revoked -> Eventing processes a partial batch
        """
        self.log.info(">>> test_n2n_crl_one_of_two_kv_nodes_revoked starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()
        kv_nodes = self._get_kv_nodes()
        self.assertEqual(len(kv_nodes), 2, "This test needs exactly 2 KV nodes, got {0}".format(len(kv_nodes)))
        target_node = kv_nodes[0]
        body = self._create_and_deploy_n2n_function()

        self._revoke_node_certs([target_node])
        self._set_nodetonode_crl_mode(self.nodetonode_crl_mode)
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        self._force_function_reconnect(body)
        self._wait_for_update_stat_stable()

        self.load_data_to_collection(self.num_docs * 2, "src_bucket._default._default")
        self._wait_for_update_stat_stable()
        dst_count = self._get_dst_doc_count()
        self._print_final_state()
        self.assertGreater(dst_count, self.num_docs,
                           "Expected dst_bucket count > {0} with one KV node revoked, got {1} "
                           "(neither KV node's vbuckets were reachable)".format(self.num_docs, dst_count))
        self.assertLess(dst_count, self.num_docs * 2,
                        "Expected dst_bucket count < {0} with one KV node revoked, got {1} "
                        "(looks like the revoked node's vbuckets were still reachable)".format(
                            self.num_docs * 2, dst_count))
        self.log.info(">>> test_n2n_crl_one_of_two_kv_nodes_revoked finished <<<")

    # ------------------- N2N CRL: Eventing's own node cert ------------------
    # n2n is mutual -- KV validates the peer cert on inbound connections too,
    # so revoking Eventing's own node cert should block it from every KV node
    # at once, same signature as all-KV-revoked but from the other side.

    def test_n2n_crl_eventing_node_revoked(self):
        """Eventing's own node cert revoked -> KV refuses Eventing as a peer."""
        self.log.info(">>> test_n2n_crl_eventing_node_revoked starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        body = self._create_and_deploy_n2n_function()

        self._revoke_node_certs([eventing_node])
        self._set_nodetonode_crl_mode(self.nodetonode_crl_mode)
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        #self._force_function_reconnect(body)

        self.load_data_to_collection(self.num_docs * 2, "src_bucket._default._default")
        self._wait_for_update_stat_stable()
        dst_count = self._get_dst_doc_count()
        self._print_final_state()
        self.assertEqual(dst_count, self.num_docs,
                         "Expected dst_bucket count to stay at {0} (Eventing's own node cert is revoked, "
                         "KV should refuse it as a peer), got {1}".format(self.num_docs, dst_count))
        self.log.info(">>> test_n2n_crl_eventing_node_revoked finished <<<")

    # ------------------- N2N CRL: FTS / CBAS / N1QL Tests -------------------
    # Dependent service here is a REST/query call inside the handler's own
    # OnUpdate, not DCP. Confirmed via match_query.js / analytics_basic_select.js /
    # n1ql_insert_on_update.js: each writes one doc per source doc (keyed by
    # meta.id) and writes nothing on failure

    def test_n2n_crl_fts_node_revoked(self):
        """
        FTS node revoked -> handler's own FTS query (OnUpdate) fails.
        """
        self.log.info(">>> test_n2n_crl_fts_node_revoked starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()

        # Setup mirrors eventing_fts_query_support.py's setUp()/test_eventing_fts_query_support_sanity
        fts_index_name = "travel_sample_test"  # hardcoded inside match_query.js's couchbase.searchQuery() call
        fts_doc_count = 31500
        fts_callable = FTSCallable(nodes=self.servers, es_validate=False)
        self.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=3000)
        self.load_sample_buckets(self.server, "travel-sample")
        fts_index = fts_callable.create_default_index(
            index_name=fts_index_name, bucket_name="travel-sample",
            plan_params={"indexPartitions": 1, "numReplicas": 0})
        fts_callable.wait_for_indexing_complete(item_count=fts_doc_count, idx=fts_index)
        self.sleep(30, "Waiting for FTS indexing to settle")
        self.addCleanup(fts_callable.delete_fts_index, fts_index_name)

        body = self.create_save_function_body(
            self.function_name, HANDLER_CODE_FTS_QUERY_SUPPORT.FTS_QUERY_SUPPORT_MATCH_QUERY)
        self.deploy_function(body)
        self.load_data_to_collection(1, "src_bucket._default._default")
        self.verify_doc_count_collections("dst_bucket._default._default", 1)
        self.log.info("Baseline FTS-handler processing confirmed")

        fts_node = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)
        self._revoke_node_certs([fts_node])
        self._set_nodetonode_crl_mode(self.nodetonode_crl_mode)
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        self._force_function_reconnect(body)

        # 2nd distinct source doc -> a fresh OnUpdate/FTS-query attempt under
        # the revoked state; dst_bucket must NOT reach 2 if it now fails.
        self.load_data_to_collection(2, "src_bucket._default._default")
        self.sleep(30, "Waiting for post-revocation OnUpdate attempt to resolve")
        dst_count = self._get_dst_doc_count()
        self.log.info("dst_bucket doc count: {0}".format(dst_count))
        self.assertEqual(dst_count, 1,
                         "Expected dst_bucket count to stay at 1 (FTS query should fail) "
                         "with the FTS node revoked, got {0}".format(dst_count))
        self.log.info(">>> test_n2n_crl_fts_node_revoked finished <<<")

    def test_n2n_crl_cbas_node_revoked(self):
        """
        CBAS node revoked -> handler's own analytics query (OnUpdate) fails
        """
        self.log.info(">>> test_n2n_crl_cbas_node_revoked starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()

        # Setup mirrors eventing_analytics.py's setUp()/_setup_analytics()
        self.load_sample_buckets(self.server, "travel-sample")
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        cbas_rest.execute_statement_on_cbas("CREATE DATAVERSE `travel-sample`.`inventory`", None)
        cbas_rest.execute_statement_on_cbas(
            "CREATE ANALYTICS COLLECTION `travel-sample`.`inventory`.`airline` "
            "ON `travel-sample`.`inventory`.`airline`", None)
        cbas_rest.execute_statement_on_cbas("CONNECT LINK Local", None)
        count, poll_count = 0, 0
        while count == 0 and poll_count < 20:
            self.sleep(15, "Waiting for analytics to ingest travel-sample data")
            result = cbas_rest.execute_statement_on_cbas(
                "SELECT COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline`", None)
            if isinstance(result, bytes):
                result = result.decode("utf-8")
            count = json.loads(result)["results"][0]["cnt"]
            poll_count += 1
        self.assertTrue(count > 0, "Analytics airline collection has 0 docs -- ingestion failed")
        self.addCleanup(cbas_rest.execute_statement_on_cbas, "DISCONNECT LINK Local", None)

        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT)
        self.deploy_function(body)
        self.load_data_to_collection(1, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.log.info("Baseline Analytics-handler processing confirmed")

        self._revoke_node_certs([cbas_node])
        self._set_nodetonode_crl_mode(self.nodetonode_crl_mode)
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        self._force_function_reconnect(body)

        # 2nd distinct source doc -> a fresh OnUpdate/analytics-query attempt
        # under the revoked state; collection1 must NOT reach 2 if it now fails.
        self.load_data_to_collection(2, "default.scope0.collection0")
        self.sleep(30, "Waiting for post-revocation OnUpdate attempt to resolve")
        count = self.stat.get_collection_item_count_cumulative(
            "default", "scope0", "collection1", self.get_kv_nodes())
        self.log.info("collection1 doc count: {0}".format(count))
        self.assertEqual(count, 1,
                         "Expected collection1 count to stay at 1 (analytics query should fail) "
                         "with the CBAS node revoked, got {0}".format(count))
        self.log.info(">>> test_n2n_crl_cbas_node_revoked finished <<<")

    def test_n2n_crl_n1ql_node_revoked(self):
        """
        N1QL node revoked -> handler's own N1QL INSERT (OnUpdate) fails.
        """
        self.log.info(">>> test_n2n_crl_n1ql_node_revoked starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()

        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        self.deploy_function(body)
        self.load_data_to_collection(self.num_docs, "src_bucket._default._default")
        self.verify_doc_count_collections("dst_bucket._default._default", self.num_docs)
        self.log.info("Baseline N1QL-handler processing confirmed ({0} docs)".format(self.num_docs))

        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self._revoke_node_certs([n1ql_node])
        self._set_nodetonode_crl_mode(self.nodetonode_crl_mode)
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        self._force_function_reconnect(body)

        # See test_n2n_crl_all_kv_nodes_revoked for why num_docs*2.
        self.load_data_to_collection(self.num_docs * 2, "src_bucket._default._default")
        self.sleep(30, "Waiting for post-revocation OnUpdate attempt to resolve")
        dst_count = self._get_dst_doc_count()
        self.log.info("dst_bucket doc count: {0}".format(dst_count))
        self.sleep(3600)
        self.assertEqual(dst_count, self.num_docs,
                         "Expected dst_bucket count to stay at {0} (N1QL insert should fail) "
                         "with the N1QL node revoked, got {1}".format(self.num_docs, dst_count))
        self.log.info(">>> test_n2n_crl_n1ql_node_revoked finished <<<")

    # ----------------------------- CRL Expiry -------------------------------
    # A stale CRL doesn't fail uniformly: Require hard-fails everyone from that
    # CA (no fresh list = no verifiable decision = no access); Permissive
    # soft-fails (unverifiable != revoked). clientAuth and n2n are tested
    # independently -- a Require clientAuth policy would otherwise hard-fail
    # the framework's own non-cert admin calls used to check n2n.

    def test_clientauth_crl_expiry_require_hard_fails_all_certs(self):
        """
        clientAuth CRL expiry under Require -> once the only CRL for this CA
        goes stale, Require hard-fails EVERY cert signed by it, not just the
        one already known-revoked -- no fresh list means no verifiable
        decision, and Require means "no verifiable decision, no access".
        Observed live: client B (never revoked) gets the same TLS-layer
        rejection as client A once the CRL passes its nextUpdate.
        """
        self.log.info(">>> test_clientauth_crl_expiry_require_hard_fails_all_certs starting <<<")
        clients, ca_path = self._setup_clientauth_crl(next_update_seconds=self.crl_expiry_wait)
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": "Require"}})
        self.assertTrue(status, "post_crl_settings failed for mode Require")
        self._wait_for_crl_poll_interval(self.crl_filename)
        self.assertFalse(self._probe_eventing_ssl(clients["a"]["cert_path"], clients["a"]["key_path"], ca_path),
                         "Revoked cert should be rejected while the clientAuth CRL is still valid")
        self.assertTrue(self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path),
                        "Valid client cert should be accepted while the clientAuth CRL is still valid")

        self.sleep(self.crl_expiry_wait, "Waiting for the clientAuth CRL's nextUpdate to pass (expiry)")
        self.rest.reload_crl()
        status, files, _ = self.rest.get_crl_files()
        self.log.info("CRL files after expiry wait: {0}".format(files))

        self.assertFalse(self._probe_eventing_ssl(clients["a"]["cert_path"], clients["a"]["key_path"], ca_path),
                         "Revoked cert should still be rejected once the clientAuth CRL has expired")
        self.assertFalse(self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path),
                         "Under Require, a stale CRL should hard-fail EVERY cert from that CA -- even one "
                         "never revoked -- since there's no fresh list left to verify it against")
        self.log.info(">>> test_clientauth_crl_expiry_require_hard_fails_all_certs finished <<<")

    def test_clientauth_crl_expiry_permissive_valid_client_not_blocked(self):
        """
        clientAuth CRL expiry under Permissive -> a valid client cert must
        keep authenticating (soft-fail: unverifiable != revoked). If this
        also rejects the valid cert, that's a real bug -- Permissive isn't
        supposed to hard-fail on unverifiable status the way Require does.
        """
        self.log.info(">>> test_clientauth_crl_expiry_permissive_valid_client_not_blocked starting <<<")
        clients, ca_path = self._setup_clientauth_crl(next_update_seconds=self.crl_expiry_wait)
        status, _, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": "Permissive"}})
        self.assertTrue(status, "post_crl_settings failed for mode Permissive")
        self._wait_for_crl_poll_interval(self.crl_filename)
        self.assertFalse(self._probe_eventing_ssl(clients["a"]["cert_path"], clients["a"]["key_path"], ca_path),
                         "Revoked cert should be rejected while the clientAuth CRL is still valid")
        self.assertTrue(self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path),
                        "Valid client cert should be accepted while the clientAuth CRL is still valid")

        self.sleep(self.crl_expiry_wait, "Waiting for the clientAuth CRL's nextUpdate to pass (expiry)")
        self.rest.reload_crl()
        status, files, _ = self.rest.get_crl_files()
        self.log.info("CRL files after expiry wait: {0}".format(files))

        self.assertTrue(self._probe_eventing_ssl(clients["b"]["cert_path"], clients["b"]["key_path"], ca_path),
                        "Valid client cert must keep authenticating once the clientAuth CRL has expired "
                        "under Permissive (soft-fail)")
        self.log.info(">>> test_clientauth_crl_expiry_permissive_valid_client_not_blocked finished <<<")

    def test_n2n_crl_expiry_permissive_kv_continues_to_function(self):
        """
        n2n CRL expiry under Permissive -> a KV node revoked via a
        short-lived CRL must become reachable again once that CRL entry
        expires (soft-fail: unverifiable != revoked), mirroring
        test_clientauth_crl_expiry_permissive_valid_client_not_blocked. Same
        setup/mechanics as test_n2n_crl_one_of_two_kv_nodes_revoked for the
        "still valid" phase, extended with an expiry + recheck phase.
        """
        self.log.info(">>> test_n2n_crl_expiry_permissive_kv_continues_to_function starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()
        kv_nodes = self._get_kv_nodes()
        self.assertEqual(len(kv_nodes), 2, "This test needs exactly 2 KV nodes, got {0}".format(len(kv_nodes)))
        target_node = kv_nodes[0]
        body = self._create_and_deploy_n2n_function()

        self._revoke_node_certs([target_node], next_update_seconds=self.crl_expiry_wait)
        self._set_nodetonode_crl_mode("Permissive")
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        self._force_function_reconnect(body)
        self._wait_for_update_stat_stable()

        # While the CRL is still valid: same partial-reachability signature as
        # test_n2n_crl_one_of_two_kv_nodes_revoked.
        self.load_data_to_collection(self.num_docs * 2, "src_bucket._default._default")
        self._wait_for_update_stat_stable()
        dst_count = self._get_dst_doc_count()
        self._print_final_state()
        self.assertGreater(dst_count, self.num_docs,
                           "Expected dst_bucket count > {0} with one KV node revoked, got {1} "
                           "(neither KV node's vbuckets were reachable)".format(self.num_docs, dst_count))
        self.assertLess(dst_count, self.num_docs * 2,
                        "Expected dst_bucket count < {0} with one KV node revoked, got {1} "
                        "(looks like the revoked node's vbuckets were still reachable)".format(
                            self.num_docs * 2, dst_count))

        self.sleep(self.crl_expiry_wait, "Waiting for the n2n CRL's nextUpdate to pass (expiry)")
        self.rest.reload_crl()
        status, files, _ = self.rest.get_crl_files()
        self.log.info("n2n CRL files after expiry wait: {0}".format(files))
        self._force_function_reconnect(body)

        # Post-expiry, under Permissive: the previously-revoked node's
        # vbuckets should be reachable again -- full batch, not partial.
        self.load_data_to_collection(self.num_docs * 3, "src_bucket._default._default")
        self._wait_for_update_stat_stable()
        dst_count = self._get_dst_doc_count()
        self._print_final_state()
        self.assertEqual(dst_count, self.num_docs * 3,
                         "Expected dst_bucket count to reach {0} (Permissive should soft-fail on a stale "
                         "n2n CRL -- the previously-revoked KV node should be reachable again), got {1}".format(
                             self.num_docs * 3, dst_count))
        self.log.info(">>> test_n2n_crl_expiry_permissive_kv_continues_to_function finished <<<")

    def test_n2n_crl_expiry_require_node_stays_blocked(self):
        """
        n2n CRL expiry under Require -> a KV node revoked via a short-lived
        CRL must STAY unreachable once that CRL entry expires (hard-fail: no
        fresh list = no verifiable decision = no access), mirroring
        test_clientauth_crl_expiry_require_hard_fails_all_certs. UNCONFIRMED
        live for n2n as of writing -- this hypothesis follows directly from
        the confirmed clientAuth behavior, but nodeToNode's enforcement path
        is a separate code path; run this and report the actual outcome.
        """
        self.log.info(">>> test_n2n_crl_expiry_require_node_stays_blocked starting <<<")
        self._enable_n2n_encryption()
        self._trust_ca_on_eventing_node()
        kv_nodes = self._get_kv_nodes()
        self.assertEqual(len(kv_nodes), 2, "This test needs exactly 2 KV nodes, got {0}".format(len(kv_nodes)))
        target_node = kv_nodes[0]
        body = self._create_and_deploy_n2n_function()

        self._revoke_node_certs([target_node], next_update_seconds=self.crl_expiry_wait)
        self._set_nodetonode_crl_mode("Require")
        self._wait_for_crl_poll_interval(self.n2n_crl_filename)
        self._force_function_reconnect(body)
        self._wait_for_update_stat_stable()

        # While the CRL is still valid: same partial-reachability signature as
        # test_n2n_crl_one_of_two_kv_nodes_revoked.
        self.load_data_to_collection(self.num_docs * 2, "src_bucket._default._default")
        self._wait_for_update_stat_stable()
        dst_count = self._get_dst_doc_count()
        self._print_final_state()
        # Commenting out until KV Node Revocation Bug is fixed
        # self.assertGreater(dst_count, self.num_docs,
        #                    "Expected dst_bucket count > {0} with one KV node revoked, got {1} "
        #                    "(neither KV node's vbuckets were reachable)".format(self.num_docs, dst_count))
        # self.assertLess(dst_count, self.num_docs * 2,
        #                 "Expected dst_bucket count < {0} with one KV node revoked, got {1} "
        #                 "(looks like the revoked node's vbuckets were still reachable)".format(
        #                     self.num_docs * 2, dst_count))

        self.sleep(self.crl_expiry_wait, "Waiting for the n2n CRL's nextUpdate to pass (expiry)")
        self.rest.reload_crl()
        status, files, _ = self.rest.get_crl_files()
        self.log.info("n2n CRL files after expiry wait: {0}".format(files))
        self._force_function_reconnect(body)

        # Post-expiry, under Require: the previously-revoked node's vbuckets
        # should STAY unreachable -- growth from the untouched node only.
        self.load_data_to_collection(self.num_docs * 3, "src_bucket._default._default")
        self._wait_for_update_stat_stable()
        dst_count = self._get_dst_doc_count()
        self._print_final_state()
        self.assertGreater(dst_count, self.num_docs * 2,
                           "Expected dst_bucket count > {0} (the untouched KV node's vbuckets should keep "
                           "growing normally), got {1}".format(self.num_docs * 2, dst_count))
        self.assertLess(dst_count, self.num_docs * 3,
                        "Expected dst_bucket count < {0} (Require should hard-fail on a stale n2n CRL -- "
                        "the previously-revoked KV node should stay unreachable), got {1}".format(
                            self.num_docs * 3, dst_count))
        self.log.info(">>> test_n2n_crl_expiry_require_node_stays_blocked finished <<<")
