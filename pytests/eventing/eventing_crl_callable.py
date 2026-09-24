"""
EventingCRLCallable -- CRL test helper for eventing suites that don't inherit
CRLBase/EventingCRL (rebalance, recovery, failover, security, ...)

Owns:
  - CA/node cert issue + trust + deploy
  - CRL upload + policy mode for both scopes:
      clientAuth -- admin calls to Eventing's own HTTPS port
      nodeToNode -- Eventing dialing out to KV/FTS/CBAS/N1QL
  - cert-based REST calls
  - CRL-diagnostics polling

Quick start:

    crl = EventingCRLCallable(self.master, self.servers, log=self.log)
    crl.enable_n2n_encryption()
    kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
    eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
    crl.trust_ca_on_cluster(crl.ca_cert, server=eventing_node)
    body = self.create_save_function_body(self.function_name, "handler_code/delete_doc_bucket_op.js")
    self.deploy_function(body)

    crl.revoke_node_certs([kv_node])
    crl.set_nodetonode_crl_mode("Require")
    crl.wait_for_crl_poll_interval(crl.n2n_crl_filename)
    self.kill_producer(eventing_node)  # force reconnect under the new CRL state
    self.sleep(120, "Waiting for eventing-producer to respawn")
    self.wait_for_handler_state(body['appname'], "deployed")
    ...
    crl.cleanup()  # call from tearDown
"""
import datetime
import json
import os
import ssl
import tempfile
import time

import requests
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.crl_utils import CRLUtils
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.rbac_base import RbacBase
from pytests.security.x509main import x509main


class EventingCRLCallable:
    # CNs of the test CAs this helper and EventingCRL/CRLBase generate -- a node cert
    # issued by one of these is a leftover from an earlier CRL test run
    TEST_CA_CNS = ("EventingCRLTestCA", "TestCA1")

    def __init__(self, master, servers, log=None, ca_cn="EventingCRLTestCA",
                 crl_filename="eventing_crl_test.pem",
                 n2n_crl_filename="eventing_n2n_crl_test.pem",
                 eventing_ssl_port=18096):
        self.master = master
        self.servers = servers
        self.log = log
        self.rest = RestConnection(master)
        self.crl_utils = CRLUtils(log=log)
        self.crl_filename = crl_filename
        self.n2n_crl_filename = n2n_crl_filename
        self.eventing_ssl_port = eventing_ssl_port
        self._created_files = []
        self._rbac_users = []
        self._temp_files = []
        self._n2n_enabled = False
        # nodes given a test-CA cert/trust -- reset in cleanup() so a later test's
        # fresh CA doesn't find a cert it can't verify (addNode: "Unknown CA")
        self._cert_touched_nodes = {}
        self._clientauth_crl_number = 0
        self._n2n_crl_number = 0

        if not self.rest.is_enterprise_edition():
            raise RuntimeError("CRL support requires an Enterprise Edition cluster.")

        self.reset_stale_test_certs(set(self.TEST_CA_CNS) | {ca_cn})
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca(ca_cn)
        self.trust_ca_on_cluster(self.ca_cert, server=master)
        self._log("EventingCRLCallable ready, CA={0}, trusted on master {1}".format(ca_cn, master.ip))

    def _log(self, msg):
        if self.log:
            self.log.info(msg)

    # ---- CA / node cert plumbing ----

    def trust_ca_on_cluster(self, ca_cert, server=None):
        """
        Write ca_cert's PEM into the node's real inbox/CA folder and instruct
        the cluster to load it (POST /node/controller/loadTrustedCAs).
        """
        server = server or self.master
        self._cert_touched_nodes[server.ip] = server
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)
        install_path = x509main(host=server).install_path
        ca_dir = "{0}{1}/CA".format(install_path, x509main.CHAINFILEPATH)
        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command("mkdir -p {0}".format(ca_dir))
            local_path = self._write_temp_pem(pem_bytes)
            shell.copy_file_local_to_remote(local_path, "{0}/crl_test_ca.pem".format(ca_dir))
        finally:
            shell.disconnect()
        # loadTrustedCAs is per-node -- must be called against `server` itself,
        # not whatever self.rest happens to be pinned to.
        node_rest = RestConnection(server) if server.ip != self.master.ip else self.rest
        status, content = node_rest.load_trusted_CAs()
        if not status:
            raise RuntimeError("Failed to load trusted CAs on {0}: {1}".format(server.ip, content))
        self._log("CA trusted on {0}".format(server.ip))

    def deploy_node_cert(self, server):
        """
        Issue a CA-signed node cert, push it over SSH, activate via
        reloadCertificate. Returns the serial (revoke target later).
        """
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, cn=server.ip, dns_names=[server.ip],
            extended_key_usage=[ExtendedKeyUsageOID.SERVER_AUTH],
        )
        self._cert_touched_nodes[server.ip] = server
        install_path = x509main(host=server).install_path
        node_dir = "{0}{1}".format(install_path, x509main.CHAINFILEPATH)
        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command("mkdir -p {0}".format(node_dir))
            for filename, pem_bytes in [("chain.pem", self.crl_utils.cert_to_pem(cert)),
                                         ("pkey.key", self.crl_utils.key_to_pem(key))]:
                local_path = self._write_temp_pem(pem_bytes)
                shell.copy_file_local_to_remote(local_path, "{0}/{1}".format(node_dir, filename))
        finally:
            shell.disconnect()
        node_rest = RestConnection(server) if server.ip != self.master.ip else self.rest
        status, content = node_rest.reload_certificate()
        if not status:
            raise RuntimeError("reloadCertificate failed on {0}: {1}".format(server.ip, content))
        self._log("Node cert deployed + activated on {0} (serial={1})".format(server.ip, serial))
        self.verify_active_node_cert(server, serial)
        return serial

    def get_active_node_cert_serial(self, server):
        """Serial of `server`'s active cert, or None if it can't be read."""
        status, content = self.rest.get_node_certificate_by_name(server.ip)
        if not status:
            # not a cluster member (e.g. a node about to be rebalanced in) -- the
            # cluster can't report its cert, so read the one it presents instead
            try:
                pem = ssl.get_server_certificate((server.ip, 18091), timeout=10)
                return x509.load_pem_x509_certificate(pem.encode()).serial_number
            except Exception as exc:
                self._log("Could not fetch active cert for node {0}: {1} / {2}".format(server.ip, content, exc))
                return None
        try:
            cert_info = json.loads(content) if isinstance(content, (str, bytes)) else content
            pem = cert_info.get('pem') or cert_info.get('cert')
            if not pem:
                self._log("No pem/cert field in cert response for node {0}".format(server.ip))
                return None
            pem_bytes = pem.encode() if isinstance(pem, str) else pem
            return x509.load_pem_x509_certificate(pem_bytes).serial_number
        except Exception as e:
            self._log("Could not parse active cert for node {0}: {1}".format(server.ip, e))
            return None

    def verify_active_node_cert(self, server, expected_serial):
        """Log MATCH/MISMATCH of `server`'s active cert vs `expected_serial`; True on a match."""
        actual_serial = self.get_active_node_cert_serial(server)
        match = actual_serial == expected_serial
        self._log("Active cert serial on node {0}: {1} (expected {2}) -> {3}".format(
            server.ip, actual_serial, expected_serial, "MATCH" if match else "MISMATCH"))
        return match

    # ---- Node-to-node (n2n) CRL ----

    def enable_n2n_encryption(self, level="all"):
        current_level = self.rest.get_pools_default().get("clusterEncryptionLevel")
        if current_level in ("all", "strict"):
            # already on (e.g. an upgrade suite set strict) -- don't downgrade it, and
            # leave _n2n_enabled False so cleanup() doesn't turn off what it didn't turn on
            self._log("n2n encryption already on (clusterEncryptionLevel={0}), leaving it as is".format(
                current_level))
            return
        self._log("Enabling n2n encryption cluster-wide (level={0})".format(level))
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=level)
        actual_level = self.rest.get_pools_default().get("clusterEncryptionLevel")
        if actual_level != level:
            raise RuntimeError("n2n encryption enable failed: expected clusterEncryptionLevel={0}, got {1}".format(level, actual_level))
        self._n2n_enabled = True

    def disable_n2n_encryption(self):
        if self._n2n_enabled:
            try:
                ntonencryptionBase().disable_nton_cluster([self.master])
            except Exception as exc:
                self._log("Failed to disable n2n encryption: {0}".format(exc))
            actual_level = self.rest.get_pools_default().get("clusterEncryptionLevel")
            if actual_level not in (None, "control"):
                raise RuntimeError("n2n encryption disable failed: cluster still reports clusterEncryptionLevel={0}".format(actual_level))
            self._n2n_enabled = False

    def revoke_node_certs(self, nodes_to_revoke, next_update_seconds=None):
        """Issue + deploy a cert per node, upload one CRL revoking all their serials
        under nodeToNode. nodes_to_revoke=[] revokes nothing (for expiry-only tests).
        Returns the list of serials."""
        serials = []
        for node in nodes_to_revoke:
            self.trust_ca_on_cluster(self.ca_cert, server=node)
            serials.append(self.deploy_node_cert(node))
        next_update = self._next_update(next_update_seconds)
        self._n2n_crl_number += 1
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=serials, crl_number=self._n2n_crl_number,
            next_update=next_update)
        status, content, _ = self.rest.upload_crl_file(self.n2n_crl_filename, crl_pem)
        if not status:
            raise RuntimeError("n2n CRL upload failed: {0}".format(content))
        self._track_uploaded_file(self.n2n_crl_filename)
        self._log("n2n CRL uploaded (crl_number={0}), revoked node(s)={1} (serials={2})".format(
            self._n2n_crl_number, [n.ip for n in nodes_to_revoke], serials))
        return serials

    def unrevoke_n2n(self):
        """Upload a newer nodeToNode CRL revoking nothing, and wait for it to load."""
        self._n2n_crl_number += 1
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[], crl_number=self._n2n_crl_number)
        status, content, _ = self.rest.upload_crl_file(self.n2n_crl_filename, crl_pem)
        if not status:
            raise RuntimeError("n2n un-revoke CRL upload failed: {0}".format(content))
        self._track_uploaded_file(self.n2n_crl_filename)
        self._log("n2n CRL re-uploaded with no revoked serials (crl_number={0})".format(self._n2n_crl_number))
        self.wait_for_crl_poll_interval(self.n2n_crl_filename)

    def set_nodetonode_crl_mode(self, mode):
        status, content, _ = self.rest.post_crl_settings({"policyPerScope": {"nodeToNode": mode}})
        if not status:
            raise RuntimeError("post_crl_settings (nodeToNode) failed for mode {0}: {1}".format(mode, content))
        self._log("nodeToNode CRL mode -> {0}".format(mode))

    # ---- clientAuth CRL ----

    def generate_client_certs(self, labels=(("a", "test-client-a"), ("b", "test-client-b"))):
        """Returns {label: {cert, key, serial, cn, cert_path, key_path}}."""
        clients = {}
        for label, cn in labels:
            cert, key, serial = self.crl_utils.generate_leaf_cert(self.ca_cert, self.ca_key, cn=cn)
            clients[label] = {
                "cert": cert, "key": key, "serial": serial, "cn": cn,
                "cert_path": self._write_temp_pem(self.crl_utils.cert_to_pem(cert)),
                "key_path": self._write_temp_pem(self.crl_utils.key_to_pem(key)),
            }
        self._log("Client certs generated: {0}".format(
            {label: c["serial"] for label, c in clients.items()}))
        return clients

    def setup_clientauth_crl(self, revoke_label="a", client_cert_auth_state="hybrid",
                              role="admin", next_update_seconds=None, mode="Require"):
        """Enable clientCertAuth, create RBAC users, revoke `revoke_label`, upload CRL,
        and set clientAuth policy to `mode`. Returns (clients, ca_path). To revoke a
        second client later, use revoke_and_apply_clientauth() instead of calling
        this again."""
        clients = self.generate_client_certs()
        status, content = self.rest.client_cert_auth(
            state=client_cert_auth_state,
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}],
        )
        if not status:
            raise RuntimeError("client_cert_auth failed: {0}".format(content))
        for client in clients.values():
            self.create_rbac_test_user(client["cn"], role)
        next_update = self._next_update(next_update_seconds)
        self._clientauth_crl_number += 1
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[clients[revoke_label]["serial"]],
            crl_number=self._clientauth_crl_number, next_update=next_update)
        status, content, _ = self.rest.upload_crl_file(self.crl_filename, crl_pem)
        if not status:
            raise RuntimeError("CRL upload failed: {0}".format(content))
        self._track_uploaded_file(self.crl_filename)
        status, content, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": mode}})
        if not status:
            raise RuntimeError("post_crl_settings (clientAuth) failed for mode {0}: {1}".format(mode, content))
        ca_path = self._write_temp_pem(self.crl_utils.cert_to_pem(self.ca_cert))
        self._log("clientAuth CRL set up, revoked client={0}, mode={1}".format(revoke_label, mode))
        return clients, ca_path

    def setup_clientauth_crl_multi(self, specs, client_cert_auth_state="hybrid"):
        """Like setup_clientauth_crl, but each client gets its own CN/role (specs:
        list of (label, cn, role)). Revokes nobody -- call revoke_and_apply_clientauth()
        for whichever client needs revoking."""
        clients = self.generate_client_certs([(label, cn) for label, cn, _role in specs])
        status, content = self.rest.client_cert_auth(
            state=client_cert_auth_state,
            prefixes=[{"path": "subject.cn", "prefix": "", "delimiter": ""}],
        )
        if not status:
            raise RuntimeError("client_cert_auth failed: {0}".format(content))
        for label, cn, role in specs:
            self.create_rbac_test_user(cn, role)
            self._log("RBAC user created: label={0} cn={1} role={2}".format(label, cn, role))
        ca_path = self._write_temp_pem(self.crl_utils.cert_to_pem(self.ca_cert))
        return clients, ca_path

    def revoke_and_apply_clientauth(self, serials, mode):
        """Revoke `serials` under clientAuth and set the policy mode -- for revoking
        a second client mid-run. serials: int or list. crl_number is tracked
        internally and always higher than the last one uploaded."""
        if isinstance(serials, int):
            serials = [serials]
        self._clientauth_crl_number += 1
        crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, revoked_serials=serials,
                                           crl_number=self._clientauth_crl_number)
        status, content, _ = self.rest.upload_crl_file(self.crl_filename, crl_pem)
        if not status:
            raise RuntimeError("CRL upload failed: {0}".format(content))
        self._track_uploaded_file(self.crl_filename)
        status, content, _ = self.rest.post_crl_settings({"policyPerScope": {"clientAuth": mode}})
        if not status:
            raise RuntimeError("post_crl_settings failed for mode {0}: {1}".format(mode, content))
        self._log("Revoked serial(s) {0} under clientAuth (crl_number={1}), mode -> {2}".format(
            serials, self._clientauth_crl_number, mode))
        self.wait_for_crl_poll_interval(self.crl_filename)

    # ---- Cert-based REST calls (mTLS) ----

    def function_scope_query_string(self, function_scope):
        if not function_scope:
            return ""
        return "?bucket={0}&scope={1}".format(function_scope["bucket"], function_scope["scope"])

    def call_eventing_https_endpoint(self, eventing_node, cert_path, key_path, ca_path,
                                      method, path, json_body=None, eventing_ssl_port=None):
        """Make one HTTPS call to the Eventing node, presenting a client cert (plain
        `requests`, since RestConnection can't). Returns a Response on success; raises
        SSLError/ConnectionError on a TLS-layer rejection."""
        port = eventing_ssl_port or self.eventing_ssl_port
        url = "https://{0}:{1}{2}".format(eventing_node.ip, port, path)
        return requests.request(
            method, url, cert=(cert_path, key_path), verify=ca_path,
            json=json_body, timeout=30,
        )

    def create_function_via_cert(self, eventing_node, cert_path, key_path, ca_path,
                                  name, body, function_scope=None, eventing_ssl_port=None):
        return self.call_eventing_https_endpoint(
            eventing_node, cert_path, key_path, ca_path, "POST",
            "/api/v1/functions/{0}{1}".format(name, self.function_scope_query_string(function_scope)),
            json_body=body, eventing_ssl_port=eventing_ssl_port)

    def set_settings_via_cert(self, eventing_node, cert_path, key_path, ca_path, name,
                               deployment_status, processing_status, function_scope=None,
                               eventing_ssl_port=None):
        body = {"deployment_status": deployment_status, "processing_status": processing_status}
        return self.call_eventing_https_endpoint(
            eventing_node, cert_path, key_path, ca_path, "POST",
            "/api/v1/functions/{0}/settings{1}".format(name, self.function_scope_query_string(function_scope)),
            json_body=body, eventing_ssl_port=eventing_ssl_port)

    def assert_accepted(self, fn, *args, **kwargs):
        """Assert fn(*args, **kwargs) is accepted -- raise if it's rejected instead."""
        try:
            resp = fn(*args, **kwargs)
            if resp.status_code == 401:
                raise RuntimeError("Expected acceptance but got rejected via HTTP 401: {0}".format(resp.text))
            return resp
        except requests.exceptions.SSLError as e:
            raise RuntimeError("Expected acceptance but got a TLS-layer rejection: {0}".format(e))
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError("Expected acceptance but got a connection error, not a TLS rejection: {0}".format(e))

    def assert_rejected(self, fn, *args, **kwargs):
        """Assert fn(*args, **kwargs) is rejected -- raise if it succeeds instead."""
        try:
            resp = fn(*args, **kwargs)
            if resp.status_code == 401:
                self._log("Correctly rejected via HTTP 401 (fallback for a TLS alert): {0}".format(resp.text))
                return
            raise RuntimeError("Expected a TLS-layer rejection but got a response: {0} {1}".format(
                resp.status_code, resp.text))
        except requests.exceptions.SSLError as e:
            self._log("Correctly rejected at the TLS layer: {0}".format(e))
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError("Expected a TLS-layer rejection but got a connection error instead: {0}".format(e))

    def probe_eventing_ssl(self, eventing_node, cert_path, key_path, ca_path, eventing_ssl_port=None,
                           connect_retries=12, connect_retry_sleep=5):
        """mTLS probe of Eventing's HTTPS port: True = accepted, False = rejected (TLS error or 401).
        Connection refused is retried (listener restarts after a cert reload), then raised."""
        port = eventing_ssl_port or self.eventing_ssl_port
        for attempt in range(1, connect_retries + 1):
            try:
                resp = self.crl_utils.perform_mtls_handshake(
                    eventing_node.ip, port, cert_path, key_path, ca_path, path="/api/v1/functions")
                if resp.status_code == 401:
                    self._log("mTLS probe against {0}:{1} -> REJECTED via HTTP 401 (fallback for a TLS alert): {2}".format(
                        eventing_node.ip, port, resp.text))
                    return False
                self._log("mTLS probe against {0}:{1} -> ACCEPTED".format(eventing_node.ip, port))
                return True
            except requests.exceptions.SSLError as e:
                # SSLError subclasses ConnectionError -- must be caught first
                self._log("mTLS probe against {0}:{1} -> REJECTED: {2}".format(eventing_node.ip, port, e))
                return False
            except requests.exceptions.ConnectionError as e:
                if attempt == connect_retries:
                    raise RuntimeError(
                        "mTLS probe against {0}:{1} failed with a connection error, not a TLS rejection "
                        "(after {2} attempts): {3}".format(eventing_node.ip, port, connect_retries, e))
                self._log("mTLS probe against {0}:{1}: port not accepting connections yet "
                          "(attempt {2}/{3}), retrying in {4}s: {5}".format(
                              eventing_node.ip, port, attempt, connect_retries, connect_retry_sleep, e))
                time.sleep(connect_retry_sleep)

    # ---- Eventing-side helpers (take the calling EventingBaseTest as `test`) ----

    def wait_for_handler_state_with_retry(self, test, name, status, retries=5, retry_sleep=10):
        """test.wait_for_handler_state(), retried on transient errors (e.g. INTERNAL_SERVER_ERROR)."""
        last_exc = None
        for attempt in range(retries):
            try:
                test.wait_for_handler_state(name, status)
                return
            except Exception as e:
                last_exc = e
                self._log("wait_for_handler_state({0}, {1}) failed (attempt {2}/{3}): {4}".format(
                    name, status, attempt + 1, retries, e))
                time.sleep(retry_sleep)
        raise last_exc

    def restart_eventing_producer_and_wait(self, test, appname, tolerate_failure=False, respawn_wait=120):
        """Kill eventing-producer on all eventing nodes and wait for `appname` to be deployed.
        tolerate_failure=True logs instead of raising."""
        for eventing_node in test.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True):
            self._log("Killing eventing-producer on {0} to force reconnection".format(eventing_node.ip))
            test.kill_producer(eventing_node)
        test.sleep(respawn_wait, "Waiting for eventing-producer to respawn")
        try:
            self.wait_for_handler_state_with_retry(test, appname, "deployed")
        except Exception as e:
            if not tolerate_failure:
                raise
            self._log("{0} did not reach 'deployed' after eventing-producer restart "
                      "(to be checked if this is expected behaviour): {1}".format(appname, e))

    def get_collection_doc_count(self, test, namespace):
        bucket, scope, collection = namespace.split(".")
        return test.stat.get_collection_item_count_cumulative(bucket, scope, collection, test.get_kv_nodes())

    def wait_for_collection_count_stable(self, test, namespace, interval=15, max_checks=8):
        """Poll `namespace`'s item count until it stops changing; returns the count."""
        previous = self.get_collection_doc_count(test, namespace)
        for _ in range(max_checks):
            test.sleep(interval, "Waiting for {0} count to stabilize (currently {1})".format(namespace, previous))
            current = self.get_collection_doc_count(test, namespace)
            if current == previous:
                return current
            previous = current
        self._log("{0} count did not stabilize within {1}s, using last value {2}".format(
            namespace, interval * max_checks, previous))
        return previous

    # ---- Misc ----

    def wait_for_crl_poll_interval(self, filename=None, wait_seconds=6):
        """Sleep out the CRL poll interval, then confirm `filename` was fetched/loaded
        on every node (raises otherwise). Returns just that file's per-node status --
        the endpoint otherwise dumps every crlFile, including the unrelated ootb.crl."""
        filename = filename or self.n2n_crl_filename
        time.sleep(wait_seconds)
        status, content, _ = self.rest.get_diagnostics_status()
        if not status:
            raise RuntimeError("get_diagnostics_status call failed: {0}".format(content))
        per_node = json.loads(content)
        relevant = {}
        for node, node_status in per_node.items():
            for crl_file in node_status.get("crlFiles", []):
                if crl_file.get("filename") == filename:
                    relevant[node] = crl_file
                    break
        missing = sorted(set(per_node) - set(relevant))
        if missing:
            raise RuntimeError("diagnostics/status for {0} was not fetched on node(s): {1}".format(
                filename, missing))
        self._log("diagnostics/status for {0} after CRL change: {1}".format(filename, relevant))
        return relevant

    def create_rbac_test_user(self, username, role, password="Couchbase@1234"):
        user = [{'id': username, 'password': password, 'name': 'Some Name'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': username, 'name': 'Some Name', 'roles': role}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')
        self._rbac_users.append(username)
        return username, password

    def _next_update(self, next_update_seconds):
        if next_update_seconds is None:
            return None
        return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=next_update_seconds)

    def _write_temp_pem(self, pem_bytes):
        fd, path = tempfile.mkstemp(suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(pem_bytes)
        self._temp_files.append(path)
        return path

    def _track_uploaded_file(self, filename):
        if filename not in self._created_files:
            self._created_files.append(filename)

    def cleanup(self):
        """Call from tearDown: removes CRL files, policy, clientCertAuth, RBAC users, temp
        files, n2n encryption, and resets touched nodes' certs."""
        for filename in self._created_files:
            try:
                self.rest.delete_crl_file(filename)
            except Exception as exc:
                self._log("CRL file cleanup error for {0}: {1}".format(filename, exc))
        self._created_files = []
        try:
            self.rest.post_crl_settings({"policyPerScope": {"clientAuth": "Disabled", "nodeToNode": "Disabled"}})
        except Exception as exc:
            self._log("CRL settings reset error: {0}".format(exc))
        try:
            self.rest.client_cert_auth(state="disable", prefixes=[])
        except Exception as exc:
            self._log("clientCertAuth disable error: {0}".format(exc))
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self._log("RBAC user cleanup error for {0}: {1}".format(username, exc))
        self._rbac_users = []
        for path in self._temp_files:
            try:
                os.remove(path)
            except OSError as exc:
                self._log("Temp file cleanup error for {0}: {1}".format(path, exc))
        self._temp_files = []
        try:
            self.disable_n2n_encryption()
        finally:
            self.reset_node_certs()

    def reset_node_certs(self):
        """Reset every node this helper touched back to a cluster-generated cert."""
        for server in list(self._cert_touched_nodes.values()):
            self._reset_node_cert(server)
        self._cert_touched_nodes = {}

    def reset_stale_test_certs(self, test_ca_cns):
        """Reset any server still presenting a cert from a previous run's test CA."""
        for server in self.servers:
            issuer_cn = self._presented_cert_issuer_cn(server)
            if issuer_cn in test_ca_cns:
                self._log("Stale test cert on {0} (issuer CN={1}) -- resetting before the test".format(
                    server.ip, issuer_cn))
                self._reset_node_cert(server)

    def _presented_cert_issuer_cn(self, server, port=18091):
        """Issuer CN of the cert `server` presents on `port`, or None (works on spare nodes)."""
        try:
            pem = ssl.get_server_certificate((server.ip, port), timeout=10)
            cert = x509.load_pem_x509_certificate(pem.encode())
            attrs = cert.issuer.get_attributes_for_oid(NameOID.COMMON_NAME)
            return attrs[0].value if attrs else None
        except Exception as exc:
            self._log("Could not read presented cert on {0}:{1}: {2}".format(server.ip, port, exc))
            return None

    def _reset_node_cert(self, server):
        """Remove the test cert files from `server`'s inbox and regenerate its cert."""
        try:
            inbox = "{0}{1}".format(x509main(host=server).install_path, x509main.CHAINFILEPATH)
            shell = RemoteMachineShellConnection(server)
            try:
                shell.execute_command("rm -f {0}/chain.pem {0}/pkey.key {0}/CA/crl_test_ca.pem".format(inbox))
            finally:
                shell.disconnect()
        except Exception as exc:
            self._log("Inbox cleanup error on {0}: {1}".format(server.ip, exc))
        try:
            status, content = RestConnection(server).refresh_certificate()
            if status:
                self._log("Node cert regenerated on {0}".format(server.ip))
            else:
                self._log("regenerateCertificate refused on {0}: {1}".format(server.ip, content))
        except Exception as exc:
            self._log("regenerateCertificate failed on {0}: {1}".format(server.ip, exc))
