"""Smoke test: FTS keeps working with the internal identity password check on.

This shows only that turning the setting on breaks nothing in an ordinary
index-and-query run. It cannot catch an IPC regression in cbft's internal
traffic, because none of that traffic depends on the setting:
  - cbft -> ns_server: plain HTTP on 127.0.0.1:8091, no certificate.
  - DCP from KV: cert + SASL, and memcached ignores the setting.
  - Scatter-gather (and Query's SEARCH() via n1fty): gRPC, Basic creds only.
  - Partition moves on rebalance: cfg/metakv over cbauth revrpc (Basic).
FTS's real exposure is inbound on 18094, which fts_ipc.py covers.

`IPCEnforcementMixin` turns the setting on after the inherited setUp and puts
it back in tearDown. The line uses `multiple_ca=True,use_client_certs=True`
and `client_cert_state=hybrid`, the configuration in which cbft does present
its internal certificate (on KV and gRPC), and setUp fails fast otherwise.
mandatory is refused because the framework's httplib2 REST calls present no
client certificate, so mandatory would lock the run out of the cluster.
"""

import json

from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from pytests.security.ipc_base import IPCMixin
from security.ntonencryptionBase import ntonencryptionBase

from .stable_topology_fts import StableTopFTS


class IPCEnforcementMixin(IPCMixin):
    """Enables the IPC setting around an inherited FTS suite.

    Uses only IPCMixin's setting accessors — no CA, certificates or RBAC users
    are created, since the inherited suite brings its own.
    """

    def setUp(self):
        # A previous line that died in mandatory mode would otherwise fail
        # this one's setUp on the TLS handshake.
        self._self_heal_stuck_client_cert_auth()
        super(IPCEnforcementMixin, self).setUp()
        self.rest = RestConnection(self.master)
        self._original_client_cert_auth = None
        self._original_ipc_state = self.get_ipc_setting()
        try:
            self._relax_encryption_for_sdk_paths()
            self._apply_client_cert_state()
            self._assert_internal_certs_in_play()
            self.set_ipc_setting(True)
            self.log.info("{0}={1} for the inherited suite (was {2})".format(
                self.IPC_SETTING, self.get_ipc_setting(),
                self._original_ipc_state))
        except Exception:
            # unittest skips tearDown when setUp raises, and super().setUp()
            # left the cluster at strict — undo it here or every later test in
            # the job dies on plaintext REST.
            self._ipc_reuse_cleanup()
            try:
                ntonencryptionBase().disable_nton_cluster(self._input.servers)
                CbServer.use_https = False
            except Exception as cleanup_error:
                self.log.warning("could not drop node encryption after a "
                                 "failed setUp: {0}".format(cleanup_error))
            raise

    def tearDown(self):
        try:
            self._ipc_reuse_cleanup()
        finally:
            super(IPCEnforcementMixin, self).tearDown()

    def _ipc_reuse_cleanup(self):
        # clientCertAuth first: while it is stuck the setting cannot be reached.
        if self._original_client_cert_auth is not None:
            try:
                self.rest.client_cert_auth(
                    state=self._original_client_cert_auth["state"],
                    prefixes=self._original_client_cert_auth["prefixes"])
            except Exception as exc:
                self.log.warning("clientCertAuth restore error: {0}; falling "
                                 "back to plain HTTP".format(exc))
                self._self_heal_stuck_client_cert_auth()
        try:
            self._restore_ipc_setting()
        except Exception as exc:
            self.log.warning("{0} restore error: {1}".format(
                self.IPC_SETTING, exc))

    def _relax_encryption_for_sdk_paths(self):
        """Drop clusterEncryptionLevel from strict to all, as
        CRLEnforcementMixin does and for the same reason: the inherited suites
        load data over plaintext couchbase://, which strict closes. `all`
        still encrypts node-to-node traffic, which is where internal client
        certificates are presented. `ipc_keep_strict=True` skips this.
        """
        if self._input.param("ipc_keep_strict", False):
            return
        status, content = self.rest.set_node_encryption_level("all")
        if not status:
            self.log.warning("could not lower clusterEncryptionLevel to "
                             "'all': {0}".format(content))

    def _apply_client_cert_state(self):
        state = self._input.param("client_cert_state", None)
        if not state:
            return
        if state == "mandatory" and CbServer.use_https:
            # RestConnection._http_request (httplib2) presents no client
            # certificate, so mandatory locks the framework out of 18091 —
            # including the calls that would restore clientCertAuth.
            self.fail("client_cert_state=mandatory is not usable in the reuse "
                      "suites: the framework's REST client sends no client "
                      "certificate over HTTPS. Use hybrid.")
        status, content, _ = self.rest.get_client_cert_auth()
        if not status:
            self.fail("GET /settings/clientCertAuth failed: {0}".format(
                content))
        current = json.loads(content)
        self._original_client_cert_auth = {
            "state": current.get("state"),
            "prefixes": current.get("prefixes", [])}
        status, content = self.rest.client_cert_auth(
            state=state, prefixes=current.get("prefixes", []))
        if not status:
            self.fail("Failed to set clientCertAuth={0}: {1}".format(
                state, content))
        self.log.info("clientCertAuth state {0} -> {1}".format(
            current.get("state"), state))

    def _assert_internal_certs_in_play(self):
        """Fail unless the run is in the configuration where cbft presents
        its internal client certificate.

        cbauth sets ShouldClientsUseClientCert only for hybrid and mandatory,
        and the certificate travels only on node-to-node TLS. Below either the
        smoke test would not even run cbft the way a customer with the
        setting on does.
        """
        status, content, _ = self.rest.get_client_cert_auth()
        state = json.loads(content).get("state") if status else None
        if state not in ("hybrid", "mandatory"):
            self.fail("clientCertAuth is {0!r}; reuse lines need "
                      "client_cert_state=hybrid, or no internal client "
                      "presents a certificate".format(state))
        status, content, _ = self.rest._http_request(
            self.rest.baseUrl + "pools/default")
        nodes = json.loads(content).get("nodes", []) if status else []
        unencrypted = [node.get("hostname") for node in nodes
                       if not node.get("nodeEncryption")]
        if not nodes or unencrypted:
            self.fail("node-to-node encryption is off on {0}; reuse lines "
                      "need multiple_ca=True,use_client_certs=True".format(
                          unencrypted or "the cluster"))


class IPCStableTopFTS(IPCEnforcementMixin, StableTopFTS):
    """StableTopFTS with the IPC setting on — indexing and querying."""

