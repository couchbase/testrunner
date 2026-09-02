"""XDCR CRL suite — MB-72047 P0.

`XDCRCRLBase` is the shared fixture (PKI generation, the clusterEncryptionLevel/
clientCertAuth baseline, CRL upload/policy/reload helpers, and teardown). This
file defines two test classes on top of it. XDCRCRLLocalTests covers the
intra-cluster surfaces (the CRL lives on the source cluster, under the
nodeToNode scope). XDCRCRLRemoteTests, below it, covers the remote-cluster
reference (CRL on the TARGET cluster, under the clientAuth scope) -- kept as a
*separate* class so a failed local teardown cannot poison the remote cases.

XDCRCRLRemoteTests is written against docs/xdcr-crl/PHASE0-8.5-FINDINGS.md,
which measured DEF-1 and DEF-3 as NOT fixed and DEF-2 as fixed only on the
pipeline-restart path on 8.5.0-1077. Four of its tests are written to
SPECIFIED behaviour and are expected to fail until DEF-1's hint-text fix
lands (GROUP=P2: F11 validate, F12 create, F13 edit -- see below).
test_running_replication_remote_revocation (F10) and
test_mid_replication_error_propagation (F15) exercise DEF-2's
already-working restart path and pass (GROUP=P0).
test_hot_reload_remote_ref_cert (F14) was ORIGINALLY predicted P2 by
extrapolating DEF-2's restart-only finding onto rotation as well as
revocation -- a live run corrected that: F14 passes (GROUP=P0). See "F14 was
mis-predicted" below, and that file (not this docstring) for the underlying
evidence throughout.

See docs/superpowers/specs/2026-09-01-xdcr-crl-automation-design.md.
"""
import datetime
import time
import urllib.parse

import logger
from couchbase_helper.documentgenerator import BlobGenerator
from cryptography import x509
from membase.api.rest_client import RestConnection
from security.crl_utils import CRLUtils
from security.rbac_base import RbacBase
from TestInput import TestInputSingleton
from xdcr.crl_xdcr_utils import (delete_inbox_contents, goxdcr_log_count,
                                 goxdcr_pid, install_ca_cert,
                                 install_internal_client_cert,
                                 wait_for_goxdcr_phrase)
from xdcr.xdcrnewbasetests import OPS, XDCRNewBaseTest

REVOCATION_PHRASE = "possible certificate revocation"
INTERNAL_SAN_DOMAIN = "internal.couchbase.com"

# CN of the remote reference's own client cert (XDCRCRLRemoteTests). Fixed,
# not derived, because a Couchbase user of the SAME name must exist on the
# TARGET cluster for clientCertAuth's subject.cn mapping to resolve it to
# anyone at all -- see XDCRCRLRemoteTests._provision_xdcr_client_user.
XDCR_CLIENT_CN = "xdcrclient"

# The stable body of DEF-1's hint (metadata_svc/remote_cluster_service.go:3144
# -3150, PHASE0-8.5-FINDINGS.md DEF-1), deliberately missing its leading
# word: the shipped string is prefixed with the wrapped err.Error() text, and
# the sentence itself starts "The client certificate..." where the plan's own
# prose renders it lower-case ("the client certificate...") -- matching only
# the case-stable remainder sidesteps that without weakening the assertion.
DEF1_HINT = ("client certificate supplied for this remote cluster may have "
            "been revoked by the target cluster's certificate revocation "
            "(CRL) policy")

# The stable body of the REMOTE form of D6's replication-error message
# (pipeline_manager.go:584's getCertRevocationErrMsg, isLocal=False).
# Deliberately excludes "The client certificate for remote cluster <name>"
# (the reference name varies) and "for pipeline <replId>" (varies too) --
# and deliberately does NOT overlap with the LOCAL form, which says
# "revoked under the local cluster's...", not "revoked by the target
# cluster's...", so this substring can never accidentally match a local
# revocation entry.
REMOTE_REVOCATION_MESSAGE = ("may have been revoked by the target "
                             "cluster's certificate revocation (CRL) policy")

# openssl's own default crlnumber from a fresh CA db is 0x1000 == 4096, and two
# CRLs sharing a number collide silently — the second upload has no effect and
# the revocation simply does not take. Starting above it and incrementing per
# CA makes that class of bug unreachable rather than merely documented.
CRL_NUMBER_BASE = 5000

# ns_server's node/controller/{enableExternalListener,setupNetConfig} require
# afamily/afamilyOnly even when the family itself isn't changing. This fleet
# is IPv4-only.
NET_CONFIG_AFAMILY = {"afamily": "ipv4", "afamilyOnly": "false"}

# subject.cn with no prefix/delimiter -- the whole CN maps to the username.
# This is the §3.3 baseline: clientCertAuth=hybrid at this prefix.
CLIENT_CERT_PREFIXES = [{"path": "subject.cn", "prefix": "", "delimiter": ""}]


class XDCRCRLBase(XDCRNewBaseTest):

    def setUp(self):
        # super().setUp() is what normally sets self.log -- but it also does
        # the cluster (re)join this method runs ahead of, so get a logger of
        # our own first. logger.Logger.get_logger() is the same call
        # XDCRNewBaseTest.setUp() makes; super().setUp() below reassigns the
        # same value momentarily afterward, harmlessly.
        self.log = logger.Logger.get_logger()
        # Runs before super().setUp() builds/rejoins the clusters: a node left
        # at clusterEncryptionLevel=all by a prior run trusts only that run's
        # CA, and rejects a plain addNode's TLS-authenticated internal traffic
        # from a fresh peer with "certificate issued by unknown CA" -- the
        # join dies before any of our own code runs. See task-6-report.md.
        self._restore_joinable_state()
        super(XDCRCRLBase, self).setUp()
        self.crl = CRLUtils(log=self.log)
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        # Per node, not one read reused everywhere (I7): this fleet has no
        # NTP and nodes drift from EACH OTHER, not just from the host, so a
        # peer whose clock trails the master's would have every one of its
        # OWN in-test log lines sort lexicographically below a `since` taken
        # from the master -- making a per-node "nothing logged" assertion
        # (F1) vacuously true on an empty window rather than a real absence.
        self._since_by_node = {}
        for cluster in (self.src_cluster, self.dest_cluster):
            for node in cluster.get_nodes():
                self._since_by_node[node.ip] = self._read_node_started_utc(
                    RestConnection(node))
        self._test_started_utc = self._since_by_node.get(self.src_master.ip)

        self._crl_numbers = {}
        self._uploaded_crls = []      # (rest, filename)
        self.internal_certs = {}      # node ip -> (cert, key, serial)
        self.cas = {}                 # ca name -> (cert, key)

        self._require_enterprise()
        self._build_pki()
        try:
            self._establish_crl_baseline()
        except Exception:
            # unittest never calls tearDown when setUp raises, so without
            # this a failed baseline leaves the fleet encrypted and every
            # subsequent test in the suite dies at cluster join instead of
            # just this one -- re-raise so THIS test still fails, but leave
            # the fleet joinable first so the failure stays isolated.
            self._restore_joinable_state()
            raise

    def tearDown(self):
        try:
            self._reset_crl_state()
        finally:
            try:
                # Undo the baseline this suite forced on, so the NEXT test's
                # cluster (re)formation doesn't hit the same TLS join failure
                # this fixes in setUp. Nested try/finally: each step, and
                # then the base teardown, must run regardless of the others.
                self._restore_joinable_state()
            finally:
                super(XDCRCRLBase, self).tearDown()

    def _read_node_started_utc(self, rest):
        """ISO-8601 UTC timestamp marking the start of THIS test, AS SEEN
        BY `rest`'s own node, fixed-width and millisecond-precision to
        match goxdcr's own log format exactly (`2026-09-01T21:27:00.727Z`,
        verified by hand against a live node's goxdcr.log). That match
        matters, not just cosmetically: a coarser format breaks the
        lexicographic '>=' comparison
        `count_new_goxdcr_lines`/`wait_for_new_goxdcr_phrase` rely on --
        e.g. '...:00Z' sorts AFTER '...:00.727Z' ('.' < 'Z'), which would
        make a same-second log line look like it came before `since` when it
        did not.

        Read from `rest`'s OWN node clock via diag_eval
        (`erlang:system_time(millisecond)`, an integer epoch -- easier and
        more precise to convert than parsing `calendar:universal_time()`'s
        tuple output back out of Erlang term syntax), NOT the Mac's and NOT
        reused across nodes (I7): this fleet has no NTP and its nodes drift
        from the host AND FROM EACH OTHER. A `since` read once from the
        source master and reused for a peer whose clock trails it would put
        every one of THAT peer's own in-test log lines lexicographically
        below `since`, making a per-node "nothing logged" assertion (F1)
        vacuously true on an empty window rather than a real absence.

        Falls back to the HOST's own UTC clock, loudly, if the node read
        fails for any reason -- a wrong `since` silently changes what every
        goxdcr-log assertion against this node sees, so a fallback must
        never be silent.
        """
        try:
            status, content = rest.diag_eval(
                "erlang:system_time(millisecond).", print_log=False)
            if not status:
                raise ValueError(
                    "diag_eval was refused: {0}".format(content))
            epoch_ms = int(str(content).strip())
            dt = datetime.datetime.utcfromtimestamp(epoch_ms / 1000.0)
            return "{0}.{1:03d}Z".format(
                dt.strftime("%Y-%m-%dT%H:%M:%S"), epoch_ms % 1000)
        except Exception as error:
            dt = datetime.datetime.utcnow()
            since = "{0}.{1:03d}Z".format(
                dt.strftime("%Y-%m-%dT%H:%M:%S"), dt.microsecond // 1000)
            self._log_warning(
                "could not read {0}'s own clock to timestamp this test's "
                "start ({1}); falling back to the HOST's own UTC clock "
                "({2}) instead -- this fleet's nodes are not NTP-synced "
                "against the host OR EACH OTHER, so goxdcr-log 'since' "
                "assertions against {0} in this test may be off by clock "
                "skew".format(rest.ip, error, since))
            return since

    def _require_enterprise(self):
        """CRL is EE-only; on CE the whole suite is meaningless, not failing."""
        pools = self.src_rest.get_pools_info()
        if not pools.get("isEnterprise", True):
            self.skipTest("CRL is an Enterprise-only feature")

    # ---- prerequisite baseline (clusterEncryptionLevel / clientCertAuth) --

    def _log_warning(self, message):
        """Route a restore/verification warning through both the logger and
        stdout. This class runs code ahead of super().setUp(), before
        per-test log routing is necessarily wired up -- a real run came back
        with an empty 0-byte test.log and every self.log.warning() call from
        this path was invisible as a result. print() lands regardless of
        logging configuration, so use both rather than debug this twice.
        """
        self.log.warning(message)
        print("[crlXDCR] WARNING: {0}".format(message), flush=True)

    def _log_info(self, message):
        """Same visibility contract as `_log_warning`, for a message that is
        routine/expected rather than a warning (e.g. the tolerated 400 in
        `_reset_node_pki`). `self.log.info()` alone is invisible under the
        exact empty-`test.log` condition `_log_warning` documents -- print()
        lands regardless of logging configuration.
        """
        self.log.info(message)
        print("[crlXDCR] INFO: {0}".format(message), flush=True)

    def _log_warning_unless_bug(self, message, error):
        """Log `error` via `_log_warning` UNLESS it is one of the types
        that, in this codebase, can only mean a bug in OUR OWN call --
        in which case re-raise it instead of swallowing it.

        Every `except Exception` in this class's best-effort restore/
        verification path must route through here rather than logging
        unconditionally. A prior revision's
        `status, content, _ = rest.set_node_encryption_level("control")`
        unpacked 3 values from a method that returns 2
        (`on_prem_rest_client.py`'s `set_node_encryption_level` ends
        `return status, content`), so it raised `ValueError` on EVERY
        single run and was swallowed silently right here: the level was
        never actually reset by that call, and the bug went undetected
        for several rounds because the three-endpoint sequence happened to
        do the real work anyway. A best-effort cleanup path must tolerate
        a hostile *environment*, never a bug in itself.

        TypeError/ValueError/AttributeError are the ones that, in this
        codebase specifically, can only originate from a call WE made
        incorrectly: `RestConnection._http_request` already retries its
        own internal socket/AttributeError/httplib2 failures and converts
        them to `ServerUnavailableException` once retries are exhausted
        (see its source), so none of those three ever legitimately reaches
        here FROM a REST call gone wrong -- if one does, it is our own
        wrong arity, wrong argument type, or a typo'd attribute, not the
        fleet being uncooperative.

        One known, accepted imprecision: `json.JSONDecodeError` is a
        `ValueError` subclass, so a genuinely malformed (not merely
        absent/erroring) JSON response would also propagate here rather
        than being tolerated. That trade favors surfacing a real anomaly
        loudly over a catch broad enough to hide the next version of this
        exact bug.
        """
        if isinstance(error, (TypeError, ValueError, AttributeError)):
            raise error
        self._log_warning(message)

    def _restore_joinable_state(self):
        """Undo whatever a previous run left behind, so cluster (re)formation
        can proceed. Runs directly against every node named in the ini --
        called before super().setUp() (self.src_cluster/self.dest_cluster
        don't exist yet), from setUp's own exception handler (they may still
        be joined), and again from tearDown (they may already be gone after
        a failed rebuild). Best-effort per node and per call: a node that is
        unreachable, already stock, or only half-provisioned must never
        abort the rest -- this is cleanup, not the test.

        Runs in PHASES across the whole fleet, not node-by-node: any two
        nodes here may currently be clustered together (this runs both
        pre-join, when none are, and post-join from tearDown/setUp's
        exception path), so a step with a live cross-node dependency --
        node encryption -- must finish on every node before the next phase
        starts on any of them. See `_set_node_encryption` for what happens
        when that ordering is violated.

        Also resets CRL policy/files (`_reset_crl_on_node`) -- the settings
        this whole suite exists to manipulate are otherwise reset only by
        `_reset_crl_state` from `tearDown`, which `unittest` skips whenever
        `setUp` raised, leaving a leaked policy for the next run.
        """
        rests = []
        for server in TestInputSingleton.input.servers:
            try:
                rests.append(RestConnection(server))
            except Exception as error:
                self._log_warning_unless_bug(
                    "could not reach {0} to restore joinable state: "
                    "{1}".format(getattr(server, "ip", server), error),
                    error)

        # Network config calls are refused outright while auto-failover is
        # enabled ("Can't change network configuration when auto-failover
        # is enabled."), so this must go first, on every node.
        for rest in rests:
            try:
                if not rest.update_autofailover_settings(False, 120):
                    self._log_warning(
                        "auto-failover disable was refused on {0}".format(
                            rest.ip))
            except Exception as error:
                self._log_warning_unless_bug(
                    "could not disable auto-failover on {0}: {1}".format(
                        rest.ip, error), error)

        for rest in rests:
            try:
                # set_node_encryption_level returns a 2-tuple
                # (status, content) -- NOT 3. Unpacking 3 here previously
                # raised ValueError on every call and was silently
                # swallowed by the except below; see
                # `_log_warning_unless_bug` for the full story.
                status, content = rest.set_node_encryption_level("control")
                if not status:
                    self._log_warning(
                        "clusterEncryptionLevel reset to 'control' was "
                        "refused on {0}: {1}".format(rest.ip, content))
            except Exception as error:
                self._log_warning_unless_bug(
                    "could not reset clusterEncryptionLevel on {0}: "
                    "{1}".format(rest.ip, error), error)

        # clusterEncryptionLevel=control is NOT "encryption off" -- it only
        # lowers what is required to be encrypted. Node-to-node encryption
        # itself stays enabled until told otherwise, so a join still dials
        # the TLS port and still hits "unknown CA" unless this runs too.
        # ns_server requires the level to already be 'control' (immediately
        # above) before this is accepted. Phase-by-phase across the whole
        # `rests` list, not per node -- see `_set_node_encryption`.
        self._disable_node_encryption(rests)

        # add_node hardcodes HTTPS for the join handshake on Enterprise
        # 6.5+ regardless of any encryption setting above, so a join can
        # still fail on certificate trust alone. Reset each node's own
        # identity and drop whatever CAs a previous run left it trusting.
        for rest in rests:
            self._reset_node_pki(rest)

        # Not scoped by _set_node_encryption's cross-node-dependency
        # constraint -- unlike node encryption, CRL policy/files have no
        # live cross-node handshake during this reset, so per-node is fine.
        for rest in rests:
            self._reset_crl_on_node(rest)

        for rest in rests:
            self._disable_client_cert_auth(rest)

        for rest in rests:
            self._verify_node_encryption(rest, expected=False)

    def _reset_node_pki(self, rest):
        """Regenerate `rest`'s own certificate and delete every trusted CA a
        previous run left it with, best-effort.

        `RestConnection.add_node` hardcodes HTTPS for the join handshake on
        Enterprise 6.5+ (`protocol = "https"` whenever the major version is
        >= 6.5 and the edition is enterprise) -- unconditional on any
        encryption setting this suite manages. Toggling
        clusterEncryptionLevel or node encryption alone can therefore never
        fix a join that fails on certificate trust; what actually matters is
        that nodes trust each OTHER's certificates, and a previous run's
        uploaded CA breaks exactly that.

        Order matters, verified by hand against a live node (every call
        200/204 except the one expected 400): `regenerateCertificate` FIRST
        gives the node a fresh self-signed identity, which is what frees the
        OLD CA(s) for deletion -- deleting them first returns HTTP 400 "The
        CA certificate is in use by the following nodes" for whichever CA is
        still backing the node's live identity. That 400, on whichever CA
        the node was using right up until it just regenerated, is EXPECTED
        and tolerated here, not treated as a failure.

        Wipes the node's inbox (`delete_inbox_contents`) BEFORE any of
        that REST work, mirroring x509_multiple_CA_util's own teardown
        ordering -- see that function's docstring for why the file-level
        cleanup has to come first rather than after.
        """
        server = next(
            (s for s in TestInputSingleton.input.servers
             if getattr(s, "ip", None) == rest.ip), None)
        if server is None:
            self._log_warning(
                "could not find a server object for {0} in the ini -- "
                "skipping its inbox wipe before regenerating its "
                "certificate".format(rest.ip))
        else:
            try:
                delete_inbox_contents(server)
            except Exception as error:
                self._log_warning_unless_bug(
                    "could not wipe inbox contents on {0} before "
                    "regenerating its certificate: {1}".format(
                        rest.ip, error), error)

        try:
            status, content = rest.refresh_certificate()
            if not status:
                self._log_warning(
                    "regenerateCertificate was refused on {0}: {1}".format(
                        rest.ip, content))
        except Exception as error:
            self._log_warning_unless_bug(
                "regenerateCertificate failed on {0}: {1}".format(
                    rest.ip, error), error)

        try:
            cas = rest.get_trusted_CAs()
        except Exception as error:
            self._log_warning_unless_bug(
                "could not list trustedCAs on {0}: {1}".format(
                    rest.ip, error), error)
            return
        if not isinstance(cas, list):
            self._log_warning("unexpected trustedCAs shape on {0}: "
                              "{1}".format(rest.ip, cas))
            return

        for ca in cas:
            ca_id = ca.get("id") if isinstance(ca, dict) else None
            if ca_id is None:
                continue
            try:
                status, content, response = rest.delete_trusted_CA(ca_id)
                if not status:
                    # response is the raw httplib2 response dict
                    # (response['status'] is the HTTP status code as a
                    # string) -- only a 400 is the "still in use" case this
                    # is meant to tolerate. A 403/500/other-cause 400 must
                    # not be swallowed the same way: that is exactly the
                    # failure this whole method exists to prevent (a
                    # previous run's uploaded CA still trusted, breaking the
                    # next add_node).
                    code = response.get("status") if isinstance(
                        response, dict) else None
                    if code == "400":
                        self._log_info(
                            "trustedCA {0} not deleted on {1} (likely "
                            "still in use, expected): {2}".format(
                                ca_id, rest.ip, content))
                    else:
                        self._log_warning(
                            "trustedCA {0} not deleted on {1} (status "
                            "{2}): {3}".format(
                                ca_id, rest.ip, code, content))
            except Exception as error:
                self._log_warning_unless_bug(
                    "deleting trustedCA {0} failed on {1}: {2}".format(
                        ca_id, rest.ip, error), error)

        # Post-condition: everything that could be deleted should now be
        # gone, leaving only the node's own freshly generated CA. Anything
        # else still present is a previous run's CA that survived the loop
        # above for a reason other than "still backing the live identity"
        # -- exactly the state that breaks the next add_node.
        try:
            cas_after = rest.get_trusted_CAs()
        except Exception as error:
            self._log_warning_unless_bug(
                "could not re-read trustedCAs on {0} after reset: "
                "{1}".format(rest.ip, error), error)
            return
        if not isinstance(cas_after, list):
            self._log_warning(
                "unexpected trustedCAs shape on {0} after reset: "
                "{1}".format(rest.ip, cas_after))
            return
        leftover = [ca for ca in cas_after
                   if not (isinstance(ca, dict)
                           and ca.get("type") == "generated")]
        if leftover:
            self._log_warning(
                "trustedCAs on {0} still holds non-generated entries "
                "after reset -- a previous run's CA may still be trusted, "
                "which is what this reset exists to prevent: "
                "{1}".format(rest.ip, leftover))

    def _reset_crl_on_node(self, rest):
        """Best-effort CRL cleanup for a single node: disable both policy
        scopes and delete every uploaded CRL file.

        `_reset_crl_state` does the same thing, but only from `tearDown`,
        which `unittest` never runs when `setUp` raises -- so a test that
        dies in `setUp` (e.g. in `_establish_crl_baseline`) leaves
        nodeToNode=Require (or whatever the dying test last set) and its CRL
        files behind. The NEXT run's `_build_pki` mints a fresh CA, so that
        leaked policy then meets an issuer with no CRL uploaded for it at
        all -- not the same failure `_establish_crl_baseline`'s own
        `_restore_joinable_state` call guards against, but just as capable
        of wedging the next run before any of ITS own code runs.

        Uses `CRLUtils.parse_content` directly (the class-level helper,
        not `self.crl`) rather than going through `set_crl_policy`, which
        wraps that same class-level call but ends in an `assertTrue`
        that would break this method's best-effort contract -- the same
        reason `_disable_client_cert_auth` parses its own read-back
        directly instead of asserting on it.
        """
        try:
            status, content, _ = rest.post_crl_settings(
                {"policyPerScope": {"clientAuth": "Disabled",
                                    "nodeToNode": "Disabled"}})
            if not status:
                self._log_warning(
                    "CRL policy reset was refused on {0}: {1}".format(
                        rest.ip, content))
        except Exception as error:
            self._log_warning_unless_bug(
                "could not reset CRL policy on {0}: {1}".format(
                    rest.ip, error), error)

        try:
            status, content, _ = rest.get_crl_files()
        except Exception as error:
            self._log_warning_unless_bug(
                "could not list CRL files on {0}: {1}".format(
                    rest.ip, error), error)
            return
        if not status:
            self._log_warning(
                "listing CRL files was refused on {0}: {1}".format(
                    rest.ip, content))
            return
        files = CRLUtils.parse_content(content)
        if not isinstance(files, list):
            self._log_warning(
                "unexpected CRL file listing shape on {0}: {1}".format(
                    rest.ip, files))
            return
        for entry in files:
            filename = entry.get("filename") if isinstance(entry, dict) else None
            if not filename:
                continue
            try:
                del_status, del_content, _ = rest.delete_crl_file(filename)
                if not del_status:
                    self._log_warning(
                        "could not delete leftover CRL {0} on {1}: "
                        "{2}".format(filename, rest.ip, del_content))
            except Exception as error:
                self._log_warning_unless_bug(
                    "deleting leftover CRL {0} on {1} failed: {2}".format(
                        filename, rest.ip, error), error)

    def _disable_client_cert_auth(self, rest):
        """Disable clientCertAuth on `rest`, best-effort, then read it back.

        A live run hit `ClientCertConfig: wrong type for key:prefixes,
        null` from memcached_config_mgr right after
        `client_cert_auth("disable", [])` -- ns_server accepted the POST
        (200) but then failed to push the resulting config to memcached,
        silently leaving the setting half-applied. The Python encoding
        itself is not the bug: `json.dumps({..., "prefixes": []})` already
        produces a proper JSON `[]`, confirmed by reading
        `RestConnection.client_cert_auth` directly -- the `null` memcached
        complained about is never something this call puts on the wire. The
        likelier mechanism is ns_server's own state=disable handling
        collapsing an empty prefixes list server-side. Passing the same
        non-empty, already-established `CLIENT_CERT_PREFIXES` this suite
        uses for "hybrid" sidesteps that -- prefixes are moot once auth is
        disabled either way -- rather than proving the exact mechanism.
        """
        try:
            status, content = rest.client_cert_auth(
                "disable", CLIENT_CERT_PREFIXES)
            if not status:
                self._log_warning(
                    "clientCertAuth disable was refused on {0}: "
                    "{1}".format(rest.ip, content))
                return
            verify_status, verify_content, _ = rest.get_client_cert_auth()
            verified = CRLUtils.parse_content(verify_content)
            state = verified.get("state") if isinstance(verified, dict) else None
            if not verify_status or state != "disable":
                self._log_warning(
                    "clientCertAuth on {0} reads back as {1!r} after a "
                    "'disable' call that reported success -- ns_server may "
                    "have rejected the memcached config update silently "
                    "(look for 'ClientCertConfig'/memcached_config_mgr in "
                    "ns_server's own log on {0} for the authoritative "
                    "reason)".format(rest.ip, state))
        except Exception as error:
            self._log_warning_unless_bug(
                "could not disable/verify clientCertAuth on {0}: "
                "{1}".format(rest.ip, error), error)

    def _set_node_encryption(self, rests, state, assert_success):
        """Toggle node-to-node encryption across every RestConnection in
        `rests`, phase-by-phase ACROSS NODES: `enableExternalListener` runs
        on every node, then `setupNetConfig` on every node, then
        `disableUnusedExternalListeners` on every node -- never all three
        run to completion on one node before another has even started.

        That per-node-to-completion shape is exactly what a live run's
        AssertionError exposed: running all three calls on node A while
        node B (already clustered with A) had not yet enabled its own TLS
        listener left A TLS-only mid-fleet, so A's own `setupNetConfig`
        call failed on a P2P reconnect:
        `{"errors":{"_":"Reconnect to 'ns_1@<B>' retries exceeded"}}`.
        §3.3's `enableExternalListener -> setupNetConfig ->
        disableUnusedExternalListeners` arrows describe three CLUSTER-WIDE
        phases, not a three-call sequence scoped to one node at a time.

        This exact three-call sequence is also REQUIRED IN BOTH
        DIRECTIONS: an earlier version of the "off" path dropped
        `enableExternalListener` as a supposedly-unneeded optimisation, and
        turning encryption off then failed outright -- `setupNetConfig`
        came back HTTP 400 "Missing TCP-IPv4 listener (needed for external
        communication)", because `disableUnusedExternalListeners` had torn
        the plain listener down when encryption last went ON, leaving
        `setupNetConfig` nothing to switch to. `enableExternalListener` is
        what brings it back before the switch. Both facts measured by hand
        against live nodes; do not re-drop either.

        No wrapped RestConnection method covers this (the only precedent,
        `enable_ip_version`, always passes `nodeEncryption='off'` for IPv6
        toggling, and operates on one node at a time), so this goes through
        `_http_request` directly -- the same way other pytests reach past
        RestConnection's own surface (e.g. security/rbac_base.py).

        Args:
            rests: list of RestConnection, one per node. Every node that
                is (or may be) clustered with another in this list must be
                included in the SAME call, or the cross-node ordering
                guarantee above is lost.
            state: "on" or "off".
            assert_success: True to raise via self.assertTrue on any
                non-2xx response (the enable path, which this suite already
                depends on being healthy); False to log-and-continue (the
                best-effort restore path, which runs against nodes that may
                be unreachable, mid-join, or already stock, and must never
                raise or assert).
        """
        params = urllib.parse.urlencode(
            dict(NET_CONFIG_AFAMILY, nodeEncryption=state))
        for endpoint in ("enableExternalListener", "setupNetConfig",
                         "disableUnusedExternalListeners"):
            for rest in rests:
                api = "{0}node/controller/{1}".format(rest.baseUrl, endpoint)
                try:
                    status, content, _ = rest._http_request(
                        api, "POST", params)
                except Exception as error:
                    # Asserted path: any failure must surface, unchanged.
                    # Best-effort path: still never swallow a bug in OUR
                    # OWN call (see `_log_warning_unless_bug`) -- only a
                    # genuine connection/REST refusal is tolerated here.
                    if assert_success:
                        raise
                    self._log_warning_unless_bug(
                        "{0}(nodeEncryption={1}) failed on {2}: "
                        "{3}".format(endpoint, state, rest.ip, error),
                        error)
                    continue
                if assert_success:
                    self.assertTrue(status, "{0} failed on {1}: {2}".format(
                        endpoint, rest.ip, content))
                elif not status:
                    self._log_warning(
                        "{0}(nodeEncryption={1}) was refused on {2}: "
                        "{3}".format(endpoint, state, rest.ip, content))

    def _enable_node_encryption(self, rests):
        """Turn TLS listeners ON across every node in `rests`,
        phase-by-phase. Asserted -- see `_set_node_encryption`.
        """
        self._set_node_encryption(rests, "on", assert_success=True)

    def _disable_node_encryption(self, rests):
        """Turn TLS listeners OFF across every node in `rests`,
        phase-by-phase. Best-effort -- see `_set_node_encryption`. Runs
        from `_restore_joinable_state` against nodes that may be
        unreachable, mid-join, or already stock, unlike
        `_enable_node_encryption`, which runs against a cluster this suite
        already depends on being healthy.
        """
        self._set_node_encryption(rests, "off", assert_success=False)

    def _verify_node_encryption(self, rest, expected):
        """Best-effort proof that `rest` actually reflects
        node_encryption=`expected` -- the authoritative ns_server config
        key, read directly via diag/eval, the same call used to
        hand-verify the restore-to-off fix against a live node.

        A prior version of the restore-to-off check instead read
        `/pools/default/nodeServices` for a plain 'mgmt' entry. That check
        could never have failed on the bug it was meant to catch: 'mgmt':
        8091 is advertised even while node_encryption is still true, so it
        passed cleanly on an unrestored, still-encrypted node. A check
        that cannot fail when the thing it guards is broken is worse than
        no check -- it buys false confidence. Read the real key instead.

        Generalised to take `expected` (rather than hardcoding "false") so
        `_establish_crl_baseline` can call this with `expected=True` right
        after `_enable_node_encryption`: until this, only the disable
        direction had a per-node read-back -- the enable direction trusted
        the endpoints' own 2xx plus the CLUSTER-WIDE `clusterEncryptionLevel`
        read in `assert_crl_baseline`, so a node whose `setupNetConfig`
        returned 200 without actually applying would go unnoticed.

        Args:
            expected: True to verify node_encryption=true (after
                enabling); False to verify node_encryption=false/undefined
                (after restoring -- "undefined" is accepted only in this
                direction, since a node that was never part of a pool
                never had the key set at all).

        Never raises -- logs loudly (via `_log_warning`) instead.
        """
        try:
            status, content = rest.diag_eval(
                "ns_config:read_key_fast({node,node(),node_encryption}, "
                "undefined).", print_log=False)
            if not status:
                self._log_warning("could not read node_encryption on {0}: "
                                  "{1}".format(rest.ip, content))
                return
            value = str(content).strip().strip('"').lower()
            ok_values = ("true",) if expected else ("false", "undefined")
            if value not in ok_values:
                if not expected and value == "true":
                    self._log_warning(
                        "RESTORE DID NOT TAKE on {0}: node_encryption is "
                        "still true. The next cluster join against this "
                        "node will fail on TLS/unknown-CA exactly like "
                        "the bug this restore exists to prevent.".format(
                            rest.ip))
                else:
                    self._log_warning(
                        "node_encryption on {0} reads {1!r}, expected one "
                        "of {2} -- a cluster join or CRL check against "
                        "this node may not behave as this suite "
                        "assumes.".format(rest.ip, content, ok_values))
        except Exception as error:
            self._log_warning_unless_bug(
                "could not verify node_encryption on {0}: {1}".format(
                    rest.ip, error), error)

    def _establish_crl_baseline(self):
        """Put both clusters into the state the CRL feature actually needs.

        Without clusterEncryptionLevel=all, goxdcr's intra-cluster traffic
        (P2P, topology REST, conflict-log KV) never runs over TLS at all;
        without clientCertAuth=hybrid it never presents a client certificate
        on that traffic either way. Skip either and every CRL test in this
        suite would pass while asserting nothing. Every step here is
        asserted, not merely logged -- contrast `_restore_joinable_state`,
        which is deliberately best-effort cleanup rather than the fixture
        this suite depends on.
        """
        for cluster in (self.src_cluster, self.dest_cluster):
            master_rest = RestConnection(cluster.get_master_node())
            self.assertTrue(
                master_rest.update_autofailover_settings(False, 120),
                "failed to disable auto-failover on {0}".format(
                    cluster.get_name()))

            # Phase-by-phase across both of this cluster's nodes, not one
            # node fully switched over before the other has even started --
            # see `_set_node_encryption`.
            node_rests = [RestConnection(node) for node in cluster.get_nodes()]
            self._enable_node_encryption(node_rests)

            # Per-node read-back, not just the endpoints' own 2xx: see
            # `_verify_node_encryption` for why the disable direction had
            # this and the enable direction didn't. Best-effort/log-only,
            # like the rest of that helper -- `assert_crl_baseline` below
            # is what actually gates this method's success.
            for node_rest in node_rests:
                self._verify_node_encryption(node_rest, expected=True)

            # set_node_encryption_level returns a 2-tuple (status, content),
            # not 3 -- unpacking 3 here raised ValueError on every call.
            # On this asserted path it surfaced immediately (unlike the
            # same bug in _restore_joinable_state's best-effort twin, which
            # was silently swallowed); see `_log_warning_unless_bug`.
            status, content = master_rest.set_node_encryption_level("all")
            self.assertTrue(
                status, "failed to set clusterEncryptionLevel=all on "
                "{0}: {1}".format(cluster.get_name(), content))

            status, content = master_rest.client_cert_auth(
                "hybrid", CLIENT_CERT_PREFIXES)
            self.assertTrue(
                status, "failed to set clientCertAuth=hybrid on "
                "{0}: {1}".format(cluster.get_name(), content))

            self.assert_crl_baseline(cluster)

    def assert_crl_baseline(self, cluster):
        """Assert `cluster` is actually running the CRL feature's
        prerequisite baseline: clusterEncryptionLevel=all and
        clientCertAuth=hybrid. If either can't be asserted, the suite is
        testing nothing regardless of how its own assertions read.
        """
        rest = RestConnection(cluster.get_master_node())

        status, content, _ = rest.get_security_settings()
        settings = self.crl.parse_content(content)
        self.assertTrue(
            status and isinstance(settings, dict),
            "could not read /settings/security on {0}: {1}".format(
                cluster.get_name(), content))
        self.assertEqual(
            "all", settings.get("clusterEncryptionLevel"),
            "clusterEncryptionLevel not 'all' on {0}: {1}".format(
                cluster.get_name(), settings))

        status, content, _ = rest.get_client_cert_auth()
        auth = self.crl.parse_content(content)
        self.assertTrue(
            status and isinstance(auth, dict),
            "could not read /settings/clientCertAuth on {0}: {1}".format(
                cluster.get_name(), content))
        self.assertEqual(
            "hybrid", auth.get("state"),
            "clientCertAuth not 'hybrid' on {0}: {1}".format(
                cluster.get_name(), auth))

    # ---- CRL number ------------------------------------------------------

    def next_crl_number(self, ca_name):
        """Strictly increasing CRL number for `ca_name`.

        Every CRL revision must carry a higher number than the last from the
        same issuer, or the upload silently does nothing. Callers must take a
        number ONCE and reuse the value — calling this twice for one CRL
        wastes a number and makes filenames disagree with content.
        """
        current = self._crl_numbers.get(ca_name, CRL_NUMBER_BASE)
        self._crl_numbers[ca_name] = current + 1
        return current + 1

    # ---- PKI -------------------------------------------------------------

    def _build_pki(self):
        """Two independent CAs, so local and remote revocation stay isolated.

        Each node gets its OWN internal client cert, which is what lets a CRL
        revoke exactly one node's identity instead of the whole cluster.
        """
        for name, cluster in (("ca1", self.src_cluster),
                              ("ca2", self.dest_cluster)):
            ca_cert, ca_key = self.crl.generate_ca(name)
            self.cas[name] = (ca_cert, ca_key)
            self._trust_ca(cluster, ca_cert)
            for node in cluster.get_nodes():
                cn = "int-{0}".format(node.ip.replace(".", "-"))
                cert, key, serial = self.crl.generate_leaf_cert(
                    ca_cert, ca_key, cn,
                    email_sans=["{0}@{1}".format(cn, INTERNAL_SAN_DOMAIN)])
                self.assertTrue(
                    install_internal_client_cert(
                        node, self.crl.cert_to_pem(cert),
                        self.crl.key_to_pem(key)),
                    "failed to install/reload the internal client cert on "
                    "{0}".format(node.ip))
                self.internal_certs[node.ip] = (cert, key, serial)

    def _trust_ca(self, cluster, ca_cert):
        """Make `cluster` trust `ca_cert`.

        load_trusted_CAs() loads whatever .pem files are already in the node's
        inbox/CA directory -- it does not accept a certificate. So the CA has
        to be WRITTEN there first. Calling load_trusted_CAs() on an empty
        directory succeeds and trusts nothing, after which every client cert
        is signed by an untrusted CA and the whole suite dies in setUp with a
        TLS handshake error that reads exactly like a product defect.
        install_ca_cert does the write and the load together.
        """
        for node in cluster.get_nodes():
            self.assertTrue(
                install_ca_cert(node, self.crl.cert_to_pem(ca_cert)),
                "failed to install/trust the CA on {0}".format(node.ip))

    def ca_for(self, cluster):
        return self.cas["ca1"] if cluster is self.src_cluster else self.cas["ca2"]

    def ca_name_for(self, cluster):
        return "ca1" if cluster is self.src_cluster else "ca2"

    # ---- CRL operations --------------------------------------------------

    def set_crl_policy(self, rest, client_auth=None, node_to_node=None):
        """Set CRL policy per scope. Only the scopes named are changed."""
        # get_crl_settings()'s content is raw bytes (never a pre-parsed
        # dict) -- parse it, or `scopes` below is always {} and the "only
        # the scopes named are changed" behaviour this docstring promises
        # never actually happens (a scope another test set gets silently
        # cleared by the merge into policyPerScope).
        current = CRLUtils.parse_content(rest.get_crl_settings()[1])
        scopes = {}
        if isinstance(current, dict):
            scopes = dict(current.get("policyPerScope", {}))
        if client_auth is not None:
            scopes["clientAuth"] = client_auth
        if node_to_node is not None:
            scopes["nodeToNode"] = node_to_node
        status, content, _ = rest.post_crl_settings({"policyPerScope": scopes})
        self.assertTrue(status, "failed to set CRL policy: {0}".format(content))

    def upload_crl(self, rest, ca_name, revoked_serials):
        """Build and upload a CRL for `ca_name`, revoking `revoked_serials`."""
        ca_cert, ca_key = self.cas[ca_name]
        number = self.next_crl_number(ca_name)
        crl_pem = self.crl.build_crl(ca_cert, ca_key,
                                     revoked_serials=revoked_serials,
                                     crl_number=number)
        filename = "{0}-{1}.pem".format(ca_name, number)
        status, content, _ = rest.upload_crl_file(filename, crl_pem)
        self.assertTrue(status, "CRL upload failed: {0}".format(content))
        self._uploaded_crls.append((rest, filename))
        return filename

    def reload_crl_on_all(self, cluster):
        """reloadCrl is per node, so a cluster-wide effect needs every node."""
        for node in cluster.get_nodes():
            RestConnection(node).reload_crl()

    def revoke_internal_client_cert(self, cluster, nodes, mode="Permissive"):
        """Revoke the internal client certs of `nodes` on `cluster`.

        Sets the **nodeToNode** scope, never clientAuth. An internal client
        cert — the one whose SAN is an @internal.couchbase.com address — is
        evaluated under nodeToNode. With clientAuth=Permissive and
        nodeToNode=Disabled a revoked internal cert is happily accepted, so a
        test that sets clientAuth here passes while testing nothing. This
        helper owns the choice so no test author has to remember it.
        """
        serials = [self.internal_certs[n.ip][2] for n in nodes]
        rest = RestConnection(cluster.get_master_node())
        filename = self.upload_crl(rest, self.ca_name_for(cluster), serials)
        self.set_crl_policy(rest, node_to_node=mode)
        self.reload_crl_on_all(cluster)
        return filename

    # ---- goxdcr.log, scoped to this test ----------------------------------

    def _since_for(self, server):
        """This test's start time AS SEEN BY `server`'s own node clock
        (I7, see `_read_node_started_utc`) -- falls back to the source
        master's own reading for a server this suite never clocked
        itself, which should not happen for any node this class's tests
        actually touch.
        """
        return self._since_by_node.get(server.ip, self._test_started_utc)

    def count_new_goxdcr_lines(self, server, pattern):
        """`goxdcr_log_count`, scoped to lines logged during THIS test ON
        THIS NODE.

        goxdcr.log is never truncated between tests or runs, so an all-time
        count picks up residue left by earlier tests or manual probing --
        see `_read_node_started_utc`. Use this (not `goxdcr_log_count`
        directly) for any assertion that cares what THIS test caused.
        """
        return goxdcr_log_count(server, pattern, since=self._since_for(server))

    def wait_for_new_goxdcr_phrase(self, server, pattern, timeout=180,
                                   interval=10):
        """`wait_for_goxdcr_phrase`, scoped to lines logged during THIS
        test ON THIS NODE. See `count_new_goxdcr_lines`.
        """
        return wait_for_goxdcr_phrase(
            server, pattern, timeout=timeout, interval=interval,
            since=self._since_for(server))

    # ---- assertions ------------------------------------------------------

    def assert_no_cert_material_logged(self, server):
        """R3 redaction: no PEM body and no raw serial in goxdcr.log.

        Deliberately the ALL-TIME count (no `since`): certificate material
        must never be logged at any point in the node's history, not only
        during this test.
        """
        for pattern in ("BEGIN CERTIFICATE", "Serial Number"):
            count = goxdcr_log_count(server, pattern)
            self.assertEqual(
                0, count,
                "goxdcr.log on {0} leaked {1!r} ({2} occurrences)".format(
                    server.ip, pattern, count))

    def assert_replication_moving(self, timeout=180):
        """Assert data actually moves.

        Never assert on a task's `status` field: it reports `running` while a
        pipeline is stalled with certificate errors and nothing is replicating.

        This only proves a GAP CLOSED if the caller wrote NEW data after the
        event under test -- `_wait_for_replication_to_catchup`'s condition is
        source-count == dest-count, trivially satisfied by 0 == 0 whenever
        nothing new was written. Use `load_second_batch` to create a gap
        this call can meaningfully close.
        """
        self._wait_for_replication_to_catchup(timeout=timeout)

    def _assert_revocation_took(self, rest=None):
        """Prove a just-uploaded revocation actually took, BEFORE asserting
        on any of its consequences (I5). Without this, a CRL that silently
        failed to apply (wrong serial, wrong CA, policy not yet reloaded)
        produces "the phrase/effect never appeared", which reads as a
        product bug rather than a fixture failure -- exactly what happened
        on the first run of `test_local_revocation_p2p` (F2), the one test
        this check originated in. Every other local-revocation test now
        calls this too: `test_log_redaction_under_revocation`,
        `test_hot_reload_local_cert`, `test_unrevoke_without_rotating`,
        `test_connection_precheck_reports_revocation`, and
        `_revoke_and_restart_pipeline` (shared by F3a/F3b).

        `post_diagnostics_validate`'s content comes back raw off
        `_http_request` (bytes/str, never a pre-parsed dict) -- and
        `CRLUtils.parse_content` returns the RAW STRING unchanged when the
        body is not JSON (M9), so this must not touch the result as a dict
        without checking first, or a transport hiccup raises
        AttributeError instead of failing an assertion.
        """
        rest = rest or self.src_rest
        status, content, _ = rest.post_diagnostics_validate()
        self.assertTrue(
            status, "CRL diagnostics did not respond: {0}".format(content))
        content = self.crl.parse_content(content)
        self.assertTrue(
            isinstance(content, dict),
            "CRL diagnostics response was not JSON: {0!r}".format(content))
        statuses = [r.get("status") for r in content.get("results", [])]
        self.assertIn(
            "revoked", statuses,
            "no certificate reports as revoked after uploading the CRL, so "
            "the revocation did not take: {0}".format(content))

    def load_second_batch(self, key_prefix, num_items=100):
        """Load `num_items` NEW documents onto the SOURCE cluster only,
        under a key prefix distinct from the fixture's own initial load
        (`"<cluster-name>-key-"`, see `CouchbaseCluster.async_load_all_buckets`).

        This exists to close the gap CRITICAL 1/CRITICAL 2 in the Phase 2
        review found in `test_hot_reload_local_cert` and
        `test_unrevoke_without_rotating`: both loaded their only documents
        once, long before the revocation, and never wrote again -- so
        `assert_replication_moving()` (source count == dest count) held
        whether or not the recovery it was meant to prove actually worked.
        A second, DISTINCT batch, written only to the source AFTER
        recovery, makes the destination's item count a gap only a
        genuinely-working P2P/pipeline path can close.

        Cannot simply call `CouchbaseCluster.load_all_buckets_from_generator`
        a second time in the same test: that method caches the first
        generator it is ever given for `OPS.CREATE` (its own comment
        admits this -- "TODO append generator values if op_type is
        already present") and on every later call deep-copies THAT cached
        object regardless of what is passed in. Verified by hand: once the
        cached generator is fully consumed (which `load_and_setup_xdcr`'s
        initial load already did), a second call silently writes ZERO
        documents. Overwriting the cached entry before calling works
        around the bug without reimplementing the loader that owns
        compression/batching/task-manager wiring.
        """
        gen = BlobGenerator(key_prefix, key_prefix, self._value_size,
                            start=0, end=num_items)
        self.src_cluster._CouchbaseCluster__kv_gen[OPS.CREATE] = gen
        self.src_cluster.load_all_buckets_from_generator(gen)

    # ---- replication state, via the real framework API --------------------

    def restart_pipeline(self, cluster, settle=10):
        """Force a pipeline restart, which is what invokes CheckVBMaster.

        A pipeline only dials its P2P peer on (re)start, so a revoked
        certificate produces no P2P failure at all until something restarts it.
        """
        cluster.pause_all_replications()
        time.sleep(settle)
        cluster.resume_all_replications()

    def get_xdcr_errors(self, cluster):
        """Error + warning strings from /pools/default/tasks for every
        replication on `cluster`'s master — the source the UI's XDCR Errors
        panel renders.

        goxdcr returns either plain strings or {"time":..., "errorMsg":...}
        dicts, so each entry is normalised to a string. This mirrors
        conflictLoggingTests._get_xdcr_errors; keep them in step.
        """
        rest = RestConnection(cluster.get_master_node())
        errors = []
        for repl in rest.get_replications():
            for field in ("errors", "warnings"):
                entries = repl.get(field)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if isinstance(entry, dict):
                        errors.append(str(entry.get("errorMsg")
                                          or entry.get("msg") or entry))
                    else:
                        errors.append(str(entry))
        return errors

    def any_replication_paused(self, cluster):
        """True when any replication reports pauseRequested.

        Read from the replication document rather than the task `status`
        field, which says `running` even while the pipeline is stalled.
        """
        rest = RestConnection(cluster.get_master_node())
        for repl in rest.get_replications():
            if str(repl.get("pauseRequested", "")).lower() == "true":
                return True
        return False

    def wait_for_pause(self, cluster, timeout=420, interval=30):
        """Poll until a replication pauses. Returns True if one did."""
        deadline = time.time() + timeout
        while True:
            if self.any_replication_paused(cluster):
                return True
            if time.time() >= deadline:
                return False
            time.sleep(interval)

    def set_replication_param(self, cluster, name, value):
        """Set one XDCR replication setting on every bucket pair."""
        rest = RestConnection(cluster.get_master_node())
        for bucket in cluster.get_buckets():
            rest.set_xdcr_params(bucket.name, bucket.name, {name: value})

    # ---- teardown --------------------------------------------------------

    def _reset_crl_state(self):
        """Reset BOTH clusters — CRLBase does this for one, this suite has two."""
        for rest, filename in reversed(self._uploaded_crls):
            try:
                status, content, _ = rest.delete_crl_file(filename)
                if not status:
                    self._log_warning(
                        "could not delete CRL {0} on {1}: {2}".format(
                            filename, rest.ip, content))
            except Exception as error:
                self._log_warning_unless_bug(
                    "could not delete CRL {0} on {1}: {2}".format(
                        filename, rest.ip, error), error)
        self._uploaded_crls = []
        for cluster in (self.src_cluster, self.dest_cluster):
            try:
                rest = RestConnection(cluster.get_master_node())
                self.set_crl_policy(rest, client_auth="Disabled",
                                    node_to_node="Disabled")
                self.reload_crl_on_all(cluster)
            except Exception as error:
                self._log_warning_unless_bug(
                    "CRL reset failed on {0}: {1}".format(
                        cluster.get_name(), error), error)


class XDCRCRLLocalTests(XDCRCRLBase):

    def test_crl_fixture_smoke(self):
        """Scaffolding check: PKI installs, the baseline actually took, both
        clusters reachable, data moves.

        Not a CRL assertion — it proves the fixture works, baseline included,
        before any test depends on it. If clusterEncryptionLevel/clientCertAuth
        didn't take, the suite would be testing nothing while still passing,
        so that is asserted here explicitly rather than only in setUp.
        """
        expected_certs = (len(self.src_cluster.get_nodes())
                         + len(self.dest_cluster.get_nodes()))
        self.assertEqual(expected_certs, len(self.internal_certs),
                         "expected one internal client cert per node")
        self.assert_crl_baseline(self.src_cluster)
        self.assert_crl_baseline(self.dest_cluster)
        self.load_and_setup_xdcr()
        self.assert_replication_moving()
        self.assert_no_cert_material_logged(self.src_master)

    def test_crl_enforced_happy_path(self):
        """CRL enforcement on, nothing revoked — replication is unaffected.

        The cheapest regression guard in the suite. It asserts through
        post_diagnostics_validate that the CRL is actually loaded: without
        that, this test passes just as happily on a cluster where CRL is off,
        which makes it worthless as a guard.
        """
        empty_crl = self.upload_crl(self.src_rest, "ca1", revoked_serials=[])
        self.set_crl_policy(self.src_rest, node_to_node="Permissive")
        self.reload_crl_on_all(self.src_cluster)

        # Prove the CRL is actually LOADED AND USABLE. Do NOT asssert on the
        # diagnostics `policy` field: it reports "Require" no matter what is
        # configured -- measured on 8.5.0-1077 with both scopes set to
        # "Disabled". Asserting on it would make this test pass on a cluster
        # with CRL enforcement entirely off, which is the one thing it exists
        # to rule out.
        status, files, _ = self.src_rest.get_crl_files()
        self.assertTrue(status, "CRL file listing failed: {0}".format(files))
        self.assertIn(empty_crl, str(files),
                      "uploaded CRL {0} is not in the cluster's file list: "
                      "{1}".format(empty_crl, files))

        status, content, _ = self.src_rest.post_diagnostics_validate()
        self.assertTrue(status,
                        "CRL diagnostics did not respond: {0}".format(content))
        # post_diagnostics_validate's content comes back raw off
        # _http_request (bytes/str, never a pre-parsed dict) -- same reason
        # assert_crl_baseline routes get_security_settings/get_client_cert_auth
        # through CRLUtils.parse_content before touching it as a dict.
        content = self.crl.parse_content(content)
        self.assertTrue(
            isinstance(content, dict),
            "CRL diagnostics response was not JSON: {0!r}".format(content))
        results = content.get("results", [])
        # I3: an EMPTY (or missing) "results" gives statuses == set() below,
        # which passes assertNotEqual({"undetermined"}, set()) vacuously --
        # assert there is something to judge before judging it.
        self.assertTrue(
            results,
            "CRL diagnostics returned no per-certificate results at all -- "
            "an empty list would make the 'not undetermined' check below "
            "pass vacuously: {0}".format(content))
        statuses = {r.get("status") for r in results}
        self.assertNotEqual(
            {"undetermined"}, statuses,
            "every certificate is still 'undetermined' -- the CRL was uploaded "
            "but is not usable for these certs, so nothing is being enforced: "
            "{0}".format(content))

        # I3: post_diagnostics_validate BYPASSES the configured policy --
        # its own "policy" field is always "Require", whatever
        # /settings/crl actually says (measured on 8.5.0-1077 with both
        # scopes "Disabled"). It proves the CRL parses against these
        # certs, not that nodeToNode enforcement is switched ON. Read the
        # real configured policy back separately
        # (PHASE0-8.5-FINDINGS.md:702/I3).
        status, settings_content, _ = self.src_rest.get_crl_settings()
        self.assertTrue(
            status,
            "GET /settings/crl did not respond: {0}".format(
                settings_content))
        settings = self.crl.parse_content(settings_content)
        self.assertTrue(
            isinstance(settings, dict),
            "GET /settings/crl response was not JSON: {0!r}".format(
                settings))
        self.assertEqual(
            "Permissive",
            settings.get("policyPerScope", {}).get("nodeToNode"),
            "GET /settings/crl does not report the nodeToNode policy this "
            "test set -- enforcement may not actually be switched on: "
            "{0}".format(settings))

        self.load_and_setup_xdcr()
        self.assert_replication_moving()

        for node in self.src_cluster.get_nodes():
            count = self.count_new_goxdcr_lines(node, REVOCATION_PHRASE)
            self.assertEqual(
                0, count,
                "{0} logged {1!r} {2}x during this test with nothing "
                "revoked".format(node.ip, REVOCATION_PHRASE, count))
        self.assert_no_cert_material_logged(self.src_master)

    def test_local_revocation_p2p(self):
        """Revoking a source node's internal cert breaks P2P, diagnosably.

        Revokes the MASTER's cert, not the peer's. On the P2P dial the
        master is the TLS CLIENT and the peer is the server, and a TLS
        server validates the CLIENT's certificate -- so it is the master's
        identity that must be revoked for the peer to reject it. Measured
        (D4): the error reads `Post "https://<peer>:18091/...": remote
        error: tls: revoked certificate`, where `remote error:` means the
        alert came FROM the peer, and every phrase line lands on the node
        whose cert was revoked. Revoking the peer instead produces no
        failure at all: the master never validates the peer's client cert,
        because the peer is not acting as a client. A single-node source
        cluster has no peer at all and this test would pass without
        exercising anything.
        """
        self.load_and_setup_xdcr()
        self.assert_replication_moving()

        # Still needed: the failing dial's destination is the peer, so its
        # address is what the log-line assertion below looks for.
        peer = [n for n in self.src_cluster.get_nodes()
               if n.ip != self.src_master.ip][0]
        self.revoke_internal_client_cert(self.src_cluster, [self.src_master])

        # Prove the revocation actually took before asserting on its
        # consequence (I5). Without this, a CRL that silently failed to
        # apply produces "the phrase never appeared", which reads as a
        # product bug rather than a setup failure -- exactly what happened
        # on the first run of this test.
        self._assert_revocation_took()

        # Pause/resume forces a pipeline restart, which is what invokes
        # CheckVBMaster and therefore dials the peer over P2P.
        self.restart_pipeline(self.src_cluster)

        # The phrase lands only after retry exhaustion -- roughly 51s after
        # the first error, never on the first -- so this polls rather than
        # asserting immediately. Scoped to this test's own start time: an
        # unscoped count would go green against a match left by an earlier
        # test or manual probing, without this test having caused anything.
        count = self.wait_for_new_goxdcr_phrase(
            self.src_master, REVOCATION_PHRASE, timeout=300)
        self.assertGreater(
            count, 0,
            "{0!r} never appeared in goxdcr.log on {1} within 300s".format(
                REVOCATION_PHRASE, self.src_master.ip))

        # I4: count_new_goxdcr_lines(self.src_master, peer.ip) alone is
        # near-tautological -- it counts ANY new line containing the
        # peer's IP, and goxdcr logs peer addresses continuously during
        # normal traffic, revocation or not. Require the revocation phrase
        # AND the peer's P2P endpoint on ONE line instead: D4's measured
        # emitter lines carry the phrase before "Last error: <peer>:18091"
        # in the same physical log line (the multi-line appearance in the
        # findings doc is markdown wrapping, not real newlines), so `.*`
        # between the two substrings requires them on one line in that
        # order -- a much stronger claim than merely naming the peer.
        combined_pattern = "{0}.*{1}:18091".format(REVOCATION_PHRASE, peer.ip)
        self.assertGreater(
            self.count_new_goxdcr_lines(self.src_master, combined_pattern), 0,
            "no single goxdcr.log line during this test carried both "
            "{0!r} and the peer endpoint {1}:18091 -- naming the peer "
            "alone is not enough, since goxdcr logs peer addresses "
            "continuously during normal traffic too".format(
                REVOCATION_PHRASE, peer.ip))

        # Poll here too, same reasoning as the log-phrase wait above:
        # goxdcr writing its log line and ns_server's tasks endpoint
        # publishing the replication error are not the same event, so an
        # immediate read can land in the gap between them and see an empty
        # list even though the revocation genuinely fired. Assert on the
        # message body ("may have been revoked"), not the component
        # prefix -- D6's remote-case component contains parentheses and the
        # replication id, so splitting or matching the whole line is
        # fragile where a substring match is not.
        deadline = time.time() + 120
        errors = []
        while time.time() < deadline:
            errors = self.get_xdcr_errors(self.src_cluster)
            if any("may have been revoked" in e for e in errors):
                break
            time.sleep(10)
        self.assertTrue(
            any("may have been revoked" in e for e in errors),
            "replication error list never carried a revocation message "
            "within 120s: {0}".format(errors))

        self.assert_no_cert_material_logged(self.src_master)

    def test_log_redaction_under_revocation(self):
        """R3: a revocation failure must not put cert material in the log.

        assert_no_cert_material_logged also runs in the other tests, but this
        one drives a revocation FIRST and then checks -- redaction only
        matters on the path that logs a certificate error.

        Revokes the MASTER's cert, not the peer's -- see
        `test_local_revocation_p2p` for why revoking the peer produces no
        failure at all on this fleet.
        """
        self.load_and_setup_xdcr()
        self.revoke_internal_client_cert(self.src_cluster, [self.src_master])
        self._assert_revocation_took()   # I5
        self.restart_pipeline(self.src_cluster)
        self.assertGreater(
            self.wait_for_new_goxdcr_phrase(
                self.src_master, REVOCATION_PHRASE, timeout=300),
            0, "revocation never took effect, so redaction proves nothing")

        for node in self.src_cluster.get_nodes():
            self.assert_no_cert_material_logged(node)

    def test_hot_reload_local_cert(self):
        """R4: rotating a revoked cert recovers P2P with no goxdcr restart.

        The PID comparison IS the requirement. Without it a test that
        restarts goxdcr -- or that watches goxdcr crash and come back --
        passes while proving the opposite of what R4 asks.

        Revokes and then rotates the MASTER's own cert, not the peer's: the
        master is the TLS client on the P2P dial, so it is the master's
        identity that must be revoked (and then rotated) for this to prove
        anything -- see `test_local_revocation_p2p`.
        """
        self.load_and_setup_xdcr()
        self.revoke_internal_client_cert(self.src_cluster, [self.src_master])
        self._assert_revocation_took()   # I5
        self.restart_pipeline(self.src_cluster)
        self.assertGreater(
            self.wait_for_new_goxdcr_phrase(
                self.src_master, REVOCATION_PHRASE, timeout=300),
            0, "revocation never took effect, so recovery proves nothing")

        pid_before = goxdcr_pid(self.src_master)
        self.assertIsNotNone(pid_before, "goxdcr is not running on the master")

        # A fresh cert with a NEW serial, so the existing CRL cannot cover it.
        ca_cert, ca_key = self.cas["ca1"]
        cn = "int-{0}-rotated".format(self.src_master.ip.replace(".", "-"))
        cert, key, serial = self.crl.generate_leaf_cert(
            ca_cert, ca_key, cn,
            email_sans=["{0}@{1}".format(cn, INTERNAL_SAN_DOMAIN)])
        self.assertTrue(
            install_internal_client_cert(
                self.src_master, self.crl.cert_to_pem(cert),
                self.crl.key_to_pem(key)),
            "reloadClientCertificate did not accept the rotated certificate")
        self.internal_certs[self.src_master.ip] = (cert, key, serial)

        # CRITICAL 1: assert_replication_moving() alone (source count ==
        # dest count) is satisfied vacuously here -- the fixture's only
        # documents were loaded once, before the revocation, and both sides
        # have sat at the same count ever since; the assertion would hold
        # whether or not the cert rotation actually restored P2P. Write a
        # SECOND, distinct batch now (a different key prefix, so it cannot
        # be confused with the original load) and require the destination
        # to absorb it -- a gap only a genuinely-recovered P2P path closes.
        self.load_second_batch("hotreload-", num_items=100)
        self.assert_replication_moving(timeout=300)

        pid_after = goxdcr_pid(self.src_master)
        self.assertEqual(
            pid_before, pid_after,
            "goxdcr restarted ({0} -> {1}); R4 requires recovery without "
            "one".format(pid_before, pid_after))

    def test_unrevoke_without_rotating(self):
        """Correcting an admin mistake: drop the cert from the CRL, no
        rotation.

        The second CRL carries a higher crlNumber because next_crl_number()
        owns that. Two CRLs from one issuer at the same number collide and
        the second upload silently does nothing -- this test would then
        'fail' against a product that behaved correctly.

        Revokes the MASTER's own cert -- see `test_local_revocation_p2p` for
        why revoking the peer instead produces no failure at all.
        """
        self.load_and_setup_xdcr()
        self.revoke_internal_client_cert(self.src_cluster, [self.src_master])
        self._assert_revocation_took()   # I5
        self.restart_pipeline(self.src_cluster)
        self.assertGreater(
            self.wait_for_new_goxdcr_phrase(
                self.src_master, REVOCATION_PHRASE, timeout=300),
            0, "revocation never took effect, so un-revoking proves nothing")

        # An empty CRL at a higher number supersedes the revocation.
        self.upload_crl(self.src_rest, "ca1", revoked_serials=[])
        self.reload_crl_on_all(self.src_cluster)

        # CRITICAL 2: assert_replication_moving() alone is satisfied
        # vacuously here too, the same defect as F6 -- the fixture's only
        # documents were loaded once, before the revocation, and both
        # sides have sat at the same count ever since. Write a SECOND,
        # distinct batch now and require the destination to absorb it -- a
        # gap only a genuinely un-revoked path closes.
        self.load_second_batch("unrevoke-", num_items=100)
        self.assert_replication_moving(timeout=300)

        status, content, _ = self.src_rest.post_diagnostics_validate()
        self.assertTrue(status,
                        "CRL diagnostics did not respond: {0}".format(content))
        content = self.crl.parse_content(content)   # raw bytes, see F1
        self.assertTrue(
            isinstance(content, dict),
            "CRL diagnostics response was not JSON: {0!r}".format(content))
        statuses = [r.get("status") for r in content.get("results", [])]
        # Positive form (CRITICAL 2): assert some certificate reports
        # "valid" (the un-revoked state), not merely that "revoked" is
        # absent -- assertNotIn("revoked", statuses) passes vacuously when
        # statuses is [] (an empty or missing "results"), which is exactly
        # the shape a broken/empty diagnostics response would have.
        self.assertIn(
            "valid", statuses,
            "no certificate reports 'valid' after un-revoking -- the CRL "
            "update may not have taken: {0}".format(content))
        self.assertNotIn(
            "revoked", statuses,
            "a certificate is still reported revoked after un-revoking: "
            "{0}".format(content))

    def test_connection_precheck_reports_revocation(self):
        """Pre-check must blame the local certificate, not the target.

        This is the distinction an operator acts on: a revoked local cert
        and an unreachable target produce the same replication symptom, and
        only the pre-check separates them.

        Revokes the MASTER's own cert (never the peer's -- see
        `test_local_revocation_p2p`), since it is the master's own outbound
        identity, over the mTLS this suite's baseline requires for every
        intra-cluster call, that the pre-check's own dispatch depends on.
        """
        self.load_and_setup_xdcr()
        self.revoke_internal_client_cert(self.src_cluster, [self.src_master])
        self._assert_revocation_took()   # I5

        precheck = self.src_rest.start_connection_pre_check(
            self.dest_master.ip, self.dest_master.port,
            self.dest_master.rest_username, self.dest_master.rest_password,
            "C2")
        task_id = precheck.get("taskId") if isinstance(precheck, dict) else None
        self.assertIsNotNone(
            task_id, "pre-check returned no taskId: {0}".format(precheck))

        # The pre-check is asynchronous, and on 8.5.0-1077 `done` never
        # flips to True at all: measured across 18 polls of one task, 12
        # returned `done: False` WITH complete per-node results and 7
        # returned `done: False` with `result: None` -- the payload
        # oscillates rather than converging. So this polls for a payload
        # that HAS results, never re-reading after deciding to assert (a
        # second read can land on an empty beat even after a populated one
        # was already seen).
        #
        # M10: the payload can also oscillate CONTENT-wise -- a partial
        # first populated payload could be missing the P2P entry the
        # substring checks below look for. Retry those substring checks
        # INSIDE this same loop (against each newly-captured payload) and
        # only fail at the deadline, instead of asserting once against
        # whichever payload happened to be first.
        result = {}
        text = ""
        deadline = time.time() + 300
        while time.time() < deadline:
            payload = self.src_rest.connection_pre_check_status(
                self.dest_master.rest_username,
                self.dest_master.rest_password, task_id)
            if payload and payload.get("result"):
                result = payload
                text = str(result).lower()
                if ("local client-certificate revocation" in text
                        and "not a target connectivity issue" in text):
                    break
            time.sleep(5)
        self.assertTrue(
            result.get("result"),
            "pre-check never returned per-node results within 300s: "
            "{0}".format(result))

        # Assert the POSITIVE signal. Do NOT assert that "target
        # connectivity issue" is absent: the correct message phrases it as a
        # negation -- "...(possible local client-certificate revocation
        # under the local cluster's CRL policy), not a target connectivity
        # issue" -- so a substring check for absence trips on exactly the
        # wording that proves the product got it right. Measured verbatim
        # on 8.5.0-1077.
        self.assertIn(
            "local client-certificate revocation", text,
            "pre-check never attributes the failure to local client-cert "
            "revocation within 300s: {0}".format(result))
        self.assertIn(
            "not a target connectivity issue", text,
            "pre-check did not rule out target connectivity within 300s: "
            "{0}".format(result))

    def _revoke_and_restart_pipeline(self):
        """Shared driver: revoke the master's cert and force a pipeline
        restart.

        Revokes the MASTER's own cert, not the peer's -- see
        `test_local_revocation_p2p` for why revoking the peer produces no
        failure at all on this fleet.
        """
        self.load_and_setup_xdcr()
        self.revoke_internal_client_cert(self.src_cluster, [self.src_master])
        self._assert_revocation_took()   # I5
        self.restart_pipeline(self.src_cluster)

    def test_pipeline_restarts_on_default_retry_setting(self):
        """Default retryOnRemoteAuthErr: restarts recur, no auto-pause.

        This is the shipped behaviour, not the desirable one -- the gate is
        a REMOTE auth setting governing a LOCAL certificate failure, and it
        is unbounded. The test pins it so a silent change is caught.

        F3a and F3b are a matched pair (I6): this test's negative (no
        pause) is only meaningful because F3b
        (test_pipeline_autopauses_when_retry_disabled) proves
        `wait_for_pause` can detect a REAL pause on this same fixture --
        otherwise "never paused" could just as well mean "wait_for_pause
        never works".
        """
        self._revoke_and_restart_pipeline()
        self.assertGreater(
            self.wait_for_new_goxdcr_phrase(
                self.src_master, REVOCATION_PHRASE, timeout=300),
            0, "revocation never took effect")

        # Watch well past any single backoff cycle.
        paused = self.wait_for_pause(self.src_cluster, timeout=420)
        self.assertFalse(
            paused,
            "replication auto-paused with retryOnRemoteAuthErr at its "
            "default; 8.1 restarted indefinitely -- re-check the D5 finding")

        # I6: this test never asserted the "restarts RECUR" half of its own
        # claim -- only that no pause happened. D5 measured six restart
        # events over a 10-minute watch; >=2 is conservative and still
        # rules out "the pipeline restarted once and then just sat there
        # for an unrelated reason".
        self.assertGreaterEqual(
            self.count_new_goxdcr_lines(self.src_master, REVOCATION_PHRASE),
            2,
            "goxdcr logged {0!r} fewer than 2 times over the watch window "
            "-- the pipeline did not keep restarting as D5 found it "
            "should".format(REVOCATION_PHRASE))

    def test_pipeline_autopauses_when_retry_disabled(self):
        """retryOnRemoteAuthErr=false: the pipeline pauses within one
        cycle.
        """
        self._revoke_and_restart_pipeline()
        self.set_replication_param(
            self.src_cluster, "retryOnRemoteAuthErr", "false")

        paused = self.wait_for_pause(self.src_cluster, timeout=420)
        self.assertTrue(
            paused,
            "replication never paused with retryOnRemoteAuthErr=false")
        self.assertGreater(
            self.count_new_goxdcr_lines(
                self.src_master, "It will now be paused"),
            0, "pause happened without the operator-facing message")


class XDCRCRLRemoteTests(XDCRCRLBase):
    """Surface #4 — the remote cluster reference.

    The CRL lives on the TARGET cluster and the scope is clientAuth, not
    nodeToNode: the reference presents an ordinary client cert, not an internal
    one, so it is enforced under clientAuth normally.

    Half of this class's six tests are written to SPECIFIED behaviour rather
    than shipped behaviour and are EXPECTED TO FAIL on 8.5.0-1077 --
    F11/F12/F13, all downstream of DEF-1 (docs/xdcr-crl/PHASE0-8.5-FINDINGS.md):

      DEF-1 (not fixed) -- getCombinedError() wraps a full-encryption
      reference's connectivity failure unconditionally, before
      fetchRemoteClusterInfo's IsCertRevocationRelatedError check is ever
      reached, so validate/create/edit never surface the CRL hint --
      only the generic "cannot use HostName ... as a https address or a
      http address" wrapper. test_remote_ref_validate_revoked_cert (F11),
      test_create_remote_ref_with_revoked_cert (F12) and
      test_edit_remote_ref_to_revoked_cert (F13) assert the hint anyway,
      per spec, and are GROUP=P2.

      DEF-2 (fixed ONLY on the pipeline-restart path) -- a replication that
      has not restarted since the revocation shows only the generic
      connectivity wrapper; pause->resume produces the specific
      "may have been revoked by the target cluster's certificate revocation
      (CRL) policy" message, but only after a poll of up to ~3 minutes (5
      for margin -- measured 0s in one trial, 2m46s in the other).
      test_running_replication_remote_revocation (F10) and
      test_mid_replication_error_propagation (F15) are written against
      exactly this restart+poll shape and are the two tests in this class
      expected to PASS -- GROUP=P0.

      DEF-3 (not fixed) -- dismissPeriodicPushCertAlerts' inverted guard.
      PHASE0-8.5-FINDINGS.md is explicit that no cluster probe can improve
      on the source read that already proves this dead code (the finding
      was made by reading the shipped revision directly, not measured on a
      cluster), so no test for it is added here.

    test_hot_reload_remote_ref_cert (F14) was ORIGINALLY predicted GROUP=P2
    by analogy with DEF-2's restart-only finding: unlike the LOCAL
    hot-reload case (an already-open P2P backoff loop dials again on its
    own and picks up a rotated cert with no explicit restart), DEF-2 found
    that the remote reference's dial only re-validates on an explicit
    pipeline restart when the certificate is REVOKED. A live run showed
    that reasoning does not transfer to ROTATION: editing to a fresh,
    unrevoked cert (no restart_pipeline() call) DOES recover the
    replication. DEF-1 concerns the hint TEXT surfaced on a failing
    validate/create/edit; F14 is about recovery after a SUCCEEDING edit --
    a different code path that DEF-1 says nothing about and evidently
    already works. F14 is GROUP=P0, unmodified from its original assertions
    (the PID-unchanged bar and the second-batch absorption proof both still
    apply and both pass) -- only the prediction, never the test, was wrong.

    Never softened to match a prediction -- see task-15-report.md's
    "F14 was mis-predicted" section for the full account, and its per-test
    table for why every other test is expected to pass or fail on this
    build.
    """

    def setUp(self):
        super(XDCRCRLRemoteTests, self).setUp()
        ca_cert, ca_key = self.cas["ca2"]
        # XDCR_CLIENT_CN, never a hardcoded literal: this CN must stay in
        # lockstep with the RBAC username _provision_xdcr_client_user()
        # creates on the target, or every dial fails auth for a reason
        # that looks nothing like revocation.
        self.xdcr_client_cert, self.xdcr_client_key, self.xdcr_client_serial = (
            self.crl.generate_leaf_cert(ca_cert, ca_key, XDCR_CLIENT_CN))
        self.remote_ref_name = None
        self._provision_xdcr_client_user()

    def revoke_remote_client_cert(self):
        """Revoke the reference's client cert on the TARGET, under clientAuth."""
        filename = self.upload_crl(self.dest_rest, "ca2",
                                   [self.xdcr_client_serial])
        self.set_crl_policy(self.dest_rest, client_auth="Permissive")
        self.reload_crl_on_all(self.dest_cluster)
        return filename

    # ---- fixture helpers ---------------------------------------------------

    def _provision_xdcr_client_user(self):
        """Create the RBAC user the reference's client cert authenticates AS.

        The suite's baseline maps clientAuth via subject.cn with no
        prefix/delimiter (CLIENT_CERT_PREFIXES): the whole CN becomes the
        username Couchbase looks up. The internal client certs _build_pki
        installs sidestep this (their @internal.couchbase.com SAN is
        recognised as intra-cluster node identity, not an RBAC username),
        but this class's "xdcrclient" cert has no such SAN -- it is an
        ordinary client cert, exactly what a real remote-cluster reference
        presents -- so a Couchbase user actually named "xdcrclient" must
        exist on the TARGET or every dial that presents it fails
        authentication before CRL enforcement is ever reached, for a
        reason that has nothing to do with revocation. replication_target[*]
        is the same minimal role this codebase already grants XDCR's own
        replicator_user (see XDCRNewBaseTest.setUp's _replicator_role
        block) -- pytests/xdcr/AGENTS.md's CNG entry separately confirms it
        is sufficient for a full classic couchbase:// replication, which
        is exactly this surface once the reference converts to
        secureType=full.
        """
        testuser = [{"id": XDCR_CLIENT_CN, "name": XDCR_CLIENT_CN,
                    "password": "password"}]
        RbacBase().create_user_source(testuser, "builtin", self.dest_master)
        role_list = [{"id": XDCR_CLIENT_CN, "name": XDCR_CLIENT_CN,
                     "roles": "replication_target[*]"}]
        RbacBase().add_user_role(role_list, self.dest_rest, "builtin")

    @staticmethod
    def _pem_str(pem):
        return pem.decode() if isinstance(pem, (bytes, bytearray)) else pem

    @staticmethod
    def _response_text(content):
        """Every _http_request failure returns `content` RAW (bytes, never
        pre-parsed) -- see rule 4 in the task-15 brief. Decode before any
        substring assertion, or a transport hiccup trips a TypeError
        instead of failing the assertion it was meant to guard.
        """
        if isinstance(content, (bytes, bytearray)):
            return content.decode("utf-8", "replace")
        return str(content)

    def _discover_remote_ref(self):
        """The XDCRRemoteClusterRef OBJECT load_and_setup_xdcr() created
        for C1->C2 -- not just its name.

        NOT used to tear down its replication (F12 needs exactly that,
        but through RestConnection.remove_all_replications() instead --
        see test_create_remote_ref_with_revoked_cert for why
        XDCRRemoteClusterRef.stop_all_replications() cannot be used).
        Kept for _discover_remote_ref_name(), which needs the object only
        to read its .get_name().
        """
        for ref in self.src_cluster.get_remote_clusters():
            if ref.get_dest_cluster() is self.dest_cluster:
                return ref
        self.fail("no C1->C2 remote cluster reference exists yet -- "
                  "load_and_setup_xdcr() must run before this helper")

    def _discover_remote_ref_name(self):
        """The name load_and_setup_xdcr() gave the C1->C2 reference (the
        framework's own Utility.get_rc_name, not anything this class picks).
        """
        return self._discover_remote_ref().get_name()

    def _target_trust_certificate(self):
        """The CA(s) that sign the TARGET's own node (TLS server)
        certificates -- what the reference's 'certificate' field must
        carry, per XDCRRemoteClusterRef.add()'s own convention
        (rest_conn_dest.get_cluster_ceritificate(), the well-tested path
        every other encrypted-XDCR suite in this codebase uses).

        GET /pools/default/certificate -- i.e. get_cluster_ceritificate()
        itself -- CANNOT be used here. Once a cluster has an UPLOADED CA,
        ns_server disables that endpoint outright:
            400 b'this API is disabled, please use GET
            /pools/default/trustedCAs, see documentation for details'
        and _build_pki's install_ca_cert(ca2) on the dest cluster puts
        EVERY test in this suite into exactly that state. (Confirmed live:
        the endpoint answers 200 on a cluster with no uploaded CA, which
        is exactly why this could not have been caught by reading the
        code alone -- do not reach for get_cluster_ceritificate() again
        here without re-reading this comment.)

        _build_pki never re-signs node identity certs -- only the per-node
        internal client certs and this class's own xdcrclient leaf are
        signed by ca2 -- so each target node's TLS identity stays its own
        self-signed, auto-generated ("generated") trusted-CA entry.
        GET /pools/default/trustedCAs lists BOTH kinds; keep only
        type=="generated" (skip "uploaded", which is ca2 itself -- the CA
        that signs the CLIENT cert under test, never a node's own TLS
        identity), and concatenate EVERY generated entry's pem, not just
        the first: the target is a two-node cluster and each node carries
        its own generated CA, so trusting only one leaves the reference
        intermittently broken depending on which node it happens to dial.
        """
        # get_trusted_CAs() already json.loads()s its own response (unlike
        # the raw CRL/diagnostics endpoints this suite otherwise wraps),
        # but parse_content()+isinstance still guards the shape -- rule 4.
        cas = self.crl.parse_content(self.dest_rest.get_trusted_CAs())
        self.assertTrue(
            isinstance(cas, list),
            "GET /pools/default/trustedCAs on {0} did not return a list: "
            "{1!r}".format(self.dest_master.ip, cas))
        generated = [ca for ca in cas
                    if isinstance(ca, dict) and ca.get("type") == "generated"
                    and ca.get("pem")]
        self.assertTrue(
            generated,
            "no 'generated' trustedCAs entries on {0} -- every node's "
            "own auto-generated identity CA should be one: {1}".format(
                self.dest_master.ip, cas))
        return "".join(ca["pem"] for ca in generated)

    def _remote_ref_payload(self, cert_pem, key_pem, name=None):
        """POST body for a secureType=full reference authenticated with
        `cert_pem`/`key_pem` as clientCertificate/clientKey.

        'certificate' is every 'generated' CA trusted on the TARGET (see
        _target_trust_certificate) -- what a client needs to validate the
        target's TLS server identity -- NOT ca2 (ca2 only signs the
        client cert being presented here, and the per-node internal
        client certs; it is never a target node's own TLS server
        identity). Username/password are deliberately never included:
        DEF-1's finding notes ns_server rejects "username and client
        certificate cannot both be given when secure type is full"
        outright.
        """
        return {
            "hostname": "{0}:{1}".format(
                self.dest_master.ip, self.dest_master.port),
            "name": name or self.remote_ref_name,
            "demandEncryption": "on",
            "secureType": "full",
            "certificate": self._target_trust_certificate(),
            "clientCertificate": self._pem_str(cert_pem),
            "clientKey": self._pem_str(key_pem),
        }

    def _post_remote_cluster_raw(self, params, path_name=None,
                                 just_validate=False):
        """Raw POST to /pools/default/remoteClusters[/<path_name>]
        [?just_validate=1].

        Deliberately bypasses RestConnection.add_remote_cluster /
        modify_remote_cluster: both retry on failure and raise once
        retries are exhausted (see __remote_clusters), which would
        swallow exactly the failure response F11-F14 need to inspect.
        Mirrors the direct-REST style stagedCredentialsXDCR.py uses for
        the same reason (test_mutual_exclusion_credentials_certs).
        """
        api = self.src_rest.baseUrl + "pools/default/remoteClusters"
        if path_name:
            api += "/" + urllib.parse.quote(path_name, safe="")
        if just_validate:
            api += "?just_validate=1"
        encoded = urllib.parse.urlencode(params)
        return self.src_rest._http_request(api, "POST", encoded)

    def _convert_reference_to_full_encryption(self, cert_pem, key_pem):
        """Edit the (already-created, plain) C1->C2 reference in place to
        secureType=full, authenticated with `cert_pem`/`key_pem`. Uses the
        wrapped, retrying modify_remote_cluster deliberately -- unlike
        _post_remote_cluster_raw's callers, this call is expected to
        succeed (the cert is not yet revoked at this point in every test
        that calls it) and should raise loudly if it does not, rather than
        letting a fixture failure masquerade as a product one.

        certificate= is _target_trust_certificate(), NOT
        self.dest_rest.get_cluster_ceritificate() -- see that helper's
        docstring: GET /pools/default/certificate 400s once a cluster has
        an uploaded CA, which _build_pki guarantees for every test here.
        """
        self.src_rest.modify_remote_cluster(
            self.dest_master.ip, self.dest_master.port,
            self.dest_master.rest_username, self.dest_master.rest_password,
            self.remote_ref_name, demandEncryption=1,
            certificate=self._target_trust_certificate(),
            clientCertificate=self._pem_str(cert_pem),
            clientKey=self._pem_str(key_pem), encryptionType="full")

    def _setup_full_encryption_replication(self):
        """Standard fixture for every test in this class: a plain reference
        + replication via the framework's own load_and_setup_xdcr() (same
        call XDCRCRLLocalTests uses), converted in place to secureType=full
        with the (not yet revoked) xdcrclient cert, then explicitly
        restarted so every test starts from an identical, already-settled
        state rather than trusting that the demandEncryption edit's own
        implicit pipeline restart already finished.
        """
        self.load_and_setup_xdcr()
        self.remote_ref_name = self._discover_remote_ref_name()
        self._convert_reference_to_full_encryption(
            self.crl.cert_to_pem(self.xdcr_client_cert),
            self.crl.key_to_pem(self.xdcr_client_key))
        self.restart_pipeline(self.src_cluster)
        self.assert_replication_moving()

    def _assert_remote_revocation_took(self, crl_filename):
        """I5, remote case -- deliberately NOT XDCRCRLBase._assert_revocation_took.

        That helper (what the LOCAL half uses) asserts that SOME
        certificate reads "revoked" via POST
        /settings/crl/diagnostics/validate. That endpoint only evaluates
        the cluster's INSTALLED certificates -- node certs and internal
        client certs -- and this class's xdcrclient certificate is never
        installed on any node; it lives only inside the remote-cluster
        reference. Diagnostics therefore has no entry for it and can
        NEVER report it "revoked", no matter how correctly the CRL
        loaded. It works for the LOCAL half only because the internal
        client certs that half revokes are always installed on a node --
        the mechanism, not the assertion, is what differs here. Asserting
        "revoked" in this class would be a precondition the endpoint
        cannot observe, and every one of the six tests would fail on it
        instead of on what each is actually written to test.

        Three checks that CAN observe the remote case, all against
        self.dest_rest (the CRL lives on the TARGET, not self.src_rest):

          1. the uploaded CRL file is present -- the same direct proof
             F1 uses on the local half.
          2. the clientAuth policy reads back as what
             revoke_remote_client_cert() set. Read GET /settings/crl
             for this, never the diagnostics call's own "policy" field,
             which always reports "Require" regardless of the configured
             policy (I3, see assert_crl_baseline/test_crl_enforced_happy_path).
          3. diagnostics/validate parses against ca2's certificates at
             all -- no entry reads "undetermined" (the shape a CRL that
             failed to parse for this issuer would show, detail "no
             usable CRL for this certificate"). Deliberately does NOT
             require any entry to read "revoked": none of the
             certificates diagnostics evaluates (node/internal-client)
             were ever revoked by this CRL, only xdcrclient was, and
             diagnostics cannot see xdcrclient at all.
        """
        status, files, _ = self.dest_rest.get_crl_files()
        self.assertTrue(
            status, "CRL file listing failed on {0}: {1}".format(
                self.dest_master.ip, files))
        self.assertIn(
            crl_filename, str(files),
            "uploaded CRL {0} is not in {1}'s file list: {2}".format(
                crl_filename, self.dest_master.ip, files))

        status, settings_content, _ = self.dest_rest.get_crl_settings()
        self.assertTrue(
            status, "GET /settings/crl did not respond on {0}: {1}".format(
                self.dest_master.ip, settings_content))
        settings = self.crl.parse_content(settings_content)
        self.assertTrue(
            isinstance(settings, dict),
            "GET /settings/crl response on {0} was not JSON: "
            "{1!r}".format(self.dest_master.ip, settings))
        self.assertEqual(
            "Permissive",
            settings.get("policyPerScope", {}).get("clientAuth"),
            "GET /settings/crl on {0} does not report clientAuth="
            "Permissive -- enforcement may not actually be switched on: "
            "{1}".format(self.dest_master.ip, settings))

        status, content, _ = self.dest_rest.post_diagnostics_validate()
        self.assertTrue(
            status, "CRL diagnostics did not respond on {0}: {1}".format(
                self.dest_master.ip, content))
        content = self.crl.parse_content(content)
        self.assertTrue(
            isinstance(content, dict),
            "CRL diagnostics response on {0} was not JSON: {1!r}".format(
                self.dest_master.ip, content))
        results = content.get("results", [])
        self.assertTrue(
            results,
            "CRL diagnostics on {0} returned no per-certificate results "
            "at all: {1}".format(self.dest_master.ip, content))
        statuses = {r.get("status") for r in results}
        self.assertNotIn(
            "undetermined", statuses,
            "a certificate reads 'undetermined' on {0} -- the CRL is "
            "not usable against ca2's certificates: {1}".format(
                self.dest_master.ip, content))

    def _connectivity_cause_shows_revocation(self, timeout=120, interval=10):
        """Poll /pools/default/remoteClusters on the SOURCE for this
        reference's connectivityErrors[].causeOfError to mention the
        revoked certificate -- available immediately, without waiting for
        a pipeline restart (PHASE0-8.5-FINDINGS.md DEF-2: connectivityStatus
        goes RC_ERROR and causeOfError carries 'remote error: tls: revoked
        certificate' from the very first dial after the CRL takes effect,
        well before the restart-only specific message appears).

        get_remote_clusters() already parses its own JSON (unlike the raw
        CRL/diagnostics endpoints this suite otherwise wraps), so no
        CRLUtils.parse_content step is needed here.
        """
        deadline = time.time() + timeout
        causes = []
        while time.time() < deadline:
            for ref in self.src_rest.get_remote_clusters():
                if ref.get("name") != self.remote_ref_name:
                    continue
                errors = ref.get("connectivityErrors") or []
                causes = [str(e.get("causeOfError", "")) for e in errors
                         if isinstance(e, dict)]
                if any("revoked certificate" in c for c in causes):
                    return True, causes
            time.sleep(interval)
        return False, causes

    # ---- F10 / F15: the two tests expected to PASS -------------------------

    def test_running_replication_remote_revocation(self):
        """F10 — a RUNNING full-encryption replication surfaces the
        classified remote-revocation error and keeps retrying, rather than
        dying outright.

        DEF-2 is fixed only on the pipeline-restart path: an unrestarted
        replication shows only the generic connectivity wrapper. This test
        forces the restart (self.restart_pipeline) and then POLLS for up to
        5 minutes -- never asserting immediately -- because the specific
        message was measured to appear anywhere from 0s to 2m46s after the
        restart, depending on how much work was pending at resume. GROUP=P0.
        """
        self._setup_full_encryption_replication()

        crl_filename = self.revoke_remote_client_cert()
        self._assert_remote_revocation_took(crl_filename)   # I5, remote case

        # NOT asserted (corrected after a live run): D4's emitter table in
        # PHASE0-8.5-FINDINGS.md lists exactly one remote-path site for
        # REVOCATION_PHRASE -- metadata_svc/remote_cluster_service.go:3148,
        # annotated "see DEF-1 -- unreachable in practice". DEF-1 means
        # fetchRemoteClusterInfo's classification branch that would log it
        # is never reached for a full-encryption reference, so this phrase
        # is never written to goxdcr.log for a REMOTE revocation at all --
        # unlike the local half, where P2PManager/XDCRFactory/
        # GenericPipeline all emit it for a LOCAL one. Asserting on it here
        # would be asserting on a mechanism DEF-1 makes unreachable, not a
        # product regression; the remote error surfaces through the
        # REPLICATION ERROR LIST instead (REMOTE_REVOCATION_MESSAGE below,
        # and F15). Scoped count logged for visibility only (rule 1: still
        # the scoped helper, never the unscoped goxdcr_log_count).
        self._log_info(
            "{0!r} count in goxdcr.log on {1} after revoking the remote "
            "reference's client cert (informational only -- DEF-1 makes "
            "this phrase unreachable for a remote revocation, see D4): "
            "{2}".format(
                REVOCATION_PHRASE, self.src_master.ip,
                self.count_new_goxdcr_lines(
                    self.src_master, REVOCATION_PHRASE)))

        # Available before any restart: the reference's own connectivity
        # state already attributes the failure to the revoked certificate,
        # and keeps being retried rather than the reference going silent.
        shown, causes = self._connectivity_cause_shows_revocation()
        self.assertTrue(
            shown,
            "connectivityErrors on reference {0!r} never attributed the "
            "connection failure to the revoked certificate within 120s: "
            "{1}".format(self.remote_ref_name, causes))

        self.restart_pipeline(self.src_cluster)

        # Never assert immediately -- see PHASE0-8.5-FINDINGS.md DEF-2: 0s
        # in one trial, 2m46s in the other. 300s is the documented margin.
        deadline = time.time() + 300
        errors = []
        while time.time() < deadline:
            errors = self.get_xdcr_errors(self.src_cluster)
            if any("The client certificate for remote cluster" in e
                  and REMOTE_REVOCATION_MESSAGE in e for e in errors):
                break
            time.sleep(15)
        self.assertTrue(
            any("The client certificate for remote cluster" in e
               and REMOTE_REVOCATION_MESSAGE in e for e in errors),
            "replication error list on {0} never carried the remote-"
            "revocation message within 300s of the restart: {1}".format(
                self.src_cluster.get_name(), errors))

        # Keeps retrying, not auto-paused. NOT a presence check
        # (len(get_replications()) >= 1): D8 (PHASE0-8.5-FINDINGS.md)
        # records the task's own "status" field stays "running"
        # throughout the stall in EVERY reachable state here, including
        # an auto-paused one -- so a presence-only assertion is true no
        # matter what happens and covers nothing. any_replication_paused()
        # reads pauseRequested directly (the same signal wait_for_pause
        # polls, D5's local-surface auto-pause distinction) and is what
        # actually tells "still retrying on the existing backoff" apart
        # from "auto-paused" on this remote surface.
        self.assertFalse(
            self.any_replication_paused(self.src_cluster),
            "the replication auto-paused after the restart -- expected "
            "it to keep retrying on the existing backoff instead")

    def test_mid_replication_error_propagation(self):
        """F15 — the specific hint propagates to REPLICATION STATUS (not
        just the reference's connectivityErrors), and what the errors list
        GAINS is the specific message -- never assert that an earlier
        generic entry is still there to retain: PHASE0-8.5-FINDINGS.md
        measured the errors array capped at 10 entries with the generic
        wrappers aged out by the time the specific entry appeared. GROUP=P0.
        """
        self._setup_full_encryption_replication()

        crl_filename = self.revoke_remote_client_cert()
        self._assert_remote_revocation_took(crl_filename)

        def has_specific(errors):
            return any(REMOTE_REVOCATION_MESSAGE in e for e in errors)

        # Baseline, pre-restart: DEF-2 says a running (unrestarted)
        # pipeline shows only the generic wrapper. Confirming the specific
        # message is NOT already present makes "gains it" below meaningful
        # rather than residue from this same fixture's own earlier state.
        baseline = self.get_xdcr_errors(self.src_cluster)
        self.assertFalse(
            has_specific(baseline),
            "the specific remote-revocation message was already present "
            "BEFORE the pipeline ever restarted -- that contradicts DEF-2 "
            "and means this fixture is not measuring what this test "
            "thinks it is: {0}".format(baseline))

        self.restart_pipeline(self.src_cluster)

        deadline = time.time() + 300
        errors = baseline
        while time.time() < deadline:
            errors = self.get_xdcr_errors(self.src_cluster)
            if has_specific(errors):
                break
            time.sleep(15)
        self.assertTrue(
            has_specific(errors),
            "replication status never GAINED the specific remote-"
            "revocation message within 300s of the restart: {0}".format(
                errors))

        # Assert the message BODY (rule 6 / D6): the remote-case component
        # is 'pipelineMgr.validatePipeline(<replId>)', which embeds the
        # replication id and its own parentheses/slashes, so splitting the
        # entry on ':' or matching a whole reconstructed line is fragile --
        # match the stable message body directly instead.
        self.assertTrue(
            any("The client certificate for remote cluster" in e
               and REMOTE_REVOCATION_MESSAGE in e for e in errors),
            "no entry carried the full remote-revocation message body: "
            "{0}".format(errors))

    # ---- F11 / F12 / F13: DEF-1, written to fail -----------------------

    def test_remote_ref_validate_revoked_cert(self):
        """F11 — SPECIFIED: just_validate=1 against a full-encryption
        reference secured with a revoked client cert must return the
        handshake failure AND the DEF-1 hint.

        DEF-1 is NOT fixed on 8.5.0-1077: getCombinedError() wraps a
        full-encryption reference's connectivity failure unconditionally,
        before fetchRemoteClusterInfo's IsCertRevocationRelatedError check
        is ever reached -- so only the generic 'cannot use HostName ... as
        a https address or a http address' wrapper comes back, never the
        hint. Written to the SPECIFIED behaviour; expected to FAIL until
        DEF-1 is fixed. GROUP=P2.
        """
        self._setup_full_encryption_replication()
        crl_filename = self.revoke_remote_client_cert()
        self._assert_remote_revocation_took(crl_filename)

        params = self._remote_ref_payload(
            self.crl.cert_to_pem(self.xdcr_client_cert),
            self.crl.key_to_pem(self.xdcr_client_key))
        status, content, _ = self._post_remote_cluster_raw(
            params, path_name=self.remote_ref_name, just_validate=True)
        body = self._response_text(content)

        self.assertFalse(
            status,
            "just_validate=1 unexpectedly ACCEPTED a reference secured "
            "with a revoked client certificate: {0}".format(body))
        self.assertIn(
            DEF1_HINT, body,
            "DEF-1: just_validate did not surface the CRL hint for a "
            "revoked remote-reference client cert -- got: {0}".format(body))

    def test_create_remote_ref_with_revoked_cert(self):
        """F12 — SPECIFIED: creating a fresh reference whose client cert is
        already revoked must fail immediately with the hint, rather than
        succeeding and failing later.

        Topology workaround (PHASE0-8.5-FINDINGS.md, 'Probes that could
        not be run'): ns_server refuses a second reference to a cluster
        that already has one, and this fleet has only two clusters.
        Delete the existing C1->C2 reference first (this also removes the
        replication under it -- acceptable here, this test is about
        creation-time validation, not an ongoing replication), then create
        a fresh one under the SAME name with the already-revoked cert.
        That exercises the same creation-time validation path F12 is for.

        DEF-1 not fixed: expected to FAIL, same hint as F11. GROUP=P2.
        """
        self._setup_full_encryption_replication()
        crl_filename = self.revoke_remote_client_cert()
        self._assert_remote_revocation_took(crl_filename)

        ref_name = self.remote_ref_name
        # ns_server refuses to delete a reference while a replication
        # still uses it ("remoteCluster API 'remove cluster' failed",
        # confirmed on a live run) -- the replication must go first.
        #
        # XDCRRemoteClusterRef.stop_all_replications() looked like the
        # right way to do that (through the framework, not a raw REST
        # call), but it is BROKEN in xdcrnewbasetests.py, confirmed on a
        # live run: XDCReplication.cancel() -> __validate_cancel_event()
        # unconditionally calls XDCRServiceEvents.delete_replication()
        # with ONE argument while that function requires SIX
        # (source_bucket, target_bucket, id, remote_ref_name,
        # filter_expression) -- a TypeError on EVERY caller of cancel(),
        # not something specific to this class. That is a pre-existing
        # defect shared by every XDCR suite that calls
        # stop_all_replications()/cancel(); it is out of scope for this
        # change (do not modify xdcrnewbasetests.py here) and is being
        # filed separately. Do NOT "simplify" this back to
        # stop_all_replications() -- it will raise.
        #
        # RestConnection.remove_all_replications() sidesteps the bug
        # entirely: it never touches XDCRRemoteClusterRef or the
        # audit-event path, just a plain REST DELETE against each
        # replication's own cancelURI (the exact call
        # CouchbaseCluster.cleanup_cluster(from_rest=True) already uses
        # for its own bulk teardown elsewhere in this codebase). Safe to
        # use un-scoped-by-reference here: this fixture's topology
        # (ctopology=chain, one bucket) never has more than the single
        # C1->C2 replication under test at this point.
        self.src_rest.remove_all_replications()
        # stop_replication() (inside remove_all_replications()) only
        # waits for ns_server to ACCEPT the DELETE, not for goxdcr to
        # finish tearing the pipeline down -- remove_remote_cluster()
        # raises a bare Exception on any non-2xx, and this exact
        # rejection ("remoteCluster API 'remove cluster' failed") was
        # seen once already during development. An errors=1 here would
        # be INDISTINGUISHABLE in a results table from F12's designed
        # P2 hint-text failure -- a real regression would be recorded
        # as "pending DEF-1" when the test never even ran its
        # assertion. Poll for the replication to actually be gone
        # first, with a clearly-labelled failure if it never is, rather
        # than racing the delete.
        deadline = time.time() + 60
        while time.time() < deadline and self.src_rest.get_replications():
            time.sleep(5)
        self.assertFalse(
            self.src_rest.get_replications(),
            "replication(s) still present 60s after "
            "remove_all_replications() -- cannot safely delete the "
            "reference out from under a live replication")
        self.src_rest.remove_remote_cluster(ref_name)

        params = self._remote_ref_payload(
            self.crl.cert_to_pem(self.xdcr_client_cert),
            self.crl.key_to_pem(self.xdcr_client_key), name=ref_name)
        status, content, _ = self._post_remote_cluster_raw(params)
        body = self._response_text(content)

        self.assertFalse(
            status,
            "creating a brand-new reference with an already-revoked "
            "client certificate unexpectedly SUCCEEDED: {0}".format(body))
        self.assertIn(
            DEF1_HINT, body,
            "DEF-1: create did not surface the CRL hint for an "
            "already-revoked client cert -- got: {0}".format(body))

    def test_edit_remote_ref_to_revoked_cert(self):
        """F13 — SPECIFIED: editing an EXISTING reference so its client
        cert is revoked must surface the hint at edit time, rather than
        silently accepting a doomed config.

        DEF-1 not fixed: expected to FAIL, same hint as F11/F12. GROUP=P2.
        """
        self._setup_full_encryption_replication()
        crl_filename = self.revoke_remote_client_cert()
        self._assert_remote_revocation_took(crl_filename)

        params = self._remote_ref_payload(
            self.crl.cert_to_pem(self.xdcr_client_cert),
            self.crl.key_to_pem(self.xdcr_client_key))
        status, content, _ = self._post_remote_cluster_raw(
            params, path_name=self.remote_ref_name, just_validate=False)
        body = self._response_text(content)

        self.assertFalse(
            status,
            "editing the reference to a revoked client certificate "
            "unexpectedly SUCCEEDED -- ns_server accepted a doomed "
            "config: {0}".format(body))
        self.assertIn(
            DEF1_HINT, body,
            "DEF-1: edit did not surface the CRL hint for a revoked "
            "client cert -- got: {0}".format(body))

    # ---- F14: recovery via rotation, no restart -- passes -----------------

    def test_hot_reload_remote_ref_cert(self):
        """F14 — editing the reference to a FRESH, unrevoked client cert
        recovers it (and its replications), with NO restart.

        Mirrors XDCRCRLLocalTests.test_hot_reload_local_cert (R4) for the
        remote surface: the same PID-unchanged bar, and the same 'load a
        second, distinct batch and require the destination to absorb it'
        proof of genuine recovery rather than a vacuous count comparison.

        PREDICTION CORRECTED (was GROUP=P2, now GROUP=P0): this test was
        originally written expecting to FAIL, by analogy with DEF-2's
        finding that the remote reference's dial only re-validates on an
        explicit pipeline restart -- reasoning that was measured true for
        REVOKING a cert but does not hold for ROTATING one. A live run
        showed the edit-only recovery below (no restart_pipeline() call)
        DOES work: DEF-1 is about the hint TEXT on a failing
        validate/create/edit, and this edit does not fail -- recovery via a
        SUCCEEDING edit is a different mechanism DEF-1 says nothing about.
        The test itself was not weakened to fit the old prediction; only
        this docstring and its GROUP were corrected. GROUP=P0.
        """
        self._setup_full_encryption_replication()
        crl_filename = self.revoke_remote_client_cert()
        self._assert_remote_revocation_took(crl_filename)
        self.restart_pipeline(self.src_cluster)

        deadline = time.time() + 300
        broken = False
        while time.time() < deadline:
            errors = self.get_xdcr_errors(self.src_cluster)
            if any("The client certificate for remote cluster" in e
                  and REMOTE_REVOCATION_MESSAGE in e for e in errors):
                broken = True
                break
            time.sleep(15)
        self.assertTrue(
            broken,
            "revocation never took effect on the running replication "
            "within 300s of the restart, so recovery proves nothing")

        pid_before = goxdcr_pid(self.src_master)
        self.assertIsNotNone(
            pid_before, "goxdcr is not running on the source master")

        # A FRESH cert -- same CN (the RBAC user is already provisioned
        # for it), new serial the existing CRL cannot cover.
        ca_cert, ca_key = self.cas["ca2"]
        fresh_cert, fresh_key, _ = self.crl.generate_leaf_cert(
            ca_cert, ca_key, XDCR_CLIENT_CN)

        params = self._remote_ref_payload(
            self.crl.cert_to_pem(fresh_cert), self.crl.key_to_pem(fresh_key))
        status, content, _ = self._post_remote_cluster_raw(
            params, path_name=self.remote_ref_name, just_validate=False)
        self.assertTrue(
            status,
            "editing the reference to a FRESH, unrevoked client cert was "
            "refused: {0}".format(self._response_text(content)))

        # No explicit restart_pipeline() call -- F14's bar is recovery
        # WITHOUT one, and (per the docstring) that bar is in fact met.
        self.load_second_batch("remote-hotreload-", num_items=100)
        self.assert_replication_moving(timeout=300)

        pid_after = goxdcr_pid(self.src_master)
        self.assertEqual(
            pid_before, pid_after,
            "goxdcr restarted ({0} -> {1}); F14 requires recovery without "
            "one".format(pid_before, pid_after))
