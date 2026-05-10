import time

from lib.membase.api.exception import XDCRException
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from testconstants import CB_REPO, CB_VERSION_NAME

from .xdcrnewbasetests import XDCRNewBaseTest


class CEEnforcementXDCRTests(XDCRNewBaseTest):
    """[CBQE-8900] XDCR CE enforcement.

    .ini: 2 clusters (C1, C2), both starting at CE. EE->CE downgrade is
    not supported by Couchbase, so once a cluster is upgraded to EE in
    this suite, it stays EE until the lab is re-provisioned.

    Tests are grouped by their state-mutation profile. Suite order matches:
      A. Strict CE  (T4/T5/T8/T9): both clusters stay CE. Pre-flight
         `_require_both_ce` refuses to run if either cluster has been
         upgraded.
      B. Backdoor   (T10 + non-master variant): toggles
         `disable_ce_restrictions` via diag/eval. Flag persists for the
         rest of the suite -- backdoor disable is not reliably
         verifiable in-process.
      C. EE-side    (T1/T3/T2): upgrades a cluster CE->EE in place.
         `_select_ce_ee` returns the (ce, ee) pair regardless of which
         is currently C1 vs C2; `_set_src_dest` rebinds src/dest and
         reorders the framework's chain head. T2 reuses the
         already-upgraded cluster by swapping which side is src.

    After group C, re-provision the lab to reset to all-CE if you want
    to re-run from a clean slate.
    """

    _PKG_NAME = {"EE": "couchbase-server", "CE": "couchbase-server-community"}
    _PKG_FILENAME_PREFIX = {
        "EE": "couchbase-server-enterprise",
        "CE": "couchbase-server-community",
    }

    # Enforcement starts at Totoro (8.1.0). 8.0.2 backport cancelled
    # (MB-70439, Vibhav 2026-03-09).
    ENFORCED_CE_MIN_VERSION = (8, 1, 0)

    # --- Lifecycle -------------------------------------------------------

    def setUp(self):
        super(CEEnforcementXDCRTests, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)

    def tearDown(self):
        super(CEEnforcementXDCRTests, self).tearDown()

    # --- CE -> EE upgrade (one direction only) ----------------------------

    def _ensure_cluster_is_ee(self, cluster):
        """Idempotent CE -> EE upgrade. No-op if already EE."""
        master = cluster.get_master_node()
        master_rest = RestConnection(master)
        if master_rest.is_enterprise_edition():
            self.log.info("[edition] %s already EE -- no-op", master.ip)
            return

        raw_version = master_rest.get_nodes_version() or ""
        version = raw_version.split("-community")[0].split("-enterprise")[0]
        self.assertTrue(
            version,
            "[edition] could not parse version from %s; raw=%r"
            % (master.ip, raw_version))
        # 8.10.x must map to "8.10", not "8.1" (naive [:3] slice bug).
        parts = version.split(".")
        major_minor = ".".join(parts[:2]) if len(parts) >= 2 else ""
        codename = CB_VERSION_NAME.get(major_minor)
        self.assertTrue(
            codename,
            "[edition] no codename for %r (version=%r) in CB_VERSION_NAME"
            % (major_minor, version))
        build_num = version.split("-")[-1]
        self.log.info(
            "[edition] upgrading %s CE -> EE (version=%s codename=%s)",
            master.ip, version, codename)

        for node in cluster.get_nodes():
            self._install_edition_on_node(node, version, codename, build_num)

        self.sleep(30, "[edition] settling after dpkg")

        for node in cluster.get_nodes():
            ok = RestHelper(RestConnection(node)).is_ns_server_running(
                timeout_in_seconds=300)
            self.assertTrue(
                ok, "[edition] %s ns_server not back after EE install" % node.ip)

        # dpkg swap wipes cluster state; reinit + recreate the default bucket.
        cluster.init_cluster()
        bucket_storage = self._input.param("bucket_storage", "couchstore")
        replicas = 1 if len(cluster.get_nodes()) > 1 else 0
        RestConnection(master).create_bucket(
            bucket='default',
            ramQuotaMB=128,
            replicaNumber=replicas,
            bucketType='membase',
            storageBackend=bucket_storage)

        deadline = time.time() + 60
        while time.time() < deadline and not master_rest.is_enterprise_edition():
            time.sleep(3)
        self.assertTrue(
            master_rest.is_enterprise_edition(),
            "[edition] %s REST doesn't report EE after upgrade" % master.ip)
        self.log.info("[edition] %s confirmed EE", master.ip)

    def _install_edition_on_node(self, node, version, codename, build_num):
        self.log.info("[edition] %s: installing EE", node.ip)
        shell = RemoteMachineShellConnection(node)
        try:
            info = shell.extract_remote_info()
            target_prefix = self._PKG_FILENAME_PREFIX["EE"]
            current_pkg = self._PKG_NAME["CE"]
            # The install_cmd chains remove + install; trailing `echo RC=$?`
            # captures the install step's exit code (shell's `;` returns the
            # last command's rc). dpkg -r can legitimately fail if CE isn't
            # registered, we don't care about that; the install rc is what
            # tells us whether EE actually landed.
            if info.deliverable_type == "deb":
                arch = "arm64" if info.architecture_type == "aarch64" else "amd64"
                pkg = "%s_%s-linux_%s.deb" % (target_prefix, version, arch)
                install_cmd = (
                    "DEBIAN_FRONTEND=noninteractive dpkg -r %s 2>&1; "
                    "DEBIAN_FRONTEND=noninteractive dpkg -i /tmp/%s 2>&1; "
                    "echo INSTALL_RC=$?"
                ) % (current_pkg, pkg)
            elif info.deliverable_type == "rpm":
                arch = info.architecture_type
                pkg = "%s-%s-linux.%s.rpm" % (target_prefix, version, arch)
                install_cmd = (
                    "rpm -e %s 2>&1; rpm -Uvh /tmp/%s 2>&1; "
                    "echo INSTALL_RC=$?"
                ) % (current_pkg, pkg)
            else:
                self.fail(
                    "[edition] %s: unsupported deliverable_type=%r"
                    % (node.ip, info.deliverable_type))

            url = "%s%s/%s/%s" % (CB_REPO, codename, build_num, pkg)
            self.log.info("[edition] %s: url=%s", node.ip, url)

            head_out, _ = shell.execute_command(
                "curl -sI -o /dev/null -w '%%{http_code}' '%s'" % url)
            http_code = " ".join(head_out or []).strip()
            self.assertEqual(
                http_code, "200",
                "[edition] %s: EE package not at %s (HTTP %s)"
                % (node.ip, url, http_code))

            shell.execute_command("rm -f /tmp/%s" % pkg)
            shell.execute_command(
                "cd /tmp && curl -sSL -O '%s' && wc -c %s" % (url, pkg))
            ins_out, _ = shell.execute_command(install_cmd)
            output = "\n".join(ins_out or []).rstrip()
            self.log.info(
                "[edition] %s: install stdout:\n%s",
                node.ip, output or "(empty)")
            rc = None
            for line in (ins_out or []):
                if line.strip().startswith("INSTALL_RC="):
                    try:
                        rc = int(line.strip().split("=", 1)[1])
                    except (ValueError, IndexError):
                        pass
            self.assertEqual(
                rc, 0,
                "[edition] %s: install command exited rc=%r (expected 0).\n"
                "Output:\n%s" % (node.ip, rc, output))
            shell.execute_command("rm -f /tmp/%s" % pkg)
        finally:
            shell.disconnect()

    # --- Routing helpers -------------------------------------------------

    def _select_ce_ee(self):
        """Return (ce_cluster, ee_cluster). Upgrades C2 to EE if both
        clusters are still CE. Fails if both are EE (re-provision needed)."""
        c1 = self.get_cb_cluster_by_name('C1')
        c2 = self.get_cb_cluster_by_name('C2')
        c1_ee = RestConnection(c1.get_master_node()).is_enterprise_edition()
        c2_ee = RestConnection(c2.get_master_node()).is_enterprise_edition()
        if c1_ee and c2_ee:
            self.fail(
                "Both C1 (%s) and C2 (%s) are EE; can't downgrade to CE. "
                "Re-provision the lab."
                % (c1.get_master_node().ip, c2.get_master_node().ip))
        if not c1_ee and not c2_ee:
            self.log.info("[route] both clusters CE -- upgrading C2 to EE")
            self._ensure_cluster_is_ee(c2)
            c2_ee = True
        ce = c2 if c1_ee else c1
        ee = c1 if c1_ee else c2
        self.log.info("[route] CE=%s, EE=%s",
                      ce.get_master_node().ip, ee.get_master_node().ip)
        return ce, ee

    def _set_src_dest(self, src, dest):
        """Rebind src/dest convenience attrs and reorder the framework's
        internal cluster list so XDCR's chain topology takes our intended
        src as the chain head. __cb_clusters is name-mangled -- explicit
        attribute access required."""
        self.src_cluster = src
        self.src_master = src.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_cluster = dest
        self.dest_master = dest.get_master_node()
        self.dest_rest = RestConnection(self.dest_master)
        self._XDCRNewBaseTest__cb_clusters = [src, dest]
        self.log.info("[route] src=%s, dest=%s",
                      self.src_master.ip, self.dest_master.ip)

    def _require_both_ce(self):
        """Pre-flight for CE-only tests: refuse to run if either cluster
        has been upgraded. Without this, a CE-only test running after a
        prior EE upgrade would mis-detect enforcement."""
        for cluster, label in ((self.src_cluster, "C1"),
                               (self.dest_cluster, "C2")):
            master = cluster.get_master_node()
            if RestConnection(master).is_enterprise_edition():
                self.fail(
                    "%s on %s is EE; CE-only tests need both clusters CE. "
                    "Re-provision the lab." % (label, master.ip))

    # --- Enforcement / version helpers -----------------------------------

    def _node_has_enforced_ce(self, server):
        rest = RestConnection(server)
        if rest.is_enterprise_edition():
            return False
        version = rest.get_nodes_version() or ""
        parts = version.split("-")[0].split(".")
        try:
            triple = (int(parts[0]), int(parts[1]), int(parts[2]))
        except (IndexError, ValueError):
            self.log.warning(
                "Could not parse version %r on %s; assuming non-enforced.",
                version, server.ip)
            return False
        return triple >= self.ENFORCED_CE_MIN_VERSION

    def _cluster_has_any_enforced_ce_node(self, cluster):
        return any(self._node_has_enforced_ce(n) for n in cluster.get_nodes())

    # --- Backdoor helpers ------------------------------------------------
    # Note: _toggle_ce_backdoor_on_node lives on XDCRNewBaseTest (the base
    # class) and is reused here via inheritance.

    def _toggle_ce_backdoor(self, cluster, enable):
        for node in cluster.get_nodes():
            self._toggle_ce_backdoor_on_node(node, enable)
        self.sleep(15, "wait for goxdcr to be respawned")

    # --- Tests -- Group A: strict CE (no mutations) -----------------------

    def test_ce_to_ce_replication_is_rejected(self):
        """T4: CE -> CE create must fail at REST."""
        self._require_both_ce()
        try:
            self.setup_xdcr()
        except (XDCRException, Exception) as e:
            msg = str(e).lower()
            self.assertIn(
                "community", msg,
                "Replication create failed but error does not mention CE: %s" % e)
            return
        self.fail("CE -> CE replication was accepted; expected CE enforcement rejection.")

    def test_ce_to_ce_via_couchbase_cli_reports_error(self):
        """T5: couchbase-cli xdcr-* fails on CE <-> CE.

        Drives both xdcr-setup and xdcr-replicate. The PRD-implied gate
        may fire at either step. Asserts: (a) at least one of the two
        invocations exited non-zero (rejection), and (b) the combined
        output mentions 'community'.
        """
        self._require_both_ce()
        shell = RemoteMachineShellConnection(self.src_master)
        try:
            setup_cmd = (
                "/opt/couchbase/bin/couchbase-cli xdcr-setup --create "
                "--cluster {src}:{port} -u {u} -p {p} "
                "--xdcr-cluster-name C2 "
                "--xdcr-hostname {dst} "
                "--xdcr-username {du} --xdcr-password {dp}; "
                "echo CLI_RC=$?"
            ).format(
                src=self.src_master.ip, port=self.src_master.port,
                u=self.src_master.rest_username, p=self.src_master.rest_password,
                dst=self.dest_master.ip,
                du=self.dest_master.rest_username,
                dp=self.dest_master.rest_password,
            )
            setup_out, setup_err = shell.execute_command(setup_cmd)
            replicate_cmd = (
                "/opt/couchbase/bin/couchbase-cli xdcr-replicate --create "
                "--cluster {src}:{port} -u {u} -p {p} "
                "--xdcr-cluster-name C2 "
                "--xdcr-from-bucket default --xdcr-to-bucket default; "
                "echo CLI_RC=$?"
            ).format(
                src=self.src_master.ip, port=self.src_master.port,
                u=self.src_master.rest_username, p=self.src_master.rest_password,
            )
            rep_out, rep_err = shell.execute_command(replicate_cmd)
        finally:
            shell.disconnect()

        def _parse_rc(lines):
            for line in lines or []:
                if line.strip().startswith("CLI_RC="):
                    try:
                        return int(line.strip().split("=", 1)[1])
                    except (ValueError, IndexError):
                        return None
            return None

        setup_rc = _parse_rc(setup_out)
        replicate_rc = _parse_rc(rep_out)
        joined = " ".join(setup_out + setup_err + rep_out + rep_err).lower()

        self.assertTrue(
            (setup_rc not in (0, None)) or (replicate_rc not in (0, None)),
            "Neither cli invocation exited non-zero (setup_rc=%r, "
            "replicate_rc=%r). At least one must fail to reject CE->CE. "
            "Combined output: %s" % (setup_rc, replicate_rc, joined))
        self.assertIn(
            "community", joined,
            "cli did not mention CE enforcement (setup_rc=%r, "
            "replicate_rc=%r): %s" % (setup_rc, replicate_rc, joined))

    def test_mixed_source_cluster_enforces_when_any_node_is_enforced_ce(self):
        """T8: any enforced-CE node on src -> cluster enforces CE -> CE rejection.

        Requires the .ini to stage a mix on C1 (>=1 node at enforced-CE >= 8.1.0,
        rest at older CE). Skipped cleanly if no mix is present.
        """
        self._require_both_ce()
        if not self._cluster_has_any_enforced_ce_node(self.src_cluster):
            self.skipTest(
                "C1 has no enforced-CE (>= 8.1.0) node; .ini must stage a "
                "version mix to exercise this test.")
        try:
            self.setup_xdcr()
        except (XDCRException, Exception) as e:
            self.assertIn(
                "community", str(e).lower(),
                "Mixed cluster should enforce per effective-edition rule: %s" % e)
            return
        self.fail("Mixed source cluster accepted CE -> CE replication.")

    def test_mixed_destination_cluster_enforces_when_any_node_is_enforced_ce(self):
        """Mirror of T8 on the destination side.

        The PRD's "any enforced CE node enforces" rule is described for the
        source cluster. This test exercises the same condition with the
        version-mix on the DEST cluster instead: src is plain CE, dest is
        mixed (>= 1 enforced-CE node, rest older CE). Expected outcome is
        the same as T4 (rejection) because src's own enforcement already
        applies; this test documents that mixing on dest doesn't somehow
        bypass it.

        Requires the .ini to stage a mix on C2. Skipped cleanly if no mix.
        """
        self._require_both_ce()
        if not self._cluster_has_any_enforced_ce_node(self.dest_cluster):
            self.skipTest(
                "C2 has no enforced-CE (>= 8.1.0) node; .ini must stage a "
                "version mix on the dest cluster to exercise this test.")
        try:
            self.setup_xdcr()
        except (XDCRException, Exception) as e:
            self.assertIn(
                "community", str(e).lower(),
                "Mixed dest cluster: CE -> CE should still be rejected: %s" % e)
            return
        self.fail("Mixed dest cluster accepted CE -> CE replication.")

    def test_ce_enforcement_error_message_is_actionable(self):
        """T9: error must mention 'community' AND give shape."""
        self._require_both_ce()
        try:
            self.setup_xdcr()
        except (XDCRException, Exception) as e:
            msg = str(e).lower()
            self.assertIn(
                "community", msg,
                "Error does not mention Community Edition: %s" % e)
            self.assertTrue(
                any(t in msg for t in ("cluster", "xdcr", "edition",
                                       "source", "destination", "enterprise")),
                "Error mentions 'community' but no shape token "
                "(cluster/xdcr/edition/source/destination/enterprise): %s" % e)
            return
        self.fail("CE -> CE replication was accepted; no error to validate.")

    # --- Tests -- Group B: backdoor (sets flag, persists) -----------------
    # Note: `disable_ce_restrictions` is a cluster-wide ns_config key.
    # Toggling on any node in a running cluster propagates to every node
    # via ns_config replication. There's no per-node enforcement state,
    # so "partial backdoor" isn't a thing to test -- either the cluster
    # has the flag or it doesn't. T10 covers the "cluster has it" case.

    def test_backdoor_allows_ce_to_ce_replication(self):
        """T10: backdoor on src cluster -> CE -> CE works end-to-end."""
        self._require_both_ce()
        self._toggle_ce_backdoor(self.src_cluster, enable=True)
        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()

    def test_backdoor_persists_across_goxdcr_kill(self):
        """Backdoor flag in ns_config survives goxdcr being killed mid-life.

        Per the design doc, `disable_ce_restrictions=true` lives in ns_config
        (disk-backed) and babysitter passes `--disableCERestrictions` on
        every goxdcr respawn based on that flag. This test does an EXTRA
        kill after the initial toggle to confirm the flag persists in
        ns_config (rather than only being honored on the toggle's
        immediate first respawn):

          1. _toggle_ce_backdoor   -> sets flag + first kill + respawn
          2. extra kill_goxdcr     -> babysitter respawns again
          3. setup_xdcr_and_load   -> if flag persisted, CE->CE works
        """
        self._require_both_ce()
        self._toggle_ce_backdoor(self.src_cluster, enable=True)
        # Extra kill on every src node, without re-setting the flag. If
        # the flag didn't persist, the respawned goxdcr would come up
        # without --disableCERestrictions and the next setup_xdcr would
        # hit the CE->CE rejection.
        for node in self.src_cluster.get_nodes():
            self.log.info("[persist] killing goxdcr on %s (no re-toggle)",
                          node.ip)
            shell = RemoteMachineShellConnection(node)
            try:
                shell.kill_goxdcr()
            finally:
                shell.disconnect()
        self.sleep(15, "wait for babysitter to respawn goxdcr")
        # If the flag persisted, this succeeds; if not, setup_xdcr raises.
        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()

    # --- Tests -- Group C: EE-side (mutates one cluster) ------------------

    def test_ce_to_ee_replication_succeeds(self):
        """T1: CE -> EE. Upgrades C2 if needed via _select_ce_ee."""
        ce, ee = self._select_ce_ee()
        self._set_src_dest(ce, ee)
        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()

    def test_bidirectional_ce_ee_replication_succeeds(self):
        """T3: bidirectional CE <-> EE."""
        ce, ee = self._select_ce_ee()
        self._set_src_dest(ce, ee)
        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()

    def test_ee_to_ce_replication_succeeds(self):
        """T2: EE -> CE. Reuses the EE-upgraded cluster as src; CE side as dest."""
        ce, ee = self._select_ce_ee()
        self._set_src_dest(ee, ce)
        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()
