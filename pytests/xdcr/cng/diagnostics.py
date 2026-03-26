from ..xdcrnewbasetests import NodeHelper

import logger

log = logger.Logger.get_logger()


class Diagnostics:
    """CNG-specific diagnostic surface.

    Most methods are thin delegates to base/NodeHelper helpers — kept here
    so test call-sites continue to read `self._diag.scan_goxdcr_log(...)`
    rather than dispersing into base methods + NodeHelper statics.

    Callbacks injected via the constructor: each receives a small set of
    base-instance methods rather than the whole TestCase, preserving
    unit-test isolation.

      log_xdcr_state_fn:               XDCRNewBaseTest.log_xdcr_state
      log_replications_state_fn:       XDCRNewBaseTest.log_replications_state
      log_remote_clusters_state_fn:    XDCRNewBaseTest.log_remote_clusters_state
      log_per_replication_checkpoints_fn: XDCRNewBaseTest.log_per_replication_checkpoints
      wait_for_new_checkpoint_fn:      XDCRNewBaseTest.wait_for_new_checkpoint
    """

    GOXDCR_SCAN_PATTERNS = [
        "ERRO",
        "cancelXDCR",
        "Unexpected server error",
        "deleted replication spec",
        "Replication spec.*deleted",
        "DelReplicationSpec",
        "Stopping pipeline",
        "pipeline will shutdown",
        "pipeline stopped",
        "RemoteClusterChanged",
        "remote cluster.*changed",
        "metakv.*fail",
        "refresh.*fail",
    ]

    def __init__(self, log_xdcr_state_fn, log_replications_state_fn,
                 log_remote_clusters_state_fn,
                 log_per_replication_checkpoints_fn,
                 wait_for_new_checkpoint_fn):
        self._log_xdcr = log_xdcr_state_fn
        self._log_replications = log_replications_state_fn
        self._log_remote_clusters = log_remote_clusters_state_fn
        self._log_per_repl_ckpts = log_per_replication_checkpoints_fn
        self._wait_for_new_ckpt = wait_for_new_checkpoint_fn

    def wait_for_new_checkpoint(self, rest_src, ckpt_before,
                                poll_interval, timeout):
        return self._wait_for_new_ckpt(
            rest_src, ckpt_before, poll_interval, timeout)

    def log_per_replication_checkpoints(self, rest, label):
        return self._log_per_repl_ckpts(rest, label)

    def log_replications_state(self, rest, label):
        return self._log_replications(rest, label)

    def log_remote_clusters_state(self, rest, label):
        return self._log_remote_clusters(rest, label)

    def log_xdcr_state(self, rest, cluster, label):
        return self._log_xdcr(rest, cluster, label)

    def scan_goxdcr_log(self, cluster, label, extra_patterns=None,
                        rc_name=None, repl_ids=None, tail_lines=10):
        """Scan goxdcr.log on every node using CNG's ref-switch pattern set."""
        return NodeHelper.scan_goxdcr_log(
            cluster, label,
            patterns=self.GOXDCR_SCAN_PATTERNS,
            extra_patterns=extra_patterns,
            rc_name=rc_name, repl_ids=repl_ids, tail_lines=tail_lines)

    def grep_goxdcr_for_pre_replicate(self, node):
        """Recent pre-replicate markers in goxdcr.log. CNG refs must not emit
        these — the XDCR pipeline validates against CNG's failover-log API
        locally instead."""
        _, count = NodeHelper.check_goxdcr_log(
            node, "pre-replicate", print_matches=False)
        return count
