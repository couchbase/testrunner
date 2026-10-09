"""Node-shell backends for dual-run tests.

These are the internal implementations behind ``RunTarget``'s node-shell facade.
``SSHNodeOps`` drives nodes over SSH (on-prem, or Capella when SSH is reachable);
``CapellaNodeOps`` covers the no-SSH Capella case where only control-plane log
collection is available. Tests do not use these directly -- they call
``self.target.<op>`` and ``self.target.supports(...)``.

``RemoteMachineShellConnection`` is imported lazily inside methods to avoid
import cycles at module load time.
"""

# TODO: remove the temporary DUALRUN-FLOW logging once all components have tested the dual-run flow.

from abc import ABC

from lib.testops.capabilities import Capability, UnsupportedOnTarget


class NodeOps(ABC):
    """Interface for node-level operations. Base methods raise ``UnsupportedOnTarget``
    so any backend that does not override an op fails loudly (and, since the error
    is a ``SkipTest``, converts to a clean skip)."""

    CAPS = frozenset()

    def __init__(self, target):
        self.target = target
        self.test = target.test
        self.log = getattr(target.test, "log", None)

    def supports(self, capability):
        return capability in self.CAPS

    def install_packages(self, packages, nodes=None):
        raise UnsupportedOnTarget(Capability.NODE_SHELL)

    def restart_service(self, service, node):
        raise UnsupportedOnTarget(Capability.NODE_SHELL)

    def reboot(self, node):
        raise UnsupportedOnTarget(Capability.NODE_SHELL)

    def read_service_log(self, node, log_path, pattern):
        # Reading a node's log file needs a shell (zgrep over SSH); LOG_ACCESS only
        # covers triggering a log collection.
        raise UnsupportedOnTarget(Capability.NODE_SHELL)

    def collect_logs(self, reason=None):
        raise UnsupportedOnTarget(Capability.LOG_ACCESS)


class SSHNodeOps(NodeOps):
    """Node ops over SSH. Used on-prem, or on Capella when SSH is reachable."""

    CAPS = frozenset({Capability.NODE_SHELL, Capability.LOG_ACCESS})

    def _shell(self, node):
        from lib.remote.remote_util import RemoteMachineShellConnection
        return RemoteMachineShellConnection(node)

    def install_packages(self, packages, nodes=None):
        nodes = nodes if nodes is not None else self.test.servers
        for node in nodes:
            shell = self._shell(node)
            try:
                for pkg in packages:
                    shell.execute_command(
                        command="apt-get install -qq -y %s > /dev/null && echo 1 || echo 0" % pkg,
                        get_pty=True)
            finally:
                shell.disconnect()

    def restart_service(self, service, node):
        shell = self._shell(node)
        try:
            return shell.execute_command("pkill %s" % service)
        finally:
            shell.disconnect()

    def reboot(self, node):
        shell = self._shell(node)
        try:
            return shell.execute_command("reboot")
        finally:
            shell.disconnect()

    def read_service_log(self, node, log_path, pattern):
        shell = self._shell(node)
        try:
            output, _ = shell.execute_command("zgrep '%s' %s" % (pattern, log_path))
            return output
        finally:
            shell.disconnect()

    def collect_logs(self, reason=None):
        return self.test.get_cbcollect_info(self.test.servers)


class CapellaNodeOps(NodeOps):
    """Node ops on Capella with no SSH access: only control-plane log collection."""

    CAPS = frozenset({Capability.LOG_ACCESS})

    def collect_logs(self, reason=None):
        return self.test.capella_api.trigger_log_collection(cluster_id=self.test.cluster_id)


def make_node_ops(target):
    """Pick the node-ops backend for a ``RunTarget``.

    Capella without reachable SSH -> ``CapellaNodeOps`` (log collection only).
    Everything else (on-prem, or Capella with SSH) -> ``SSHNodeOps``.
    """
    _log = getattr(target.test, "log", None)
    if target.capella_run and not target.ssh_reachable:
        if _log:
            _log.info("DUALRUN-FLOW: make_node_ops -> CapellaNodeOps (capella_run, no SSH)")
        return CapellaNodeOps(target)
    if _log:
        _log.info("DUALRUN-FLOW: make_node_ops -> SSHNodeOps")
    return SSHNodeOps(target)
