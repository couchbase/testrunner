"""Capability vocabulary and the unsupported-operation signal for dual-run tests.

Kept dependency-light (only stdlib) so that low-level modules such as
``lib/couchbase_helper/cluster.py`` can import it without creating import cycles.

A ``Capability`` is a *coarse kind of access*, not one entry per method. Many
operations map to the same capability (``install_packages``, ``restart_service``,
``reboot`` all need ``NODE_SHELL``), so adding operations does not grow this enum.
"""

import unittest
from enum import Enum, auto


class Capability(Enum):
    NODE_SHELL = auto()          # arbitrary commands on a node (install, pkill, reboot, read files)
    TOPOLOGY_CONTROL = auto()    # add / remove / rebalance / scale nodes
    NODE_FAILOVER = auto()       # failover a specific node
    CLUSTER_SETTINGS = auto()    # mutate cluster/index settings via REST
    LOG_ACCESS = auto()          # trigger / collect service logs (cbcollect on-prem, control plane on Capella)
    PRIVATE_ENDPOINT = auto()    # Capella-only concept, no on-prem equivalent
    QUERY = auto()               # run N1QL statements


class UnsupportedOnTarget(unittest.SkipTest):
    """Raised when an operation is not available on the current run target.

    Subclasses ``unittest.SkipTest`` so that raising it anywhere on the main
    thread (a ``RunTarget`` call in the test body, or ``Cluster.async_failover``)
    is recorded by unittest as a skip rather than a failure -- the "backstop" for
    an operation a test did not pre-check with ``RunTarget.supports``.
    """

    def __init__(self, capability):
        self.capability = capability
        name = getattr(capability, "name", capability)
        super().__init__(f"operation unsupported on this target: {name}")
