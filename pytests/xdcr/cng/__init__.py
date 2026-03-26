"""Managers used by CNG XDCR tests.

Each manager owns one concern (certs, infra, refs, replication, diagnostics,
topology) with explicit dependencies passed through its constructor.
CNGXDCRBaseTest wires them together in setUp.
"""

from .registry import CNGInfraRegistry, CNGClusterInfra
from .certs import CertManager
from .infra import InfraManager, CNG_CERT_DIR, CNG_CERT_PATH, CNG_KEY_PATH
from .refs import RemoteRefManager
from .replication import ReplicationManager
from .diagnostics import Diagnostics
from .topology import TopologyBuilder

__all__ = [
    "CNGInfraRegistry", "CNGClusterInfra",
    "CertManager",
    "InfraManager", "CNG_CERT_DIR", "CNG_CERT_PATH", "CNG_KEY_PATH",
    "RemoteRefManager",
    "ReplicationManager",
    "Diagnostics",
    "TopologyBuilder",
]
