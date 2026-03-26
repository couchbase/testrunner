from remote.remote_util import RemoteMachineShellConnection
from ..cng_utils import CNGHelper
from ..haproxy_utils import HAProxyHelper
from .certs import CertManager
from .registry import CNGClusterInfra

import logger

log = logger.Logger.get_logger()

CNG_CERT_DIR = CertManager.CNG_CERT_DIR
CNG_CERT_PATH = CertManager.CNG_CERT_PATH
CNG_KEY_PATH = CertManager.CNG_KEY_PATH


class InfraManager:
    """Stands up CNG + HAProxy in front of one or more clusters.

    Supplies both single-backend (one CNG on master) and multi-backend
    (one CNG per node) modes through a single code path — callers pass
    multi_backend=True to fan out. All produced infra is held in the
    registry so teardown is symmetric with bootstrap.
    """

    def __init__(self, registry, cert_manager):
        self._registry = registry
        self._certs = cert_manager

    def install_cng_on_node(self, node, key_local, chain_local):
        """Copy cert/key to a node and start a CNGHelper bound to 127.0.0.1."""
        shell = RemoteMachineShellConnection(node)
        try:
            shell.execute_command("mkdir -p {0}".format(CNG_CERT_DIR))
            shell.copy_file_local_to_remote(chain_local, CNG_CERT_PATH)
            shell.copy_file_local_to_remote(key_local, CNG_KEY_PATH)
        finally:
            shell.disconnect()
        helper = CNGHelper(server=node, cert_path=CNG_CERT_PATH,
                           key_path=CNG_KEY_PATH, cb_host="127.0.0.1")
        helper.install_and_build()
        helper.start()
        return helper

    def setup_for_cluster(self, cluster, multi_backend=False):
        """Stand up CNG in front of one cluster.

        multi_backend=True runs CNG on every node and configures HAProxy
        with all nodes as backends; False runs CNG on the master only.
        """
        if cluster.get_name() in self._registry:
            return self._registry.get(cluster.get_name())

        lb_server = self._registry.reserve_lb_server()
        nodes = cluster.get_nodes() if multi_backend \
            else [cluster.get_master_node()]
        node_ips = [n.ip for n in nodes]

        key_local, chain_local = self._certs.generate_shared_cng_cert(
            nodes, lb_server.ip)

        cng_helpers = [self.install_cng_on_node(n, key_local, chain_local)
                       for n in nodes]

        haproxy_helper = HAProxyHelper(server=lb_server)
        haproxy_helper.setup_multi(backend_ips=node_ips)

        infra = CNGClusterInfra(
            cluster=cluster, lb_server=lb_server,
            haproxy_helper=haproxy_helper, cng_helpers=cng_helpers,
            node_ips=list(node_ips))
        self._registry.register(cluster.get_name(), infra)
        log.info("CNG infra ready for {0}: LB={1}, backends={2}".format(
            cluster.get_name(), lb_server.ip, node_ips))
        return infra

    def bootstrap_targets(self, all_clusters, target_clusters,
                          multi_backend=False):
        """Generate certs across all_clusters and stand up CNG for every
        named target cluster."""
        self._certs.setup_for_clusters(all_clusters)
        for cluster in target_clusters:
            self.setup_for_cluster(cluster, multi_backend=multi_backend)

    def cleanup_all(self):
        errors = []
        for name, infra in self._registry.items():
            try:
                infra.haproxy_helper.cleanup()
            except Exception as e:
                errors.append("HAProxy cleanup ({0}) failed: {1}".format(name, e))
                log.warning(errors[-1])
            for helper in infra.cng_helpers:
                try:
                    helper.cleanup()
                except Exception as e:
                    errors.append("CNG cleanup on {0} failed: {1}".format(
                        helper.server.ip, e))
                    log.warning(errors[-1])
                try:
                    shell = RemoteMachineShellConnection(helper.server)
                    try:
                        shell.execute_command("rm -rf {0}".format(CNG_CERT_DIR))
                    finally:
                        shell.disconnect()
                except Exception as e:
                    errors.append("CNG cert cleanup on {0} failed: {1}".format(
                        helper.server.ip, e))
                    log.warning(errors[-1])
            self._registry.return_lb_server(infra.lb_server)
            self._registry.pop(name)
        if errors:
            log.warning(
                "InfraManager.cleanup_all: {0} failure(s) during teardown; "
                "see warnings above".format(len(errors)))
