from lib.Cb_constants.CBServer import CbServer
from remote.remote_util import RemoteMachineShellConnection
from ..xdcrnewbasetests import NodeHelper

import logger

log = logger.Logger.get_logger()


class CertManager:
    """CNG-specific cert generation + thin delegates to NodeHelper for
    generic x509 setup/teardown.

    Generic operations (multi-cluster x509 init, EMFILE-aware teardown,
    floating-server pre-provision) live on NodeHelper as staticmethods and
    are reusable by any XDCR test. CNG-specific cert generation (with LB
    SAN + /opt/cng/certs paths) stays here.
    """

    CNG_CERT_DIR = "/opt/cng/certs"
    CNG_CERT_PATH = CNG_CERT_DIR + "/cng.pem"
    CNG_KEY_PATH = CNG_CERT_DIR + "/cng.key"

    def setup_for_clusters(self, clusters):
        """Generate and upload x509 certificates for all clusters."""
        NodeHelper.setup_x509_certificates_multi_cluster(clusters)

    def provision_pending_floating_servers(self, num_nodes, floating_pool):
        """Pre-provision certs on floating servers awaiting rebalance-in."""
        return NodeHelper.provision_certs_for_floating_servers(
            num_nodes, floating_pool)

    def teardown(self, clusters, log_fd_fn=None):
        """EMFILE-aware x509 teardown."""
        NodeHelper.teardown_x509_certificates_with_retry(
            clusters, max_attempts=4, log_fd_fn=log_fd_fn)

    def generate_cng_certs_on_target(self, target_cluster, lb_ip):
        """Generate TLS certificates for CNG with LB IP in SAN and copy them
        to the target master. Returns remote (cert_path, key_path)."""
        cng_server = target_cluster.get_master_node()
        node_ip = cng_server.ip

        int_ca_key, int_ca_pem, _ = NodeHelper.get_ca_paths_for_node(node_ip)
        cng_local_dir = CbServer.x509.CACERTFILEPATH + "cng/"
        key_local, cert_local, chain_local = NodeHelper.create_cert_with_san(
            cert_dir=cng_local_dir, cert_name="cng",
            san_ips=[node_ip, lb_ip],
            int_ca_key=int_ca_key, int_ca_pem=int_ca_pem,
            cn="cng.gateway.svc")

        shell = RemoteMachineShellConnection(cng_server)
        try:
            shell.execute_command("mkdir -p {0}".format(self.CNG_CERT_DIR))
            shell.copy_file_local_to_remote(chain_local, self.CNG_CERT_PATH)
            shell.copy_file_local_to_remote(key_local, self.CNG_KEY_PATH)
        finally:
            shell.disconnect()
        return self.CNG_CERT_PATH, self.CNG_KEY_PATH

    def generate_shared_cng_cert(self, nodes, lb_ip, cn="cng.gateway.svc"):
        """Generate a single cert whose SAN covers every CNG node IP + LB IP.

        Signed by the intermediate CA of nodes[0], so every cluster trusting
        that CA accepts the cert. Returns local (key, chain) paths.
        """
        primary = nodes[0]
        int_ca_key, int_ca_pem, _ = NodeHelper.get_ca_paths_for_node(primary.ip)
        cng_local_dir = CbServer.x509.CACERTFILEPATH + "cng/"
        san_ips = [n.ip for n in nodes] + [lb_ip]
        key_local, _, chain_local = NodeHelper.create_cert_with_san(
            cert_dir=cng_local_dir, cert_name="cng_{0}".format(primary.ip),
            san_ips=san_ips,
            int_ca_key=int_ca_key, int_ca_pem=int_ca_pem, cn=cn)
        return key_local, chain_local

    def refresh_cert_for_cluster(self, cluster, lb_ip):
        """Regenerate the shared CNG cert for a cluster after node-list change."""
        return self.generate_shared_cng_cert(cluster.get_nodes(), lb_ip)
