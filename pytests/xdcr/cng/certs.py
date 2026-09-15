import json
import os
import subprocess

from lib.Cb_constants.CBServer import CbServer
from membase.api.rest_client import RestConnection
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
    # CA bundle handed to the gateway as --client-ca-cert. Lives under
    # CNG_CERT_DIR so infra teardown removes it with the rest.
    CNG_CLIENT_CA_PATH = CNG_CERT_DIR + "/client_ca.pem"

    # x509main's default client-cert-auth mapping is
    # paths="subject.cn:san.dnsname:san.uri" with prefixes "www.cb-:us.:www."
    # and delimiter ".". The generated client cert carries
    # SAN DNS=us.cbadminbucket.com, and SAN.DNS always wins when present, so
    # 'us.' is stripped and the value truncated at the first '.' -- the cert
    # authenticates as the built-in 'cbadminbucket' user. Tests must create
    # that user (basetestcase.add_built_in_server_user does, with role admin)
    # on any cluster expected to accept the cert.
    CLIENT_CERT_MAPPED_USER = "cbadminbucket"
    # The intermediate CA whose client cert x509main generates by default.
    DEFAULT_CLIENT_INT_CA = "iclient1_clientroot"

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

    # ------------------------------------------------------------------ #
    # Client-certificate (mTLS) support
    # ------------------------------------------------------------------ #

    def ca_bundle_local_path(self):
        """Local path to the all-roots CA bundle x509main builds.

        generate_multiple_x509_certs() calls create_ca_bundle() itself, so
        this file exists once setup_for_clusters() has run. It holds every
        ROOT CA pem; that is sufficient for the gateway's ClientCAs pool
        because the client presents leaf+intermediate (the 'long_chain'
        file from get_client_cert), and Go builds the intermediate set from
        what the peer sent.
        """
        return CbServer.x509.ALL_CAs_PATH + CbServer.x509.ALL_CAs_PEM_NAME

    def install_client_ca_on_node(self, node):
        """Copy the all-roots CA bundle onto a CNG node.

        @return: remote path to pass as --client-ca-cert.
        """
        local_bundle = self.ca_bundle_local_path()
        shell = RemoteMachineShellConnection(node)
        try:
            shell.execute_command("mkdir -p {0}".format(self.CNG_CERT_DIR))
            shell.copy_file_local_to_remote(
                local_bundle, self.CNG_CLIENT_CA_PATH)
        finally:
            shell.disconnect()
        log.info("Installed client CA bundle on {0} at {1}".format(
            node.ip, self.CNG_CLIENT_CA_PATH))
        return self.CNG_CLIENT_CA_PATH

    def get_client_cert_pems(self, int_ca_name=None):
        """Read the x509 client cert/key back as PEM strings.

        The REST API takes the PEM bodies inline (not paths), so the files
        x509main leaves on the slave are read here rather than shipped.

        @return: (client_cert_pem, client_key_pem)
        """
        int_ca_name = int_ca_name or self.DEFAULT_CLIENT_INT_CA
        cert_path, key_path = CbServer.x509.get_client_cert(
            int_ca_name=int_ca_name)
        with open(cert_path, "r") as handle:
            cert_pem = handle.read()
        with open(key_path, "r") as handle:
            key_pem = handle.read()
        return cert_pem, key_pem

    def enable_client_cert_auth(self, cluster):
        """Turn on cert->user mapping on a cluster and record it on the object.

        Without this the gateway's cbauth CheckCertificate returns
        ErrCertAuthDisabled for every presented certificate, no matter how
        well it verifies at the TLS layer.
        """
        master = cluster.get_master_node()
        CbServer.x509.upload_client_cert_settings(server=master)
        cluster.set_client_cert_auth(True)
        log.info("Enabled client cert auth on {0} ({1})".format(
            cluster.get_name(), master.ip))

    def disable_client_cert_auth(self, cluster, prefixes=None):
        """Turn cert->user mapping off again, and verify the read-back.

        Deliberately posts a NON-EMPTY prefixes list: ns_server has been
        seen to accept `state=disable` with `prefixes: []` (HTTP 200) and
        then fail to push the resulting config to memcached, leaving the
        setting half-applied -- the same trap crlXDCR._disable_client_cert_auth
        documents. Prefixes are moot once the state is 'disable', so sending
        the established ones costs nothing and sidesteps it.

        @return: the state string read back, for the caller to assert on.
        """
        if prefixes is None:
            prefixes = [{"path": "subject.cn", "prefix": "www.cb-",
                         "delimiter": "."}]
        rest = RestConnection(cluster.get_master_node())
        status, content = rest.client_cert_auth("disable", prefixes)
        if not status:
            raise Exception(
                "clientCertAuth disable refused on {0}: {1}".format(
                    cluster.get_name(), content))
        _, verify_content, _ = rest.get_client_cert_auth()
        if isinstance(verify_content, bytes):
            verify_content = verify_content.decode()
        state = json.loads(verify_content).get("state")
        cluster.set_client_cert_auth(state != "disable")
        log.info("clientCertAuth on {0} now reads back as {1!r}".format(
            cluster.get_name(), state))
        return state

    def generate_expired_client_cert(self, node_ip, cert_dir=None):
        """Generate a client cert that is signed by the cluster's own
        intermediate CA but whose validity window is already in the past.

        Signed by the REAL CA on purpose: the gateway trusts the chain, the
        subject/SAN are the ones the cluster maps to a user, and the only
        thing wrong with it is the dates. That isolates expiry from the
        untrusted-chain case, which fails at a different point in the
        handshake -- two tests that would otherwise be the same test.

        @param node_ip: a node of the cluster whose intermediate CA should
            sign the cert (its CA is what the gateway was given).
        @return: (cert_pem, key_pem)
        """
        int_ca_key, int_ca_pem, _ = NodeHelper.get_ca_paths_for_node(node_ip)
        cert_dir = cert_dir or (CbServer.x509.CACERTFILEPATH + "expired/")
        os.makedirs(cert_dir, exist_ok=True)
        key_path = os.path.join(cert_dir, "expired_client.key")
        csr_path = os.path.join(cert_dir, "expired_client.csr")
        cert_path = os.path.join(cert_dir, "expired_client.pem")
        ext_path = os.path.join(cert_dir, "expired_client.ext")

        with open(ext_path, "w") as handle:
            handle.write("basicConstraints=CA:FALSE\n")
            handle.write("extendedKeyUsage=clientAuth\n")
            handle.write("keyUsage=digitalSignature\n")
            handle.write("subjectAltName=DNS:us.cbadminbucket.com\n")

        quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
        subprocess.check_call(
            ["openssl", "genrsa", "-out", key_path, "2048"], **quiet)
        subprocess.check_call(
            ["openssl", "req", "-new", "-key", key_path, "-out", csr_path,
             "-subj", "/C=UA/O=MyCompany/OU=People/CN=clientuser"], **quiet)
        # Explicit past window. -days cannot express "already expired", and
        # openssl 3.x takes absolute timestamps here.
        subprocess.check_call(
            ["openssl", "x509", "-req", "-in", csr_path,
             "-CA", int_ca_pem, "-CAkey", int_ca_key, "-CAcreateserial",
             "-out", cert_path, "-sha256", "-extfile", ext_path,
             "-not_before", "20240101000000Z",
             "-not_after", "20240102000000Z"], **quiet)

        with open(cert_path, "r") as handle:
            cert_pem = handle.read()
        with open(key_path, "r") as handle:
            key_pem = handle.read()
        log.info("Generated EXPIRED client cert (signed by the cluster CA) "
                 "at {0}".format(cert_path))
        return cert_pem, key_pem

    def generate_foreign_client_cert(self, cert_dir=None):
        """Generate a self-signed client cert that chains to NOTHING the
        cluster or the gateway trusts.

        Used by the untrusted-cert test: it is a well-formed, parseable
        cert/key pair (so goxdcr's own X509KeyPair validation accepts it and
        the reference is created), which fails only at the gateway's TLS
        handshake. That is the distinction the test needs -- a malformed pair
        would be rejected by goxdcr up front and never reach the gateway.

        @return: (cert_pem, key_pem, ca_pem) -- for a self-signed cert the
            ca_pem is the cert itself, usable as a --client-ca-cert bundle
            that trusts this cert and nothing else.
        """
        cert_dir = cert_dir or (CbServer.x509.CACERTFILEPATH + "foreign/")
        os.makedirs(cert_dir, exist_ok=True)
        key_path = os.path.join(cert_dir, "foreign_client.key")
        cert_path = os.path.join(cert_dir, "foreign_client.pem")
        subprocess.check_call([
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", key_path, "-out", cert_path,
            "-days", "2", "-nodes",
            "-subj", "/C=UA/O=NotOurCompany/OU=People/CN=foreignuser",
            "-addext", "subjectAltName=DNS:us.cbadminbucket.com",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(cert_path, "r") as handle:
            cert_pem = handle.read()
        with open(key_path, "r") as handle:
            key_pem = handle.read()
        log.info("Generated foreign (untrusted) client cert at {0}".format(
            cert_path))
        return cert_pem, key_pem, cert_pem
