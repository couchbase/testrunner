from membase.api.rest_client import RestConnection
from ..haproxy_utils import HAPROXY_FRONTEND_PORT

import logger

log = logger.Logger.get_logger()


class RemoteRefManager:
    """Add / modify / interrogate remote cluster references.

    CNG refs use couchbase2://<lb_ip>:<HAPROXY_FRONTEND_PORT>; standard refs
    use the dest master's cluster_ip:port. Modify operations log XDCR state
    around the flip so the surrounding behaviour can be reconstructed from
    the test log alone.

    Callbacks injected through the constructor:
      get_cluster_certificates_fn:    XDCRNewBaseTest.get_cluster_certificates
      diagnostics:                    cng.Diagnostics instance
      registry:                       CNGInfraRegistry
      active_remote_refs_fn:          XDCRNewBaseTest.active_remote_refs
      change_standard_credentials_fn: XDCRNewBaseTest.change_standard_remote_ref_credentials
    """

    def __init__(self, get_cluster_certificates, diagnostics, registry,
                 active_remote_refs_fn=None,
                 change_standard_credentials_fn=None):
        self._get_certs = get_cluster_certificates
        self._diag = diagnostics
        self._registry = registry
        self._active_refs = active_remote_refs_fn
        self._change_standard_creds = change_standard_credentials_fn

    def add_cng_ref(self, src_cluster, dest_cluster, lb_ip, name):
        """Create a remote cluster ref routing through LB/CNG (couchbase2://)."""
        rest = RestConnection(src_cluster.get_master_node())
        dest_master = dest_cluster.get_master_node()
        certificate = self._get_certs(dest_cluster)
        rest.add_remote_cluster(
            "couchbase2://{0}".format(lb_ip), str(HAPROXY_FRONTEND_PORT),
            dest_master.rest_username, dest_master.rest_password,
            name, demandEncryption=1,
            certificate=certificate, encryptionType="full")

    def add_standard_ref(self, src_cluster, dest_cluster, name):
        """Create a standard (non-CNG) remote cluster ref with full encryption."""
        rest = RestConnection(src_cluster.get_master_node())
        dest_master = dest_cluster.get_master_node()
        certificate = self._get_certs(dest_cluster)
        rest.add_remote_cluster(
            dest_master.cluster_ip, dest_master.port,
            dest_master.rest_username, dest_master.rest_password,
            name, demandEncryption=1,
            certificate=certificate, encryptionType="full")

    def modify_to_cng(self, src_cluster, dest_cluster, lb_ip, name):
        rest = RestConnection(src_cluster.get_master_node())
        dest_master = dest_cluster.get_master_node()
        certificate = self._get_certs(dest_cluster)
        self._diag.log_xdcr_state(rest, src_cluster,
                                  "pre-modify-to-cng:{0}".format(name))
        rest.modify_remote_cluster(
            "couchbase2://{0}".format(lb_ip), str(HAPROXY_FRONTEND_PORT),
            dest_master.rest_username, dest_master.rest_password,
            name, demandEncryption=1,
            certificate=certificate, encryptionType="full")
        log.info("modify_remote_cluster -> CNG returned for '{0}'".format(name))
        self._diag.log_xdcr_state(rest, src_cluster,
                                  "post-modify-to-cng:{0}".format(name))
        self._diag.scan_goxdcr_log(src_cluster,
                                   "post-modify-to-cng:{0}".format(name),
                                   rc_name=name)

    def modify_to_standard(self, src_cluster, dest_cluster, name):
        rest = RestConnection(src_cluster.get_master_node())
        dest_master = dest_cluster.get_master_node()
        certificate = self._get_certs(dest_cluster)
        self._diag.log_xdcr_state(rest, src_cluster,
                                  "pre-modify-to-standard:{0}".format(name))
        rest.modify_remote_cluster(
            dest_master.cluster_ip, dest_master.port,
            dest_master.rest_username, dest_master.rest_password,
            name, demandEncryption=1,
            certificate=certificate, encryptionType="full")
        log.info("modify_remote_cluster -> standard returned for '{0}'".format(name))
        self._diag.log_xdcr_state(rest, src_cluster,
                                  "post-modify-to-standard:{0}".format(name))
        self._diag.scan_goxdcr_log(src_cluster,
                                   "post-modify-to-standard:{0}".format(name),
                                   rc_name=name)

    def add_cng_ref_with_creds(self, src_cluster, dest_cluster, lb_ip, name,
                               username, password):
        """Create a CNG remote ref using explicit credentials (not dest master's)."""
        rest = RestConnection(src_cluster.get_master_node())
        certificate = self._get_certs(dest_cluster)
        rest.add_remote_cluster(
            "couchbase2://{0}".format(lb_ip), str(HAPROXY_FRONTEND_PORT),
            username, password, name,
            demandEncryption=1, certificate=certificate, encryptionType="full")

    def change_credentials(self, src_cluster, dest_cluster, rc_name,
                           username, password, lb_ip=None):
        """Modify remote ref auth credentials. CNG branch (lb_ip given) flips
        through the LB; standard branch delegates to the base helper."""
        if lb_ip:
            rest = RestConnection(src_cluster.get_master_node())
            certificate = self._get_certs(dest_cluster)
            rest.modify_remote_cluster(
                "couchbase2://{0}".format(lb_ip), str(HAPROXY_FRONTEND_PORT),
                username, password, rc_name,
                demandEncryption=1, certificate=certificate, encryptionType="full")
            log.info("Updated credentials for ref '{0}' (username={1})".format(
                rc_name, username))
            return
        self._change_standard_creds(
            src_cluster, dest_cluster, rc_name, username, password)

    def stage_credentials(self, src_rest, rc_name, username, password):
        """Stage secondary credentials on a remote cluster ref."""
        log.info("Staging credentials for ref '{0}': username={1}".format(
            rc_name, username))
        return src_rest.stage_remote_cluster_credentials(rc_name, username, password)

    def active_refs(self, rest):
        """Active (non-deleted) remote cluster refs."""
        return self._active_refs(rest)

    def add_cng_ref_through_registry(self, src_cluster, dest_cluster,
                                     rc_name=None):
        """Add a CNG remote ref using the dest infra's LB, looked up via registry."""
        infra = self._registry.get(dest_cluster.get_name())
        if infra is None:
            raise Exception("No CNG infra for dest cluster '{0}'".format(
                dest_cluster.get_name()))
        name = rc_name or "cng_{0}_to_{1}".format(
            src_cluster.get_name(), dest_cluster.get_name())
        self.add_cng_ref(src_cluster, dest_cluster, infra.lb_server.ip, name)
        return name
