"""
Utility module for managing HAProxy load balancer configuration
on remote VMs for XDCR CNG integration tests.

HAProxy is pre-installed on all test VMs. This module handles
generating configuration, deploying it, and managing the HAProxy
service lifecycle.
"""

import base64
import ipaddress
import time
import logger

from remote.remote_util import RemoteMachineShellConnection

log = logger.Logger.get_logger()

HAPROXY_CFG_PATH = "/etc/haproxy/haproxy.cfg"
HAPROXY_CFG_BACKUP = "/etc/haproxy/haproxy.cfg.testrunner.bak"

# Default CNG listener port (stellar-gateway default)
CNG_PORT = 18098

# Ports to forward through HAProxy for CNG access.
# HAProxy listens on these ports and forwards to the CNG backend.
HAPROXY_FRONTEND_PORT = 18098

PORT_READY_TIMEOUT = 30
PORT_READY_INTERVAL = 2


def _wait_for_port(shell, port, server_ip, timeout=PORT_READY_TIMEOUT,
                   interval=PORT_READY_INTERVAL):
    """Poll until a TCP port is accepting connections on the remote VM.

    Uses `ss` native filter so the check cannot false-match a digit sequence
    appearing in a process name or PID column.

    @param shell: An open RemoteMachineShellConnection.
    @param port: Port number to check.
    @param server_ip: IP of the server (for log messages).
    @param timeout: Max seconds to wait.
    @param interval: Seconds between polls.
    @raises Exception: If port is not ready within timeout.
    """
    end_time = time.time() + timeout
    check_cmd = (
        "ss -Hltn 'sport = :{port}' 2>/dev/null | grep -q LISTEN "
        "&& echo READY || true").format(port=port)
    while time.time() < end_time:
        output, _ = shell.execute_command(check_cmd)
        if output and any("READY" in line for line in output):
            log.info("Port {0} is listening on {1}".format(port, server_ip))
            return
        time.sleep(interval)
    raise Exception(
        "Port {0} not listening on {1} after {2}s".format(
            port, server_ip, timeout))


def generate_haproxy_config(backend_ip, backend_port=CNG_PORT,
                            frontend_port=HAPROXY_FRONTEND_PORT):
    """Generate an HAProxy configuration for forwarding TLS traffic to a CNG
    instance. Uses TCP mode (layer 4) to pass through TLS without terminating.

    @param backend_ip: IP of the CNG (stellar-gateway) instance.
    @param backend_port: Port of the CNG instance.
    @param frontend_port: Port HAProxy listens on.
    @return: HAProxy config string.
    """
    return generate_haproxy_config_multi_backend(
        backend_ips=[backend_ip],
        backend_port=backend_port,
        frontend_port=frontend_port)


def generate_haproxy_config_multi_backend(backend_ips, backend_port=CNG_PORT,
                                          frontend_port=HAPROXY_FRONTEND_PORT):
    """Generate an HAProxy config that round-robins TCP across multiple CNG
    backends. Used when every node of the target cluster runs its own CNG pod.

    @param backend_ips: Non-empty list of CNG backend IPs. Must be unique and
        parseable as IPv4/IPv6 addresses — duplicates would unbalance the
        round-robin; malformed entries fail haproxy's parser with an opaque
        error, so we reject them up front.
    @param backend_port: Port CNG listens on (same for every backend).
    @param frontend_port: Port HAProxy listens on.
    @return: HAProxy config string.
    """
    if not backend_ips:
        raise ValueError("backend_ips must be non-empty")
    if len(set(backend_ips)) != len(backend_ips):
        raise ValueError(
            "backend_ips contains duplicates: {0}".format(backend_ips))
    for ip in backend_ips:
        try:
            ipaddress.ip_address(ip)
        except ValueError as e:
            raise ValueError(
                "backend_ips entry {0!r} is not a valid IP: {1}".format(ip, e))

    server_lines = "\n".join(
        "    server cng{i} {ip}:{p} check".format(i=i + 1, ip=ip, p=backend_port)
        for i, ip in enumerate(backend_ips))

    config = """
global
    log /dev/log local0
    log /dev/log local1 notice
    maxconn 4096
    daemon

defaults
    log     global
    mode    tcp
    option  tcplog
    option  dontlognull
    timeout connect 5000ms
    timeout client  300000ms
    timeout server  300000ms
    retries 3

frontend cng_frontend
    bind *:{frontend_port}
    default_backend cng_backend

backend cng_backend
    balance roundrobin
{servers}
""".format(
        frontend_port=frontend_port,
        servers=server_lines,
    )
    return config.strip() + "\n"


class HAProxyHelper:
    """Manages HAProxy configuration and service lifecycle on remote VMs.

    Supports context-manager usage so cleanup runs on any exception path::

        with HAProxyHelper(lb_server) as lb:
            lb.setup_multi(backend_ips=[...])
            ...
    """

    def __init__(self, server):
        """
        @param server: TestInputServer object for the VM running HAProxy.
        """
        self.server = server

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.cleanup()
        except Exception as e:
            log.warning("HAProxy cleanup on __exit__ failed: {0}".format(e))
        return False

    def deploy_config(self, config_content):
        """Backup existing config, write new HAProxy configuration, and
        validate it on the remote VM.

        Validation is keyed off the shell exit code echoed into stdout —
        relying on tokens alone risks passing when haproxy prints nothing
        on stderr but still fails.

        @param config_content: Full HAProxy config file content as string.
        """
        shell = RemoteMachineShellConnection(self.server)
        try:
            shell.execute_command(
                "cp {0} {1} 2>/dev/null || true".format(
                    HAPROXY_CFG_PATH, HAPROXY_CFG_BACKUP))
            log.info("Backed up HAProxy config on {0}".format(self.server.ip))

            # base64 round-trip avoids shell quoting hazards even if the
            # config contains quotes, backticks, or $-interpolations.
            # Match _login_cmd's pattern: pass the encoded blob unquoted —
            # base64's [A-Za-z0-9+/=] alphabet is shell-safe. Wrapping in
            # extra '' (as a previous version did) only invites breakage if
            # something later tries to substitute a non-base64 string here.
            encoded = base64.b64encode(
                config_content.encode('utf-8')).decode('ascii')
            shell.execute_command(
                "printf %s {0} | base64 -d > {1}".format(
                    encoded, HAPROXY_CFG_PATH))

            output, _ = shell.execute_command(
                "haproxy -c -f {0} >/dev/null 2>&1; "
                "echo HAPROXY_RC=$?".format(HAPROXY_CFG_PATH))
            output_str = " ".join(output) if output else ""
            if "HAPROXY_RC=0" not in output_str:
                detail_out, detail_err = shell.execute_command(
                    "haproxy -c -f {0} 2>&1".format(HAPROXY_CFG_PATH))
                raise Exception(
                    "HAProxy config validation failed on {0} "
                    "(rc line={1!r}): {2}".format(
                        self.server.ip, output_str,
                        (detail_out or []) + (detail_err or [])))

            log.info("HAProxy config deployed on {0}".format(self.server.ip))
        finally:
            shell.disconnect()

    def start(self, frontend_port=HAPROXY_FRONTEND_PORT):
        """Start or restart HAProxy service and verify port readiness.

        Pre-flight clears stale state from a prior aborted run: prior
        systemctl 'failed' status (which blocks restart until cleared),
        zombie haproxy processes, and any lingering bind on the frontend
        port. Without these, a server returned to the floating pool by an
        imperfect tearDown carries forward a stuck haproxy state and the
        very next test's `systemctl restart haproxy` reports active=False.

        @param frontend_port: Port to verify is listening after start.
        """
        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Starting HAProxy on {0}".format(self.server.ip))
            # Best-effort pre-flight. Each command tolerates absence
            # (|| true) so a fresh host with no prior state is OK.
            shell.execute_command(
                "systemctl reset-failed haproxy 2>/dev/null || true; "
                "systemctl stop haproxy 2>/dev/null || true; "
                "pkill -9 -x haproxy 2>/dev/null || true; "
                "fuser -k {0}/tcp 2>/dev/null || true; "
                "rm -f /run/haproxy.pid /var/run/haproxy.pid 2>/dev/null "
                "|| true".format(frontend_port))
            time.sleep(1)

            shell.execute_command("systemctl restart haproxy")

            end_time = time.time() + 15
            while time.time() < end_time:
                output, _ = shell.execute_command("systemctl is-active haproxy")
                if output and output[0].strip() == "active":
                    break
                time.sleep(2)
            else:
                status_out, _ = shell.execute_command(
                    "systemctl status haproxy --no-pager -l 2>&1 | tail -40")
                journal_out, _ = shell.execute_command(
                    "journalctl -u haproxy --no-pager -n 40 2>&1")
                log.error("HAProxy status on {0}: {1}".format(
                    self.server.ip, status_out))
                log.error("HAProxy journal on {0}: {1}".format(
                    self.server.ip, journal_out))
                raise Exception(
                    "HAProxy failed to start on {0}".format(self.server.ip))

            _wait_for_port(shell, frontend_port, self.server.ip)
            log.info("HAProxy started and port {0} ready on {1}".format(
                frontend_port, self.server.ip))
        finally:
            shell.disconnect()

    def stop(self):
        """Stop HAProxy service. Must be called during cleanup to ensure
        future tests are not affected by leftover proxy forwarding."""
        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Stopping HAProxy on {0}".format(self.server.ip))
            shell.execute_command("systemctl stop haproxy")

            end_time = time.time() + 10
            while time.time() < end_time:
                output, _ = shell.execute_command(
                    "systemctl is-active haproxy")
                if output and output[0].strip() != "active":
                    log.info("HAProxy stopped on {0}".format(self.server.ip))
                    return
                time.sleep(2)

            shell.execute_command("pkill -9 haproxy || true")
            time.sleep(1)
            log.info("HAProxy force-stopped on {0}".format(self.server.ip))
        finally:
            shell.disconnect()

    def cleanup(self):
        """Stop HAProxy, free the frontend port, clear any systemd 'failed'
        state, and restore the original configuration. Verifies port 18098
        is no longer bound — if it still is, escalates with pkill+fuser so
        the floating server returns to the pool in a state usable by the
        next test's setup_for_cluster.
        """
        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Cleaning up HAProxy on {0}".format(self.server.ip))
            shell.execute_command("systemctl stop haproxy 2>/dev/null || true")

            end_time = time.time() + 10
            while time.time() < end_time:
                output, _ = shell.execute_command(
                    "systemctl is-active haproxy")
                if output and output[0].strip() != "active":
                    break
                time.sleep(2)
            else:
                shell.execute_command("pkill -9 -x haproxy 2>/dev/null || true")
                time.sleep(1)

            # Belt-and-braces: kill anything still bound on the frontend
            # port and any residual haproxy process by name, then clear
            # systemd's record of any failed start. Each command is
            # best-effort.
            shell.execute_command(
                "fuser -k {0}/tcp 2>/dev/null || true; "
                "pkill -9 -x haproxy 2>/dev/null || true; "
                "systemctl reset-failed haproxy 2>/dev/null || true; "
                "rm -f /run/haproxy.pid /var/run/haproxy.pid 2>/dev/null "
                "|| true".format(HAPROXY_FRONTEND_PORT))

            shell.execute_command(
                "test -f {0} && cp {0} {1} || true".format(
                    HAPROXY_CFG_BACKUP, HAPROXY_CFG_PATH))

            # Final verification: port must be free for the next test.
            check_out, _ = shell.execute_command(
                "ss -Hltn 'sport = :{0}' 2>/dev/null | head -1 "
                "|| true".format(HAPROXY_FRONTEND_PORT))
            if check_out and any((line or "").strip() for line in check_out):
                raise Exception(
                    "HAProxy cleanup on {0}: port {1} still bound after "
                    "stop/kill ({2!r}); refusing to silently return a "
                    "poisoned LB server to the floating pool".format(
                        self.server.ip, HAPROXY_FRONTEND_PORT, check_out))
            log.info("HAProxy cleaned up on {0}".format(self.server.ip))
        finally:
            shell.disconnect()

    def setup(self, backend_ip, backend_port=CNG_PORT,
              frontend_port=HAPROXY_FRONTEND_PORT):
        """Convenience method: generate config, deploy, and start.

        @param backend_ip: IP of the CNG backend server.
        @param backend_port: Port of the CNG backend.
        @param frontend_port: Port HAProxy will listen on.
        """
        self.setup_multi(
            backend_ips=[backend_ip], backend_port=backend_port,
            frontend_port=frontend_port)

    def setup_multi(self, backend_ips, backend_port=CNG_PORT,
                    frontend_port=HAPROXY_FRONTEND_PORT):
        """Generate a multi-backend config, deploy, and start HAProxy.

        @param backend_ips: List of CNG backend IPs to round-robin across.
        """
        config = generate_haproxy_config_multi_backend(
            backend_ips=backend_ips,
            backend_port=backend_port,
            frontend_port=frontend_port)
        self.deploy_config(config)
        self.start(frontend_port=frontend_port)
