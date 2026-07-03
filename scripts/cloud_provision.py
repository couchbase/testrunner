from argparse import ArgumentParser
import base64
import sys, json, subprocess
import boto3
import paramiko
from google.cloud.compute_v1 import ImagesClient, InstancesClient, ZoneOperationsClient
from google.cloud.compute_v1.types.compute import AccessConfig, AttachedDisk, AttachedDiskInitializeParams, BulkInsertInstanceRequest, BulkInsertInstanceResource, DeleteInstanceRequest, GetFromFamilyImageRequest, InstanceProperties, ListInstancesRequest, NetworkInterface, Operation
from google.cloud.dns import Client as DnsClient
import dns.resolver
import time
import io
import logging
import os as OS
from azure.identity import ClientSecretCredential
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Suppress Azure SDK HTTP request/response logging (set to WARNING to hide INFO-level dumps)
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)

"""
  Azure needs to install azure cli 'az' in dispatcher slave
"""


def accept_all_traffic(host, username="root", password="couchbase"):
    commands = ["iptables -I INPUT -j ACCEPT",
                "iptables-save > /etc/sysconfig/iptables"]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,
                username=username,
                password=password)

    for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            if stdout.channel.recv_exit_status() != 0:
                ssh.exec_command("sudo shutdown")
                time.sleep(10)
                ssh.close()
                raise Exception("The iptables could not be added to on {}".format(host))

    ssh.close()


def check_root_login(host, username ="root", password="couchbase"):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host,
                    username=username,
                    password=password)
        return True
    except :
        return False

def increase_suse_linux_default_tasks_max(host, username="root", password="couchbase"):
    '''
    According to MB-58559, the default number of threads in SUSE Linux should be increased
    for analytics to function properly. This function increases the number of threads
    and reload the daemon
    '''
    # Desired number of TasksMax is 65535
    desired_value="65535"
    # Path to the systemd configuration file
    systemd_conf="/etc/systemd/system.conf"
    # Uncomment the line and set the value to 65535
    command_to_change_thread = "sed -i 's/^#\?DefaultTasksMax=.*/DefaultTasksMax='\"{}\"'/' \"{}\"".format(desired_value,
                                                                                                           systemd_conf)
    command_to_reload_daemon = "systemctl daemon-reload"
    commands = [command_to_change_thread, command_to_reload_daemon]

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,
                username=username,
                password=password)

    for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            if stdout.channel.recv_exit_status() != 0:
                ssh.exec_command("sudo shutdown")
                time.sleep(10)
                ssh.close()
                raise Exception("The DefaultTasksMax could not be changed to on {}".format(host))

    ssh.close()

def configure_centos7_vault_repos(ssh):
    """Configure CentOS 7 vault repositories if needed"""
    stdin, stdout, stderr = ssh.exec_command("cat /etc/centos-release || true")
    centos_release = stdout.read().decode().strip()
    if "CentOS Linux release 7" in centos_release:
        log.info("Detected CentOS 7, configuring vault repositories")
        vault_commands = [
            "mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak 2>/dev/null || true",
            """cat > /etc/yum.repos.d/CentOS-Base.repo <<'EOF'
[base]
name=CentOS-7.9 - Base
baseurl=http://vault.centos.org/7.9.2009/os/$basearch/
gpgcheck=1
enabled=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7

[updates]
name=CentOS-7.9 - Updates
baseurl=http://vault.centos.org/7.9.2009/updates/$basearch/
gpgcheck=1
enabled=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7

[extras]
name=CentOS-7.9 - Extras
baseurl=http://vault.centos.org/7.9.2009/extras/$basearch/
gpgcheck=1
enabled=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
EOF""",
            "yum clean all && yum makecache"
        ]
        return vault_commands
    return []

def install_zip_unzip(host, username="root", password="couchbase"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Try password authentication first
    try:
        log.info(f"DEBUG: Attempting password authentication to {host} as {username}")
        ssh.connect(host,
                    username=username,
                    password=password)
        log.info(f"DEBUG: Password authentication successful to {host}")
    except paramiko.ssh_exception.BadAuthenticationType as e:
        log.error(f"DEBUG: Password authentication failed to {host}: {e}")
        log.error(f"DEBUG: Allowed authentication types: {e.allowed_types}")
        ssh.close()
        raise Exception(f"SSH authentication failed to {host}. Password authentication not allowed. Allowed types: {e.allowed_types}. This indicates the post_provisioner SSH configuration failed.")
    except Exception as e:
        log.error(f"DEBUG: SSH connection failed to {host}: {e}")
        ssh.close()
        raise

    commands = []

    stdin, stdout, stderr = ssh.exec_command("which tdnf 2>/dev/null")
    has_tdnf = stdout.channel.recv_exit_status() == 0

    stdin, stdout, stderr = ssh.exec_command("yum --help")
    has_yum = stdout.channel.recv_exit_status() == 0

    if has_tdnf:
        commands.append("tdnf install -y zip unzip")
    elif has_yum:
        commands.extend(configure_centos7_vault_repos(ssh))
        commands.append("yum install -y zip unzip")
    else:
        commands.extend([
            "apt-get update -y",
            "apt-get install -y zip unzip"
        ])

    for command in commands:
        log.info(f"Executing on {host}: {command}")
        stdin, stdout, stderr = ssh.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()

        output = stdout.read().decode().strip()
        error_output = stderr.read().decode().strip()

        log.info(f"Command: {command}")
        log.info(f"Exit status: {exit_status}")

        if exit_status != 0:
            log.error(f"Command: {command}")
            log.error(f"Exit status: {exit_status}")
            log.error(f"Stdout: {output}")
            log.error(f"Stderr: {error_output}")

            ssh.exec_command("sudo shutdown")
            time.sleep(10)
            ssh.close()
            raise Exception("Command failed on {}: {}\nExit status: {}\nOutput: {}\nError: {}".format(
                host, command, exit_status, output, error_output))

    ssh.close()


def diagnose_debian13_networking(host, username="root", password="couchbase"):
    """
    Comprehensive networking diagnostics for Debian 13 to identify issues
    that could cause rebalance failures on AWS but not on server pool VMs
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        log.info(f"=== Starting comprehensive network diagnostics for {host} ===")
        ssh.connect(host,
                    username=username,
                    password=password)

        diagnostic_commands = [
            # 1. NSSwitch Configuration
            ("NSSwitch Config", "cat /etc/nsswitch.conf"),

            # 2. Systemd-resolved status and configuration
            ("Systemd-resolved Status", "systemctl status systemd-resolved --no-pager || echo 'systemd-resolved not running'"),
            ("Systemd-resolved Config", "cat /etc/systemd/resolved.conf 2>/dev/null || echo 'No resolved.conf'"),
            ("Current DNS Settings", "resolvectl status 2>/dev/null || systemd-resolve --status 2>/dev/null || cat /etc/resolv.conf"),

            # 3. Hostname and DNS resolution
            ("Hostname", "hostname"),
            ("FQDN", "hostname -f 2>/dev/null || echo 'FQDN resolution failed'"),
            ("Hostname Resolution", "getent hosts $(hostname)"),
            ("Reverse DNS Check", "dig -x $(hostname -I | awk '{print $1}') +short 2>/dev/null || echo 'Reverse DNS check failed'"),

            # 4. /etc/hosts file
            ("/etc/hosts Content", "cat /etc/hosts"),

            # 5. DNS resolver configuration
            ("/etc/resolv.conf", "cat /etc/resolv.conf"),
            ("DNS Server Test", "nslookup google.com 2>&1 | head -10 || dig google.com +short"),

            # 6. Network interfaces and IPs
            ("Network Interfaces", "ip addr show"),
            ("Routing Table", "ip route show"),
            ("DNS via NetworkManager", "nmcli dev show 2>/dev/null | grep DNS || echo 'NetworkManager not available'"),

            # 7. MTU settings (important for AWS)
            ("MTU Settings", "ip link show | grep -i mtu"),

            # 8. Cloud-init network config (AWS specific)
            ("Cloud-init Network", "cat /etc/netplan/*.yaml 2>/dev/null || cat /etc/network/interfaces 2>/dev/null || echo 'No network config found'"),

            # 9. Check if systemd-resolved is intercepting DNS
            ("Resolved.conf Symlink", "ls -la /etc/resolv.conf"),

            # 10. Test actual DNS resolution
            ("Test Local Hostname Resolution", "ping -c 1 $(hostname) 2>&1 | head -5 || echo 'Cannot ping own hostname'"),

            # 11. Check for any DNS caching services
            ("DNS Cache Services", "ps aux | grep -E 'dnsmasq|nscd|systemd-resolved' | grep -v grep || echo 'No DNS caching services found'"),

            # 12. Network connectivity test
            ("Network Connectivity", "timeout 3 nc -zv 8.8.8.8 53 2>&1 || echo 'DNS port unreachable'"),
        ]

        for label, command in diagnostic_commands:
            log.info(f"\n--- {label} ---")
            stdin, stdout, stderr = ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode().strip()
            error_output = stderr.read().decode().strip()

            if output:
                log.info(f"{output}")
            if error_output and exit_status != 0:
                log.warning(f"Error: {error_output}")

        log.info(f"=== Completed network diagnostics for {host} ===")
        ssh.close()

    except Exception as e:
        log.error(f"Error during diagnostics on {host}: {e}")
        ssh.close()
        raise


def fix_debian13_nsswitch(host, username="root", password="couchbase"):
    """
    Fix DNS resolution on Debian 13 by modifying /etc/nsswitch.conf
    Changes 'hosts: files resolve [NOTFOUND=return] dns' to 'hosts: files dns'
    This bypasses systemd-resolved which can cause DNS issues in cloud environments
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        log.info(f"Checking and fixing nsswitch.conf for Debian 13 on {host}")
        ssh.connect(host,
                    username=username,
                    password=password)

        # Print current configuration
        log.info(f"Reading current /etc/nsswitch.conf on {host}")
        stdin, stdout, stderr = ssh.exec_command("cat /etc/nsswitch.conf")
        current_config = stdout.read().decode().strip()
        log.info(f"Current /etc/nsswitch.conf on {host}:\n{current_config}")

        # Check for various problematic configurations
        stdin, stdout, stderr = ssh.exec_command("grep '^hosts:' /etc/nsswitch.conf")
        hosts_line = stdout.read().decode().strip()
        log.info(f"Current hosts line: {hosts_line}")

        needs_fix = False
        reason = ""

        # Check if using systemd-resolved with NOTFOUND=return
        stdin, stdout, stderr = ssh.exec_command("grep 'hosts:.*resolve.*NOTFOUND.*dns' /etc/nsswitch.conf")
        if stdout.read().decode().strip():
            needs_fix = True
            reason = "Found 'resolve [NOTFOUND=return] dns' configuration"

        # Also check if systemd-resolved is being used at all (might cause issues)
        if not needs_fix:
            stdin, stdout, stderr = ssh.exec_command("grep 'hosts:.*resolve' /etc/nsswitch.conf")
            if stdout.read().decode().strip():
                needs_fix = True
                reason = "Found 'resolve' in hosts line which may cause issues in AWS"

        if needs_fix:
            log.info(f"Fixing nsswitch.conf on {host}: {reason}")

            # Backup the original file
            commands = [
                "sudo cp /etc/nsswitch.conf /etc/nsswitch.conf.backup.$(date +%s)",
                # More comprehensive sed that handles various resolve configurations
                "sudo sed -i 's/^hosts:.*$/hosts:          files dns/' /etc/nsswitch.conf",
                "echo 'Modified /etc/nsswitch.conf - New configuration:' && grep '^hosts:' /etc/nsswitch.conf"
            ]

            for command in commands:
                log.info(f"Executing: {command}")
                stdin, stdout, stderr = ssh.exec_command(command)
                exit_status = stdout.channel.recv_exit_status()
                output = stdout.read().decode().strip()
                error_output = stderr.read().decode().strip()

                if output:
                    log.info(f"Output: {output}")
                if error_output and exit_status != 0:
                    log.warning(f"Error: {error_output}")

                if exit_status != 0 and "grep" not in command:
                    log.error(f"Failed to modify nsswitch.conf on {host}")
                    ssh.close()
                    raise Exception(f"Failed to modify nsswitch.conf on {host}")

            log.info(f"Successfully modified /etc/nsswitch.conf on {host}")

            # Also disable or reconfigure systemd-resolved if it's running
            log.info(f"Checking systemd-resolved status on {host}")
            stdin, stdout, stderr = ssh.exec_command("systemctl is-active systemd-resolved")
            if stdout.read().decode().strip() == "active":
                log.info(f"systemd-resolved is active, configuring it to not interfere with DNS")
                resolved_fix_commands = [
                    # Configure systemd-resolved to use traditional /etc/resolv.conf
                    "sudo mkdir -p /etc/systemd/resolved.conf.d/",
                    "echo -e '[Resolve]\\nDNSStubListener=no' | sudo tee /etc/systemd/resolved.conf.d/nostub.conf",
                    "sudo systemctl restart systemd-resolved",
                    # Replace the symlink with a real file pointing to the right resolver
                    "sudo rm -f /etc/resolv.conf",
                    "sudo ln -sf /run/systemd/resolve/resolv.conf /etc/resolv.conf",
                    "echo 'New /etc/resolv.conf:' && cat /etc/resolv.conf"
                ]
                for cmd in resolved_fix_commands:
                    stdin, stdout, stderr = ssh.exec_command(cmd)
                    exit_status = stdout.channel.recv_exit_status()
                    output = stdout.read().decode().strip()
                    if output:
                        log.info(f"Output: {output}")
        else:
            log.info(f"No problematic nsswitch configuration found on {host} (current: {hosts_line})")

        ssh.close()

    except Exception as e:
        log.error(f"Error fixing nsswitch.conf on {host}: {e}")
        ssh.close()
        raise

def create_non_root_user(host, username="root", password="couchbase"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        log.info(f"DEBUG: create_non_root_user attempting password authentication to {host} as {username}")
        ssh.connect(host,
                    username=username,
                    password=password)
        log.info(f"DEBUG: create_non_root_user password authentication successful to {host}")
    except paramiko.ssh_exception.BadAuthenticationType as e:
        log.error(f"DEBUG: create_non_root_user password authentication failed to {host}: {e}")
        log.error(f"DEBUG: Allowed authentication types: {e.allowed_types}")
        ssh.close()
        raise Exception(f"SSH authentication failed to {host} for nonroot user creation. Password authentication not allowed. This indicates the SSH configuration was not properly applied.")
    except Exception as e:
        log.error(f"DEBUG: create_non_root_user SSH connection failed to {host}: {e}")
        ssh.close()
        raise
    commands = ["useradd -m nonroot",
                "echo -e 'couchbase\ncouchbase' | sudo passwd nonroot",
                "echo \"nonroot       soft  nofile         200000\" >> /etc/security/limits.conf",
                "echo \"nonroot       hard  nofile         200000\" >> /etc/security/limits.conf",
                "echo \"nonroot       soft  nproc          10000\" >> /etc/security/limits.conf",
                "echo \"nonroot       hard  nproc          10000\" >> /etc/security/limits.conf"]

    for command in commands:
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.channel.recv_exit_status() != 0:
            break

    if check_root_login(host):
        log.info("nonroot login to host {} successful.".format(host))
    else:
        log.error("nonroot login to host {} failed. Terminating the EC2 instance".format(host))
        ssh.exec_command("sudo shutdown")
        time.sleep(10)

def install_elastic_search(host, username="root", password="couchbase"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,
                username=username,
                password=password)

    # Configure CentOS 7 vault repos if needed
    commands = configure_centos7_vault_repos(ssh)

    # Enhanced installation commands with better error handling
    commands.extend([
        "yum install -y wget",
        "yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel",
        "cd /tmp && wget -O elasticsearch-1.7.3.tar.gz https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.3.tar.gz",
        "cd /tmp && tar -zxvf elasticsearch-1.7.3.tar.gz",
        "rm -rf /usr/share/elasticsearch && mv /tmp/elasticsearch-1.7.3 /usr/share/elasticsearch",
        "chown -R root:root /usr/share/elasticsearch",
        "chmod +x /usr/share/elasticsearch/bin/elasticsearch"
    ])

    log.info(f"Installing Elasticsearch on {host}")
    for command in commands:
        log.info(f"Executing: {command}")
        stdin, stdout, stderr = ssh.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            output = stdout.read().decode().strip()
            error = stderr.read().decode().strip()
            log.error(f"Command failed with exit status {exit_status}")
            log.error(f"Output: {output}")
            log.error(f"Error: {error}")
            ssh.close()
            raise Exception(f"Failed to install Elasticsearch on {host}: {command}")

    # Start Elasticsearch with proper environment
    log.info(f"Starting Elasticsearch on {host}")
    start_command = """
    export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
    export ES_HEAP_SIZE=512m
    cd /usr/share/elasticsearch
    ./bin/elasticsearch -d
    sleep 5
    """
    stdin, stdout, stderr = ssh.exec_command(start_command)
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        log.error(f"Failed to start Elasticsearch: {output}, {error}")
    else:
        log.info(f"Elasticsearch started successfully on {host}")

    ssh.close()

def restart_elastic_search(ips, username="root", password="couchbase"):
    for ip in ips:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip,
                    username=username,
                    password=password)

        # Check if Elasticsearch is already running
        check_command = "pgrep -f elasticsearch || echo 'not_running'"
        stdin, stdout, stderr = ssh.exec_command(check_command)
        output = stdout.read().decode().strip()

        if "not_running" in output:
            log.info(f"Starting Elasticsearch on {ip}")

            # Kill any zombie processes first
            ssh.exec_command("pkill -f elasticsearch 2>/dev/null || true")
            time.sleep(2)

            # Create a script to start Elasticsearch with proper environment
            start_script = """#!/bin/bash
cd /usr/share/elasticsearch
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
export ES_HEAP_SIZE=512m
./bin/elasticsearch -d
sleep 5
if pgrep -f elasticsearch > /dev/null; then
    echo "Elasticsearch started successfully"
    exit 0
else
    echo "Failed to start Elasticsearch"
    exit 1
fi
"""
            # Write the script to the server
            ssh.exec_command("cat > /tmp/start_es.sh << 'EOF'\n" + start_script + "\nEOF")
            ssh.exec_command("chmod +x /tmp/start_es.sh")

            # Execute the script
            stdin, stdout, stderr = ssh.exec_command("/tmp/start_es.sh")
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode().strip()
            error_output = stderr.read().decode().strip()

            log.info(f"Elasticsearch start output: {output}")
            if error_output:
                log.warning(f"Elasticsearch start errors: {error_output}")

            if exit_status != 0:
                log.error(f"Failed to start Elasticsearch on {ip}. Exit status: {exit_status}")
                log.error(f"Output: {output}")
                log.error(f"Error: {error_output}")

                # Try alternative approach - check if Java is available and try direct start
                stdin, stdout, stderr = ssh.exec_command("java -version 2>&1")
                java_version = stdout.read().decode().strip()
                log.info(f"Java version: {java_version}")

                # Check if elasticsearch directory exists
                stdin, stdout, stderr = ssh.exec_command("ls -la /usr/share/elasticsearch/bin/")
                es_dir = stdout.read().decode().strip()
                log.info(f"Elasticsearch directory: {es_dir}")

                ssh.close()
                raise Exception(f"Unable to start Elasticsearch on {ip}")
        else:
            log.info(f"Elasticsearch is already running on {ip} (PID: {output})")

        ssh.close()

def create_fallback_ssh_config(ssh, host):
    """Create a completely new SSH config as fallback for problematic modern distributions"""
    log.info(f"DEBUG: Creating fallback SSH configuration for {host}")

    # Detect the correct SFTP server path for the distribution
    log.info(f"DEBUG: Detecting SFTP server path on {host}")
    stdin, stdout, stderr = ssh.exec_command("find /usr -name 'sftp-server' 2>/dev/null | head -1")
    sftp_path = stdout.read().decode().strip()

    if not sftp_path:
        # Default paths for common distributions
        stdin, stdout, stderr = ssh.exec_command("cat /etc/os-release 2>/dev/null || echo 'unknown'")
        os_info = stdout.read().decode().strip().lower()

        if "red hat" in os_info or "rhel" in os_info or "centos" in os_info:
            sftp_path = "/usr/libexec/openssh/sftp-server"
        elif "ubuntu" in os_info or "debian" in os_info:
            sftp_path = "/usr/lib/openssh/sftp-server"
        else:
            sftp_path = "/usr/libexec/openssh/sftp-server"  # Default for modern systems

    log.info(f"DEBUG: Using SFTP server path: {sftp_path}")

    # Modern SSH configuration without deprecated directives
    fallback_config = f"""# Couchbase Test SSH Configuration - Modern Fallback
# Basic SSH Settings
Port 22

# Host Keys (modern approach)
HostKey /etc/ssh/ssh_host_rsa_key
HostKey /etc/ssh/ssh_host_ecdsa_key
HostKey /etc/ssh/ssh_host_ed25519_key

# Logging
SyslogFacility AUTH
LogLevel INFO

# Authentication Settings
LoginGraceTime 120
PermitRootLogin yes
StrictModes no
PubkeyAuthentication yes
PasswordAuthentication yes
ChallengeResponseAuthentication no
KbdInteractiveAuthentication no
UsePAM yes

# Connection Settings
MaxAuthTries 6
MaxSessions 10
ClientAliveInterval 300
ClientAliveCountMax 2

# Security Settings
X11Forwarding yes
PrintMotd no
PrintLastLog yes
TCPKeepAlive yes

# Environment
AcceptEnv LANG LC_*

# SFTP Subsystem (critical for file operations)
Subsystem sftp {sftp_path}

# Additional modern security settings
AllowUsers *
PermitTunnel no
GatewayPorts no
"""

    fallback_commands = [
        "sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.backup.$(date +%s)",
        f"echo '{fallback_config}' | sudo tee /etc/ssh/sshd_config",
        "sudo chmod 644 /etc/ssh/sshd_config",
        # Ensure SFTP server package is installed (distribution-specific)
        "if command -v yum &> /dev/null; then sudo yum install -y openssh-sftp-server; elif command -v dnf &> /dev/null; then sudo dnf install -y openssh-sftp-server; elif command -v apt-get &> /dev/null; then sudo apt-get update && sudo apt-get install -y openssh-server; else echo 'No supported package manager found'; fi",
        # Test configuration
        "sudo sshd -t || echo 'Fallback config test failed'",
        # Test SFTP subsystem specifically
        f"test -f {sftp_path} && echo 'SFTP server binary exists' || echo 'ERROR: SFTP server binary missing at {sftp_path}'",
        # Restart SSH with proper sequence for SFTP (distribution-aware)
        "if systemctl list-units --full -all | grep -Fq 'ssh.service'; then SSH_SERVICE='ssh'; else SSH_SERVICE='sshd'; fi",
        "echo \"Using SSH service: $SSH_SERVICE\"",
        "sudo systemctl stop $SSH_SERVICE 2>/dev/null || true",
        "sleep 3",
        "sudo systemctl start $SSH_SERVICE",
        "sleep 5",
        # Verify SFTP is working
        "sudo systemctl status $SSH_SERVICE --no-pager -l | head -10"
    ]

    for cmd in fallback_commands:
        log.info(f"DEBUG: Fallback command: {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            log.error(f"DEBUG: Fallback command failed: {cmd} (exit: {exit_status})")


def post_provisioner(host, username, ssh_key_path, modify_hosts=False):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host,
                    username=username,
                    key_filename=ssh_key_path)

        # Detect OS version for modern distribution handling
        stdin, stdout, stderr = ssh.exec_command("cat /etc/redhat-release 2>/dev/null || cat /etc/os-release 2>/dev/null || echo 'unknown'")
        os_info = stdout.read().decode().strip()
        log.info(f"DEBUG: Detected OS on {host}: {os_info}")

        # Check for modern distributions that may have stricter SSH defaults
        is_rhel9_or_newer = "Red Hat Enterprise Linux release 9" in os_info or "rhel:9" in os_info
        is_rhel10_or_newer = "Red Hat Enterprise Linux release 10" in os_info or "rhel:10" in os_info
        is_debian13_or_newer = "debian" in os_info.lower() and ("13" in os_info or "bookworm" in os_info.lower())
        is_rocky10_or_newer = "rocky" in os_info.lower() and "10" in os_info
        is_modern_distro = is_rhel10_or_newer or is_debian13_or_newer or is_rocky10_or_newer or is_rhel9_or_newer

        log.info(f"DEBUG: Modern distribution detected: {is_modern_distro} (RHEL10+: {is_rhel10_or_newer}, Debian13+: {is_debian13_or_newer}, Rocky10+: {is_rocky10_or_newer})")

        # Check initial SSH configuration and active SSH service
        stdin, stdout, stderr = ssh.exec_command("echo 'INITIAL SSH CONFIG:' && sudo cat /etc/ssh/sshd_config | grep -E '(PermitRootLogin|PasswordAuthentication)' && echo 'SSH SERVICE STATUS:' && sudo systemctl status sshd --no-pager -l | head -10")
        initial_config = stdout.read().decode().strip()
        log.info(f"DEBUG: Initial SSH state on {host}:\n{initial_config}")

        # Base SSH configuration commands
        commands = ["echo -e 'couchbase\ncouchbase' | sudo passwd root",
                    "sudo sed -i '/#PermitRootLogin yes/c\PermitRootLogin yes' /etc/ssh/sshd_config",
                    "sudo sed -i '/PermitRootLogin no/c\PermitRootLogin yes' /etc/ssh/sshd_config",
                    "sudo sed -i '/PermitRootLogin prohibit-password/c\PermitRootLogin yes' /etc/ssh/sshd_config",
                    "sudo sed -i '/PermitRootLogin forced-commands-only/c\#PermitRootLogin forced-commands-only' /etc/ssh/sshd_config",
                    "sudo sed -i '/PasswordAuthentication no/c\PasswordAuthentication yes' /etc/ssh/sshd_config"]

        # Modern distribution specific configuration
        if is_modern_distro:
            commands.extend([
                # First, check what override directories exist
                "echo 'CHECKING SSH OVERRIDE DIRS:' && find /etc/ssh/ /usr/lib/ssh/ /lib/systemd/ -name '*ssh*' -type d 2>/dev/null || true",
                "echo 'CHECKING SSH OVERRIDE FILES:' && find /etc/ssh/ -name '*.conf' -o -name '*.d' 2>/dev/null | head -20 || true",
                # Backup important override files before removing
                "sudo mkdir -p /tmp/ssh_backup && sudo cp -r /etc/ssh/sshd_config.d/ /tmp/ssh_backup/ 2>/dev/null || true",
                # Check for SFTP configuration in override files
                "echo 'CHECKING FOR SFTP CONFIG:' && sudo grep -r 'sftp' /etc/ssh/sshd_config.d/ 2>/dev/null || echo 'No SFTP config found in overrides'",
                # Only remove non-SFTP override files, preserve SFTP configs
                "sudo find /etc/ssh/sshd_config.d/ -name '*.conf' -exec grep -L 'sftp\\|Subsystem' {} \\; | sudo xargs rm -f 2>/dev/null || true",
                # Check for systemd overrides
                "sudo find /etc/systemd /lib/systemd -name '*ssh*' -type f 2>/dev/null | head -10 || true",
                # Detect correct SFTP server path
                "SFTP_PATH=$(find /usr -name 'sftp-server' 2>/dev/null | head -1) && echo \"DETECTED SFTP PATH: $SFTP_PATH\"",
                # Ensure SFTP subsystem is properly configured
                "SFTP_PATH=$(find /usr -name 'sftp-server' 2>/dev/null | head -1) && if [ -n \"$SFTP_PATH\" ]; then echo \"Subsystem sftp $SFTP_PATH\" | sudo tee -a /etc/ssh/sshd_config; fi",
                # Force append critical settings to ensure they override any includes
                "echo '' | sudo tee -a /etc/ssh/sshd_config",
                "echo '# FORCED COUCHBASE TEST CONFIGURATION' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'PermitRootLogin yes' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'PasswordAuthentication yes' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'PubkeyAuthentication yes' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'ChallengeResponseAuthentication no' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'KbdInteractiveAuthentication no' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'UsePAM yes' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'MaxSessions 10' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'ClientAliveInterval 300' | sudo tee -a /etc/ssh/sshd_config"])
        else:
            commands.append("sudo rm -rf /etc/ssh/sshd_config.d/*")

        # Additional modern distribution security handling
        if is_modern_distro:
            commands.extend([
                # Check for and handle SELinux/AppArmor
                "echo 'SECURITY CONTEXT CHECK:' && (getenforce 2>/dev/null || aa-status 2>/dev/null | head -5 || echo 'No SELinux/AppArmor detected')",
                # Handle security frameworks (SELinux for RHEL, AppArmor for Debian)
                "sudo setenforce 0 2>/dev/null || sudo aa-complain /usr/sbin/sshd 2>/dev/null || echo 'Security framework handling attempted'",
                # Install SFTP server if missing (distribution-specific approach)
                "if command -v yum &> /dev/null; then sudo yum install -y openssh-sftp-server; elif command -v dnf &> /dev/null; then sudo dnf install -y openssh-sftp-server; elif command -v apt-get &> /dev/null; then sudo apt-get update && sudo apt-get install -y openssh-server; else echo 'No supported package manager found'; fi",
                # Test SSH config before restart
                "echo 'SSH CONFIG TEST:' && sudo sshd -t && echo 'SSH config is valid' || echo 'WARNING: SSH config test failed'"])

        # Add debugging commands
        commands.extend([
            "echo 'DEBUG: Current SSH config:' && sudo grep -E '(PermitRootLogin|PasswordAuthentication)' /etc/ssh/sshd_config | tail -10",
            "echo 'DEBUG: SSH config dir contents:' && sudo ls -la /etc/ssh/sshd_config.d/ 2>/dev/null || echo 'No sshd_config.d directory'"])

        # MB-68627/DOC-13818 workaround for Debian 13. 
        # Debian 13 has the "hosts:  files myhostname resolve [!UNAVAIL=return] dns" in /etc/nsswitch.conf
        # Entries like 'myhostname' can make the "net" package in golang to force glibc getaddrinfo to lookup DNS instead of Native Go DNS lookup
        # glibc getaddrinfo will result in SIGSEGV from indexer service of Couchbase. So we will just have "hosts: files dns" in /etc/nsswitch.conf
        if is_debian13_or_newer:
            commands.append("sudo sed -i 's/^hosts:.*$/hosts: files dns/' /etc/nsswitch.conf")

        # SSH restart with enhanced error handling for modern distributions
        if is_modern_distro:
            if is_debian13_or_newer:
                # Debian-specific SSH restart sequence
                commands.extend([
                    "echo 'DEBIAN SSH RESTART SEQUENCE...'",
                    "sudo systemctl stop ssh.service 2>/dev/null || sudo systemctl stop sshd.service 2>/dev/null || true",
                    "sleep 3",
                    "echo 'STARTING SSH SERVICE...' && sudo systemctl start ssh.service || sudo systemctl start sshd.service",
                    "sleep 5",
                    "echo 'VERIFYING SSH SERVICE...' && sudo systemctl is-active ssh.service || sudo systemctl is-active sshd.service",
                    "echo 'RELOADING SSH CONFIG...' && sudo systemctl reload ssh.service || sudo systemctl reload sshd.service || true"])
            else:
                # RHEL/CentOS-specific SSH restart sequence
                commands.extend([
                    "echo 'RHEL SSH RESTART SEQUENCE...'",
                    "sudo systemctl stop sshd.service 2>/dev/null || sudo systemctl stop ssh.service 2>/dev/null || true",
                    "sleep 3",
                    "echo 'STARTING SSH SERVICE...' && sudo systemctl start sshd.service || sudo systemctl start ssh.service",
                    "sleep 5",
                    "echo 'VERIFYING SSH SERVICE...' && sudo systemctl is-active sshd.service || sudo systemctl is-active ssh.service",
                    "echo 'RELOADING SSH CONFIG...' && sudo systemctl reload sshd.service || sudo systemctl reload ssh.service || true"])
        else:
            commands.extend([
                "sudo systemctl restart sshd.service",
                "sudo service sshd restart || true",
                "sudo systemctl restart sshd || true",
                "sudo systemctl restart ssh || true"])

        # Post-restart verification and testing
        commands.extend([
            "echo 'DEBUG: SSH service status:' && (sudo systemctl status sshd --no-pager -l || sudo systemctl status ssh --no-pager -l)",
            "echo 'DEBUG: SSH process check:' && sudo ps aux | grep '[s]shd'",
            "echo 'DEBUG: SSH config test:' && sudo sshd -t",
            "echo 'DEBUG: SFTP subsystem check:' && sudo grep -i 'subsystem.*sftp' /etc/ssh/sshd_config || echo 'No SFTP subsystem found'",
            "echo 'DEBUG: SFTP server binary check:' && find /usr -name 'sftp-server' 2>/dev/null || echo 'SFTP server binary not found'",
            "echo 'DEBUG: Final SSH config verification:' && sudo cat /etc/ssh/sshd_config | tail -20",
            # Test SFTP functionality locally
            "echo 'DEBUG: Local SFTP test:' && timeout 10 bash -c 'echo \"ls\" | sftp -o BatchMode=no -o StrictHostKeyChecking=no -o ConnectTimeout=5 root@localhost 2>&1' | head -10 || echo 'Local SFTP test completed'",
            # Test what authentication methods are actually being advertised
            "echo 'DEBUG: SSH auth methods test:' && timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 -o PreferredAuthentications=password root@localhost 'echo test' 2>&1 | head -5 || echo 'Local SSH password test completed'",
            "sudo shutdown -P +800"])

        for command in commands:
            log.info(f"DEBUG: Executing command on {host}: {command}")
            stdin, stdout, stderr = ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode().strip()
            error_output = stderr.read().decode().strip()

            log.info(f"DEBUG: Command exit status: {exit_status}")
            if output:
                log.info(f"DEBUG: Command output: {output}")
            if error_output:
                log.warning(f"DEBUG: Command error: {error_output}")

            if exit_status != 0:
                log.error("The command {} failed and gave a exit staus {}".format(command, exit_status))

        # Wait for SSH to fully restart and stabilize
        log.info(f"DEBUG: Waiting 15 seconds for SSH service to stabilize on {host}")
        time.sleep(15)

        # Perform comprehensive SSH authentication test
        log.info(f"DEBUG: Testing SSH password authentication on {host}")

        # Test with multiple attempts as sometimes it takes a moment for the config to take effect
        auth_success = False
        for attempt in range(3):
            log.info(f"DEBUG: Password authentication attempt {attempt + 1}/3 on {host}")
            if check_root_login(host):
                log.info(f"DEBUG: Password authentication successful on attempt {attempt + 1} for {host}")
                auth_success = True
                break
            else:
                log.warning(f"DEBUG: Password authentication failed on attempt {attempt + 1} for {host}")
                if attempt < 2:  # Don't sleep on the last attempt
                    time.sleep(5)

        if auth_success:
            log.info("root login to host {} successful.".format(host))
        else:
            log.error("root login to host {} failed after 3 attempts.".format(host))

            # Try fallback SSH configuration for modern distributions
            if is_modern_distro:
                log.info(f"DEBUG: Attempting fallback SSH configuration for modern distribution on {host}")
                try:
                    create_fallback_ssh_config(ssh, host)

                    # Wait for fallback config to take effect
                    log.info(f"DEBUG: Waiting 10 seconds for fallback SSH config to take effect on {host}")
                    time.sleep(10)

                    # Test fallback configuration
                    if check_root_login(host):
                        log.info(f"SUCCESS: Fallback SSH configuration worked for {host}")
                        auth_success = True
                    else:
                        log.error(f"FAILED: Fallback SSH configuration also failed for {host}")
                except Exception as e:
                    log.error(f"ERROR: Fallback configuration attempt failed: {e}")

            if not auth_success:
                log.error("Terminating the EC2 instance after all SSH configuration attempts failed")
                # Get comprehensive debugging info before terminating
                log.info(f"GATHERING FINAL DEBUG INFO for {host}...")
                try:
                    stdin, stdout, stderr = ssh.exec_command("echo 'FINAL DEBUG - SSH CONFIG:' && sudo cat /etc/ssh/sshd_config | tail -30 && echo 'FINAL DEBUG - SSH STATUS:' && sudo systemctl status sshd ssh --no-pager -l 2>/dev/null | head -20 && echo 'FINAL DEBUG - SSH PROCESSES:' && sudo ps aux | grep ssh && echo 'FINAL DEBUG - SSH LISTEN PORTS:' && sudo netstat -tlnp | grep ':22'")
                    debug_output = stdout.read().decode().strip()
                    log.info(f"FINAL DEBUG OUTPUT for {host}:\n{debug_output}")
                except Exception as e:
                    log.error(f"Could not gather final debug info: {e}")

                ssh.exec_command("sudo shutdown")
                time.sleep(10)

        if modify_hosts:
            # add hostname to /etc/hosts so node-init-hostname works by binding to 127.0.0.1
            # as recommended in https://docs.couchbase.com/server/current/cloud/couchbase-cloud-deployment.html
            sftp = ssh.open_sftp()
            with io.BytesIO() as fl:
                sftp.getfo('/etc/hosts', fl)
                hosts = fl.getvalue().decode()
                modified_hosts = str()
                for line in hosts.splitlines():
                    ip, hostnames = line.split(maxsplit=1)
                    if ip == "127.0.0.1":
                        hostnames += f" {host}"
                    modified_hosts += f"{ip} {hostnames}\n"
                # need to be root to write to /etc/hosts so write to a new file and move it with sudo
                sftp.putfo(io.BytesIO(modified_hosts.encode()), '/tmp/newhosts')
                ssh.exec_command("sudo mv /tmp/newhosts /etc/hosts")

        ssh.close()


def aws_terminate(name):
    ec2_client = boto3.client('ec2')
    instances = ec2_client.describe_instances(
        Filters=[
            {
                'Name': 'tag:Name',
                'Values': [
                    name
                ]
            }
        ]
    )
    instance_ids = []
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            instance_ids.append(instance['InstanceId'])
    if instance_ids:
        ec2_client.terminate_instances(
            InstanceIds=instance_ids
        )

AWS_AMI_MAP = {
    "couchbase": {
        "amzn2": {
            "aarch64": "ami-0289ff69e0069c2ed",
            "x86_64": "ami-070ac986a212c4d9b"
        },
        "al2023": {
            "aarch64": "ami-08f0305fa4286a9d0",
            "x86_64": "ami-037d1d9dfa436c7c6"
        },
        "al2023nonroot": {
            "aarch64": "ami-08f0305fa4286a9d0",
            "x86_64": "ami-037d1d9dfa436c7c6"
        },
        "ubuntu20": {
            "x86_64": "ami-09168ff916a2e8ed3",
            "aarch64": "ami-0d70a59d7191a8079"
        },
        "ubuntu22": {
            "x86_64": "ami-0e57437fb9199c899",
            "aarch64": "ami-05b98dc6de6e09e97"
        },
        "ubuntu22nonroot": {
            "x86_64": "ami-0e57437fb9199c899",
            "aarch64": "ami-05b98dc6de6e09e97"
        },
        "ubuntu24": {
            "x86_64": "ami-067e3046d94c5f19f",
            "aarch64": "ami-04bc6fa168e83352d"
        },
        "ubuntu24nonroot": {
            "x86_64": "ami-067e3046d94c5f19f",
            "aarch64": "ami-04bc6fa168e83352d"
        },
        "ubuntu26": {
            "x86_64": "ami-091138d0f0d41ff90",
            "aarch64": "ami-07ad186bc37b8dac4"
        },
        "ubuntu26nonroot": {
            "x86_64": "ami-091138d0f0d41ff90",
            "aarch64": "ami-07ad186bc37b8dac4"
        },
        "oel8": {
            "x86_64": "ami-0b5aaeac901e41860"
        },
        "rhel8": {
            "x86_64": "ami-07f5ef252bd61130b"
        },
        "rhel9": {
            "x86_64": "ami-0859d5937ea3b22db"
        },
        "suse15": {
            "x86_64": "ami-059c3ca86322facbe"
        },
        "suse12": {
            "x86_64": "ami-023f4e041769c362b"
        },
        "alma9": {
            "x86_64": "ami-0ec549aa7bb28072e"
        },
        "centos7": {
            "x86_64": "ami-0599a9ff8a4ca809c"
        },
        "rocky9": {
            "x86_64": "ami-0441302605ba7fdb4"
        },
        "oel10": {
            "x86_64": "ami-0059431db83e2427a"
        },
        "rhel10": {
            "x86_64": "ami-014722b0961444108"
        },
        "rhel10nonroot": {
            "x86_64": "ami-014722b0961444108"
        }, 
        "debian13": {
            "x86_64": "ami-041cdfd075e0148d5"
        },
        "debian13nonroot": {
            "x86_64": "ami-041cdfd075e0148d5"
        },
        "rocky10": {
            "x86_64": "ami-09e2e4303c332efc1"
        },
        "alma10": {
            "x86_64": "ami-0eae1bc22be3a6cf9"
        }
    },
    # The following AMIs are faulty and not working
    # TODO - Investigate and create new AMI for the same
    "elastic-fts": "ami-0c48f8b3129e57beb",
    "localstack": "ami-0702052d7d7f58aad"
}

AWS_OS_USERNAME_MAP = {
    "amzn2": "ec2-user",
    "ubuntu20": "ubuntu",
    "centos7": "centos",
    "centos": "centos",
    "al2023": "ec2-user",
    "al2023nonroot": "ec2-user",
    "ubuntu22": "ubuntu",
    "ubuntu22nonroot": "ubuntu",
    "ubuntu24": "ubuntu",
    "ubuntu24nonroot": "ubuntu",
    "ubuntu26": "ubuntu",
    "ubuntu26nonroot": "ubuntu",
    "oel8": "ec2-user",
    "rhel8": "ec2-user",
    "rhel9": "ec2-user",
    "suse15": "ec2-user",
    "suse12": "ec2-user",
    "alma9": "ec2-user",
    "rocky9": "rocky",
    "alma10": "ec2-user",
    "rocky10": "rocky",
    "oel10": "ec2-user",
    "rhel10": "ec2-user",
    "rhel10nonroot": "ec2-user",
    "debian13": "admin",
    "debian13nonroot": "admin"
}

# GPU-enabled instances for tests that need NVIDIA hardware (e.g. FTS GPU vector
# indexing). The AMI is the Amazon-published "Deep Learning Base OSS Nvidia
# Driver GPU AMI (Ubuntu 22.04)" — NVIDIA driver + CUDA pre-installed, Ubuntu
# 22.04 underneath, so post_provisioner / install_zip_unzip work unchanged.
AWS_GPU_AMI_MAP = {
    "ubuntu22": {
        "x86_64": "ami-024dd2172acc1577e"
    }
}
AWS_GPU_INSTANCE_TYPE = "g5.2xlarge"
AWS_GPU_OS_USERNAME_MAP = {
    "ubuntu22": "ubuntu"
}

def _describe_ips_ordered(ec2_client, instance_ids):
    """Return PublicDnsName for each id in the same order as `instance_ids`."""
    if not instance_ids:
        return []
    resp = ec2_client.describe_instances(InstanceIds=instance_ids)
    by_id = {inst['InstanceId']: inst['PublicDnsName']
             for reservation in resp['Reservations']
             for inst in reservation['Instances']}
    return [by_id[i] for i in instance_ids]


def aws_get_servers(name, count, os, type, ssh_key_path, architecture=None, gpu_count=0):
    # Returned IPs are CPU nodes first, GPU nodes last. The dispatcher writes
    # them into the .ini in that order, so this lines up with confs that put
    # GPU-requiring services (e.g. FTS) at the end of cluster=D,D,D,D,F.
    if gpu_count < 0 or gpu_count > count:
        raise ValueError(f"gpu_count ({gpu_count}) must be between 0 and count ({count})")
    if gpu_count > 0 and type != "couchbase":
        raise ValueError("gpu_count is only supported for type='couchbase'")

    cpu_count = count - gpu_count

    instance_type = "t3.xlarge"
    ssh_username = AWS_OS_USERNAME_MAP[os]

    if type != "couchbase":
        image_id = AWS_AMI_MAP[type]
        # TODO: Change to centos x86 AMI (CBQE-7627)
        if type == "localstack":
            instance_type = "t4g.xlarge"
            ssh_username = "ec2-user"
    else:
        image_id = AWS_AMI_MAP["couchbase"][os][architecture]
        if architecture in ["aarch64", "arm64"]:
            instance_type = "t4g.xlarge"

    if type == "elastic-fts":
        '''
        The elastic-fts Instances are created with the following config
        OS - Centos7
        Architecture - x86
        Instance Type - t2.large
        Elastic Search Version - 1.7.3
        '''
        instance_type = "t2.large"
        os = "centos7"
        architecture = "x86_64"
        ssh_username = AWS_OS_USERNAME_MAP[os]
        image_id = AWS_AMI_MAP["couchbase"][os][architecture]

    ec2_resource = boto3.resource('ec2', region_name='us-east-1')
    ec2_client = boto3.client('ec2', region_name='us-east-1')

    base_tags = [
        {'Key': 'Name', 'Value': name},
        {'Key': 'Owner', 'Value': 'ServerRegression'},
    ]
    # GPU instances get an extra Workload=GPU tag so AWS Cost Explorer can
    # break out GPU spend from the rest of the test fleet.
    gpu_tags = base_tags + [{'Key': 'Workload', 'Value': 'GPU'}]

    common_kwargs = dict(
        LaunchTemplate={
            'LaunchTemplateName': 'qe-al-template',
            'Version': '1'
        },
        InstanceInitiatedShutdownBehavior='terminate'
    )

    cpu_instance_ids = []
    if cpu_count > 0:
        cpu_instances = ec2_resource.create_instances(
            ImageId=image_id,
            MinCount=cpu_count,
            MaxCount=cpu_count,
            InstanceType=instance_type,
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': base_tags}],
            **common_kwargs
        )
        cpu_instance_ids = [instance.id for instance in cpu_instances]

    gpu_instance_ids = []
    gpu_ssh_username = None
    if gpu_count > 0:
        gpu_arch = architecture or "x86_64"
        if os not in AWS_GPU_AMI_MAP or gpu_arch not in AWS_GPU_AMI_MAP[os]:
            raise ValueError(f"No GPU AMI registered for os={os}, arch={gpu_arch}")
        gpu_image_id = AWS_GPU_AMI_MAP[os][gpu_arch]
        gpu_ssh_username = AWS_GPU_OS_USERNAME_MAP.get(os, AWS_OS_USERNAME_MAP[os])
        gpu_instances = ec2_resource.create_instances(
            ImageId=gpu_image_id,
            MinCount=gpu_count,
            MaxCount=gpu_count,
            InstanceType=AWS_GPU_INSTANCE_TYPE,
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': gpu_tags}],
            **common_kwargs
        )
        gpu_instance_ids = [instance.id for instance in gpu_instances]

    instance_ids = cpu_instance_ids + gpu_instance_ids
    log.info("Waiting for instances: " + str(instance_ids))
    ec2_client.get_waiter('instance_status_ok').wait(InstanceIds=instance_ids)

    cpu_ips = _describe_ips_ordered(ec2_client, cpu_instance_ids)
    gpu_ips = _describe_ips_ordered(ec2_client, gpu_instance_ids)
    log.info("EC2 CPU Instances : " + str(cpu_ips))
    if gpu_ips:
        log.info("EC2 GPU Instances : " + str(gpu_ips))
    for ip in cpu_ips:
        post_provisioner(ip, ssh_username, ssh_key_path)
        if type == "elastic-fts":
            install_elastic_search(ip)
        if "suse" not in os:
            install_zip_unzip(ip)
        else:
            increase_suse_linux_default_tasks_max(ip)
        if os == "debian13":
            # Run diagnostics first to capture the original state
            diagnose_debian13_networking(ip)
            # Then apply the fix
            fix_debian13_nsswitch(ip)
        if "nonroot" in os:
            create_non_root_user(ip)
            check_root_login(ip,username="nonroot",password="couchbase")

    # GPU nodes run on the Ubuntu 22 NVIDIA DL AMI: same apt-based post-provision
    # as ubuntu22, no SUSE/Debian13/nonroot variants.
    for ip in gpu_ips:
        post_provisioner(ip, gpu_ssh_username, ssh_key_path)
        install_zip_unzip(ip)

    # Rebooting due to CBQE-8153
    # It was observed that all the ports were reachable when the instances were stopped and started
    # It was observed that all the ports were reachable on reboot
    # The temporary fix now is to reboot instances before passing onto tests
    # TODO - Investigate the reason for failures and fix it
    log.info("Rebooting instances : " + str(instance_ids))
    ec2_client.reboot_instances(InstanceIds=instance_ids)
    ec2_client.get_waiter('instance_status_ok').wait(InstanceIds=instance_ids)
    time.sleep(180)

    cpu_ips = _describe_ips_ordered(ec2_client, cpu_instance_ids)
    gpu_ips = _describe_ips_ordered(ec2_client, gpu_instance_ids)
    ips = cpu_ips + gpu_ips

    if type == "elastic-fts":
        restart_elastic_search(ips)
    return ips

ZONE = "us-central1-a"
PROJECT = "couchbase-qe"
DISK_SIZE_GB = 40
SSH_USERNAME = "couchbase"
# Long term we should move the provisioning to a server manager that handles these variables in a config file rather than hardcoding
CLOUD_DNS_DOMAIN_NAME = "couchbase6.com"
CLOUD_DNS_ZONE_NAME = "couchbase6"

def gcp_terminate(name):
    client = InstancesClient()
    try:
        instances = client.list(ListInstancesRequest(project=PROJECT, zone=ZONE, filter=f"labels.name = {name}"))
        for instance in instances:
            client.delete(DeleteInstanceRequest(instance=instance.name, project=PROJECT, zone=ZONE))
        delete_dns_records(instances)
    except Exception:
        return

GCP_TEMPLATE_MAP = {
    "couchbase": {
        "centos7": {
            "x86_64": { "project": "centos-cloud", "family": "centos-7" }
        }
    },
    # TODO
    "elastic-fts": { "project": "couchbase-qe", "image": "" },
    "localstack": { "project": "couchbase-qe", "image": "" }
}

def gcp_wait_for_operation(operation):
    log.info(f"Waiting for {operation} to complete")
    client = ZoneOperationsClient()
    while True:
        result = client.get(project=PROJECT, zone=ZONE, operation=operation)

        if result.status == Operation.Status.DONE:
            if result.error:
                raise Exception(result.error)
            return result

        time.sleep(1)


def hostname_from_ip(ip, for_dns=False):
    hostname = f"gcp-{ip.replace('.', '-')}.{CLOUD_DNS_DOMAIN_NAME}"
    if for_dns:
        hostname += "."
    return hostname


def ip_from_instance(instance):
    return instance.network_interfaces[0].access_configs[0].nat_i_p


def delete_dns_records(instances):
    modify_dns_records(instances, delete=True)


def create_dns_records(instances):
    modify_dns_records(instances, delete=False)


def modify_dns_records(instances, delete):
    zone = DnsClient().zone(CLOUD_DNS_ZONE_NAME, CLOUD_DNS_DOMAIN_NAME)
    changes = zone.changes()
    for instance in instances:
        ip = ip_from_instance(instance)
        hostname = hostname_from_ip(ip, for_dns=True)
        resource_record_set = zone.resource_record_set(
            name=hostname, rrdatas=[ip], record_type="A", ttl=60)
        if delete:
            changes.delete_record_set(resource_record_set)
        else:
            changes.add_record_set(resource_record_set)
    changes.create()


def wait_for_dns_propagation(instances):
    propagated = False
    while not propagated:
        propagated = True
        for instance in instances:
            ip = ip_from_instance(instance)
            hostname = hostname_from_ip(ip)
            try:
                answer = dns.resolver.resolve(hostname)
                propagated &= answer[0].to_text() == ip
            except Exception:
                propagated = False
                time.sleep(1)
                continue


def gcp_get_servers(name, count, os, type, ssh_key_path, architecture):
    machine_type = "e2-standard-4"

    if type != "couchbase":
        image_descriptor = GCP_TEMPLATE_MAP[type]
    else:
        image_descriptor = GCP_TEMPLATE_MAP["couchbase"][os][architecture]

    if "family" in image_descriptor:
        image = ImagesClient().get_from_family(GetFromFamilyImageRequest(project=image_descriptor["project"], family=image_descriptor["family"]))
    else:
        image = ImagesClient().get(project=image_descriptor["project"], image=image_descriptor["image"])

    disk = AttachedDisk(auto_delete=True, boot=True, initialize_params=AttachedDiskInitializeParams(source_image=image.self_link, disk_size_gb=DISK_SIZE_GB))
    network_interface = NetworkInterface(network="global/networks/default", access_configs=[AccessConfig(type_=AccessConfig.Type.ONE_TO_ONE_NAT)])
    instances = BulkInsertInstanceResource(name_pattern=f"{name}-####", count=count, instance_properties=InstanceProperties(labels={ "name": name }, machine_type=machine_type, disks=[disk], network_interfaces=[network_interface]))
    op = InstancesClient().bulk_insert(BulkInsertInstanceRequest(project=PROJECT, zone=ZONE, bulk_insert_instance_resource_resource=instances))

    gcp_wait_for_operation(op.name)

    instances = list(InstancesClient().list(ListInstancesRequest(project=PROJECT, zone=ZONE, filter=f"labels.name = {name}")))

    create_dns_records(instances)

    wait_for_dns_propagation(instances)

    hostnames = [hostname_from_ip(ip_from_instance(instance))
                 for instance in instances]

    for hostname in hostnames:
        post_provisioner(hostname, SSH_USERNAME, ssh_key_path, modify_hosts=True)

    return hostnames

# AZ_TEMPLATE_MAP = {
#     "couchbase"  : "qe-image-20211129",
#     "elastic-fts": "qe-es-image-20211129",
#     "localstack" : "qe-localstack-image-20211129"
# }

AZ_OS_IMAGE_REFERENCE_MAP = {
    "centos7": {
        "publisher": "ntegralinc1586961136942",
        "offer": "ntg_centos_7",
        "sku": "ntg_centos_7_els",
        "version": "latest",
    },
    "mariner2": {
        "publisher": "MicrosoftCBLMariner",
        "offer": "cbl-mariner",
        "sku": "cbl-mariner-2",
        "version": "latest",
    },
    "azurelinux3.0": {
        "publisher": "ntegralinc1586961136942",
        "offer": "ntg_azurelinux_3",
        "sku": "ntg_azurelinux_3_gen2",
        "version": "latest",
    },
}


def az_get_servers(name, count, os, type, ssh_public_key_path, ssh_private_key_path):
    public_hostnames = []
    private_ip_addresses = []

    if type == "elastic-fts":
        '''
        The elastic-fts Instances are created with the following config
        OS - Centos7
        Instance Type - Standard_B4ms
        Elastic Search Version - 1.7.3
        '''
        os = "centos7"

    subscription_id = OS.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group = 'qe-os-certify'
    location = 'West US 2'
    vnet_name = 'os-certify-vnet'
    subnet_name = 'default'
    nsg_name = 'qe-os-certify-nsg'
    credential = ClientSecretCredential(
        tenant_id=OS.environ["AZURE_TENANT_ID"],
        client_id=OS.environ["AZURE_CLIENT_ID"],
        client_secret=OS.environ["AZURE_CLIENT_SECRET"]
    )
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    if os not in AZ_OS_IMAGE_REFERENCE_MAP:
        raise ValueError("Unsupported Azure OS '{}'. Supported: {}".format(
            os, list(AZ_OS_IMAGE_REFERENCE_MAP.keys())))
    image_reference = AZ_OS_IMAGE_REFERENCE_MAP[os]

    vm_username = 'azureuser'
    subnet = network_client.subnets.get(resource_group, vnet_name, subnet_name)
    nsg = network_client.network_security_groups.get(resource_group, nsg_name)

    with open(ssh_public_key_path, 'r') as pub_ssh_file:
        pub_ssh_key = pub_ssh_file.read().strip()

    vm_names = []
    for i in range(count):
        vm_names.append(name + "-" + str(i))

    # Phase 1: Submit all public IP creations in parallel
    log.info("Phase 1: Creating {} public IPs in parallel".format(count))
    ip_pollers = []
    for vm_name in vm_names:
        ip_name = vm_name + '_ip'
        public_ip_addess_params = {
            "location": location,
            "sku": {"name": "Standard"},
            "public_ip_allocation_method": "Static",
            "public_ip_address_version": "IPV4",
            "dns_settings": {
                "domain_name_label": vm_name.lower()
            }
        }
        log.info("Submitting public IP creation for {}".format(vm_name))
        poller = network_client.public_ip_addresses.begin_create_or_update(
            resource_group, ip_name, public_ip_addess_params)
        ip_pollers.append((vm_name, poller))

    public_ips = {}
    for vm_name, poller in ip_pollers:
        public_ips[vm_name] = poller.result()
        log.info("Public IP ready for {}".format(vm_name))

    # Phase 2: Submit all NIC creations in parallel
    log.info("Phase 2: Creating {} NICs in parallel".format(count))
    nic_pollers = []
    for vm_name in vm_names:
        nic_name = vm_name + '_nic'
        nic_params = {
            'location': location,
            'ip_configurations': [{
                'name': 'ip_config',
                'public_ip_address': {
                    'id': public_ips[vm_name].id
                    },
                'subnet': {
                    'id': subnet.id
                }
            }],
            'network_security_group': {
                'id': nsg.id
            }
        }
        log.info("Submitting NIC creation for {}".format(vm_name))
        poller = network_client.network_interfaces.begin_create_or_update(
            resource_group, nic_name, nic_params)
        nic_pollers.append((vm_name, poller))

    nics = {}
    for vm_name, poller in nic_pollers:
        nic_result = poller.result()
        nics[vm_name] = nic_result
        public_hostnames.append(public_ips[vm_name].dns_settings.fqdn)
        private_ip_addresses.append(nic_result.ip_configurations[0].private_ip_address)
        log.info("NIC ready for {}".format(vm_name))

    # cloud-init script injected at VM creation time so PubkeyAcceptedAlgorithms
    # is set before the first SSH connection (post_provisioner) is attempted.
    az_cloud_init = base64.b64encode("""#cloud-config
runcmd:
  - echo 'PubkeyAcceptedAlgorithms +ssh-rsa' >> /etc/ssh/sshd_config
  - systemctl restart sshd 2>/dev/null || systemctl restart ssh
""".encode()).decode()

    # Phase 3: Submit all VM creations in parallel
    log.info("Phase 3: Creating {} VMs in parallel".format(count))
    vm_pollers = []
    for vm_name in vm_names:
        nic_name = vm_name + '_nic'
        vm_params = {
            'location': location,
            'plan': {
                'name': image_reference['sku'],
                'publisher': image_reference['publisher'],
                'product': image_reference['offer']
            },
            'properties': {
                'osProfile': {
                    'computerName': vm_name,
                    'adminUsername': vm_username,
                    'customData': az_cloud_init,
                    'linuxConfiguration': {
                        'disablePasswordAuthentication': True,
                        'ssh': {
                            'publicKeys': [{
                                'path': '/home/{}/.ssh/authorized_keys'.format(vm_username),
                                'keyData': pub_ssh_key
                            }]
                        }
                    }
                },
                'hardwareProfile': {
                    'vmSize': 'Standard_B4ms'
                },
                'storageProfile': {
                    'imageReference': {
                        'publisher': image_reference['publisher'],
                        'offer': image_reference['offer'],
                        'sku': image_reference['sku'],
                        'version': image_reference['version']
                    }
                },
                'networkProfile': {
                    'networkInterfaces': [{
                        'id': network_client.network_interfaces.get(resource_group, nic_name).id,
                    }]
                }
            }
        }
        log.info("Submitting VM creation for {}".format(vm_name))
        poller = compute_client.virtual_machines.begin_create_or_update(
            resource_group, vm_name, vm_params)
        vm_pollers.append((vm_name, poller))

    for vm_name, poller in vm_pollers:
        poller.result()
        log.info("VM ready: {}".format(vm_name))

    for host in public_hostnames:
        print("Provisioning vm {}".format(host))
        post_provisioner(host=host,
                         username="azureuser",
                         ssh_key_path=ssh_private_key_path,
                         modify_hosts=True)
        if type == "elastic-fts":
            install_elastic_search(host)
        accept_all_traffic(host=host)
        install_zip_unzip(host=host)

    if type == "elastic-fts":
        restart_elastic_search(public_hostnames)

    log.info("Azure VMs' hostnames: " + str(public_hostnames))
    log.info("Azure VMs' private IPs: " + str(private_ip_addresses))
    return public_hostnames, private_ip_addresses


def az_terminate(name):
    subscription_id = '4211ea4a-0bc3-4612-8a69-3a38979ac3fa'
    resource_group = 'qe-os-certify'
    credential = ClientSecretCredential(
        tenant_id=OS.environ["AZURE_TENANT_ID"],
        client_id=OS.environ["AZURE_CLIENT_ID"],
        client_secret=OS.environ["AZURE_CLIENT_SECRET"]
    )
    compute_client = ComputeManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)

    vms = compute_client.virtual_machines.list(resource_group)
    matched_vms = [vm.name for vm in vms if vm.name.startswith(name)]

    for vm_name in matched_vms:
        nic_name = vm_name + '_nic'
        ip_name = vm_name + '_ip'

        log.info("delete vm {0}".format(vm_name))
        compute_client.virtual_machines.begin_delete(
            resource_group, vm_name).result()

        log.info("delete network of vm {0}".format(vm_name))
        network_client.network_interfaces.begin_delete(
            resource_group, nic_name).result()

        log.info("delete public ip of vm {0}".format(vm_name))
        network_client.public_ip_addresses.begin_delete(
            resource_group, ip_name).result()


if __name__ == "__main__":
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="provider")

    providers = ["aws", "gcp", "az"]

    for provider in providers:
        provider_parser = subparsers.add_parser(provider)
        provider_subparsers = provider_parser.add_subparsers(dest="cmd")
        provider_terminate_parser = provider_subparsers.add_parser("terminate")
        provider_terminate_parser.add_argument("name", type=str)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if args.provider == "aws":
        if args.cmd == "terminate":
            aws_terminate(args.name)
    elif args.provider == "gcp":
        if args.cmd == "terminate":
            gcp_terminate(args.name)
    elif args.provider == "az":
        if args.cmd == "terminate":
            az_terminate(args.name)
