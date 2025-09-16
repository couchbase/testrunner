from argparse import ArgumentParser
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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)
"""
  Azure needs to install azure cli 'az' in dispatcher slave
"""

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

    stdin, stdout, stderr = ssh.exec_command("yum --help")
    if stdout.channel.recv_exit_status() != 0:
        #Debian or Ubuntu...
        commands.extend([
            "apt-get update -y",
            "apt-get install -y zip unzip"
        ])
    else:
        commands.extend(configure_centos7_vault_repos(ssh))
        commands.append("yum install -y zip unzip")

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
                "echo \"nonroot       hard  nofile         200000\" >> /etc/security/limits.conf"]

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

    fallback_config = """# Couchbase Test SSH Configuration - FALLBACK
Port 22
Protocol 2
HostKey /etc/ssh/ssh_host_rsa_key
HostKey /etc/ssh/ssh_host_ecdsa_key
HostKey /etc/ssh/ssh_host_ed25519_key
UsePrivilegeSeparation yes
KeyRegenerationInterval 3600
ServerKeyBits 1024
SyslogFacility AUTH
LogLevel INFO
LoginGraceTime 120
PermitRootLogin yes
StrictModes no
RSAAuthentication yes
PubkeyAuthentication yes
PasswordAuthentication yes
ChallengeResponseAuthentication no
UsePAM yes
X11Forwarding yes
PrintMotd no
AcceptEnv LANG LC_*
Subsystem sftp /usr/lib/openssh/sftp-server
"""

    fallback_commands = [
        "sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.backup.$(date +%s)",
        f"echo '{fallback_config}' | sudo tee /etc/ssh/sshd_config",
        "sudo chmod 644 /etc/ssh/sshd_config",
        "sudo sshd -t || echo 'Fallback config test failed'",
        "sudo systemctl restart sshd || sudo systemctl restart ssh"
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
        is_rhel10_or_newer = "Red Hat Enterprise Linux release 10" in os_info or "rhel:10" in os_info
        is_debian13_or_newer = "debian" in os_info.lower() and ("13" in os_info or "bookworm" in os_info.lower())
        is_modern_distro = is_rhel10_or_newer or is_debian13_or_newer

        log.info(f"DEBUG: Modern distribution detected: {is_modern_distro} (RHEL10+: {is_rhel10_or_newer}, Debian13+: {is_debian13_or_newer})")

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
                # Remove all potential override directories
                "sudo rm -rf /etc/ssh/sshd_config.d/*",
                "sudo rm -rf /etc/ssh/ssh_config.d/*",
                # Check for systemd overrides
                "sudo find /etc/systemd /lib/systemd -name '*ssh*' -type f 2>/dev/null | head -10 || true",
                # Force append critical settings to ensure they override any includes
                "echo '' | sudo tee -a /etc/ssh/sshd_config",
                "echo '# FORCED COUCHBASE TEST CONFIGURATION' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'PermitRootLogin yes' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'PasswordAuthentication yes' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'PubkeyAuthentication yes' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'ChallengeResponseAuthentication no' | sudo tee -a /etc/ssh/sshd_config",
                "echo 'UsePAM yes' | sudo tee -a /etc/ssh/sshd_config"])
        else:
            commands.append("sudo rm -rf /etc/ssh/sshd_config.d/*")

        # Additional modern distribution security handling
        if is_modern_distro:
            commands.extend([
                # Check for and handle SELinux/AppArmor
                "echo 'SECURITY CONTEXT CHECK:' && (getenforce 2>/dev/null || aa-status 2>/dev/null | head -5 || echo 'No SELinux/AppArmor detected')",
                # Temporarily disable SELinux if present
                "sudo setenforce 0 2>/dev/null || true",
                # Test SSH config before restart
                "echo 'SSH CONFIG TEST:' && sudo sshd -t && echo 'SSH config is valid' || echo 'WARNING: SSH config test failed'"])

        # Add debugging commands
        commands.extend([
            "echo 'DEBUG: Current SSH config:' && sudo grep -E '(PermitRootLogin|PasswordAuthentication)' /etc/ssh/sshd_config | tail -10",
            "echo 'DEBUG: SSH config dir contents:' && sudo ls -la /etc/ssh/sshd_config.d/ 2>/dev/null || echo 'No sshd_config.d directory'"])

        # SSH restart with enhanced error handling for modern distributions
        if is_modern_distro:
            commands.extend([
                # For modern distributions, use a more careful restart approach
                "echo 'STOPPING SSH SERVICE...' && sudo systemctl stop sshd.service ssh.service 2>/dev/null || true",
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
            "echo 'DEBUG: Final SSH config verification:' && sudo cat /etc/ssh/sshd_config | tail -20",
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
        "debian13": {
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
    "ubuntu22": "ubuntu",
    "ubuntu22nonroot": "ubuntu",
    "ubuntu24": "ubuntu",
    "ubuntu24nonroot": "ubuntu",
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
    "debian13": "admin"
}

def aws_get_servers(name, count, os, type, ssh_key_path, architecture=None):
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

    instances = ec2_resource.create_instances(
        ImageId=image_id,
        MinCount=count,
        MaxCount=count,
        InstanceType=instance_type,
        LaunchTemplate={
            'LaunchTemplateName': 'qe-al-template',
            'Version': '1'
        },
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': name
                    },
                    {
                        'Key': 'Owner',
                        'Value': 'ServerRegression'
                    }
                ]
            },
        ],
        InstanceInitiatedShutdownBehavior='terminate'
    )

    instance_ids = [instance.id for instance in instances]
    log.info("Waiting for instances: " + str(instance_ids))
    ec2_client.get_waiter('instance_status_ok').wait(InstanceIds=instance_ids)

    instances = ec2_client.describe_instances(InstanceIds=instance_ids)
    ips = [instance['PublicDnsName'] for instance in instances['Reservations'][0]['Instances']]
    log.info("EC2 Instances : " + str(ips))
    for ip in ips:
        post_provisioner(ip, ssh_username, ssh_key_path)
        if type == "elastic-fts":
            install_elastic_search(ip)
        if "suse" not in os:
            install_zip_unzip(ip)
        else:
            increase_suse_linux_default_tasks_max(ip)
        if "nonroot" in os:
            create_non_root_user(ip)
            check_root_login(ip,username="nonroot",password="couchbase")

    # Rebooting due to CBQE-8153
    # It was observed that all the ports were reachable when the instances were stopped and started
    # It was observed that all the ports were reachable on reboot
    # The temporary fix now is to reboot instances before passing onto tests
    # TODO - Investigate the reason for failures and fix it
    log.info("Rebooting instances : " + str(instance_ids))
    ec2_client.reboot_instances(InstanceIds=instance_ids)
    ec2_client.get_waiter('instance_status_ok').wait(InstanceIds=instance_ids)
    time.sleep(180)

    instances = ec2_client.describe_instances(InstanceIds=instance_ids)
    ips = [instance['PublicDnsName'] for instance in instances['Reservations'][0]['Instances']]

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

AZ_TEMPLATE_MAP = {
    "couchbase"  : "qe-image-20211129",
    "elastic-fts": "qe-es-image-20211129",
    "localstack" : "qe-localstack-image-20211129"
}
def az_get_servers(name, count, os, type, ssh_key_path, architecture):
    vm_type = "Standard_B4ms"
    security_group_name = "qe-test-nsg"
    group_name = "qe-test"
    ips = []
    internal_ips = []

    if type != "couchbase":
        image_name = AZ_TEMPLATE_MAP[type]
    else:
        image_name = AZ_TEMPLATE_MAP["couchbase"]

    """ az vm create -g qe-test -n from-w22 --image 'qe-test-image-20211112-westus2' --nsg qe-test-nsg
           --size Standard_B4ms --output json  --count 2 --public-ip-sku Standard
        image_name = "qe-test-image-20211112-westus2"
    """
    for x in range(1, count + 1):
        cmd = "az vm create -g {0} -n {1}{2} --image {3} --nsg {4} --size {5} --public-ip-sku Standard --output json "\
              .format(group_name, name, x, image_name, security_group_name, vm_type)
        log.info("create vm {0}{1}".format(name, x))
        stdout = subprocess.check_output(cmd, shell=True)
        if isinstance(stdout, bytes):
            # convert to string to load json
            stdout = stdout.decode('utf-8')
        if isinstance(stdout, str):
            stdout = json.loads(stdout)
            ips.append(stdout["publicIpAddress"])
            internal_ips.append(stdout["privateIpAddress"])
    """ no need post_provisioner run on azure """
    log.info("public ips of vms: " + str(ips))
    log.info("private ips of vms: " + str(internal_ips))
    return ips, internal_ips

def az_terminate(name):
    group_name = "qe-test"
    cmd = "az vm list |  grep -o '\"computerName\": \"[^\"]*' | grep -o '[^\"]*$' | grep ^{}".format(name)
    vms = subprocess.check_output(cmd, shell=True).decode('utf-8')
    vms = vms.split("\n")
    vms = [s for s in vms if s]
    for vm_name in vms:
        cmd1 = "az vm delete -g {0} -n {1} -y "\
               .format(group_name, vm_name)
        cmd2 = "az network nic delete -g {0} -n {1}VMNic --no-wait"\
                .format(group_name, vm_name)
        cmd3 = "az network public-ip delete -g {0} -n {1}PublicIP"\
                .format(group_name, vm_name)
        log.info("delete vm {0}".format(name))
        subprocess.check_output(cmd1, shell=True)
        log.info("delete network of vm {0}".format(name))
        subprocess.check_output(cmd2, shell=True)
        log.info("delete public ip of vm {0}".format(name))
        subprocess.check_output(cmd3, shell=True)


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
