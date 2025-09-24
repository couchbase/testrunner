import socket
from optparse import OptionParser
import paramiko
import logging
import sys
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

log = logging.getLogger("fix_failed_install")
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
log.addHandler(ch)


def set_log_level(log_level='info'):
    if log_level and log_level.lower() == 'info':
        log.setLevel(logging.INFO)
    elif log_level and log_level.lower() == 'warning':
        log.setLevel(logging.WARNING)
    elif log_level and log_level.lower() == 'debug':
        log.setLevel(logging.DEBUG)
    elif log_level and log_level.lower() == 'error':
        log.setLevel(logging.ERROR)
    elif log_level and log_level.lower() == 'critical':
        log.setLevel(logging.CRITICAL)
    else:
        log.setLevel(logging.NOTSET)


if len(sys.argv) < 6:
    log.error("Usage: fix_failed_install.py <poolid> <osusername> <osuserpwd> <cbuser> <cbuserpassword>")
    sys.exit(1)

poolId = sys.argv[1]
username = sys.argv[2]
password = sys.argv[3]
cb_username = sys.argv[4]
cb_userpassword = sys.argv[5]

cluster = Cluster('couchbase://172.23.216.60')
authenticator = PasswordAuthenticator(cb_username,cb_userpassword)
cluster.authenticate(authenticator)
cb = cluster.open_bucket('QE-server-pool')

def ssh_exec_cmd(ssh, cmds):
    log.debug('Running commands: {0}'.format(cmds))
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
    out_1 = ssh_stdout.read()
    err_1 = ssh_stderr.read()
    if len(out_1) > 0:
        log.info('STDOUT: {0}'.format(out_1))
    if len(err_1) > 0:
        log.error('STDERR: {0}'.format(err_1))
    return out_1


def uninstall_clean_cb_centos(ssh):
    log.debug("*** Cleaning centos CB ***")
    cmds = 'rpm -qa | grep couchbase-server'
    out_2 = ssh_exec_cmd(ssh, cmds)
    if out_2 != '':
        cmds = 'rpm -e `rpm -qa | grep couchbase-server` && ' \
               'rm -rf /etc/systemd/system/multi-user.target.wants/couchbase-server.service && ' \
               'systemctl daemon-reload && ' \
               'systemctl list-units --type service --all'
        ssh_exec_cmd(ssh, cmds)
    else:
        log.warning("No couchbase-server rpm to delete!")


def uninstall_clean_cb_debian(ssh):
    log.debug("*** Cleaning debian CB ***")
    cmds = 'dpkg -s couchbase-server'
    out_2 = ssh_exec_cmd(ssh, cmds)
    if out_2 != '':
        cmds = "dpkg -P couchbase-server"
        ssh_exec_cmd(ssh, cmds)
        cmds = "rm -rf /var/lib/dpkg/info/couchbase-server.*"
        ssh_exec_cmd(ssh, cmds)
        cmds = "dpkg --configure -a"
        ssh_exec_cmd(ssh, cmds)
        cmds = "apt-get update"
        ssh_exec_cmd(ssh, cmds)
        cmds = "rm -rf /opt/couchbase"
        ssh_exec_cmd(ssh, cmds)
        cmds = 'iptables -F'
        ssh_exec_cmd(ssh, cmds)
        cmds = 'dpkg -s couchbase-server'
        ssh_exec_cmd(ssh, cmds)
        cmds = "ps aux | grep 'couchbase' | grep -v grep | " \
               "awk '{print $2}' | xargs kill -9"
        ssh_exec_cmd(ssh, cmds)
    else:
        log.warning("No couchbase-server dpkg to delete!")


def setpoolstate(pool_id, ip, fromstate, tostate):
    query = "update `QE-server-pool` set state='{0}' where ('{1}' in poolId or poolId='{1}') and " \
            "state like '{2}%' and ipaddr='{3}';".format(str(tostate), str(pool_id),
                                                         str(fromstate), str(ip))
    log.info('Running: {0}'.format(query))
    row_iter = cb.n1ql_query(N1QLQuery(query))
    for row in row_iter:
        log.info(row)


def setipstate(ip, fromstate, tostate):
    query = "update `QE-server-pool` set state='{0}' where state like '{1}%' and ipaddr='{2}';"\
            .format(str(fromstate), str(tostate), str(ip))
    log.debug('Running: {0}'.format(query))
    row_iter = cb.n1ql_query(N1QLQuery(query))
    for row in row_iter:
        log.info(row)


def get_ips_without_libtinfo5(pool_id):
    query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'debian') AND " \
            "(poolId = '{0}' OR '{0}' IN poolId)".format(pool_id)
    if pool_id == 'all':
        query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'debian')"
    log.debug('Running: {0}'.format(query))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    row_iter = cb.n1ql_query(N1QLQuery(query))
    total_ips = len(row_iter)
    hosts_down = []
    no_apt = []
    for row in row_iter:
        try:
            log.debug("-----------------------------------------------------")
            log.info(row)
            ssh.connect(row['ipaddr'], username=username, password=password, timeout=10)
            try:
                stdout = ssh_exec_cmd(ssh, 'apt-get install -y libtinfo5')
                if len(stdout) == 0:
                    no_apt.append(row['ipaddr'])
            except Exception as e:
                log.error("Error installing for IP: {0} => {1}".format(row['ipaddr'], e))
            ssh.close()
        except socket.timeout as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection timed out to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except socket.error as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection Failed to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except Exception as e:
            log.error("Unknown Exception: {0}".format(e))

    log.debug("*****************************************************")
    log.info("Total IPs: {0}".format(total_ips))
    log.info("Installed on IPs {0}".format(total_ips - len(no_apt) - len(hosts_down)))
    if len(no_apt) > 0:
        log.warning("{0} IPs did not have apt-get. => {1}".format(len(no_apt), no_apt))
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs. => {1}".format(len(hosts_down), hosts_down))


def get_ips_without_wget(pool_id):
    query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'ubuntu22' OR os = 'debian') AND " \
            "(poolId = '{0}' OR '{0}' IN poolId)".format(pool_id)
    if pool_id == 'all':
        query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'ubuntu22' OR os = 'debian')"
    log.debug('Running: {0}'.format(query))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    row_iter = cb.n1ql_query(N1QLQuery(query))
    total_ips = len(row_iter)
    hosts_down = []
    no_apt = []
    for row in row_iter:
        try:
            log.debug("-----------------------------------------------------")
            log.info(row)
            ssh.connect(row['ipaddr'], username=username, password=password, timeout=10)
            try:
                stdout = ssh_exec_cmd(ssh, 'apt-get install -y wget')
                if len(stdout) == 0:
                    no_apt.append(row['ipaddr'])
            except Exception as e:
                log.error("Error installing for IP: {0} => {1}".format(row['ipaddr'], e))
            ssh.close()
        except socket.timeout as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection timed out to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except socket.error as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection Failed to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except Exception as e:
            log.error("Unknown Exception: {0}".format(e))

    log.debug("*****************************************************")
    log.info("Total IPs: {0}".format(total_ips))
    log.info("Installed on IPs {0}".format(total_ips - len(no_apt) - len(hosts_down)))
    if len(no_apt) > 0:
        log.warning("{0} IPs did not have apt-get. => {1}".format(len(no_apt), no_apt))
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs. => {1}".format(len(hosts_down), hosts_down))


def get_ips_without_curl(pool_id):
    query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'debian') AND " \
            "(poolId = '{0}' OR '{0}' IN poolId)".format(pool_id)
    if pool_id == 'all':
        query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'debian')"
    log.debug('Running: {0}'.format(query))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    row_iter = cb.n1ql_query(N1QLQuery(query))
    total_ips = len(row_iter)
    hosts_down = []
    no_curl = []
    no_apt = []
    for row in row_iter:
        try:
            log.debug("-----------------------------------------------------")
            log.info(row)
            # res = os.system("ping -c 1 " + row['ipaddr'])
            # if res != 0:
            #     hosts_down.append(row['ipaddr'])
            #     log.error("Could not connect to host {0}".format(row['ipaddr']))
            #     continue
            ssh.connect(row['ipaddr'], username=username, password=password, timeout=10)
            check_for_curl = 'curl -V'
            stdout = ssh_exec_cmd(ssh, check_for_curl)
            if len(stdout) == 0:
                no_curl.append(row['ipaddr'])
                try:
                    stdout_2 = ssh_exec_cmd(ssh, 'apt-get --assume-yes install curl')
                    if len(stdout_2) == 0:
                        no_apt.append(row['ipaddr'])
                    else:
                        log.info("Curl Installed on {0}".format(row['ipaddr']))
                except Exception as e:
                    log.error("Error installing curl for IP: {0} => {1}".format(row['ipaddr'], e))
            ssh.close()
        except socket.timeout as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection timed out to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except socket.error as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection Failed to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except Exception as e:
            log.error("Unknown Exception: {0}".format(e))

    log.debug("*****************************************************")
    log.info("Total IPs: {0}".format(total_ips))
    if len(no_curl) > 0:
        log.debug("{0} IPs did not have curl.".format(len(no_curl)))
        log.info("Curl installed on all accessible IPs successfully")
    else:
        log.info("All hosts had Curl!")
    if len(no_apt) > 0:
        log.warning("{0} IPs did not have apt-get. => {1}".format(len(no_apt), no_apt))
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs. => {1}".format(len(hosts_down), hosts_down))


def uninstall_all(ips):
    all_ips = ips.split(',')
    for ip in all_ips:
        setipstate(ip, "available", "booked")
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username=username, password=password, timeout=10)
            uninstall_clean_cb_centos(ssh)
            out = ssh_exec_cmd(ssh, "rpm -qa | grep couchbase-server")
            if out == '':
                setipstate(ip, "booked", "available")
        except Exception as e:
            log.error(e)
            pass


def install_ntp_on_deb(pool_id):
    query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'ubuntu22' OR os = 'debian') AND " \
            "('{0}' IN poolId OR poolId = '{0}')".format(str(pool_id))
    if pool_id == 'all':
        query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'ubuntu22' OR os = 'debian')"
    log.info('Running: {0}'.format(query))
    row_iter = cb.n1ql_query(N1QLQuery(query))
    total_ips = len(row_iter)
    hosts_down = []
    no_apt = []
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for row in row_iter:
        try:
            log.debug("-----------------------------------------------------")
            log.info(row)
            ssh.connect(row['ipaddr'], username=username, password=password, timeout=10)
            try:
                stdout = ssh_exec_cmd(ssh, 'apt-get install -y ntp')
                if len(stdout) == 0:
                    no_apt.append(row['ipaddr'])
                else:
                    ssh_exec_cmd(ssh, 'service ntp restart')
            except Exception as e:
                log.error("Error installing for IP: {0} => {1}".format(row['ipaddr'], e))
            ssh.close()
        except socket.timeout as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection timed out to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except socket.error as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection Failed to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except Exception as e:
            log.error("Unknown Exception: {0}".format(e))

    log.debug("*****************************************************")
    log.info("Total IPs: {0}".format(total_ips))
    log.info("Installed on IPs {0}".format(total_ips - len(no_apt) - len(hosts_down)))
    if len(no_apt) > 0:
        log.warning("{0} IPs did not have apt-get. => {1}".format(len(no_apt), no_apt))
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs => {1}".format(len(hosts_down), hosts_down))


def install_ntp_on_cent(pool_id):
    query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'rhel9' OR os = 'oel9') " \
            "AND ('{0}' IN poolId OR poolId = '{0}')".format(str(pool_id))
    if pool_id == 'all':
        query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE (os = 'rhel9' OR os = 'oel9')"
    log.debug('Running: {0}'.format(query))
    row_iter = cb.n1ql_query(N1QLQuery(query))
    total_ips = len(row_iter)
    hosts_down = []
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for row in row_iter:
        server = row['ipaddr']
        try:
            log.debug("-----------------------------------------------------")
            log.info(row)
            ssh.connect(server, username=username, password=password, timeout=10)
            cmds = 'yum install -y ntpstat && ntpstat'
            ssh_exec_cmd(ssh, cmds)
            ssh.close()
        except socket.timeout as e:
            hosts_down.append(server)
            log.error("Connection timed out to IP: {0}, Exception: {1}".format(server, e))
        except socket.error as e:
            hosts_down.append(server)
            log.error("Connection Failed to IP: {0}, Exception: {1}".format(server, e))
        except Exception as e:
            log.error("Unknown Exception: {0}".format(e))

    log.debug("*****************************************************")
    log.info("Total IPs: {0}".format(total_ips))
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs => {1}".format(len(hosts_down), hosts_down))


def clean_failedinstsall_vms(pool_id):
    log.debug("*** Cleaning failedInstall VMs ***")
    query = 'SELECT * FROM `QE-server-pool` WHERE ("{0}" IN poolId OR poolId = "{0}") AND ' \
            'state LIKE "failedInstall%%";'.format(str(pool_id))
    log.info('Running: {0}'.format(query))
    row_iter = cb.n1ql_query(N1QLQuery(query))
    fixed_ips = []
    hosts_down = []
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for row in row_iter:
        log.debug("-----------------------------------------------------")
        server = row['QE-server-pool']['ipaddr']
        host_os = row['QE-server-pool']['os']
        try:
            log.info('Server: {0}'.format(server))
            ssh.connect(server, username=username, password=password, timeout=10)
            out_2 = ''
            if host_os == 'centos':
                uninstall_clean_cb_centos(ssh)
                cmds = 'rpm -qa | grep couchbase-server'
                out_2 = ssh_exec_cmd(ssh, cmds)
                cmds = 'yum -y install yum-utils'
                ssh_exec_cmd(ssh, cmds)
                cmds = 'yum-complete-transaction --cleanup-only; yum clean all'
                ssh_exec_cmd(ssh, cmds)
            elif host_os == 'debian':
                uninstall_clean_cb_debian(ssh)
                cmds = 'dpkg -s couchbase-server'
                out_2 = ssh_exec_cmd(ssh, cmds)
                cmds = 'apt-get install -y ntp && service ntp restart'
                ssh_exec_cmd(ssh, cmds)
            cmds = 'iptables -F'
            ssh_exec_cmd(ssh, cmds)
            # cmds = 'mkdir /var/lib/rpm/backup | cp -a /var/lib/rpm/__db* /var/lib/rpm/backup/ |
            # rm
            # -f /var/lib/rpm/__db.[0-9][0-9]* | rpm --quiet -qa | rpm --rebuilddb | yum clean all'
            # ssh_exec_cmd(ssh,server,cmds)
            if len(out_2) == 0:
                fixed_ips.append(server)
                setpoolstate(pool_id, server, "failedInstall", "available")
            # ssh.exec_command('reboot')
            else:
                log.info("--> Not changed the server state as couchbase-server package"
                         " removal not returned empty! {0}".format(out_2))
            ssh.close()
        except socket.timeout as e:
            hosts_down.append(row['QE-server-pool']['ipaddr'])
            log.error("Connection timed out to IP: {0}, Exception: {1}".format(row['QE-server-pool']['ipaddr'], e))
        except socket.error as e:
            hosts_down.append(row['QE-server-pool']['ipaddr'])
            log.error("Connection Failed to IP: {0}, Exception: {1}".format(row['QE-server-pool']['ipaddr'], e))
        except Exception as e:
            log.error("Exception: {0}".format(e))

    log.debug("*****************************************************")
    if len(fixed_ips) > 0:
        log.info("*** Fixed IPs list: ***")
        log.debug(fixed_ips)
    else:
        log.info("*** Fixed IPs: None ***")
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs".format(len(hosts_down)))
        log.warning(hosts_down)


parser = OptionParser()
parser.add_option("-a", "--action", dest="action",
                  default="clean_failedinstsall_vms",
                  help="Enter action")
parser.add_option("-l", "--log-level", dest="log_level",
                  default="debug",
                  help="Enter Log Level")
(options, args) = parser.parse_args()
set_log_level(options.log_level)

if options.action == "clean_failedinstsall_vms":
    clean_failedinstsall_vms(poolId)
elif options.action == "uninstall":
    uninstall_all(poolId)
elif options.action == "get_ips_without_curl":
    get_ips_without_curl(poolId)
elif options.action == "install_ntp_on_deb":
    install_ntp_on_deb(poolId)
elif options.action == "install_ntp_on_cent":
    install_ntp_on_cent(poolId)
elif options.action == "get_ips_without_wget":
    get_ips_without_wget(poolId)
elif options.action == "get_ips_without_libtinfo5":
    get_ips_without_libtinfo5(poolId)
else:
    log.error("Not yet supported action!")
