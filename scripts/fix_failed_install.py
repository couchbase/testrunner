import socket
from optparse import OptionParser
import paramiko
import logging
import sys
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions

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

auth = PasswordAuthenticator(cb_username, cb_userpassword)
cluster = Cluster('couchbase://172.23.104.162', ClusterOptions(authenticator=auth))
cb = cluster.bucket('QE-server-pool').default_collection()


def ssh_exec_cmd(ssh, cmds):
    log.debug('Running commands: {0}'.format(cmds))
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
    out_1 = ssh_stdout.read()
    err_1 = ssh_stderr.read()
    if len(err_1) > 0:
        log.error('STDERR: {0}'.format(err_1))
    if len(out_1) > 0:
        log.info('STDOUT: {0}'.format(out_1))
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
        cmds = 'apt remove -y --purge --autoremove couchbase-server && ' \
               'rm -rf /var/lib/dpkg/info/couchbase-server.* && ' \
               'dpkg --configure -a && ' \
               'apt-get update && ' \
               'rm -rf /opt/couchbase'
        ssh_exec_cmd(ssh, cmds)
        # cmds = "rm -rf /opt/couchbase"
        # ssh_exec_cmd(ssh,cmds)
    else:
        log.warning("No couchbase-server dpkg to delete!")


def setpoolstate(pool_id, ip, fromstate, tostate):
    query = "update `QE-server-pool` set state='{0}' where ('{1}' in poolId or poolId='{2}') and " \
            "state like '{3}%' and ipaddr='{4}';".format(str(tostate), str(pool_id),
                                                         str(pool_id), str(fromstate), str(ip))
    log.info('Running: {0}'.format(query))
    row_iter = cluster.query(query)
    for row in row_iter.rows():
        log.info(row)


def setipstate(ip, fromstate, tostate):
    query = "update `QE-server-pool` set state='{0}' where state like '{1}%' and ipaddr='{2}';"\
            .format(str(fromstate), str(tostate), str(ip))
    log.debug('Running: {0}'.format(query))
    row_iter = cluster.query(query)
    for row in row_iter.rows():
        log.info(row)


def get_ips_without_curl(pool_id):
    query = "SELECT username, ipaddr, state FROM `QE-server-pool` WHERE os = 'debian' AND " \
            "(poolId = '{0}' OR '{1}' IN poolId)".format(pool_id, pool_id)
    log.debug('Running: {0}'.format(query))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    row_iter = cluster.query(query)
    hosts_down = []
    no_curl = []
    for row in row_iter.rows():
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
                    log.info("Curl Installed on {0}. STDOUT => {1}".format(row['ipaddr'], stdout_2))
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
            log.error("Exception: {0}".format(e))

    log.debug("*****************************************************")
    log.info("Total IPs: {0}".format(len(row_iter.rows())))
    if len(no_curl) > 0:
        log.debug("{0} IPs did not have curl. => {1}".format(len(no_curl), no_curl))
        log.info("Curl installed on all accessible IPs successfully")
    else:
        log.info("All hosts had Curl!")
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs => {1}".format(len(hosts_down), hosts_down))


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


def clean_failedinstsall_vms(pool_id):
    log.debug("*** Cleaning failedInstall VMs ***")
    query = 'SELECT * FROM `QE-server-pool` WHERE ("{0}" IN poolId OR poolId = "{1}") AND ' \
            'state LIKE "failedInstall%%";'.format(str(pool_id), str(pool_id))
    log.info('Running: {0}'.format(query))
    row_iter = cluster.query(query)
    fixed_ips = []
    hosts_down = []
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for row in row_iter.rows():
        log.debug('********************************')
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
            hosts_down.append(row['ipaddr'])
            log.error("Connection timed out to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except socket.error as e:
            hosts_down.append(row['ipaddr'])
            log.error("Connection Failed to IP: {0}, Exception: {1}".format(row['ipaddr'], e))
        except Exception as e:
            log.error("Exception: {0}".format(e))
        log.debug('********************************')

    if len(fixed_ips) > 0:
        log.info("*** Fixed IPs list: ***")
        log.info(fixed_ips)
        for ip in fixed_ips:
            setpoolstate(pool_id, ip, "failedInstall", "available")
    else:
        log.info("*** Fixed IPs: None ***")
    if len(hosts_down) > 0:
        log.warning("Failed to connect to {0} IPs".format(len(hosts_down)))
        log.warning(hosts_down)


parser = OptionParser()
parser.add_option("-a", "--action", dest="action",
                  default="clean_failedinstsall_vms",
                  help="Enter action")
parser.add_option("--log-level", dest="log_level",
                  default="info",
                  help="Enter Log Level")
(options, args) = parser.parse_args()
set_log_level(options.log_level)

if options.action == "clean_failedinstsall_vms":
    clean_failedinstsall_vms(poolId)
elif options.action == "uninstall":
    uninstall_all(poolId)
elif options.action == "get_ips_without_curl":
    get_ips_without_curl(poolId)
else:
    log.error("Not yet supported action!")
