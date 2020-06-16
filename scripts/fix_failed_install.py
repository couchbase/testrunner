from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
import paramiko
import sys

if len(sys.argv) < 6:
    print("Usage: fix_failed_install.py <poolid> <osusername> <osuserpwd> <cbuser> "
          "<cbuserpassword>")
    sys.exit(1)

poolId = sys.argv[1]
username = sys.argv[2]
password = sys.argv[3]
cb_username = sys.argv[4]
cb_userpassword = sys.argv[5]

cluster = Cluster('couchbase://172.23.104.162')
authenticator = PasswordAuthenticator(cb_username,cb_userpassword)
cluster.authenticate(authenticator)
cb = cluster.open_bucket('QE-server-pool')

def ssh_exec_cmd(ssh, server, cmds):
    print('cmds: %s' % cmds)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
    out_1 = ssh_stdout.read()
    err_1 = ssh_stderr.read()
    print('STDERR:')
    print(err_1)
    print('STDOUT:')
    print(out_1)
    return out_1


def uninstall_clean_cb(ssh, server):
    print("*** Cleaning CB ***")
    cmds = 'rpm -qa | grep couchbase-server'
    out_2 = ssh_exec_cmd(ssh, server, cmds)
    if out_2 != '':
        cmds = 'rpm -e `rpm -qa | grep couchbase-server` && rm -rf ' \
			   '/etc/systemd/system/multi-user.target.wants/couchbase-server.service && systemctl ' \
			   'daemon-reload && systemctl list-units --type service --all'
        ssh_exec_cmd(ssh, server, cmds)
    else:
        print("No couchbase-server rpm to delete!")


def setpoolstate(poolId, ip, fromstate, tostate):
    query = "update `QE-server-pool` set state='%s' where ('%s' in poolId or poolId='%s') and " \
			"state like '%s%%' and ipaddr='%s';" % (
    str(tostate), str(poolId), str(poolId), str(fromstate), str(ip))
    print('Running: %s' % query)
    row_iter = cb.n1ql_query(N1QLQuery(query))
    for row in row_iter:
        print(row)


def setipstate(ip, fromstate, tostate):
    query = "update `QE-server-pool` set state='%s' where state like '%s%%' and ipaddr='%s';" % (
    str(fromstate), str(tostate), str(ip))
    print('Running: %s' % query)
    row_iter = cb.n1ql_query(N1QLQuery(query))
    for row in row_iter:
        print(row)


def uninstall_all(ips):
    all_ips = ips.split(',')
    for ip in all_ips:
        setipstate(ip, "available", "booked")
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username=username, password=password, timeout=10)
            uninstall_clean_cb(ssh, ip)
            out = ssh_exec_cmd(ssh, ip, "rpm -qa | grep couchbase-server")
            if out == '':
                setipstate(ip, "booked", "available")
        except Exception as e:
            print(e)
            pass


def clean_failedinstsall_vms(poolId):
    print("*** Cleaning failedInstall VMs ***")
    query = 'select * from `QE-server-pool` where ("%s" in poolId or poolId="%s") and state like ' \
			'"failedInstall%%";' % (
    str(poolId), str(poolId))
    print('Running: %s' % query)
    row_iter = cb.n1ql_query(N1QLQuery(query))
    fixed_ips = []
    index = 1
    for row in row_iter:
        try:
            print('********************************')
            ipcount = len(row['QE-server-pool'])
            server = row['QE-server-pool']['ipaddr']
            print('Server#%d: %s' % (index, server))
            index = index + 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(server, username=username, password=password, timeout=10)
            uninstall_clean_cb(ssh, server)
            cmds = 'rpm -qa | grep couchbase-server'
            out_2 = ssh_exec_cmd(ssh, server, cmds)
            cmds = 'yum -y install yum-utils'
            ssh_exec_cmd(ssh, server, cmds)
            cmds = 'yum-complete-transaction --cleanup-only; yum clean all'
            ssh_exec_cmd(ssh, server, cmds)
            cmds = 'iptables -F'
            ssh_exec_cmd(ssh, server, cmds)
            # cmds = 'mkdir /var/lib/rpm/backup | cp -a /var/lib/rpm/__db* /var/lib/rpm/backup/ |
			# rm
            # -f /var/lib/rpm/__db.[0-9][0-9]* | rpm --quiet -qa | rpm --rebuilddb | yum clean all'
            # ssh_exec_cmd(ssh,server,cmds)
            if out_2 == '':
                fixed_ips.append(server)
                setpoolstate(poolId, server, "failedInstall",
                             "available")  # ssh_stdin, ssh_stdout,   # ssh_stderr =
				# ssh.exec_command('reboot')
            else:
                print("-->Not changed the server state as couchbase-server package removal not "
                      "returned empty! {}".format(out_2))

        except Exception as e:
            print('Connection Failed: %s' % server)
            print(e)
            pass
        ssh.close()
        print('********************************\n\n\n')

    if (len(fixed_ips) > 0):
        print("*** Fixed IPs list: ***")
        for ip in fixed_ips:
            # setpoolstate(poolId,ip,"failedInstall","available")
            print(ip)
    else:
        print("*** Fixed IPs: None ***")


from optparse import OptionParser

parser = OptionParser()
parser.add_option("-a", "--action", dest="action", default="clean_failedinstsall_vms",
                  help="Enter action")
(options, args) = parser.parse_args()

if options.action == "clean_failedinstsall_vms":
    clean_failedinstsall_vms(poolId)
elif options.action == "uninstall":
    uninstall_all(poolId)
else:
    print("Not yet supported action!")
