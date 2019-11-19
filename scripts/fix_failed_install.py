from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
import paramiko
import sys

if len(sys.argv) < 4:
	print("Usage: fix_failed_install.py <poolid> <osusername> <osuserpwd>")
	sys.exit(1)

poolId = sys.argv[1]
username = sys.argv[2]
password = sys.argv[3]
cb = Bucket('couchbase://172.23.105.177/QE-server-pool')
query = 'select * from `QE-server-pool` where ("%s" in poolId or poolId="%s") and state = "failedInstall";' % (str(poolId),str(poolId))
print('Running: %s' % query)
row_iter = cb.n1ql_query(N1QLQuery(query))
fixed_ips = []
index=1
for row in row_iter: 
	try:
		print('********************************')
		ipcount=len(row['QE-server-pool'])
		server = row['QE-server-pool']['ipaddr']
		print('Server#%d: %s' % (index,server))
		index=index+1
		cmds = 'rpm -e `rpm -qa | grep couchbase-server` && rm -rf /etc/systemd/system/multi-user.target.wants/couchbase-server.service && systemctl daemon-reload && systemctl list-units --type service --all'
		print('cmds: %s' % cmds)
		ssh = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh.connect(server, username=username, password=password, timeout=10)
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
		out_1 = ssh_stdout.read()
		err_1 = ssh_stderr.read()
		print('STDERR:')
		print(err_1)
		print('STDOUT:')
		print(out_1)
		cmds = 'rpm -qa | grep couchbase-server'
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
		out_2 = ssh_stdout.read()
		err_2 = ssh_stderr.read()
		print('STDERR:')
		print(err_2)
		print('STDOUT:')
		print(out_2)
		cmds = 'yum -y install yum-utils'
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
		out_3 = ssh_stdout.read()
		err_3 = ssh_stderr.read()
		print('STDERR:')
		print(err_3)
		print('STDOUT:')
		print(out_3)
		cmds = 'yum-complete-transaction --cleanup-only'
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
		out_4 = ssh_stdout.read()
		err_4 = ssh_stderr.read()
		print('STDERR:')
		print(err_4)
		print('STDOUT:')
		print(out_4)
		cmds = 'mkdir /var/lib/rpm/backup | cp -a /var/lib/rpm/__db* /var/lib/rpm/backup/ | rm -f /var/lib/rpm/__db.[0-9][0-9]* | rpm --quiet -qa | rpm --rebuilddb | yum clean all'
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
		out_5 = ssh_stdout.read()
		err_5 = ssh_stderr.read()
		print('STDERR:')
		print(err_5)
		print('STDOUT:')
		print(out_5)
		cmds = 'iptables -F'
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmds)
		out_6 = ssh_stdout.read()
		err_6 = ssh_stderr.read()
		print('STDERR:')
		print(err_6)
		print('STDOUT:')
		print(out_6)
		if out_2 == '':
			fixed_ips.append(server)
			#ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('reboot')

	except Exception as e:
		print('Connection Failed: %s' % server)
		print(e)
		pass
	ssh.close()
	print('********************************\n\n\n')

for ip in fixed_ips:
	query = "update `QE-server-pool` set state='available' where ('%s' in poolId or poolId='%s') and state='failedInstall' and ipaddr='%s';" % (str(poolId),str(poolId),str(ip))
	print('Running: %s' % query)
	row_iter = cb.n1ql_query(N1QLQuery(query))
	for row in row_iter:
		print(row)
