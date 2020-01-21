from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
import paramiko
import sys

poolId = sys.argv[1]
username = sys.argv[2]
password = sys.argv[3]
cb = Bucket('couchbase://172.23.105.177/QE-server-pool')
query = 'select * from `QE-server-pool` where "%s" in poolId and state = "failedInstall";' % str(poolId)
print(('Running: %s' % query))
row_iter = cb.n1ql_query(N1QLQuery(query))
fixed_ips = []
for row in row_iter: 
	try:
		print('********************************')
		server = row['QE-server-pool']['ipaddr']
		print(('Server: %s' % server))
		cmds = 'rpm -e `rpm -qa | grep couchbase-server` && rm -rf /etc/systemd/system/multi-user.target.wants/couchbase-server.service && systemctl daemon-reload && systemctl list-units --type service --all'
		print(('cmds: %s' % cmds))
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
		if out_2 == '':
			fixed_ips.append(server)
			#ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('reboot')

	except Exception as e:
		print(('Connection Failed: %s' % server))
		print(e)
		pass
	ssh.close()
	print('********************************\n\n\n')

for ip in fixed_ips:
	query = "update `QE-server-pool` set state='available' where '%s' in poolId and state='failedInstall' and ipaddr='%s';" % (str(poolId), str(ip))
	print(('Running: %s' % query))
	row_iter = cb.n1ql_query(N1QLQuery(query))
	for row in row_iter:
		print(row)
