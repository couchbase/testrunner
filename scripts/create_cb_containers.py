#!/usr/bin/env/python
import os, sys, argparse, json, subprocess, paramiko, requests, time, threading
from scp import SCPClient

spark_worker_container='spark_worker'
spark_master_container='spark_master'
couchbase_container='couchbase_base'
couchbase_ips = []
spark_worker_ips = []
spark_master_ips = []
container_prefix = "dockercb"
cluster_config_file = "config.json"
buckets = ['default']
masterIp = None
masterClient = None

def run_command(args):
	p = subprocess.Popen(args)
	p.wait()
	if p.returncode != 0:
		print('{0} failed with exit status'.format(p.returncode))
		os._exit(1)


def get_ips_and_configure(cb_nodes, prefix, download_url, descfile):
	for i in range(1, int(cb_nodes) + 1):
		container_id = "{0}_{1}_{2}".format(prefix, couchbase_container, i)
		args = ["docker", "inspect", "--format='{{.NetworkSettings.IPAddress}}'", container_id]
                print('the args are', args)
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		out, err = process.communicate()
                print('out is', out)
                print('err is', err)
                if out.rstrip() == '':
                     print('failed to get an IP')
                     return
                else:
		    couchbase_ips.append(out.rstrip())


        print('the ips are', couchbase_ips)
	with open(descfile, "w+") as f:
             json.dump( couchbase_ips, f)

	tasks = []

	lock = threading.Lock()
	for i in range(0, int(cb_nodes)):
		isMaster = False
		if i == 0:
			isMaster = True
		task = threading.Thread(target=install_couchbase, args=(couchbase_ips[i], download_url, isMaster))
		task.start()
		tasks.append(task)
	[task.join() for task in tasks]

	time.sleep(20) #install should not take longer than this - use a better way
	for i in range(0, int(cb_nodes)):
                print('requesting from', "http://{0}:8091/pools".format(couchbase_ips[i]))
		r = requests.get("http://{0}:8091/pools".format(couchbase_ips[i]))
		if r.status_code != 200:
			print("Server not installed correctly. Received status code:".format(r.status_code))
			os._exit(1)

	#initialize_nodes_rebalance(couchbase_ips)

def install_couchbase(ip, url, isMaster):
	client = paramiko.SSHClient()
	client.load_system_host_keys()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print('the ip is', ip)
	client.connect(ip, username="root", password="root")
	scp = SCPClient(client.get_transport())
	if isMaster == True:
		global masterClient
		masterClient = client
		global masterIp
		masterIp = ip
	scp = SCPClient(client.get_transport())
	scp.put('cluster-install.py', 'cluster-install.py')
	command = "python cluster-install.py {0}".format(url)
	(stdin, stdout, stderr) = client.exec_command(command)
	for line in stdout.readlines():
		print(line)
	if isMaster != True:
		client.close()

def start_environment(cbnodes, prefix):
	cb_args = "couchbase_base={0}".format(cbnodes)
	args = ["docker-compose", "-p='{0}'".format(prefix), "scale", cb_args] #,  "spark_master=1",  spark_worker_args]
        print('start environment args are', args)
	run_command(args)

def cleanup_environment():
	args = ["python", "stop_cluster.py", "--prefix={0}".format(container_prefix)]
	run_command(args)

parser = argparse.ArgumentParser(description='Setup couchbase and spark clusters. Currently supports one spark master')
parser.add_argument('--cb-nodes', dest='cbnodes', required=True, help='Number of couchbase nodes in cb cluster')
parser.add_argument('--desc-file', dest='descfile', required=True, help='File to put the IPs in')
parser.add_argument('--url', dest='url', required=True, help='Couchbase-server version')
parser.add_argument('--desc', dest='desc', required=True, help='Identify the user')
args = parser.parse_args()
#cleanup_environment()
prefix = 'cbserver' + args.desc
print('the prefix is', prefix)

start_environment(args.cbnodes, prefix)
get_ips_and_configure(args.cbnodes, prefix, args.url, args.descfile)
