#!/usr/bin/env python
import sys
import EC2
import cli_interface
import mc_bin_client
from membase_install import *
from testrunner_common import *

#if len(sys.argv) < 2:
#    print sys.argv[0] + " <keyfile>"
#    sys.exit(1)
config = Config()
keyfile = "/Users/mikewied/.ssh/aws-key" #print sys.argv[1]
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = file(keyfile).next().split()
SECURITY_GROUP_NAME = "ec2-example-rb-test-group"
conn = EC2.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

#print "----- Adding another VM -----"
#result = conn.run_instances("ami-0f42a966", minCount=1, maxCount=1, keyName="mikew_key", instanceType="m1.large", availabilityZone="us-east-1d")
#print result



#
# Get all needed information for the EC2 instances we created
#

instanceIds=["i-86e1f8eb", "i-84e1f8e9"]#, "i-bf50b4d3", "i-b950b4d5"]
public_dns = []
private_dns = []
for id in instanceIds:
    instanceInfo =  conn.describe_instances([id])
    public_dns.append(Server(instanceInfo.parse()[1][3]))
    private_dns.append(Server(instanceInfo.parse()[1][4]))

#print public_dns
#print private_dns


#
# Test 1:
# Set up the 2 node cluster with 1 replica
#
# Load the large key set by running mc_loader
# Load 6 million item with memcachetest
# Run memcachetest on a working set of 1 million items
#
bucket_port = "11212"

server_ip = "75.101.225.91"
server_instance = "membase-server-community_x86_64_1.6.4.1r.rpm"

for server in public_dns:
    reinstall_membase(server.host, server_instance)
    print cli_interface.CLIInterface(server.host).node_init("/var/opt/membase/1.6.4.1r/data/ns_1")

print public_dns[0].host
cli = cli_interface.CLIInterface(public_dns[0].host, ssh=True, sshkey="/Users/mikewied/.ssh/mikew_key.pem")
print cli.cluster_init()
print cli.bucket_create("test_bucket", "membase", 11212)
print cli.server_add(public_dns[1].host)
print cli.rebalance()



#ssh(public_dns[0].host, "wget https://download.github.com/mikewied-Testsuite-80e7b38.tar.gz ; \
#tar xzf mikewied-Testsuite* ; \
#rm -rf *.tar.gz ; \
#mv mikewied-Testsuite* Testsuite ; \
#cd Testsuite ; \
#./setup.sh");

#ssh(public_dns[0].host, "/opt/memcachetestv/bin/memcachetest -h%s:%s -i600 -c100 -M2048 -F -t8 ; \
#/root/Testsuite/mc-loader/mc-loader %s:%s keyset_10 binary 1024" % (public_dns[1].host, bucket_port, public_dns[1].host, bucket_port));




