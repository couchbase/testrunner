#!/usr/bin/env python

import sys

from boto.ec2.connection import EC2Connection


if len(sys.argv) != 2:
    print "aws_ini.py <tagname>"
    sys.exit()

# use environment variables to set credentials
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY

tag_name = sys.argv[1]

conn = EC2Connection()

reservations = conn.get_all_instances()

instances = []

for reservation in reservations:
    for instance in reservation.instances:
        if u'Name' in instance.tags:
            if instance.tags[u'Name'] == tag_name:
                instances.append({"private_ip_address":instance.private_ip_address,
                                  "public_dns_name":instance.public_dns_name})

print '''[global]
username:ec2-user
ssh_key:/root/key.pem
port:8091
data_path:/mnt/ebs/

[servers]'''
i = 1
for instance in instances:
    print `i` + ":" + instance["private_ip_address"]
    print "#public_dns_name:" + instance["public_dns_name"]
    i += 1
print '''
[membase]
rest_username:Administrator
rest_password:password'''
