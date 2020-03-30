#!/usr/bin/python
import sys
import os

master_node="10.6.2.164"
ram_quota="22000"
print(" Set the ram quota")
os.system("curl -d memoryQuota={0} 'http://Administrator:password@{1}:8091/pools/default'".format(ram_quota, master_node))

print(" Add nodes in to create the basic setup")
server_dict={"10.6.2.167":"kv", "10.6.2.168":"kv","10.6.2.232":"kv","10.6.2.194":"n1ql", "10.6.2.195":"n1ql","10.6.2.233":"index","10.6.2.234":"index", "10.6.2.237":"index", "10.6.2.238":"index"}

for key, value in server_dict.items(): 
	os.system("curl -u Administrator:password http://{0}:8091/controller/addNode -d 'hostname={1}&user=Administrator&password=password&services={2}'".format(master_node, key, value))

print(" Now lets rebalance in all the nodes")
str="knownNodes=ns_1@{0},".format(master_node)
for key in list(server_dict.keys()):
	str=str+'ns_1@{0},'.format(key)

nodes=str[:-1]
print(nodes)
print(("curl -v -X POST -u Administrator:password 'http://{0}:8091/controller/rebalance' -d '{1}'".format(master_node, nodes)))
os.system("curl -v -X POST -u Administrator:password 'http://{0}:8091/controller/rebalance' -d '{1}'".format(master_node, nodes))

print(" done!")

