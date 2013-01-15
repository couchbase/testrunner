#!/bin/sh -ex
echo "Collecting btree stats..."
hostname=`awk -F: '/\[servers\]/ { getline; print $0 }' ${ini_file} | cut -c 3-`
./bin/btrc -n $hostname:8091 -c btree_stats

echo "Collecting server info..."
./bin/collect_server_info -i ${ini_file} > /dev/null

echo "Collecting atop stats..."
./bin/grab_atops -i ${ini_file}

echo "Couchbase server log..."
username=`grep rest_username ${ini_file} | awk -F: '{print $2}'`
password=`grep rest_password ${ini_file} | awk -F: '{print $2}'`
curl -m 2 $username:$password@$hostname:8091/logs 2> /dev/null | python -mjson.tool
