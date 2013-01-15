#!/bin/sh -ex
erlang_threads=`grep erlang_threads ${test_conf} | awk -F"=" '{print $2}'`
if [ -z ${erlang_threads} ]; then
    ./bin/install -i ${ini_file} -p product=cb,version=${version_number},vbuckets=1024,parallel=True
else
    ./bin/install -i ${ini_file} -p product=cb,version=${version_number},vbuckets=1024,parallel=True,erlang_threads=${erlang_threads}
fi

mem_quota=`grep mem_quota ${test_conf} | awk -F"=" '{print $2}'`
./bin/do_cluster -i ${ini_file} -c ${test_conf} -p num_nodes=${num_nodes},mem_quota=${mem_quota} setUp

./bin/ssh -i ${ini_file} "swapoff -a && swapon -a"
