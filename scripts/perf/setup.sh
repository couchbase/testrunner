#!/bin/sh
mem_quota=`grep mem_quota conf/perf/vperf-lnx.conf | awk -F"=" '{print $2}'`
./bin/install -i ${ini_file} -p product=cb,version=${version_number},vbuckets=1024,parallel=True
./bin/do_cluster -i ${ini_file} -c ${test_conf} -p num_nodes=${num_nodes},mem_quota=${mem_quota} setUp
./bin/ssh -i ${ini_file} "swapoff -a && swapon -a"
