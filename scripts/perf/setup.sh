#!/bin/sh -ex

#### INSTALL ####
erlang_threads=`grep erlang_threads ${test_conf} | awk -F"=" '{print $2}'`
if [ -n ${erlang_threads} ] ; then
    erlang_threads=",erlang_threads=${erlang_threads}"
fi

if [ -n ${toy_build} ] ; then
   toy=",toy=${toy_build}"
fi

./bin/install -i ${ini_file} \
    -p product=cb,version=${version_number},vbuckets=1024,parallel=True,use_domain_names=1${erlang_threads}${toy}

#### CLUSTER SETUP ####
index_replicas=`grep index_replicas ${test_conf} | awk -F"=" '{print $2}'`
if [ -z ${index_replicas} ]; then
    index_replicas=1
fi

mem_quota=`grep mem_quota ${test_conf} | awk -F"=" '{print $2}'`

./bin/do_cluster -i ${ini_file} -c ${test_conf} \
    -p num_nodes=${num_nodes},mem_quota=${mem_quota},index_replicas=${index_replicas} setUp

#### SWAP RESET ####
./bin/ssh -i ${ini_file} "swapoff -a && swapon -a"
