#!/bin/sh -ex
num_clients=`grep total_clients ${test_conf} | awk -F"=" '{print $2}'`

./bin/do_cluster -i ${ini_file} -c ${test_conf} -p num_clients=$num_clients tearDown

for f in final.*.json
do
    ./bin/post_perf_data -n http://10.5.2.41:5984 -d eperf -i $f
done
rm -rf final.*.json
