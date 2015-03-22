#!/bin/bash

cd /opt/couchbase/bin

echo 1. Create default bucket
./couchbase-cli bucket-create -c localhost:8091 --bucket=default --bucket-type=couchbase --bucket-port=11211 --bucket-ramsize=105 --bucket-replica=1 --bucket-priority=low --wait -u Administrator -p password

echo 2. Create bucket with name of “standard" with password
./couchbase-cli bucket-create -c localhost:8091 --bucket=standard --bucket-type=couchbase --bucket-password=password  --bucket-port=11211 --bucket-ramsize=105 --bucket-replica=1 --bucket-priority=low --wait -u Administrator -p password

echo 3. Create bucket to test port
./couchbase-cli bucket-create -c localhost:8091 --bucket=port88 --bucket-type=couchbase --bucket-port=1025 --bucket-ramsize=105 --bucket-replica=1 --bucket-priority=low --wait -u Administrator -p password

echo 4. Create bucket with special characters in bucket name
 ./couchbase-cli bucket-create -c localhost:8091 --bucket=test_%E-.5  --bucket-type=couchbase  --bucket-port=11213  --bucket-ramsize=105 --bucket-replica=1 --bucket-priority=low --wait -u Administrator -p password

echo 5. Create memcached bucket
/opt/couchbase/bin/couchbase-cli bucket-create -c localhost:8091 -u Administrator -p password  --bucket=test_sasl --bucket-type=memcached --bucket-password=password --bucket-ramsize=105 --bucket-eviction-policy=valueOnly --enable-flush=1   -u Administrator -p password
