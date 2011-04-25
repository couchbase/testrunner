#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    KEYFILE
#    VERSION

ret=0
datasize=40000
sleeptime=120

if [[ ! -a bin/mc-loader ]] ; then
    echo "[$TESTNAME] WARNING, missing bin/mc-loader"
    exit 0
fi

MASTER=$(echo $SERVERS | cut -d " " -f 1 | cut -f 1 -d ":")
SLAVE=$(echo $SERVERS | cut -d " " -f 2 | cut -f 1 -d ":")

ssh $MASTER /opt/membase/bin/membase bucket-edit -c localhost -u Administrator -p password --bucket=default --bucket-type=membase --bucket-password='' --bucket-ramsize=100 &> /dev/null
ssh $MASTER /opt/membase/bin/membase bucket-create -c localhost -u Administrator -p password --bucket=mb1 --bucket-type=membase --bucket-password='' --bucket-ramsize=100 --bucket-replica=1 &> /dev/null

sleep 5

bin/mc-loader $MASTER bin/keytest/keyset_10 binary valuesize $datasize &> /dev/null
if [[ $? -ne 0 ]] ; then
    echo "[$TESTNAME] Set to default bucket failed"
    ret=1
fi
bin/mc-loader $MASTER bin/keytest/keyset_10 binary valuesize $datasize sasl mb1 &> /dev/null
if [[ $? -ne 0 ]] ; then
    echo "[$TESTNAME] Set to second bucket failed"
    ret=1
fi

sleep 1

ssh $MASTER /opt/membase/bin/membase server-add -c localhost -u Administrator -p password --server-add=$SLAVE --server-add-username=Administrator --server-add-password=password &> /dev/null
ssh $MASTER /opt/membase/bin/membase rebalance -c localhost -u Administrator -p password &> /dev/null
count=$(ssh $MASTER /opt/membase/bin/membase server-list -c $MASTER -u Administrator -p password 2> /dev/null | grep "healthy active" | wc -l)
if [[ $count -ne 2 ]] ; then
    echo "[$TESTNAME] Add server failed"
    ret=1
fi
status=$(ssh $MASTER /opt/membase/bin/membase server-list -c $MASTER -u Administrator -p password -o json 2> /dev/null| sed -e 's/^.*"balanced":\(.*\),"rebalanceStatus.*$/\1/')
if [[ $status == "false" ]] ; then
    echo "[$TESTNAME] Cluster is not balanced after add"
    ret=1
fi

sleep $sleeptime

ssh $MASTER /opt/membase/bin/membase failover -c localhost -u Administrator -p password --server-failover $SLAVE &> /dev/null
sleep 5
count=$(ssh $MASTER /opt/membase/bin/membase server-list -c $MASTER -u Administrator -p password 2> /dev/null | grep "healthy active" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "[$TESTNAME] Failover server failed"
    ret=1
fi
status=$(ssh $MASTER /opt/membase/bin/membase server-list -c $MASTER -u Administrator -p password -o json 2> /dev/null | sed -e 's/^.*"balanced":\(.*\),"rebalanceStatus.*$/\1/')
if [[ $status == "false" ]] ; then
    echo "[$TESTNAME] Cluster is not balanced after failover"
    ret=1
fi

d=$(bin/mc-loader $MASTER bin/keytest/keyset_10 binary valuesize $datasize check 2> /dev/null)
if [[ $? -ne 0 ]] ; then
    echo "[$TESTNAME] Get from default bucket failed"
    echo "$d"
    ret=1
fi
d=$(bin/mc-loader $MASTER bin/keytest/keyset_10 binary valuesize $datasize sasl mb1 check 2> /dev/null)
if [[ $? -ne 0 ]] ; then
    echo "[$TESTNAME] Get from second bucket failed"
    echo "$d"
    ret=1
fi


exit $ret
