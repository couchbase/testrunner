#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME

NODECOUNT=2
TIMEOUT=120

MASTER=$(echo $SERVERS | cut -d " " -f 1)
SLAVE=$(echo $SERVERS | cut -d " " -f 2)

MASTER_IP=$(echo $MASTER | cut -d ":" -f 1)
MASTER_REST=$(echo $MASTER | cut -d ":" -f 2)
MASTER_MOXI=$(echo $MASTER | cut -d ":" -f 3)
MASTER_MEMCACHED=$(echo $MASTER | cut -d ":" -f 4)

SLAVE_IP=$(echo $SLAVE | cut -d ":" -f 1)
SLAVE_REST=$(echo $SLAVE | cut -d ":" -f 2)
SLAVE_MOXI=$(echo $SLAVE | cut -d ":" -f 3)
SLAVE_MEMCACHED=$(echo $SLAVE | cut -d ":" -f 4)

echo "[$TESTNAME] Running Add/remove rebalance on \"$MASTER $SLAVE\""

echo "[$TESTNAME] Setting 100 keys"
for i in {0..99} ; do
    echo -e "set $i 0 0 1\n1\r" | nc $MASTER_IP $MASTER_MOXI &> /dev/null
done

ret=0
echo "[$TESTNAME] Adding server $SLAVE to $MASTER"
$MEMBASE_CLI rebalance -c $MASTER_IP:$MASTER_REST --server-add=$SLAVE_IP:$SLAVE_REST -u $REST_USER -p $REST_PASSWORD --server-add-username=$REST_USER --server-add-password=$REST_PASSWORD &> /dev/null
count=$($MEMBASE_CLI server-list -c $MASTER_IP:$MASTER_REST -u $REST_USER -p $REST_PASSWORD 2> /dev/null | grep "healthy active" | wc -l)
if [[ $count -ne 2 ]] ; then
    echo "[$TESTNAME] Add server failed"
    ret=1
fi 
status=$($MEMBASE_CLI server-list -c $MASTER_IP:$MASTER_REST -u $REST_USER -p $REST_PASSWORD -o json 2> /dev/null| sed -e 's/^.*"balanced":\(.*\),"rebalanceStatus.*$/\1/')
if [[ $status == "false" ]] ; then
    echo "[$TESTNAME] Cluster is not balanced after add"
    ret=1
fi

echo "[$TESTNAME] Removing server $SLAVE from $MASTER"
$MEMBASE_CLI rebalance -c $MASTER_IP:$MASTER_REST --server-remove=$SLAVE_IP:$SLAVE_REST -u $REST_USER -p $REST_PASSWORD &> /dev/null
count=$($MEMBASE_CLI server-list -c $MASTER_IP:$MASTER_REST -u $REST_USER -p $REST_PASSWORD 2> /dev/null | grep "healthy active" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "[$TESTNAME] Remove server failed"
    ret=1
fi
status=$($MEMBASE_CLI server-list -c $MASTER_IP:$MASTER_REST -u $REST_USER -p $REST_PASSWORD -o json 2> /dev/null| sed -e 's/^.*"balanced":\(.*\),"rebalanceStatus.*$/\1/')
if [[ $status == "false" ]] ; then
    echo "[$TESTNAME] Cluster is not balanced after remove"
    ret=1
fi

exit $ret

