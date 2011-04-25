#!/bin/sh
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    VERSION
#    KEYFILE

RETCODE=0

for SERVER in $SERVERS ; do
    SERVER_IP=$(echo $SERVER | cut -f 1 -d ":")
    echo "[$TESTNAME] resetting $SERVER"
    ssh -i $KEYFILE root@$SERVER_IP "service membase-server stop &> /dev/null ; rm -f /var/opt/membase/*/data/ns_1/* /var/opt/membase/*/mnesia/* /etc/opt/membase/*/ns_1/* &> /dev/null ; service membase-server restart &> /dev/null" 2> /dev/null &
done
wait
sleep 2

for SERVER in $SERVERS ; do
    SERVER_IP=$(echo $SERVER | cut -f 1 -d ":")
    echo "[$TESTNAME] initting $SERVER"
    ssh -i $KEYFILE root@$SERVER_IP /opt/membase/bin/membase cluster-init -c localhost -u Administrator -p password --cluster-init-username=Administrator --cluster-init-password=password --cluster-init-port=8091
done
wait
sleep 2

for SERVER in $SERVERS ; do
    SERVER_IP=$(echo $SERVER | cut -f 1 -d ":")
    echo "[$TESTNAME] creating default bucket on $SERVER"
    ssh -i $KEYFILE root@$SERVER_IP /opt/membase/bin/membase bucket-create -c localhost -u Administrator -p password --bucket=default --bucket-type=membase --bucket-password="" --bucket-ramsize=300 --bucket-replica=1 &> /dev/null &
done
wait
sleep 10


exit $RETCODE
