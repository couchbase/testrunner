#!/bin/sh
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    VERSION
#    KEYFILE

if [ -z "$KEYFILE" ]; then
    echo "[$TESTNAME] KEYFILE is not set."
    exit 1
fi

RETCODE=0

for SERVER in $SERVERS ; do
    echo "[$TESTNAME] resetting $SERVER"
    ssh -i $KEYFILE root@$SERVER "service membase-server stop &> /dev/null ; rm -f /var/opt/membase/*/data/ns_1/* /var/opt/membase/*/mnesia/* /etc/opt/membase/*/ns_1/* &> /dev/null ; service membase-server restart &> /dev/null" 2> /dev/null &
done
wait
sleep 2

for SERVER in $SERVERS ; do
    echo "[$TESTNAME] initting $SERVER"
    curl -d "port=SAME&initStatus=done&username=Administrator&password=password" "$SERVER:8091/settings/web" &> /dev/null &
done
wait
sleep 2

for SERVER in $SERVERS ; do
    echo "[$TESTNAME] creating default bucket on $SERVER"
    ssh -i $KEYFILE root@$SERVER /opt/membase/bin/cli/membase bucket-create -c localhost -u Administrator -p password --bucket=default --bucket-type=membase --bucket-password="" --bucket-ramsize=300 --bucket-replica=1 &> /dev/null &
done
wait
sleep 10


exit $RETCODE
