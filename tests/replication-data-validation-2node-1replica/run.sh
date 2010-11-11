#!/bin/sh
#
# Variables passed:
#    SERVERS
#    TESTNAME

NODECOUNT=2
REPCOUNT=1
ITEMSPERVB=100

SERVERS=$(echo $SERVERS | cut -f 1-$NODECOUNT -d " ")

echo "[$TESTNAME] Running vbucket_replication_validation.py -s \"$SERVERS\" -r $REPCOUNT -i $ITEMSPERVB -u Administrator -p password -m membase-server-enterprise_x86_$VERSION.rpm"

python bin/vbucket_replication_validation.py --servers="$SERVERS" -r $REPCOUNT -i $ITEMSPERVB -u Administrator -p password -m membase-server-enterprise_x86_$VERSION.rpm
