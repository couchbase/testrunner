#!/bin/sh
#
# Variables passed:
#    SERVERS
#    TESTNAME

NODECOUNT=4
REPCOUNT=3

SERVERS=$(echo $SERVERS | cut -f 1-$NODECOUNT -d " ")

echo "[$TESTNAME] Running vbucket_check.py -s \"$SERVERS\" -r $REPCOUNT -u Administrator -p password -m membase-server-enterprise_x86_$VERSION.rpm"

python bin/vbucket_check.py -s "$SERVERS" -r $REPCOUNT -u Administrator -p password -m membase-server-enterprise_x86_$VERSION.rpm
