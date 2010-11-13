#!/bin/sh
#
# Variables passed:
#    SERVERS
#    TESTNAME

NODECOUNT=2
REPCOUNT=1

SERVERS=$(echo $SERVERS | cut -f 1-$NODECOUNT -d " ")

echo "[$TESTNAME] Running vbmap_vbmigrator_validation.py -s \"$SERVERS\" -r $REPCOUNT -u Administrator -p password -m membase-server-enterprise_x86_$VERSION.rpm"

python bin/vbmap_vbmigrator_validation.py --servers="$SERVERS" -r $REPCOUNT -u Administrator -p password -m membase-server-enterprise_x86_$VERSION.rpm
