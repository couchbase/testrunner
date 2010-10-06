#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME

# if a test requires only one server use the first one.
#
# if [ -z "$SERVER" ]; then
#         SERVER=`head -1 "$SERVERFILE"`
# fi

# if SERVER isn't set, make a comma delimited list out of our server file

NODECOUNT=2
TIMEOUT=120

I=0
if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		# this may seem weird, but if server is empty we don't
		# want to prepend our first entry with a comma.
		I=$((I+1))
	        if [[ $I -gt $NODECOUNT ]] ; then
		        break
		fi
		if [ -z "$SERVER" ]; then
			SERVER="$entry"
		else
			SERVER=$SERVER,$entry
		fi
	done
fi

MASTER=$(echo $SERVER | cut -d "," -f 1)
SLAVE=$(echo $SERVER | cut -d "," -f 2)

echo "[$TESTNAME] Running Add/remove rebalance on \"$SERVER\""

echo "[$TESTNAME] Setting 100 keys"
ssh $MASTER "for i in {0..0} ; do x=\"\" ; for j in {0..99} ; do x=\"\${x}set key_\${i}\${j} 0 0 1\n1\r\n\" ; done ; echo -e \"\$x\" | nc localhost 11211 > /dev/null; done" 2>/dev/null

ret=0
echo "[$TESTNAME] Adding server $SLAVE to $MASTER"
ssh $MASTER /opt/membase/bin/cli/membase rebalance -c $MASTER:8091 --server-add=$SLAVE:8091 -u Administrator -p password --server-add-username=Administrator --server-add-password=password &> /dev/null
count=$(ssh $MASTER /opt/membase/bin/cli/membase server-list -c $MASTER -u Administrator -p password 2> /dev/null | grep "healthy active" | wc -l)
if [[ $count -ne 2 ]] ; then
    echo "[$TESTNAME] Add server failed"
    ret=1
fi 
status=$(ssh $MASTER /opt/membase/bin/cli/membase server-list -c $MASTER -u Administrator -p password -o json 2> /dev/null| sed -e 's/^.*"balanced":\(.*\),"rebalanceStatus.*$/\1/')
if [[ $status == "false" ]] ; then
    echo "[$TESTNAME] Cluster is not balanced after add"
    ret=1
fi

echo "[$TESTNAME] Removing server $SLAVE from $MASTER"
ssh $MASTER /opt/membase/bin/cli/membase rebalance -c $MASTER:8091 --server-remove=$SLAVE:8091 -u Administrator -p password &> /dev/null
count=$(ssh $MASTER /opt/membase/bin/cli/membase server-list -c $MASTER -u Administrator -p password 2> /dev/null | grep "healthy active" | wc -l)
if [[ $count -ne 1 ]] ; then
    echo "[$TESTNAME] Remove server failed"
    ret=1
fi
status=$(ssh $MASTER /opt/membase/bin/cli/membase server-list -c $MASTER -u Administrator -p password -o json 2> /dev/null | sed -e 's/^.*"balanced":\(.*\),"rebalanceStatus.*$/\1/')
if [[ $status == "false" ]] ; then
    echo "[$TESTNAME] Cluster is not balanced after add"
    ret=1
fi

exit $ret






