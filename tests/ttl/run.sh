#!/bin/bash
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

NODECOUNT=1

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

function wait_for_persist {
    local stat="ep_total_persisted"
    local ip=$SERVER
    local port=11211
    local count=$1
    local tstart=$(date +%s)
    local tend=0
    local statcount=-1

    while [[ $statcount -lt $count ]] ; do
	statcount=$(echo "stats" | nc $ip $port | awk '$2 ~ /^'$stat'$/{print $3}' | tr -d '\015')
	if [[ -z $statcount ]] ; then
	    statcount=-1
	fi

	if [[ $statcount -lt $count ]] ; then
	    sleep 1
	fi
    done
    tend=$(date +%s)
    echo "$stat: $statcount, $((tend-tstart))s"
}


echo "[$TESTNAME] $SERVER"

ttl=60

curtime=$(date +%s)
exptime=$((curtime + ttl))

pers=$(echo stats | nc $SERVER 11211 | grep ep_total_persisted | cut -f 3 -d " " | tr -d '\015')

start=$(date +%s)
echo -e "set key_$$ 0 $ttl 1\na\r" | nc $SERVER 11211 | tr -d '\015' | grep STORED &> /dev/null
if [[ $? -ne 0 ]] ; then
    echo "[$TESTNAME] failed to set key"
    ret=1
    exit $ret
fi
wait_for_persist $((pers+1)) &> /dev/null
end=$(date +%s)
echo "[$TESTNAME] took $((end-start)) seconds to persist, $((ttl-(end-start))) seconds left of the timeout"
echo -e "get key_$$\r" | nc $SERVER 11211 | grep VALUE &> /dev/null
if [[ $? -ne 0 ]] ; then
    echo "[$TESTNAME] failed to get key before restart"
    ret=1
    exit $ret
fi
echo "[$TESTNAME] stopping server on $SERVER"
ssh $SERVER service northscale-server stop &> /dev/null
curtime=$(date +%s)
sleep $(((exptime - curtime) - (ttl/2)))
echo "[$TESTNAME] starting server on $SERVER"
ssh $SERVER service northscale-server restart &> /dev/null
curtime=$(date +%s)
echo "[$TESTNAME] we have $((exptime-curtime)) seconds left of the timeout"
curtime=$(date +%s)
sleep $((exptime-curtime-5))
echo -e "get key_$$\r" | nc $SERVER 11211 | grep VALUE &> /dev/null
if [[ $? -ne 0 ]] ; then
    echo "[$TESTNAME] failed to get key before timeout"
    ret=1
fi

sleep 10
echo -e "get key_$$\r" | nc $SERVER 11211 | grep VALUE &> /dev/null
if [[ $? -eq 0 ]] ; then
    echo "[$TESTNAME] key still exists after timeout"
    ret=1
fi

exit $ret