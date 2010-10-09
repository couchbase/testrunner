#!/bin/bash
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    KEYFILE
#    VERSION

# if a test requires only one server use the first one.
#
# if [ -z "$SERVER" ]; then
#         SERVER=`head -1 "$SERVERFILE"`
# fi

# if SERVER isn't set, make a comma delimited list out of our server file

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		# this may seem weird, but if server is empty we don't
		# want to prepend our first entry with a comma.
		if [ -z "$SERVER" ]; then
			SERVER="$entry"
		else
			SERVER="$SERVER $entry"
		fi
	done
fi

ret=0

for S in $(echo $SERVER) ; do
    V=$(ssh -i $KEYFILE $S cat /opt/membase/*/VERSION.txt 2> /dev/null)
    echo "[$TESTNAME] $S"
    echo "[$TESTNAME] Version: $V"
    if [[ -z $V ]] ; then
	echo "[$TESTNAME] failed to get version"
	ret=1
    fi
    if ! echo $V | grep "$VERSION-" &> /dev/null ; then
	echo "[$TESTNAME] version mismatch"
	ret=0
    fi

    echo "[$TESTNAME] Checking set/get $S"

    echo -e "set key_$$ 0 0 1\na\r" | nc $S 11211 | tr -d '\015' | grep STORED &> /dev/null
    if [[ $? -ne 0 ]] ; then
	echo "[$TESTNAME] failed to set key key_$$"
	ret=1
    fi
    echo -e "get key_$$\r" | nc $S 11211 | grep VALUE &> /dev/null
    if [[ $? -ne 0 ]] ; then
	echo "[$TESTNAME] failed to get key key_$$"
	ret=1
    fi
done

exit $ret