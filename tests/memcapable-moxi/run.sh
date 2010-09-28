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
    echo "[$TESTNAME] $S"
    res=$(memcapable -h $S -p 11211 2>&1 | grep -v -e setq -e flushq -e addq -e replaceq -e deleteq)
    echo "$res" | grep FAIL
    if [[ $? -eq 0 ]] ; then
        ret=1
    fi
    echo "$res" | grep pass &> /dev/null
    if [[ $? -eq 1 ]] ; then
        echo "[$TESTNAME] Unable to connect to server"
        ret=1
    fi
done

exit $ret