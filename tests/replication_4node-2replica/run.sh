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

NODECOUNT=4
REPCOUNT=2

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

echo "[$TESTNAME] Running vbucket_check.py -s \"$SERVER\" -r $REPCOUNT -u Administrator -p password -m membase-server_x86_$VERSION.rpm"

python bin/vbucket_check.py -s "$SERVER" -r $REPCOUNT -u Administrator -p password -m membase-server_x86_$VERSION.rpm
