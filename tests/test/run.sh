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

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		# this may seem weird, but if server is empty we don't
		# want to prepend our first entry with a comma.
		if [ -z "$SERVER" ]; then
			SERVER="$entry"
		else
			SERVER=$SERVER,$entry
		fi
	done
fi

echo "[$TESTNAME] Running test.pl -s $SERVER"

tests/$TESTNAME/test.pl -s $SERVER
