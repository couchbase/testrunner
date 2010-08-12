#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    KEYFILE

RETVAL=0

for entry in `cat $SERVERFILE`; do
	if [ -z "$SERVER" ]; then
		SERVER=$entry
	else
		echo "[$TESTNAME] Adding $entry to cluster on $SERVER"
		tests/$TESTNAME/server-add.pl $SERVER $entry
		if [ $? -eq 1 ]; then
			RETVAL=1
		fi
	fi
done


exit $RETVAL
