#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    KEYFILE

# we're only going to fail over *one* server.

for entry in `cat $SERVERFILE`; do
	if [ -z "$SERVER" ]; then
		SERVER=$entry
	else
		echo "[$TESTNAME] Removing $entry from cluster on $SERVER"
		tests/$TESTNAME/server-failover.pl $SERVER $entry
		if [ $? -eq 1 ]; then
			exit 1
		fi
		exit 0
	fi
done
