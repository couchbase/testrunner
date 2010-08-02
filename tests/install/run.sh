#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    VERSION

# If VERSION isn't set, bail.

if [ -z "$VERSION" ]; then
	echo "[$TESTNAME] VERSION is not set."
	exit 1
fi

export SSHKEY="~/.ssh/ustest20090719.pem"

# if SERVER isn't set, make a comma delimited list out of our server file

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		echo "[$TESTNAME] Running install.pl -s $entry"
		tests/$TESTNAME/install.pl -s $entry &
	done
	SERVER=""
else
	echo "[$TESTNAME] Running install.pl -s $SERVER"
	tests/$TESTNAME/install.pl -s $SERVER 
fi

# We sent install.pl to background because it can take forever, and this is a
# pain when installing to lots of servers. However, we don't want the script
# to exit early if installs are still going on. This is where it gets fun.

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		ps|grep ssh|grep $entry|grep $VERSION > /dev/null
		RUNNING=$?
		while [ "$RUNNING" -eq "1" ]; do
			sleep 5
			ps|grep ssh|grep $entry|grep $VERSION > /dev/null
			RUNNING=$?
		done
	done
fi
