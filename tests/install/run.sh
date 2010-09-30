#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    VERSION
#    KEYFILE

# If VERSION isn't set, bail.

if [ -z "$KEYFILE" ]; then
	echo "[$TESTNAME] KEYFILE is not set."
	exit 1
fi

if [ -z "$VERSION" ]; then
	echo "[$TESTNAME] VERSION is not set."
	exit 1
fi

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

# zzzzzz
sleep 10 

# We sent install.pl to background because it can take forever, and this is a
# pain when installing to lots of servers. However, we don't want the script
# to exit early if installs are still going on. This is where it gets fun.

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		ps|grep ssh|grep $entry|grep $VERSION > /dev/null
		RUNNING=$?
		while [ "$RUNNING" -eq "0" ]; do
			sleep 5
			ps|grep ssh|grep $entry|grep $VERSION > /dev/null
			RUNNING=$?
		done
	done
fi

# So now we've verified that there are no more ssh processes running for any
# of our installations. This isn't a guarentee that everything is installed
# properly, so we should ssh into each server and verify it's installed
# and running.

# These tests were sometimes failing for no apparent reason, and I think it's
# because they needed more time to start up. 
sleep 15

RETCODE=0

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		# just checking the init script should be the same. of course, this only works on linux.
                ssh -i $KEYFILE root@$entry "[[ -f /var/run/membase-server.pid ]] && ps -p \$(cat /var/run/membase-server.pid) &> /dev/null"
		RET=$?
		if [ "$RET" -ne "0" ]; then
			echo "[$TESTNAME] server not running on $entry"
			RETCODE=1
		fi
	done
else
        ssh -i $KEYFILE root@$SERVER "[[ -f /var/run/membase-server.pid ]] && ps -p \$(cat /var/run/membase-server.pid) &> /dev/null"
	RET=$?
	if [ "$RET" -ne "0" ]; then
		echo "[$TESTNAME] server not running on $SERVER"
		RETCODE=1
	fi
fi

# sometimes we're running tests after install. If we run them too quickly, bad
# things happen.
sleep 5

exit $RETCODE
