#!/bin/sh
#
# Variables passed:
#    SERVERS
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

for SERVER in $SERVERS ; do
    echo "[$TESTNAME] Running install.pl -s $SERVER"
    tests/$TESTNAME/install.pl -s $SERVER &
done
wait

# So now we've verified that there are no more ssh processes running for any
# of our installations. This isn't a guarentee that everything is installed
# properly, so we should ssh into each server and verify it's installed
# and running.

RETCODE=0

for SERVER in $SERVERS ; do
    ssh -i $KEYFILE root@$SERVER "ps -ef | grep -v grep | grep memcached &> /dev/null" 2> /dev/null
    if [[ $? -ne 0 ]] ; then
        echo "[$TESTNAME] server not running on $SERVER"
        echo "[$TESTNAME] $SERVER: $(echo "stats" | nc $SERVER 11211 | grep uptime)"
        RETCODE=1
    fi
done

# sometimes we're running tests after install. If we run them too quickly, bad
# things happen.
sleep 5

exit $RETCODE
