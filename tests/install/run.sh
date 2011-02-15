#!/bin/sh
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    VERSION
#    KEYFILE

# If VERSION isn't set, bail.

if [[ -z "$VERSION" && -z "$MEMBASE_DIR" ]]; then
    echo "[$TESTNAME] VERSION or MEMBASE_DIR is not set."
    exit 1
fi

for SERVER in $SERVERS ; do
    echo "[$TESTNAME] Running install -s $SERVER"
    tests/$TESTNAME/install -s $SERVER -v "$VERSION" -b "$MEMBASE_DIR" -k "$KEYFILE" -l "$BUILDPATH" -x "$BUILDSERVER" &
done
wait

# So now we've verified that there are no more ssh processes running for any
# of our installations. This isn't a guarentee that everything is installed
# properly, so we should ssh into each server and verify it's installed
# and running.

RETCODE=0

for SERVER in $SERVERS ; do
    SERVER_IP=$(echo $SERVER | cut -f 1 -d ":")
    MOXI_IP=$(echo $SERVER | cut -f 3 -d ":")

    echo "stats" | nc $SERVER_IP $MOXI_IP | grep uptime &> /dev/null

    if [[ $? -ne 0 ]] ; then
        echo "[$TESTNAME] server not running on $SERVER"
        RETCODE=1
    fi
done

# sometimes we're running tests after install. If we run them too quickly, bad
# things happen.
sleep 5

exit $RETCODE
