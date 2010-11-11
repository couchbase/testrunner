#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    KEYFILE
#    VERSION

if [[ -z "$VERSION" && -z "$MEMBASE_DIR" ]]; then
    echo "[$TESTNAME] VERSION is not set."
    exit 1
fi

if [[ -n "$MEMBASE_DIR" ]]; then
    echo "[$TESTNAME] VERSION not applicable, exiting with success."
    exit 0
fi

ret=0

for SERVER in $SERVERS ; do
    SERVER_IP=$(echo $SERVER | cut -f 1 -d ":")
    SERVER_MOXI=$(echo $SERVER | cut -f 3 -d ":")

    V=$(ssh -i $KEYFILE $SERVER_IP cat /opt/membase/*/VERSION.txt 2> /dev/null)
    echo "[$TESTNAME] $SERVER_IP"
    echo "[$TESTNAME] Version: $V"
    if [[ -z $V ]] ; then
        echo "[$TESTNAME] failed to get version"
        ret=1
    fi
    if ! echo $V | grep "$VERSION" &> /dev/null ; then
        echo "[$TESTNAME] version mismatch"
        ret=1
    fi
done

exit $ret
