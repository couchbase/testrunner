#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    KEYFILE
#    VERSION

if [ -z "$VERSION" ]; then
    echo "[$TESTNAME] VERSION is not set."
    exit 1
fi

ret=0

for S in $SERVERS ; do
    V=$(ssh -i $KEYFILE $S cat /opt/membase/*/VERSION.txt 2> /dev/null)
    echo "[$TESTNAME] $S"
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
