#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    KEYFILE
#    VERSION

ret=0

for S in $(echo $SERVERS) ; do
    echo "[$TESTNAME] $S"
    res=$(memcapable -h $S -p 11211 2>&1 | grep -v -e setq -e flushq -e addq -e replaceq -e deleteq)
    echo "$res" | grep FAIL
    if [[ $? -eq 0 ]] ; then
        ret=1
    fi
    echo "$res" | grep pass &> /dev/null
    if [[ $? -eq 1 ]] ; then
        echo "[$TESTNAME] Unable to connect to server"
        ret=1
    fi
done

exit $ret