#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    KEYFILE
#    VERSION

ret=0

for SERVER in $(echo $SERVERS) ; do
    SERVER_IP=$(echo $SERVER | cut -f 1 -d ":")
    SERVER_MOXI=$(echo $SERVER | cut -f 3 -d ":")
    echo "[$TESTNAME] $SERVER_IP"
    res=$(memcapable -h $SERVER_IP -p $SERVER_MOXI 2>&1 | grep -v -e setq -e flushq -e addq -e replaceq -e deleteq)
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