#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    KEYFILE
#    VERSION


ret=0

for SERVER in $SERVERS ; do
    SERVER_IP=$(echo $SERVER | cut -f 1 -d ":")
    SERVER_MOXI=$(echo $SERVER | cut -f 3 -d ":")
    echo "[$TESTNAME] Checking set/get key_$$"

    echo -e "set key_$$ 123 0 1\r\na\r" | nc $SERVER_IP $SERVER_MOXI | tr -d '\015' | grep STORED &> /dev/null
    if [[ $? -ne 0 ]] ; then
        echo "[$TESTNAME] failed to set key key_$$"
        ret=1
    fi
    echo -e "get key_$$\r" | nc $SERVER_IP $SERVER_MOXI | grep VALUE &> /dev/null
    if [[ $? -ne 0 ]] ; then
        echo "[$TESTNAME] failed to get key key_$$"
        ret=1
    fi
    echo -e "get key_$$\r" | nc $SERVER_IP $SERVER_MOXI | grep 123 &> /dev/null
    if [[ $? -ne 0 ]] ; then
        echo "[$TESTNAME] failed to get key key_$$ flags"
        ret=1
    fi
done

exit $ret
