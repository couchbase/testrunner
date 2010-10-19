#!/bin/bash
#
# Variables passed:
#    SERVERS
#    TESTNAME
#    KEYFILE
#    VERSION


ret=0

for S in $SERVERS ; do
    echo "[$TESTNAME] Checking set/get $S"

    echo -e "set key_$$ 0 0 1\na\r" | nc $S 11211 | tr -d '\015' | grep STORED &> /dev/null
    if [[ $? -ne 0 ]] ; then
        echo "[$TESTNAME] failed to set key key_$$"
        ret=1
    fi
    echo -e "get key_$$\r" | nc $S 11211 | grep VALUE &> /dev/null
    if [[ $? -ne 0 ]] ; then
        echo "[$TESTNAME] failed to get key key_$$"
        ret=1
    fi
done

exit $ret
