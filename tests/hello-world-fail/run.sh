#!/bin/bash

ret=1

for x in SERVER TESTNAME VERSION KEYFILE PYTHONPATH ; do
    echo -n "$x = "
    eval echo \$$x
done

exit $ret