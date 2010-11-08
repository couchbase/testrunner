#!/bin/bash

ret=1

for x in SERVERS TESTNAME VERSION KEYFILE PYTHONPATH ; do
    echo -n "$x = "
    eval echo \$$x
done

exit $ret