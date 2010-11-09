#!/bin/bash

ret=0

for x in SERVERS VERSION KEYFILE MEMBASE_DIR REST_USER REST_PASSWORD MEMBASE_CLI ; do
    echo -n "$x = "
    eval echo \$$x
done

exit $ret