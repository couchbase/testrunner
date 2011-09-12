#!/bin/bash

if [[ -z $2 ]] ; then
    echo "$0 <ini> <conf>"
    exit
fi

ini=$1
conf=$2

servers_count=0
while read line ; do
    if $(echo "$line" | grep "^\[.*\]$" &> /dev/null) ; then
        section="$line"
        read line
    fi
    if [[ "$section" == "[servers]" && "$line" != "" ]] ; then
        servers_count=$((servers_count + 1))
    fi        
done < $ini

wd=$(pwd)
pushd .
cd ../ns_server
./cluster_run --nodes=$servers_count &> $wd/cluster_run.log &
pid=$!
popd

if [[ -f $conf ]] ; then
    ./testrunner -c $conf -i $ini 2>&1 | tee make_test.log
else
    ./testrunner -t $conf -i $ini 2>&1 | tee make_test.log
fi
kill $pid
wait
! grep FAILED make_test.log &> /dev/null
