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
python ./cluster_run --nodes=$servers_count &> $wd/cluster_run.log &
pid=$!
popd

if [[ -f $conf ]] ; then
    python ./testrunner -c $conf -i $ini 2>&1 -p makefile=True | tee make_test.log
else
    python ./testrunner -t $conf -i $ini 2>&1 -p makefile=True | tee make_test.log
fi
kill $pid
wait
! grep FAILED make_test.log &> /dev/null
