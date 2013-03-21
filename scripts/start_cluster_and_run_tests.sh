#!/bin/bash

if [[ -z $2 ]] ; then
    echo "$0 <ini> <conf>"
    exit
fi


# test if a number was passed instead of an ini file
if [ "$1" -eq "$1" ] 2>/dev/null; then
    numnodes=$1
    ini=make_test.ini

    echo "[global]
username:Administrator
password:asdasd

[membase]
rest_username:Administrator
rest_password:asdasd

[servers]" > ${ini}
    i=1
    while [[ $i -le $numnodes ]] ; do
        echo "$i:127.0.0.1_$i" >> ${ini}
        i=$((i+1))
    done
    i=1
    while [[ $i -le $numnodes ]] ; do
        echo "" >> ${ini}
        echo "[127.0.0.1_$i]" >> ${ini}
        echo "ip:127.0.0.1" >> ${ini}
        echo "port:$((9000+i-1))" >> ${ini}
        i=$((i+1))
    done
else
    ini=$1
fi

# test if a conf file or testcase was passed
if [[ -f $2 ]] ; then
    conf=" -c $2"
else
    conf=" -t $2"
fi

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
make dataclean
make
COUCHBASE_NUM_VBUCKETS=64 MAX_COUCH_REPS_PER_DOC=64 XDCR_FAILURE_RESTART_INTERVAL=1 XDCR_LATENCY_OPTIMIZATION=True python ./cluster_run --nodes=$servers_count &> $wd/cluster_run.log &
pid=$!
popd

python ./testrunner $conf -i $ini 2>&1 -p makefile=True | tee make_test.log

kill $pid
wait
tail -c 1G $wd/cluster_run.log &> $wd/tmp.log
cp $wd/tmp.log $wd/cluster_run.log
rm $wd/tmp.log
! grep FAILED make_test.log &> /dev/null
