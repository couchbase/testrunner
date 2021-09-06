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
if [[ -n $3 ]] && [[ $3 == 0 ]] ; then
    quiet=true
    test_params=" -p log_level=CRITICAL"
else
    test_params=""
fi
# this is specifically added to handle the gsi_type param for gsi integration test
echo "Printing $5"
echo $5
if [ ! -z "$5" -a "$5" != " " ]; then
    	echo "In here"
	extra_test_params=" -p makefile=True,$5"
	echo $extra_test_params
else
	extra_test_params="-p makefile=True"
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
if [ "${quiet}" = "true" ]
then
   make dataclean >> make.out 2>&1
   make >> make.out 2>&1
else
   make dataclean
   make
fi
COUCHBASE_NUM_VBUCKETS=64 python3 ./cluster_run --nodes=$servers_count &> $wd/cluster_run.log &
pid=$!
popd
python3 ./testrunner.py $conf -i $ini $test_params $extra_test_params 2>&1 | tee make_test.log

kill $pid
wait
tail -c 1073741824 $wd/cluster_run.log &> $wd/tmp.log
cp $wd/tmp.log $wd/cluster_run.log
rm $wd/tmp.log
! grep FAILED make_test.log &> /dev/null
