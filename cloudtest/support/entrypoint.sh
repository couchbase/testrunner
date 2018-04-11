#!/bin/bash

numOfNodes=$1
currentNamespace=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
nodeConfigName="${numOfNodes}node"
declare -a podIpArray

if [ "$numOfNodes" == "" ]; then
    echo "Exiting: Number of nodes missing"
    exit 1
fi

# Start required services #
echo 'root:couchbase' | chpasswd
sed -i 's/PermitRootLogin .*\$/PermitRootLogin yes/' /etc/ssh/sshd_config
sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
service ssh stop
service ssh start

# Install Python libraries for Kubernetes #
git clone --recursive https://github.com/kubernetes-client/python.git
cd python
python setup.py install
cd ..

# Manipulate IPs in node.ini file #
index=0
for podIp in $(python getNodeIps.py $currentNamespace | grep "cb-example" | awk '{print $2}')
do
    if [ "$podIp" != "" ]
    then
        podIpArray+=($podIp)
    fi
done

if [ ${#podIpArray[@]} -ne $numOfNodes ]
then
    echo "Abort: IPs are less than expected pods"
    exit 1
fi

sed -i "s/ip:.*$/ip:/" ${nodeConfigName}.ini
for index in ${!podIpArray[@]}
do
    occurence=$(expr $index + 1)
    if [ $index -eq 0 ]
    then
        tr '\n' '^' < ${nodeConfigName}.ini | sed "s/ip:/ip:${podIpArray[$index]}/$occurence" | tr '^' '\n' > ${nodeConfigName}.ini.$occurence
        rm -f ${nodeConfigName}.ini
    else
        tr '\n' '^' < ${nodeConfigName}.ini.$index | sed "s/ip:/ip:${podIpArray[$index]}/$occurence" | tr '^' '\n' > ${nodeConfigName}.ini.$occurence
        rm -f ${nodeConfigName}.ini.$index
    fi
done
mv ${nodeConfigName}.ini.$numOfNodes ${nodeConfigName}.ini

# Print current testrunner branch
echo "Git Branch details:"
git branch
echo ""

echo "Git head info"
git log -n 1
echo ""

# Start Testrunner code #
python ./testrunner.py -i ./${nodeConfigName}.ini -c ./testcases.conf -p get-logs=true,get-cbcollect-info=true

echo "Testrunner: command completed"
while true; do sleep 1000; done

