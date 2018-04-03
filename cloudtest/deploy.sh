#!/bin/bash

function showHelp() {
    echo ""
    echo " Arguments:"
    echo "  --namespace [string]  Kubernetes namespace to use"
    echo "  --nodes [n]           Number of nodes to run"
    echo "  --cbversion [string]  Couchbase server version to use"
    echo "  --dockerhub [string]  Dockerhub account to use. Default is 'couchbase'"
    echo ""
    exit 1
}

function validateArgs() {
    errCondition=false
    if [[ -z "$KUBENAMESPACE" ]]
    then
        echo "Exiting: KUBENAMESPACE not defined"
        errCondition=true
    fi

    if $errCondition
    then
        showHelp
    fi
}

function exitOnError() {
    if [ $1 -ne 0 ]
    then
        echo "Abort: $2"
        exit 1
    fi
}

function downloadClusterYamlFiles() {
    wget https://packages.couchbase.com/kubernetes/beta/couchbase-cluster.yaml
    wget https://packages.couchbase.com/kubernetes/beta/secret.yaml
    wget https://packages.couchbase.com/kubernetes/beta/operator.yaml
}

function checkForClusterYamlFileExists() {
    for fileName in $deploymentFile $secretFile $cbClusterFile
    do
        if [ ! -f "$fileName" ]
        then
            echo "Exiting: File '$fileName' not found!"
            exit 1
        fi
    done
}

function editClusterYamlFiles() {
    sed -i '/version:/{a\
        \ \ paused: false
        }' couchbase-cluster.yaml
    exitOnError $? "Unable to append version string in cbcluster yaml"

    sed -i "/name: couchbase-operator/{a\
        \ \ namespace: $KUBENAMESPACE
        }" operator.yaml
    exitOnError $? "Unable to append namespace string in cbcluster yaml"
}

function clearK8SCluster() {
    echo "Using name space '$KUBENAMESPACE'"
    kubectl delete deployment --namespace=$KUBENAMESPACE --all
    kubectl delete replicaset --namespace=$KUBENAMESPACE --all
    kubectl delete service -l app=couchbase --namespace=$KUBENAMESPACE
    kubectl delete job --all --namespace=$KUBENAMESPACE
    kubectl delete pods --namespace=$KUBENAMESPACE -l name=couchbase-operator
    kubectl delete pods --namespace=$KUBENAMESPACE -l name=testrunner
    kubectl delete pods --namespace=$KUBENAMESPACE -l app=couchbase
    kubectl delete --all couchbaseclusters --namespace=$KUBENAMESPACE
    kubectl delete secrets basic-test-secret --namespace=$KUBENAMESPACE
    
    while [ true ]
    do
        if [ `kubectl --namespace=$KUBENAMESPACE get po | grep "NAME" | wc -l | awk '{print $1}'` -eq 0 ]
        then
            break
        fi
        sleep 5
    done
}

function checkForPodsReady () {
    reqPodNum=$1
    clusterPrefix=$2
    podsReady=false

    for i in {1..60}
    do
        if [ `kubectl --namespace=$KUBENAMESPACE get po | grep "cb-example" | grep Running | wc -l` -eq $reqPodNum ]
        then
            podsReady=true
            break
        fi
        sleep 5
    done

    if [ "$podsReady" == "false" ]
    then
        echo "Abort: Pods not ready for 5 mins"
        exit 1
    fi
}

function pushDockerImage() {
    dockerImageName=$1
    tarFileName=$2

    baseName=$(echo $dockerImageName | cut -d":" -f 1)
    tagName=$(echo $dockerImageName | cut -d":" -f 2)
    dockerImageId=`docker image ls | grep "$baseName" | grep "$tagName" | awk '{print $3}'`
    echo "Created docker image '$baseName:$tagName' with id '$dockerImageId'"

    docker tag $dockerImageId $baseName:$tagName
    docker push $dockerImageName
    exitOnError $? "Unable to push docker image '$baseName:$tagName' '$dockerImageId' to dockerhub"

    if [ "$tarFileName" != "" ]
    then
        docker save -o ./$tarFileName $dockerImageName
        exitOnError $? "Unable to save docker image '$baseName:$tagName' '$dockerImageId' to '$tarFileName' locally"
    fi

    unset baseName tagName dockerImageId
}

function createTestrunnerDockerfile() {
    dockerFileString=""
    dockerFileString+="FROM ubuntu:15.04\n"
    dockerFileString+="RUN apt-get update\n"
    dockerFileString+="RUN apt-get install -y gcc g++ make cmake git-core libevent-dev libev-dev libssl-dev libffi-dev psmisc iptables zip unzip python-dev python-pip vim curl\n"
    dockerFileString+="# build libcouchbase\n"
    dockerFileString+="RUN git clone git://github.com/couchbase/libcouchbase.git && mkdir libcouchbase/build\n"
    dockerFileString+="\n"
    dockerFileString+="WORKDIR libcouchbase/build\n"
    dockerFileString+="RUN ../cmake/configure --prefix=/usr && make && make install\n"
    dockerFileString+="\n"
    dockerFileString+="WORKDIR /\n"
    dockerFileString+="RUN git clone git://github.com/couchbase/testrunner.git\n"
    dockerFileString+="WORKDIR testrunner\n"
    dockerFileString+="ARG BRANCH=master\n"
    dockerFileString+="RUN git checkout \$BRANCH\n"
    dockerFileString+="\n"
    dockerFileString+="# install python deps\n"
    dockerFileString+="RUN pip2 install --upgrade packaging appdirs\n"
    dockerFileString+="RUN pip install -U pip setuptools\n"
    dockerFileString+="RUN pip install paramiko && pip install gevent && pip install boto && pip install httplib2 && pip install pyyaml && pip install couchbase\n"
    dockerFileString+="\n"
    dockerFileString+="COPY getNodeIps.py getNodeIps.py\n"
    dockerFileString+="COPY entrypoint.sh entrypoint.sh\n"
    dockerFileString+="COPY 4node.ini 4node.ini\n"
    dockerFileString+="COPY testcases.conf testcases.conf\n"
    dockerFileString+="RUN chmod +x ./entrypoint.sh\n"
    dockerFileString+="ENTRYPOINT [\"./entrypoint.sh\", \"$numOfNodes\"]\n"

    printf "$dockerFileString" > $testrunnerDir/Dockerfile
}

function createNodeIniFile() {
    nodeIniFileString=""
    nodeIniFileString+="[global]\n"
    nodeIniFileString+="  port:8091\n"
    nodeIniFileString+="  username:root\n"
    nodeIniFileString+="  password:couchbase\n"
    nodeIniFileString+="  index_port:9102\n"
    nodeIniFileString+="  n1ql_port:18903\n"
    nodeIniFileString+="\n"

    nodeIniFileString+="[servers]\n"
    for index in {1..$numOfNodes}
    do
       nodeIniFileString+="  $index:vm$index\n"
    done
    nodeIniFileString+="\n"

    for index in {1..$numOfNodes}
    do
        nodeIniFileString+="[vm$index]\n"
        nodeIniFileString+="  ip:172.17.1.2\n"
        nodeIniFileString+="  services=n1ql,kv,index\n"
    done
    nodeIniFileString+="\n"

    nodeIniFileString+="[membase]\n"
    nodeIniFileString+="  rest_username:Administrator\n"
    nodeIniFileString+="  rest_password:password\n"

    printf "$nodeIniFileString" > $testrunnerDir/${numOfNodes}node.ini
}

function copyFilesForTestRunnerImage() {
    createTestrunnerDockerfile
    createNodeIniFile

    # Create node config files #
    mkdir ${numOfNodes}node
    for fileName in $testrunnerDir/Dockerfile $testrunnerDir/${numOfNodes}node.ini $testrunnerDir/testcases.conf $testrunnerDir/entrypoint.sh $testrunnerDir/getNodeIps.py
    do
        if [ ! -f "$fileName" ]; then
            echo "Exiting: File '$fileName' not found!"
            cd ..
            rm -rf ${numOfNodes}node DockerFile
            exit 1
        fi
        cp $fileName ${numOfNodes}node
    done
}

function buildCbServerDockerImage() {
    dockerFileString="FROM couchbase/server:enterprise-${serverVersion}\n"
    dockerFileString="${dockerFileString}MAINTAINER Couchbase Docker Team <docker@couchbase.com>\n"
    dockerFileString="${dockerFileString}RUN apt update\n"
    dockerFileString="${dockerFileString}RUN apt install openssh-client openssh-server -y\n"
    dockerFileString="${dockerFileString}RUN mkdir /var/run/sshd\n"
    dockerFileString="${dockerFileString}RUN echo 'root:couchbase' | chpasswd\n"
    dockerFileString="${dockerFileString}RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config\n"
    dockerFileString="${dockerFileString}# SSH login fix. Otherwise user is kicked off after login\n"
    dockerFileString="${dockerFileString}RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd\n"
    dockerFileString="${dockerFileString}RUN echo 'export VISIBLE=now' >> /etc/profile\n"
    dockerFileString="${dockerFileString}RUN /usr/sbin/sshd -D &\n"
    dockerFileString="${dockerFileString}EXPOSE 8091 8092 8093 8094 9100 9101 9102 9103 9104 9105 9998 9999 11207 11210 11211 18091 18092 22\n"

    printf "$dockerFileString" > Dockerfile
    docker build . -t $cbServerDockerImageName
    exitOnError $? "Unable to create docker image"
    #pushDockerImage $cbServerDockerImageName
}

function buildTestRunnerImage() {
    copyFilesForTestRunnerImage

    # Build testrunner docker file #
    cd ${numOfNodes}node
    docker build . -t $testRunnerDockerImageName
    exitOnError $? "Failed to build testrunner docker image"
    #pushDockerImage $testRunnerDockerImageName
    cd ..
}

function deployCluster() {
    imageName=$(echo $cbServerDockerImageName | cut -d':' -f 1)
    tagName=$(echo $cbServerDockerImageName | cut -d':' -f 2)

    sed -i "s/paused: true/paused: false/" $cbClusterFile
    exitOnError $? "Unable to replace pause string in cbcluster yaml"

    sed -i "s#baseImage:.*\$#baseImage: $imageName#" $cbClusterFile
    exitOnError $? "Unable to replace baseImage string in cbcluster yaml"

    sed -i "s/version:.*$/version: $tagName/" $cbClusterFile
    exitOnError $? "Unable to replace version string in cbcluster yaml"

    sed -i "s/size:.*$/size: $numOfNodes/" $cbClusterFile
    exitOnError $? "Unable to replace size in cbcluster yaml"

    unset imageName tagName

    kubectl --namespace=$KUBENAMESPACE create -f $deploymentFile
    kubectl --namespace=$KUBENAMESPACE create -f $secretFile
    kubectl --namespace=$KUBENAMESPACE create -f $cbClusterFile
    checkForPodsReady $numOfNodes "$clusterName"
}

function pauseCbOperator() {
    sed -i "s/paused: false/paused: true/g" $cbClusterFile
    exitOnError $? "Unable to replace string in cbcluster yaml"

    kubectl --namespace=$KUBENAMESPACE apply -f $cbClusterFile
    exitOnError $? "Unable to pause the cbcluster"
}

function createTestRunnerYamlFile() {
    fileString=""
    fileString+="---\n"
    fileString+="apiVersion: batch/v1\n"
    fileString+="kind: Job\n"
    fileString+="metadata:\n"
    fileString+="  name: testrunner\n"
    fileString+="spec:\n"
    fileString+="  template:\n"
    fileString+="    metadata:\n"
    fileString+="      name: testrunner\n"
    fileString+="    spec:\n"
    fileString+="      containers:\n"
    fileString+="      - name: testrunner\n"
    fileString+="        image: $testRunnerDockerImageName\n"
    fileString+="      restartPolicy: Never\n"
    printf "$fileString" > $testRunnerYamlFileName

    echo "$testRunnerYamlFileName file content:"
    cat $testRunnerYamlFileName
}

# Variable declaration and parsing argument#
dockerHubAccount="couchbase"
while [ $# -ne 0 ]
do
    case "$1" in
        "--namespace")
            KUBENAMESPACE=$2
            shift ; shift
            ;;
        "--nodes")
            numOfNodes=$2
            re='^[0-9]+$'
            if ! [[ $numOfNodes =~ $re ]] ; then
               echo "Exiting: Invalid '$1' value. Should be an integer"
               showHelp
            fi
            shift ; shift
            ;;
        "--cbversion")
            serverVersion=$2
            shift ; shift
            ;;
        "--dockerhub")
            dockerHubAccount=$2
            shift ; shift
            ;;
        *)
            echo "Exiting: Invalid argument '$1'"
            showHelp
    esac
done

validateArgs

deploymentFile="operator.yaml"
secretFile="secret.yaml"
cbClusterFile="couchbase-cluster.yaml"
testrunnerDir="testrunner_files"
testRunnerYamlFileName="testrunner.yaml"
clusterName=$(grep "name:" $cbClusterFile | head -1 | xargs | cut -d' ' -f 2)

cbServerDockerImageName="${dockerHubAccount}/couchbase-server:custom-${serverVersion}"
testRunnerDockerImageName="${dockerHubAccount}/testrunner-kubernetes:${numOfNodes}node"

declare -a podIpArray

# Build required images #
#buildCbServerDockerImage
clearK8SCluster
deployCluster
pauseCbOperator
buildTestRunnerImage

createTestRunnerYamlFile
kubectl --namespace=$KUBENAMESPACE create -f $testRunnerYamlFileName
exitOnError $? "Unable to start testrunner container"

rm -rf ${numOfNodes}node DockerFile

exit $?

