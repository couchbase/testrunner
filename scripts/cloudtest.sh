#!/bin/bash 
########################################################
# Description: Cloud Tests runtime
#
########################################################
command="$1"
if [ "$command" = "" ]; then
  echo "Usage $0: command"
  exit 1
fi

DATE_TIME="`date '+%m%d%y_%H%M%S'`"
REPO="testrunner-dbaas"
CURDIR=`pwd`
VIRTUAL_DIR=./virtualpy
INI_FILE=cbruntime.ini
BUCKET="cloudtest"

checkout()
{
  if [ ! -d ./testrunner-dbaas ]; then
     echo "*** Cloud Tests runtime: checkout workspace ***"
     git clone http://github.com/couchbaselabs/${REP}.git
  fi
}

check()
{
   echo "*** Cloud Tests runtime: check environment ***"
   if [ -f $VIRTUAL_DIR/bin/activate ]; then
      source $VIRTUAL_DIR/bin/activate
   fi
   PY=`python -V`
   which python python3
   echo $PY
   CB="`pip3 freeze |egrep couchbase`"
   echo $PY ${CB}
   pip3 freeze
   if [ "${CB}" = "" ]; then
     echo "WARNING: Couchbase Python SDK is not installed!"
   fi
   echo testrunner_client=${testrunner_client}
}

setuppython()
{
  if [ ! -d ${VIRTUAL_DIR} ]; then
    virtualenv -p python3 $VIRTUAL_DIR
    source $VIRTUAL_DIR/bin/activate
    check
    CB_SDK="$1"
    : ${CB_SDK:="2.5.12"}
    pip3 install couchbase==${CB_SDK}
    pip3 install sgmllib3k
    pip3 install paramiko
    pip3 install httplib2
    pip3 install pyyaml
    pip3 install Geohash
    pip3 install python-geohash
    pip3 install deepdiff
    pip3 install pyes
    pip3 install bs4
    pip3 install requests
  else
    echo "Python virtual env exists"
    source $VIRTUAL_DIR/bin/activate
    check
  fi
}

installini()
{
  INI_FILE=$1
  HOST="$3"
  OS_USERPWD="$4"
  CB_USERPWD="$5"
  SERVICES="$6"
  CB_USER="$7"
  CB_PORT="$8"
  : ${OS_USERPWD:="couchbase"}
  : ${CB_USERPWD:="password"}
  : ${CB_USER:="Administrator"}
  : ${SERVICES:="kv,index,n1ql,fts,cbas,eventing"}
  : ${CB_PORT:="8091"}
  if [ -f ${INI_FILE} ]; then
    rm ${INI_FILE}
  fi
  if [ ! -f ${INI_FILE} ]; then
    cat >> ${INI_FILE} <<EOL
[global] 
username:root
password:${OS_USERPWD}
port:${CB_PORT}

[membase]
rest_username:${CB_USER}
rest_password:${CB_USERPWD}
 
[servers]
1:_1
 
[_1]
ip:${HOST}
services:${SERVICES}
EOL
  fi
  cat ${INI_FILE}

}

install()
{
  echo "*** Cloud Tests runtime: Install couchbase cluster *** "
  version_number="$1"
  HOST="$2"
  OS_USERPWD="$3"
  CB_USERPWD="$4"
  CB_SERVICES="$5"
  : ${OS_USERPWD:="couchbase"}
  : ${CB_USERPWD:="password"}
  : ${SERVICES:="kv,index,n1ql,fts,cbas,eventing"}
  if [ "$CB_USERPWD" = "" ]; then
    echo "Usage $0 install version_number host root_userpwd cbadmin_userpwd"
    exit 1
  fi
  installini cbinstall.ini $@
  cd ${REPO}
  product="cb"
  setuppython
  echo python3 scripts/ssh.py -i $INI_FILE "iptables -F"
  python3 scripts/ssh.py -i $INI_FILE "iptables -F"
  echo python3 scripts/new_install.py -i $INI_FILE -p version=${version_number},product=${product},parallel=True
  python3 scripts/new_install.py -i $INI_FILE -p version=${version_number},product=${product},parallel=True
  cd ${CURDIR}
}

preq()
{
  echo "*** pre-requirements ***"
  HOST="`cat ${INI_FILE}|egrep ip|cut -f2 -d':'`"
  PORT="`cat ${INI_FILE}|egrep port|cut -f2 -d':'`"
  CB_USER="`cat ${INI_FILE}|egrep rest_username|cut -f2 -d':'`"
  CB_USERPWD="`cat ${INI_FILE}|egrep rest_password|cut -f2 -d':'`"
  curl -v -X POST -u ${CB_USER}:${CB_USERPWD} http://${HOST}:${PORT}/pools/${BUCKET} -d memoryQuota=900 -d ftsMemoryQuota=256
  java -Drun=connectClusterOnly,createBuckets -Dbucket=${BUCKET} -Durl=${HOST}  -jar ~/couchbaselabs/cbtest/target/cbtest-0.0.1-SNAPSHOT-jar-with-dependencies.jar

}
reset()
{
  echo "*** Reset ***"
  BUCKET="$1"
  HOST="`cat ${INI_FILE}|egrep ip|cut -f2 -d':'`"
  PORT="`cat ${INI_FILE}|egrep port|cut -f2 -d':'`"
  CB_USER="`cat ${INI_FILE}|egrep rest_username|cut -f2 -d':'`"
  CB_USERPWD="`cat ${INI_FILE}|egrep rest_password|cut -f2 -d':'`"
  java -Drun=connectClusterOnly,createBuckets -Dbucket=${BUCKET},src_bucket,dst_bucket,metadata -Doperation=drop -Durl=${HOST} -jar ~/couchbaselabs/cbtest/target/cbtest-0.0.1-SNAPSHOT-jar-with-dependencies.jar
  java -Drun=connectClusterOnly,createBuckets -Dbucket=${BUCKET} -Durl=${HOST}  -jar ~/couchbaselabs/cbtest/target/cbtest-0.0.1-SNAPSHOT-jar-with-dependencies.jar
}

runtimeini()
{ 
  INI_FILE=$CURDIR/$1
  HOST="`echo $2|cut -f2 -d':'`"
  CB_USER="$3"
  CB_USERPWD="$4"
  SERVICES="$5"
  CB_PORT="$6"
  : ${OS_USERPWD:="couchbase"}
  : ${CB_USERPWD:="password"}
  : ${CB_USER:="Administrator"}
  : ${SERVICES:="kv,index,n1ql,fts,cbas,eventing"}
  : ${CB_PORT:="18091"}
  if [ -f ${INI_FILE} ]; then
    rm ${INI_FILE}
  fi 
  if [ ! -f ${INI_FILE} ]; then
    cat >> ${INI_FILE} <<EOL
[global]
port:${CB_PORT}

[membase]
rest_username:${CB_USER}
rest_password:${CB_USERPWD}
 
[servers]
1:_1
 
[_1]
ip:${HOST}
services:${SERVICES}
EOL
  fi
  cat ${INI_FILE}

}

run()
{
  echo "*** Running tests ***"
  runtimeini cbruntime.ini $@
  export testrunner_client="python_sdk"
  setuppython
  cd ${REPO}
  echo time python3 testrunner.py -i ${INI_FILE} -c py-1node-sanity-cloud.conf -p skip_host_login=True,skip_init_check_cbserver=True,get-cbcollect-info=False,http_protocol=https,bucket_size=100,default_bucket_name=${BUCKET},use_sdk_client=True,skip_bucket_setup=True,skip_buckets_handle=True,is_secure=True,skip_setup_cleanup=True,servers_map=${2}
  time python3 testrunner.py -i ${INI_FILE} -c py-1node-sanity-cloud.conf -p skip_host_login=True,skip_init_check_cbserver=True,get-cbcollect-info=False,http_protocol=https,bucket_size=100,default_bucket_name=${BUCKET},use_sdk_client=True,skip_bucket_setup=True,skip_buckets_handle=True,is_secure=True,skip_setup_cleanup=True,servers_map=${2}  |tee ${CURDIR}/run_${DATETIME}.txt
  cd ${CURDIR}
}

help()
{
  echo " *** Cloud Tests runtime: help commands ***"
  echo checkout : test workspace
  echo check : environment
  echo setuppython : setup python lib
  echo preq : pre-requirements with min config
  echo reset : reset buckets
  echo install version host rootpwd cbpwd cbuser services : new cluster
  echo run host cbuser cbpwd services : tests
}

$@
