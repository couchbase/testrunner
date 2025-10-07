#!/bin/bash

support_ver="6.5"
small_ver=${version_number:0:3}
host_ip=$(hostname -I | awk '{print $1}')
echo $host_ip
rerun_job=true
py_executable=python3

# Block was prev. in a separate shell block before
echo Desc: $descriptor
if [[ ${component} == "backup_recovery"  || ${component} == "xdcr" ]]; then
  echo "Forcefully setting branch and testrunner_tag=master"
  branch=master
  testrunner_tag=master
fi

git checkout ${branch}
git pull origin ${branch}

touch rerun_props_file
if [ ${fresh_run} == false ]; then
  ${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --manual_run
fi
export `cat rerun_props_file`
# End of block

echo "Set ALLOW_HTP to False so test could run."
sed -i 's/ALLOW_HTP.*/ALLOW_HTP = False/' lib/testconstants.py

###### Added on 22/March/2018 to fix auto merge failures. Please revert this if this does not work.
git fetch
git reset --hard origin/${branch}
######

git submodule init
git submodule update --init --force --remote

## cherrypick the gerrit request if it was defined
if [ "$cherrypick" != "None" ]; then
   sh -c "$cherrypick"
fi

set +e
echo newState=available>propfile
newState=available

majorRelease=`echo ${version_number} | awk '{print substr($0,1,1)}'`
echo the major release is $majorRelease
echo ${servers}

${py_executable} -m easy_install httplib2 ground hypothesis_geometry

if [ -f /etc/redhat-release ]; then
  echo 'centos'
  yum install -y docker
  # CBQE-6231
  if [ -f /usr/bin/systemctl ]; then
    systemctl start docker
  else
    service docker start
  fi
  docker pull docker.io/jamesdbloom/mockserver
fi

if [ -f /etc/lsb-release ]; then
  echo 'ubuntu'
  apt-install docker
  service docker start
  docker pull docker.io/jamesdbloom/mockserver
fi

UPDATE_INI_VALUES=""
if [ ! "${username}" = "" ]; then
  UPDATE_INI_VALUES='"username":"'${username}'"'
fi
if [ ! "${password}" = "" ]; then
  if [ "${UPDATE_INI_VALUES}" = "" ]; then
    UPDATE_INI_VALUES='"password":"'${password}'"'
  else
    UPDATE_INI_VALUES=`echo ${UPDATE_INI_VALUES}',"password":"'${password}'"'`
  fi
fi

# Fix for the ini format issue where both : and = used
sed 's/=/:/' ${iniFile} >/tmp/testexec_reformat.$$.ini
cat /tmp/testexec_reformat.$$.ini

# To make sure the files exists
touch /tmp/testexec_reformat.$$.ini /tmp/testexec.$$.ini

if [[ "${slave}" = "bhive_slave_test" ]]; then
  echo "Running on bhive_slave_test"
  export PATH=/opt/maven/bin:/opt/java-8/bin:/root/.pyenv/shims:/root/.pyenv/bin:$PATH
  echo "PATH is: $PATH"
  cd magma_loader/DocLoader
  mvn clean install
  cd ../..
  touch $WORKSPACE/testexec.$$.ini
  set -x
  docker run --rm \
    -v /tmp/testexec_reformat.$$.ini:/testrunner/testexec_reformat.$$.ini \
    -v /tmp/testexec.$$.ini:/testrunner/testexec.$$.ini  \
    testrunner:install python3 scripts/populateIni.py $skip_mem_info \
    -s ${servers} $internal_servers_param \
    -d ${addPoolServerId} \
    -a ${addPoolServers} \
    -i testexec_reformat.$$.ini \
    -p ${os} \
    -o testexec.$$.ini \
    -k '{'${UPDATE_INI_VALUES}'}'
    set +x
else
  if [ "${small_ver}" = "7.0" ]; then
    echo ${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i /tmp/testexec_reformat.$$.ini -p ${os} -o /tmp/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
    ${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i /tmp/testexec_reformat.$$.ini -p ${os} -o /tmp/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
    #${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i ${iniFile} -p ${os} -o /tmp/testexec.$$.ini
  else
    echo ${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i /tmp/testexec_reformat.$$.ini -p ${os} -o /tmp/testexec.$$.ini
    ${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i /tmp/testexec_reformat.$$.ini -p ${os} -o /tmp/testexec.$$.ini
  fi
fi

if [ "$os" = "windows" ]; then
   echo "Have Windows,"
   parallel=true   # serial worked even worse but may come back to is
 else
   parallel=true
fi

if [ "$component" = "xdcr" ]; then
   installParameters='init_clusters=True'
fi

if [ "$installParameters" = "None" ]; then
   extraInstall=''
else
   extraInstall=,$installParameters
fi

echo extra install is $extraInstall
timedatectl
status=0

if [ "$os" = "windows" ] || [ $(${py_executable} -c "print($small_ver < $support_ver)") = True ]; then
	${py_executable} scripts/install.py -i /tmp/testexec.$$.ini -p version=${version_number},product=cb,parallel=${parallel},init_nodes=${initNodes},url=${url}${extraInstall}
else
    #To handle nonroot user
    echo sed 's/nonroot/root/g' /tmp/testexec.$$.ini > /tmp/testexec_root.$$.ini
    sed 's/nonroot/root/g' /tmp/testexec.$$.ini > /tmp/testexec_root.$$.ini
    echo ${py_executable} scripts/ssh.py -i /tmp/testexec_root.$$.ini "iptables -F"
    ${py_executable} scripts/ssh.py -i /tmp/testexec_root.$$.ini "iptables -F"

    if [ "${INSTALL_TIMEOUT}" = "" ]; then
       INSTALL_TIMEOUT="1200"
    fi
    # 6.5.x has install issue. Reverting to older style
    if [ "${SKIP_LOCAL_DOWNLOAD}" = "" ]; then
       SKIP_LOCAL_DOWNLOAD="False"
    fi

    if [ ! "$majorRelease" = "7" ]; then
       SKIP_LOCAL_DOWNLOAD="True"
    fi

    echo "Starting server installation"
    if [[ "${slave}" = "bhive_slave_test" ]]; then
      echo "Starting server installation"
      set -x
      docker run --rm \
        -v /tmp/testexec.$$.ini:/testrunner/testexec.$$.ini \
        testrunner:install python3 scripts/new_install.py \
        -i testexec.$$.ini \
        -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${version_number},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
      status=$?
      set +x
    else
      cp /tmp/testexec.$$.ini $WORKSPACE/
      set -x
      initial_version=$(echo "$parameters" | sed -n 's/.*initial_version=\([^,]*\).*/\1/p')
	  echo "Initial version: $initial_version"
      if [ -n "$initial_version" ]; then
        echo ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${initial_version},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
        ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${initial_version},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
   	  else
        echo ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${version_number},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
        ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${version_number},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
      fi
      status=$?
      set +x
    fi
fi

# Set to available and mark it as failed install only if `exit status=2`
newState=available
if [ $status -ne 0 ]; then
  if [ $status -eq 2 ]; then
    echo exiting
    echo Desc: $desc
    newState=failedInstall
    echo newState=failedInstall>propfile
    if [ ${rerun_job} == true ]; then
      echo "${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure"
      ${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --install_failure || true
    fi
  fi
  exit 1
fi

desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`
${py_executable} scripts/ssh.py -i /tmp/testexec.$$.ini "iptables -F"

if [[ "$testrunner_tag" != "master" ]]; then
   git checkout $testrunner_tag
fi
echo "Need to set ALLOW_HTP back to True to do git pull branch"
sed -i 's/ALLOW_HTP.*/ALLOW_HTP = True/' lib/testconstants.py

## cherrypick the gerrit request if it was defined
if [ "$cherrypick" != "None" ]; then
     sh -c "$cherrypick"
fi

## Setup for java sdk client
git submodule init
git submodule update --init --force --remote
if [ -f /etc/redhat-release ]; then
  yum install -y maven
fi
if [ -f /etc/lsb-release ]; then
  apt install maven
fi
###### Added on 4/April/2018 to fix issues related to disk full on slaves.
find /data/workspace/*/logs/* -type d -ctime +10 -exec rm -rf {} \;
find /data/workspace/ -type d -ctime +10 -exec rm -rf {} \;
find /root/workspace/*/logs/* -type d -ctime +10 -exec rm -rf {} \;
find /root/workspace/ -type d -ctime +10 -exec rm -rf {} \;
######

##Added on August 2nd 2017 to kill all python processes older than 10days, comment if it causes any failures
## Updated on 11/21/19 by Mihir to kill all python processes older than 3 days instead of 10 days.
killall --older-than 72h ${py_executable}

if [ -z "${rerun_params_manual}" ] && [ -z "${rerun_params}" ]; then
  rerun_param=
elif [ -z "${rerun_params_manual}" ]; then
  rerun_param=$rerun_params
else
  rerun_param=${rerun_params_manual}
fi

set -x
${py_executable} testrunner.py -i /tmp/testexec.$$.ini -c ${confFile} -p ${parameters} ${rerun_param}
set +x

fails=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($3,s1,"=");print s1[2]}' | sed s/\"//g | awk '{s+=$1} END {print s}'`

trimmed_fails=$(echo "$fails" | tr -d '[:space:]')
if [ -z "$trimmed_fails" ]; then
  fails=0
else
  fails=$trimmed_fails
fi

echo fails is $fails
total_tests=`cat $WORKSPACE/logs/*/*.xml | grep 'testsuite errors' | awk '{split($6,s1,"=");print s1[2]}' | sed s/\"//g |awk '{s+=$1} END {print s}'`
echo $total_tests
echo Desc1: $version_number - $desc2 - $os \($(( $total_tests - $fails ))/$total_tests\)
if [ ${rerun_job} == true ]; then
  echo "${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters}"
  ${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --run_params=${parameters} || true
fi
