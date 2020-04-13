#!/bin/bash

python_exe="python3";
grep "centos" /etc/issue -i -q
if [ $? = '0' ];then
python_exe="python27"
echo ${version_number} | grep "7\.0" && python_exe="python3"
fi

if [ -n $BUILD_CAUSE ]
then
  if [ "$BUILD_CAUSE" = "UPSTREAMTRIGGER" ]
  then
    echo "!!!!!it's downstream project but will use own parameters for ini_file, config_file, test_params, install_params, run_install, group"
    sudo pip install python-jenkins
    export ini_file=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); paramDef=filter(None, j.get_job_info(os.environ["JOB_NAME"])["actions"])[0]["parameterDefinitions"]; [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in paramDef if p["name"] == "ini_file"]'`
    export config_file=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); paramDef=filter(None, j.get_job_info(os.environ["JOB_NAME"])["actions"])[0]["parameterDefinitions"]; [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in paramDef if p["name"] == "config_file"]'`
    export test_params=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); paramDef=filter(None, j.get_job_info(os.environ["JOB_NAME"])["actions"])[0]["parameterDefinitions"]; [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in paramDef if p["name"] == "test_params"]'`
    export install_params=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); paramDef=filter(None, j.get_job_info(os.environ["JOB_NAME"])["actions"])[0]["parameterDefinitions"]; [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in paramDef if p["name"] == "install_params"]'`
    export run_install=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); paramDef=filter(None, j.get_job_info(os.environ["JOB_NAME"])["actions"])[0]["parameterDefinitions"]; [sys.stdout.write(str(p["defaultParameterValue"]["value"]).lower()) for p in paramDef if p["name"] == "run_install"]'`
    export group=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); paramDef=filter(None, j.get_job_info(os.environ["JOB_NAME"])["actions"])[0]["parameterDefinitions"]; [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in paramDef if p["name"] == "group"]'`
  elif [ "$BUILD_CAUSE" = "MANUALTRIGGER" ]
  then
    echo "WILL USE JOB's PARAMETERS:"
  else
    echo "BUILD_CAUSE is undefined"
  fi
else
  echo -e "BUILD_CAUSE not set\n"
fi

if [ -z "$install_params" ];
then
	echo "install_params not set! Will use default value: product=cb,parallel=True";
	install_params="product=cb,parallel=True";
fi

if ! [ -z "$url" ];
then
    install_params="$install_params,url=$url"
fi

if [ -z "$group" ];
then
	echo "group not set! Will run all tests: GROUP=ALL";
	group="ALL";
fi

export

echo "ini_file to be used: " ${ini_file}

if [ -z "$ini_file" ];
then
	echo "ini_file not set!!!";
	exit;
fi


INI_NOT_FOUND=`cat ${ini_file} 2>&1| grep "No such file or directory"| wc -l`

if [ ${INI_NOT_FOUND} -ge 1 ]
    then
    echo "ini_file doesn't exist!: " ${ini_file}
    exit ${INI_NOT_FOUND}
fi



set +x
echo '---------------------------- PRE-SETUP VERIFICATION -----------------------'
echo ${version_number}
echo ${url}
FAIL_CONNECTIONS=`$python_exe scripts/ssh.py -i ${ini_file} 'ls' 2>&1| grep 'No handlers' | wc -l`

if [ ${FAIL_CONNECTIONS} -ge 1 ]
    then
    $python_exe scripts/ssh.py -i ${ini_file} 'pwd'
    echo '---------------------------- !!!SOME VMS ARE UNAVAILABLE!!! -----------------------'
    exit ${FAIL_CONNECTIONS}
fi

$python_exe scripts/ssh.py -i ${ini_file} "ntpdate ntp.ubuntu.com"
$python_exe scripts/ssh.py -i ${ini_file} "date"
$python_exe scripts/ssh.py -i ${ini_file} "cat /etc/*rele*"
$python_exe scripts/ssh.py -i ${ini_file} "lscpu"
$python_exe scripts/ssh.py -i ${ini_file} "find /tmp/core* -mtime +10 -exec rm {} \;"
$python_exe scripts/ssh.py -i ${ini_file} "df"
$python_exe scripts/ssh.py -i ${ini_file} "free"
$python_exe scripts/ssh.py -i ${ini_file} "ls -la /tmp/"
ulimit -a

if [ ${run_install} = true ]
       then
       echo '---------------------------- INSTALLATION -----------------------'
       echo "python_exe scripts/install.py -i ${ini_file} -p version=${version_number},vbuckets=${vbuckets},${install_params}  2>&1 | tee install.log"
       $python_exe scripts/install.py -i ${ini_file} -p version=${version_number},vbuckets=${vbuckets},${install_params}  2>&1 | tee install.log
       INSTALL_FAILED=`cat install.log 2>&1| grep "some nodes were not install successfully!"| wc -l`

       if [ ${INSTALL_FAILED} -ge 1 ]
          then
             echo "some nodes were not install successfully! Tests will not run!"
             exit ${INSTALL_FAILED}
       fi
       # Sleeping after installation to ensure if all the servers are properly up and running
       sleep 90
fi

echo '---------------------------- TESTS RUN -----------------------'
if test X"${upgrade_version}" != X"" ;
then
	$python_exe testrunner.py -i ${ini_file} -c ${config_file} -p ${test_params},GROUP=${group},upgrade_version=${version_number},initial_vbuckets=${vbuckets}
else
	$python_exe testrunner.py -i ${ini_file} -c ${config_file} -p ${test_params},GROUP=${group}
fi


free
df
export
$python_exe scripts/ssh.py -i ${ini_file} "ls -la /tmp/"
bash -c "$python_exe scripts/getchanges.py ${version_number};exit 0;"
bash -c "$python_exe scripts/getcoredumps.py	 -i ${ini_file};exit 0;"
##  Find the way to delete logs
# find ./logs/* -mtime +7 -exec rm -rf {} \;
