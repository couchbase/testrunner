#!/bin/bash

python_exe = "python";
grep "centos" /etc/issue -i -q
if [ $? = '0' ];then
python_exe = "python27"
fi

if [ -n $BUILD_CAUSE ]
then
  if [ "$BUILD_CAUSE" = "UPSTREAMTRIGGER" ]
  then
    echo "!!!!!it's downstream project but will use own parameters for ini_file, config_file, test_params & install_params"
    sudo pip install python-jenkins
    export ini_file=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "ini_file"]'`
    export config_file=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "config_file"]'`
    export test_params=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "test_params"]'`
    export install_params=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "install_params"]'`
    export run_install=`$python_exe -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "run_install"]'`
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
FAIL_CONNECTIONS=`$python_exe scripts/ssh.py -i ${ini_file} 'ls' 2>&1| grep 'No handlers' | wc -l`

if [ ${FAIL_CONNECTIONS} -ge 1 ]
    then
    echo '---------------------------- SOME VMS ARE UNAVAILABLE -----------------------'
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
       $python_exe scripts/install.py -i ${ini_file} -p version=${version_number},vbuckets=${vbuckets},upr=${upr},${install_params}
fi

echo '---------------------------- TESTS RUN -----------------------'
$python_exe ./testrunner -i ${ini_file} -c ${config_file} -p ${test_params}



free
df
export
$python_exe scripts/ssh.py -i ${ini_file} "ls -la /tmp/"
bash -c "$python_exe scripts/getchanges.py ${version_number};exit 0;"
bash -c "$python_exe scripts/getcoredumps.py	 -i ${ini_file};exit 0;"
