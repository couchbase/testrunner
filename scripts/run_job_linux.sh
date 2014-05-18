#!/bin/bash

if [ -n $BUILD_CAUSE ]
then
  if [ "$BUILD_CAUSE" = "UPSTREAMTRIGGER" ]
  then
    echo "!!!!!it's downstream project but will use own parameters for ini_file, config_file & test_params"
    sudo pip install python-jenkins
    export ini_file=`python -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "ini_file"]'`             
    export config_file=`python -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "config_file"]'`
    export test_params=`python -c 'import sys;import jenkins; import os;import json; j = jenkins.Jenkins(os.environ["HUDSON_URL"]); [sys.stdout.write(p["defaultParameterValue"]["value"]) for p in j.get_job_info(os.environ["JOB_NAME"])["actions"][1]["parameterDefinitions"] if p["name"] == "test_params"]'`
  elif [ "$BUILD_CAUSE" = "MANUALTRIGGER" ]
  then
    echo "WILL USE JOB's PARAMETERS:"
  else
    echo "BUILD_CAUSE is undefined"
  fi
else
  echo -e "BUILD_CAUSE not set\n"
fi
export


set +x
echo '---------------------------- PRE-SETUP VERIFICATION -----------------------'
echo version=${version_number}
FAIL_CONNECTIONS=`python scripts/ssh.py -i ${ini_file} 'ls' 2>&1| grep 'No handlers' | wc -l`

if [ ${FAIL_CONNECTIONS} -ge 1 ]
    then
    echo '---------------------------- SOME VMS ARE UNAVAILABLE -----------------------'
    exit ${FAIL_CONNECTIONS}
fi

python scripts/ssh.py -i ${ini_file} "ntpdate ntp.ubuntu.com"
python scripts/ssh.py -i ${ini_file} "date"
python scripts/ssh.py -i ${ini_file} "cat /etc/*rele*"
python scripts/ssh.py -i ${ini_file} "lscpu"
python scripts/ssh.py -i ${ini_file} "find /tmp/core* -mtime +10 -exec rm {} \;"
python scripts/ssh.py -i ${ini_file} "df"
python scripts/ssh.py -i ${ini_file} "free"
python scripts/ssh.py -i ${ini_file} "ls -la /tmp/"
ulimit -a
if [ ${run_install} = true ]
       then
       echo '---------------------------- INSTALLATION -----------------------'
       python scripts/install.py -i ${ini_file} -p version=${version_number},product=cb,parallel=True,vbuckets=${vbuckets},upr=${upr}
fi

echo '---------------------------- TESTS RUN -----------------------'
./testrunner -i ${ini_file} -c conf/py-documentkeys.conf -p ${test_params}



free
df
export
python scripts/ssh.py -i ${ini_file} "ls -la /tmp/"
bash -c "python scripts/getchanges.py ${version_number};exit 0;"
bash -c "python scripts/getcoredumps.py	 -i ${ini_file};exit 0;"