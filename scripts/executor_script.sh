#!/bin/bash

# Heartbeat marking this workspace as in-use for the disk-cleanup sweep below -
# refreshed at the start of every run, so a workspace this job is (or very
# recently was) actively using never gets reclaimed as "abandoned".
touch "$WORKSPACE/.executor_lock" 2>/dev/null

cleanup_workspace() {
  # Remove any installers downloaded locally
  rm -rf *.deb *.rpm *.msi
}

# Set desired python env
export PYENV_VERSION="3.10.13"
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local $PYENV_VERSION

support_ver="6.5"
small_ver=${version_number:0:3}
host_ip=$(hostname -I | awk '{print $1}')
echo $host_ip
rerun_job=true
py_executable=python3

# Block was prev. in a separate shell block before
echo Desc: $descriptor

echo "###########################################"
echo "  Populating env file for downstream jobs"
echo "1/4 Extracting is_dynamic_vms value"
export is_dynamic_vms=`echo $dispatcher_params| sed -n 's/.*"use_dynamic_vms": *\([^,]*\).*/\1/p' | tr -d ' '`
echo "is_dynamic_vms value: $is_dynamic_vms"

echo "2/4 Creating file: savejoblogs_job_params"
echo "test_job_url=${JOB_URL}" > savejoblogs_job_params
echo "test_job_build=${BUILD_NUMBER}" >> savejoblogs_job_params
echo "test_name=${descriptor}" >> savejoblogs_job_params
echo "addPoolServers=$addPoolServers" >> savejoblogs_job_params
echo "version_number=$version_number" >> savejoblogs_job_params
echo "is_dynamic_vms=$is_dynamic_vms" >> savejoblogs_job_params

echo "3/4 Creating file: cleanup_job_params"
echo "descriptor=$descriptor" > cleanup_job_params
echo "UPSTREAM_BUILD_NUMBER=${BUILD_NUMBER}" >> cleanup_job_params
echo "addPoolServers=$addPoolServers" >> cleanup_job_params
echo "version_number=$version_number" >> cleanup_job_params
echo "is_dynamic_vms=$is_dynamic_vms" >> cleanup_job_params

echo "4/4 Creating file: aws_cleanup_job_params"
echo "servers=${servers}" > aws_cleanup_job_params
echo "###########################################"

touch rerun_props_file
if [ ${fresh_run} == false ]; then
  ${py_executable} scripts/rerun_jobs.py ${version_number} --executor_jenkins_job --manual_run
fi
# End of block

git submodule init
git submodule update --init --force --remote

## cherrypick the gerrit request if it was defined
if [ "$cherrypick" != "None" ]; then
   sh -c "$cherrypick"
fi

echo "##### Python packages check #####"
if [ "$(python3 --version | cut -d' ' -f 2)" != "$PYENV_VERSION" ]; then
  echo "Python version is not $PYENV_VERSION, exiting"
  exit 1
fi

# Run pip install to handle any new deps introduced
${py_executable} -m pip install -r requirements.txt

set +e
echo newState=available>propfile
newState=available


if [ -f /etc/redhat-release ]; then
  echo 'centos'
  yum install -y docker
  # CBQE-6231
  if [ -f /usr/bin/systemctl ]; then
    systemctl start docker
  else
    service docker start
  fi
  docker pull docker.io/mockserver/mockserver:5.15.0
fi

if [ -f /etc/lsb-release ]; then
  echo 'ubuntu'
  apt-install docker
  service docker start
  docker pull docker.io/mockserver/mockserver:5.15.0
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

if [[ "${slave}" = "bhive_slave" ]]; then
  echo "Running on bhive_slave"
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
    echo ${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i /tmp/testexec_reformat.$$.ini -p ${os} -o /tmp/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
    ${py_executable}  scripts/populateIni.py -s ${servers} -d ${addPoolServerId} -a ${addPoolServers} -i /tmp/testexec_reformat.$$.ini -p ${os} -o /tmp/testexec.$$.ini -k '{'${UPDATE_INI_VALUES}'}'
  fi
fi

if [ "$os" = "windows" ]; then
   echo "Have Windows,"
   parallel=true   # serial worked even worse but may come back to is
 else
   parallel=true
fi

if [ "$component" = "xdcr" ]; then
   if [ "$installParameters" = "None" ] || [ "$installParameters" = "" ]; then
      installParameters='init_clusters=True'
   else
      installParameters="${installParameters},init_clusters=True"
   fi
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

    echo "Starting server installation"
    if [[ "${slave}" = "bhive_slave_test" ]]; then
      set -x
      docker run --rm \
        -v /tmp/testexec.$$.ini:/testrunner/testexec.$$.ini \
        testrunner:install python3 scripts/new_install.py \
        -i testexec.$$.ini \
        -p force_reinstall=True,timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${version_number},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
      status=$?
      set +x
    else
      cp /tmp/testexec.$$.ini $WORKSPACE/
      initial_version=$(echo "$parameters" | sed -n 's/.*initial_version=\([^,]*\).*/\1/p')
	  echo "Initial version: $initial_version"
      if [ -n "$initial_version" ]; then
        version_to_use=$initial_version
   	  else
        version_to_use=$version_number
      fi
      set -x
      ${py_executable} scripts/new_install.py -i /tmp/testexec.$$.ini -p force_reinstall=True,timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD},get-cbcollect-info=True,version=${version_to_use},product=cb,debug_logs=True,ntp=True,url=${url}${extraInstall}
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
  cleanup_workspace
  exit 1
fi

desc2=`echo $descriptor | awk '{split($0,r,"-");print r[1],r[2]}'`
if [ -f /tmp/testexec_root.$$.ini ]; then
    ${py_executable} scripts/ssh.py -i /tmp/testexec_root.$$.ini "iptables -F"
else
    ${py_executable} scripts/ssh.py -i /tmp/testexec.$$.ini "iptables -F"
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
# Clean this build's own old per-run log dirs - each run gets a freshly
# timestamped logs/testrunner-<ts>/ (testrunner.py os.makedirs), so only
# genuinely completed runs of THIS job ever match; the live run's own dir
# always has a fresh ctime. logs/ doesn't exist yet on a wiped workspace,
# hence 2>/dev/null - the bare error used to look like the point where a
# stalled build died.
find "$WORKSPACE/logs" -mindepth 1 -maxdepth 1 -type d -ctime +10 \
  -exec rm -rf {} \; 2>/dev/null

# Reclaim whole workspaces of jobs that haven't run in 10+ days, node-wide.
# logs/ itself only gets a new entry when testrunner.py actually starts a
# run, so its own ctime is a reliable "last ran" signal - unlike checking
# ctime on arbitrary directories, which previously let one job's cleanup
# delete a different, concurrently running job's live workspace (its cwd
# would vanish mid-test, ending the build with zero artifacts archived
# since allowEmptyArchive/allowEmptyResults are both true). Layered guards
# against catching a workspace still in use: never touch this build's own
# $WORKSPACE, skip anything with a recent .executor_lock heartbeat
# (touched at the top of every run), and skip anything with an open file
# handle.
#
# Everything here is hard-bounded in time. `lsof +D` walks every file under
# a whole workspace and blocks indefinitely on a stale NFS mount (these
# nodes force-unmount NFS during install), so it gets a per-workspace
# timeout, and the sweep as a whole gets a budget. A build must never spend
# its wall-clock on housekeeping: one sweep stuck in `lsof +D` consumed a
# full 780-minute job timeout before a single test ran, and the build was
# aborted with zero results (test_suite_executor/85412).
#
# Note the exit-code handling: lsof exits 0 when it finds open handles and
# 1 when it finds none. ONLY a clean 1 permits deletion. A timed-out lsof
# exits 124 and a missing lsof exits 127, and neither means "idle" - any
# outcome we can't read as "proven idle" skips the workspace, because the
# cost of being wrong is deleting a live build's cwd.
RECLAIM_BUDGET_SECS=300      # whole node-wide sweep
RECLAIM_LSOF_TIMEOUT=30      # single `lsof +D <workspace>`
RECLAIM_FIND_TIMEOUT=60      # single top-level `find <base>`

reclaim_stale_workspaces() {
  local deadline=$((SECONDS + RECLAIM_BUDGET_SECS))
  local base logdir parent rc

  # Both tools are mandatory, not optional: without lsof we cannot prove a
  # workspace is idle before deleting it, and without timeout we cannot bound
  # lsof. Skipping loudly beats either deleting blind or hanging - and don't
  # assume a tool is present just because it usually is (see killall below).
  if ! command -v lsof >/dev/null 2>&1 || ! command -v timeout >/dev/null 2>&1; then
    echo "Skipping node-wide workspace reclaim: lsof and timeout are both" \
         "required to reclaim a workspace safely"
    return 0
  fi

  for base in /data/workspace /root/workspace; do
    [ -d "$base" ] || continue
    while IFS= read -r logdir; do
      if [ "$SECONDS" -ge "$deadline" ]; then
        echo "Node-wide workspace reclaim hit its ${RECLAIM_BUDGET_SECS}s" \
             "budget; remaining stale workspaces left for the next run"
        return 0
      fi
      parent="$(dirname "$logdir")"
      [ "$parent" = "$WORKSPACE" ] && continue
      [ -n "$(find "$parent/.executor_lock" -mtime -1 2>/dev/null)" ] && continue
      timeout "$RECLAIM_LSOF_TIMEOUT" lsof +D "$parent" >/dev/null 2>&1
      rc=$?
      if [ "$rc" -ne 1 ]; then
        echo "Keeping $parent: lsof exited $rc (open handles, timeout or" \
             "error) - not proven idle"
        continue
      fi
      rm -rf "$parent"
    done < <(timeout "$RECLAIM_FIND_TIMEOUT" find "$base" -mindepth 2 -maxdepth 2 \
               -type d -name logs -ctime +10 2>/dev/null)
  done
}

reclaim_stale_workspaces
######

##Added on August 2nd 2017 to kill all python processes older than 10days, comment if it causes any failures
## Updated on 11/21/19 by Mihir to kill all python processes older than 3 days instead of 10 days.
## killall comes from psmisc, which isn't installed on every slave - on the
## debian 12 nodes this was a silent no-op ("killall: command not found"), so
## fall back to an equivalent age filter over ps. 72h is far beyond the 780
## minute job timeout, so nothing still doing useful work can match.
if command -v killall >/dev/null 2>&1; then
  killall --older-than 72h ${py_executable}
else
  ps -eo pid=,etimes=,comm= | while read -r stale_pid stale_age stale_comm; do
    [ "$stale_comm" = "${py_executable}" ] || continue
    [ "$stale_age" -gt 259200 ] 2>/dev/null || continue
    echo "Killing stale ${py_executable} pid $stale_pid (${stale_age}s old)"
    kill -9 "$stale_pid" 2>/dev/null
  done
fi

# Trim whitespaces to detect empty input
rerun_params=$(echo "$rerun_params" | xargs)
if [ "$rerun_params" == "" ]; then
  # Only if user has no input given, get rerun data from
  # the file created by prev. rerun_jobs.py script
  rerun_file_data=$(cat rerun_props_file)
  if [ "$rerun_file_data" != "" ]; then
    rerun_params="$rerun_file_data"
  fi
fi

set -x
${py_executable} testrunner.py -i /tmp/testexec.$$.ini -c ${confFile} -p ${parameters} ${rerun_params}
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

cleanup_workspace
