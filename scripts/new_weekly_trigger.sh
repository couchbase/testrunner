set +e     # keep going even if there is a shell error e.g. bad wget

sleep_func() {
  date
  echo "Sleep $1..."
  sleep $1
}

set_os_to_run() {
  os_to_run=$1
  echo "### OS :: $os_to_run ###"
}

trigger_old_dispatcher_call() {
  echo "### Triggering component :: '$components_to_trigger' ###"
  output=$(set -x ; python3 trigger_dispatcher.py \
    -v $version_number \
    --os $os_to_run \
    --suite $suite_to_use \
    --component $components_to_trigger \
    --subcomponent "$subcomponent_to_use" \
    --url "$url" \
    --branch $branch \
    --extraParameters "$extra_params" \
    --use_dockerized_dispatcher True \
    --use_predefined_params \
    --fresh_run \
    --no_confirm)
  triggered_build_num=$(echo $output | sed -n "s/.*'number': \([0-9]\{1,\}\).*/\1/p")
  echo "     Dispatcher build_num :: '$triggered_build_num'"
  triggered_builds_arr+=($components_to_trigger)
  triggered_builds_arr+=($triggered_build_num)
}

code=`echo ${version_number} | cut -d "-" -f1`

# 17-Oct-2023 :: Changes made by Ashwin / Ankush
# For releases < 7.6 only couchstore will be taken (irrespective of the storage you select)
# For magma triggers < 7.6, need to do it manually / special triggers
bucket_storage_param=""
bucket_storage_with_extra_params=""
if [ "$kv_storage" != "magma" ]; then
   bucket_storage_param=",bucket_storage=$kv_storage"
   bucket_storage_with_extra_params="&extraParameters=bucket_storage=$kv_storage"
fi

old_dispatcher="http://qa.sc.couchbase.com/job/test_suite_dispatcher"
triggered_builds_arr=()

# Variables used during the triggers
set_os_to_run "debian"
suite_to_use=$suite
components_to_trigger=""
subcomponent_to_use="None"
extra_params="get-cbcollect-info=True${bucket_storage_param}"
# End of variables

components_to_trigger="durability,transaction"
trigger_old_dispatcher_call
sleep_func 600

components_to_trigger="xdcr"
trigger_old_dispatcher_call
sleep_func 600

components_to_trigger="query,2i"
trigger_old_dispatcher_call

components_to_trigger="ephemeral,backup_recovery"
trigger_old_dispatcher_call

components_to_trigger="cli,cli_imex"
trigger_old_dispatcher_call

components_to_trigger="logredaction,subdoc"
trigger_old_dispatcher_call
sleep_func 600

components_to_trigger="failover_network"
trigger_old_dispatcher_call
sleep_func 600

#cgroups tests added for GSI
wget "http://qa.sc.couchbase.com/job/ubuntu-gsi_cgroup-limits/buildWithParameters?token=trigger_weekly_cgroups&version_number=$version_number" -O trigger.log
sleep_func 600

components_to_trigger="plasma"
trigger_old_dispatcher_call

components_to_trigger="analytics,eventing"
trigger_old_dispatcher_call

components_to_trigger="view,geo"
extra_params="get-cbcollect-info=True,bucket_storage=couchstore"
trigger_old_dispatcher_call

components_to_trigger="fts"
extra_params="get-cbcollect-info=True,bucket_storage=couchstore"
trigger_old_dispatcher_call

suite_to_use="vector_search_large"
components_to_trigger="fts"
extra_params=""
trigger_old_dispatcher_call
sleep 600

echo "### Triggering security weekly jobs ###"
suite_to_use=$suite
components_to_trigger="security"
extra_params="get-cbcollect-info=True${bucket_storage_param}"
trigger_old_dispatcher_call

# echo "### Triggering RBAC Upgrade jobs ###"
# wget  "$old_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=12hour&component=cli&subcomponent=offline-upgrade-rbac&url=$url&serverPoolId=regression&branch=$branch&addPoolId=elastic-fts"

echo "### Triggering rbac weekly jobs ###"
set_os_to_run "centos"
components_to_trigger="rbac"
trigger_old_dispatcher_call

# CE only
set_os_to_run "debian"
components_to_trigger="ce_only"
subcomponent_to_use="1a,1b"
extra_params=""
trigger_old_dispatcher_call

components_to_trigger="nserv"
subcomponent_to_use="None"
extra_params="get-cbcollect-info=True${bucket_storage_param}"
trigger_old_dispatcher_call

components_to_trigger="tunable,epeng,rza"
trigger_old_dispatcher_call

# Triggering CE weekly jobs
components_to_trigger="query,2i"
suite_to_use="ce"
trigger_old_dispatcher_call

components_to_trigger="rqg"
suite_to_use=$suite
extra_params="bucket_storage=$kv_storage"
trigger_old_dispatcher_call

components_to_trigger="collections"
extra_params="get-cbcollect-info=True${bucket_storage_param}"
trigger_old_dispatcher_call

# echo "### Triggering IPV6 weekly jobs ###"
# wget "http://qa.sc.couchbase.com/job/trigger_ipv6_weekly_jobs/buildWithParameters?token=trigger_all&version_number=$version_number&run_centos=$run_centos&run_windows=$run_windows&url=$url&branch=$branch"

wget  "http://qa.sc.couchbase.com/job/centos-query_cbo-focus-suites/buildWithParameters?token=trigger&version_number=$version_number" -O trigger.log
wget  "http://qa.sc.couchbase.com/job/centos-query_join_enum/buildWithParameters?token=trigger&version_number=$version_number" -O trigger.log

# trigger ent bkrs bwc jobs
wget  "http://qa.sc.couchbase.com/job/trigger_ent_bkrs_jobs/buildWithParameters?token=trigger_all&version_number=$version_number&url=$url&branch=$branch" -O trigger.log

echo "### Trigerring ep_engine daily job ###"
wget  "http://qa.sc.couchbase.com/job/trigger_ep_engine_jobs/buildWithParameters?token=trigger_all&version_number=$version_number&run_centos=true&url=$url&branch=$branch${bucket_storage_with_extra_params}" -O trigger.log

# breakpad tested in ubuntu but put centos in its name to be capture in greenboard in centos
# Ashok disabling/commenting following job after discussing with Bala and Ashwin on 17th July.
#wget  "http://qa.sc.couchbase.com/job/centos-u1604-p0-breakpad-sanity/buildWithParameters?token=extended_sanity&version_number=$version_number&url=$url&branch=master" -O trigger.log

echo "### Triggering xAttr jobs - Convergence ###"
# Ashok disabling/commenting following job after discussing with Bala and Ashwin on 17th July.
# wget  "http://qa.sc.couchbase.com/job/cen006-P0-converg-xattrs-vset0-00-subdoc-nested-data/buildWithParameters?token=trigger_all&version_number=$version_number&url=$url&branch=$branch${bucket_storage_with_extra_params}"
wget  "http://qa.sc.couchbase.com/job/cen006-P0-converg-xattrs-vset0-01-subdoc-sdk/buildWithParameters?token=trigger_all&version_number=$version_number&url=$url&branch=$branch${bucket_storage_with_extra_params}" -O trigger.log

echo "### Trigerring EEOnly daily job ###"
wget  "http://qa.sc.couchbase.com/job/cen006-p0-EEonly-vset00-00-feature/buildWithParameters?token=trigger_all&version_number=$version_number&branch=$branch" -O trigger.log

echo "### Trigerring Auto Failover weekly job ###"
wget  "http://qa.sc.couchbase.com/job/deb12-nserv-autofailover-server-stop/buildWithParameters?token=trigger_all&version_number=$version_number&url=$url&testrunner_tag=$testrunner_tag&branch=$branch" -O trigger.log

sleep 2m

# Trigger Analytics, Eventing, FTS, Views and Geo tests only for Couchstore KV Storage
echo "### Triggering Mobile jobs ###"
curl -X POST http://trigger:dd2b75aa552ae7c9a59ce1ea5af93f2b@uberjenkins.sc.couchbase.com:8080/job/_couchbase-server-upstream/buildWithParameters\?token\=trigger_all\&COUCHBASE_SERVER_VERSION\=$version_number
# centos upgrade
echo "### Triggering Upgrade jobs ###"
# Mihir : 9/3/21 : Temporarily commenting this trigger as it needs some fixing
# Thuan: 9/20/2021 : Moved extra params to job config in test suite.  Trigger is ok now
# wget  "http://qa.sc.couchbase.com/job/trigger_upgrade_jobs/buildWithParameters?token=trigger_all&version_number=$version_number&url=$url&branch=$branch&addPoolId=elastic-fts" -O trigger.log
## wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=centos&version_number=$version_number&suite=weekly&component=upgrade&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True"

echo "### Triggering Alternate Address jobs ###"
wget  "http://qa.sc.couchbase.com/job/centos-nserv_alternate-address-feature/buildWithParameters?token=trigger_all&version_number=$version_number&url=$url&branch=$branch" -O trigger.log

echo "### Triggering Jepsen weekly ###"
wget "http://qa.sc.couchbase.com/job/jepsen-durability-trigger-new/buildWithParameters?token=jepsen-trigger-new&version_number=$version_number&test_suite=weekly&build_type=official" -O trigger.log

echo "### Triggering Nutshell job ###"
wget "http://qa.sc.couchbase.com/job/centos-nutshell/buildWithParameters?token=trigger&version_number=$version_number" -O trigger.log

echo "### Trigerring Forestdb jobs  ###"
wget  "http://qa.sc.couchbase.com/job/cen006-p0-forestdb-sanity/buildWithParameters?token=trigger&version_number=$version_number&url=$url&branch=$branch" -O trigger.log


#echo "### Triggering 2i Upgrade weekly jobs ###"
#3/20: Commenting the 2i functional2itests job until it is fixed
#wget "http://qa.sc.couchbase.com/job/cen7-2i-set4-job1-functional2itests/buildWithParameters?token=trigger&version_number=$version_number&gsi_type=memory_optimized&url=$url&branch=$branch" -O trigger.log
#wget  "http://qa.sc.couchbase.com/job/cen7-2i-plasma-set5-job1-upgrade-6-0-3_RED/buildWithParameters?token=trigger&version_number=$version_number&url=$url&branch=$branch${bucket_storage_with_extra_params}" -O trigger.log
#wget  "http://qa.sc.couchbase.com/job/cen7-2i-plasma-set5-job1-upgrade-5-0-1-int64/buildWithParameters?token=trigger&version_number=$version_number&url=$url&branch=$branch" -O trigger.log

#echo "### Triggering OS Certification jobs ###"
#wget   --server-response "http://qa.sc.couchbase.com/job/trigger_os_certification/buildWithParameters?token=trigger_all&version_number=$version_number&run_centos=$run_centos&run_windows=$run_windows&url=$url&branch=$branch${bucket_storage_with_extra_params}" 2>&1 | grep "HTTP/" | awk '{print $2}'

echo "Sleep for 60 seconds...."
sleep 60

# SDK situational tests
# java situational tests
# wget  --user "jake.rawsthorne@couchbase.com" --password $SDK_JENKINS_TOKEN "http://sdkbuilds.sc.couchbase.com/view/JAVA/job/sdk-java-situational-release/job/sdk-java-situational-all/buildWithParameters?token=sdkbuilds&cluster_version=$version_number&run_regular=true&run_n1ql=true&run_subdoc=true" -O trigger.log

# LCB situational tests
# wget  --user "jake.rawsthorne@couchbase.com" --password $SDK_JENKINS_TOKEN "https://sdk.jenkins.couchbase.com/view/Situational/job/c-cpp/job/lcb/job/centos-lcb-sdk-server-situational-tests/buildWithParameters?token=sdkbuilds&cluster_version=$version_number&run_regular=true&run_n1ql=true&run_subdoc=true" -O trigger.log

# .NET situational tests
wget  --user "jake.rawsthorne@couchbase.com" --password $SDK_JENKINS_TOKEN "http://sdkbuilds.sc.couchbase.com/view/.NET/job/sdk-net-situational-release/job/dotnet-situational-all//buildWithParameters?token=sdkbuilds&cluster_version=$version_number&run_regular=true&run_n1ql=true&run_subdoc=true" -O trigger.log

# Go situational tests
# wget  --user "jake.rawsthorne@couchbase.com" --password $SDK_JENKINS_TOKEN "http://sdkbuilds.sc.couchbase.com/view/GO/job/sdk-go-situational-release/job/go-sdk-situational-all/buildWithParameters?token=sdkbuilds&cluster_version=$version_number&run_regular=true&run_n1ql=true&run_subdoc=true" -O trigger.log

echo "### Triggering Windows jobs ###"
wget "http://qa.sc.couchbase.com/job/test_suite_dispatcher_dynvm/buildWithParameters?token=extended_sanity&version_number=$version_number&suite=mustpass&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True" -O trigger.log

# Following block is from 'trigger_upgrade_jobs' job
if [ 0 == 1 ]; then
  day=`date '+%a'`

  echo version=${version_number}
  upgrade_ver=${version_number:0:3}
  short_ver=`echo ${version_number} | head -c3`
  echo $short_ver

  customExtraParameters="get-cbcollect-info=True,bucket_storage=couchstore"
  extraParameters=$customExtraParameters

  ## Upgrade for specific components - analytics,cli,backup_recovery,fts,query,xdcr
  echo "analytics,cli,backup_recovery,fts,query,xdcr,2i"

  components_to_trigger="2i,analytics,cli,backup_recovery,fts,query,xdcr"
  suite_to_use="12hr_upgrade"
  extra_params=$extraParameters
  trigger_old_dispatcher_call

  wget -O dispatcher.out  "http://qa.sc.couchbase.com/job/test_suite_dispatcher_dynvm/buildWithParameters?token=extended_sanity&OS=windows22&version_number=$version_number&suite=12hr_upgrade&component=fts&subcomponent=None&url=$url&serverPoolId=$serverPoolId&addPoolId=elastic-fts&branch=$branch&extraParameters=$extraParameters"

  # Triggering all upgrades with couchstore
  components_to_trigger="upgrade"
  trigger_old_dispatcher_call
  echo "All upgrades with couchstore are being triggered"

  if [ "$(echo "${short_ver} >= 7.6" | bc)" -eq 1 ]; then
    echo "******* upgrade to ${version_number} ********"
    echo "magma upgrades with TAF are triggered"
    customExtraParameters="get-cbcollect-info=True"
    extraParameters=$customExtraParameters
    #wget -O dispatcher.out "$old_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=12hr_upgrade_TAF&component=upgrade&subcomponent=None&url=$url&serverPoolId=$serverPoolId&addPoolId=elastic-fts&branch=$branch&extraParameters=$extraParameters"
  fi
fi
