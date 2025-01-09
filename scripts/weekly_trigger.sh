set +e     # keep going even if there is a shell error e.g. bad wget
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

echo "### Running jobs on Debian"
echo "### Triggering Durability weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=durability,transaction&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}&use_dockerized_dispatcher=true" -O trigger.log
echo "Sleep for 60 seconds...."
sleep 600

echo "### Triggering n/w failover and slow disk autofailover scenarios ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=failover_network&url=$url&serverPoolId=failover&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}&use_dockerized_dispatcher=true" -O trigger.log
echo "Sleep for 60 seconds...."
sleep 600

#echo "### Triggering XDCR weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=xdcr&retries=2&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}" -O trigger.log
sleep 600

echo "### Triggering query,2i,ephemeral,backup_recovery,logredaction,cli weekly jobs, cbo_focus_suites,join_enum ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=query&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}&use_dockerized_dispatcher=true" -O trigger.log
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=2i&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}&use_dockerized_dispatcher=true" -O trigger.log
#cgroups tests added for GSI
wget "http://qa.sc.couchbase.com/job/ubuntu-gsi_cgroup-limits/buildWithParameters?token=trigger_weekly_cgroups&version_number=$version_number" -O trigger.log
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=ephemeral,backup_recovery&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}" -O trigger.log

wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=cli,cli_imex&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}" -O trigger.log

wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=logredaction,subdoc&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}" -O trigger.log

sleep 600

echo "### Triggering plasma jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=plasma&url=$url&serverPoolId=magmanew&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}" -O trigger.log

echo "### Triggering Analytics, Eventing, FTS, Views and Geo weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=analytics,eventing&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True" -O trigger.log
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=view,geo&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True,bucket_storage=couchstore" -O trigger.log
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=fts&url=$url&serverPoolId=regression&branch=$branch&addPoolId=elastic-fts&extraParameters=get-cbcollect-info=True" -O trigger.log
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=vector_search_large&component=fts&url=$url&serverPoolId=magmanew&branch=$branch&addPoolId=None&extraParameters=get-cbcollect-info=True" -O trigger.log

sleep 600


echo "### Triggering security weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=security&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}" -O trigger.log

# echo "### Triggering RBAC Upgrade jobs ###"
# wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=12hour&component=cli&subcomponent=offline-upgrade-rbac&url=$url&serverPoolId=regression&branch=$branch&addPoolId=elastic-fts"

echo "### Triggering rbac weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=centos&version_number=$version_number&suite=$suite&component=rbac&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}" -O trigger.log

#CE only
echo "### Triggering CE only weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=ce_only&subcomponent=1a,1b&url=$url&serverPoolId=regression&branch=$branch&executor_job_parameters=installParameters=edition=community" -O trigger.log

echo "### Triggering nserv weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=nserv&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}&use_dockerized_dispatcher=true" -O trigger.log

echo "### Triggering tunable,epeng,rza weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=tunable,epeng,rza&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True${bucket_storage_param}&use_dockerized_dispatcher=true" -O trigger.log

echo "### Triggering CE weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=ce&component=query&url=$url&serverPoolId=regression&branch=$branch&executor_job_parameters=installParameters=edition=community${bucket_storage_with_extra_params}" -O trigger.log
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=ce&component=2i&url=$url&serverPoolId=regression&branch=$branch&executor_job_parameters=installParameters=edition=community${bucket_storage_with_extra_params}" -O trigger.log

echo "### Triggering RQG jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=rqg&url=$url&serverPoolId=regression&branch=${branch}${bucket_storage_with_extra_params}" -O trigger.log

echo "### Triggering Collections weekly jobs ###"
wget  "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=$suite&component=collections&url=$url&serverPoolId=regression&branch=$branch&extraParameters=get-cbcollect-info=True,log_level=info${bucket_storage_param}&use_dockerized_dispatcher=true" -O trigger.log
echo "Sleep for 600 seconds...."

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

echo "### Triggering Forestdb jobs  ###"
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
  wget -O dispatcher.out "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=12hr_upgrade&component=2i,analytics,cli,backup_recovery,fts,query,xdcr&subcomponent=None&url=$url&serverPoolId=$serverPoolId&addPoolId=elastic-fts&branch=$branch&extraParameters=$extraParameters"
  wget -O dispatcher.out  "http://qa.sc.couchbase.com/job/test_suite_dispatcher_dynvm/buildWithParameters?token=extended_sanity&OS=windows22&version_number=$version_number&suite=12hr_upgrade&component=fts&subcomponent=None&url=$url&serverPoolId=$serverPoolId&addPoolId=elastic-fts&branch=$branch&extraParameters=$extraParameters"

  # Triggering all upgrades with couchstore
  echo "All upgrades with couchstore are being triggered"
  wget -O dispatcher.out "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=12hr_upgrade&component=upgrade&subcomponent=None&url=$url&serverPoolId=$serverPoolId&addPoolId=elastic-fts&branch=$branch&extraParameters=$extraParameters"

  if [ "$(echo "${short_ver} >= 7.6" | bc)" -eq 1 ]; then
    echo "******* upgrade to ${version_number} ********"
    echo "magma upgrades with TAF are triggered"
    customExtraParameters="get-cbcollect-info=True"
    extraParameters=$customExtraParameters
    #wget -O dispatcher.out "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token=extended_sanity&OS=debian&version_number=$version_number&suite=12hr_upgrade_TAF&component=upgrade&subcomponent=None&url=$url&serverPoolId=$serverPoolId&addPoolId=elastic-fts&branch=$branch&extraParameters=$extraParameters"
  fi
fi
