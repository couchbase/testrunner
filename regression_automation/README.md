# Trigger Dispatcher (For http://qa.sc.couchbase.com)

```
$ python3 trigger_dispatcher.py
usage: trigger_dispatcher.py 
    -v VERSION 
    --suite {12hour,12hourVM,12hourbeta,weekly,sanity,fullRegression,demo,extendedSanity,test,jre-less,12hr_weekly,12hr_upgrade,12hr_upgrade_TAF,12hrelixir,12hrelixirDaily,ce,quarantined,weekly_unstable,mustpass,disabled,magmareg,magmaWeekly,capella_mock,magmanew,ipv6,capella_weekly,vector_search_large,vector_search,columnar}
    --component COMPONENT 
    --serverPoolId {regression,plasma_new,12hour,12hrreg,failover,upgrade,dev,durability,extendedSanity,spock12hour,test,2core,2i,nserv,autofailover,security,query,xdcr,lww,bkrs,fts,ipv6,ipv6hostname,test_windows,12hour_new,win-ipv6,jre-less,RQG,temp_query,jre,os_certification,12hourwin,magmareg,project100-sshtest1,plasma,cardconnect_cbse,dynamic_win,temp,capella,magmanew,magmaUpgrade}
    [--branch BRANCH]
    [--os {centos,centos8,centosnonroot,debian,debian11,debian12,debian11nonroot,oel8,oel9,rhel8,rhel9,rocky9,suse15,ubuntu18,ubuntu20,ubuntu22,windows,windows2019}]
    [--columnar_version_number COLUMNAR_VERSION_NUMBER]
    [--subcomponent SUBCOMPONENT] 
    [--subcomponent_regex SUBCOMPONENT_REGEX]
    [--addPoolId {None,crdb-fts,elastic-fts,elastic7-fts,elastic-xdcr,new-elastic}]
    [--extraParameters EXTRAPARAMETERS]
    [--url URL]
    [--cherrypick CHERRYPICK]
    [--retries RETRIES]
    [--rerun_condition {ALL,FAILED,UNSTABLE,PENDING}]
    [--rerun_params RERUN_PARAMS]
    [--executor_suffix EXECUTOR_SUFFIX]
    [--executor_job_parameters EXECUTOR_JOB_PARAMETERS]
    [--check_vm {True,False}]
    [--initial_version INITIAL_VERSION]
    [--serverType {ON_PREM,CAPELLA_MOCK,ELIXIR_ONPREM}]
    [--skip_install]
    [--use_dockerized_dispatcher {True,False}]
    [--fresh_run]
    [--no_confirm]
    [-h]
```

### Default values
- Default values for the above optional params are in [default.py](/regression_automation/defaults.py)
- If `--branch` is 'default', the target branch is auto-selected from [py_constants repo rel_branch_map.py](https://github.com/couchbaselabs/py_constants/blob/master/cb_constants/rel_branch_map.py)
- `--no_confirm` flag is useful for triggering the script from other shell scripts. Avoid this flag in manual mode to confirm the params before the trigger
