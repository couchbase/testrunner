from argparse import ArgumentParser

import sys

from regression_automation.defaults import DispatcherDefaults


class DispatcherCommandLineArgs(object):
    @staticmethod
    def parse_arguments():
        parser = ArgumentParser(description="Trigger dispatcher job for the given version and suites")
        parser.add_argument("--os",
                            dest="os",
                            choices=["centos", "centos8", "centosnonroot",
                                     "debian", "debian11", "debian12", "debian11nonroot",
                                     "oel8", "oel9",
                                     "rhel8", "rhel9",
                                     "rocky9",
                                     "suse15",
                                     "ubuntu18", "ubuntu20", "ubuntu22",
                                     "windows", "windows2019"],
                            default=DispatcherDefaults.OS,
                            help="Platform on which the regression is triggered")
        parser.add_argument("-v", "--version",
                            required=True,
                            help="Server version to run regression")
        parser.add_argument("--columnar_version_number",
                            dest="columnar_version_number",
                            default=DispatcherDefaults.COLUMNAR_VERSION_NUMBER,
                            help="Columnar build number like 1.0.0-2239")
        parser.add_argument("--suite",
                            required=True,
                            choices=["12hour", "12hourVM", "12hourbeta",
                                     "weekly", "sanity", "fullRegression",
                                     "demo", "extendedSanity", "test",
                                     "jre-less", "12hr_weekly", "12hr_upgrade",
                                     "12hr_upgrade_TAF", "12hrelixir",
                                     "12hrelixirDaily", "ce", "quarantined",
                                     "weekly_unstable", "mustpass", "disabled",
                                     "magmareg", "magmaWeekly", "capella_mock",
                                     "magmanew", "ipv6", "capella_weekly",
                                     "vector_search_large", "vector_search",
                                     "columnar"],
                            help="Suite type to run")
        parser.add_argument("--component",
                            dest="component",
                            required=True,
                            help="Comma separated components to run regression")
        parser.add_argument("--subcomponent",
                            dest="subcomponent",
                            default=DispatcherDefaults.SUBCOMPONENT,
                            help="Specific jobs to run under the given components. "
                                 "Default 'None' means run all")
        parser.add_argument("--subcomponent_regex",
                            default=DispatcherDefaults.SUBCOMPONENT_REGEX,
                            help="Pass sub component regex like %tls%")
        parser.add_argument("--serverPoolId",
                            dest="serverPoolId",
                            required=True,
                            choices=["regression", "plasma_new",
                                     "12hour", "12hrreg",
                                     "failover", "upgrade", "dev", "durability",
                                     "extendedSanity", "spock12hour", "test",
                                     "2core", "2i", "nserv", "autofailover",
                                     "security", "query", "xdcr", "lww", "bkrs",
                                     "fts", "ipv6", "ipv6hostname", "test_windows",
                                     "12hour_new", "win-ipv6", "jre-less", "RQG",
                                     "temp_query", "jre", "os_certification",
                                     "12hourwin", "magmareg", "project100-sshtest1",
                                     "plasma", "cardconnect_cbse", "dynamic_win",
                                     "temp", "capella", "magmanew", "magmaUpgrade"],
                            help="Server poolId to use for running the regression")
        parser.add_argument("--addPoolId",
                            dest="addPoolId",
                            default=DispatcherDefaults.ADD_POOL_ID,
                            choices=["None", "crdb-fts",
                                     "elastic-fts", "elastic7-fts",
                                     "elastic-xdcr", "new-elastic"],
                            help="Additional poolID to grab the servers. "
                                 "Used mostly in upgrade tests")
        parser.add_argument("--extraParameters",
                            dest="extraParameters",
                            default=DispatcherDefaults.EXTRA_PARAMETERS,
                            help="Extra test runner parameters")
        parser.add_argument("--url",
                            dest="url",
                            default=DispatcherDefaults.URL,
                            help="Url of a build. Used for running toy-builds")
        parser.add_argument("--branch",
                            dest="branch",
                            default=DispatcherDefaults.BRANCH,
                            help="Specify the branch to run the regression. "
                                 "If 'default' auto-pick the branch")
        parser.add_argument("--cherrypick",
                            dest="cherrypick",
                            default=DispatcherDefaults.CHERRYPICK,
                            help="Git cherry-pick command to run before the run."
                                 "Eg: git fetch http://review.couchbase.org/TAF refs/changes/10/12345/1> && git cherry-pick FETCH_HEAD")
        parser.add_argument("--retries",
                            dest="retries",
                            default=DispatcherDefaults.RETRIES,
                            help="Number of retries to perform incase of failures")
        parser.add_argument("--fresh_run",
                            dest="fresh_run",
                            action="store_true",
                            help="Treat the run as fresh_run")
        parser.add_argument("--rerun_condition",
                            dest="rerun_condition",
                            choices=["ALL", "FAILED", "UNSTABLE", "PENDING"],
                            default=DispatcherDefaults.RERUN_CONDITION,
                            help="Rerun condition while rerunning tests")
        parser.add_argument("--rerun_params",
                            dest="rerun_params",
                            default=DispatcherDefaults.RERUN_PARAMS,
                            help="Rerun params for the job."
                                 "Eg: 'failed=<jenkins_job_url>', 'passed=<jenkins_job_url>'")
        parser.add_argument("--executor_suffix",
                            dest="executor_suffix",
                            default=DispatcherDefaults.EXECUTOR_SUFFIX,
                            help="-j option to dispatcher")
        parser.add_argument("--executor_job_parameters",
                            dest="executor_job_parameters",
                            default=DispatcherDefaults.EXECUTOR_JOB_PARAMETERS,
                            help="Send the any job parameters to the executor job. "
                                 "Eg: username=nonroot&password=couchbase&installParameters=timeout=1200,skip_local_download=True")
        parser.add_argument("--check_vm",
                            type=bool,
                            dest="check_vm",
                            default=DispatcherDefaults.CHECK_VM,
                            choices=[True, False],
                            help="Check the vms to see if they are accessible before dispatching the job. "
                                 "Check this option if you want to check the vms (CBQE-6914)")
        parser.add_argument("--initial_version",
                            dest="initial_version",
                            default=DispatcherDefaults.INITIAL_VERSION)
        parser.add_argument("--serverType",
                            dest="serverType",
                            choices=["ON_PREM", "CAPELLA_MOCK", "ELIXIR_ONPREM"],
                            default=DispatcherDefaults.SERVER_TYPE)
        parser.add_argument("--skip_install",
                            dest="skip_install",
                            action="store_true")
        parser.add_argument("--use_dockerized_dispatcher",
                            type=bool,
                            dest="use_dockerized_dispatcher",
                            default=DispatcherDefaults.USE_DOCKERIZED_DISPATCHER,
                            choices=[True, False],
                            help="To run dispatcher as docker (py3 version)")
        parser.add_argument("--no_confirm",
                            dest="no_confirm",
                            action="store_true")
        return parser.parse_args(sys.argv[1:])
