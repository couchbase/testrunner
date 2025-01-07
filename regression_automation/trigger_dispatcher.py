import sys
import requests
from jenkins import Jenkins
from pprint import pprint

sys.path = ["..", "../py_constants"] + sys.path

from regression_automation.defaults import DispatcherDefaults
from lib.arg_parsers.dispatcher_arg_parser import DispatcherCommandLineArgs
from cb_constants.jenkins import JenkinsConstants
from cb_constants.rel_branch_map import CB_VERSION_NAME


class RegressionDispatcher(object):
    def __init__(self, cmd_args=None,
                 os=DispatcherDefaults.OS,
                 version_number=None,
                 columnar_version_number=DispatcherDefaults.COLUMNAR_VERSION_NUMBER,
                 suite=DispatcherDefaults.SUITE,
                 component=DispatcherDefaults.COMPONENT,
                 subcomponent=DispatcherDefaults.SUBCOMPONENT,
                 subcomponent_regex=DispatcherDefaults.SUBCOMPONENT_REGEX,
                 server_pool_id=DispatcherDefaults.SERVER_POOL_ID,
                 add_pool_id=DispatcherDefaults.ADD_POOL_ID,
                 extra_parameters=DispatcherDefaults.EXTRA_PARAMETERS,
                 url=DispatcherDefaults.URL,
                 branch=DispatcherDefaults.BRANCH,
                 cherrypick=DispatcherDefaults.CHERRYPICK,
                 retries=DispatcherDefaults.RETRIES,
                 fresh_run=DispatcherDefaults.FRESH_RUN,
                 rerun_condition=DispatcherDefaults.RERUN_CONDITION,
                 rerun_params=DispatcherDefaults.RERUN_PARAMS,
                 executor_suffix=DispatcherDefaults.EXECUTOR_SUFFIX,
                 executor_job_parameters=DispatcherDefaults.EXECUTOR_JOB_PARAMETERS,
                 check_vm=DispatcherDefaults.CHECK_VM,
                 server_type=DispatcherDefaults.SERVER_TYPE,
                 initial_version=DispatcherDefaults.INITIAL_VERSION,
                 skip_install=DispatcherDefaults.SKIP_INSTALL,
                 use_dockerized_dispatcher=DispatcherDefaults.USE_DOCKERIZED_DISPATCHER,
                 no_confirm=False):
        if cmd_args:
            # If the script is called directly from cmd-line
            self.os = cmd_args.os
            self.version_number = cmd_args.version
            self.columnar_version_number = cmd_args.columnar_version_number
            self.suite = cmd_args.suite
            self.component = cmd_args.component
            self.subcomponent = cmd_args.subcomponent
            self.subcomponent_regex = cmd_args.subcomponent_regex
            self.serverPoolId = cmd_args.serverPoolId
            self.addPoolId = cmd_args.addPoolId
            self.extraParameters = cmd_args.extraParameters
            self.url = cmd_args.url
            self.branch = cmd_args.branch
            self.cherrypick = cmd_args.cherrypick
            self.retries = cmd_args.retries
            self.fresh_run = cmd_args.fresh_run
            self.rerun_condition = cmd_args.rerun_condition
            self.rerun_params = cmd_args.rerun_params
            self.executor_suffix = cmd_args.executor_suffix
            self.executor_job_parameters = cmd_args.executor_job_parameters
            self.check_vm = cmd_args.check_vm
            self.initial_version = cmd_args.initial_version
            self.serverType = cmd_args.serverType
            self.skip_install = cmd_args.skip_install
            self.use_dockerized_dispatcher = cmd_args.use_dockerized_dispatcher
            self.no_confirm = cmd_args.no_confirm
        else:
            # If the script is called from other scripts
            self.os = os
            self.version_number = version_number
            self.columnar_version_number = columnar_version_number
            self.suite = suite
            self.component = component
            self.subcomponent = subcomponent
            self.subcomponent_regex = subcomponent_regex
            self.serverPoolId = server_pool_id
            self.addPoolId = add_pool_id
            self.extraParameters = extra_parameters
            self.url = url
            self.branch = branch
            self.cherrypick = cherrypick
            self.retries = retries
            self.fresh_run = fresh_run
            self.rerun_condition = rerun_condition
            self.rerun_params = rerun_params
            self.executor_suffix = executor_suffix
            self.executor_job_parameters = executor_job_parameters
            self.check_vm = check_vm
            self.initial_version = initial_version
            self.serverType = server_type
            self.skip_install = skip_install
            self.use_dockerized_dispatcher = use_dockerized_dispatcher
            self.no_confirm = no_confirm
        if not self.version_number:
            raise Exception(f"Invalid version_number '{self.version_number}'")

    def confirm_params(self):
        if self.no_confirm or str(input("\nIs the params okay ? [y/n] ")).lower() == "y":
            return True
        print("Not triggering")
        return False

    def run(self):
        if self.branch == "default":
            self.branch = CB_VERSION_NAME[self.version_number[:3]]
            # print(f"Auto selected branch: {self.branch}")

        result_data = dict()
        build_params = {
            "OS": self.os,
            "version_number": self.version_number,
            "columnar_version_number": self.columnar_version_number,
            "suite": self.suite,
            "component": self.component,
            "subcomponent": self.subcomponent,
            "subcomponent_regex": self.subcomponent_regex,
            "serverPoolId": self.serverPoolId,
            "addPoolId": self.addPoolId,
            "extraParameters": self.extraParameters,
            "url": self.url,
            "branch": self.branch,
            "cherrypick": self.cherrypick,
            "retries": self.retries,
            "fresh_run": self.fresh_run,
            "rerun_condition": self.rerun_condition,
            "rerun_params": self.rerun_params,
            "executor_suffix": self.executor_suffix,
            "executor_job_parameters": self.executor_job_parameters,
            "check_vm": self.check_vm,
            "initial_version": self.initial_version,
            "serverType": self.serverType,
            "skip_install": self.skip_install,
            "use_dockerized_dispatcher": self.use_dockerized_dispatcher,
            "token": JenkinsConstants.TOKEN}
        pprint(build_params)

        if not self.confirm_params():
            result_data["status"] = True
            result_data["info"] = "Confirm params failed!"
            return result_data

        jenkins_obj = Jenkins(JenkinsConstants.OLD_JENKINS_URL)
        last_known_build = jenkins_obj.get_job_info(JenkinsConstants.DEFAULT_DISPATCHER_JOB)["builds"][0]["url"]

        possible_builds = list()
        req_result = requests.get(f"{JenkinsConstants.OLD_JENKINS_URL}/job/{JenkinsConstants.DEFAULT_DISPATCHER_JOB}/buildWithParameters?", params=build_params)
        if 200 <= int(req_result.status_code) <= 210:
            last_5_builds = jenkins_obj.get_job_info(JenkinsConstants.DEFAULT_DISPATCHER_JOB)["builds"][:5]
            for t_build in last_5_builds:
                b_url = t_build["url"]
                if b_url == last_known_build:
                    break
                possible_builds.append(b_url)
        else:
            result_data["status"] = False
            result_data["info"] = "Failed to dispatch the job"

        params_to_check = [
            "OS", "version_number", "columnar_version_number",
            "suite", "component", "subcomponent", "serverPoolId", "addPoolId",
            "extraParameters", "branch", "cherrypick", "fresh_run",
            "rerun_condition", "rerun_params", "executor_job_parameters",
            "serverType", "use_dockerized_dispatcher"]
        for t_build in possible_builds:
            # print(f"Possible build: {t_build}")
            b_num = t_build.split('/')[-2]
            build_info = jenkins_obj.get_build_info(JenkinsConstants.DEFAULT_DISPATCHER_JOB, b_num)
            found_cause_action = False
            found_param_match = False
            for action in build_info["actions"]:
                if not action:
                    continue
                if action["_class"] == "hudson.model.CauseAction" \
                        and action["causes"][0]["_class"] == "hudson.model.Cause$RemoteCause" \
                        and "Started by remote host" in action["causes"][0]["shortDescription"]:
                    found_cause_action = True
                if action["_class"] == "hudson.model.ParametersAction":
                    for param in action["parameters"]:
                        if param["name"] in params_to_check:
                            if param["value"] != build_params[param["name"]]:
                                # print(f"Mismatch in {param['name']}")
                                break
                    else:
                        found_param_match = True

            if found_cause_action and found_param_match:
                result_data["status"] = True
                result_data["info"] = "Build triggered"
                result_data["build_info"] = build_info
                break
        return result_data


if __name__ == "__main__":
    args = DispatcherCommandLineArgs.parse_arguments()
    result = RegressionDispatcher(cmd_args=args).run()
    pprint(result)
