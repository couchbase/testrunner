import sys
import requests
from jenkins import Jenkins
from pprint import pprint


sys.path = ["..", "../py_constants"] + sys.path


from lib.dispatcher_arg_parser import DispatcherCommandLineArgs
from cb_constants.jenkins import JenkinsConstants
from cb_constants.rel_branch_map import CB_VERSION_NAME


def confirm_params(no_confirm_flag):
    if no_confirm_flag \
            or str(input("\nIs the params okay ? [y/n] ")).lower() == "y":
        return True
    print("Not triggering")
    return False


def main():
    args = DispatcherCommandLineArgs.parse_arguments()

    if args.branch == "default":
        args.branch = CB_VERSION_NAME[args.version[:3]]
        print(f"Picking branch from the map: {args.branch}")

    build_params = {
        "OS": args.os,
        "version_number": args.version,
        "columnar_version_number": args.columnar_version_number,
        "suite": args.suite,
        "component": args.component,
        "subcomponent": args.subcomponent,
        "subcomponent_regex": args.subcomponent_regex,
        "serverPoolId": args.serverPoolId,
        "addPoolId": args.addPoolId,
        "extraParameters": args.extraParameters,
        "url": args.url,
        "branch": args.branch,
        "cherrypick": args.cherrypick,
        "retries": args.retries,
        "fresh_run": args.fresh_run,
        "rerun_condition": args.rerun_condition,
        "rerun_params": args.rerun_params,
        "executor_suffix": args.executor_suffix,
        "executor_job_parameters": args.executor_job_parameters,
        "check_vm": args.check_vm,
        "initial_version": args.initial_version,
        "serverType": args.serverType,
        "skip_install": args.skip_install,
        "use_dockerized_dispatcher": args.use_dockerized_dispatcher,
        "token": JenkinsConstants.TOKEN}

    pprint(build_params)
    if not confirm_params(args.no_confirm):
        return
    jenkins_obj = Jenkins(JenkinsConstants.OLD_JENKINS_URL)
    last_known_build = jenkins_obj.get_job_info(JenkinsConstants.DEFAULT_DISPATCHER_JOB)["builds"][0]["url"]

    possible_builds = list()
    result = requests.get(f"{JenkinsConstants.OLD_JENKINS_URL}/job/{JenkinsConstants.DEFAULT_DISPATCHER_JOB}/buildWithParameters?", params=build_params)
    if 200 <= int(result.status_code) <= 210:
        last_5_builds = jenkins_obj.get_job_info(JenkinsConstants.DEFAULT_DISPATCHER_JOB)["builds"][:5]
        for t_build in last_5_builds:
            b_url = t_build["url"]
            if b_url == last_known_build:
                break
            possible_builds.append(b_url)
    else:
        print("Failed to dispatch the job")

    build_triggered = None
    params_to_check = [
        "OS", "version_number", "columnar_version_number",
        "suite", "component", "subcomponent", "serverPoolId", "addPoolId",
        "extraParameters", "branch", "cherrypick", "fresh_run",
        "rerun_condition", "rerun_params", "executor_job_parameters",
        "serverType", "use_dockerized_dispatcher"]
    for t_build in possible_builds:
        print(f"Possible build: {t_build}")
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
                            print(f"Mismatch in {param['name']}")
                            break
                else:
                    found_param_match = True

        if found_cause_action and found_param_match:
            build_triggered = build_info
            break

    print(f"Triggered:")
    pprint(build_triggered)


if __name__ == "__main__":
    main()
