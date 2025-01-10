#!/usr/bin/python3

import json
import os
import re
import shutil
from pprint import pprint

import requests
import sys
import tempfile
from datetime import datetime, timedelta

from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import LookupInOptions
import couchbase.subdocument as subdoc

sys.path = ["..", "../py_constants"] + sys.path
from config.run_analyzer import run_analyzer_db_info as run_analyzer
from lib.arg_parsers.jenkins_log_arg_parser import JenkinsLogParserCmdLineArgs
from lib.sdk_conn import SDKClient


datetime_format = "%Y-%m-%d %H:%M:%S.%f"
daemon_killed = False
install_block = False
test_case_started = False
timestamp_pattern = re.compile(r"\d+-\d+-\d+ \d+:\d+:\d+,\d+")
test_timestamp_pattern = re.compile(r"^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]+) ")
test_complete_line_pattern = re.compile(r"Ran 1 test in (\d+\.\d+s)")
test_num = 0
test_result = None
test_report_delimiter = '=' * 70
test_report_stage = 0
tmp_file = None
final_out_file_name = None
s3_url = "http://cb-logs-qe.s3-website-us-west-2.amazonaws.com"
section_delimitter = "=" * 80
job_details = None
build_aborted_pattern = re.compile(
    r"Build timed out \(after ([0-9]+ [a-zA-Z]+)\)\. "
    r"Marking the build as aborted.")


def log(msg):
    if arguments.only_analyze:
        return
    print(msg)


def print_and_exit(msg, exit_code=1):
    print(msg)
    exit(exit_code)


def process_install_steps(chunks):
    global install_block, timestamp_pattern, tmp_file, final_out_file_name, \
        job_details, run_analyzer

    install_logs = ""
    install_complete = False
    install_failed_pattern = re.compile(r"FAILED ON[\t :]+\d+\.\d+\.\d+\.\d+")
    install_not_started_pattern = re.compile(
        r"NOT STARTED ON:[\t :]+\d+\.\d+\.\d+\.\d+")
    time_elapsed = None
    desc_set_log = re.compile(r"\[description-setter] Description set: (.*)")
    l_idx = 0
    chunk = ['']
    for chunk in chunks:
        tmp_file.write(chunk)
        chunk = chunk.decode("utf-8").split("\n")
        for l_idx, line in enumerate(chunk):
            if "executor_name" not in job_details:
                line_match = re.compile(
                    r"Building remotely on ([0-9a-zA-Z\-_]+) ").findall(line)
                if line_match:
                    job_details["executor_name"] = line_match[0]
            # elif "branch_used" not in job_details:
            #     branch_used_pattern = re.compile(
            #         r"/usr/bin/git rev-parse ([a-zA-Z0-9_\-/]+)\^{commit}")
            #     line_match = branch_used_pattern.findall(line)
            #     if line_match:
            #         job_details["branch_used"] = line_match[0]
            elif "commit_ref" not in job_details:
                commit_ref_pattern = re.compile(
                    r"Checking out Revision ([0-9a-zA-Z]+) \(")
                line_match = commit_ref_pattern.findall(line)
                if line_match:
                    job_details["commit_ref"] = line_match[0]
            # elif "commit_msg" not in job_details:
            #     commit_msg_pattern = re.compile("Commit message: \"(.*)\"")
            #     line_match = commit_msg_pattern.findall(line)
            #     if line_match:
            #         job_details["commit_msg"] = line_match[0]
            # elif "servers" not in job_details:
            #     server_info_pattern = re.compile(
            #         r"the given server info is ([\"0-9.,]+)")
            #     line_match = server_info_pattern.findall(line)
            #     if line_match:
            #         job_details["servers"] = line_match[0].replace("\"", "").split(',')

            desc_log = desc_set_log.match(line)
            if "Starting server installation" in line \
                    or " scripts/new_install.py " in line \
                    or "Trying to check is this url alive" in line:
                install_block = True
            elif "TOTAL INSTALL TIME" in line:
                install_logs += line + "\n"
                install_block = False
                install_complete = True
                elapsed_pattern = re.compile(
                    r".*TOTAL INSTALL TIME = (\d+ seconds)")
                time_elapsed = elapsed_pattern.match(line)
                if time_elapsed:
                    time_elapsed = time_elapsed[1]
                break
            elif desc_log:
                print("Description: %s" % desc_log[1])
                desc_log = desc_log[1].split(" ")
                final_out_file_name = "%s.%s.%s" % (desc_log[0], desc_log[2],
                                                    desc_log[3])

            if install_block:
                install_logs += line + "\n"

        if install_complete:
            break
    install_logs = install_logs.strip()
    ts_occurances = timestamp_pattern.findall(install_logs)
    install_failed_ips = install_failed_pattern.findall(install_logs)
    install_not_started_ips = install_not_started_pattern.findall(install_logs)
    if install_logs == "" or not install_complete or install_failed_ips or install_not_started_ips:
        job_details["run_note"] = "install_failed"
    print("-" * 70)
    print("Install summary:")
    print("-" * 70)
    if ts_occurances:
        print("   Servers.....%s" % job_details["servers"])
        print("   Started.....%s" % ts_occurances[0])
        print("   End time....%s" % ts_occurances[-1])
        print("   Elapsed.....%s" % time_elapsed)
    for ips in [install_failed_ips, install_not_started_ips]:
        if ips:
            print('   ' + '\n'.join(ips))
    print("-" * 70)
    remaining_lines = chunk[l_idx:]
    return install_logs, '\n'.join(remaining_lines)


def process_test_line(line):
    global daemon_killed, test_case_started, test_report_stage,\
        test_num, test_result, datetime_format

    if line.strip() == '':
        return

    if "- End of the daemon log -" in line:
        print("Test aborted due to end of daemon message")
        daemon_killed = True
        return

    if line.startswith("Test Input params:"):
        test_num += 1
        print(f"\nTest #{test_num} ... ", end='')
        test_case_started = True
        return

    if "test_first_recorded_timestamp" not in job_details:
        line_match = test_timestamp_pattern.findall(line)
        if line_match:
            datetime_str = line_match[0].replace(",", ".")
            dt_object = datetime.strptime(datetime_str, datetime_format)
            unix_timestamp = int(dt_object.timestamp())
            job_details["test_first_recorded_timestamp"] = unix_timestamp
    else:
        line_match = test_timestamp_pattern.findall(line)
        if line_match:
            datetime_str = line_match[0].replace(",", ".")
            dt_object = datetime.strptime(datetime_str, datetime_format)
            unix_timestamp = int(dt_object.timestamp())
            job_details["test_last_recorded_timestamp"] = unix_timestamp

    # To check the test is completed
    test_complete_line = test_complete_line_pattern.match(line)
    if test_complete_line:
        if not test_result:
            test_result = "OK"
            job_details["tests"].append({"result": "PASS"})
        print(f"{test_result}\nTest time elapsed....{test_complete_line[1]}")
        test_case_started = False
        test_result = None
        return

    # To check the build is aborted
    line_match = build_aborted_pattern.findall(line)
    if line_match:
        job_details["run_note"] = "build_aborted"
        test_case_started = False
        test_result = "ABORT"
        job_details["tests"].append({"result": "NA"})
        print("Build Timed out")
        return

    if test_case_started:
        if test_report_stage == 0 and line == test_report_delimiter:
            test_report_stage = 1
            print("Test failure report:")
            test_result = "FAILED"
            job_details["tests"].append({"result": "FAIL", "backtrace": ""})
            return
        if test_report_stage > 0:
            if line.replace("-", "") != "":
                (job_details["tests"][-1])["backtrace"] += line + "\n"
            if test_report_stage == 1 and line == '-' * 70:
                test_report_stage = 2
            elif test_report_stage == 2 and line == '-' * 70:
                test_report_stage = 0
                # If error backtrace > 1 KB, then truncate the logs
                if len((job_details["tests"][-1])["backtrace"]) > 1000:
                    job_details["tests"][-1]["backtrace"] = \
                        job_details["tests"][-1]["backtrace"][-1000:]
                return


def process_test_cases(remaining_lines, chunks):
    global daemon_killed
    for line in remaining_lines.split("\n"):
        process_test_line(line)
        if daemon_killed:
            return

    for chunk in chunks:
        tmp_file.write(chunk)
        chunk = chunk.decode("utf-8").split("\n")
        chunk = [remaining_lines[-1] + chunk[0]] + chunk[1:]
        for l_idx, line in enumerate(chunk):
            process_test_line(line)
            if daemon_killed:
                return
        remaining_lines = [chunk[-1]]


def stream_and_process(url_str):
    try:
        with requests.get(url_str, stream=True) as r:
            r.raise_for_status()
            chunks = r.iter_content(chunk_size=8192)
            _, remaining_lines = process_install_steps(chunks)
            process_test_cases(remaining_lines, chunks)
    except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as err:
        print(err)


def fetch_jobs_for_component(server_ip, username, password, bucket_name,
                             version, os_type, component):
    client = SDKClient(server_ip, username, password, bucket_name)
    run_data = client.collection.lookup_in(
        f"{version}_server",
        [subdoc.get(f"os.{os_type}.{component}")],
        LookupInOptions(timeout=timedelta(seconds=30)))
    client.close()
    return run_data.content_as[dict](0)


def record_details(version, j_name, j_details):
    doc_key = f"{version}_{j_name}"
    # Remove redundant data from the sub-doc
    j_details.pop("component", None)
    j_details.pop("subcomponent", None)

    # doc_key always exists since we have called check_if_job_already_parsed()
    # where the doc is created if not exists
    doc = run_analyzer["sdk_client"].get_doc(doc_key).content_as[dict]

    # Parse doc["runs"] list & insert the job in desc order (Last run first)
    index_with_least_job_id = -1
    for index, run_details in enumerate(doc["runs"]):
        run_details["job_id"] = int(run_details["job_id"])
        if j_details["job_id"] > run_details["job_id"]:
            index_with_least_job_id = index
            break
    if index_with_least_job_id != -1:
        doc["runs"].insert(index_with_least_job_id, j_details)
    else:
        # This is the first run getting recorded
        doc["runs"].append(j_details)

    run_analyzer["sdk_client"].upsert_sub_doc(
        doc_key, "runs", doc["runs"], create_parents=True)


def check_if_job_already_parsed(version, j_name, j_details):
    doc_key = f"{version}_{j_name}"

    # To make sure the doc exists and create if not present
    try:
        doc = run_analyzer["sdk_client"].get_doc(doc_key).content_as[dict]
        # Parse doc["runs"] list to check if job_id already exists
        for index, run_details in enumerate(doc["runs"]):
            run_details["job_id"] = int(run_details["job_id"])
            if run_details["job_id"] == j_details["job_id"]:
                print(f"Job {run_details['job_id']} already parsed!")
                return True
    except DocumentNotFoundException:
        doc = {"cb_version": j_details["cb_version"],
               "component": j_details["component"],
               "subcomponent": j_details["subcomponent"],
               "runs": list()}
        run_analyzer["sdk_client"].collection.insert(doc_key, doc)
    return False


if __name__ == '__main__':
    arguments = JenkinsLogParserCmdLineArgs.parse_cmd_arguments()
    if arguments.url:
        url = arguments.url
    else:
        if arguments.version is None:
            print_and_exit("Exiting: Pass --version [Eg: 7.6.0-1000]")

    jobs = None
    jenkins_job = None

    if arguments.build_num:
        jobs = {arguments.job_name: [
            {"displayName": "temp", "olderBuild": False,
             "failCount": "NA", "totalCount": "NA",
             "build_id": arguments.build_num}]}
    elif arguments.gb_ip and arguments.gb_bucket \
            and arguments.os_type and arguments.component:
        jobs = fetch_jobs_for_component(
            arguments.gb_ip, arguments.username, arguments.password,
            arguments.gb_bucket, arguments.version,
            arguments.os_type.upper(), arguments.component.upper())
        arguments.dont_save_content = True
    else:
        print_and_exit("Exiting: Pass --build_num")

    if arguments.store_results_to_analyzer:
        run_analyzer["sdk_client"] = SDKClient(
            run_analyzer["host"],
            run_analyzer["username"],
            run_analyzer["password"],
            run_analyzer["bucket_name"])
        run_analyzer["sdk_client"].select_collection(
            run_analyzer["scope"], run_analyzer["collection"])
    delete_tmp_file_flag = True if arguments.dont_save_content else False
    tmp_file = tempfile.NamedTemporaryFile(dir="/tmp",
                                           delete=delete_tmp_file_flag)
    result_tbl = dict()
    for job_name, runs in jobs.items():
        print(section_delimitter)
        print("Job::%s, Total runs: %s" % (job_name, len(runs)))
        print(section_delimitter)
        job_info_json_url = None
        for run in runs[::-1]:
            # Params to read from job's jenkins parameters
            component = None
            subcomponent = None
            cb_version = None
            servers = None
            branch = None
            slave_label = None

            test_num = 0
            is_best_run = ""
            if arguments.url:
                url = arguments.url
            elif "url" in run:
                jenkins_job = (run["url"].strip("/")).split('/')[-1]
                if jenkins_job in ["test_suite_executor-TAF",
                                   "test_suite_executor"]:
                    job_url = "%s/%s/jenkins_logs/%s/%s" \
                              % (s3_url, arguments.version, jenkins_job,
                                 run["build_id"])
                    url = f"{job_url}/consoleText.txt"
                    job_info_json_url = f"{job_url}/jobinfo.json"
                else:
                    continue
            else:
                if arguments.repo == "TAF":
                    jenkins_job = "test_suite_executor-TAF"
                elif arguments.repo == "testrunner":
                    jenkins_job = "test_suite_executor"
                job_url = "%s/%s/jenkins_logs/%s/%s" \
                          % (s3_url, arguments.version, jenkins_job,
                             run["build_id"])
                url = f"{job_url}/consoleText.txt"
                job_info_json_url = f"{job_url}/jobinfo.json"
            if not run["olderBuild"]:
                is_best_run = " (Best run)"
                result_tbl[run["displayName"]] = [run["failCount"],
                                                  run["totalCount"]]
            if arguments.check_only_best_run and not is_best_run:
                continue

            if job_info_json_url:
                resp_txt = requests.get(job_info_json_url).text
                try:
                    job_info = json.loads(requests.get(job_info_json_url).text)
                    for action in job_info["actions"]:
                        if "_class" in action \
                                and action["_class"] == "hudson.model.ParametersAction":
                            for parameter in action["parameters"]:
                                if parameter["name"] == "version_number":
                                    cb_version = parameter["value"]
                                elif parameter["name"] == "servers":
                                    servers = parameter["value"].replace("\"", "")
                                    servers = servers.split(",")
                                elif parameter["name"] == "component":
                                    component = parameter["value"]
                                elif parameter["name"] == "subcomponent":
                                    subcomponent = parameter["value"]
                                elif parameter["name"] == "branch":
                                    branch = parameter["value"]
                                elif parameter["name"] == "slave":
                                    slave_label = parameter["value"]
                except json.decoder.JSONDecodeError:
                    pass

            if not all([component, subcomponent, cb_version]):
                # Skip recording if component/subcomponent is unknown
                continue

            job_details = {"component": component,
                           "subcomponent": subcomponent,
                           "servers": servers,
                           "cb_version": cb_version,
                           "branch": branch,
                           "slave_label": slave_label,
                           "run_note": None,
                           "job_id": int(run["build_id"]),
                           "tests": list()}

            # Check if the same build has already parsed by us
            if arguments.store_results_to_analyzer \
                    and job_name != "dummy" \
                    and check_if_job_already_parsed(arguments.version,
                                                    job_name,
                                                    job_details):
                continue

            print("Parsing URL: %s %s" % (url, is_best_run))
            try:
                stream_and_process(url)
            except Exception as e:
                print(e)

            if arguments.store_results_to_analyzer and job_name != "dummy":
                result = set([t_test["result"] for t_test in job_details["tests"]])
                if len(result) == 1 and result.pop() == "PASS":
                    job_details["run_note"] = "PASS"
                record_details(arguments.version, job_name, job_details)
            else:
                pprint(job_details)
            if not arguments.dont_save_content:
                user_input = input("Do you want to save this log ? [y/n]: ")
                tmp_file.close()
                if user_input.strip() in ["y", "Y"]:
                    print("Saving content into ./%s" % final_out_file_name)
                    shutil.move(tmp_file.name, "./%s" % final_out_file_name)
                else:
                    os.remove(tmp_file.name)
        print("End of job: %s" % job_name)
        print("")

    test_desc_len = 80
    print("| %s|%s|%s|" % ("-" * test_desc_len, "-" * 8, "-" * 7))
    print("| %s|%s|%s|" % ("Description".ljust(test_desc_len, "."),
                           " Failed ", " Total "))
    print("| %s|%s|%s|" % ("-" * test_desc_len, "-" * 8, "-" * 7))
    for job_name, result in result_tbl.items():
        print("| %s %s %s " % (job_name.ljust(test_desc_len, "."),
                               str(result[0]).rjust(8, " "),
                               str(result[1]).rjust(7, " ")))
    print("| %s|%s|%s|" % ("-" * test_desc_len, "-" * 8, "-" * 7))
