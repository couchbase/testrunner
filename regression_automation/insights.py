from pprint import pprint
import sys
import hashlib

# import pandas as pd
import torch
from couchbase.exceptions import DocumentNotFoundException
from transformers import RobertaTokenizer, RobertaModel


sys.path = ["..", "../py_constants"] + sys.path

from config.run_analyzer import run_analyzer_db_info as run_analyzer
from config.run_analyzer import error_db_info
from lib.arg_parsers.insights_arg_parser import InsightsCmdLineArgs
from lib.sdk_conn import SDKClient
from regression_automation.trigger_dispatcher import RegressionDispatcher


def get_backtrace_embedding(backtrace):
    # Tokenize and encode the backtrace
    inputs = tokenizer(backtrace, return_tensors='pt', truncation=True,
                       padding=True, max_length=512)
    with torch.no_grad():
        # Pass through the CodeBERT model
        outputs = model(**inputs)

    # Use the [CLS] token's embedding as the representation of the backtrace
    embedding = outputs.last_hidden_state[:, 0, :].squeeze().numpy()
    return embedding


def get_rerun_jobs_for_failed_runs(t_install_failed_jobs):
    """
    :param t_install_failed_jobs: List of failed jobs data
    :return rerun_dict: Format:
      {
        "component_1": {
          "sub_comp_1": {
            <branch>: {
              "db_doc_key": <doc_key>,
              "jenkins_job_num": <jenkins_job_num>,
              "automation_action": <bool>
            }
          }
        },
        "component_2": {
          "sub_comp_1": {
            <branch>: {
              "db_doc_key": <doc_key>,
              "jenkins_job_num": <jenkins_job_num>,
              "automation_action": <bool>
            }
          }
        }
      }
    """
    rerun_dict = dict()
    for t_failed_job in t_install_failed_jobs:
        component = t_failed_job[0]
        subcomponent = t_failed_job[1]
        job_run_branch = t_failed_job[2]
        jenkins_job_id = t_failed_job[8]
        automation_action = t_failed_job[10]
        db_doc_key = t_failed_job[11]

        if component not in rerun_dict:
            rerun_dict[component] = dict()
        if subcomponent not in rerun_dict[component]:
            rerun_dict[component][subcomponent] = dict()

        if "automation_action" in rerun_dict[component][subcomponent] \
                and rerun_dict[component][subcomponent]["automation_action"] is True:
            # Subcomponent has been processed for rerun in prev. iterations
            continue
        elif automation_action == "rerun_triggered":
            # Job is already triggered for rerun during prev. script run.
            # This can happen if we trigger this script twice by mistake.
            rerun_dict[component][subcomponent]["automation_action"] = True
            continue

        rerun_dict[component][subcomponent][job_run_branch] = dict()
        rerun_dict[component][subcomponent][job_run_branch]["db_doc_key"] = db_doc_key
        rerun_dict[component][subcomponent][job_run_branch]["jenkins_job_num"] = jenkins_job_id
        if automation_action == "rerun_triggered":
            # Set this so that other failures for the same subcomponent
            # won't be taken any action (Assumption here is,
            # the job_nums are in decr order as per parse_test_logs.py script)
            rerun_dict[component][subcomponent]["automation_action"] = True
    return rerun_dict


def update_automation_action_field(sdk_connection, docs_info, action_val):
    for doc_info in docs_info:
        doc_key = doc_info["db_doc_key"]
        res_doc = sdk_connection.get_doc(doc_key).content_as[dict]
        for t_run in res_doc["runs"]:
            if int(t_run["job_id"]) == int(doc_info["jenkins_job_num"]):
                t_run["automation_action"] = action_val
                sdk_connection.upsert_doc(doc_key, res_doc)
                break


def trigger_rerun(rerun_dict):
    """
    :param rerun_dict: Dict returned from get_rerun_jobs_for_failed_runs()
    """
    run_analyzer["sdk_client"].select_collection(
        run_analyzer["scope"], run_analyzer["collection"])

    run_map = dict()
    for component_name, components_dict in rerun_dict.items():
        for subcomponent_name, sub_comp_dict in components_dict.items():
            for branch_to_run, job_info_dict in sub_comp_dict.items():
                if "db_doc_key" in job_info_dict:
                    os_to_run = job_info_dict["db_doc_key"].split('_')[1].split("-")[0]
                    if os_to_run not in run_map:
                        run_map[os_to_run] = dict()
                    if branch_to_run not in run_map[os_to_run]:
                        run_map[os_to_run][branch_to_run] = dict()
                    if component_name not in run_map[os_to_run][branch_to_run]:
                        run_map[os_to_run][branch_to_run][
                            component_name] = dict()
                    run_map[os_to_run][branch_to_run][component_name][
                        subcomponent_name] = job_info_dict

    for os_name, os_dict in run_map.items():
        for branch_name, branch_dict in os_dict.items():
            for component_name, comp_dict in branch_dict.items():
                subcomponents = list()
                job_info_dicts = list()
                for sub_comp_name, job_info in comp_dict.items():
                    subcomponents.append(sub_comp_name)
                    job_info_dicts.append(job_info)

                prefix = f"{os_name} :: {branch_name} :: {component_name}"
                if not subcomponents:
                    print(f"{prefix} - Nothing to trigger")
                    continue

                print(f"{prefix} :: {subcomponents}")
                # Below two lines should be a transaction
                update_automation_action_field(run_analyzer["sdk_client"],
                                               job_info_dicts,
                                               action_val="rerun_triggered")
                # Trigger the rerun now
                dispatcher_trigger_result = RegressionDispatcher(
                    os=os_name,
                    version_number=arguments.version,
                    suite="12hr_weekly",
                    component=component_name,
                    subcomponent=",".join(subcomponents),
                    branch=branch_name,
                    use_dockerized_dispatcher=True,
                    no_confirm=True).run()
                print(f"Rerun triggered: ")
                pprint(dispatcher_trigger_result)


print("Parsing command line args")
arguments = InsightsCmdLineArgs.parse_command_line_arguments()
collection = f"`{run_analyzer['bucket_name']}`.`{run_analyzer['scope']}`.`{run_analyzer['collection']}`"
if arguments.component:
    query_str = f"SELECT meta().id,* FROM {collection} " \
                f"WHERE cb_version='{arguments.version}'" \
                f" AND component='{arguments.component}'"
elif arguments.subcomponent:
    query_str = f"SELECT meta().id,* FROM {collection} " \
                f"WHERE cb_version='{arguments.version}'" \
                f" AND subcomponent LIKE '%{arguments.subcomponent}%'"
else:
    InsightsCmdLineArgs.parse_command_line_arguments(["--help"])
    exit(1)

print("Connecting SDK client")
run_analyzer["sdk_client"] = SDKClient(
    run_analyzer["host"],
    run_analyzer["username"],
    run_analyzer["password"],
    run_analyzer["bucket_name"])
run_analyzer["sdk_client"].select_collection(
    run_analyzer["scope"], run_analyzer["collection"])

print(f"Fetching data from {collection}")
rows = run_analyzer["sdk_client"].cluster.query(query_str)

data = list()
error_data = dict()
backtraces_hash_map = dict()
tests_failed_jobs = list()
install_failed_jobs = list()
subcomponents_to_rerun = set()
other_warnings = list()

print("Parsing each run for subcomponents")
for row in rows:
    if arguments.parse_last_run_only:
        run = row["run_analysis"]["runs"][0]
        if run["run_note"] != "PASS":
            data.append([
                row["run_analysis"]["component"],       # 0
                row["run_analysis"]["subcomponent"],    # 1
                run.get("branch", "NA"),                # 2
                run.get("commit_ref", "NA"),            # 3
                run.get("slave_label", "NA"),           # 4
                run.get("executor_name", "NA"),         # 5
                run.get("servers", "NA"),               # 6
                run.get("run_note", "install_failed"),  # 7
                run.get("job_id", "NA"),                # 8
                run.get("tests", []),                   # 9
                run.get("automation_action", "NA"),     # 10
                row['id'],                              # 11
            ])
    else:
        runs = row["run_analysis"]["runs"]
        subcomponent_jobs = list()
        test_passed = False
        for r_index, run in enumerate(runs):
            data_entry = [
                row["run_analysis"]["component"],       # 0
                row["run_analysis"]["subcomponent"],    # 1
                run.get("branch", "NA"),                # 2
                run.get("commit_ref", "NA"),            # 3
                run.get("slave_label", "NA"),           # 4
                run.get("executor_name", "NA"),         # 5
                run.get("servers", "NA"),               # 6
                run.get("run_note", "install_failed"),  # 7
                run.get("job_id", "NA"),                # 8
                run.get("tests", []),                   # 9
                run.get("automation_action", "NA"),     # 10
                row['id'],                              # 11
            ]
            # If run_note == "PASS", we can break without validating other runs
            if data_entry[7] == "PASS":
                test_passed = True
                if r_index != 0:
                    other_warnings.append(f"WARNING!! {data_entry[0]}::{data_entry[1]} has rerun after job passed")
                break
            subcomponent_jobs.append(data_entry)

        if not test_passed:
            data.extend(subcomponent_jobs)

# Load CodeBERT model and tokenizer
tokenizer = RobertaTokenizer.from_pretrained('microsoft/codebert-base')
model = RobertaModel.from_pretrained('microsoft/codebert-base')

print("Opening SDK for error hash comparison")
error_db_info["sdk_client"] = SDKClient(
    error_db_info["host"],
    error_db_info["username"],
    error_db_info["password"],
    error_db_info["bucket_name"])
error_db_info["sdk_client"].select_collection(
    error_db_info["scope"], error_db_info["collection"])

query_str = \
    f"SELECT * FROM " \
    f"`{error_db_info['bucket_name']}`.`{error_db_info['scope']}`.`{error_db_info['collection']}`" \
    f" WHERE vector_hash='%s'"

for run in data:
    if run[7] == "install_failed":
        install_failed_jobs.append(run)
        continue

    tests_failed_jobs.append(run)
    # run[9] is the list the individual tests run in the given run
    for test_num, test in enumerate(run[9]):
        if 'backtrace' not in test:
            continue
        err_vector = get_backtrace_embedding(test["backtrace"])
        err_vector_pylist = err_vector.tolist()
        vector_hash = hashlib.sha256(err_vector.tobytes()).hexdigest()
        if vector_hash not in error_data:
            error_data[vector_hash] = list()
            backtraces_hash_map[vector_hash] = test["backtrace"]
        error_data[vector_hash].append({"component": run[0],
                                        "subcomponent": run[1],
                                        "job_id": run[8],
                                        "test_num": test_num+1})
        try:
            error_db_info["sdk_client"].collection.get(vector_hash)
        except DocumentNotFoundException:
            doc = {"vector": err_vector_pylist, "vector_hash": vector_hash,
                   "failure_history": list()}
            error_db_info["sdk_client"].collection.insert(vector_hash, doc)

        rows = error_db_info["sdk_client"].cluster.query(
            query_str % vector_hash)
        for row in rows:
            row = row[error_db_info['collection']]
            if row["vector"] != err_vector_pylist:
                continue
            for failed_hist in row["failure_history"]:
                if failed_hist["component"] == arguments.component \
                        and failed_hist["subcomponent"] == run[1] \
                        and failed_hist["cb_version"] == arguments.version \
                        and failed_hist["test_num"] == test_num+1:
                    break
            else:
                # Given error not present in the known error history
                failure_history = row["failure_history"]
                failure_history.append({"component": arguments.component,
                                        "subcomponent": run[1],
                                        "cb_version": arguments.version,
                                        "test_num": test_num+1,
                                        "job_id": run[8]})
                error_db_info["sdk_client"].upsert_sub_doc(
                    vector_hash, "failure_history", failure_history)

print("*" * 100)
print(" " * 20 + "End of Analysis")

if install_failed_jobs:
    print("*" * 100)
    print("Install failed jobs: ")
    pprint(install_failed_jobs)
    print("*" * 100)

print()

if error_data:
    print("*" * 100)
    print("Other error insights: ")
    pprint(error_data)
    for hash_val, failed_jobs in error_data.items():
        print("::::: Back Trace ::::::")
        print(backtraces_hash_map[hash_val])
        print(f"Jobs / Tests failing - {len(failed_jobs)}")
        print(failed_jobs)
        for failed_job in failed_jobs:
            subcomponents_to_rerun.add(failed_job["subcomponent"])
        print("-" * 100)
        print()
    print("*" * 100)

if subcomponents_to_rerun:
    print("Rerun:")
    print(','.join(subcomponents_to_rerun))

if other_warnings:
    print("!!! Warnings !!!")
    for warning_str in other_warnings:
        print(warning_str)

jobs_to_rerun = list()
if arguments.rerun_failed_jobs:
    jobs_to_rerun.extend(tests_failed_jobs)
if arguments.rerun_failed_install:
    jobs_to_rerun.extend(install_failed_jobs)

rerun_jobs = get_rerun_jobs_for_failed_runs(jobs_to_rerun)
trigger_rerun(rerun_jobs)

print("Closing SDK clients")
run_analyzer["sdk_client"].close()
error_db_info["sdk_client"].close()
