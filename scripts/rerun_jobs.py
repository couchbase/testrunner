import json
import os as OS
import subprocess
import sys
import urllib.parse
try:
    import httplib2
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "httplib2"])
    import httplib2
try:
    import requests
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "requests"])
    import requests
import argparse
import find_rerun_job
import get_jenkins_params
import merge_reports
import shutil
import zipfile

host = '172.23.120.87'
bucket_name = 'rerun_jobs'
AWS_LINK = 'http://cb-logs-qe.s3-website-us-west-2.amazonaws.com'
TIMEOUT = 60


def parse_args():
    """
    Parse command line arguments into a dictionary
    :return: Dictionary of parsed command line arguments
    :rtype: dict
    """
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("build_version", type=str,
                                 help="Couchbase build version of the "
                                      "job")
    argument_parser.add_argument("--executor_jenkins_job",
                                 action='store_true',
                                 help="Run with current executor job")
    argument_parser.add_argument("--jenkins_job", action="store_true",
                                 help="Run with current jenkins job")
    argument_parser.add_argument("--install_failure",
                                 action='store_true',
                                 help="Was there install failure in "
                                      "the run?")
    argument_parser.add_argument("--manual_run", action="store_true",
                                 help="Is this a manual rerun of the "
                                      "job")
    argument_parser.add_argument("--run_params", type=str, default="",
                                 help="Testrunner extra params for "
                                      "the job")
    args = vars(argument_parser.parse_args())
    return args


def build_args(build_version, executor_jenkins_job=False,
               jenkins_job=False, install_failure=False):
    """
    Build a dictionary of arguments needed for the program
    :param build_version: Couchbase build version of the job
    :type build_version: str
    :param executor_jenkins_job: Run with current Executor job
    :type executor_jenkins_job: bool
    :param jenkins_job: Run with current jenkins job
    :type jenkins_job: bool
    :param install_failure: Was there install failure in the run?
    :type install_failure: bool
    :return: Dictionary of parameters
    :rtype: dict
    """
    return locals()


def merge_xmls(rerun_document, run_params=""):
    """
    Merge the xml of the runs into a single xml for the jenkins job
    to consume to show the test results
    :param rerun_document: The rerun document containing the details
    of previous runs.
    :type rerun_document: dict
    :return: The merged testsuites from runs
    :rtype: dict
    """
    if not rerun_document:
        testsuites = merge_reports.merge_reports("logs/**/*.xml",
                                                 run_params)
        return testsuites
    print("Merging xmls")
    num_runs = rerun_document['num_runs'] - 1
    valid_run = False
    job = None
    while not valid_run and num_runs > 0:
        job = rerun_document['jobs'][num_runs - 1]
        if job['install_failure']:
            num_runs -= 1
        else:
            valid_run = True
    if not job:
        print("no valid jobs found with run results")
        testsuites = merge_reports.merge_reports("logs/**/*.xml",
                                                 run_params)
        return testsuites
    job_url = job['job_url']
    artifacts = get_jenkins_params.get_js(job_url, "tree=artifacts[*]")
    if not artifacts or len(artifacts['artifacts']) == 0:
        print("Could not find the job. Job might be deleted")
        print("Trying to get the job logs from AWS")
        logs = get_from_aws(rerun_document, job_url)
    else:
        relative_paths = []
        for artifact in artifacts["artifacts"]:
            if artifact["relativePath"].startswith("logs/") and \
                    artifact["relativePath"].endswith(".xml"):
                relative_paths.append(artifact["relativePath"])
        logs = []
        for rel_path in relative_paths:
            xml_data = get_jenkins_params.download_url_data("{0}artifact/"
                                                            "{1}".format(
                job_url, rel_path))
            try:
                file_name = rel_path.split('/')[-1]
                file_name = "Old_Report_{0}".format(file_name)
                f = open(file_name, "w")
                f.writelines(xml_data.decode('utf-8'))
                f.close()
                logs.append(file_name)
            except Exception as e:
                print(e)
        if not logs:
            print("Could not download the artifacts")
            print("Trying to download from AWS")
            logs = get_from_aws(rerun_document, job_url)
    if logs is None or not logs:
        print("Could not download any previous logs")
        logs = []
    logs.append("logs/**/*.xml")
    testsuites = merge_reports.merge_reports(logs, run_params)
    try:
        # Remove old logs from the machine
        try:
            logs.remove("logs/**/*.xml")
        except ValueError:
            pass
        for path in logs:
            OS.remove(path)
    except:
        pass
    print("merged xmls")
    return testsuites


def get_from_aws(rerun_document, job_url):
    """
    Get the previous job's logs from aws.
    :param rerun_document: The rerun document containing reerun details
    :type rerun_document: dict
    :param job_url: Job url of the job whose logs have to be downloaded
    :type job_url: str
    :return: List of downloaded files if successful, else None
    :rtype: list
    """
    build = rerun_document['build']
    job_name = job_url.rstrip('/').split('/')[-2]
    job_build_number = job_url.rstrip('/').split('/')[-1]
    aws_link = '{0}/{1}/jenkins_logs/{2}/{3}' \
               '/archive.zip'.format(AWS_LINK, build, job_name,
                                     job_build_number)
    archive_zip = get_jenkins_params.download_url_data(aws_link)
    if not archive_zip:
        print('Could not get the archive files from aws. ')
        return None
    try:
        file_name = 'old_archive.zip'
        f = open(file_name, "wb")
        f.write(archive_zip)
        f.close()
        archive = zipfile.ZipFile('old_archive.zip')
        extracted_files = []
        for file in archive.namelist():
            if file.startswith("archive/logs/") and file.endswith(
                    ".xml"):
                temp_folder = "temp"
                test_log_file = 'Old_Report_{0}'.format(file.split(
                    '/')[-1])
                archive.extract(file, temp_folder)
                shutil.copyfile("{0}/{1}".format(temp_folder, file),
                                test_log_file)
                shutil.rmtree(temp_folder)
                extracted_files.append(test_log_file)
        OS.remove('old_archive.zip')
        return extracted_files
    except Exception as e:
        print(e)
        return None


def should_rerun_tests(testsuites=None, install_failure=False,
                       retries=0):
    """
    Finds out if the job has to be rerun again based on number of
    failure in the current job, if number of retries has been exceeded
    :param testsuites: The testsuite containing the merged results
    from current and previous runs.
    :type testsuites: dict
    :param install_failure: Was there an install failure in this job
    :type install_failure: bool
    :param retries: Number of times to retry
    :type retries: int
    :return: Boolean telling whether to rerun the job or not
    :rtype: bool
    """
    if install_failure and retries > 0:
        return True
    if retries < 1:
        return False
    should_rerun = False
    for tskey in testsuites.keys():
        tests = testsuites[tskey]['tests']
        for testname in tests.keys():
            testcase = tests[testname]
            errors = testcase['error']
            if errors:
                should_rerun = True
                break
        if should_rerun:
            break
    return should_rerun


def get_rerun_parameters(rerun_document=None, is_rerun=False):
    """
    Get the rerun parameters for the rerun of the job
    :param rerun_document: Document containing the run history of the job
    :type rerun_document: dict
    :param is_rerun: Was this job a rerun
    :type is_rerun: bool
    :return: Re-run parameters to be used in the next job or current job
    :rtype: str
    """
    rerun_params = None
    if not is_rerun and not rerun_document and (rerun_document and
                                               rerun_document['num_runs'] == 1):
        current_job_url = OS.getenv("BUILD_URL")
        rerun_params = "-d failed={}".format(current_job_url)
    num_runs = rerun_document['num_runs']
    valid_run = False
    valid_job = None
    while not valid_run and num_runs > 0:
        job = rerun_document['jobs'][num_runs - 1]
        if job['install_failure']:
            num_runs -= 1
        else:
            job_url = job['job_url']
            artifacts = get_jenkins_params.get_js(job_url, "tree=artifacts[*]")
            if not artifacts or len(artifacts['artifacts']) == 0:
                print("Could not find the job. Job might be deleted. Seeing if the job was saved on s3")
                build = rerun_document['build']
                job_name = job_url.rstrip('/').split('/')[-2]
                job_build_number = job_url.rstrip('/').split('/')[-1]
                aws_link = '{0}/{1}/jenkins_logs/{2}/{3}' \
                           '/archive.zip'.format(AWS_LINK, build, job_name,
                                                 job_build_number)
                archive_zip = get_jenkins_params.download_url_data(aws_link)
                if not archive_zip:
                    print("Job wasn't saved. Trying with older build.")
                    num_runs -= 1
                    continue
            valid_run = True
            valid_job = job
    if valid_run and valid_job:
        job_url = valid_job['job_url']
        rerun_params = "-d failed={}".format(job_url)
    return rerun_params


def run_jenkins_job(url, params):
    """
    Trigger a jenkins job with the url provided and the params to the job
    :param url: Jenkins job url
    :type url: str
    :param params: Parameters to be passed to the job
    :type params: dict
    :return: Response and content of the call
    :rtype: (response, content)
    """
    url = "{0}&{1}".format(url, urllib.parse.urlencode(params))
    print(url)
    response, content = httplib2.Http(timeout=TIMEOUT).request(url,
                                                               'GET')
    return response, content


def rerun_job(args):
    """
    Rerun a job based on the arguments to the program. Determine if a
    rerun has to occur or not
    :param args: Dictionary of arguments to the program
    :type args: dict
    :return: Nothing
    :rtype: None
    """
    build_version = args['build_version']
    executor_jenkins_job = args['executor_jenkins_job']
    jenkins_job = args['jenkins_job']
    install_failure = args['install_failure']
    fresh_run = OS.getenv('fresh_run', False)
    run_params = args['run_params']
    is_rerun_args = find_rerun_job.build_args(build_version,
                                              executor_jenkins_job=executor_jenkins_job,
                                              jenkins_job=jenkins_job,
                                              store_data=True,
                                              install_failure=install_failure)
    is_rerun, rerun_document = find_rerun_job.find_rerun_job(is_rerun_args)
    test_suites = {}
    if is_rerun and not install_failure and (fresh_run != 'true' or
                                             fresh_run is False):
        test_suites = merge_xmls(rerun_document, run_params)
    else:
        test_suites = merge_xmls({}, run_params)
    retry_count = OS.getenv("retries")
    if not retry_count:
        if "retries" in args:
            retry_count = args['retries']
        else:
            retry_count = 0
    if isinstance(retry_count, str):
        retry_count = int(retry_count)
    should_rerun = should_rerun_tests(test_suites, install_failure,
                                      retry_count)
    if not should_rerun:
        print("No more failed tests. Stopping reruns")
        return
    rerun_params = get_rerun_parameters(rerun_document, is_rerun)
    if not rerun_params:
        if install_failure:
            rerun_params = ''
        else:
            return
    if jenkins_job:
        current_job_url = OS.getenv('BUILD_URL')
        current_job_params = get_jenkins_params.get_params(
            current_job_url)
        current_job_params['rerun_params'] = rerun_params
        current_job_params['retries'] = retry_count - 1
        job_url = OS.getenv("JOB_URL")
        job_token = args['token']
        job_url = "{0}buildWithParameters?token={1}".format(job_url,
                                                          job_token)
        response, content = run_jenkins_job(job_url, current_job_params)
        return
    dispatcher_params = OS.getenv('dispatcher_params').lstrip(
        "parameters=")
    dispatcher_params = json.loads(dispatcher_params)
    dispatcher_params['rerun_params'] = rerun_params
    dispatcher_params['retries'] = retry_count - 1
    dispatcher_params['component'] = OS.getenv('component')
    dispatcher_params['subcomponent'] = OS.getenv('subcomponent')
    dispatcher_params['fresh_run'] = "false"
    job_url = dispatcher_params.pop('dispatcher_url')
    job_url = "{0}buildWithParameters?token=extended_sanity".format(
        job_url)
    response, content = run_jenkins_job(job_url, dispatcher_params)


def manual_rerun(args):
    """
    Get the rrerun parameters for manual rerun of the job. Puts the
    parameter into a file to be consumed by  jenkins job
    :param args: Dictionary of arguments to the program
    :type args: dict
    :return: Nothing
    :rtype: None
    """
    build_version = args['build_version']
    executor_jenkins_job = args['executor_jenkins_job']
    jenkins_job = args['jenkins_job']
    is_rerun_args = find_rerun_job.build_args(build_version,
                                              executor_jenkins_job=executor_jenkins_job,
                                              jenkins_job=jenkins_job,
                                              store_data=False,
                                              install_failure=False)
    is_rerun, rerun_document = find_rerun_job.find_rerun_job(
        is_rerun_args)
    if not is_rerun:
        print("This is the first run for this build.")
        return
    rerun_param = get_rerun_parameters(rerun_document, is_rerun)
    if not rerun_param:
        print("Could not find a valid previous build to run with")
        return
    with open("rerun_props_file", 'w') as f:
        f.write("{}".format(rerun_param))


if __name__ == '__main__':
    args = parse_args()
    if args['manual_run']:
        manual_rerun(args)
    else:
        rerun_job(args)
