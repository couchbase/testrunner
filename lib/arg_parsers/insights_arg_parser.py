import sys
from argparse import ArgumentParser


class InsightsCmdLineArgs(object):
    @staticmethod
    def parse_command_line_arguments(custom_args=None):
        parser = ArgumentParser(description="Paser for test logs")
        parser.add_argument("--component", dest="component",
                            help="Target component for which to parse the jobs")
        parser.add_argument("--subcomponent", dest="subcomponent",
                            help="If component not provided, then search for subcomponent regexp")
        parser.add_argument("--version", dest="version", required=True,
                            help="Version on which the job has run")
        parser.add_argument("--job_name", dest="job_name", default=None,
                            help="Target one job to analyze")
        parser.add_argument("--parse_last_run_only",
                            dest="parse_last_run_only",
                            default=False, action="store_true",
                            help="Runs the script only on the last run of each subcomponent")
        parser.add_argument("--rerun_failed_install", action="store_true",
                            help="If set, will rerun all failed install run")
        parser.add_argument("--rerun_failed_jobs", action="store_true",
                            help="If set, will rerun all jobs with test failures")
        return parser.parse_args(custom_args or sys.argv[1:])
