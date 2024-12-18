from argparse import ArgumentParser


class JenkinsLogParserCmdLineArgs(object):
    @staticmethod
    def parse_cmd_arguments():
        parser = ArgumentParser(description="Paser for test logs")
        parser.add_argument("--server_ip", dest="gb_ip", default="",
                            help="Server IP on which Couchbase db is hosted")
        parser.add_argument("--username", dest="username",
                            default="Administrator",
                            help="Username to use login")
        parser.add_argument("--password", dest="password", default="password",
                            help="Password to use login")
        parser.add_argument("--bucket", dest="gb_bucket", default="",
                            help="Greenboard bucket name")
        parser.add_argument("--os", dest="os_type", default="debian",
                            help="Target OS for which to parse the jobs")
        parser.add_argument("--component", dest="component", default="",
                            help="Target component for which to parse the jobs")

        parser.add_argument("-v", "--version", dest="version", default=None,
                            help="Version on which the job has run")
        parser.add_argument("-b", "--build_num", dest="build_num",
                            default=None,
                            help="Build number of jenkins run")
        parser.add_argument("--job_name", dest="job_name", default="dummy",
                            help="Name given in the Greenboard subdoc. If 'dummy' don't save")

        parser.add_argument("--url", dest="url", default=None,
                            help="Use this URL to parse the logs directly")

        parser.add_argument("--repo", dest="repo", default='TAF',
                            help="Repo using which the logs are generated",
                            choices=["TAF"])

        parser.add_argument("--store_results_to_analyzer",
                            dest="store_results_to_analyzer", default=False,
                            action="store_true",
                            help="Don't save job_details into db for further insights")
        parser.add_argument("--dont_save", dest="dont_save_content",
                            default=False,
                            action="store_true",
                            help="Won't save the content locally after parsing")
        parser.add_argument("--only_best_run", dest="check_only_best_run",
                            default=False, action="store_true",
                            help="Parse only best run logs and discard other "
                                 "other runs for the sub-component")
        return parser.parse_args()
