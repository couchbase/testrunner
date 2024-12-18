from argparse import ArgumentParser


class TestCaseParserCmdLineArgs(object):
    @staticmethod
    def parse_cmd_arguments():
        parser = ArgumentParser(description="Get test cases")
        parser.add_argument("--server_ip", dest="gb_ip", required=True,
                            help="Server IP on which Couchbase db is hosted")
        parser.add_argument("--username", dest="username", default="Administrator",
                            help="Username to use login")
        parser.add_argument("--password", dest="password", default="password",
                            help="Password to use login")
        parser.add_argument("--bucket", dest="gb_bucket", default="greenboard",
                            help="Greenboard bucket name")
        parser.add_argument("--os", dest="os_type", default="DEBIAN",
                            help="Target OS for which to parse the jobs")
        parser.add_argument("--component", dest="component",
                            help="Target component for which to parse the jobs")

        parser.add_argument("-v", "--version", dest="version", required=True,
                            help="Couchbase version on which the job has run")

        parser.add_argument("--job_url", dest="job_url", default=None,
                            help="Job URL excluding the build_num")
        parser.add_argument("--job_num", dest="job_num", default=None,
                            help="Job's build_num")
        parser.add_argument("--job_name", dest="job_name", default=None,
                            help="Job_name used in creating the greenboard entry")

        return parser.parse_args()
