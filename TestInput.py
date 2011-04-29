import getopt
from builds.build_query import BuildQuery
import logger
import ConfigParser

#class to parse the inputs either from command line or from a ini file
#command line supports a subset of
# configuration
# which tests
# ideally should accept a regular expression

class TestInputSingleton():
    input = None

class TestInput(object):

    def __init__(self):
        self.servers = []
        self.membase_settings = None
        self.membase_build = None
        self.test_params = {}
        #servers , each server can have u,p,port,directory


class TestInputServer(object):
    def __init__(self):
        self.ip = ''
        self.ssh_username = ''
        self.ssh_password = ''
        self.ssh_key = ''
        self.rest_username = ''
        self.rest_password = ''
        self.port = ''
        self.cli_path = ''

class TestInputMembaseSetting(object):

    def __init__(self):
        self.rest_username = ''
        self.rest_password = ''

class TestInputBuild(object):
    def __init__(self):
        self.version = ''
        self.url = ''

# we parse this and then pass it on to all the test case
class TestInputParser():


    @staticmethod
    def get_test_input(argv):
        #if file is given use parse_from_file
        #if its from command line
        (opts, args) = getopt.getopt(argv[1:],'h:t:c:v:s:i:p:', [])
        #first let's loop over and find out if user has asked for help
        #if it has i
        params = {}
        has_ini = False
        ini_file = ''
        for option,argument in opts:
            if option == '-h':
                print 'usage'
                return
            if option == '-i':
                has_ini = True
                ini_file = argument
            if option == '-p':
                print 'test specific parameters'
                pairs = argument.split(',')
                for pair in pairs:
                    key_value = pair.split('=')
                    params[key_value[0]] = key_value[1]

        if has_ini:
            input = TestInputParser.parse_from_file(ini_file)
            #now let's get the test specific parameters
        else:
            input = TestInputParser.parse_from_command_line(argv)
        input.test_params = params
        return input

    @staticmethod
    def parse_from_file(file):
        servers = []
        ips = []
        input = TestInput()
        config = ConfigParser.ConfigParser()
        config.read(file)
        sections = config.sections()
        global_properties = {}
        for section in sections:
            if section == 'servers':
                ips = TestInputParser.get_server_ips(config,section)
            elif section == 'build':
                input.membase_build = TestInputParser.get_membase_build(config,section)
            elif section == 'membase':
                input.membase_settings = TestInputParser.get_membase_settings(config,section)
            elif  section == 'global':
                #get global stuff and override for those unset
                for option in config.options(section):
                    global_properties[option] = config.get(section,option)
        #create one server object per ip

        for ip in ips:
            servers.append(TestInputParser.get_server(ip,config))

        for server in servers:
            if server.ssh_username == '' and 'username' in global_properties:
                server.ssh_username = global_properties['username']
            if server.ssh_password == '' and 'password' in global_properties:
                server.ssh_password = global_properties['password']
            if server.ssh_key == '' and 'ssh_key' in global_properties:
                server.ssh_key = global_properties['ssh_key']
            if not server.port and 'port' in global_properties:
                server.port = global_properties['port']
            if server.cli_path == '' and 'cli' in global_properties:
                server.ssh_username = global_properties['cli']
            if server.rest_username == '' and input.membase_settings.rest_username != '':
                server.rest_username = input.membase_settings.rest_username
            if server.rest_password == '' and input.membase_settings.rest_password != '':
                server.rest_password = input.membase_settings.rest_password



        input.servers = servers
        return input

    @staticmethod
    def get_server_ips(config,section):
        ips = []
        options = config.options(section)
        for option in options:
            ips.append(config.get(section,option))
        return ips


    @staticmethod
    def get_server(ip,config):
        server = TestInputServer()
        server.ip = ip
        for section in config.sections():
            if section == ip:
                options = config.options(section)
                for option in options:
                    if option == 'username':
                        server.ssh_username = config.get(section,option)
                    if option == 'password':
                        server.ssh_password = config.get(section,option)
                    if option == 'cli':
                        server.cli_path = config.get(section,option)
                    if option == 'ssh_key':
                        server.ssh_key = config.get(section,option)
                    if option == 'port':
                        server.port = config.get(section,option)
                break
                #get username
                #get password
                #get port
                #get cli_path
                #get key
        return server

    @staticmethod
    def get_membase_build(config,section):
        membase_build = TestInputBuild()
        for option in config.options(section):
            if option == 'version':
                pass
            if option == 'url':
                pass
        return membase_build

    @staticmethod
    def get_membase_settings(config,section):
        membase_settings = TestInputMembaseSetting()
        for option in config.options(section):
            if option == 'rest_username':
                membase_settings.rest_username = config.get(section,option)
            if option == 'rest_password':
                membase_settings.rest_password = config.get(section,option)
        return membase_settings


        


    @staticmethod
    def parse_from_command_line(argv):

        input = TestInput()

        try:
            # -f : won't be parse here anynore
            # -s will have comma separated list of servers
            # -t : wont be parsed here anymore
            # -v : version
            # -u : url
            # -b : will have the path to cli
            # -k : key file
            # -p : for smtp ( taken care of by jenkins)
            # -o : taken care of by jenkins
            servers = []
            input_build = None
            membase_setting = None
            (opts, args) = getopt.getopt(argv[1:],'h:t:c:v:s:i:p:', [])
            #first let's loop over and find out if user has asked for help
            need_help = False
            for option,argument in opts:
                if option == "-h":
                    print 'usage...'
                    need_help = True
                    break
            if need_help:
                return


            #first let's populate the server list and the version number
            for option,argument in opts:
                if option == "-s":
                    #handle server list
                    servers = TestInputParser.handle_command_line_s(argument)
                elif option == "-u" or option == "-v":
                    input_build = TestInputParser.handle_command_line_u_or_v(option,argument)

            #now we can override the username pass and cli_path info
            for option,argument in opts:
                if option == "-k":
                    #handle server list
                    for server in servers:
                        if server.ssh_key == '':
                            server.ssh_key = argument
                elif option == "--username":
                    #handle server list
                    for server in servers:
                        if server.ssh_username == '':
                            server.ssh_username = argument
                elif option == "--password":
                    #handle server list
                    for server in servers:
                        if server.ssh_password == '':
                            server.ssh_password = argument
                elif option == "-b":
                    #handle server list
                    for server in servers:
                        if server.cli_path == '':
                            server.cli_path = argument
            # loop over stuff once again and set the default
            # value
            for server in servers:
                if server.ssh_username == '':
                    server.ssh_username = 'root'
                if server.ssh_password == '':
                    server.ssh_password = 'northscale!23'
                if server.cli_path == '':
                    server.cli_path = '/opt/membase/bin/'
                if not server.port:
                    server.port = 8091
            input.servers  = servers
            input.membase_build = input_build
            input.membase_settings = membase_setting
            return input
        except Exception:
            log = logger.Logger.get_logger()
            log.error("unable to parse input arguments")
            raise

    @staticmethod
    def handle_command_line_u_or_v(option,argument):
        input_build = TestInputBuild()
        if option == "-u":
            # let's check whether this url exists or not
            # let's extract version from this url
            pass
        if option == "-v":
            allbuilds = BuildQuery().get_all_builds()
            for build in allbuilds:
                if build.product_version == argument:
                    input_build.url = build.url
                    input_build.version = argument
                    break
        return input_build


    #returns list of server objects
    @staticmethod
    def handle_command_line_s(argument):
        #ip:port:username:password:clipath

        ips = argument.split(",")
        servers = []

        for ip in ips:
            server = TestInputServer()
            if ip.find(":") == -1:
                pass
            else:
                info = ip.split(":")
                #info[0] : ip
                #info[1] : port
                #info[2] :username
                #info[3] : password
                #info[4] : cli path
                server.ip = info[0]
                server.port = info[1]
                server.ssh_username = info[2]
                server.ssh_password = info[3]
                server.cli_path = info[4]
                servers.append(server)

        return servers
