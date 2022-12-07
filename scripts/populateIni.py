import json
from optparse import OptionParser
import configparser
from collect_server_info import memInfoRunner
from TestInput import TestInputServer

# takes an ini template as input, standard out is populated with the server pool
# need a descriptor as a parameter

# need a timeout param

def main():
    print('in main')
    usage = '%prog -i inifile -o outputfile -s servers'
    parser = OptionParser(usage)
    parser.add_option('-s', '--servers', dest='servers')
    parser.add_option('-d', '--addPoolServerId', dest='addPoolServerId', default=None)
    parser.add_option('-a', '--addPoolServers', dest='addPoolServers', default=None)
    parser.add_option('-i', '--inifile', dest='inifile')
    parser.add_option('-o', '--outputFile', dest='outputFile')
    parser.add_option('-p', '--os', dest='os')
    parser.add_option('-k', '--keyValue', dest='keyValue')
    parser.add_option('-r', '--replaceValue', dest='replaceValue')
    options, args = parser.parse_args()

    print('the ini file is', options.inifile)
    servers = []
    DEFAULT_LINUX_USER = 'root'
    DEFAULT_LINUX_PWD = 'couchbase'
    DEFAULT_WIN_USER = 'Administrator'
    DEFAULT_WIN_PWD = 'Membase123'

    print('the given server info is', options.servers)

    if options.servers:
        if not options.servers.startswith('['):
            options.servers='['+options.servers+']'
        servers = json.loads(options.servers)
        # Sort servers by total memory
        test_servers = []
        for server_ip in servers:
            server = TestInputServer()
            server.ip = server_ip
            server.os = options.os
            if 'windows' in options.os:
                server.ssh_username = DEFAULT_WIN_USER
                server.ssh_password = DEFAULT_WIN_PWD
            else:
                if options.inifile:
                    with open(options.inifile, 'rt') as tempfile:
                        for line in tempfile:
                            if line.startswith('username:'):
                                server.ssh_username = line.split(':')[1].strip()
                            elif line.startswith('password:'):
                                server.ssh_password = line.split(':')[1].strip()
                            if server.ssh_username and server.ssh_password:
                                break
                if not server.ssh_username:
                    server.ssh_username = DEFAULT_LINUX_USER
                if not server.ssh_password:
                    server.ssh_password = DEFAULT_LINUX_PWD
            test_servers.append(server)
        runner = memInfoRunner(test_servers)
        runner.run()
        orig_servers = servers
        servers = []
        if len(runner.succ) > 0:
            sorted_by_mem = sorted(runner.succ.items(), key=lambda item: int(item[1]))
            print('the servers memory info is', sorted_by_mem)
            for (k,v) in sorted_by_mem:
                servers.append(k)
        for (server, e) in runner.fail:
            print("CAN'T GET MEMORY FROM {0}: {1}".format(server, e))
            servers.append(server)
        for nomemserver in orig_servers:
            if nomemserver not in servers:
                print("CAN'T GET MEMORY FROM {0}: unknown error".format(server))
                servers.append(nomemserver)

    addPoolServers = []

    if options.addPoolServers != None and options.addPoolServers != "None":
        if not options.addPoolServers.startswith('['):
            options.addPoolServers = '[' + options.addPoolServers + ']'
        print('the additional server pool info is', options.addPoolServers)
        addPoolServers = json.loads(options.addPoolServers)

    if options.keyValue:
        if options.outputFile:
            update_config(options.inifile, options.keyValue, options.outputFile)
        else:
            update_config(options.inifile, options.keyValue, None)

    if options.keyValue and options.outputFile:
        f = open(options.outputFile)
    else:
        f = open(options.inifile)

    data = f.readlines()

    for i in range( len(data) ):
          if 'dynamic' in data[i] and servers:
             data[i] = data[i].replace('dynamic', servers[0])
             servers.pop(0)
          elif addPoolServers and options.addPoolServerId in data[i]:
             data[i] = data[i].replace(options.addPoolServerId, addPoolServers[0])
             addPoolServers.pop(0)

          if 'windows' in options.os:
              if 'username:root' in data[i]:
                  data[i] = data[i].replace('root', DEFAULT_WIN_USER)
              if 'password:couchbase' in data[i]:
                  data[i] = data[i].replace('couchbase', DEFAULT_WIN_PWD)

          if 'es_ssh_username:root' in data[i]:
              data[i] = data[i].replace('es_ssh_username:root', 'username:'+DEFAULT_LINUX_USER)
          if 'es_ssh_password:couchbase' in data[i]:
              data[i] = data[i].replace('es_ssh_password:couchbase', 'password:'+DEFAULT_LINUX_PWD)

          if 'es_ssh_username:Administrator' in data[i]:
              data[i] = data[i].replace('es_ssh_username:Administrator', 'username:'+DEFAULT_LINUX_USER)
          if 'es_ssh_password:Membase123' in data[i]:
              data[i] = data[i].replace('es_ssh_password:Membase123', 'password:'+DEFAULT_LINUX_PWD)

          if options.replaceValue:
              for oldnew in options.replaceValue.split(','):
                  old, new = oldnew.split("=")
                  if old in data[i]:
                    data[i] = data[i].replace(old, new)


    for d in data:
          print(d.strip())

    if options.outputFile:
        f = open(options.outputFile, 'w')
        f.writelines(data)
    else:
        for d in data:
            print(d.strip())

def update_config(config_in_file, json_object, config_out_file):
    config = configparser.RawConfigParser(delimiters=(':', ':'))
    config.read(config_in_file)
    key_values = json.loads(json_object)
    for section in config.sections():
        for key in key_values.keys():
            try:
                old_value = config.get(section, key)
                config.set(section, key, key_values.get(key))
                #print("Replaced {}==> {}".format(old_value,key_values.get(key)))
            except Exception as e:
                #print(e)
                pass

    try:
        if config_out_file:
            with open(config_out_file, 'w') as configfile:
                config.write(configfile, space_around_delimiters=False)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

