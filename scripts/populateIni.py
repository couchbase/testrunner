
import sys
import urllib2
import urllib
import httplib2
import json
import string
import time
from optparse import OptionParser


# takes an ini template as input, standard out is populated with the server pool
# need a descriptor as a parameter

# need a timeout param

def main():
    print 'in main'
    usage = '%prog -i inifile -o outputfile -s servers'
    parser = OptionParser(usage)
    parser.add_option('-s','--servers', dest='servers')
    parser.add_option('-d','--addPoolServerId', dest='addPoolServerId')
    parser.add_option('-a','--addPoolServers', dest='addPoolServers')
    parser.add_option('-i','--inifile', dest='inifile')
    parser.add_option('-o','--outputFile', dest='outputFile')
    parser.add_option('-p','--os', dest='os')
    options, args = parser.parse_args()


    print 'the ini file is', options.inifile

    print 'the server info is', options.servers

    servers = json.loads(options.servers)
    addPoolServers = json.loads(options.addPoolServers)

    f = open(options.inifile)
    data = f.readlines()

    for i in range( len(data) ):
          if 'dynamic' in data[i]:
             data[i] = string.replace(data[i], 'dynamic', servers[0])
             servers.pop(0)
          elif options.addPoolServerId in data[i] and addPoolServers:
             data[i] = string.replace(data[i], options.addPoolServerId, addPoolServers)
             addPoolServers.pop(0)

          if options.os == 'windows':
              if 'root' in data[i]:
                  data[i] = string.replace(data[i], 'root', 'Administrator')
              if 'couchbase' in data[i]:
                  data[i] = string.replace(data[i], 'couchbase', 'Membase123')

    for d in data:
          print d,

    f = open(options.outputFile, 'w')
    f.writelines(data)



if __name__ == "__main__":
    main()

