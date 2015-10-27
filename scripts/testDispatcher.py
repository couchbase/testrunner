
import sys
import urllib2
import urllib
import httplib2
import json
import string
import time
from optparse import OptionParser

from couchbase import Couchbase
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError
from couchbase.n1ql import N1QLQuery


# takes an ini template as input, standard out is populated with the server pool
# need a descriptor as a parameter

# need a timeout param

POLL_INTERVAL = 15
SERVER_MANAGER = '172.23.105.177:8081'
TEST_SUITE_DB = '172.23.105.177'


def getNumberOfServers( iniFile):
    f = open(iniFile)
    contents = f.read()
    f.close()
    return contents.count('dynamic')


def main():

    usage = '%prog -s suitefile -v version -o OS'
    parser = OptionParser(usage)
    parser.add_option('-v','--version', dest='version')
    parser.add_option('-r','--run', dest='run')
    parser.add_option('-o','--os', dest='os')
    parser.add_option('-n','--noLaunch', action="store_true", dest='noLaunch', default=False)

    options, args = parser.parse_args()



    print 'the run is', options.run
    print 'the  version is', options.version

    print 'nolaunch', options.noLaunch
    print 'os', options.os


    #f = open(options.suiteFile)
    #data = f.readlines()

    testsToLaunch = []

    #for d in data:
     #  fields = d.split()
     #  testsToLaunch.append( {'descriptor':fields[0],'confFile':fields[1],'iniFile':fields[2],
     #                         'serverCount':int(fields[3]), 'timeLimit':int(fields[4]),
     #                          'parameters':fields[5]})


    cb = Bucket('couchbase://' + TEST_SUITE_DB + '/QE-Test-Suites')


    query = N1QLQuery("select * from `QE-Test-Suites` where '" + options.run + "' in partOf")
    results = cb.n1ql_query( query )

    #print 'the query results are', results
    for row in results:
        data = row['QE-Test-Suites']
        print 'row', data


        if 'os' not in data or (data['os'] == options.os) or \
            (data['os'] == 'linux' and options.os in set(['centos','ubuntu']) ):
            testsToLaunch.append( {'component':data['component'], 'subcomponent':data['subcomponent'],'confFile':data['confFile'],
                               'iniFile':data['config'],
                             'serverCount':getNumberOfServers(data['config']), 'timeLimit':data['timeOut'],
                             'parameters':data['parameters']})
        else:
            print 'OS does not apply to', data['component'], data['subcomponent']

    print 'tests to launch',testsToLaunch


    launchString = 'http://qa.sc.couchbase.com/job/test_suite_executor/buildWithParameters?token=test_dispatcher&' + \
                        'version_number={0}&confFile={1}&descriptor={2}&component={3}&subcomponent={4}&' + \
                         'iniFile={5}&servers={6}&parameters={7}&os={8}'

    summary = []

    while len(testsToLaunch) > 0:
        response, content = httplib2.Http(timeout=60).request('http://172.23.105.177:8081/getavailablecount/{0}'.format(options.os), 'GET')
        if response.status != 200:
           print time.asctime( time.localtime(time.time()) ), 'invalid server response', content
           time.sleep(POLL_INTERVAL)
        elif int(content) == 0:
            print time.asctime( time.localtime(time.time()) ), 'no VMs'
            time.sleep(POLL_INTERVAL)
        else:
            #see if we can match a test
            serverCount = int(content)
            print time.asctime( time.localtime(time.time()) ), 'there are', serverCount, ' servers available'

            haveTestToLaunch = False
            i = 0
            while not haveTestToLaunch and i < len(testsToLaunch):
                #print 'i', i, 'ttl sc', testsToLaunch[i]['serverCount']
                if testsToLaunch[i]['serverCount'] <= serverCount:
                    haveTestToLaunch = True
                else:
                    i = i + 1

            if haveTestToLaunch:
                descriptor = testsToLaunch[i]['component'] + '-' + testsToLaunch[i]['subcomponent']
                # get the VMs, they should be there
                response, content = httplib2.Http(timeout=60).request('http://' + SERVER_MANAGER +
                        '/getservers/{0}?count={1}&expiresin={2}&os={3}'.
                   format(descriptor, testsToLaunch[i]['serverCount'],testsToLaunch[i]['timeLimit'],options.os), 'GET')

                if response.status == 499:
                    time.sleep(POLL_INTERVAL) # some error checking here at some point
                else:
                    r2 = json.loads(content)
                    url = launchString.format(options.version, testsToLaunch[i]['confFile'],
                                         descriptor, testsToLaunch[i]['component'],
                                         testsToLaunch[i]['subcomponent'], testsToLaunch[i]['iniFile'],
                                         urllib.quote(json.dumps(r2).replace(' ','')),
                                         urllib.quote(testsToLaunch[i]['parameters']), options.os)
                    print 'launching', url
                    print time.asctime( time.localtime(time.time()) ), 'launching ', descriptor


                    if not options.noLaunch:  # sorry for the double negative
                        response, content = httplib2.Http(timeout=60).request(url, 'GET')
                    testsToLaunch.pop(i)
                    summary.append( {'test':descriptor, 'time':time.asctime( time.localtime(time.time()) ) } )
            else:
                print 'no VMs at this time'
                time.sleep(POLL_INTERVAL)
        #endif checking for servers
    #endwhile


    print '\n\n\ndone, everything is launched'
    for i in summary:
        print i['test'], 'was launched at', i['time']
    return





if __name__ == "__main__":
    main()

