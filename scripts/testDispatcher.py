import sys
import urllib.request, urllib.error, urllib.parse
import urllib.request, urllib.parse, urllib.error
import httplib2
import json
import string
import time
from optparse import OptionParser
import traceback

from couchbase import Couchbase
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError
from couchbase.n1ql import N1QLQuery


# takes an ini template as input, standard out is populated with the server pool
# need a descriptor as a parameter

# need a timeout param

POLL_INTERVAL = 60
SERVER_MANAGER = '172.23.105.177:8081'
TEST_SUITE_DB = '172.23.105.177'


def getNumberOfServers( iniFile):
    f = open(iniFile)
    contents = f.read()
    f.close()
    return contents.count('dynamic')

def getNumberOfAddpoolServers(iniFile, addPoolId):
    f = open(iniFile)
    contents = f.read()
    f.close()
    try:
        return contents.count(addPoolId)
    except:
        return 0

def rreplace(str, pattern, num_replacements):
    return str.rsplit(pattern, num_replacements)[0]

def main():

    usage = '%prog -s suitefile -v version -o OS'
    parser = OptionParser(usage)
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-r', '--run', dest='run')   # run is ambiguous but it means 12 hour or weekly
    parser.add_option('-o', '--os', dest='os')
    parser.add_option('-n', '--noLaunch', action="store_true", dest='noLaunch', default=False)
    parser.add_option('-c', '--component', dest='component', default=None)
    parser.add_option('-p', '--poolId', dest='poolId', default='12hour')
    parser.add_option('-a', '--addPoolId', dest='addPoolId', default=None)
    parser.add_option('-t', '--test', dest='test', default=False, action='store_true') # use the test Jenkins
    parser.add_option('-s', '--subcomponent', dest='subcomponent', default=None)
    parser.add_option('-e', '--extraParameters', dest='extraParameters', default=None)
    parser.add_option('-y', '--serverType', dest='serverType', default='VM')   # or could be Docker
    parser.add_option('-u', '--url', dest='url', default=None)
    parser.add_option('-j', '--jenkins', dest='jenkins', default=None)
    parser.add_option('-b', '--branch', dest='branch', default='master')

    # dashboardReportedParameters is of the form param1=abc,param2=def
    parser.add_option('-d', '--dashboardReportedParameters', dest='dashboardReportedParameters', default=None)

    options, args = parser.parse_args()



    print('the run is', options.run)
    print('the  version is', options.version)
    releaseVersion = float( '.'.join( options.version.split('.')[:2]) )
    print('release version is', releaseVersion)

    print('nolaunch', options.noLaunch)
    print('os', options.os)
    #print 'url', options.url


    print('url is', options.url)

    print('the reportedParameters are', options.dashboardReportedParameters)


    # What do we do with any reported parameters?
    # 1. Append them to the extra (testrunner) parameters
    # 2. Append the right hand of the equals sign to the subcomponent to make a report descriptor


    if options.extraParameters is None:
        if options.dashboardReportedParameters is None:
            runTimeTestRunnerParameters = None
        else:
            runTimeTestRunnerParameters = options.dashboardReportedParameters
    else:
        runTimeTestRunnerParameters = options.extraParameters
        if options.dashboardReportedParameters is not None:
            runTimeTestRunnerParameters = options.extraParameters + ',' + options.dashboardReportedParameters



    #f = open(options.suiteFile)
    #data = f.readlines()

    testsToLaunch = []

    #for d in data:
     #  fields = d.split()
     #  testsToLaunch.append( {'descriptor':fields[0],'confFile':fields[1],'iniFile':fields[2],
     #                         'serverCount':int(fields[3]), 'timeLimit':int(fields[4]),
     #                          'parameters':fields[5]})


    cb = Bucket('couchbase://' + TEST_SUITE_DB + '/QE-Test-Suites')

    if options.component is None or options.component == 'None':
        queryString = "select * from `QE-Test-Suites` where '" + options.run + "' in partOf order by component"
    else:
        if options.subcomponent is None or options.subcomponent == 'None':
            splitComponents = options.component.split(',')
            componentString = ''
            for i in range( len(splitComponents) ):
                componentString = componentString + "'" + splitComponents[i] + "'"
                if i < len(splitComponents) - 1:
                    componentString = componentString + ','


            queryString = "select * from `QE-Test-Suites` where \"{0}\" in partOf and component in [{1}] order by component;".format(options.run, componentString)

        else:
            # have a subcomponent, assume only 1 component

            splitSubcomponents = options.subcomponent.split(',')
            subcomponentString = ''
            for i in range( len(splitSubcomponents) ):
                print('subcomponentString is', subcomponentString)
                subcomponentString = subcomponentString + "'" + splitSubcomponents[i] + "'"
                if i < len(splitSubcomponents) - 1:
                    subcomponentString = subcomponentString + ','
            queryString = "select * from `QE-Test-Suites` where \"{0}\" in partOf and component in ['{1}'] and subcomponent in [{2}];".\
                format(options.run, options.component, subcomponentString)


    print('the query is', queryString) #.format(options.run, componentString)
    query = N1QLQuery(queryString )
    results = cb.n1ql_query( queryString )

    framework = None
    for row in results:
        try:
            data = row['QE-Test-Suites']
            data['config'] = data['config'].rstrip()       # trailing spaces causes problems opening the files
            print('row', data)

            # check any os specific
            if 'os' not in data or (data['os'] == options.os) or \
                (data['os'] == 'linux' and options.os in {'centos', 'ubuntu'} ):

                # and also check for which release it is implemented in
                if 'implementedIn' not in data or releaseVersion >= float(data['implementedIn']):
                    if 'jenkins' in data:
                        # then this is sort of a special case, launch the old style Jenkins job
                        # not implemented yet
                        print('Old style Jenkins', data['jenkins'])
                    else:
                        if 'initNodes' in data:
                            initNodes = data['initNodes'].lower() == 'true'
                        else:
                            initNodes = True
                        if 'installParameters' in data:
                            installParameters = data['installParameters']
                        else:
                            installParameters = 'None'
                        if 'slave' in data:
                            slave = data['slave']
                        else:
                            slave = 'P0'
                        if 'owner' in data:
                            owner = data['owner']
                        else:
                            owner = 'QE'
                        if 'mailing_list' in data:
                            mailing_list = data['mailing_list']
                        else:
                            mailing_list = 'qa@couchbase.com'
                        if 'mode' in data:
                            mode = data["mode"]
                        else:
                            mode = 'java'
                        if 'framework' in data:
                            framework = data["framework"]
                        else:
                            framework = 'testrunner'
                        # if there's an additional pool, get the number
                        # of additional servers needed from the ini
                        addPoolServerCount = getNumberOfAddpoolServers(
                            data['config'],
                            options.addPoolId)

                        testsToLaunch.append({
                            'component':data['component'],
                            'subcomponent':data['subcomponent'],
                            'confFile':data['confFile'],
                            'iniFile':data['config'],
                            'serverCount':getNumberOfServers(data['config']),
                            'addPoolServerCount': addPoolServerCount,
                            'timeLimit':data['timeOut'],
                            'parameters':data['parameters'],
                            'initNodes':initNodes,
                            'installParameters':installParameters,
                            'slave': slave,
                            'owner': owner,
                            'mailing_list': mailing_list,
                            'mode': mode
                            })
                else:
                    print(data['component'], data['subcomponent'], ' is not supported in this release')
            else:
                print('OS does not apply to', data['component'], data['subcomponent'])

        except Exception as e:
            print('exception in querying tests, possible bad record')
            print(traceback.format_exc())
            print(data)

    print('tests to launch:')
    for i in testsToLaunch: print(i['component'], i['subcomponent'])
    print('\n\n')




    launchStringBase = 'http://qa.sc.couchbase.com/job/test_suite_executor'

    # optional add [-docker] [-Jenkins extension]
    if options.serverType.lower() == 'docker':
        launchStringBase = launchStringBase + '-docker'
    if options.test:
        launchStringBase = launchStringBase + '-test'
#     if options.framework.lower() == "jython":
    if framework == "jython":
        launchStringBase = launchStringBase + '-jython'
    if framework == "TAF":
        launchStringBase = launchStringBase + '-TAF'
    elif options.jenkins is not None:
        launchStringBase = launchStringBase + '-' + options.jenkins



    # this are VM/Docker dependent - or maybe not
    launchString = launchStringBase + '/buildWithParameters?token=test_dispatcher&' + \
                        'version_number={0}&confFile={1}&descriptor={2}&component={3}&subcomponent={4}&' + \
                         'iniFile={5}&parameters={6}&os={7}&initNodes={' \
                         '8}&installParameters={9}&branch={10}&slave={' \
                         '11}&owners={12}&mailing_list={13}&mode={14}&timeout={15}'
    if options.url is not None:
        launchString = launchString + '&url=' + options.url

    summary = []

    while len(testsToLaunch) > 0:
        try:
            # this bit is Docker/VM dependent
            getAvailUrl =  'http://' + SERVER_MANAGER + '/getavailablecount/'
            if options.serverType.lower() == 'docker':
                # may want to add OS at some point
                getAvailUrl = getAvailUrl +  'docker?os={0}&poolId={1}'.format(options.os, options.poolId)
            else:
                getAvailUrl = getAvailUrl + '{0}?poolId={1}'.format(options.os, options.poolId)

            response, content = httplib2.Http(timeout=60).request(getAvailUrl, 'GET')
            if response.status != 200:
               print(time.asctime( time.localtime(time.time()) ), 'invalid server response', content)
               time.sleep(POLL_INTERVAL)
            elif int(content) == 0:
                print(time.asctime( time.localtime(time.time()) ), 'no VMs')
                time.sleep(POLL_INTERVAL)
            else:
                #see if we can match a test
                serverCount = int(content)
                print(time.asctime( time.localtime(time.time()) ), 'there are', serverCount, ' servers available')

                haveTestToLaunch = False
                i = 0
                while not haveTestToLaunch and i < len(testsToLaunch):
                    if testsToLaunch[i]['serverCount'] <= serverCount:
                        if testsToLaunch[i]['addPoolServerCount']:
                            getAddPoolUrl = 'http://' + SERVER_MANAGER + '/getavailablecount/'
                            if options.serverType.lower() == 'docker':
                                # may want to add OS at some point
                                getAddPoolUrl = getAddPoolUrl + 'docker?os={0}&poolId={1}'.format(
                                    options.os, options.addPoolId)
                            else:
                                getAddPoolUrl = getAddPoolUrl + '{0}?poolId={1}'.format(
                                    options.os, options.addPoolId)

                            response, content = httplib2.Http(
                                timeout=60).request(getAddPoolUrl, 'GET')
                            if response.status != 200:
                                print(time.asctime(time.localtime(
                                    time.time())), 'invalid server response', content)
                                time.sleep(POLL_INTERVAL)
                            elif int(content) == 0:
                                print(time.asctime(
                                    time.localtime(time.time())),\
                                    'no {0} VMs at this time'.format(options.addPoolId))
                                i = i + 1
                            else:
                                print(time.asctime( time.localtime(time.time()) ),\
                                    "there are {0} {1} servers available".format(int(content), options.addPoolId))
                                haveTestToLaunch = True
                        else:
                            haveTestToLaunch = True
                    else:
                        i = i + 1


                if haveTestToLaunch:
                    # build the dashboard descriptor
                    dashboardDescriptor = urllib.parse.quote(testsToLaunch[i]['subcomponent'])
                    if options.dashboardReportedParameters is not None:
                        for o in options.dashboardReportedParameters.split(','):
                            dashboardDescriptor += '_' + o.split('=')[1]

                    # and this is the Jenkins descriptor
                    descriptor = urllib.parse.quote(testsToLaunch[i]['component'] + '-' + testsToLaunch[i]['subcomponent'] +
                                        '-' + time.strftime('%b-%d-%X') + '-' + options.version )


                    # grab the server resources
                    # this bit is Docker/VM dependent
                    if options.serverType.lower() == 'docker':
                         getServerURL = 'http://' + SERVER_MANAGER + \
                                '/getdockers/{0}?count={1}&os={2}&poolId={3}'. \
                           format(descriptor, testsToLaunch[i]['serverCount'], \
                                  options.os, options.poolId)

                    else:
                        getServerURL = 'http://' + SERVER_MANAGER + \
                                '/getservers/{0}?count={1}&expiresin={2}&os={3}&poolId={4}'. \
                           format(descriptor, testsToLaunch[i]['serverCount'], testsToLaunch[i]['timeLimit'], \
                                  options.os, options.poolId)
                    print('getServerURL', getServerURL)

                    response, content = httplib2.Http(timeout=60).request(getServerURL, 'GET')
                    print('response.status', response, content)

                    if options.serverType.lower() != 'docker':
                        # sometimes there could be a race, before a dispatcher process acquires vms,
                        # another waiting dispatcher process could grab them, resulting in lesser vms
                        # for the second dispatcher process
                        if len(json.loads(content)) != testsToLaunch[i]['serverCount']:
                            continue

                    # get additional pool servers as needed
                    if testsToLaunch[i]['addPoolServerCount']:
                        if options.serverType.lower() == 'docker':
                             getServerURL = 'http://' + SERVER_MANAGER + \
                                    '/getdockers/{0}?count={1}&os={2}&poolId={3}'. \
                               format(descriptor,
                                      testsToLaunch[i]['addPoolServerCount'],
                                      options.os,
                                      options.addPoolId)

                        else:
                            getServerURL = 'http://' + SERVER_MANAGER + \
                                    '/getservers/{0}?count={1}&expiresin={2}&os={3}&poolId={4}'. \
                               format(descriptor,
                                      testsToLaunch[i]['addPoolServerCount'],
                                      testsToLaunch[i]['timeLimit'], \
                                      options.os,
                                      options.addPoolId)
                        print('getServerURL', getServerURL)

                        response2, content2 = httplib2.Http(timeout=60).request(getServerURL, 'GET')
                        print('response2.status', response2, content2)



                    if response.status == 499 or \
                            (testsToLaunch[i]['addPoolServerCount'] and
                            response2.status == 499):
                        time.sleep(POLL_INTERVAL) # some error checking here at some point
                    else:
                        # and send the request to the test executor


                        # figure out the parameters, there are test suite specific, and added at dispatch time
                        if  runTimeTestRunnerParameters is None:
                            parameters = testsToLaunch[i]['parameters']
                        else:
                            if testsToLaunch[i]['parameters'] == 'None':
                                parameters = runTimeTestRunnerParameters
                            else:
                                parameters = testsToLaunch[i]['parameters'] + ',' + runTimeTestRunnerParameters



                        url = launchString.format(options.version,
                                                  testsToLaunch[i]['confFile'],
                                                  descriptor,
                                                  testsToLaunch[i]['component'],
                                                  dashboardDescriptor,
                                                  testsToLaunch[i]['iniFile'],
                                                  urllib.parse.quote(parameters),
                                                  options.os,
                                                  testsToLaunch[i]['initNodes'],
                                                  testsToLaunch[i]['installParameters'],
                                                  options.branch,
                                                  testsToLaunch[i]['slave'],
                                                  urllib.parse.quote(testsToLaunch[i]['owner']),
                                                  urllib.parse.quote(
                                                      testsToLaunch[i]['mailing_list']),
                                                  testsToLaunch[i]['mode'],
                                                  testsToLaunch[i]['timeLimit'])


                        if options.serverType.lower() != 'docker':
                            r2 = json.loads(content)
                            servers = json.dumps(r2).replace(' ', '').replace('[', '', 1)
                            servers = rreplace(servers, ']', 1)
                            url = url + '&servers=' + urllib.parse.quote(servers)

                            if testsToLaunch[i]['addPoolServerCount']:
                                addPoolServers = content2.replace(' ', '')\
                                                   .replace('[', '', 1)
                                addPoolServers = rreplace(addPoolServers, ']', 1)
                                url = url + '&addPoolServerId=' +\
                                      options.addPoolId +\
                                      '&addPoolServers=' +\
                                      urllib.parse.quote(addPoolServers)


                        print('\n', time.asctime( time.localtime(time.time()) ), 'launching ', url)
                        print(url)

                        if options.noLaunch:
                            # free the VMs
                            time.sleep(3)
                            if options.serverType.lower() == 'docker':
                                pass # figure docker out later
                            else:
                                response, content = httplib2.Http(timeout=60).\
                                    request('http://' + SERVER_MANAGER + '/releaseservers/' + descriptor + '/available', 'GET')
                                print('the release response', response, content)
                        else:
                            response, content = httplib2.Http(timeout=60).request(url, 'GET')

                        testsToLaunch.pop(i)
                        summary.append( {'test':descriptor, 'time':time.asctime( time.localtime(time.time()) ) } )
                        if options.noLaunch:
                            pass # no sleeping necessary
                        elif options.serverType.lower() == 'docker':
                            time.sleep(240)     # this is due to the docker port allocation race
                        else:
                            time.sleep(30)
                else:
                    print('not enough servers at this time')
                    time.sleep(POLL_INTERVAL)
            #endif checking for servers

        except Exception as e:
            print('have an exception')
            print(traceback.format_exc())
            time.sleep(POLL_INTERVAL)
    #endwhile

    print('\n\n\ndone, everything is launched')
    for i in summary:
        print(i['test'], 'was launched at', i['time'])
    return


if __name__ == "__main__":
    main()

