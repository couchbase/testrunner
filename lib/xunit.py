import xml.dom.minidom

# a junit compatible xml example
#<?xml version="1.0" encoding="UTF-8"?>
#<testsuite name="nosetests" tests="1" errors="1" failures="0" skip="0">
#    <testcase classname="path_to_test_suite.TestSomething"
#              name="path_to_test_suite.TestSomething.test_it" time="0">
#        <error type="exceptions.TypeError">
#        Traceback (most recent call last):
#        ...
#        TypeError: oops, wrong type
#        </error>
#    </testcase>
#</testsuite>

#
# XUnitTestCase has name , time and error
# error is a XUnitTestCase
class XUnitTestCase(object):
    def __init__(self):
        self.name  =""
        self.time = 0
        self.error = None

#
# XUnitTestCaseError has type and message
#
class XUnitTestCaseError(object):
    def __init__(self):
        self.type = ""
        self.message = ""


#
# XUnitTestSuite has name , time , list of XUnitTestCase objects
# errors : number of errors
# failures : number of failures
# skips : number of skipped tests
#
class XUnitTestSuite(object):
    def __init__(self):
        self.name = ""
        self.time = 0
        self.tests = []
        self.errors = 0
        self.failures = 0
        self.skips = 0

    # create a new XUnitTestCase and update the errors/failures/skips count
    def add_test(self,name,time = 0,errorType = None,errorMessage = None,status = 'pass'):
        #create a test_case and add it to this suite
        # todo: handle 'skip' or 'setup_failure' or other
        # status codes that testrunner might pass to this function
        test = XUnitTestCase()
        test.name = name
        test.time = time
        if status == 'fail':
            error = XUnitTestCaseError()
            error.type = errorType
            error.message = errorMessage
            test.error = error
        self.tests.append(test)
        if status == 'fail':
            self.failures += 1
            self.errors += 1
        elif status == 'skip':
            self.skips += 1
        self.time += time


    # generate the junit xml representation from the XUnitTestSuite object
    # todo : create an element for errorMessage and append it to to error node
    def to_xml(self):
        doc = xml.dom.minidom.Document()
        testsuite = doc.createElement('testsuite')
        #<testsuite name="nosetests" tests="1" errors="1" failures="0" skip="0">
        testsuite.setAttribute('name',self.name)
        testsuite.setAttribute('errors',str(self.errors))
        testsuite.setAttribute('failures',str(self.failures))
        testsuite.setAttribute('errors',str(self.errors))
        testsuite.setAttribute('tests',str(len(self.tests)))
        testsuite.setAttribute('time',str(self.time))
        testsuite.setAttribute('skip',str(self.skips))


        for testobject in self.tests:
            testcase = doc.createElement('testcase')
            testcase.setAttribute('name',testobject.name)
            testcase.setAttribute('time',str(testobject.time))
            if testobject.error:
                error = doc.createElement('error')
                error.setAttribute('type', testobject.error.type)
                if testobject.error.message:
                    message = doc.createTextNode(testobject.error.message)
                    error.appendChild(message)
                testcase.appendChild(error)
            testsuite.appendChild(testcase)
        doc.appendChild(testsuite)
        return doc.toprettyxml()