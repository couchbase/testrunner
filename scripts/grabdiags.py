import sys
import os
import urllib2
import zipfile
import base64

sys.path.append('.')
sys.path.append('lib')

from TestInput import TestInputParser

def create_headers(username, password):
    authorization = base64.encodestring('%s:%s' % (username, password))
    return {'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic %s' % authorization,
            'Accept': '*/*'}


if __name__ == "__main__":
    input = TestInputParser.get_test_input(sys.argv)
    for serverInfo in input.servers:
        print "grabbing diags from ".format(serverInfo.ip)
        diag_url = "http://{0}:8091/diag".format(serverInfo.ip)
        print diag_url
        try:
            req = urllib2.Request(diag_url)
            req.headers = create_headers(input.membase_settings.rest_username,
                                         input.membase_settings.rest_password)
            filename = "{0}-diag.txt".format(serverInfo.ip)
            page = urllib2.urlopen(req)
            with open(filename, 'wb') as output:
                os.write(1, "downloading {0} ...".format(serverInfo.ip))
                while True:
                    buffer = page.read(65536)
                    if not buffer:
                        break
                    output.write(buffer)
                    os.write(1, ".")
            zipped = zipfile.ZipFile("{0}.zip".format(filename), "w", compression=zipfile.ZIP_DEFLATED)
            zipped.write(filename)
            zipped.close()
            os.remove(filename)
            print "downloaded and zipped diags @ : {0}".format("{0}.zip".format(filename))
        except urllib2.URLError as error:
            print "unable to obtain diags from {0}".format(diag_url)

