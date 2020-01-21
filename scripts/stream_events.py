from http.client import BadStatusLine
import sys
import urllib.request, urllib.error, urllib.parse
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
    master = input.servers[0]
    print("Streaming events from {0}".format(master.ip))
    diag_url = "http://{0}:{1}/diag/masterEvents".format(master.ip, master.port)
    print(diag_url)
    try:
        req = urllib.request.Request(diag_url)
        req.headers = create_headers(input.membase_settings.rest_username,
                                     input.membase_settings.rest_password)
        response = urllib.request.urlopen(req)
        for line in response:
            print(line.rstrip())

    except urllib.error.URLError as error:
        print("unable to stream masterEvents from {0}".format(diag_url))
    except BadStatusLine:
        print("unable to stream masterEvents from {0}".format(diag_url))
    except Exception:
        print("unable to stream masterEvents from {0}".format(diag_url))
