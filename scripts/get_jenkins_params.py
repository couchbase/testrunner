import subprocess
import sys
try:
    import requests
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "requests"])
    import requests


def get_params(url):
    """
    Get parameters from the jenkins job
    :param url: The jenkins job URL
    :type url: str
    :return: Dictionary of job parameters
    :rtype: dict
    """
    res = get_js(url, params="tree=actions[parameters[*]]")
    parameters = {}
    if not res:
        print("Error: could not get parameters")
        return None
    for vals in res['actions']:
        if "parameters" in vals:
            for params in vals['parameters']:
                parameters[params['name']] = params['value']
            break
    return parameters


def get_js(url, params=None):
    """
    Get the parameters from Jenkins job using Jenkins rest api
    :param url: The jenkins job URL
    :type url: str
    :param params: Parameters to be passed to the json/api
    :type params: str
    :return: Response from the rest api
    :rtype: dict
    """
    res = None
    try:
        res = requests.get("%s/%s" % (url, "api/json"),
                           params=params, timeout=15)
        data = res.json()
        return data
    except:
        print("Error: url unreachable: %s" % url)
        return None


def download_url_data(url, params=None):
    """
    Download the data from the given url and with given parameters
    from the jenkins job
    :param url: Jenkins job url
    :type url: str
    :param params: Parameters to be passed to the api
    :type params: str
    :return: Content of the request to the jenkins api
    :rtype: requests.content
    """
    res = None
    try:
        res = requests.get("%s" % url, params=params, timeout=15)
        return res.content
    except:
        print("[Error] url unreachable: %s" % url)
        res = None
    return res