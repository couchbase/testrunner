import requests


class RestHelper(object):
    def __init__(self):
        pass

    @staticmethod
    def get_request(url, params=None):
        response = requests.get(url, params)
        return response

    @staticmethod
    def post_request(url, payload):
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json'}

        response = requests.post(url, headers=headers, json=payload)
        return response

    @staticmethod
    def delete_request(url):
        response = requests.delete(url)
        return response
