import requests


class RestHelper(object):
    def __init__(self):
        pass

    @staticmethod
    def get_request(url, params=None):
        response = requests.get(url, params)
        return response

    @staticmethod
    def post_request(url, data):
        response = requests.post(url, data)
        return response

    @staticmethod
    def delete_request(url):
        response = requests.delete(url)
        return response