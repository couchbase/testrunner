import json


class JsonValidationHelper():

    def __init__(self):
        return

    def compare_json_files(self, file1, file2):
        """

        :param file1:
        :param file2:
        :return:
        """
        json1 = json.load(file1)
        json2 = json.load(file2)
        return  json1 == json2

