import json
from os import path


class JSONGenerator:
    def __init__(self, template, input_dict):
        pwd = path.dirname(__file__)
        file_path = path.join(pwd, template)
        self.template = file_path
        self.input = input_dict
        self.object = None

    def generate(self, obj, template_json):
        """
        Recursively generates map for the given json template
        :param obj: map
        :param template_json: json
        """
        if isinstance(template_json, dict):
            for key in list(template_json.keys()):
                if not isinstance(template_json[key], list) and not isinstance(template_json[key], dict):
                    if key in self.input and self.input[key] != "":
                        obj[key] = self.input[key]
                    elif key in template_json:
                        obj[key] = template_json[key]
                    else:
                        obj[key] = ""
                    continue
                if isinstance(template_json[key], list):
                    obj[key] = []
                    if key in self.input and self.input[key]:
                        obj[key].extend(self.input[key])
                    elif key in template_json and template_json[key]:
                        obj[key].extend(template_json[key])
                    else:
                        obj[key] = []
                    continue
                if isinstance(template_json[key], dict):
                    if key in self.input:
                        iterate = self.input[key]
                        if not isinstance(iterate, list):
                            iterate = [iterate]
                        for item in iterate:
                            obj[item] = {}
                            self.generate(obj[item], template_json[key])
                    else:
                        obj[key] = {}
                        self.generate(obj[key], template_json[key])

    def generate_json(self):
        template = json.load(open(self.template, 'r'))
        self.object = {}
        self.generate(self.object, template)
