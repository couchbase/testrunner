
import json

class DesignDocument():
    def __init__(self, json_string):
        json_object = json.loads(clean_json_string(json_string))
        self.id = json_object['_id']
        self.rev = json_object['_rev']
        self.views = []

        views_json = json_object['views']
        for view in views_json.items():
            self.views.append(View(json.dumps(view)))

    def __init__(self, name, views, rev=None):
        self.id = '_design/{0}'.format(name)
        self.rev = rev
        self.views = views

    def as_json(self):
        json_object = {'_id': self.id,
                 'views': {}}
        if self.rev is not None:
            json_object['_rev'] = self.rev
        for view in self.views:
            json_object['views'][view.name] = view.as_json()[view.name]
        return json_object

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return json.dumps(self.as_json())

class View():
    def __init__(self, json_string):
        json_object = json.loads(clean_json_string(json_string))
        self.name = json_object[0]
        self.map_func = json_object[1]['map']

        try:
            self.red_func = json_object[1]['reduce']
        except KeyError:
            self.red_func = None

    def __init__(self, name, map_func, red_func=None):
        self.name = name
        self.map_func = map_func
        self.red_func = red_func

    def as_json(self):
        if self.red_func is None:
            return {self.name: {'map': self.map_func}}
        else:
            return {self.name: {'map': self.map_func, 'reduce': self.red_func}}

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return json.dumps(self.as_json())

def clean_json_string(json_string):
    return json_string.replace('\n', '').replace('\r', '')

