import json

class DesignDocument():
    def __init__(self, name, views, rev=None):
        self.id = '_design/{0}'.format(name)
        self.rev = rev
        self.views = views

    @classmethod
    def _init_from_json(ddoc_self, design_doc_name, json_object):
        ddoc_self = DesignDocument(design_doc_name, [])

        views_json = json_object['views']
        for view in views_json.items():
            ddoc_self.views.append(View._init_from_json(view))

        return ddoc_self

    def add_view(self, view):
        i = 0
        for current_view in self.views:
            # if view already exists it will be updated
            if view.name == current_view.name:
                self.views[i] = view
                break
            i += 1

        if i == len(self.views):
            self.views.append(view)

    def delete_view(self, view):
        view_deleted = False
        i = 0
        for current_view in self.views:
            if view.name == current_view.name:
                del self.views[i]
                view_deleted = True
                break
            i += 1

        return view_deleted

    def as_json(self):
        json_object = {'views': {}}
        for view in self.views:
            json_object['views'][view.name] = view.as_json()[view.name]
        return json_object

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return json.dumps(self.as_json())

class View():
    def __init__(self, name, map_func, red_func=None, dev_view=True):
        self.name = name
        self.map_func = map_func
        self.red_func = red_func
        self.dev_view = dev_view

    @classmethod
    def _init_from_json(view_self, json_object):
        name = json_object[0]
        map_func = clean_string(json_object[1]['map'])

        if 'reduce' in json_object[1]:
            red_func = clean_string(json_object[1]['reduce'])
        else:
            red_func = None

        return View(name, map_func, red_func)

    def as_json(self):
        if self.red_func is None:
            return {self.name: {'map': self.map_func}}
        else:
            return {self.name: {'map': self.map_func, 'reduce': self.red_func}}

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return json.dumps(self.as_json())

def clean_string(str_):
    return str_.replace('\n', '').replace('\r', '')

