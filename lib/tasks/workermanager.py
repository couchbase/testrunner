import collections
import itertools
import types
import marshal
import multiprocessing

"""
Currently implementing the map reduce class for
faster query processing
"""


class mapreduceworkers(object):
    def __init__(self, num_workers=None):
        self.mapfn = self.reducefn = self.collectfn = None
        self.pool = multiprocessing.Pool(num_workers)
        self.lock = multiprocessing.Lock()
        self.datasource = None

    def set_mapfn(self, command, mapfn):
        self.mapfn = types.FunctionType(marshal.loads(mapfn), globals(), 'mapfn')

    def set_collectfn(self, command, collectfn):
        self.collectfn = types.FunctionType(marshal.loads(collectfn), globals(), 'collectfn')

    def set_reducefn(self, command, reducefn):
        self.reducefn = types.FunctionType(marshal.loads(reducefn), globals(), 'reducefn')




