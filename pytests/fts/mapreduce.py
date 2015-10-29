import multiprocessing
import collections
import itertools

class test_parallel(object):

    def __init__(self,map_fucntion,reduce_function,num_workers):
        self.__mapper =  map_fucntion
        self.__reducer=  reduce_function
        if num_workers <= 0:
            num_workers = 1
        self.__mp_pool = multiprocessing.Pool(num_workers)
        self.__intermediate_result = None
        self.__combine_container=collections.defaultdict(list)
        self.__result=None
        self.__shuffle_data=None

    def mapper_call(self,input_data):
        self.__intermediate_result= self.__mp_pool.map(self.__mapper, input_data)
        return self.combiner_call()

    def combiner_call(self):
        for val in self.__intermediate_result:
           for k,v in val.iteritems():
               self.__combine_container[k].append(v)
        self.__shuffle_data=self.__combine_container.items()

    def reducer_call(self):
        self.__result=self.__mp_pool.map(self.__reducer,self.__shuffle_data)
        return self.__result

    def __del__(self):
        self.__mp_pool.close()
        self.__mp_pool.terminate()

def sums(input):
    return {1: float(sum(input) / len(input)) }

def avg(input):
    sm,c = input
    print float(sum(c) / len(c))

A=test_parallel(sums,avg,3)
print A.mapper_call([[1,2,3,4],[5,6,7,8],[5,5]])
A.reducer_call()