import os
import random

class JsonGenerator(object):

    def execute_json_data_generator(self,
            executatble_path = None, key_extension=  "", bag_dir = None,
            pod_path = None, num_items = 1, seed = None):
        "This method runs monster tool using localhost, creates a map of json based on a pattern"
        map = {}
        command = executatble_path
        dest_path = "/tmp/{0}.txt".format(int(random.random()*1000))
        if pod_path == None:
            return map
        if bag_dir != None:
            command += " -bagdir {0}".format(bag_dir)
        if seed != None:
            command += " -s {0}".format(seed)
        command += " -n {0}".format(num_items)
        command += " -o {0}".format(dest_path)
        if pod_path != None:
            command += " {0}".format(pod_path)
        print "Will run the following command: {0}".format(command)
        # run command and generate temp file
        os.system(command)
        # read file and generate list
        with open(dest_path) as f:
            i= 1
            for line in f.readlines():
                key = "{0}{1}".format(key_extension,i)
                map[key] = line[:len(line)-1]
                i+=1
        os.remove(dest_path)
        return map

