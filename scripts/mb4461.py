from optparse import OptionParser


import sys

sys.path.append('.')
sys.path.append('lib')
import uuid
import time

from mc_bin_client import MemcachedClient


def info(msg):
    print(msg)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-n", "--node", dest="node", default="127.0.0.1",
                      help="couchdb ip , defaults to 127.0.0.1:5984")
    parser.add_option("-s", "--sleep", dest="sleep", default="600",
                      help="sleep time before each iteration")
    parser.add_option("-i", "--items", dest="items", default="10000",
                      help="number of items")

    options, args = parser.parse_args()

    node = options.node
    sleep_time = int(options.sleep)

    prefix = str(uuid.uuid4())
    number_of_items = int(options.items)

    mc = MemcachedClient("127.0.0.1", 11211)

    keys = ["{0}-{1}".format(prefix, i) for i in range(0, number_of_items)]
    info("inserting {0} items".format(number_of_items))
    for k in keys:
        mc.set(k, 0, 0, str(uuid.uuid4())[0:16])

    while True:
        info("now remove 3 chars from 80% of keys - if < 3 chars delete the key - if key does not exist create it")
        for i in range(0, 3):
            for k in keys:
                try:
                    a, b, value = mc.get(k)
                    if len(value) < 3:
                        mc.delete(k)
                    else:
                        mc.set(k, 0, 0, value[0:len(value) - 7])
                except:
                    mc.set(k, 0, 0, str(uuid.uuid4())[0:16])
            time.sleep(sleep_time)

        for k in keys:
            try:
                mc.prepend(k, "two")
            except:
                mc.set(k, 0, 0, str(uuid.uuid4())[0:16])

        time.sleep(sleep_time)
