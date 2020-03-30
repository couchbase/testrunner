# this class will implement a service that starts monitoring
# checkpoints in a cluster and then let you get the detail for
# active vbucket
import queue
from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import MemcachedClientHelper
import time

class CheckpointStatParser(object):
    def parse_one_bucket(self):
        pass

    def parse_output(self, output, node):
        result = {"node": node}
        for k, v in list(output.items()):
            #which vbucket ?
            vb_pos_start = k.find("_")
            vb_pos_end = k.find(":")
            vb = k[vb_pos_start + 1:vb_pos_end]
            checkpoint_attribute = k[vb_pos_end + 1:]
            if vb not in result:
                result[vb] = {}
            result[vb][checkpoint_attribute] = v
        return result

    def merge_results(self, total_results, per_node_result):
        node = per_node_result["node"]
        del per_node_result["node"]
        for vb, attributes in list(per_node_result.items()):
            if vb not in total_results:
                total_results[vb] = {}
            total_results[vb][node] = attributes
        return total_results


class GetCheckpointsHelper(object):

    def get_checkpoints_node(self, node, bucket):
        pass

    def get_checkpoints_from_cluster(self, master, bucket):
        parser = CheckpointStatParser()
        rest = RestConnection(master)
        servers = rest.get_nodes()
        merged = {}
        for server in servers:
            mc = MemcachedClientHelper.direct_client(server, bucket)
            per_node_checkpoint = mc.stats("checkpoint")
            parsed = parser.parse_output(per_node_checkpoint, server.id)
            merged = parser.merge_results(merged, parsed)
            mc.close()
        return merged

    def monitor_checkpoints(self, master, bucket, state,
                                   interval, max_allowed, command_queue):
        #monitor all checkpoints and if num_checkpoints is greater than max_allowed
        #it alerts
        #this should be started in a thread and stopped there
        while True:
            try:
                command = command_queue.get_nowait()
                if command and command == "stop":
                    break
            except queue.Empty:
                pass
            merged = self.get_checkpoints_from_cluster(master, bucket)
            alarms = []
            for vb, checkpoints in list(merged.items()):
                for node, checkpoint_attributes in list(checkpoints.items()):
                    if checkpoint_attributes["state"] == state:
                        if int(checkpoint_attributes["num_checkpoints"]) > max_allowed:
                            alarms.append("active vbucket {0} num_checkpoints is {1}".format(vb,
                                                                                             checkpoint_attributes[
                                                                                             "num_checkpoints"]))
            for alarm in alarms:
                print(alarm)
            time.sleep(interval)








