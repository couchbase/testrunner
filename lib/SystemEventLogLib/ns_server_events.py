from SystemEventLogLib.Events import Event
from cb_constants.system_event_log import NsServer


class NsServerEvents(object):
    @staticmethod
    def baby_sitter_respawn(node, process_id):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.BabySitterRespawn,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Babysitter crashed",
            Event.Fields.SEVERITY: Event.Severity.FATAL,
            Event.Fields.EXTRA_ATTRS: {'process_id': process_id}
        }

    @staticmethod
    def rebalance_started(node, triggered_by):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Rebalance initiated",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'triggered_by': triggered_by}
        }

    @staticmethod
    def rebalance_success(node, rebalance_time):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Rebalance completed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                'time': rebalance_time,
                "completion_message": "Rebalance completed successfully."
            }
        }

    @staticmethod
    def rebalance_failed(node, rebalance_time, failure_reason):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceFailure,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Rebalance failed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'time': rebalance_time,
                                       'failure_reason': failure_reason}
        }

    @staticmethod
    def node_added(node, node_added, node_services):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.NodeAdded,
            Event.Fields.DESCRIPTION: "Node successfully joined the cluster",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'node_added': node_added,
                                       'services': node_services}
        }

    @staticmethod
    def node_offline(node, new_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.NodeOffline,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'node': new_node}
        }

    @staticmethod
    def auto_failover_started(node, failover_nodes, orchestrator_node,
                              failover_threshold, failover_reason):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailoverStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Auto failover initiated",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'node': ['ns_1@' + ip
                                                for ip in failover_nodes],
                                       'master_node': 'ns_1@'+orchestrator_node,
                                       'threshold': failover_threshold,
                                       'reason': failover_reason}
        }

    @staticmethod
    def auto_failover_complete(node, failover_nodes, orchestrator_node,
                               failover_threshold, failover_reason):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Auto failover completed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {
                'node': ['ns_1@' + ip for ip in failover_nodes],
                'master_node': 'ns_1@' + orchestrator_node,
                'time_taken': 0,
                'threshold': failover_threshold,
                'reason': failover_reason,
                "completion_message": "Failover completed successfully."}
        }

    @staticmethod
    def auto_failover_failure(node, failover_node, orchestrator_node,
                              failover_threshold, failure_reason):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailoverFailed,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Auto failover failed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'node': failover_node,
                                       'orchestrator': orchestrator_node,
                                       'threshold': failover_threshold,
                                       'reason': failure_reason}
        }

    @staticmethod
    def graceful_failover_started(node, trigger_method, failover_node,
                                  orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.GracefulFailoverStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Graceful failover started",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'trigger_method': trigger_method,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def graceful_failover_complete(node, trigger_method, failover_node,
                                   orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.GracefulFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Graceful failover complete",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'trigger_method': trigger_method,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def graceful_failover_failed(node, trigger_method, failover_node,
                                 orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.GracefulFailoverFailed,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Graceful failover failed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'trigger_method': trigger_method,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def hard_failover_started(node, trigger_type, failover_node,
                              orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.HardFailoverStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Hard failover started",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'trigger_type': trigger_type,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def hard_failover_complete(node, trigger_type, failover_node,
                               orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.HardFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Hard failover complete",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'trigger_type': trigger_type,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def hard_failover_failed(node, trigger_type, failover_node,
                             orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.HardFailoverComplete,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Hard failover failed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'trigger_type': trigger_type,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def orchestrator_change(old_orchestrator, new_orchestrator):
        return {
            Event.Fields.EVENT_ID: NsServer.MasterSelected,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.DESCRIPTION: "Master selected",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {'old_master': 'ns_1@'+old_orchestrator,
                                       'new_master': 'ns_1@'+new_orchestrator}
        }

    @staticmethod
    def topology_update(node, old_topology, new_topology):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.TopologyChange,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'old_topology': old_topology,
                                       'new_topology': new_topology}
        }
