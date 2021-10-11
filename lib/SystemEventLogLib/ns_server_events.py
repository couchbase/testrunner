from SystemEventLogLib.Events import Event
from cb_constants.system_event_log import NsServer


class NsServerEvents(object):
    @staticmethod
    def baby_sitter_respawn(node, process_id):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.BabySitterRespawn,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'process_id': process_id}
        }

    @staticmethod
    def rebalance_started(node, triggered_by):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceStarted,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'triggered_by': triggered_by}
        }

    @staticmethod
    def rebalance_success(node, rebalance_time):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceSuccess,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'time': rebalance_time}
        }

    @staticmethod
    def rebalance_failed(node, rebalance_time, failure_reason):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.RebalanceFailure,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'time': rebalance_time,
                                       'failure_reason': failure_reason}
        }

    @staticmethod
    def node_added(node, node_services):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.NodeAdded,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'services': node_services}
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
    def auto_failover(node, failover_node, orchestrator_node,
                      failover_threshold, failover_reason):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailover,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'node': failover_node,
                                       'orchestrator': orchestrator_node,
                                       'threshold': failover_threshold,
                                       'reason': failover_reason}
        }

    @staticmethod
    def auto_failover_failure(node, failover_node, orchestrator_node,
                              failover_threshold, failure_reason):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.AutoFailoverFailure,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'node': failover_node,
                                       'orchestrator': orchestrator_node,
                                       'threshold': failover_threshold,
                                       'reason': failure_reason}
        }

    @staticmethod
    def graceful_failover_manual(node, trigger_method, failover_node,
                                 orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.GracefulFailoverManual,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'trigger_method': trigger_method,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def hard_failover_manual(node, trigger_type, failover_node,
                             orchestrator_node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: NsServer.HardFailoverManual,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'trigger_type': trigger_type,
                                       'node': failover_node,
                                       'orchestrator': orchestrator_node}
        }

    @staticmethod
    def orchestrator_change(old_orchestrator, new_orchestrator):
        return {
            Event.Fields.EVENT_ID: NsServer.OrchestratorChange,
            Event.Fields.COMPONENT: Event.Component.NS_SERVER,
            Event.Fields.EXTRA_ATTRS: {'prev_orchestrator': old_orchestrator,
                                       'new_orchestrator': new_orchestrator}
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
