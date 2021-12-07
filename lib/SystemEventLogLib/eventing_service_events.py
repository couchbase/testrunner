from SystemEventLogLib.Events import Event
from constants.cb_constants.system_event_log import Eventing


class EventingServiceEvents(object):
    @staticmethod
    def producer_startup(node):
        return {
            Event.Fields.EVENT_ID: Eventing.ProducerStartup,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "eventing-producer process startup",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def consumer_startup(node):
        return {
            Event.Fields.EVENT_ID: Eventing.ConsumerStartup,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "eventing-producer process startup",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def consumer_crash(node):
        return {
            Event.Fields.EVENT_ID: Eventing.ConsumerCrash,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "eventing-producer process startup",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def start_tracing(node):
        return {
            Event.Fields.EVENT_ID: Eventing.StartTracing,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Tracing started",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def stop_tracing(node):
        return {
            Event.Fields.EVENT_ID: Eventing.StopTracing,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Tracing stopped",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def start_debugger(node, appname):
        return {
            Event.Fields.EVENT_ID: Eventing.StartDebugger,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Debugger started",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"appName": appname}
        }

    @staticmethod
    def stop_debugger(node, appname):
        return {
            Event.Fields.EVENT_ID: Eventing.StopDebugger,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Debugger stopped",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"appName": appname}
        }

    @staticmethod
    def create_function(node, appname):
        return {
            Event.Fields.EVENT_ID: Eventing.CreateFunction,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Create Function",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"appName": appname}
        }

    @staticmethod
    def delete_function(node, appname):
        return {
            Event.Fields.EVENT_ID: Eventing.DeleteFunction,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Delete Function",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node,
            Event.Fields.EXTRA_ATTRS: {"appName": appname}
        }

    @staticmethod
    def import_functions(node):
        return {
            Event.Fields.EVENT_ID: Eventing.ImportFunctions,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Import Functions",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def export_functions(node):
        return {
            Event.Fields.EVENT_ID: Eventing.ExportFunctions,
            Event.Fields.COMPONENT: Event.Component.EVENTING,
            Event.Fields.DESCRIPTION: "Export Functions",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "eventing-producer",
            Event.Fields.NODE_NAME: node
        }