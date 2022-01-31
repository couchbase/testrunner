from SystemEventLogLib.Events import Event
from constants.cb_constants.system_event_log import Views


class ViewsServiceEvents(object):
    @staticmethod
    def DDoc_Created(node):
        return {
            Event.Fields.EVENT_ID: Views.DDocCreated,
            Event.Fields.COMPONENT: Event.Component.VIEWS,
            Event.Fields.DESCRIPTION: "views - DDoc created",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "views-DDoc",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def DDoc_Deleted(node):
        return {
            Event.Fields.EVENT_ID: Views.DDocDeleted,
            Event.Fields.COMPONENT: Event.Component.VIEWS,
            Event.Fields.DESCRIPTION: "views - DDoc Deleted",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "views-DDoc",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def DDoc_Updated(node):
        return {
            Event.Fields.EVENT_ID: Views.DDocModified,
            Event.Fields.COMPONENT: Event.Component.VIEWS,
            Event.Fields.DESCRIPTION: "views - DDoc Modified",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "views-DDoc",
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def DDoc_Settings_change(node):
        return {
            Event.Fields.EVENT_ID: Views.ViewEngineSettingsChange,
            Event.Fields.COMPONENT: Event.Component.VIEWS,
            Event.Fields.DESCRIPTION: "views - DDoc created",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "views-DDoc",
            Event.Fields.NODE_NAME: node
        }
