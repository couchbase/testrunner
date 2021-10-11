from SystemEventLogLib.Events import Event
from cb_constants.system_event_log import Analytics


class AnalyticsEvents(object):
    @staticmethod
    def node_restart(node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.NodeRestart,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS
        }

    @staticmethod
    def process_crashed(node, crashed_pid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.ProcessCrashed,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'pid': crashed_pid}
        }

    @staticmethod
    def dateset_created(node, dataset):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.DataSetCreated,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'dataset': dataset}
        }

    @staticmethod
    def dateset_deleted(node, dataset):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.DataSetDeleted,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'dataset': dataset}
        }

    @staticmethod
    def date_verse_created(node, data_verse):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.DateVerseCreated,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'data_verse': data_verse}
        }

    @staticmethod
    def date_verse_deleted(node, data_verse):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.DateVerseDeleted,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'data_verse': data_verse}
        }

    @staticmethod
    def index_created(node, index_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.IndexCreated,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'index_uuid': index_uuid}
        }

    @staticmethod
    def index_deleted(node, index_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.IndexDropped,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'index_uuid': index_uuid}
        }

    @staticmethod
    def link_created(node, link_uuid, target_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.LinkCreated,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'link_uuid': link_uuid,
                                       'target_uuid': target_uuid}
        }

    @staticmethod
    def link_deleted(node, link_uuid, target_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.LinkDropped,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'link_uuid': link_uuid,
                                       'target_uuid': target_uuid}
        }

    @staticmethod
    def settings_changed(node, prev_settings, new_settings):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Analytics.SettingChanged,
            Event.Fields.COMPONENT: Event.Component.ANALYTICS,
            Event.Fields.EXTRA_ATTRS: {'prev_settings': prev_settings,
                                       'new_settings': new_settings}
        }
