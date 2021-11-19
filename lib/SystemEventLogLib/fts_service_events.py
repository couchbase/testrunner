from SystemEventLogLib.Events import Event
from constants.cb_constants.system_event_log import Fts


class SearchServiceEvents(object):
    @staticmethod
    def index_created(node, index_uuid, index_name, source_name):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Fts.IndexCreateEventID,
            Event.Fields.COMPONENT: Event.Component.SEARCH,
            Event.Fields.EXTRA_ATTRS: {'indexName': index_name, 'indexUUID': index_uuid, 'sourceName': source_name}
        }

    @staticmethod
    def index_deleted(node, index_uuid, index_name, source_name):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Fts.IndexDeleteEventID,
            Event.Fields.COMPONENT: Event.Component.SEARCH,
            Event.Fields.EXTRA_ATTRS: {'indexName': index_name, 'indexUUID': index_uuid, 'sourceName': source_name}
        }

    @staticmethod
    def index_updated(node, index_uuid, index_name, source_name):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Fts.IndexUpdateEventID,
            Event.Fields.COMPONENT: Event.Component.SEARCH,
            Event.Fields.EXTRA_ATTRS: {'indexName': index_name, 'indexUUID': index_uuid, 'sourceName': source_name}
        }

    @staticmethod
    def fts_crash(node, process_id):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Fts.CrashEventID,
            Event.Fields.COMPONENT: Event.Component.SEARCH,
            Event.Fields.SEVERITY: "fatal",
            Event.Fields.DESCRIPTION: "Crash",
            Event.Fields.EXTRA_ATTRS: {'details': 'main: could not write uuidPath: /data/@fts/cbft.uuid,\n  err: &os.PathError{Op:\"write\", Path:\"/data/@fts/cbft.uuid\", Err:0x1c},\n  Please check that your -data/-dataDir parameter (\"/data/@fts\")\n  is to a writable directory where cbft can persist data.',
                                       'processID': process_id, 'processName': 'cbft'}
        }

    @staticmethod
    def fts_crash_no_processid(node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Fts.CrashEventID,
            Event.Fields.COMPONENT: Event.Component.SEARCH,
            Event.Fields.SEVERITY: "fatal",
            Event.Fields.DESCRIPTION: "Crash"
        }

    @staticmethod
    def fts_settings_updated():
        return {
            Event.Fields.EVENT_ID: Fts.SettingsUpdateEventID,
            Event.Fields.COMPONENT: Event.Component.SEARCH,
            Event.Fields.DESCRIPTION: 'Manager options updated'
        }
