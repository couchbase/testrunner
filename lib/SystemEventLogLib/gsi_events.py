from SystemEventLogLib.Events import Event
from constants.cb_constants.system_event_log import Index


class IndexingServiceEvents(object):
    @staticmethod
    def index_settings_updated(old_setting, new_setting, node):
        return {
            Event.Fields.EVENT_ID: Index.IndexSettingsChanged,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: 'Indexer Settings Changed',
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"old_setting": old_setting, "new_setting": new_setting},
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def projector_settings_updated(old_setting, new_setting, node):
        return {
            Event.Fields.EVENT_ID: Index.ProjectorSettingsChanged,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: 'Projector Settings Changed',
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Projector",
            Event.Fields.EXTRA_ATTRS: {"old_setting": old_setting, "new_setting": new_setting},
            Event.Fields.NODE_NAME: node

        }

    @staticmethod
    def query_client_settings_updated(old_setting, new_setting, node):
        return {
            Event.Fields.EVENT_ID: Index.QueryClientSettingsChanged,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: 'Query Client Settings Changed',
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "GSIClient",
            Event.Fields.EXTRA_ATTRS: {"old_setting": old_setting, "new_setting": new_setting},
            Event.Fields.NODE_NAME: node
        }

    @staticmethod
    def index_crash(node, process_id):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexerCrash,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.SEVERITY: "fatal",
            Event.Fields.DESCRIPTION: "Indexer Process Crashed"
        }

    @staticmethod
    def projector_crash(node, process_id):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.ProjectorCrash,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.SEVERITY: "fatal",
            Event.Fields.DESCRIPTION: "Projector Process Crashed"
        }

    @staticmethod
    def query_client_crash(node, process_id):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.QueryClientCrash,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.SEVERITY: "fatal",
            Event.Fields.DESCRIPTION: "Query Client Crashed"
        }

    @staticmethod
    def index_created(node, definition_id, instance_id, indexer_id, replica_id, partition_id=None,
                      is_proxy_instance=False):

        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexCreated,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Instance or Partition Created",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": instance_id,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id}}
        if partition_id:
            event[Event.Fields.EXTRA_ATTRS]['partition_id'] = partition_id
        if is_proxy_instance:
            event[Event.Fields.EXTRA_ATTRS]['is_proxy_instance'] = is_proxy_instance
            event[Event.Fields.EXTRA_ATTRS]['real_instance_id'] = instance_id
            event[Event.Fields.EXTRA_ATTRS].pop('instance_id')
        return event

    @staticmethod
    def index_dropped(node, definition_id, instance_id, indexer_id, replica_id, partition_id=None,
                      is_proxy_instance=False):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexDropped,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Instance or Partition Dropped",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": instance_id,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id}}
        if partition_id:
            event[Event.Fields.EXTRA_ATTRS]['partition_id'] = partition_id
        if is_proxy_instance:
            event[Event.Fields.EXTRA_ATTRS]['is_proxy_instance'] = is_proxy_instance
            event[Event.Fields.EXTRA_ATTRS]['real_instance_id'] = instance_id
            event[Event.Fields.EXTRA_ATTRS].pop('instance_id')
        return event

    @staticmethod
    def index_online(node, definition_id, instance_id, indexer_id, replica_id, partition_id=None):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexOnline,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Instance or Partition Online",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": instance_id,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id}}
        if partition_id:
            event[Event.Fields.EXTRA_ATTRS]['partition_id'] = partition_id
        return event

    @staticmethod
    def index_building(node, definition_id, instance_id, indexer_id, replica_id, partition_id=None):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexBuilding,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Instance or Partition Building",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": instance_id,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id}}
        if partition_id:
            event[Event.Fields.EXTRA_ATTRS]['partition_id'] = partition_id
        return event

    @staticmethod
    def index_partition_merged(node, definition_id, instance_id, indexer_id, replica_id, partition_id=None):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexPartitionedMerged,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Partition Merged",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": instance_id,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id,
                                       "partition_id": partition_id}}
        return event

    @staticmethod
    def index_error(node, definition_id, instance_id, indexer_id, replica_id, error_string):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexError,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Instance or Partition Error State Change",
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": instance_id,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id,
                                       "error_string": error_string}}
        return event

    @staticmethod
    def index_schedule_error(node, definition_id, instance_id, indexer_id, replica_id, error_string):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexScheduledCreationError,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Scheduled Creation Error",
            Event.Fields.SEVERITY: Event.Severity.ERROR,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": instance_id,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id,
                                       "error_string": error_string}}
        return event

    @staticmethod
    def index_schedule_for_creation(node, definition_id, indexer_id, replica_id):
        event = {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: Index.IndexScheduledForCreation,
            Event.Fields.COMPONENT: Event.Component.INDEXING,
            Event.Fields.DESCRIPTION: "Index Scheduled for Creation",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.SUB_COMPONENT: "Indexer",
            Event.Fields.EXTRA_ATTRS: {"definition_id": definition_id,
                                       "instance_id": 0,
                                       "indexer_id": indexer_id,
                                       "replica_id": replica_id,
}}
        return event
