from SystemEventLogLib.Events import Event
from cb_constants.system_event_log import KvEngine


class DataServiceEvents(object):
    @staticmethod
    def bucket_offline(node, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketOffline,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid}
        }

    @staticmethod
    def bucket_online(node, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketOnline,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid}
        }

    @staticmethod
    def bucket_create(node, bucket_type, bucket_settings):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketCreated,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket_type': bucket_type,
                                       'settings': bucket_settings}
        }

    @staticmethod
    def bucket_dropped(node, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketDeleted,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid}
        }

    @staticmethod
    def bucket_flushed(node, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketFlushed,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid}
        }

    @staticmethod
    def bucket_updated(node, bucket_uuid, old_settings, new_settings):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketConfigChanged,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid,
                                       'old_settings': old_settings,
                                       'new_settings': new_settings}
        }

    @staticmethod
    def collection_created(node, bucket_uuid, scope, collection):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.CollectionCreated,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid,
                                       'scope': scope,
                                       'collection': collection}
        }

    @staticmethod
    def collection_dropped(node, bucket_uuid, scope, collection):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.CollectionDropped,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid,
                                       'scope': scope,
                                       'collection': collection}
        }

    @staticmethod
    def scope_created(node, bucket_uuid, scope):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.ScopeCreated,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid,
                                       'scope': scope}
        }

    @staticmethod
    def scope_dropped(node, bucket_uuid, scope):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.ScopeDropped,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid,
                                       'scope': scope}
        }

    @staticmethod
    def ephemeral_auto_reprovision(node, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.EphemeralAutoReprovision,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'bucket': bucket_uuid}
        }

    @staticmethod
    def memcached_crashed(node, crashed_pid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.MemcachedCrashed,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'pid': crashed_pid}
        }

    @staticmethod
    def memcached_settings_changed(node, prev_settings, new_settings):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.MemcachedConfigChanged,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {'prev_settings': prev_settings,
                                       'new_settings': new_settings}
        }
