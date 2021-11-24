from SystemEventLogLib.Events import Event
from cb_constants.system_event_log import KvEngine


class DataServiceEvents(object):
    @staticmethod
    def bucket_offline(node, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketOffline,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket_uuid}
        }

    @staticmethod
    def bucket_online(node, bucket, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketOnline,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Bucket online",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid}
        }

    @staticmethod
    def bucket_create(node, bucket_type, bucket, bucket_uuid, bucket_settings):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketCreated,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Bucket created",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid,
                                       "bucket_type": bucket_type,
                                       "bucket_props": bucket_settings}
        }

    @staticmethod
    def bucket_dropped(node, bucket, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketDeleted,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Bucket deleted",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid}
        }

    @staticmethod
    def bucket_flushed(node, bucket, bucket_uuid):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketFlushed,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Bucket flushed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid}
        }

    @staticmethod
    def bucket_updated(node, bucket, bucket_uuid, bucket_type,
                       old_settings, new_settings):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.BucketConfigChanged,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Bucket configuration changed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid,
                                       "type": bucket_type,
                                       "old_settings": old_settings,
                                       "new_settings": new_settings}
        }

    @staticmethod
    def collection_created(node, bucket, bucket_uuid, scope, collection):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.CollectionCreated,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Collection created",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid,
                                       "scope": scope,
                                       "collection": collection}
        }

    @staticmethod
    def collection_dropped(node, bucket, bucket_uuid, scope, collection):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.CollectionDropped,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Collection deleted",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid,
                                       "scope": scope,
                                       "collection": collection}
        }

    @staticmethod
    def scope_created(node, bucket, bucket_uuid, scope):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.ScopeCreated,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Scope created",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid,
                                       "scope": scope}
        }

    @staticmethod
    def scope_dropped(node, bucket, bucket_uuid, scope):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.ScopeDropped,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Scope deleted",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid,
                                       "scope": scope}
        }

    @staticmethod
    def ephemeral_auto_reprovision(node, bucket, bucket_uuid,
                                   nodes, restarted_on):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.EphemeralAutoReprovision,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Bucket auto reprovisioned",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"bucket": bucket,
                                       "bucket_uuid": bucket_uuid,
                                       "nodes": ["ns_1@"+ip for ip in nodes],
                                       "restarted_on":
                                           ["ns_1@"+ip for ip in restarted_on]
                                       }
        }

    @staticmethod
    def memcached_settings_changed(node, prev_settings, new_settings):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: KvEngine.MemcachedConfigChanged,
            Event.Fields.COMPONENT: Event.Component.DATA,
            Event.Fields.DESCRIPTION: "Memcached configuration changed",
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EXTRA_ATTRS: {"old_settings": prev_settings,
                                       "new_settings": new_settings}
        }
