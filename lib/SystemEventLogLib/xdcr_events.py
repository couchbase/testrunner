from SystemEventLogLib.Events import Event
from constants.cb_constants.system_event_log import XDCR

#     CreateRemoteClusterRef = 7168
#     UpdateRemoteClusterRef = 7169
#     DeleteRemoteClusterRef = 7170
#     CreateReplication = 7171
#     PauseReplication = 7172
#     ResumeReplication = 7173
#     DeleteReplication = 7174
#     UpdateDefaultReplicationSetting = 7175
#     UpdateReplicationSetting = 7176

class XDCRServiceEvents(object):
    @staticmethod
    def create_remote_cluster_ref(node, remote, uuid, encryption_type, remote_ref_name):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.CreateRemoteClusterRef,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {'hostname': remote,
                                       'uuid': uuid,
                                       'encryptionType': encryption_type,
                                       'name': remote_ref_name}
        }

    @staticmethod
    def update_remote_cluster_ref(node, remote, uuid, encryption_type, remote_ref_name, ):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.UpdateRemoteClusterRef,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {'hostname': remote,
                                       'uuid': uuid,
                                       'encryptionType': encryption_type,
                                       'name': remote_ref_name}
        }

    @staticmethod
    def delete_remote_cluster_ref(node, remote, uuid, encryption_type, remote_ref_name):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.DeleteRemoteClusterRef,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {'hostname': remote,
                                       'uuid': uuid,
                                       'encryptionType': encryption_type,
                                       'name': remote_ref_name}
        }

    @staticmethod
    def create_replication(node, source_bucket, target_bucket, id, remote_ref_name, filter_expression):
        if not filter_expression:
            filter_expression = "false"
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.CreateReplication,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {'sourceBucket': source_bucket,
                                       'targetBucket': target_bucket,
                                       'id': id,
                                       'remoteClusterName': remote_ref_name,
                                       'filter_expression': filter_expression}
        }


    @staticmethod
    def pause_replication(node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.UpdateReplicationSetting,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {}
        }

    @staticmethod
    def resume_replication(node):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.UpdateReplicationSetting,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {}
        }

    @staticmethod
    def delete_replication(node,source_bucket, target_bucket, id, remote_ref_name, filter_expression):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.UpdateReplicationSetting,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {'sourceBucket': source_bucket,
                                       'targetBucket': target_bucket,
                                       'id': id,
                                       'remoteClusterName': remote_ref_name,
                                       'filter_expression': filter_expression}
        }

    @staticmethod
    def update_default_replication_setting(node, source_bucket, target_bucket, id, setting_name, setting_value):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.UpdateReplicationSetting,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {'sourceBucket': source_bucket,
                                       'targetBucket': target_bucket,
                                       'id': id,
                                       setting_name: setting_value}
        }

    @staticmethod
    def update_replication_setting(node, source_bucket, target_bucket, id, setting_name, setting_value):
        return {
            Event.Fields.NODE_NAME: node,
            Event.Fields.EVENT_ID: XDCR.UpdateReplicationSetting,
            Event.Fields.COMPONENT: Event.Component.XDCR,
            Event.Fields.EXTRA_ATTRS: {'sourceBucket': source_bucket,
                                       'targetBucket': target_bucket,
                                       'id': id,
                                       setting_name: setting_value}
        }
