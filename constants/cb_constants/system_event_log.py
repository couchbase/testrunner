class NsServer(object):
    """Mapping of ns_server events - event_id. Valid range 0-1023"""
    NodeAdded = 0
    ServiceStarted = 1
    RebalanceStarted = 2
    RebalanceComplete = 3
    RebalanceFailure = 4
    RebalanceInterrupted = 5
    GracefulFailoverStarted = 6
    GracefulFailoverComplete = 7
    GracefulFailoverFailed = 8
    GracefulFailoverInterrupted = 9
    HardFailoverStarted = 10
    HardFailoverComplete = 11
    HardFailoverFailed = 12
    HardFailoverInterrupted = 13
    AutoFailoverStarted = 14
    AutoFailoverComplete = 15
    AutoFailoverFailed = 16
    AutoFailoverWarning = 17
    MasterSelected = 18

    BabySitterRespawn = 0
    NodeOffline = 0
    TopologyChange = 0


class KvEngine(object):
    """Mapping of KV related events - event_id. Valid range 8192:9215"""
    BucketCreated = 8192
    BucketDeleted = 8193
    ScopeCreated = 8194
    ScopeDropped = 8195
    CollectionCreated = 8196
    CollectionDropped = 8197
    BucketFlushed = 8198
    BucketOnline = 8199
    BucketOffline = 8200
    BucketConfigChanged = 8201
    MemcachedConfigChanged = 8202
    EphemeralAutoReprovision = 8203
    MemcachedCrashed = 8204


class Security(object):
    """Mapping of Security related events - event_id. Valid range 9216:10239"""
    AuditEnabled = 9216
    AuditDisabled = 9217
    AuditSettingChanged = 9218
    LdapConfigChanged = 9219
    SecurityConfigChanged = 9220
    SasldAuthConfigChanged = 9221
    PasswordPolicyChanged = 9222

    GroupAdded = 0
    GroupRemoved = 0
    LdapEnabledDisabledForGroup = 0
    LdapEnabledDisabledForUsers = 0
    PamEnabledDisabled = 0
    UserAdded = 0
    UserRemoved = 0


class Views(object):
    """Mapping of Views related events - event_id. Valid range 10240:11263"""
    DDocCreated=10240
    DDocDeleted=10241
    DDocModified=10242
    DraftDDocDeployed=10243
    ViewEngineCrash=10244
    ViewEngineSettingsChange=10245


class Query(object):
    """Mapping of Query related events - event_id. Valid range 1024:2047"""


class Index(object):
    """Mapping of Index (2i) related events - event_id. Valid range 2048:3071"""
    IndexSettingsChanged = 2048
    ProjectorSettingsChanged = 2049
    QueryClientSettingsChanged = 2050
    IndexerCrash = 2051
    ProjectorCrash = 2052
    QueryClientCrash = 2053
    IndexCreated = 2054
    IndexBuilding = 2055
    IndexOnline = 2056
    IndexDropped = 2057
    IndexPartitionedMerged = 2058
    IndexError = 2059
    IndexScheduledForCreation = 2060
    IndexScheduledCreationError = 2061



class Fts(object):
    """Mapping of FTS (search) related events - event_id. Valid range 3072:4095"""
    ServiceStartEventID = 3072
    IndexCreateEventID = 3073
    IndexUpdateEventID = 3074
    IndexDeleteEventID = 3075
    SettingsUpdateEventID = 3076
    CrashEventID = 4095

class Eventing(object):
    """Mapping of Eventing related events - event_id. Valid range 4096:5119"""
    ProducerStartup = 4096
    ConsumerStartup = 4097
    ConsumerCrash = 4098
    StartTracing = 4099
    StopTracing = 4100
    StartDebugger = 4101
    StopDebugger = 4102
    CreateFunction = 4103
    DeleteFunction = 4104
    ImportFunctions = 4105
    ExportFunctions = 4106
    DeployFunction = 4109
    UndeployFunction = 4110
    PauseFunction = 4111
    ResumeFunction = 4112


class Analytics(object):
    """Mapping of Analytics related events - event_id. Valid range 5120:6143"""
    NodeRestart = 0
    ProcessCrashed = 0
    DataSetCreated = 0
    DataSetDeleted = 0
    DateVerseCreated = 0
    DateVerseDeleted = 0
    IndexCreated = 0
    IndexDropped = 0
    LinkCreated = 0
    LinkDropped = 0
    SettingChanged = 0


class XDCR(object):
    """Mapping of XDCR related events - event_id. Valid range 7168:8191"""
    CreateRemoteClusterRef = 7168
    UpdateRemoteClusterRef = 7169
    DeleteRemoteClusterRef = 7170
    CreateReplication = 7171
    PauseReplication = 7172
    ResumeReplication = 7173
    DeleteReplication = 7174
    UpdateDefaultReplicationSetting = 7175
    UpdateReplicationSetting = 7176
    ConnectionPreCheck = 7178

class Backup(object):
    """Mapping of Backup related events - event_id. Valid range 6143:7167"""
