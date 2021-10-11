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


class Query(object):
    """Mapping of Query related events - event_id. Valid range 1024:2047"""


class Index(object):
    """Mapping of Index (2i) related events - event_id. Valid range 2048:3071"""


class Fts(object):
    """Mapping of FTS (search) related events - event_id. Valid range 3072:4095"""


class Eventing(object):
    """Mapping of Eventing related events - event_id. Valid range 4096:5119"""


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


class Xdcr(object):
    """Mapping of XDCR related events - event_id. Valid range 7168:8191"""
    CreateReplication = 0
    IncomingReplication = 0
    ModifyReplication = 0
    RemoveReplication = 0
    ProcessCrashed = 0
    SettingChanged = 0
    Paused = 0
    Resumed = 0


class Backup(object):
    """Mapping of Backup related events - event_id. Valid range 6143:7167"""
