NS_SERVER_TIMEOUT = 120
COUCHBASE_SINGLE_DEFAULT_INI_PATH = "/opt/couchbase/etc/couchdb/default.ini"
MEMBASE_DATA_PATH = "/opt/membase/var/lib/membase/data/"
COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"
WIN_MEMBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Membase/Server/var/lib/membase/data/'
WIN_COUCHBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/membase/data/'
WIN_CB_PATH = "/cygdrive/c/Program Files/Couchbase/Server/"
WIN_MB_PATH = "/cygdrive/c/Program Files/Membase/Server/"
LINUX_CB_PATH = "/opt/couchbase/"
WIN_REGISTER_ID = {"1654":"70668C6B-E469-4B72-8FAD-9420736AAF8F", "170":"AF3F80E5-2CA3-409C-B59B-6E0DC805BC3F", \
                   "171":"73C5B189-9720-4719-8577-04B72C9DC5A2", "1711":"73C5B189-9720-4719-8577-04B72C9DC5A2", \
                   "172":"374CF2EC-1FBE-4BF1-880B-B58A86522BC8", "180":"D21F6541-E7EA-4B0D-B20B-4DDBAF56882B", \
                   "181":"A68267DB-875D-43FA-B8AB-423039843F02", "200":"9E3DC4AA-46D9-4B30-9643-2A97169F02A7"}
VERSION_FILE = "VERSION.txt"
MIN_COMPACTION_THRESHOLD = 2
MAX_COMPACTION_THRESHOLD = 100
LINUX_CBBACKUP_COMMAND_PATH = "/opt/couchbase/bin/cbbackup"
WIN_CBBACKUP_COMMAND_PATH = "C:\program files\couchbase\server\bin\cbbackup.exe"
LINUX_CBRESTORE_COMMAND_PATH = "/opt/couchbase/bin/cbrestore"
WIN_CBRESTORE_COMMAND_PATH = "C:\program files\couchbase\server\bin\cbrestore.exe"
#cbbackup cbrestore is not working for windows yet.