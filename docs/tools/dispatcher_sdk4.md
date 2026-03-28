# dispatcher_sdk4.py

**Path:** `scripts/testDispatcher_sdk4.py`

**Purpose:** Modern dispatcher using Couchbase SDK4 + transactions to book VMs for regression testing.

## Context
- **Type:** Test Dispatcher / VM Booking System
- **SDK Version:** Couchbase SDK4 with transactions
- **Target Environment:** Modern infrastructure (replaces deprecated CentOS P0 slaves)
- **Status:** Active production dispatcher

## Functionality
- Takes an INI template as input and populates it with available server pool
- Books VMs from QE server pool database using Couchbase SDK4
- Uses Couchbase transactions for reliable VM booking operations
- Supports multiple server types: DOCKER, AWS, AZURE, GCP, VM, KUBERNETES, and cloud options (SERVERLESS_ONCLOUD, PROVISIONED_ONCLOUD, SERVERLESS_COLUMNAR)
- Manages server allocation for both main pools and additional pools
- Integrates with Jenkins for test pipeline triggering
- Includes detailed logging with configurable log levels

## Usage

```bash
python scripts/testDispatcher_sdk4.py -s suitefile -v version -o OS -p poolId
```

### Options
- `-v, --version`: Couchbase version to install
- `-r, --run`: Run type (should be '12hour' or 'weekly')
- `-o, --os`: Operating system to target
- `-n, --noLaunch`: Skip launching (default: False)
- `-c, --component`: Component to test
- `-p, --poolId`: Pool ID (default: '12hour')
- `-a, --addPoolId`: Additional pool ID (optional)
- `-t, --test`: Use test Jenkins (default: False)
- `-s, --subcomponent`: Subcomponent to test (optional)

## Key Features

### Transaction Support
Uses Couchbase SDK4 transactions for atomic VM booking operations:
- `QE-server-pool` bucket for server management
- Retry logic with controlled backoff
- Automatic transaction handling via ServerManager

### Logging
Configurable logging with format: `%(asctime)s: %(funcName)s:L%(lineno)d: %(levelname)s: %(message)s`

### Server Management
- TableView for server pool visualization
- ServerManager class for database operations
- AFP (Agent Framework Protocol) integration

## Dependencies
- `couchbase.cluster` (SDK4)
- `couchbase.auth` (PasswordAuthenticator)
- `couchbase.options` (ClusterOptions)
- `cloud_provision`
- `find_rerun_job`
- `capella`
- `server_manager`
- `table_view`
- `paramiko` (SSH)
- `requests`

## Differences from testDispatcher.py
| Feature | testDispatcher.py (Legacy) | dispatcher_sdk4.py (Modern) |
|---------|---------------------------|----------------------------|
| SDK Version | SDK2 | SDK4 |
| Transactions | No | Yes |
| Infrastructure | Deprecated CentOS P0 | Modern infrastructure |
| Logging | Basic | Configurable with levels |
| Server Manager | Direct HTTP calls | ServerManager class with TableView |

## Notes
- Active production dispatcher for QE regression runs
- Uses hardcoded server manager credentials (TEST_SUITE_DB)
- Polls server manager at 60-second intervals (POLL_INTERVAL)
- Maintains backward compatibility with most command-line options from legacy dispatcher
