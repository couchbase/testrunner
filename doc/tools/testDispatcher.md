# testDispatcher.py

**Path:** `scripts/testDispatcher.py`

**Purpose:** Legacy dispatcher using Couchbase SDK2 to book VMs for regression testing.

## Context
- **Type:** Test Dispatcher / VM Booking System
- **SDK Version:** Couchbase SDK2
- **Target Environment:** Deprecated CentOS P0 labeled slaves
- **Status:** Legacy (deprecated infrastructure)

## Functionality
- Takes an INI template as input and populates it with available server pool
- Books VMs from server management database for test execution
- Handles various server types: DOCKER, AWS, AZURE, GCP, VM, KUBERNETES, and cloud options
- Manages server allocation for both main pools and additional pools
- Integrates with Jenkins for test pipeline triggering

## Usage

```bash
python scripts/testDispatcher.py -s suitefile -v version -o OS -p poolId
```

### Options
- `-v, --version`: Couchbase version to install
- `-r, --run`: Run type (12 hour or weekly)
- `-o, --os`: Operating system to target
- `-n, --noLaunch`: Skip launching (default: False)
- `-c, --component`: Component to test
- `-p, --poolId`: Pool ID (default: '12hour')
- `-a, --addPoolId`: Additional pool ID (optional)
- `-t, --test`: Use test Jenkins (default: False)

## Dependencies
- `couchbase.cluster` (SDK2)
- `cloud_provision`
- `find_rerun_job`
- `capella`
- `paramiko` (SSH)
- `httplib2`

## Notes
- Deprecated infrastructure - consider migrating to `dispatcher_sdk4.py`
- Uses hardcoded server manager credentials (TEST_SUITE_DB)
- Polls server manager at 60-second intervals (POLL_INTERVAL)
