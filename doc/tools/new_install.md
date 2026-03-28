# new_install.py

**Path:** `scripts/new_install.py`

**Purpose:** Installer script used within all functional runs to install Couchbase on-prem servers for testing.

## Context
- **Type:** VM Installer / Bootstrap Script
- **Usage:** All functional test runs requiring Couchbase on-prem servers
- **Execution:** Typically called by test dispatcher frameworks

## Functionality
- Multi-threaded installation of Couchbase across multiple VMs simultaneously
- Supports installation tasks: uninstall, install, init, cleanup, tools
- Validates Columnar profile installations (for columnar servers)
- Downloads Couchbase builds and installs tools on target servers
- Configures NodeHelpers for remote server management
- Error handling and queue-based task distribution

## Installation Tasks
- **uninstall**: Uninstall existing Couchbase installation
- **install**: Install Couchbase server
- **init**: Initialize Couchbase server
- **cleanup**: Cleanup server state
- **tools**: Install required tools
- **download_build**: Download Couchbase build artifacts

## Columnar Validation
Special validation for columnar profile installs:
- Checks enterprise edition status
- Verifies configProfile matches "columnar"
- Validates version matches expected build
- Ensures node status is "healthy"
- Logs cluster node details for debugging

## Usage
```bash
python scripts/new_install.py
```

The script reads installation parameters via `install_utils.process_user_input()` which typically sources from:
- Command-line arguments
- INI files
- Environment variables

## Architecture

### Threading Model
- Uses `queue.Queue()` for task distribution
- Creates worker threads for each node
- Each node gets a dedicated installer thread

### Node Helpers
- Uses `install_utils.NodeHelpers` for remote server management
- `install_utils.get_node_helper()` creates helper instances per server
- Handles SSH connectivity and remote operations

### Error Handling
- `on_install_error()` handles installation failures
- Empties node queue on error to prevent cascading failures
- Traces exceptions for debugging

## Dependencies
- `install_utils` - Node helper utilities and parameter processing
- `install_constants` - Installation configuration constants
- `logging.config` - Logging configuration (uses "scripts.logging.conf")
- `membase.api.exception.InstallException` - Custom installation exceptions
- `membase.api.rest_client.RestConnection` - REST API for server communication

## Integration
Called after VM booking by dispatcher scripts to prepare servers for test execution flow:
1. Dispatcher books VMs (e.g., `dispatcher_sdk4.py`)
2. `new_install.py` installs Couchbase on booked VMs
3. TestRunner executes tests on provisioned servers

## Notes
- Multi-threaded installation for parallelization across many VMs
- Validation logic for Columnar servers (7.6+ versions)
- Logs to standard logger configured from "scripts.logging.conf"
- No command-line argument parsing (all params via `install_utils.process_user_input()`)
