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

### Default Installation Tasks
When `install_tasks` parameter is not explicitly provided, the script uses:
```python
DEFAULT_INSTALL_TASKS = ["uninstall", "download_build", "install", "init", "cleanup"]
DEFAULT_INSTALL_COLUMNAR_TASKS = ["uninstall", "download_build", "install", "cleanup"]
REINIT_NODE_TASKS = ["init", "cleanup"]
```

## Command-line Example
Typical execution pattern from test runners:
```bash
${py_executable} scripts/new_install.py \
  -i /tmp/testexec.$$.ini \
  -p timeout=${INSTALL_TIMEOUT},skip_local_download=${SKIP_LOCAL_DOWNLOAD}, \
     get-cbcollect-info=True,version=${initial_version},product=cb, \
     debug_logs=True,ntp=True,url=${url}${extraInstall}
```

Note: `install_tasks` is often omitted to trigger default installation steps.

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

## Execution Flow
```
main()
  ├─> install_utils.process_user_input() → Parse params from INI/CLI/env
  ├─> Create install_tasks dict and node_helpers list
  ├─> install_utils.pre_install_steps(node_helpers)           ← Pre-installation setup
  │      ├─> Tools setup (if "tools" in install_tasks)
  │      │      └─> Set dev_tools_name, admin_tools_name and URLs
  │      └─> Build URL validation & node.build object creation
  │             └─> Validate URL is live, set all_nodes_same_os flag
  │             └─> Determine build_binary (columnar vs regular)
  │             └─> Create node.build with binary, URL, filepath, version
  ├─> (if uninstall) do_uninstall(params, node_helpers)
  │      └─> Parallel uninstallation via worker threads
  ├─> (if install/download_build) install_utils.download_build(node_helpers)
  ├─> (if tools) install_utils.install_tools(node_helpers)
  └─> do_install(params, install_tasks)
         ├─> Spawn per-node threads with task queues
         ├─> Execute tasks: uninstall, install, init, cleanup
         ├─> (if init_clusters) install_utils.init_clusters()
         └─> validate_install() + validate_columnar_install()
                  └─> install_utils.print_result_and_exit()
```

### pre_install_steps() Details
Located in `install_utils.py`, called before main installation workflow:
- **Early exit**: Returns immediately if no node_helpers provided
- **Tools preparation**: Sets up development and admin tools package names/URLs
- **Build preparation**: 
  - Validates custom build URLs if provided
  - Sets `params["all_nodes_same_os"] = True` for URL-based installs
  - Determinates correct build binary per node (columnar vs regular profile)
  - Creates `node.build` objects containing download and installation metadata

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
