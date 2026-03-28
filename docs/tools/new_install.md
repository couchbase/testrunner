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
- **Per-node install_tasks customization**: Each node can have different tasks based on its state
- Automatic version checking to skip unnecessary downloads and installations
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

# Enable per-node version check optimization (skip reinstall if version matches)
${py_executable} scripts/new_install.py \
  -i /tmp/testexec.$$.ini \
  -p version=7.6.0-1234,force_reinstall=False
```

Note:
- `install_tasks` is often omitted to trigger default installation steps
- `force_reinstall` defaults to `True` (full reinstall always). Set to `False` to enable per-node version check optimization.

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
  ├─> check_if_version_already_installed() for EACH node ← Per-node version check (PARALLEL)
  │      ├─> Spawn thread per node for parallel version checking and state reset
  │      └─> Only runs if force_reinstall=False (default: True/full reinstall)
  │      └─> If version matches: reset_node_state_to_presetup() → pkill memcached, sleep 5, reset_node()
  │      └─> Remove download_build, uninstall, install from node.install_tasks
  │      └─> Per-node install_tasks customization (flexible handling of mixed node states)
  ├─> install_utils.pre_install_steps(node_helpers)           ← Pre-installation setup
  │      ├─> Tools setup (if "tools" in ANY node.install_tasks)
  │      │      └─> Set dev_tools_name, admin_tools_name and URLs for nodes needing tools
  │      └─> Build URL validation & node.build object creation
  │             └─> Only for nodes with "download_build" or "install" in install_tasks
  │             └─> Validate URL is live, set all_nodes_same_os flag
  │             └─> Determine build_binary (columnar vs regular)
  │             └─> Create node.build with binary, URL, filepath, version
  ├─> Build install_tasks dict from individual node_helper.install_tasks
  ├─> (if ANY node needs uninstall) do_uninstall(params, install_tasks)
  │      └─> Parallel uninstallation using filtered install_tasks dict (consistent pattern)
  │      └─> Only queues 'uninstall' tasks for nodes with 'uninstall' in their install_tasks
  ├─> (if ANY node needs install/download_build) install_utils.download_build(node_helpers)
  ├─> (if ANY node needs tools) install_utils.install_tools(node_helpers)
  └─> do_install(params, install_tasks)
         ├─> Spawn per-node threads with task queues (using individual node.install_tasks)
         ├─> Execute tasks per node: uninstall, install, init, cleanup (as configured per node)
         ├─> (if ANY node needs init) install_utils.init_clusters()
         └─> validate_install() + validate_columnar_install()
                  └─> install_utils.print_result_and_exit()
```

### Per-Node Installation Customization
Each `NodeHelper` instance maintains its own copy of `install_tasks` via `node.install_tasks`. This enables:

1. **Version-based optimization**: If a node already has the target version installed, `download_build`, `uninstall`, and `install` tasks are automatically removed for that node
2. **Mixed cluster handling**: In a multi-node cluster where some nodes need fresh installation while others have the target version, each node gets only the tasks it needs
3. **Efficient processing**: Build preparation, downloads, and installations only happen for nodes that actually need them
4. **Force reinstall control**: Controlled by `force_reinstall` parameter (default: `True`/full reinstall). Set to `False` to enable this optimization.

**force_reinstall parameter**:
- **`True` (default)**: Force full reinstall, skip version check entirely. All nodes follow the full installation process (uninstall, download, install, init, cleanup).
- **`False`**: Enable per-node version check optimization. Only reinstall nodes that don't have the target version. Useful for mixed cluster states or when reusing existing infrastructure.

**Version check logic** (`check_if_version_already_installed()` in `install_utils.py`):
- Uses `RestConnection` to query existing Couchbase installation
- Compares version, build number, and edition (with normalization for edition name matching)
- For 7.5+, additionally checks configProfile match (e.g., "columnar")
- Edition normalization handles format differences (e.g., `couchbase-server-enterprise` vs `enterprise`)
- Returns True if all conditions match

**Node state reset workflow** (`reset_node_state_to_presetup()` in `install_utils.py`):
When version is already installed and optimization is enabled (`force_reinstall=False`):
1. **Pre-init cleanup**: Executes `pkill -9 memcached` to force-kill any stuck memcached processes
2. **Wait**: Sleeps 5 seconds for cleanup to complete
3. **Reset node**: Calls `reset_node()` REST API to reset node to pre-init state
4. **Proceed with init**: The node is now ready for cluster init step

This ensures nodes in weird/stuck states are cleaned up before running the init task, allowing the new_install init to initialize the cluster with required parameters for the current run/job.

### pre_install_steps() Details
Located in `install_utils.py`, called before main installation workflow:
- **Early exit**: Returns immediately if no node_helpers provided
- **Per-node task filtering**: Only processes nodes that have relevant tasks
- **Tools preparation**: Sets up development and admin tools package names/URLs for nodes with "tools" task
- **Build preparation**: 
  - Only for nodes with "download_build" or "install" in their install_tasks
  - Validates custom build URLs if provided
  - Sets `params["all_nodes_same_os"] = True` for URL-based installs
  - Determines correct build binary per node (columnar vs regular profile)
  - Creates `node.build` objects containing download and installation metadata
  - **Note**: Nodes with matching version (when `force_reinstall=False`) skip build preparation and have `node.build = None`

### download_build() Details
Located in `install_utils.py`, called after pre_install_steps:
- **Mixed cluster handling**: Filters nodes with `node.build is not None` before processing
  - Nodes with matching version (skipped install) have `node.build = None` and are excluded
  - Only downloads for nodes that actually need fresh installation
- **Early exit**: Returns with info log if no nodes need build download (all nodes have matching version)
- **Optimization**: For `all_nodes_same_os=True` + `all_nodes_same_version=True` + `skip_local_download=False`:
  - Downloads once to first node, then copies to others (local optimization)

### Unified Install/Uninstall Pattern
Both `do_install()` and `do_uninstall()` now follow the same execution pattern for consistency:

**Pattern:**
1. Iterate over `install_tasks` dict keys (servers)
2. Get node_helper via `install_utils.get_node_helper(server.ip)`
3. Create queue and populate tasks based on filtered install_tasks per server
4. Spawn worker thread per node
5. Wait on `install_utils.NodeHelpers` for completion

**Benefits:**
- Single source of truth: filtered `install_tasks` dict created once
- Consistent per-node task filtering applied before both install and uninstall phases
- No risk of uninstalling nodes that have the correct version
- Simplified debugging and maintenance

do_uninstall() implementation:
```python
def do_uninstall(params, install_tasks):
    for server, tasks in install_tasks.items():
        node_helper = install_utils.get_node_helper(server.ip)
        q = queue.Queue()
        for task_to_add in tasks:
            if task_to_add == 'uninstall':
                q.put(task_to_add)  # Only adds 'uninstall' if present in filtered tasks
        t = threading.Thread(target=node_installer, args=(node_helper, q))
        t.daemon = True
        t.start()
        node_helper.queue = q
        node_helper.thread = t
    # Wait loop...
```

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
- Ensures nodes with matching version are never uninstalled (even when other nodes need reinstall)
- Uses consistent pattern for `do_install()` and `do_uninstall()` - both consume filtered `install_tasks` dict
