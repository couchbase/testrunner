# AGENTS.md

## Project Overview

Couchbase TestRunner is a Python-based integration test framework for Couchbase Server. It validates 8+ services (KV, Query/N1QL, GSI, FTS, Eventing, Analytics, Backup, XDCR) against on-premises clusters, `cluster_run` sandboxes, and cloud deployments (Capella, AWS, Azure).

## Core Commands

```bash
# Single test with cluster_run (requires ../ns_server/)
./testrunner -i b/resources/dev.ini -t setgettests.MembaseBucket.value_100b

# Suite execution with cluster setup
python3 scripts/start_cluster_and_run_tests.py MAKE <ini_file> <conf_file> <VERBOSE> <DEBUG>

# Service-specific make targets
make e2e-kv-single-node                        # KV smoke (1-node cluster_run)
make test-gsi-integrations-tests PARAMS=...    # GSI (4-node, n1ql+gsi services)
make test-fts                                  # FTS (4-node, fts service)
make test-eventing-sanity-tests                # Eventing (4-node)
make test-viewquery                            # Views (4-node)
make test-xdcr-merge                           # XDCR (4-node)

# Flexible execution
make any-test NODES=<ini_file> TEST=<test>
make any-suite NODES=<ini_file> SUITE=<conf_file>

# Dependencies
pip3 install -r requirements.txt
make init-submodules   # java_sdk_client, lib/capellaAPI, magma_loader/DocLoader, gsi_utils
```

## Repository Layout

| Path | Description |
|------|-------------|
| `pytests/` | Test suites by service: `gsi/` (65), `tuqquery/` (120+), `fts/`, `eventing/`, `xdcr/`, `security/`, `cbas/`, `backup/` |
| `conf/` | 147 test suite configs + 28 service subdirectories |
| `lib/` | Framework core: `membase/api/` (REST), `remote/` (SSH), `couchbase_helper/`, `tasks/`, `Cb_constants/` |
| `scripts/` | Orchestration: `start_cluster_and_run_tests.sh/.py`, `install.py`, `cloud_provision.py`, dispatchers |
| `b/resources/` | 153 cluster INI templates (topology, credentials, services) |
| `testrunner.py` | Main CLI entry point (47KB) |
| `Makefile` | Service-specific make targets |

**Test inheritance:** `pytests/basetestcase.py` -> service base (e.g., `gsi/base_gsi.py`) -> test module

## Development Constraints

- **Python:** 3.10.13
- **All tests are integration tests** requiring a running Couchbase cluster
- **cluster_run** needs `../ns_server/` sibling directory built via `make`
- **Working directory** must be project root for imports (`lib/`, `pytests/` on sys.path)
- **No linting or type checking configured** (no flake8, pylint, mypy, ruff, black)
- **No CI/CD pipeline visible** in the repo (no `.github/`, Jenkinsfile, or `.gitlab-ci.yml`)
- **No pytest.ini or tox.ini**; framework uses custom `testrunner.py` runner based on `pytest`
- Tests create `tmp-<timestamp>/` directories with logs and xunit XML reports (not auto-cleaned)
- INI files define cluster topology; conf files define test suites and parameters

## Validation and Evidence Required

Before completing changes:

1. **Framework changes:** `make e2e-kv-single-node`
2. **Query/GSI changes:** `make test-gsi-integrations-tests`
3. **Service-specific changes:** run the matching make target or conf file
4. Verify `tmp-<timestamp>/` contains xunit XML and test logs
5. Check pass/fail summary in test runner output

**Code quality rules:**
- Follow existing naming patterns in `pytests/` (e.g., `gsi/plasma_basic_ops.py`)
- Reuse base classes and `lib/` helpers; do not reinvent
- Match the coding style of surrounding code

## Security and Sensitive Paths

**Never commit:**
- INI files with real cluster IPs, credentials, or SSH keys (use templates in `b/resources/`)
- API keys, tokens, or cloud credentials
- Test logs containing cluster data

**Sensitive operations:**
- `scripts/install.py` -- installs Couchbase on remote machines via SSH
- `scripts/cloud_provision.py` -- manages cloud infrastructure, requires cloud credentials
- `pytests/security/` -- RBAC, LDAP, SSL, x.509 tests needing special cluster configs

## Unknowns

- No documented CI/CD pipeline; tests appear to be triggered externally
- No linting or formatting standards enforced
- `cluster_run` setup requirements beyond `../ns_server/` are undocumented

## Supporting Context

- `docs/architecture.agents.md` -- module boundaries and runtime flows
- `docs/agent-context/repo-inventory.md` -- languages, packages, key directories
- `docs/agent-context/build-test-matrix.md` -- exact validation commands per service
- `docs/agent-context/domain-glossary.md` -- Couchbase services, test types, terminology
- `docs/agent-context/troubleshooting.md` -- common cluster_run and test execution failures

## Submodule Orchestration Logic

This repository uses git submodules for specialized components. When working with code that spans these submodules, use conditional logic to route work to the appropriate repository.

### Submodule Dependencies

| Submodule | Path | Repository | Purpose |
|-----------|------|------------|---------|
| **java_sdk_client** | `java_sdk_client/` | github.com/couchbaselabs/java_sdk_client | Java SDK client for multi-language testing |
| **capellaAPI** | `lib/capellaAPI/` | github.com/couchbaselabs/CapellaRESTAPIs | Capella (cloud) REST API clients |
| **DocLoader** | `magma_loader/DocLoader/` | github.com/couchbaselabs/DocLoader.git | High-performance document loading for Magma storage |
| **gsi_utils** | `gsi_utils/` | github.com/couchbaselabs/gsi_utils.git | GSI utilities and helper functions |

### Submodule Initialization

Initialize submodules before development:
```bash
make init-submodules
# OR
git submodule init && git submodule update --init --force --remote
```

Update existing submodules to latest:
```bash
make update-submodules
# OR
git submodule update --init --force --remote
```

### Orchestration Logic for Agent Routing

When working with code that spans submodules, use conditional logic to route to the appropriate repository:

```python
import os

def route_to_repository(task):
    """
    Determine which repository should handle the given task
    
    Args:
        task (str): Description of the code work needed
    
    Returns:
        str: Repository/submodule path where work should be done
    """
    task_lower = task.lower()
    
    # Java SDK Client routing
    if any(keyword in task_lower for keyword in [
        'java sdk', 'javaclient', 'java_client', 
        'sdk client java', 'couchbase java sdk',
        'java sdk client', 'multilanguage testing'
    ]):
        return 'java_sdk_client'
    
    # Capella REST API routing
    elif any(keyword in task_lower for keyword in [
        'capella api', 'capellarestapi', 'capella rest',
        'cloud api', 'managed cloud', 'dbaas',
        'lib/capellaapi', 'capella rest client'
    ]):
        return 'lib/capellaAPI'
    
    # DocLoader routing (Magma storage)
    elif any(keyword in task_lower for keyword in [
        'docloader', 'document loader', 'magma loader',
        'magma document loading', 'high-performance loading',
        'doc generator', 'magma_loader/docloader',
        'document generation', 'magma doc loader'
    ]):
        return 'magma_loader/DocLoader'
    
    # GSI Utilities routing
    elif any(keyword in task_lower for keyword in [
        'gsi utilities', 'gsi_utils', 'gsi helper',
        'global secondary index utilities', 'gsi base class',
        'gsi utility functions'
    ]):
        return 'gsi_utils'
    
    # Default to main testrunner repository
    else:
        return 'testrunner'

# Example usage
if __name__ == "__main__":
    while True:
        user_task = input("Enter task description (or 'quit' to exit): ")
        if user_task.lower() in ['quit', 'exit']:
            break
        
        repo_path = route_to_repository(user_task)
        print(f"\nNavigate to: {repo_path}/")
        print(f"\nTo initialize submodules if needed:")
        print(f"  cd /Users/pavan.pb/Workstuff/testrunner")
        print(f"  make init-submodules")
        print(f"  cd {repo_path}")
        print()
```

### Usage Examples

```python
# Example 1: Routing documentation work
task = "Update Java SDK client for new SDK version"
repo = route_to_repository(task)
print(f"Work in: {repo}")  # Output: Work in: java_sdk_client

# Example 2: Routing bug fixes
task = "Fix Capella REST API authentication issue"
repo = route_to_repository(task)
print(f"Work in: {repo}")  # Output: Work in: lib/capellaAPI

# Example 3: Routing feature requests
task = "Add high-performance document loading for Magma"
repo = route_to_repository(task)
print(f"Work in: {repo}")  # Output: Work in: magma_loader/DocLoader
```

### Best Practices for Multi-Repository Work

1. **Identify the scope first**: Determine which repository owns the code before starting work
2. **Navigate to submodule**: Change directory to the appropriate submodule before making changes
3. **Test in context**: Run tests within the submodule's own testing framework when applicable
4. **Document cross-module dependencies**: If changes affect multiple submodules, document the relationships
5. **Keep submodules synchronized**: Always ensure submodules are up-to-date before committing changes

```bash
# Example workflow for Java SDK client work
cd /Users/pavan.pb/Workstuff/testrunner
git submodule update --init --force --remote
cd java_sdk_client
# Make changes to java_sdk_client files
cd ..
git add java_sdk_client
git commit -m "Update Java SDK client for XYZ feature"
```
