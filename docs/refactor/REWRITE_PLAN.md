# TestRunner Framework Rewrite — Zero Test Breakage Strategy

## Guiding Principle

> **Tests are the customers.** Every test module in `pytests/` is a contract. The rewrite succeeds only if `./testrunner -i <ini> -t <test>` produces identical pass/fail outcomes before and after, for every test in every `conf/` file.

---

## Phase 0: Establish a Safety Net

Before touching any code:

1. **Catalog the public API surface** — Every class, method, and attribute in `lib/` that tests import or call is a frozen contract. Generate this automatically:
   ```bash
   grep -rh "^from lib\." pytests/ | sort -u > docs/refactor/lib_imports.txt
   grep -rh "^import lib\." pytests/ | sort -u >> docs/refactor/lib_imports.txt
   ```

2. **Record a golden baseline** — Run the full suite (or at minimum, all `conf/*.conf` files) and store xunit XML. Future refactoring PRs must produce identical pass/fail.

3. **Add a compatibility test gate** — A simple script that diffs xunit XML from before/after and fails the build if any test flips status.

---

## Phase 1: Introduce an Adapter Layer (Facade Pattern)

**The key insight:** Don't modify `lib/` in place. Build a new `lib_v2/` alongside it, then make the old `lib/` modules become thin facades that delegate to the new internals.

```
lib/                          ← Keep all existing module paths & class names
  membase/api/rest_client.py  ← Becomes a facade: imports from lib_v2, re-exports same interface
  remote/remote_util.py       ← Same
  ...

lib_v2/                       ← New, clean implementation
  cluster/
  rest/
  ssh/
  ...
```

Tests continue to `from lib.membase.api.rest_client import RestConnection` — nothing changes for them.

---

## Phase 2: Modular Rewrite (Inside-Out)

Rewrite one subsystem at a time behind the facade:

### Priority Order (risk-sorted, low→high):

| # | Subsystem | Current Location | Why First |
|---|-----------|-----------------|-----------|
| 1 | **Constants/Config** | `lib/Cb_constants/`, `TestInput.py` | Pure data, no side effects |
| 2 | **Document Generators** | `lib/couchbase_helper/documentgenerator.py` | Stateless, easy to validate |
| 3 | **REST Client** | `lib/membase/api/rest_client.py` (huge) | Most duplicated code lives here |
| 4 | **SSH/Remote** | `lib/remote/remote_util.py` | Second most duplicated |
| 5 | **Task Framework** | `lib/tasks/` | Async plumbing, complex but isolated |
| 6 | **Cluster Orchestration** | `lib/couchbase_helper/cluster.py` | Ties everything together |
| 7 | **Base Test Case** | `pytests/basetestcase.py` | Highest risk — touch last |

---

## Phase 3: Design Principles for `lib_v2/`

### 3.1 Single REST Client, Service-Scoped Mixins

**Problem:** Current `rest_client.py` is 10K+ lines — KV, N1QL, FTS, CBAS, Eventing, Backup all in one class.

**Solution:**
```python
# lib_v2/rest/client.py
class RestClient:
    """Thin HTTP wrapper — auth, retry, TLS, timeout."""

# lib_v2/rest/kv.py
class KVServiceAPI(RestClient):
    """Bucket CRUD, stats, warmup."""

# lib_v2/rest/query.py
class QueryServiceAPI(RestClient):
    """N1QL execution, prepared statements, monitoring."""

# lib_v2/rest/fts.py
class FTSServiceAPI(RestClient):
    """Index CRUD, query, pause/resume."""
```

The old `RestConnection` facade delegates to the appropriate service API.

### 3.2 SSH Command Builder (Replace String Concatenation)

**Problem:** `remote_util.py` builds shell commands via string concatenation — fragile, duplicated, OS-branching everywhere.

**Solution:**
```python
# lib_v2/ssh/command.py
class Command:
    def __init__(self, binary, *args):
        ...
    def with_flag(self, flag, value=None): ...
    def as_user(self, user): ...
    def pipe(self, other): ...
    def build(self, os_type) -> str: ...
```

### 3.3 Typed Cluster Model

**Problem:** Cluster state is scattered across dicts, ad-hoc attributes, and REST responses parsed inline.

**Solution:**
```python
# lib_v2/cluster/model.py
@dataclass
class Node:
    ip: str
    port: int
    services: list[str]
    version: str

@dataclass
class Cluster:
    nodes: list[Node]
    buckets: list[Bucket]
    
    def kv_nodes(self) -> list[Node]: ...
    def fts_nodes(self) -> list[Node]: ...
```

### 3.4 Retry/Polling as a Utility, Not Inline Loops

**Problem:** Every file has its own `for i in range(N): sleep(X); try: ...` pattern.

**Solution:**
```python
# lib_v2/util/retry.py
def poll_until(predicate, timeout=120, interval=5, msg=""):
    """Retry until predicate() returns truthy or timeout expires."""

def retry_on_exception(exceptions, max_retries=3, backoff=2):
    """Decorator for retryable operations."""
```

### 3.5 Logging Standardization

**Problem:** Mix of `print()`, `self.log.info()`, and `logging.getLogger()` with inconsistent formats.

**Solution:** Single `lib_v2/util/log.py` that configures structured logging once; old code keeps working via standard `logging` module.

---

## Phase 4: Facade Migration Pattern (Per-Subsystem)

For each subsystem:

```
Step 1: Write new implementation in lib_v2/ with full unit tests
Step 2: Update facade in lib/ to delegate to lib_v2/
Step 3: Run full integration suite — must match golden baseline
Step 4: Mark old inline code as dead (but don't delete yet)
Step 5: After N successful CI cycles, remove dead code
```

Example for `RestConnection.create_bucket()`:

```python
# lib/membase/api/rest_client.py (facade)
class RestConnection:
    def __init__(self, server):
        self._kv_api = KVServiceAPI(server.ip, server.port, ...)
        # ... other service APIs ...
    
    def create_bucket(self, bucket_params):
        # Delegate to new implementation
        return self._kv_api.create_bucket(bucket_params)
    
    # ... hundreds of other methods stay, each gradually delegating ...
```

---

## Phase 5: BaseTestCase Decomposition

`basetestcase.py` (~3000+ lines) is the riskiest. Approach:

1. **Extract mixins** without changing the class hierarchy:
   ```python
   class BucketMixin:       # _bucket_creation, _bucket_cleanup
   class ClusterMixin:      # cluster_init, rebalance helpers
   class ValidationMixin:   # data verification helpers
   
   class BaseTestCase(BucketMixin, ClusterMixin, ValidationMixin, unittest.TestCase):
       pass  # Tests see no difference
   ```

2. **Move setUp orchestration** into a `TestFixture` builder:
   ```python
   class TestFixture:
       def __init__(self, ini_config): ...
       def with_buckets(self, specs): ...
       def with_services(self, services): ...
       def build(self) -> Cluster: ...
   ```
   
   `BaseTestCase.setUp()` internally uses `TestFixture` but the interface stays identical.

---

## Phase 6: Kill Duplicate Code Gradually

| Pattern | Occurrences (est.) | Solution |
|---------|-------------------|----------|
| Inline REST calls with `urllib`/`requests` | 50+ | Centralize in `RestClient` |
| SSH command string building | 100+ | `Command` builder |
| Sleep-poll loops | 200+ | `poll_until()` utility |
| JSON response parsing with nested `try/except` | 100+ | Response model classes |
| Duplicate bucket creation logic | 30+ | Single `BucketManager` |
| OS-type branching (`if linux: ... elif windows: ...`) | 80+ | Strategy pattern per OS |

---

## What NOT to Change

| Keep As-Is | Reason |
|------------|--------|
| All `pytests/**/*.py` files | They are the customers |
| All `conf/**/*.conf` files | Test suite definitions |
| `testrunner.py` CLI interface | External tooling depends on it |
| `sys.path` structure (`lib/`, `pytests/` on path) | Every import depends on it |
| INI file format | Cluster configs everywhere |
| Method signatures in `lib/` public API | Tests call them directly |

---

## Tooling to Add

| Tool | Purpose |
|------|---------|
| **`pytest`** (internal, not replacing `testrunner.py`) | Unit-test `lib_v2/` in isolation |
| **`ruff`** (on `lib_v2/` only) | Enforce code quality on new code |
| **Type hints** (on `lib_v2/` only) | Catch API contract violations |
| **API diff checker** | CI step that ensures `lib/` facade still exports all symbols tests use |

---

## Timeline Estimate

| Phase | Duration | Risk |
|-------|----------|------|
| 0 — Safety net | 1 week | Low |
| 1 — Adapter/facade skeleton | 2 weeks | Low |
| 2a — Constants + DocGen rewrite | 1 week | Low |
| 2b — REST Client rewrite | 4-6 weeks | Medium |
| 2c — SSH/Remote rewrite | 3-4 weeks | Medium |
| 2d — Task framework | 2-3 weeks | Medium |
| 2e — Cluster orchestration | 2-3 weeks | Medium-High |
| 5 — BaseTestCase decomposition | 3-4 weeks | High |

**Total: ~4-5 months** of incremental, shippable work with zero test breakage at any commit.

---

## TL;DR

1. **Freeze the API surface** — catalog every import tests use
2. **Build `lib_v2/` alongside `lib/`** — clean, typed, tested
3. **Turn `lib/` into thin facades** — delegate to `lib_v2/` one method at a time
4. **Validate every step** against a golden xunit baseline
5. **Never touch `pytests/`** — they are sacred

This gives you a modern, maintainable framework while every single test continues to pass throughout the migration.

