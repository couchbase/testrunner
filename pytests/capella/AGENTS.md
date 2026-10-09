# AGENTS.md — Unified (dual-run) functional tests: on-prem + Capella

## Overview

A **unified** (dual-run) test is one test body that runs on both on-prem and Capella. The target
is selected at runtime by `capella_run` (`True` → Capella, absent/`False` → on-prem):

- `basetestcase.py` swaps `BaseTestCase` to `capella_basetestcase.BaseTestCase` when
  `capella_run=True`. That base provisions or reuses a Capella cluster and populates
  `self.servers`.
- Every test gets a lazy `self.target` (a `RunTarget`, `lib/testops/run_target.py`). Tests call
  neutral verbs on it, and it picks the on-prem or Capella implementation.

**Mandate:** new tests that should cover Capella must be unified. Only a curated ~10% of existing
tests are being migrated; the rest stay on-prem-only and untouched. **Do not** create `*_capella`
copies of tests (see the anti-pattern callout below).

Full rationale, diagrams and the worked case study:
[docs/capella/dual_run_test_design.md](../../docs/capella/dual_run_test_design.md).

## The `self.target` API

| Need | Call | On-prem | Capella |
|---|---|---|---|
| Can this target do X? | `self.target.supports(Capability.X)` | all except `PRIVATE_ENDPOINT` | `TOPOLOGY_CONTROL`, `LOG_ACCESS`, `PRIVATE_ENDPOINT`, `QUERY` (+ `NODE_SHELL` if SSH) |
| Add / remove / swap nodes | `self.target.resize_cluster(add=, remove=, services=)` → task, `.result()` | rebalance | scaling API |
| Node to pass to `add=` | `self.target.node_to_add(services=["index"])` | spare INI server `servers[nodes_init]` (or `None`) | placeholder; Capella provisions the node |
| Re-sync after topology change | `self.target.refresh_cluster_nodes()` | no-op | re-reads live nodes; fixes `servers`/`master`/`rest`/`cluster_config` |
| Failover | `self.target.failover_node(nodes, graceful=False)` → task, `.result()` | `async_failover` | raises `UnsupportedOnTarget` → skip |
| N1QL | `self.target.run_query(query=q, server=self.target.query_node())` | query node from services map | SDK over `couchbases://<srv>` (`ssl=no_verify` unless `-p capella_tls_verify=True`); `query_node()` is `None` |
| Node-shell (incl. reading a log file) | `install_packages`, `restart_service`, `reboot`, `read_service_log` — gate on `NODE_SHELL` | SSH | `UnsupportedOnTarget` unless `-p capella_ssh_reachable=True` |
| Collect logs | `self.target.collect_logs()` | cbcollect (`get_cbcollect_info`) | control-plane log collection |

`Capability` and `UnsupportedOnTarget` live in `lib/testops/capabilities.py`. Node-shell, log
and query capabilities come from the `CAPS` declared by the selected backends, so a new access
channel (e.g. SSM) is a new backend, not a change to tests.
`UnsupportedOnTarget` subclasses `unittest.SkipTest`, so an unsupported op that reaches the
backend is recorded as a **skip**, not a failure.

## Adding a unified test

1. **Pick the base class you would use anyway.** Subclass the normal service base (e.g.
   `BaseSecondaryIndexingTests`); it already inherits from the swapped `BaseTestCase`, so
   `self.target` is available. Follow existing naming in the service folder
   (e.g. `pytests/gsi/gsi_file_based_rebalance_new.py`).

2. **Route N1QL through the target in `setUp`** (needed if a topology change can evict a query
   node on Capella):
   ```python
   def setUp(self):
       super().setUp()
       if self.capella_run:
           self.run_cbq_query = self.target.run_query
           self.n1ql_helper.run_cbq_query = self.target.run_query
       self.n1ql_server = self.target.query_node()
   ```
   Pass `server=self.target.query_node()` wherever a query node is needed. Do not call
   `get_nodes_from_services_map(service_type="n1ql")`.

3. **Write the body with neutral verbs:**
   ```python
   from lib.testops.capabilities import Capability

   def test_rebalance_out_indexer(self):
       if self.target.supports(Capability.NODE_SHELL):          # optional step: skip the step only
           self.target.install_packages(["sysstat"])
       self._create_indexes_and_load()                          # platform-neutral
       out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[:1]
       self.target.resize_cluster(remove=out, services=["index"]).result()
       self.target.refresh_cluster_nodes()                      # Capella may evict a different node
       self._validate_indexes_and_scans()                       # REST / SDK / stats only

   def test_rebalance_in_indexer(self):
       node = self.target.node_to_add(services=["index"])
       self.assertIsNotNone(node, "INI needs a spare server at servers[nodes_init]")
       self.target.resize_cluster(add=[node], services=["index"]).result()
       self.target.refresh_cluster_nodes()
   ```
   If the test is meaningless without an op, gate the whole test instead:
   `if not self.target.supports(Capability.NODE_FAILOVER): self.skipTest("no failover on this target")`.

4. **Write the conf.** One conf runs on both targets, because `capella_run` is passed with `-p`,
   not in the conf. Keep params platform-neutral. Add `nodes_init` and `services_init` matching the
   INI, and use `use_magma_loader=True` where the Java SDK client is not available. Example:
   [conf/gsi/gsi_file_based_rebalance_new_capella.conf](../../conf/gsi/gsi_file_based_rebalance_new_capella.conf).
   Never commit an environment-specific `cluster_id`.
   **Capella needs at least 3 data (kv) nodes.** A conf with fewer fails in
   `CapellaBaseTestCase.setUp` with HTTP 422 `ErrTopologyInvalidKVNodesCount` before any test code
   runs. Use `nodes_init=3,services_init=kv:n1ql:index-kv:n1ql:index-kv:n1ql:index`, or 3x
   `kv:n1ql` plus dedicated index nodes.

5. **Run on both targets** (see below). Confirm each test passes or skips cleanly on each target,
   and `grep DUALRUN-FLOW` the log to confirm which path each step took.

## Hard rules for unified test code

1. **Never** instantiate `RemoteMachineShellConnection`, call `diag_eval`, or hardcode ports /
   `http://` in a unified test.
2. Do node/cluster/query work only through `self.target` (table above). Do not call
   `self.cluster.async_rebalance` / `async_failover` directly, and do not call the
   `NodeOps` / `QueryOps` backends directly.
3. **Gate** possibly-unsupported ops with `self.target.supports(cap)`. Never branch on
   `capella_run` to decide *how* to do something; branch on capability. (Reading `capella_run`
   to rebind query helpers in `setUp`, as above, is fine.)
4. **Verify** via REST / SDK / stats that exist on both targets (not via `diag_eval` or node log
   files).
5. Keep params **platform-neutral**; let the Capella base normalize replicas, TLS, topology, and
   bucket creation. In particular, drop `port=11222` from bucket creation.

## The SSH / node-op decision rule

When a test needs a node-level action, resolve it in this order:

- **Path A**: `self.target.supports(Capability.NODE_SHELL)` (SSH reachable) → run it via
  `self.target.<op>`.
- **Path B1**: no SSH, but a **REST/SDK/stats** equivalent works on both → use it.
- **Path B2**: no SSH, but a **control-plane API** equivalent exists (scale / log-collect) → use it
  through `self.target`.
- **Path B3**: none of the above → skip the step or `self.skipTest(reason)` so the gap stays
  visible.

SSH is a **capability**, not a platform fact: some Capella deployments expose node access. Opt in
with `-p capella_ssh_reachable=True`; decide via `supports()`, never via `capella_run`.

## Running a unified test

On-prem: run the conf as usual with a normal INI. Capella: add `capella_run=True` and use
[b/resources/capella/3-nodes-template.ini](../../b/resources/capella/3-nodes-template.ini) with
real `[capella]` credentials (never commit those).

```bash
# on-prem
./testrunner -i <onprem.ini> -c conf/gsi/<suite>.conf

# Capella, reuse an existing cluster
./testrunner -i b/resources/capella/3-nodes-template.ini -c conf/gsi/<suite>.conf \
  -p capella_run=True,cluster_id=<CLUSTER_ID>

# Capella, provision + tear down a fresh cluster
./testrunner -i b/resources/capella/3-nodes-template.ini -c conf/gsi/<suite>.conf \
  -p capella_run=True,force_deploy_cluster=True
```

## Anti-pattern callout

Do **not** fork a test into a `*_capella` variant. `pytests/gsi/gsi_file_based_rebalance.py` has
`test_gsi_rebalance_indexer_node_capella` as a hand-forked copy of
`test_gsi_rebalance_out_indexer_node`. The unified version (`FileBasedRebalanceNew`) keeps one body
and moves the `capella_run` branch behind `self.target.resize_cluster`.

## Migration checklist (curated ~10% existing subset)

- [ ] Delete any `*_capella` fork; keep one test body.
- [ ] Replace inline `if self.capella_run: capella_api...` with
      `self.target.resize_cluster(...).result()` + `self.target.refresh_cluster_nodes()`.
- [ ] Replace `self.cluster.async_failover` with `self.target.failover_node`.
- [ ] Route node-shell calls through `self.target`; gate with `supports()`.
- [ ] Route N1QL through `self.target.run_query` / `query_node()`.
- [ ] Remove hardcoded ports / `http://`; drop `port=11222` from bucket creation.
- [ ] Replace fixed sleeps with target-aware waits.
- [ ] Run on-prem (regression) **and** Capella; confirm clean pass or clean skips.

## Temporary `DUALRUN-FLOW` logging

`RunTarget`, its backends and the reference tests log dispatch decisions with a `DUALRUN-FLOW:`
prefix. **TODO:** remove this logging once all components have tested the dual-run flow.

## Pointers

- Design doc: [docs/capella/dual_run_test_design.md](../../docs/capella/dual_run_test_design.md)
- Facade: `lib/testops/run_target.py`
- Capabilities: `lib/testops/capabilities.py`
- Node-shell backends: `lib/testops/node_ops.py`
- Query backends: `lib/testops/query_ops.py`
- Reference tests: `pytests/gsi/gsi_file_based_rebalance_new.py`,
  `pytests/gsi/10k_indexes/10k_gsi_indexes_capella.py`
- Capella base: [capella_basetestcase.py](../capella_basetestcase.py)
- Capella API wrapper: [lib/capella/utils.py](../../lib/capella/utils.py)
- Capella REST overrides: [lib/membase/api/capella_rest_client.py](../../lib/membase/api/capella_rest_client.py)
