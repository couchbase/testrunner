# Tool Index for Agents

**Redirect to individual docs for details.**

| Tool | File | Keywords |
|------|------|----------|
| testDispatcher.py | [testDispatcher.md](./testDispatcher.md) | legacy, SDK2, deprecated, centos, P0, dispatcher_legacy |
| testDispatcher_sdk4.py | [dispatcher_sdk4.md](./dispatcher_sdk4.md) | modern, SDK4, transactions, production, VM booking, QE, debian12, deb12, docker |
| new_install.py | [new_install.md](./new_install.md) | install, bootstrap, on-prem, columnar, functional runs |

**Routing Logic:**
- `testDispatcher*` queries → check keywords to choose legacy vs SDK4
- `install*`/`bootstrap*` → new_install.md
- `dispatcher*` → SDK4 (default), use legacy only if explicit