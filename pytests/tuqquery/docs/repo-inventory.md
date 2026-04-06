# Repo Inventory - N1QL Query Test Suite (tuqquery)

## Summary

**Profile:** Python-based integration test suite for Couchbase Query Service (N1QL/SQL++), located at `pytests/tuqquery/`. Contains 125+ test files (~88k lines of test code), 185+ configuration files, covering comprehensive Query Service functionality including vector search, window functions, UDF, collections, RBAC, and FTS integration. Built on custom pytest-based testrunner framework with Couchbase Python SDK for query execution.

**Scope:** Integration testing requiring running Couchbase cluster with Query Service enabled. Tests validate query execution, index operations, DML/DDL, advanced Query features, and integration with KV, GSI, FTS, Analytics, and Eventing services.

## Evidence

### Languages and Frameworks

- **Primary Language:** Python 3.10.13
- **Test Framework:** pytest (via custom `testrunner.py` runner)
- **Main Libraries:**
  - `couchbase==4.4.0` - Couchbase Python SDK for query execution
  - `deepdiff==5.0.2` - JSON comparison for result verification
  - `numpy==1.26.4` - Vector operations for vector search
  - `faiss-cpu==1.7.4` - ANN (Approximate Nearest Neighbor) search
  - `boto3==1.40.64` - AWS S3 integration for dataset downloads
  - `Faker==37.11.0` - Fake data generation

### Test Frameworks and Entry Points

**Test Entry Points:**
```
Main test runner: testrunner.py (project root)
Configuration-driven: conf/tuq/*.conf (185+ files)
Single test: python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests
Test suite: python testrunner.py -c conf/tuq/meta.conf
With parameters: python testrunner.py -t <test> -p param=value
```

**Test Base Classes:**
- `QueryTests` (tuq.py) → `BaseTestCase` → `unittest.TestCase`
- `QuerySanityTests` (tuq_sanity.py) → `QueryTests`
- Feature-specific classes extend `QueryTests` or `QuerySanityTests`

**Configuration System:**
- INI-style format in `conf/tuq/*.conf`
- Test parameters: `skip_index`, `doc-per-day`, `covering_index`, `GROUP`, `distance`, `nprobes`
- Priority levels: P0 (critical), P1 (high), P2 (medium), P3 (low)
- Feature groups: CE, NON_CE, LIKE, DATE, SCALAR, GROUP

### Important Top-Level Directories

```
pytests/tuqquery/               # 125+ test files
├── tuq.py                      # Base test class (QueryTests)
├── tuq_sanity.py               # Sanity tests (QuerySanityTests)
├── tuq_vectorsearch.py         # Vector search comprehensive suite
├── n1ql_window_functions.py    # Window functions
├── tuq_UDF_N1QL.py             # User Defined Functions
├── n1ql_collections_ddl.py     # Collections DDL
├── n1ql_fts_integration.py     # FTS integration
├── tuq_join.py                 # JOIN operations
├── tuq_dml.py                  # DML operations
├── n1ql_rbac_2.py              # Security/RBAC
├── serverless/                 # Serverless-specific tests
├── docs/                      # Documentation
│   ├── architecture.agents.md # AI-optimized architecture
│   └── repo-inventory.md       # This file
├── AGENTS.md                  # Agent documentation
├── README_Query_Tests.md       # Comprehensive test guide
└── README_Query_Analysis.md    # Coverage analysis

conf/tuq/                       # 185+ configuration files
├── meta.conf                  # Main sanity tests
├── py-tuq.conf                # Extended core query
├── py-tuq-gsi.conf            # GSI index tests
├── py-tuq-joins.conf          # JOIN operations
├── py-tuq-udf-n1ql.conf       # UDF tests
├── py-tuq-vector-knn.conf     # KNN search
├── py-tuq-vector-ann.conf     # ANN search
├── py-n1ql-collections_ddl.conf # Collections DDL
├── py-n1ql-rbac.conf          # RBAC tests
└── ... (175+ more config files)

lib/                           # Helper libraries
├── couchbase_helper/
│   ├── tuq_helper.py         # N1QL query execution (N1QLHelper)
│   ├── tuq_generators.py     # Query generation (TuqGenerators)
│   └── documentgenerator.py  # Document generation
├── collection/
│   └── collections_n1ql_client.py # Collections via N1QL
├── vector/
│   └── vector.py             # Vector utilities (UtilVector)
└── membase/api/
    ├── rest_client.py        # REST API client
    └── exception.py          # CBQError exception

scripts/                       # Orchestration scripts
├── start_cluster_and_run_tests.py  # Cluster setup
├── AiQG/                      # AI Query Generator
│   └── AiQG.py               # AI-powered query generation
```

### CI, Devcontainer, Hook, and Lint Evidence

**CI/CD:**
- **Status:** No visible CI/CD pipeline configuration
- **Evidence:** No `.github/`, `.gitlab-ci.yml`, `Jenkinsfile`, or similar files found
- **Assumption:** Tests triggered externally or manually

**Dev Containers:**
- **Status:** No devcontainer configuration found
- **Evidence:** No `.devcontainer/` or `Dockerfile` for development environment

**Git Hooks:**
- **Status:** No git hooks configured
- **Evidence:** No `.git/hooks/` or pre-commit configuration files

**Linting/Type Checking:**
- **Status:** No linting or type checking configured
- **Evidence:**
  - No `setup.cfg`, `pyproject.toml`, or `.flake8`
  - No `.pylintrc`, `.mypy.ini`, or `ruff.toml`
  - No `black`, `flake8`, `pylint`, `mypy`, `ruff` in requirements.txt
- **Impact:** No enforced code quality standards

**Dependency Management:**
- **File:** `requirements.txt` in project root (113 packages)
- **Key Dependencies:**
  - couchbase==4.4.0
  - deepdiff==5.0.2
  - numpy==1.26.4
  - faiss-cpu==1.7.4
  - boto3==1.40.64
  - Faker==37.11.0
  - Flask==3.1.2
  - azure-* (multiple Azure SDK packages)

### Test Distribution and Categories

**File Count Analysis:**
- Total test files: 125+
- Total configuration files: 185+
- Estimated test classes: ~120
- Estimated test methods: ~2,500+
- Total test code lines: ~88k lines

**Major Test Categories:**
1. **Core Query Operations** (15 files) - Coverage: 95%, Maturity: High
2. **Indexing** (8 files) - Coverage: 90%, Maturity: High
3. **DML Operations** (6 files) - Coverage: 85%, Maturity: High
4. **DDL Operations** (4 files) - Coverage: 80%, Maturity: Medium
5. **JOIN Operations** (3 files) - Coverage: 75%, Maturity: Medium
6. **Window Functions** (2 files) - Coverage: 70%, Maturity: Medium
7. **UDF** (3 files) - Coverage: 65%, Maturity: Medium
8. **Vector Search** (1 file, 8 configs) - Coverage: 85%, Maturity: High
9. **Collections & Scopes** (5 files) - Coverage: 85%, Maturity: High
10. **Security (RBAC)** (3 files) - Coverage: 75%, Maturity: Medium
11. **Performance & Optimization** (4 files) - Coverage: 60%, Maturity: Low
12. **Integration Tests** (3 files) - Coverage: 70%, Maturity: Medium
13. **Error Handling** (5 files) - Coverage: 80%, Maturity: Medium
14. **Cluster Operations** (3 files) - Coverage: 70%, Maturity: Medium
15. **Function Tests** (4 files) - Coverage: 85%, Maturity: High
16. **Serverless** (4 files) - Coverage: 50%, Maturity: Low

### Key External Dependencies

**Couchbase Services:**
- Query Service (N1QL) - Primary test target (port 8093)
- GSI Service - Index operations
- KV Service - Document operations
- FTS Service - Full-text search integration
- Analytics Service (CBAS) - Query-analytics integration
- Eventing Service - Limited integration

**External Data Sources:**
- Sample datasets: travel-sample, beer-sample
- Vector datasets: SIFT (dense vectors), Sparse vectors
- AI Query Generator: Requires OpenAI API key

### Documentation Status

**Existing Documentation:**
- ✅ `AGENTS.md` - Agent documentation for tuqquery
- ✅ `README_Query_Tests.md` - Comprehensive test suite guide (29k bytes)
- ✅ `README_Query_Analysis.md` - Coverage analysis (24k bytes)
- ✅ `docs/architecture.agents.md` - AI-optimized architecture
- ✅ Main project `AGENTS.md` at project root

**Missing Documentation:**
- ❌ `docs/agent-context/troubleshooting.md` - Common failure patterns
- ❌ aiqg/ documentation for AI Query Generator
- ❌ Vector dataset loading documentation
- ❌ Test data generator configuration guide

## Gaps

### Missing Operational Context

1. **CI/CD Configuration:**
   - No visible CI/CD pipeline for automated test execution
   - Unknown external test triggering mechanism
   - No automated test result aggregation

2. **Linting and Code Quality:**
   - No linting configured (flake8, pylint, mypy, ruff, black)
   - No type checking enforced
   - No code formatting standards
   - Risk of inconsistent code quality

3. **Development Environment:**
   - No devcontainer for reproducible development setup
   - No local development guide
   - No environment setup documentation

4. **Test Data Management:**
   - Vector dataset (SIFT, sparse) locations not documented
   - Dataset download mechanisms unclear
   - Test data generator configuration not cataloged
   - Sample dataset sizes and characteristics unknown

5. **API and Integration Documentation:**
   - External service integration details not documented
   - OpenAI API key storage for AiQG not specified
   - Cluster configuration requirements not enumerated

### Missing Documents Needed for Better Agent Operation

1. `docs/architecture.humans.md`:
   - Narrative overview for human understanding
   - Major components and responsibilities
   - How test workflows map to architecture
   - Tradeoffs, constraints, and known weak spots

2. `docs/agent-context/build-test-matrix.md`:
   - Exact commands for running each test category
   - Validation commands for服务质量
   - Test execution time estimates
   - Resource requirements per test category

3. `docs/agent-context/domain-glossary.md`:
   - Couchbase services terminology (N1QL, GSI, FTS, CBAS)
   - Test type definitions (P0, P1, P2, P3)
   - Naming conventions (tuq vs n1ql prefixes)
   - Configuration parameter definitions

4. `docs/agent-context/troubleshooting.md`:
   - Common cluster_run setup failures
   - Query execution timeout causes and solutions
   - Index build failure patterns
   - Vector search specific issues

5. `aiqg/README.md`:
   - AI Query Generator setup instructions
   - OpenAI API key configuration
   - Prompt file format and examples
   - Expected query patterns and variations

6. `dataset/README.md`:
   - Vector dataset (SIFT, sparse) loading procedures
   - Dataset file locations and sources
   - Dataset size and format specifications
   - Reduction to test subset procedures

### Unknowns Requiring Maintainer Input

1. **Test Execution:**
   - How are tests triggered in production CI/CD?
   - What is the test execution frequency and schedule?
   - How are test results aggregated and reported?
   - What is the test failure escalation process?

2. **External Dependencies:**
   - Where are vector datasets (SIFT, sparse) stored?
   - How are datasets downloaded and verified?
   - What is the OpenAI API key storage and rotation policy for AiQG?
   - Are there external service rate limits or quotas?

3. **Cluster Configuration:**
   - What are the minimum cluster resources required for test execution?
   - Are there specialized cluster configurations for vector search tests?
   - How are test environments provisioned and recycled?
   - What is the cluster_run setup procedure beyond `../ns_server/`?

4. **Test Maintenance:**
   - Who owns the tuqquery test suite?
   - What is the test review and approval process?
   - How are test priority levels (P0-P3) assigned and reviewed?
   - What is the test coverage target and review cadence?

5. **Performance and Scalability:**
   - What are the test execution time baselines?
   - Are there performance regression thresholds?
   - How are large dataset tests executed (vector search, joins)?
   - What is the test parallelization strategy?

6. **Security and Compliance:**
   - How are secrets (API keys, credentials) managed?
   - Are there audit logging requirements for test execution?
   - What is the data retention policy for test logs?
   - Are there GDPR/data privacy considerations for test data?

## Suggested Next Commands

### For Exploratory Analysis

```bash
# Count test files by category
cd testrunner/pytests/tuqquery
ls -1 *.py | wc -l

# Count configuration files
ls -1 ../../conf/tuq/*.conf | wc -l

# Analyze test file sizes
wc -l *.py | sort -n | tail -10

# Find tests with specific patterns
grep -r "class.*Tests" . --include="*.py" | head -20

# Check for TODO/FIXME comments
grep -r "TODO\|FIXME\|XXX" . --include="*.py"

# Find all test method names
grep -r "def test_" . --include="*.py" | wc -l
```

### For Validation

```bash
# Run sanity tests
cd testrunner
python testrunner.py -c conf/tuq/meta.conf

# Run extended sanity
python testrunner.py -c conf/tuq/py-tuq-ext-sanity.conf

# Run vector search tests
python testrunner.py -c conf/tuq/py-tuq-vector-knn.conf

# Check Python syntax
python3 -m py_compile pytests/tuqquery/tuq.py

# Verify imports
python3 -c "from pytests.tuqquery.tuq import QueryTests; print('Import OK')"
```

### For Documentation Generation

```bash
# List all configuration files with descriptions
ls -1lh ../../conf/tuq/*.conf | awk '{print $9, $5}'

# Generate test category summary
find . -name "*.py" -exec basename {} \; | sort | head -30

# Create dependency graph visualization
grep -h "^from\|^import" *.py | sort -u > dependencies.txt

# Extract test class hierarchy
grep -h "class.*Tests" *.py | sed 's/class //' | sed 's/).*//' > test_classes.txt
```

### For Maintainer Inquiry

```bash
# Find contact information
grep -r "@author\|MAINTAINER\|Contact" . --include="*.md" --include="*.py"

# Check for issue tracker references
grep -r "issue\|bug\|MB-" README*.md --include="*.md" | head -10

# Find recent changes
git log --oneline --since="30 days ago" -- pytests/tuqquery/

# Check for deprecated tests
grep -r "@deprecated\|DEPRECATED" . --include="*.py"
```

### For Setup Verification

```bash
# Check Python version
python3 --version

# Verify critical dependencies
pip3 show couchbase deepdiff numpy faiss-cpu

# Check testrunner availability
python3 testrunner.py --help 2>/dev/null || echo "testrunner not executable"

# Verify git submodules
git submodule status

# Check for required sample datasets
ls -lh ~/.couchbase/*/travel-sample 2>/dev/null || echo "travel-sample not found locally"
```

## Profile Summary

**Repository:** Couchbase Query Service Test Suite (tuqquery)
**Language:** Python 3.10.13
**Framework:** pytest (via custom testrunner.py)
**Scale:** 125+ test files, 185+ config files, ~88k lines of test code
**Complexity:** High - integration testing requiring running Couchbase cluster
**Dependencies:** Couchbase SDK, deepdiff, numpy, faiss, boto3
**Operational Sophistication:** Medium - configuration-driven but lacks CI/CD and linting
**Documentation:** Good for tests, gaps in operational context
**Agent Readiness:** High - AGENTS.md and architecture.agents.md present, humans.md pending
**Maintainer Engagement Required:** Unknowns on CI/CD, external dependencies, and cluster setup
