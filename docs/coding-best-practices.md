# TestRunner Coding Best Practices

This document captures common best practices derived from code review comments across the Couchbase TestRunner repository.

## Test Method Naming

### Use `test_` Prefix
All test methods must start with `test_` prefix to differentiate from helper methods and ensure proper test discovery.

**Bad:**
```python
def rebalance_out_during_continuous_mutations(self):
    # Test logic
```

**Good:**
```python
def test_rebalance_out_during_continuous_mutations(self):
    """
    Test rebalance behavior during continuous mutations
    """
    # Test logic
```

### Use Descriptive Names
Test method names should clearly indicate what functionality is being tested.

## Code Organization

### Move Imports to Top
All imports should be at the top of the file before any class definitions.

**Bad:**
```python
def test_something(self):
    import os
    this_file = os.path.abspath(__file__)
```

**Good:**
```python
import os
# ... other imports

class SomeTest:
    def test_something(self):
        this_file = os.path.abspath(__file__)
```

### Move Common Methods to Top
Place common helper methods and utilities at the top of the file, before test methods.

### Docstring Placement
Docstrings should be placed immediately after the function/method definition line.

**Bad:**
```python
def test_xyz(self):
    index_name = 'idx'
    dimension = 384
    """
    Test XYZ functionality
    """
```

**Good:**
```python
def test_xyz(self):
    """
    Test XYZ functionality
    
    Validates that...
    """
    index_name = 'idx'
    dimension = 384
```

## Error Handling and Exception Management

### Use Meaningful Exception Variables
Always catch exceptions with a meaningful variable name for logging.

**Bad:**
```python
except Exception:
    self.log.error("Something failed")
```

**Good:**
```python
except Exception as e:
    self.log.error(f"Operation failed: {e}")
    self.fail(f"Failed due to: {e}")
```

### Log Exceptions for Debugging
Always include the exception details in log messages to aid debugging.

**Bad:**
```python
except Exception:
    self.fail("Failed to create index")
```

**Good:**
```python
except Exception as e:
    self.log.error(f"Failed to create index: {e}")
    self.fail(f"Failed to create index: {e}")
```

### Use `self.fail()` Appropriately
Use `self.fail()` with descriptive error messages when tests encounter critical errors.

**Bad:**
```python
if critical_error:
    pass  # Continue anyway
```

**Good:**
```python
if critical_error:
    self.fail(f"Critical error encountered: {error_msg}")
```

## Logging Practices

### Use `self.log` Instead of `print()`
All logging should use the framework's logging capabilities, not print statements.

**Bad:**
```python
print(f"Query result: {result}")
```

**Good:**
```python
self.log.info(f"Query result: {result}")
```

### Add Context to Log Messages
Include relevant context in log messages to make them more useful.

**Bad:**
```python
self.log.info("Index created")
```

**Good:**
```python
self.log.info(f"Index {index_name} created successfully on {node.ip}")
```

### Don't Log Sensitive Information
Avoid logging credentials, tokens, or other sensitive data, even if they'll be masked.

**Bad:**
```python
self.log.info(f"OKTA_TOKEN: {self.okta_token}")
```

**Good:**
```python
# Store in secrets for masking in logs
self.okta_token = os.getenv("OKTA_TOKEN")
# Don't log the actual token value
```

## Code Cleanup

### Remove Commented-Out Code
Delete commented-out code before submitting. If code is temporarily disabled, add a TODO comment with justification.

**Bad:**
```python
# self.load_until_index_dgm(...)
```

**Good:**
```python
# TODO: Re-enable DGM testing after MB-XXXXX is fixed
# self.load_until_index_dgm(...)
```

### Remove Debugging Statements
Remove debugging print statements, unnecessary sleeps, and temporary fixes.

**Bad:**
```python
self.log.info(f"DEBUG: {some_var}")  # Remove before committing
task.result()  # Unused result
```

**Good:**
```python
# No debugging statements in final commit
result = task.result()
self.verify_result(result)
```

### Remove Extra Spaces
Clean up trailing whitespace and extra blank lines.

**Bad:**
```python
command = "cmd"    

```

**Good:**
```python
command = "cmd"
```

## Configuration Management

### Use Input Parameters with Defaults
Always use input parameters with sensible defaults.

**Bad:**
```python
self.timeout = 120  # Hardcoded
```

**Good:**
```python
self.timeout = self.input.param("timeout", 120)  # Configurable
```

### Use Global Variables for Paths/Constants
Define paths and constants that may change as global variables at the top of the file.

**Bad:**
```python
find_rev_cmd = f"find {kvstore_dir} -type d -name 'rev-*' 2>/dev/null"
# ... later ...
find_rev_cmd = f"find {another_path} -type d -name 'rev-*' 2>/dev/null"
```

**Good:**
```python
REV_DIR_PATTERN = "rev-*"
# ... use everywhere ...
find_rev_cmd = f"find {path} -type d -name '{REV_DIR_PATTERN}' 2>/dev/null"
```

### Document Configuration Options
Add comments explaining the purpose of configuration options, especially for complex parameters.

```python
# combine_tests: Parameter to run multiple test scenarios in a single test
# Reduces cluster setup/teardown overhead for related tests
self.combine_tests = self.input.param("combine_tests", False)
```

## Test Design

### Group Similar Tests
Combine similar test scenarios into parameterized tests to reduce duplication.

**Bad:**
```python
def test_fts_with_counter(self):
    # Test FTS + counter

def test_fts_with_crc64(self):
    # Test FTS + crc64 (similar code)

def test_fts_with_base64(self):
    # Test FTS + base64 (similar code)
```

**Good:**
```python
def test_fts_with_helper_functions(self):
    """
    Test FTS with various helper functions (counter, crc64, base64)
    Uses handler_code param to switch implementations
    """
    handler_code = self.handler_code_param
    # Single test with parameterized handler
```

### Avoid Test Duplication
Use test parameters and configuration files instead of duplicating test code.

### Use Minimum Required Cluster Configuration
Only use the minimum nodes and services needed for the test.

**Bad:**
```python
# Using 4 nodes when 1 is sufficient
test_vector_index_trainlist_retry,nodes_init=4,services_init=kv-kv-index:n1ql-index
```

**Good:**
```python
# Using 1 node with required services
test_vector_index_trainlist_retry,nodes_init=1,services_init=kv:n1ql-index
```

## Code Comments and Documentation

### Add Comments for Magic Values
Explain what hardcoded values represent.

**Bad:**
```python
self.bhive_sample_vector = [3.0, 9.0, 17.0, ...]  # What dataset is this?
```

**Good:**
```python
# Sample vector for Cars dataset (384-dim sentence-transformers embedding)
self.bhive_sample_vector = [3.0, 9.0, 17.0, ...]
```

### Document Complex Methods
Add docstrings for methods that perform complex operations.

```python
def check_storage_directory_cleaned_up(self, threshold_gb=0.025):
    """
    Verifies indexer storage directory cleanup after index operations
    
    Args:
        threshold_gb (float): Maximum allowed storage space in GB 
                            (default: 0.025 GB or ~25MB for indexer)
    
    Raises:
        Exception: If storage exceeds threshold, indicating incomplete cleanup
    """
    # Implementation
```

### Add TODO Comments for Temporary Workarounds
If you add temporary fixes, document them with TODO comments.

```python
# TODO: Remove workaround after MB-68870 is fixed
# Temporary skip for IPv6 port binding validation
if isinstance(processes, list) and "cbcontbk" in processes[0]:
    self.log.info("Skipping validation due to known issue")
```

## Security Considerations

### Credential Management
- Use environment variables for credentials
- Don't log credential values
- Use the framework's credential masking capabilities

**Bad:**
```python
password = "password123"  # Hardcoded
self.log.info(f"Connecting with {username}:{password}")
```

**Good:**
```python
password = os.getenv("CB_PASSWORD")
# Never log the password
self.log.info(f"Connecting with user: {username}")
```

## Dependency Management

### Keep requirements.txt Updated
Ensure all required packages and their versions are specified.

**Bad:**
```python
# Missing required packages
# Outdated versions
```

**Good:**
```python
sentence-transformers==5.1.2  # Pinned version
torch==2.4.0  # Compatible CUDA version included
```

### Avoid Adding Unnecessary Dependencies
Only add packages that are actually needed for the test functionality.

**Bad:**
```python
# Adding aiosignal when no async operations
aiosignal==1.3.1
```

## Resource Cleanup

### Use Conditional Resource Usage
Make resource-intensive operations conditional.

**Bad:**
```python
# Always run DGM testing
time.sleep(120)
self.load_until_index_dgm(resident_ratio=index_resident_ratio)
```

**Good:**
```python
if self.dgm:
    self.log.info(f"Target index RR is {index_resident_ratio}")
    time.sleep(120)
    self.load_until_index_dgm(resident_ratio=index_resident_ratio)
```

### Close Connections and Resources
Always close SDK connections and clean up resources.

**Bad:**
```python
result = client.default_collection.get(key=key)
# Connection never closed
```

**Good:**
```python
result = client.default_collection.get(key=key)
# ... process result ...
client.close()
```

## Code Quality Checks Before Submitting

### Review Checklist
Before submitting code, verify:

1. [ ] All test methods have `test_` prefix
2. [ ] Imports are at the top of the file
3. [ ] No commented-out code remains
4. [ ] No debugging print statements
5. [ ] No trailing whitespace
6. [ ] Exception handling includes meaningful error messages
7. [ ] Logging uses `self.log` (not `print`)
8. [ ] Sensitive data is not logged
9. [ ] Configuration is parameterized with defaults
10. [ ] Magic values have explanatory comments
11. [ ] Code duplication is minimized
12. [ ] Cluster configuration uses minimum required nodes/services
13. [ ] Requirements.txt is updated if needed
14. [ ] All resources are properly cleaned up

## Common Pitfalls to Avoid

### 1. Swallowing Exceptions
Never silently catch exceptions without proper logging.

```python
# BAD: Silent failure
try:
    risky_operation()
except:
    pass

# GOOD: Proper error handling
try:
    risky_operation()
except Exception as e:
    self.log.error(f"Risky operation failed: {e}")
    self.fail(f"Operation failed: {e}")
```

### 2. Hardcoded Timeouts
Use configurable parameters for timeouts.

```python
# BAD: Hardcoded timeout
time.sleep(120)

# GOOD: Configurable timeout
timeout = self.input.param("timeout", 120)
time.sleep(timeout)
```

### 3. Assuming Operations Succeed
Always verify and validate results.

```python
# BAD: Assuming success
create_index()
time.sleep(10)
# Continue without verification

# GOOD: Verify success
create_index()
if not wait_for_index_online(timeout=120):
    self.fail("Index failed to come online")
```

### 4. Leaving Debug Code
Remove all debugging code before submitting.

```python
# BAD: Debug code left in
print(f"DEBUG: {results}")

# GOOD: Clean production code
self.log.info(f"Query returned {len(results)} results")
```

## Framework-Specific Patterns

### GSI Test Patterns
- Use `base_gsi.py` helper methods when possible
- Reuse existing index creation/query patterns
- Follow parameter naming conventions: `gsi_type`, `services_init`, `nodes_init`

### Eventing Test Patterns
- Use `EventingBaseTest` as base class
- Verify function deployment and execution
- Check eventing logs for errors

### Security Test Patterns
- Use `BaseTestCase` for security tests
- Test both success and failure scenarios
- Verify audit logs where applicable

### Upgrade Test Patterns
- Test both offline and online upgrade modes
- Verify data integrity after upgrade
- Check service continuity during upgrade

## Summary

These best practices aim to:
- Improve code maintainability
- Enhance test reliability
- Facilitate easier debugging
- Ensure security
- Reduce code duplication
- Standardize test patterns across the repository

Following these guidelines will help create clean, maintainable, and reliable test code that serves the team effectively.
