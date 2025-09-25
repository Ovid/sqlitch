# Testing Standards and Guidelines

## Overview
This document establishes mandatory testing standards for the Sqlitch project to ensure code quality, reliability, and maintainability. All contributors must follow these guidelines without exception.

## Core Testing Principles

### 1. Test Isolation (CRITICAL)
**Every test must be completely isolated and leave no trace after execution.**

#### File System Isolation
- **NEVER create files in the project root directory**
- **ALWAYS use `tmp_path` or `tempfile` fixtures** for file operations
- **Explicitly specify file paths** to avoid writing to current working directory
- **Clean up any files, directories, or resources** created during tests

```python
# ❌ BAD: Creates files in project root
def test_config_operations():
    config = Config()
    config.set("key", "value")  # Writes to ./sqitch.conf - POLLUTION!

# ✅ GOOD: Uses temporary directory
def test_config_operations(tmp_path):
    config_file = tmp_path / "sqitch.conf"
    config = Config()
    config.set("key", "value", filename=config_file)
    # tmp_path is automatically cleaned up
```

#### Configuration Isolation
- **Use `Config(config_files=[])` to avoid loading global configs** in unit tests
- **Pass explicit `filename` parameters** to avoid writing to default locations
- **Mock file operations** when testing config behavior without actual I/O

```python
# ❌ BAD: Loads global config and writes to cwd
def test_user_detection():
    config = Config()  # Loads ~/.config/sqlitch/sqitch.conf
    sqitch = Sqitch(config=config)

# ✅ GOOD: Isolated config
def test_user_detection():
    config = Config(config_files=[])  # No global config pollution
    sqitch = Sqitch(config=config)
```

#### Directory Changes
- **Always restore original working directory** after `os.chdir()`
- **Use try/finally blocks or context managers** for directory changes
- **Prefer `runner.isolated_filesystem()`** for CLI tests

```python
# ✅ GOOD: Proper directory handling
def test_directory_operations(tmp_path):
    original_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        # Test operations here
    finally:
        os.chdir(original_cwd)
```

#### Environment Variables
- **Restore original environment** after modifying `os.environ`
- **Use `patch.dict(os.environ, {...}, clear=False)`** for safe modifications
- **Clean up any environment changes** in test teardown

### 2. Test Categories

#### Unit Tests (`tests/unit/`)
- Test individual functions, classes, and methods in isolation
- Mock all external dependencies (databases, file system, network)
- Fast execution (< 1 second per test)
- High coverage of edge cases and error conditions

#### Integration Tests (`tests/integration/`)
- Test component interactions and end-to-end workflows
- Use real databases when necessary (with proper cleanup)
- Test CLI commands with actual file operations
- Verify cross-component compatibility

#### Compatibility Tests (`tests/compatibility/`)
- Verify behavior matches Perl Sqitch reference implementation
- Compare command outputs, file formats, and database schemas
- Gracefully skip tests when Perl Sqitch unavailable
- Document any intentional deviations

### 3. Test Execution Standards

#### Before Task Completion (MANDATORY)
1. **Full test suite must pass without failures:**
   ```bash
   python -m pytest tests/ -v --tb=short
   ```

2. **Address all test warnings** - Warnings indicate potential issues

3. **Run focused tests** for implemented functionality:
   ```bash
   python -m pytest tests/ -k "feature_name" -v
   ```

4. **Verify test coverage** for new code:
   ```bash
   python -m pytest tests/ --cov=sqlitch --cov-report=term-missing
   ```

#### Test Quality Metrics
- **Zero tolerance for failing tests**
- **Zero tolerance for unresolved warnings**
- **Minimum 90% code coverage** for new code
- **All tests must complete within reasonable time** (< 30 seconds for unit tests)

### 4. Common Test Patterns

#### Using Fixtures Properly
```python
@pytest.fixture
def temp_project(tmp_path):
    """Create isolated project structure."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "deploy").mkdir()
    (project_dir / "revert").mkdir()
    (project_dir / "verify").mkdir()
    return project_dir

def test_project_operations(temp_project):
    # Use temp_project safely - automatically cleaned up
    config_file = temp_project / "sqitch.conf"
    # ... test operations
```

#### Mocking External Dependencies
```python
@patch("subprocess.run")
def test_git_operations(mock_run):
    mock_run.return_value = Mock(returncode=0, stdout="test output")
    # Test git-dependent functionality without actual git calls
```

#### CLI Testing with Isolation
```python
def test_cli_command(runner):
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["init", "testproject"])
        assert result.exit_code == 0
        # Files created in isolated filesystem, automatically cleaned up
```

### 5. Database Testing

#### Unit Tests - Mock Everything
```python
def test_database_operations():
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        # Test database logic without real database
```

#### Integration Tests - Use Test Databases
```python
@pytest.fixture
def test_database():
    """Create temporary test database."""
    # Create test database
    db_name = f"test_sqlitch_{uuid.uuid4().hex[:8]}"
    # ... setup code
    yield db_name
    # Cleanup: drop test database
```

### 6. Error Conditions and Edge Cases

#### Test All Error Paths
- Invalid inputs and malformed data
- Missing files and permissions errors
- Network failures and timeouts
- Database connection failures
- Concurrent access scenarios

#### Example Error Testing
```python
def test_config_file_not_found():
    config = Config(config_files=[Path("nonexistent.conf")])
    with pytest.raises(ConfigurationError, match="Cannot read"):
        config.get("some.key")
```

### 7. Performance and Resource Testing

#### Resource Cleanup Verification
```python
def test_resource_cleanup(tmp_path):
    # Perform operations that create resources
    initial_files = list(tmp_path.iterdir())
    
    # ... test operations
    
    # Verify no additional files created
    final_files = list(tmp_path.iterdir())
    assert len(final_files) == len(initial_files)
```

#### Memory and Performance
- Monitor memory usage in long-running tests
- Set reasonable timeouts for operations
- Test with large datasets when relevant

### 8. Test Documentation

#### Test Naming
- Use descriptive test names that explain the scenario
- Follow pattern: `test_<component>_<scenario>_<expected_result>`
- Example: `test_config_set_value_creates_file_in_specified_location`

#### Test Docstrings
```python
def test_user_detection_from_git_config():
    """Test user name detection from Git configuration.
    
    Verifies that when no config file user is set and environment
    variables are empty, the system falls back to Git config.
    """
```

## Enforcement

### Pre-commit Checks
- All tests must pass before commits
- No warnings allowed in test output
- Coverage thresholds must be maintained

### Code Review Requirements
- Test isolation must be verified
- Resource cleanup must be confirmed
- Edge cases must be covered
- Performance impact must be assessed

### Continuous Integration
- Full test suite runs on all platforms
- Database integration tests with real instances
- Compatibility tests against Perl Sqitch
- Performance regression detection

## Common Violations and Fixes

### File System Pollution
```python
# ❌ VIOLATION: Creates files in project root
config = Config()
config.set("core.engine", "pg")  # Creates ./sqitch.conf

# ✅ FIX: Use temporary directory
config = Config()
config.set("core.engine", "pg", filename=tmp_path / "sqitch.conf")
```

### Global State Modification
```python
# ❌ VIOLATION: Modifies global environment
os.environ["SQITCH_USER"] = "test"

# ✅ FIX: Use patch with restoration
with patch.dict(os.environ, {"SQITCH_USER": "test"}):
    # Test code here
# Environment automatically restored
```

### Directory Changes Without Cleanup
```python
# ❌ VIOLATION: Changes directory without restoration
os.chdir(temp_dir)
# Test code
# Directory not restored!

# ✅ FIX: Always restore
original_cwd = os.getcwd()
try:
    os.chdir(temp_dir)
    # Test code
finally:
    os.chdir(original_cwd)
```

## Summary

**Remember: Every test must be completely isolated and leave no trace after execution. This is not optional - it's mandatory for maintaining a reliable test suite.**

When in doubt, ask yourself:
1. Does this test create any files? → Use `tmp_path`
2. Does this test modify global state? → Use mocking/patching
3. Does this test change directories? → Restore original directory
4. Does this test use external resources? → Clean them up
5. Could this test affect other tests? → Fix the isolation issue

**Zero tolerance for test pollution. Clean tests are reliable tests.**