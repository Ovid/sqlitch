# Sqlitch Compatibility Testing

This directory contains comprehensive compatibility tests that verify the Python `sqlitch` implementation behaves identically to the original Perl `sqitch` implementation.

## Overview

The compatibility testing framework compares command-line interface behavior, output formatting, configuration handling, plan file parsing, and database operations between the two implementations to ensure full compatibility.

## Test Categories

### 1. CLI Compatibility Tests (`test_cli_compatibility.py`)
- **Command-line argument parsing**: Verifies that both implementations parse arguments identically
- **Global option handling**: Tests `--verbose`, `--quiet`, `--help`, `--version` options
- **Help and version output**: Ensures output format consistency
- **Error message consistency**: Validates that error messages match between implementations
- **Invalid command handling**: Tests error responses for non-existent commands

### 2. Plan File Compatibility Tests (`test_plan_compatibility.py`)
- **Plan file format**: Verifies identical plan file structure and syntax
- **Change dependency syntax**: Tests dependency declaration format
- **Tag format and placement**: Ensures tag syntax matches
- **Comment and metadata handling**: Validates comment parsing and preservation
- **Plan parsing errors**: Tests error handling for malformed plan files

### 3. Configuration Compatibility Tests (`test_config_compatibility.py`)
- **Config file format (INI)**: Verifies identical configuration file structure
- **Configuration hierarchy**: Tests system, global, and local config precedence
- **Boolean and string value handling**: Ensures value parsing consistency
- **Config command behavior**: Tests configuration get/set operations
- **Section handling**: Validates multi-section configuration files

### 4. Database Registry Compatibility Tests (`test_registry_compatibility.py`)
- **Registry table schema**: Verifies identical database table structures
- **SQL execution behavior**: Tests database operation consistency
- **Transaction handling**: Ensures transaction behavior matches
- **Error reporting**: Validates database error message consistency
- **Registry initialization**: Tests idempotent registry creation

### 5. Framework Validation Tests (`test_framework_validation.py`)
- **Framework functionality**: Tests the compatibility testing framework itself
- **Sqlitch basic operations**: Validates core sqlitch functionality works
- **Test runner integration**: Ensures test infrastructure is working correctly

## Prerequisites

### Required Software
- **Python 3.8+** with sqlitch installed
- **Perl sqitch** (original implementation) for comparison testing

### Installing Perl Sqitch
```bash
# macOS (using Homebrew)
brew install sqitch

# Ubuntu/Debian
apt-get install sqitch

# From source
# See: https://sqitch.org/download/
```

### Verifying Installation
```bash
# Check sqlitch
python -m sqlitch.cli --version

# Check sqitch  
sqitch --version
```

## Running Compatibility Tests

### Run All Compatibility Tests
```bash
# Run all compatibility tests
python -m pytest -m compatibility -v

# Run with detailed output
python -m pytest -m compatibility -v --tb=long

# Run without coverage (faster)
python -m pytest -m compatibility -v --no-cov
```

### Run Specific Test Categories
```bash
# CLI compatibility only
python -m pytest tests/compatibility/test_cli_compatibility.py -v

# Plan file compatibility only
python -m pytest tests/compatibility/test_plan_compatibility.py -v

# Configuration compatibility only
python -m pytest tests/compatibility/test_config_compatibility.py -v

# Database registry compatibility only
python -m pytest tests/compatibility/test_registry_compatibility.py -v
```

### Run Framework Validation (No Perl sqitch required)
```bash
# Test the framework itself
python -m pytest tests/compatibility/test_framework_validation.py -v
```

## Test Results Interpretation

### All Tests Pass ✅
```
======================== 25 passed in 10.5s ========================
```
This indicates full compatibility between sqlitch and Perl sqitch for all tested functionality.

### Tests Skipped (No Perl sqitch) ⏭️
```
======================== 25 skipped in 2.1s ========================
SKIPPED [25] Perl sqitch not available
```
This means Perl sqitch is not installed. Install it to run compatibility tests.

### Tests Failed ❌
```
======================== 2 failed, 23 passed in 8.2s ========================
```
This indicates compatibility issues that need to be addressed. Review the failure details to identify specific incompatibilities.

## Compatibility Test Runner

### Generate Compatibility Report
```bash
# Generate detailed compatibility report
python tests/compatibility/test_runner.py --report

# Run tests and get JSON results
python tests/compatibility/test_runner.py
```

### Example Report Output
```markdown
# Sqlitch Compatibility Report

## Status: ✅ COMPLETED

**Tests Run**: 25
**Passed**: 25
**Failed**: 0
**Skipped**: 0
**Duration**: 10.52s

## Summary

✅ **All compatibility tests passed!**

The Python sqlitch implementation demonstrates full compatibility with Perl sqitch
for all tested functionality including:

- Command-line interface behavior
- Plan file format and parsing
- Configuration file handling
- Database registry operations
```

## Test Implementation Guidelines

### Adding New Compatibility Tests

1. **Create test class** with `@pytest.mark.compatibility` decorator
2. **Check sqitch availability** using `is_sqitch_available()` method
3. **Skip if unavailable** using `pytest.skip("Perl sqitch not available")`
4. **Run both implementations** and compare results
5. **Normalize outputs** for meaningful comparison

### Example Test Structure
```python
@pytest.mark.compatibility
class TestNewFeatureCompatibility:
    def test_feature_behavior(self, compat_tester):
        if not compat_tester.is_sqitch_available():
            pytest.skip("Perl sqitch not available")
        
        # Run both implementations
        sqlitch_result = compat_tester.run_sqlitch(["command", "args"])
        sqitch_result = compat_tester.run_sqitch(["command", "args"])
        
        # Compare results
        comparison = compat_tester.compare_outputs(sqlitch_result, sqitch_result)
        assert comparison['exit_codes_match']
        assert comparison['stdout_match']
```

### Output Normalization

The framework includes output normalization to handle expected differences:

- **Timestamps**: Replaced with `TIMESTAMP` placeholder
- **Absolute paths**: Normalized to relative paths
- **Whitespace**: Normalized for consistent comparison
- **User-specific data**: Filtered out when appropriate

## Continuous Integration

### CI Configuration
The compatibility tests are integrated into the CI pipeline:

```yaml
- name: Run Compatibility Tests
  run: |
    # Install Perl sqitch
    sudo apt-get install sqitch
    
    # Run compatibility tests
    python -m pytest -m compatibility -v
```

### CI Behavior
- **Perl sqitch available**: Runs full compatibility test suite
- **Perl sqitch unavailable**: Skips compatibility tests (not a failure)
- **Compatibility failures**: Fails the CI build

## Troubleshooting

### Common Issues

#### Perl sqitch not found
```bash
# Verify installation
which sqitch
sqitch --version

# Check PATH
echo $PATH
```

#### Database connection issues
```bash
# For SQLite tests, ensure write permissions
chmod 755 /tmp

# For PostgreSQL/MySQL tests, ensure database is running
# Tests use temporary databases when possible
```

#### Test timeouts
```bash
# Increase timeout for slow systems
python -m pytest -m compatibility --timeout=60
```

### Debug Mode
```bash
# Run with maximum verbosity
python -m pytest tests/compatibility/ -v -s --tb=long --capture=no
```

## Contributing

When adding new sqlitch features:

1. **Add compatibility tests** for the new functionality
2. **Verify against Perl sqitch** behavior using the reference implementation
3. **Update documentation** to reflect new test coverage
4. **Ensure CI passes** with new tests included

The compatibility test suite is essential for maintaining fidelity to the original Perl sqitch implementation and ensuring users can migrate seamlessly between implementations.