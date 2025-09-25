"""
Compatibility test runner and utilities.

This module provides utilities for running comprehensive compatibility
tests between sqlitch and Perl sqitch implementations.
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


class CompatibilityTestRunner:
    """Runner for comprehensive compatibility tests."""

    def __init__(self):
        self.results = []
        self.sqitch_available = self._check_sqitch_availability()

    def _check_sqitch_availability(self) -> bool:
        """Check if Perl sqitch is available for testing."""
        try:
            result = subprocess.run(
                ["sqitch", "--version"], capture_output=True, text=True, timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def run_compatibility_tests(
        self, test_patterns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Run compatibility tests and return results."""
        if not self.sqitch_available:
            return {
                "status": "skipped",
                "reason": "Perl sqitch not available",
                "tests_run": 0,
                "tests_passed": 0,
                "tests_failed": 0,
            }

        # Build pytest command
        cmd = ["python", "-m", "pytest", "-m", "compatibility", "-v", "--tb=short"]

        if test_patterns:
            for pattern in test_patterns:
                cmd.extend(["-k", pattern])

        # Add compatibility test directory
        cmd.append("tests/compatibility/")

        # Run tests
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()

        # Parse results
        return self._parse_test_results(result, end_time - start_time)

    def _parse_test_results(
        self, result: subprocess.CompletedProcess, duration: float
    ) -> Dict[str, Any]:
        """Parse pytest output to extract test results."""
        output = result.stdout + result.stderr

        # Extract test counts from pytest output
        tests_run = 0
        tests_passed = 0
        tests_failed = 0
        tests_skipped = 0

        # Look for pytest summary line
        lines = output.split("\n")
        for line in lines:
            if "passed" in line and "failed" in line:
                # Parse line like "5 passed, 2 failed, 1 skipped in 10.5s"
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "passed" and i > 0:
                        tests_passed = int(parts[i - 1])
                    elif part == "failed" and i > 0:
                        tests_failed = int(parts[i - 1])
                    elif part == "skipped" and i > 0:
                        tests_skipped = int(parts[i - 1])
                break
            elif "passed" in line and "failed" not in line:
                # Parse line like "5 passed in 10.5s"
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "passed" and i > 0:
                        tests_passed = int(parts[i - 1])
                break

        tests_run = tests_passed + tests_failed

        return {
            "status": "completed",
            "exit_code": result.returncode,
            "tests_run": tests_run,
            "tests_passed": tests_passed,
            "tests_failed": tests_failed,
            "tests_skipped": tests_skipped,
            "duration": duration,
            "output": output,
            "success": result.returncode == 0 and tests_failed == 0,
        }

    def generate_compatibility_report(self) -> str:
        """Generate a compatibility report."""
        if not self.sqitch_available:
            return """
# Sqlitch Compatibility Report

## Status: SKIPPED
**Reason**: Perl sqitch not available for comparison testing

To run compatibility tests, please install Perl sqitch:
- On macOS: `brew install sqitch`
- On Ubuntu/Debian: `apt-get install sqitch`
- From source: https://sqitch.org/download/

## Test Categories

The following compatibility test categories are available:

### CLI Compatibility Tests
- Command-line argument parsing
- Global option handling
- Help and version output format
- Error message consistency

### Plan File Compatibility Tests
- Plan file format and structure
- Change dependency syntax
- Tag format and placement
- Comment and metadata handling

### Configuration Compatibility Tests
- Config file format (INI)
- Configuration hierarchy
- Boolean and string value handling
- Config command behavior

### Database Engine Compatibility Tests
- Registry table schema
- SQL execution behavior
- Transaction handling
- Error reporting

Run `python -m pytest -m compatibility` after installing Perl sqitch.
"""

        # Run tests and generate report
        results = self.run_compatibility_tests()

        status_emoji = "✅" if results["success"] else "❌"

        report = f"""
# Sqlitch Compatibility Report

## Status: {status_emoji} {results["status"].upper()}

**Tests Run**: {results["tests_run"]}
**Passed**: {results["tests_passed"]}
**Failed**: {results["tests_failed"]}
**Skipped**: {results["tests_skipped"]}
**Duration**: {results["duration"]:.2f}s

## Summary

"""

        if results["success"]:
            report += """
✅ **All compatibility tests passed!**

The Python sqlitch implementation demonstrates full compatibility with Perl sqitch
for all tested functionality including:

- Command-line interface behavior
- Plan file format and parsing
- Configuration file handling
- Database registry operations
"""
        else:
            report += f"""
❌ **{results["tests_failed"]} compatibility test(s) failed**

Some differences were detected between sqlitch and Perl sqitch implementations.
Review the test output below for details on specific incompatibilities.

## Failed Tests Output

```
{results["output"]}
```
"""

        return report


def main():
    """Main entry point for compatibility test runner."""
    runner = CompatibilityTestRunner()

    if len(sys.argv) > 1 and sys.argv[1] == "--report":
        # Generate and print compatibility report
        print(runner.generate_compatibility_report())
    else:
        # Run tests and print results
        results = runner.run_compatibility_tests()
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
