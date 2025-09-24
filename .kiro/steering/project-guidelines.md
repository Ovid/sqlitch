# Sqlitch Project Guidelines

## Project Overview
- **Tool Name**: sqlitch (not sqitch)
- **Command Line Executable**: `sqlitch`
- **Language**: Python
- **Purpose**: Python port of the Perl Sqitch database change management tool

## Development Standards

### Naming Conventions
- Use `sqlitch` consistently in all references, documentation, and code
- Command line tool should be installed and accessible as `sqlitch`
- Package name: `sqlitch`
- Module imports should reference `sqlitch`

### Code Quality
- Follow Python best practices (PEP 8, type hints, docstrings)
- Use proper error handling and logging
- Write comprehensive unit and integration tests
- Maintain clean, readable, and well-documented code

### Resource Management
- Always clean up temporary files and resources
- Use context managers (`with` statements) for file operations
- Properly close database connections and file handles
- Clean up test artifacts after test runs

### Implementation Fidelity
- Before marking any task as complete, verify implementation against the original Perl Sqitch
- Check for feature gaps, missing functionality, or behavioral differences
- Ensure command-line interface matches expected Sqitch behavior
- Validate that database operations produce equivalent results

### Testing Requirements
- Write tests before implementing features (TDD approach)
- Include both unit tests and integration tests
- Test error conditions and edge cases
- Verify compatibility with multiple database engines
- Test command-line interface thoroughly

### Task Completion Requirements
**CRITICAL**: Before marking any task as complete, you MUST:

1. **Run the full test suite** and ensure ALL tests pass without warnings:
   ```bash
   python -m pytest tests/ -v --tb=short
   ```

2. **Verify zero test failures** - Any failing tests must be fixed before task completion

3. **Address all warnings** - Test warnings indicate potential issues that must be resolved

4. **Run focused tests** for the implemented functionality:
   ```bash
   python -m pytest tests/ -k "relevant_test_pattern" -v
   ```

5. **Verify CLI integration** works end-to-end for command implementations

6. **Test against Perl sqitch reference** when applicable to ensure compatibility

**No task should be marked as completed if the test suite has any failures or unresolved warnings.**

### Documentation
- Maintain clear docstrings for all classes and methods
- Update README and documentation as features are added
- Include usage examples in documentation
- Document any deviations from original Sqitch behavior