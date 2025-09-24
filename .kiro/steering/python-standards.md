# Python Development Standards

## Code Style
- Follow PEP 8 style guidelines
- Use type hints for all function parameters and return values
- Maximum line length: 88 characters (Black formatter standard)
- Use meaningful variable and function names
- Prefer explicit over implicit code

## Project Structure
- Organize code into logical modules and packages
- Use `__init__.py` files to define package interfaces
- Separate concerns: CLI, core logic, database engines, utilities
- Keep test files parallel to source structure

## Error Handling
- Use specific exception types rather than generic `Exception`
- Create custom exception classes in `sqlitch.core.exceptions`
- Always provide meaningful error messages
- Log errors appropriately before raising

## Dependencies
- Minimize external dependencies where possible
- Pin dependency versions in requirements files
- Use standard library solutions when available
- Document why each dependency is needed

## Testing
- Use pytest as the testing framework
- Aim for >90% code coverage
- Use fixtures for common test setup
- Mock external dependencies (databases, file system) in unit tests
- Write integration tests for end-to-end workflows

### Test Execution Standards
**MANDATORY before task completion:**

1. **Full test suite must pass without failures:**
   ```bash
   python -m pytest tests/ -v
   ```

2. **Address all test warnings** - Warnings indicate potential issues:
   ```bash
   python -m pytest tests/ -v --disable-warnings  # Only if warnings are addressed
   ```

3. **Run specific test categories:**
   ```bash
   # Unit tests only
   python -m pytest tests/unit/ -v
   
   # Integration tests only  
   python -m pytest tests/integration/ -v
   
   # Feature-specific tests
   python -m pytest tests/ -k "feature_name" -v
   ```

4. **Verify test coverage** for new code:
   ```bash
   python -m pytest tests/ --cov=sqlitch --cov-report=term-missing
   ```

**Zero tolerance for failing tests or unresolved warnings in completed tasks.**

## Documentation
- Write docstrings for all public classes and methods
- Use Google-style docstrings
- Include type information in docstrings
- Provide usage examples in docstrings where helpful

## Resource Management
```python
# Good: Use context managers
with open(file_path, 'r') as f:
    content = f.read()

# Good: Clean up temporary files
try:
    temp_file = create_temp_file()
    process_file(temp_file)
finally:
    if temp_file.exists():
        temp_file.unlink()
```