# Sqitch Compatibility Guidelines

## Reference Implementation
- The Perl Sqitch implementation in `sqitch-perl-source/` is the authoritative reference
- All features should match Perl Sqitch behavior unless explicitly documented otherwise
- Command-line interface should be identical to original Sqitch

## MANDATORY Perl Source Verification
**CRITICAL REQUIREMENT**: Before implementing ANY feature or task:

1. **ALWAYS examine the corresponding Perl source files first**:
   - Read the relevant `.pm` files in `sqitch-perl-source/lib/App/Sqitch/`
   - Understand the exact behavior, error handling, and edge cases
   - Note SQL queries, table schemas, and data formats used
   - Check command-line argument parsing and validation

2. **Key Perl files to reference by feature**:
   - **Engine implementations**: `lib/App/Sqitch/Engine/{pg,mysql,sqlite}.pm`
   - **Commands**: `lib/App/Sqitch/Command/{deploy,revert,status,verify,init}.pm`
   - **Core logic**: `lib/App/Sqitch/{Plan,Config,Change}.pm`
   - **Base engine**: `lib/App/Sqitch/Engine.pm`
   - **Main app**: `lib/App/Sqitch.pm`

3. **Implementation verification steps**:
   - Compare method signatures and return values
   - Match SQL schema definitions exactly
   - Replicate error messages and exception handling
   - Ensure identical command-line behavior
   - Verify configuration file parsing matches

## Verification Process
Before completing any task:
1. **Compare with Perl source**: Review corresponding Perl modules for the feature (MANDATORY)
2. **Test command compatibility**: Ensure CLI commands work identically
3. **Validate database operations**: Check that SQL operations produce same results
4. **Verify file formats**: Ensure plan files, config files match expected format
5. **Check error handling**: Verify error messages and exit codes match

## Mandatory Test Verification
**REQUIRED before marking any task complete:**

1. **Full test suite passes** without failures or warnings:
   ```bash
   python -m pytest tests/ -v --tb=short
   ```

2. **Compatibility tests pass** for implemented features:
   ```bash
   python -m pytest tests/ -k "compatibility or integration" -v
   ```

3. **Manual verification** against Perl sqitch when possible:
   ```bash
   # Compare command outputs
   sqlitch command args > python_output.txt
   sqitch command args > perl_output.txt
   diff python_output.txt perl_output.txt
   ```

4. **File format validation** - Generated files must be identical:
   - Configuration files (`sqitch.conf`)
   - Plan files (`sqitch.plan`) 
   - SQL scripts and templates
   - Registry table schemas

**No exceptions: All tests must pass and documentation must be updated before task completion.**

## Documentation Updates Required
Before completing any task, ensure:
1. **CHANGELOG.md** is updated with a clear description of changes
2. **README.md** is updated if new features or usage patterns are introduced
3. All new functionality includes proper docstrings and examples

## Key Areas to Verify
- **Plan file parsing**: Must handle all Sqitch plan file syntax
- **Configuration**: Support all Sqitch configuration options
- **Database schemas**: Registry tables must match Perl implementation
- **Change tracking**: Deployment/revert logic must be identical
- **Template system**: Change templates should match Perl versions
- **Dependency resolution**: Change dependencies must work identically

## Documentation References
Use these Perl source files as references:
- `lib/App/Sqitch.pm` - Main application logic
- `lib/App/Sqitch/Engine.pm` - Base engine functionality
- `lib/App/Sqitch/Engine/pg.pm` - PostgreSQL engine reference
- `lib/App/Sqitch/Plan.pm` - Plan file handling
- `lib/App/Sqitch/Config.pm` - Configuration management

## Testing Against Reference
- Compare output of `sqlitch` commands with `sqitch` commands
- Use same test databases and scenarios when possible
- Validate that registry table contents are identical
- Ensure plan file parsing produces same internal representation