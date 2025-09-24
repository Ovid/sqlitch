# Sqitch Compatibility Guidelines

## Reference Implementation
- The Perl Sqitch implementation in `sqitch-perl-source/` is the authoritative reference
- All features should match Perl Sqitch behavior unless explicitly documented otherwise
- Command-line interface should be identical to original Sqitch

## Verification Process
Before completing any task:
1. **Compare with Perl source**: Review corresponding Perl modules for the feature
2. **Test command compatibility**: Ensure CLI commands work identically
3. **Validate database operations**: Check that SQL operations produce same results
4. **Verify file formats**: Ensure plan files, config files match expected format
5. **Check error handling**: Verify error messages and exit codes match

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