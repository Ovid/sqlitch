# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Deploy Command**: Complete implementation of the `sqlitch deploy` command with:
  - Deployment planning and execution logic
  - Transaction management with automatic rollback on failure
  - Progress reporting and verbose logging
  - Support for deploying up to specific changes or tags (`--to-change`, `--to-tag`)
  - Log-only mode for previewing deployments (`--log-only`)
  - Verification support with optional skip (`--verify`/`--no-verify`)
  - Comprehensive dependency validation
  - CLI integration with Click framework
  - Full test coverage including unit and integration tests
- **Revert Command**: Complete implementation of the `sqlitch revert` command with:
  - Revert planning and execution logic with proper change ordering
  - Interactive confirmation prompts with safety checks (`--no-prompt`/`-y` to skip)
  - Support for reverting to specific changes or tags (`--to-change`, `--to-tag`)
  - Log-only mode for previewing reverts (`--log-only`)
  - Strict mode requiring target specification (`--strict`)
  - Modified mode for VCS integration (`--modified`)
  - Comprehensive error handling and user feedback
  - CLI integration with Click framework
  - Full test coverage including unit and integration tests with 45 unit tests and 18 integration tests
- **Verify Command**: Complete implementation of the `sqlitch verify` command with:
  - Verification script execution and result reporting with detailed error messages
  - Support for verifying specific change ranges (`--from-change`, `--to-change`)
  - Parallel verification for improved performance with configurable worker count (`--max-workers`)
  - Sequential verification mode for debugging (`--no-parallel`)
  - Variable substitution support for verification scripts (`--set key=value`)
  - Comprehensive error reporting including out-of-order changes and missing scripts
  - Detection and reporting of undeployed changes in verification range
  - Summary reporting with change counts and error details
  - CLI integration with Click framework
  - Full test coverage with 44 unit tests and 18 integration tests
- Enhanced Plan class with additional properties and methods:
  - Added `project_name`, `creator_name`, `creator_email` properties
  - Added `get_deploy_file()`, `get_revert_file()`, `get_verify_file()` methods
- Enhanced Change class with display formatting:
  - Added `format_name_with_tags()` method for consistent change display across commands
- Enhanced steering documentation with mandatory changelog and README.md update requirements
- Task completion requirements now include documentation updates for all three steering files:
  - `project-guidelines.md`: Added step 7 requiring changelog and README updates
  - `sqitch-compatibility.md`: Added "Documentation Updates Required" section
  - `python-standards.md`: Enhanced zero tolerance policy to include documentation requirements

### Changed
- Engine creation now uses `EngineRegistry.create_engine()` for proper plan integration
- Base command class no longer includes non-existent `verbose()` method
- Deploy command uses `info()` for verbose logging instead of undefined `verbose()` method
- All steering documents now consistently require documentation updates as part of task completion
- Task completion process now has zero tolerance for missing documentation updates alongside test failures and warnings

### Fixed
- Fixed dependency validation logic in deploy command to properly map change IDs to names
- Fixed mock database connections in integration tests to support context manager protocol
- Fixed CLI integration tests to avoid system configuration file conflicts

## [0.1.0] - Initial Development

### Added
- Initial project structure and core framework
- PostgreSQL engine implementation (partial)
- Core plan file parsing functionality
- Configuration management system
- Command-line interface foundation
- Comprehensive test suite with unit and integration tests
- Development steering documentation for:
  - Sqitch compatibility guidelines
  - Python development standards  
  - Project-specific guidelines