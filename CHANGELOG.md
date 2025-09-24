# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Tag Command Implementation**: Complete tag command functionality (COMPLETED)
  - Implemented TagCommand class with full CLI compatibility to Perl sqitch
  - Added tag creation and management functionality with validation and conflict detection
  - Implemented support for tagging specific changes or last change in plan
  - Added tag listing functionality to display existing tags
  - Created comprehensive argument parsing with support for all Perl sqitch tag options
  - Implemented note handling with editor integration and multi-part note support
  - Added tag validation with proper error handling for duplicate tags and invalid names
  - Integrated with plan file management for proper tag persistence
  - Added support for --all option to tag across multiple targets
  - Created comprehensive unit tests (27/27 tests passing) covering all functionality
  - Added integration tests for end-to-end tag workflow validation
  - Supports both positional and option-based argument formats
  - Maintains full compatibility with existing Perl sqitch tag command behavior
  - **RESULT: All 1085 tests pass, 11 skipped - tag command successfully implemented**

- **Internationalization Support**: Complete i18n system implementation (COMPLETED)
  - Implemented gettext-based message translation system compatible with Perl sqitch
  - Added translation functions (__(), __x(), __n()) for simple, parameterized, and plural messages
  - Created message catalogs for German (de_DE), French (fr_FR), and Italian (it_IT) languages
  - Implemented locale-aware date/time formatting with multiple format support (ISO, RFC, CLDR)
  - Added translation markers throughout the codebase for user-facing messages
  - Created message extraction utility for maintaining translation files
  - Added comprehensive unit and integration tests for i18n functionality
  - Supports automatic locale detection and graceful fallback to English
  - Compatible with all existing Perl sqitch translated messages

- **Oracle Database Engine**: Complete Oracle database support (COMPLETED)
  - Implemented OracleEngine class with cx_Oracle driver integration
  - Added Oracle-specific connection handling with proper session configuration
  - Created Oracle registry table schema matching Perl sqitch exactly
  - Implemented Oracle-specific SQL execution with PL/SQL block support
  - Added Oracle statement splitting for proper / delimiter handling
  - Implemented Oracle-specific registry operations and metadata management
  - Added comprehensive unit tests with mock Oracle connections
  - Added integration tests for full Oracle workflow testing
  - Supports Oracle 10g and higher with proper UTF-8 encoding
  - Includes Oracle-specific error handling and transaction management

### Fixed
- **Enhanced Error Handling and User Feedback**: Comprehensive improvements to error handling system (COMPLETED)
  - Fixed exception hierarchy for proper error handling inheritance (ConnectionError and DeploymentError)
  - Resolved CLI syntax errors and improved error message formatting
  - Enhanced error message formatting matching Perl sqitch style with context-specific suggestions
  - Improved CLI error handling with better exception management and proper exit codes
  - Added operation feedback system with detailed progress reporting and timing information
  - Implemented change-level reporting for individual database operations
  - Added precondition validation with helpful error messages
  - Enhanced base command class with comprehensive error handling methods
  - Added confirmation prompts for destructive operations
  - Implemented operation summaries with timing and change count information
  - Fixed thread safety issues in progress indicators
  - Maintained backward compatibility with existing test expectations
  - All error handling tests now pass (152/152 tests passing)
  - **RESULT: All 989 tests now pass, 10 skipped - comprehensive error handling successfully implemented**
- **Add Command Implementation**: Comprehensive fix resolving all test failures (42/42 tests now passing)
  - Fixed Target class consolidation and import issues across the codebase
  - Corrected engine creation parameters and initialization flow
  - Fixed plan file format compatibility (dependency and conflict formatting)
  - Resolved timezone handling issues in datetime operations
  - Enhanced CLI output integration and editor functionality
  - Improved test infrastructure with proper mocking and environment handling
  - All add command functionality now fully compatible with original Perl Sqitch

### Added
- **Log Command Implementation**: Complete implementation of the `sqlitch log` command
  - Full CLI compatibility with Perl sqitch log command
  - Support for all log formats: raw, full, long, medium, short, oneline
  - Event filtering by type (deploy, revert, fail)
  - Pattern-based filtering for changes, projects, committers, and planners
  - Date format customization (iso, raw, short, custom strftime)
  - Color output support with auto-detection
  - Change ID abbreviation support
  - Pagination with --max-count and --skip options
  - Reverse chronological ordering
  - Header display control
  - Comprehensive ItemFormatter with git-style format codes
  - Database-specific regex support (PostgreSQL ~, MySQL REGEXP, SQLite GLOB)
  - Full test coverage with 73 unit and integration tests
- **MySQL Database Engine**: Complete implementation of MySQL/MariaDB support with:
  - MySQL-specific connection handling using PyMySQL driver
  - Registry table creation and management with proper MySQL schema
  - MySQL-specific SQL execution with DELIMITER support
  - Version compatibility checking (MySQL 5.1.0+, MariaDB 5.3.0+)
  - Transaction management with table locking for concurrent access control
  - Support for MySQL connection string formats and query parameters
  - Comprehensive unit and integration test coverage
  - Full compatibility with original Perl Sqitch MySQL engine behavior

- **SQLite Database Engine**: Complete implementation of SQLite support with:
  - SQLite-specific connection handling with proper configuration
  - Registry table creation with SQLite-specific constraints and foreign keys
  - SQLite version compatibility checking (3.8.6+)
  - File-based and in-memory database support
  - Transaction management with proper rollback on errors
  - Support for SQLite URI formats (sqlite:, sqlite://, db:sqlite:)
  - Parameter binding compatibility with base engine interface
  - Comprehensive unit and integration test coverage
  - Full compatibility with original Perl Sqitch SQLite engine behavior

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

- **VCS Integration Utilities**: Complete Git integration system with:
  - Repository detection and status checking
  - User name and email detection from Git configuration
  - Change file naming based on VCS state (branch-aware naming)
  - Commit integration for change tracking
  - Repository initialization and file management
  - File history and tracking status queries
  - Branch-aware change name suggestions
  - Comprehensive error handling for Git operations
  - Full test coverage with both unit and integration tests

- **Template System**: Complete template processing system with:
  - Jinja2-based template engine with Template Toolkit syntax conversion
  - Built-in templates for all supported database engines (PostgreSQL, MySQL, SQLite, Oracle, Snowflake, Vertica, Exasol, Firebird, CockroachDB)
  - Support for custom template directories with precedence handling
  - Template variable substitution for SQL scripts (project, change, engine, requires, conflicts)
  - Template discovery and listing functionality
  - Comprehensive error handling and validation
  - Full test coverage with unit and integration tests

- **Status Command**: Complete implementation of the `sqlitch status` command with:
  - Current deployment state reporting with project, change, and deployment details
  - Optional display of deployed changes list (`--show-changes`)
  - Optional display of deployed tags list (`--show-tags`)
  - Comparison with plan file to show undeployed changes
  - Support for multiple date formats (ISO, RFC, custom strftime)
  - Proper alignment and formatting for tabular output
  - Target database selection (`--target`)
  - Custom plan file support (`--plan-file`)
  - Project-specific status queries (`--project`)
  - CLI integration with Click framework
  - Comprehensive unit and integration test coverage
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