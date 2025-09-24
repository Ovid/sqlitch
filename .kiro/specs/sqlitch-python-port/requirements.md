# Requirements Document

## Introduction

This project aims to create a complete Python port of the Perl-based sqitch database change management tool, renamed as "sqlitch" to avoid confusion with the original. The port will maintain full CLI compatibility and feature parity while leveraging modern Python idioms and practices. The port will support all database engines currently supported by the original sqitch (PostgreSQL, MySQL, SQLite, Oracle, Snowflake, Vertica, Exasol, Firebird) and maintain compatibility with existing sqitch.conf and sqitch.plan files.

## Requirements

### Requirement 1

**User Story:** As a database developer currently using Perl sqitch, I want to use a Python version (sqlitch) with identical CLI behavior, so that I can migrate without changing my workflows or scripts.

#### Acceptance Criteria

1. WHEN a user runs any sqlitch command with the same arguments THEN the Python version SHALL produce identical output to the Perl version
2. WHEN a user provides configuration via sqitch.conf files THEN the Python version SHALL read and interpret them identically to the Perl version
3. WHEN a user has existing sqitch.plan files THEN the Python version SHALL parse and execute them without modification
4. WHEN a user runs sqlitch commands THEN the exit codes SHALL match the Perl version exactly
5. WHEN a user provides environment variables THEN the Python version SHALL honor them in the same way as the Perl version

### Requirement 2

**User Story:** As a database administrator, I want the Python sqlitch to support all database engines that the original supports, so that I can use it across my entire database infrastructure.

#### Acceptance Criteria

1. WHEN deploying to PostgreSQL THEN the Python version SHALL create identical metadata tables and execute changes correctly
2. WHEN deploying to MySQL/MariaDB THEN the Python version SHALL handle all MySQL-specific features and syntax
3. WHEN deploying to SQLite THEN the Python version SHALL manage the embedded database correctly
4. WHEN deploying to Oracle THEN the Python version SHALL handle Oracle-specific SQL and connection requirements
5. WHEN deploying to Snowflake THEN the Python version SHALL integrate with Snowflake's authentication and SQL dialect
6. WHEN deploying to Vertica THEN the Python version SHALL handle Vertica's columnar database features
7. WHEN deploying to Exasol THEN the Python version SHALL support Exasol's analytics database requirements
8. WHEN deploying to Firebird THEN the Python version SHALL work with Firebird's SQL dialect and features

### Requirement 3

**User Story:** As a Python developer, I want the sqlitch port to use modern Python practices and type hints, so that it's maintainable and follows Python conventions.

#### Acceptance Criteria

1. WHEN examining the codebase THEN all functions and methods SHALL have proper type hints
2. WHEN running static analysis THEN the code SHALL pass mypy type checking
3. WHEN reviewing the code THEN it SHALL follow PEP 8 style guidelines
4. WHEN looking at data structures THEN they SHALL use dataclasses where appropriate
5. WHEN handling errors THEN the code SHALL use Python's exception system properly
6. WHEN working with file paths THEN the code SHALL use pathlib.Path consistently

### Requirement 4

**User Story:** As a DevOps engineer, I want the Python sqlitch to integrate with version control systems, so that I can track database changes alongside application code.

#### Acceptance Criteria

1. WHEN initializing a sqlitch project THEN it SHALL integrate with Git repositories correctly
2. WHEN adding changes THEN it SHALL generate proper VCS-friendly file names and structure
3. WHEN bundling projects THEN it SHALL create portable archives that work across environments
4. WHEN checking out database states THEN it SHALL coordinate with VCS history appropriately

### Requirement 5

**User Story:** As a system administrator, I want comprehensive logging and error reporting, so that I can troubleshoot deployment issues effectively.

#### Acceptance Criteria

1. WHEN errors occur THEN the system SHALL provide clear, actionable error messages
2. WHEN running in verbose mode THEN the system SHALL log detailed operation information
3. WHEN database operations fail THEN the system SHALL report the specific SQL and error details
4. WHEN configuration is invalid THEN the system SHALL identify the specific configuration problems
5. WHEN plan parsing fails THEN the system SHALL indicate the exact line and issue in the plan file

### Requirement 6

**User Story:** As a database developer, I want template support for generating SQL scripts, so that I can create consistent, parameterized database changes.

#### Acceptance Criteria

1. WHEN using templates THEN the system SHALL support the same template syntax as the Perl version
2. WHEN template variables are provided THEN they SHALL be substituted correctly in generated SQL
3. WHEN templates are missing THEN the system SHALL provide helpful error messages
4. WHEN custom templates are defined THEN the system SHALL use them instead of defaults

### Requirement 7

**User Story:** As a project maintainer, I want comprehensive test coverage, so that I can ensure reliability and prevent regressions.

#### Acceptance Criteria

1. WHEN running the test suite THEN it SHALL achieve at least 80% code coverage
2. WHEN testing database operations THEN it SHALL use isolated test databases or containers
3. WHEN comparing with Perl sqitch THEN integration tests SHALL verify identical behavior
4. WHEN adding new features THEN they SHALL include corresponding unit tests
5. WHEN testing CLI commands THEN they SHALL verify both success and error scenarios

### Requirement 8

**User Story:** As a user installing sqlitch, I want simple installation and dependency management, so that I can get started quickly.

#### Acceptance Criteria

1. WHEN installing via pip THEN all required dependencies SHALL be installed automatically
2. WHEN running on Python 3.9+ THEN the system SHALL work without compatibility issues
3. WHEN optional database drivers are missing THEN the system SHALL provide clear installation instructions
4. WHEN checking system requirements THEN the system SHALL validate and report any missing components

### Requirement 9

**User Story:** As an international user, I want localized error messages and output, so that I can use sqlitch in my preferred language.

#### Acceptance Criteria

1. WHEN the system locale is set THEN error messages SHALL be displayed in the appropriate language where translations exist
2. WHEN translations are missing THEN the system SHALL fall back to English gracefully
3. WHEN date/time formatting is needed THEN it SHALL respect locale-specific formats
4. WHEN adding new translatable strings THEN they SHALL be properly marked for internationalization

### Requirement 10

**User Story:** As a performance-conscious user, I want the Python version to perform comparably to the Perl version, so that migration doesn't impact my deployment speed.

#### Acceptance Criteria

1. WHEN deploying large numbers of changes THEN the Python version SHALL complete within 20% of the Perl version's time
2. WHEN parsing large plan files THEN the system SHALL use lazy loading to minimize memory usage
3. WHEN connecting to databases THEN the system SHALL reuse connections efficiently
4. WHEN processing batch operations THEN the system SHALL optimize for minimal database round trips