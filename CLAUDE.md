# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sqitch is a database change management application written in Perl. It provides sensible database change management across multiple database engines without being tied to any specific framework, ORM, or platform.

## Common Development Commands

### Build and Installation
- `perl Build.PL` - Configure the build
- `./Build installdeps` - Install dependencies
- `./Build` - Build the project
- `./Build test` - Run tests
- `./Build install` - Install Sqitch

### Development from Git Clone
- `cpanm Dist::Zilla` - Install Dist::Zilla for development
- `dzil authordeps --missing | cpanm` - Install author dependencies
- `dzil listdeps --missing | cpanm` - Install all dependencies
- `dzil install` - Install from development tree
- `t/sqitch` - Run Sqitch directly from Git clone

### Testing
- `./Build test` - Run full test suite (51+ test files)
- `prove t/` - Alternative test runner
- Tests are located in `t/` directory with `.t` extension

### Bundle Creation
- `./Build bundle --install_base sqitch_bundle` - Create standalone bundle
- `./Build bundle --install_base sqitch_bundle --with postgres --with sqlite` - Bundle with specific database support
- `--dual_life 1` - Include dual-life modules in bundle

## Architecture

### Core Components

**Main Application (`lib/App/Sqitch.pm`)**
- Entry point for the Sqitch application
- Handles configuration, options, and command dispatch
- Built using Moo object system
- Uses Locale::TextDomain for internationalization

**Commands (`lib/App/Sqitch/Command/`)**
- Individual command implementations: add, bundle, check, checkout, config, deploy, engine, help, init, log, plan, rebase, revert, rework, show, status, tag, target, upgrade, verify
- Each command inherits from `App::Sqitch::Command`
- Commands follow a consistent pattern with validation and execution phases

**Database Engines (`lib/App/Sqitch/Engine/`)**
- Database-specific implementations: cockroach, exasol, firebird, mysql, oracle, pg (PostgreSQL), snowflake, sqlite, vertica
- Each engine has corresponding `.pm` (Perl module) and `.sql` (SQL schemas) files
- Engines inherit from `App::Sqitch::Engine` via `App::Sqitch::Role::DBIEngine`

**Plan Management (`lib/App/Sqitch/Plan.pm`)**
- Manages database change plans and dependencies
- Implements Merkle tree-like integrity checking
- Handles change ordering and dependency resolution

**Configuration (`lib/App/Sqitch/Config.pm`)**
- Git-like configuration system using Config::GitLike
- Supports hierarchical configuration (system, global, local)

### Database Support

Sqitch supports these database engines with version-specific features:
- PostgreSQL 8.4+ (including YugabyteDB 2.6+, CockroachDB 21+)
- SQLite 3.8.6+
- MySQL 5.1+/MariaDB 10.0+
- Oracle 10g+
- Firebird 2.0+
- Vertica 7.2+
- Exasol 6.0+
- Snowflake

### Key Design Principles

1. **Database Agnostic**: Native SQL scripts for each database engine
2. **Dependency Resolution**: Changes can declare dependencies on other changes
3. **Deployment Integrity**: Merkle tree pattern ensures deployment integrity
4. **Iterative Development**: Changes can be modified until tagged/released
5. **No Framework Lock-in**: Standalone tool independent of ORMs or frameworks

## Development Notes

### Custom Build System
- Uses custom `Module::Build::Sqitch` (in `inc/Module/Build/Sqitch.pm`)
- Supports bundling with database-specific dependencies
- Handles platform-specific requirements (Windows support via Win32 modules)

### Internationalization
- Uses Locale::TextDomain for i18n
- Message files in `lib/LocaleData/`
- UTF-8 encoding forced for all text output

### Testing Strategy
- Comprehensive test suite with 51+ test files
- Uses Test::More, Test::Exception, Test::MockModule
- Database-specific tests require corresponding DBD modules
- Mock objects used for isolated testing

### Documentation
- Extensive POD documentation in `lib/` directory
- Tutorial files for each supported database engine
- Command-specific documentation files