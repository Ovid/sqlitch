# Sqlitch - Python Database Change Management

[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Sqlitch** is a Python port of the popular [Sqitch] database change management application. It provides the same powerful, framework-agnostic approach to database schema management with the convenience and ecosystem of Python.

## What is Sqlitch?

Sqlitch is a database change management system that helps you manage database schema changes in a sane, reliable way. It's a Python implementation of the proven Sqitch methodology.

### Key Features

* **No opinions** - Not tied to any framework, ORM, or platform. Works with any Python application or standalone.

* **Native scripting** - Changes are implemented as scripts native to your database engine. Writing a PostgreSQL application? Write SQL scripts for `psql`.

* **Dependency resolution** - Database changes can declare dependencies on other changes, ensuring proper execution order even when changes are committed out-of-order.

* **Deployment integrity** - Manages changes via a plan file using a Merkle tree pattern similar to Git, ensuring deployment integrity without requiring numbered migrations.

* **Version control integration** - Automatically integrates with Git repositories, including user detection, repository initialization, and branch-aware change naming.

* **Iterative development** - Modify your change scripts as often as you like until you tag and release. Perfect for test-driven database development.

* **Internationalization** - Full i18n support with translations for German, French, and Italian. Automatic locale detection with graceful English fallback.

## Supported Databases

Sqlitch currently supports:

* **PostgreSQL** 8.4+ (including YugabyteDB and CockroachDB)
* **MySQL** 5.1.0+ and **MariaDB** 5.3.0+
* **SQLite** 3.8.6+
* **Oracle** 10g+ (requires cx_Oracle driver)
* Additional database engines in development (Snowflake, Vertica, Exasol, Firebird)

## Installation

### From PyPI (when available)

```bash
pip install sqlitch
```

### From Source

```bash
git clone git@github.com:Ovid/sqlitch.git
cd sqlitch
pip install -e .
```

### Database Driver Requirements

Sqlitch requires database-specific drivers for each engine:

* **PostgreSQL**: `psycopg2` or `psycopg2-binary`
* **MySQL/MariaDB**: `PyMySQL`
* **SQLite**: Built into Python (no additional driver needed)
* **Oracle**: `cx_Oracle` (requires Oracle Instant Client)

Install drivers as needed:
```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL/MariaDB  
pip install PyMySQL

# Oracle
pip install cx_Oracle
```

## Quick Start

1. **Initialize a new project:**
   ```bash
   sqlitch init myproject --engine pg
   ```
   This automatically initializes a Git repository and creates a `.gitignore` file. Use `--no-vcs` to skip version control setup.
   
   Supported engines: `pg` (PostgreSQL), `mysql`, `sqlite`, `oracle`

2. **Add your first change:**
   ```bash
   sqlitch add users --note "Add users table"
   ```

3. **Deploy to your database:**
   ```bash
   sqlitch deploy
   ```

4. **Verify your deployment:**
   ```bash
   sqlitch verify
   ```

5. **Check deployment status:**
   ```bash
   sqlitch status
   ```

## Database-Specific Configuration

### Oracle

For Oracle databases, you may need to set environment variables:

```bash
export ORACLE_HOME=/path/to/oracle/client
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
```

Connection strings support various formats:
```bash
# Basic connection
sqlitch init myproject --engine oracle --target oracle://user:pass@host:1521/service

# Using TNS names
sqlitch init myproject --engine oracle --target oracle://user:pass@tnsname

# Using environment variables (TWO_TASK, ORACLE_SID)
sqlitch init myproject --engine oracle --target oracle://user:pass@
```

## Available Commands

Sqlitch provides a comprehensive set of commands for database change management:

* `sqlitch init` - Initialize a new Sqlitch project
* `sqlitch add` - Add a new change to the plan
* `sqlitch deploy` - Deploy changes to the database
* `sqlitch revert` - Revert changes from the database
* `sqlitch verify` - Verify deployed changes
* `sqlitch status` - Show current deployment status
* `sqlitch log` - Show deployment history

Each command supports extensive options for customization. Use `sqlitch <command> --help` for detailed usage information.

## Internationalization

Sqlitch supports multiple languages with automatic locale detection:

* **English** (default)
* **German (Deutsch)** - Complete translation
* **French (Français)** - Complete translation  
* **Italian (Italiano)** - Complete translation

Set your locale to see Sqlitch messages in your preferred language:

```bash
# Linux/macOS
export LANG=de_DE.UTF-8  # German
export LANG=fr_FR.UTF-8  # French
export LANG=it_IT.UTF-8  # Italian

# Windows
set LANG=de_DE.UTF-8
```

Date and time formatting automatically adapts to your locale settings.

## Documentation

* [Getting Started Guide](docs/getting-started.md) - Your first steps with Sqlitch
* [Command Reference](docs/commands.md) - Complete command documentation
* [Configuration](docs/configuration.md) - How to configure Sqlitch
* [PostgreSQL Tutorial](docs/tutorial-postgresql.md) - Detailed PostgreSQL walkthrough
* [Internationalization](docs/i18n.md) - Multi-language support and localization

## Relationship to Original Sqitch

Sqlitch is a faithful Python port of the original [Sqitch] Perl application created by David E. Wheeler. It aims to provide 100% compatibility with Sqitch's:

* Plan file format
* Configuration system  
* Command-line interface
* Database registry schema
* Change deployment logic

This means you can use Sqlitch as a drop-in replacement for Sqitch in Python environments, or even migrate existing Sqitch projects to use Sqlitch.

## Development Status

🚧 **This project is currently in active development.** 

Current implementation status:
- ✅ Core plan file parsing and management
- ✅ Configuration management system
- ✅ PostgreSQL engine (complete)
- ✅ MySQL/MariaDB engine (complete)
- ✅ SQLite engine (complete)
- ✅ Oracle engine (complete)
- ✅ Command-line interface (init, add, deploy, revert, verify, status, log commands)
- ✅ Change deployment, revert, and verification logic
- ✅ Template system with Jinja2 support
- ✅ VCS integration (Git)
- ✅ Comprehensive error handling and user feedback
- ✅ Internationalization support (German, French, Italian)
- ⏳ Additional database engines (Snowflake, Vertica, Exasol, Firebird)
- ⏳ Advanced commands (tag, bundle, checkout, rebase, show)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
git clone git@github.com:Ovid/sqlitch.git
cd sqlitch
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"

# Install database drivers for testing
pip install psycopg2-binary PyMySQL cx_Oracle

# Run tests
pytest

# Run tests with coverage
pytest --cov=sqlitch --cov-report=html
```

### Testing with Different Databases

The test suite includes both unit tests and integration tests. Integration tests require running database instances:

```bash
# PostgreSQL integration tests
docker run -d --name postgres-test -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:13
export POSTGRES_TEST_HOST=localhost

# MySQL integration tests  
docker run -d --name mysql-test -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8.0
export MYSQL_TEST_HOST=localhost

# Oracle integration tests (requires Oracle container)
# See tests/integration/test_oracle_engine_integration.py for setup details
```

## License

Copyright (c) 2012-2025 David E. Wheeler, iovation Inc.

This is free software, licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

* **David E. Wheeler** - Creator of the original Sqitch
* **iovation Inc.** - Original Sqitch development
* The **Sqitch community** - For the excellent foundation and methodology

---

*Sqlitch is not affiliated with or endorsed by the original Sqitch project, but is built with deep respect for its design and methodology.*

[Sqitch]: https://sqitch.org/