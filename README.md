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

* **Iterative development** - Modify your change scripts as often as you like until you tag and release. Perfect for test-driven database development.

## Supported Databases

Sqlitch currently supports:

* **PostgreSQL** 8.4+ (including YugabyteDB and CockroachDB)
* **SQLite** 3.8.6+
* Additional database engines coming soon

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

## Quick Start

1. **Initialize a new project:**
   ```bash
   sqlitch init myproject --engine pg
   ```

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

## Documentation

* [Getting Started Guide](docs/getting-started.md) - Your first steps with Sqlitch
* [Command Reference](docs/commands.md) - Complete command documentation
* [Configuration](docs/configuration.md) - How to configure Sqlitch
* [PostgreSQL Tutorial](docs/tutorial-postgresql.md) - Detailed PostgreSQL walkthrough

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
- ✅ Core plan file parsing
- ✅ Configuration management
- ✅ PostgreSQL engine (partial)
- ✅ Command-line interface (init, deploy, revert, verify commands)
- ✅ Change deployment, revert, and verification logic
- ⏳ SQLite engine
- ⏳ Additional database engines

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
git clone git@github.com:Ovid/sqlitch.git
cd sqlitch
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"
pytest
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