# Sqitch Python Port Project

## Project Goal
Translate the Perl sqitch database change management tool to Python, maintaining feature parity and improving where possible.

## Source Reference
- Perl source code is in `sqitch-perl-source/` directory (git-ignored, read-only reference)
- Original project: https://sqitch.org/
- Documentation: https://metacpan.org/dist/App-Sqitch

## Architecture Decisions

### Core Principles
1. Maintain CLI compatibility with original sqitch
2. Support all database engines (PostgreSQL, MySQL, SQLite, Oracle, Snowflake, Vertica, Exasol, Firebird)
3. Python 3.9+ compatibility
4. Use modern Python idioms and type hints
5. Maintain the same configuration file formats (sqitch.conf, sqitch.plan)

### Python Project Structure
```
sqitch_py/
├── __init__.py
├── cli.py           # Main CLI entry point (using Click)
├── core/
│   ├── __init__.py
│   ├── sqitch.py    # Main Sqitch class (from App::Sqitch)
│   ├── engine.py    # Base engine class
│   ├── plan.py      # Deploy/revert/verify plan management
│   ├── change.py    # Individual change representation
│   ├── target.py    # Target database configuration
│   ├── config.py    # Configuration management
│   └── exceptions.py # Custom exceptions
├── engines/         # Database-specific implementations
│   ├── __init__.py
│   ├── base.py      # Base engine implementation
│   ├── pg.py        # PostgreSQL
│   ├── mysql.py     # MySQL/MariaDB
│   ├── sqlite.py    # SQLite
│   ├── oracle.py    # Oracle
│   ├── snowflake.py # Snowflake
│   └── ...
├── commands/        # CLI commands
│   ├── __init__.py
│   ├── base.py      # Base command class
│   ├── init.py      # Initialize a project
│   ├── add.py       # Add a change
│   ├── deploy.py    # Deploy changes
│   ├── revert.py    # Revert changes
│   ├── verify.py    # Verify changes
│   ├── status.py    # Show status
│   ├── log.py       # Show change log
│   ├── tag.py       # Tag a release
│   ├── bundle.py    # Bundle a project
│   ├── checkout.py  # Checkout a database
│   ├── rebase.py    # Rebase a plan
│   └── show.py      # Show change details
└── utils/
    ├── __init__.py
    ├── git.py       # VCS integration
    ├── template.py  # Template handling
    ├── datetime.py  # Date/time utilities
    └── terminal.py  # Terminal output formatting
```

## Key Perl → Python Translation Guide

### Module Mappings
| Perl Module | Python Equivalent | Notes |
|------------|-------------------|-------|
| `App::Sqitch` | `sqitch_py.core.sqitch.Sqitch` | Main application class |
| `App::Sqitch::Engine` | `sqitch_py.engines.base.Engine` | Abstract base class using ABC |
| `App::Sqitch::Plan` | `sqitch_py.core.plan.Plan` | Plan file parser and manager |
| `App::Sqitch::Change` | `sqitch_py.core.change.Change` | Use dataclass |
| `App::Sqitch::Target` | `sqitch_py.core.target.Target` | Database target configuration |
| `App::Sqitch::Config` | `sqitch_py.core.config.Config` | Use configparser |
| `Template::Tiny` | `jinja2` | Template engine |
| `DBI/DBD::*` | Direct drivers or SQLAlchemy | Database connectivity |
| `Try::Tiny` | Built-in try/except | Exception handling |
| `Moo/Moose` | dataclasses + properties | Object system |
| `Path::Class` | `pathlib.Path` | File path handling |
| `DateTime` | `datetime` + `python-dateutil` | Date/time handling |
| `Digest::SHA` | `hashlib` | SHA hashing |
| `Encode` | Built-in `encode()`/`decode()` | String encoding |
| `URI` | `urllib.parse` | URI handling |
| `IPC::Run3` | `subprocess.run()` | External command execution |

### Python Dependencies
```toml
# Core dependencies
click = "^8.1.0"          # CLI framework
configparser = "^5.3.0"   # Configuration files
python-dateutil = "^2.8.0" # Date parsing
jinja2 = "^3.1.0"        # Templates
colorama = "^0.4.0"      # Cross-platform terminal colors
tabulate = "^0.9.0"      # Table formatting

# Database drivers
psycopg2-binary = "^2.9.0"  # PostgreSQL
pymysql = "^1.0.0"          # MySQL/MariaDB
# cx-Oracle = "^8.3.0"      # Oracle (optional)
# snowflake-connector-python = "^3.0.0" # Snowflake (optional)

# VCS integration
gitpython = "^3.1.0"      # Git operations

# Development dependencies
pytest = "^7.4.0"
pytest-cov = "^4.1.0"
black = "^23.0.0"
mypy = "^1.5.0"
pylint = "^2.17.0"
```

## Translation Priority & Phases

### Phase 1: Core Framework (Week 1)
- [ ] `sqitch_py/core/sqitch.py` - Main application class
- [ ] `sqitch_py/core/config.py` - Configuration loading from sqitch.conf
- [ ] `sqitch_py/core/plan.py` - Parse sqitch.plan files
- [ ] `sqitch_py/core/change.py` - Change representation
- [ ] `sqitch_py/cli.py` - Basic CLI structure with Click
- [ ] `sqitch_py/commands/init.py` - Project initialization

### Phase 2: PostgreSQL MVP (Week 2)
- [ ] `sqitch_py/engines/base.py` - Abstract base engine
- [ ] `sqitch_py/engines/pg.py` - PostgreSQL implementation
- [ ] `sqitch_py/commands/deploy.py` - Deploy command
- [ ] `sqitch_py/commands/revert.py` - Revert command
- [ ] `sqitch_py/commands/verify.py` - Verify command
- [ ] `sqitch_py/commands/status.py` - Status command

### Phase 3: Additional Databases (Week 3)
- [ ] `sqitch_py/engines/mysql.py` - MySQL/MariaDB
- [ ] `sqitch_py/engines/sqlite.py` - SQLite
- [ ] `sqitch_py/commands/log.py` - Change log
- [ ] `sqitch_py/commands/add.py` - Add changes

### Phase 4: Full Feature Parity (Week 4+)
- [ ] Remaining database engines
- [ ] VCS integration (git, hg)
- [ ] Template support
- [ ] Bundle/checkout/rebase commands
- [ ] Internationalization (i18n)

## Implementation Guidelines

### Code Style
1. Use type hints for all function signatures
2. Follow PEP 8 (enforce with Black)
3. Use dataclasses for data structures
4. Implement proper logging with Python's logging module
5. Docstrings for all public functions/classes (Google style)
6. Properties instead of getters/setters

### Error Handling
1. Create custom exception hierarchy in `exceptions.py`
2. Match Perl's error messages for compatibility
3. Use context managers for database connections
4. Proper cleanup in finally blocks

### Testing Strategy
1. Use pytest for all tests
2. Maintain test coverage above 80%
3. Unit tests for each module
4. Integration tests using Docker containers for databases
5. Test against the same test cases as the Perl version

### Performance Considerations
1. Lazy loading of plan files
2. Connection pooling for database operations
3. Batch operations where possible
4. Progress indicators for long operations

## Key Perl Files to Analyze

### Essential Files (Start Here)
1. `lib/App/Sqitch.pm` - Main class, understand overall architecture
2. `lib/App/Sqitch/Plan.pm` - Plan file format and parsing
3. `lib/App/Sqitch/Engine.pm` - Base engine interface
4. `lib/App/Sqitch/Engine/pg.pm` - Complete PostgreSQL implementation
5. `lib/App/Sqitch/Command/deploy.pm` - Deploy logic

### Configuration & Setup
1. `lib/App/Sqitch/Config.pm` - Configuration management
2. `lib/App/Sqitch/Target.pm` - Database target handling
3. `lib/App/Sqitch/Command/init.pm` - Project initialization

### For Each Command
- Check `lib/App/Sqitch/Command/*.pm` for implementation
- Note command-line options and arguments
- Preserve exact behavior for compatibility

## Specific Translation Challenges

### 1. Plan File Parsing
The Perl version uses complex regex. In Python:
- Use a proper parser (consider pyparsing or custom parser)
- Maintain exact compatibility with existing plan files
- Handle line continuations and comments correctly

### 2. Database Metadata Tables
Each engine maintains metadata tables (sqitch.changes, sqitch.tags, etc.)
- Keep exact same schema
- Same table/column names
- Compatible data formats

### 3. Script Execution
Perl uses complex IPC for running SQL scripts:
- Use subprocess.run() with proper encoding
- Handle stdin/stdout/stderr correctly
- Maintain same error reporting

### 4. Template Variables
Perl uses Template::Tiny syntax:
- Map to Jinja2 equivalents
- Maintain backward compatibility
- Same variable names

## Questions for Implementation Decisions

1. **CLI Framework**: Click (more Pythonic) or argparse (stdlib)?
   - Decision: Use Click for better UX and easier testing

2. **Database Abstraction**: SQLAlchemy or direct drivers?
   - Decision: Direct drivers for performance and control

3. **Configuration Format**: Keep INI or move to TOML?
   - Decision: Keep INI for compatibility, consider TOML for v2

4. **Async Support**: Add async operations for database work?
   - Decision: Start synchronous, consider async for v2

5. **Distribution**: pip, conda, or standalone executable?
   - Decision: pip primary, consider others based on demand

## Current Status Tracker

### Completed
- [x] Project structure created
- [x] CLAUDE.md written

### In Progress
- [ ] Analyzing Perl codebase structure
- [ ] Setting up Python package skeleton

### Next Steps
1. Analyze `lib/App/Sqitch.pm` thoroughly
2. Create `sqitch_py/core/sqitch.py` with class structure
3. Implement configuration loading
4. Create basic CLI with `--version` and `--help`

## Notes for Claude Code

### When Translating Perl to Python:
1. Look for Perl idioms and find Python equivalents
2. Check for implicit returns (Perl) vs explicit returns (Python)
3. Handle Perl's `$_` (implicit variable) explicitly in Python
4. Convert Perl regexes to Python's `re` module syntax
5. Map Perl's `die` to raising exceptions
6. Convert Perl's `warn` to logging.warning()

### File Encoding:
- Sqitch uses UTF-8 throughout
- Always open files with `encoding='utf-8'`
- Handle BOM markers if present

### Testing Against Perl Version:
1. Run Perl sqitch with same inputs
2. Compare outputs character by character
3. Ensure database state is identical
4. Check exit codes match

## Commands Quick Reference

### Essential Commands to Implement First:
```bash
sqitch init          # Initialize a project
sqitch add <change>  # Add a new change
sqitch deploy        # Deploy changes
sqitch revert        # Revert changes
sqitch verify        # Verify deployed changes
sqitch status        # Show deployment status
sqitch log           # Show change history
```

### Each Command Should:
1. Parse same command-line arguments
2. Produce same output format
3. Return same exit codes
4. Handle same environment variables

## Resources

- [Sqitch Documentation](https://sqitch.org/docs/)
- [Sqitch GitHub](https://github.com/sqitchers/sqitch)
- [MetaCPAN Source](https://metacpan.org/dist/App-Sqitch)
- [Sqitch Plan Spec](https://sqitch.org/docs/manual/sqitch-plan/)
- [Sqitch Configuration](https://sqitch.org/docs/manual/sqitch-config/)