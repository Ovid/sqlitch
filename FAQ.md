# Frequently Asked Questions (FAQ)

## General Questions

### What is Sqlitch?

Sqlitch (pronounced "sklitch") is a Python port of the popular [Sqitch](https://sqitch.org/) database change management application. It provides a framework-agnostic approach to database schema management with the convenience and ecosystem of Python.

### How is Sqlitch different from the original Perl Sqitch?

Sqlitch is designed to be 100% compatible with Perl [Sqitch](https://sqitch.org) while offering the benefits of the Python ecosystem:

- **Drop-in replacement**: Uses identical configuration files, plan files, and command syntax
- **Python ecosystem**: Integrates seamlessly with Python applications and tooling
- **Modern features**: Type hints, async support, comprehensive error handling
- **Cross-platform**: Native Windows support without requiring Perl installation
- **Performance**: Often faster startup times and better resource usage

### Is Sqlitch production ready?

Not yet. We're pretty confident it does what is says, but not enough to make a production-ready claim.

## File Naming and Compatibility

### Why do files use `sqitch.plan` instead of `sqlitch.plan`?

This is intentional for **100% compatibility** with the original Perl Sqitch. All file names and formats remain identical:

- **Plan files**: `sqitch.plan` (not `sqlitch.plan`)
- **Configuration files**: `sqitch.conf` (not `sqlitch.conf`)
- **SQL script directories**: `deploy/`, `revert/`, `verify/`
- **Registry tables**: Use the same schema and naming as Perl Sqitch

This ensures you can:
- Use existing Sqitch projects without modification
- Switch between Perl Sqitch and Sqlitch seamlessly
- Share projects with teams using either implementation
- Migrate gradually without breaking existing workflows

### Can I use existing Sqitch projects with Sqlitch?

**Yes, absolutely!** Sqlitch is designed as a drop-in replacement. Your existing:

- `sqitch.plan` files work without modification
- `sqitch.conf` configuration files are fully compatible
- SQL scripts in `deploy/`, `revert/`, and `verify/` directories work as-is
- Database registry tables are identical

Simply install Sqlitch and run the same commands you're used to.

### What if I have both Perl Sqitch and Sqlitch installed?

Both can coexist peacefully:

- **Perl Sqitch**: Command is `sqitch`
- **Python Sqlitch**: Command is `sqlitch` (note the extra 'l')

They use the same file formats, so you can use either tool on the same project.

## Installation and Setup

### How do I install Sqlitch?

```bash
# From PyPI (recommended)
pip install sqlitch

# From source
git clone https://github.com/Ovid/sqlitch.git
cd sqlitch
pip install -e .
```

### What are the system requirements?

- **Python**: 3.9 or higher
- **Operating System**: Windows 10+, macOS 10.15+, or Linux
- **Database clients**: Install the appropriate client for your database (psql, mysql, sqlite3, etc.)

### Do I need to install Perl to use Sqlitch?

**No!** Sqlitch is a pure Python implementation. You don't need Perl, CPAN, or any Perl dependencies.

## Database Support

### Which databases does Sqlitch support?

Sqlitch supports 8 database engines:

- **PostgreSQL** (`pg`) - Full support
- **MySQL** (`mysql`) - Full support  
- **SQLite** (`sqlite`) - Full support
- **Oracle** (`oracle`) - Full support
- **Snowflake** (`snowflake`) - Full support
- **Vertica** (`vertica`) - Full support
- **Exasol** (`exasol`) - Full support
- **Firebird** (`firebird`) - Full support

### How do I configure database connections?

Use the same configuration format as Perl Sqitch:

```ini
# sqitch.conf
[core]
    engine = pg
    
[engine "pg"]
    target = db:pg://user:password@localhost/mydb
    client = psql
```

Or use environment variables:
```bash
export SQITCH_TARGET="db:pg://user:password@localhost/mydb"
```

### Can I use database-specific features?

Yes! Sqlitch encourages using native database features. Write your changes in the native SQL dialect of your target database.

## Commands and Usage

### Are the commands the same as Perl Sqitch?

Yes, the command interface is identical:

```bash
# Initialize a new project
sqlitch init myproject --engine pg

# Add a new change
sqlitch add users --note "Add users table"

# Deploy changes
sqlitch deploy

# Revert changes
sqlitch revert --to @HEAD^

# Check status
sqlitch status
```

### How do I migrate from Perl Sqitch?

Migration is seamless:

1. Install Sqlitch: `pip install sqlitch`
2. Navigate to your existing Sqitch project
3. Run Sqlitch commands: `sqlitch status`, `sqlitch deploy`, etc.

No file modifications needed!

### Can I use Sqlitch in CI/CD pipelines?

Absolutely! Sqlitch is designed for automation:

```yaml
# GitHub Actions example
- name: Deploy database changes
  run: |
    sqlitch deploy --target $DATABASE_URL
    sqlitch verify
```

## Configuration and Customization

### How do I configure multiple environments?

Use named targets in your `sqitch.conf`:

```ini
[target "development"]
    uri = db:pg://localhost/myapp_dev

[target "staging"] 
    uri = db:pg://staging.example.com/myapp

[target "production"]
    uri = db:pg://prod.example.com/myapp
```

Then deploy to specific environments:
```bash
sqlitch deploy --target production
```

### Can I customize file locations?

Yes, configure paths in `sqitch.conf`:

```ini
[core]
    top_dir = database
    plan_file = database/changes.plan
    
[engine "pg"]
    deploy_dir = database/migrations/deploy
    revert_dir = database/migrations/revert
    verify_dir = database/migrations/verify
```

### How do I handle database passwords securely?

Several secure options:

1. **Environment variables**:
   ```bash
   export SQITCH_PASSWORD="secret"
   ```

2. **URI without password** (prompt when needed):
   ```ini
   target = db:pg://user@localhost/mydb
   ```

3. **External credential management** (AWS Secrets Manager, etc.)

## Troubleshooting

### Sqlitch can't find my database client

Ensure the database client is installed and in your PATH:

```bash
# PostgreSQL
which psql

# MySQL  
which mysql

# SQLite
which sqlite3
```

Or specify the full path in configuration:
```ini
[engine "pg"]
    client = /usr/local/bin/psql
```

### I'm getting "No engine specified" errors

This usually means your project isn't initialized or the configuration is missing:

1. **Initialize the project**:
   ```bash
   sqlitch init myproject --engine pg
   ```

2. **Or add engine to existing config**:
   ```ini
   [core]
       engine = pg
   ```

### Changes aren't being deployed in the right order

Sqlitch uses dependency resolution. Declare dependencies in your plan file:

```
# sqitch.plan
users 2023-01-01T12:00:00Z Alice <alice@example.com> # Add users table
posts [users] 2023-01-01T13:00:00Z Alice <alice@example.com> # Add posts table
```

The `[users]` dependency ensures `users` is deployed before `posts`.

### How do I get help with specific commands?

Use the built-in help system:

```bash
# General help
sqlitch help

# Command-specific help
sqlitch help deploy
sqlitch help add
sqlitch help init
```

## Development and Contributing

### How can I contribute to Sqlitch?

We welcome contributions! See our [contributing guidelines](CONTRIBUTING.md) for:

- Setting up the development environment
- Running tests
- Code style guidelines
- Submitting pull requests

### How do I report bugs?

Please [open an issue](https://github.com/Ovid/sqlitch/issues) with:

- Sqlitch version (`sqlitch --version`)
- Operating system and Python version
- Complete error message
- Steps to reproduce
- Expected vs actual behavior

### Is there a roadmap?

Yes! Check our [project roadmap](https://github.com/Ovid/sqlitch/projects) for upcoming features and improvements.

## Performance and Scaling

### Can Sqlitch handle large databases?

Yes! Sqlitch is designed for enterprise use:

- **Streaming operations**: Processes large result sets efficiently
- **Connection pooling**: Reuses database connections when possible
- **Incremental deployments**: Only deploys changes that haven't been applied
- **Parallel verification**: Can verify multiple changes concurrently

## Integration and Ecosystem

### Does Sqlitch integrate with Python ORMs?

Sqlitch is ORM-agnostic by design.

### Can I use Sqlitch with Docker?

Absolutely! Sqlitch works great in containerized environments:

```dockerfile
FROM python:3.11-slim
RUN pip install sqlitch
COPY . /app
WORKDIR /app
CMD ["sqlitch", "deploy"]
```

### How does Sqlitch work with version control?

Sqlitch integrates seamlessly with Git:

- **Automatic user detection** from Git configuration
- **Branch-aware operations** for feature branch workflows
- **Merge conflict resolution** for plan files
- **Tag-based releases** for versioned deployments

---

## Still have questions?

- 📖 **Documentation**: [Full documentation](https://sqlitch.readthedocs.io/)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/Ovid/sqlitch/discussions)
- 🐛 **Issues**: [Bug reports and feature requests](https://github.com/Ovid/sqlitch/issues)
- 📧 **Email**: [Contact the maintainers](mailto:sqlitch@example.com)

*This FAQ is regularly updated. Last updated: December 2024*