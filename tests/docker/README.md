# Docker Integration Tests

This directory contains Docker-based integration tests for sqlitch database engines. These tests use real database containers to verify full deployment cycles, registry creation, change deployment, revert, and verification.

## Overview

The Docker integration tests provide comprehensive testing of:

- **Registry Creation**: Verifying that sqitch registry tables are created correctly
- **Full Deployment Cycles**: Testing complete deploy → verify → revert workflows
- **Database-Specific Features**: Testing engine-specific SQL syntax and capabilities
- **Error Handling**: Verifying proper transaction rollback and error recovery
- **Concurrency**: Testing behavior with multiple connections
- **Performance**: Ensuring reasonable performance with larger SQL scripts

## Test Files

- `test_postgresql_docker.py`: PostgreSQL Docker integration tests
- `test_mysql_docker.py`: MySQL Docker integration tests  
- `run_docker_tests.py`: Test runner script for Docker-based tests

**Note**: SQLite integration tests are located in `tests/integration/test_sqlite_engine_integration.py` since SQLite uses file-based databases and doesn't require Docker containers.

## Prerequisites

### Docker and Docker Compose

Ensure Docker and Docker Compose are installed and running:

```bash
docker --version
docker-compose --version
```

### Python Dependencies

Install the required Python packages:

```bash
pip install docker pytest pytest-cov
```

## Running Tests

### Using Docker Compose

The easiest way to run all Docker integration tests:

```bash
# Run all Docker integration tests
docker-compose --profile test up --build

# Run tests for specific database engine
docker-compose up -d postgres
python tests/docker/run_docker_tests.py --engine postgresql

docker-compose up -d mysql  
python tests/docker/run_docker_tests.py --engine mysql

# Note: SQLite tests are in tests/integration/ since SQLite doesn't need Docker
```

### Using the Test Runner Script

```bash
# Run all Docker integration tests
python tests/docker/run_docker_tests.py

# Run tests for specific engine
python tests/docker/run_docker_tests.py --engine postgresql
python tests/docker/run_docker_tests.py --engine mysql

# SQLite tests are run with the regular integration tests:
python -m pytest tests/integration/test_sqlite_engine_integration.py -v

# Run with verbose output
python tests/docker/run_docker_tests.py --verbose

# List available engines
python tests/docker/run_docker_tests.py --list-engines
```

### Using pytest Directly

```bash
# Start database services first
docker-compose up -d postgres mysql

# Run specific test file
python -m pytest tests/docker/test_postgresql_docker.py -v
python -m pytest tests/docker/test_mysql_docker.py -v
python -m pytest tests/docker/test_sqlite_docker.py -v

# Run all Docker integration tests
python -m pytest tests/docker/ -v -m integration
```

## Test Configuration

### Environment Variables

The tests use these environment variables for database connections:

```bash
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=sqlitch
POSTGRES_PASSWORD=test
POSTGRES_DB=sqlitch_test

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=sqlitch
MYSQL_PASSWORD=test
MYSQL_DB=sqlitch_test
```

### Docker Services

The `docker-compose.yml` file defines these services for testing:

- **postgres**: PostgreSQL 13 container with test database
- **mysql**: MySQL 8.0 container with test database
- **oracle**: Oracle XE container (optional, use `--profile oracle`)

### Test Fixtures

Database initialization scripts are in `tests/fixtures/`:

- `tests/fixtures/postgresql/init.sql`: PostgreSQL setup
- `tests/fixtures/mysql/init.sql`: MySQL setup

## Test Structure

Each database engine test file follows this structure:

### Test Classes

- `TestXXXDockerIntegration`: Main integration test class

### Test Fixtures

- `xxx_uri`: Database connection URI from Docker container
- `xxx_target`: sqlitch Target object for testing
- `test_plan`: Mock Plan object with sample changes and SQL files

### Test Methods

- `test_registry_creation_and_initialization`: Registry setup
- `test_full_deployment_cycle`: Complete deploy/verify/revert cycle
- `test_xxx_specific_features`: Database-specific functionality
- `test_error_handling_and_rollback`: Error scenarios
- `test_concurrent_deployments`: Multi-connection behavior
- `test_performance_with_large_changes`: Performance testing

## Database-Specific Features Tested

### PostgreSQL

- Schema isolation (sqitch schema vs public schema)
- PostgreSQL-specific SQL syntax and data types
- Transaction isolation levels
- Connection parameter parsing
- Registry table creation with proper constraints

### MySQL

- MySQL-specific features (JSON, AUTO_INCREMENT, etc.)
- Character set and collation handling (utf8mb4)
- MySQL storage engines (InnoDB)
- Transaction isolation and locking
- Registry table creation with MySQL syntax

### SQLite

SQLite integration tests are located in `tests/integration/test_sqlite_engine_integration.py` since SQLite:
- Uses file-based databases that don't require containers
- Creates database files directly on the filesystem
- Doesn't need network connectivity or service orchestration
- Is already comprehensively tested in the existing integration test suite

## Troubleshooting

### Docker Issues

```bash
# Check if Docker services are running
docker-compose ps

# View service logs
docker-compose logs postgres
docker-compose logs mysql

# Restart services
docker-compose restart postgres mysql

# Clean up and restart
docker-compose down
docker-compose up -d postgres mysql
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
docker-compose exec postgres psql -U sqlitch -d sqlitch_test -c "SELECT 1;"

# Test MySQL connection  
docker-compose exec mysql mysql -u sqlitch -ptest sqlitch_test -e "SELECT 1;"
```

### Test Failures

```bash
# Run tests with more verbose output
python tests/docker/run_docker_tests.py --verbose

# Run specific test method
python -m pytest tests/docker/test_postgresql_docker.py::TestPostgreSQLDockerIntegration::test_registry_creation_and_initialization -v

# Run with pdb on failure
python -m pytest tests/docker/ --pdb
```

## Performance Considerations

- Tests create and destroy database objects, which can be slow
- Use `pytest-xdist` for parallel test execution if needed
- Consider using database templates or snapshots for faster test setup
- Monitor Docker resource usage during test runs

## Security Notes

- Test databases use simple passwords for convenience
- Test data is automatically cleaned up after each test
- Docker containers are isolated from production systems
- Never run these tests against production databases

## Contributing

When adding new Docker integration tests:

1. Follow the existing test structure and naming conventions
2. Ensure proper cleanup in test fixtures and teardown
3. Test both success and failure scenarios
4. Include database-specific feature testing
5. Add appropriate documentation and comments
6. Verify tests pass in CI/CD environment