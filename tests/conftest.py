"""Shared test fixtures and configuration for sqlitch tests."""

import os
import tempfile
from pathlib import Path
from typing import Dict, Generator
from unittest.mock import Mock

import pytest

from sqlitch.core.config import Config
from sqlitch.core.plan import Change, Plan, Tag
from sqlitch.core.sqitch import Sqitch
from sqlitch.core.target import Target
from sqlitch.core.types import URI


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_project_dir(temp_dir: Path) -> Path:
    """Create a temporary project directory with basic structure."""
    project_dir = temp_dir / "test_project"
    project_dir.mkdir()

    # Create basic directory structure
    (project_dir / "deploy").mkdir()
    (project_dir / "revert").mkdir()
    (project_dir / "verify").mkdir()

    return project_dir


@pytest.fixture
def sample_config_content() -> str:
    """Sample sqitch configuration content."""
    return """[core]
    engine = pg
    top_dir = .
    plan_file = sqitch.plan

[engine "pg"]
    target = db:pg://user@localhost/test_db
    registry = sqitch
    client = psql

[user]
    name = Test User
    email = test@example.com
"""


@pytest.fixture
def sample_plan_content() -> str:
    """Sample sqitch plan content."""
    return """%syntax-version=1.0.0
%project=test_project
%uri=https://github.com/example/test_project

initial_schema 2023-01-15T10:30:00Z Test User <test@example.com> # Initial schema
users [initial_schema] 2023-01-16T14:20:00Z Test User <test@example.com> # Add users table
@v1.0 2023-01-20T09:00:00Z Test User <test@example.com> # Release v1.0
posts [users] 2023-01-25T11:15:00Z Jane Smith <jane@example.com> # Add posts table
"""


@pytest.fixture
def config_file(temp_project_dir: Path, sample_config_content: str) -> Path:
    """Create a temporary config file."""
    config_file = temp_project_dir / "sqitch.conf"
    config_file.write_text(sample_config_content)
    return config_file


@pytest.fixture
def plan_file(temp_project_dir: Path, sample_plan_content: str) -> Path:
    """Create a temporary plan file."""
    plan_file = temp_project_dir / "sqitch.plan"
    plan_file.write_text(sample_plan_content)
    return plan_file


@pytest.fixture
def mock_config() -> Config:
    """Create a mock configuration object."""
    config = Mock(spec=Config)
    config.get.return_value = None
    config.get_target.return_value = Target(
        name="test", uri=URI("db:pg://user@localhost/test_db"), registry="sqitch"
    )
    return config


@pytest.fixture
def mock_sqitch(mock_config: Config) -> Sqitch:
    """Create a mock Sqitch instance."""
    sqitch = Mock(spec=Sqitch)
    sqitch.config = mock_config
    sqitch.verbosity = 0
    sqitch.user_name = "Test User"
    sqitch.user_email = "test@example.com"
    return sqitch


@pytest.fixture
def sample_change() -> Change:
    """Create a sample change object."""
    from datetime import datetime

    return Change(
        name="test_change",
        note="Test change",
        tags=[],
        dependencies=[],
        conflicts=[],
        timestamp=datetime(2023, 1, 15, 10, 30, 0),
        planner_name="Test User",
        planner_email="test@example.com",
    )


@pytest.fixture
def sample_tag() -> Tag:
    """Create a sample tag object."""
    from datetime import datetime

    return Tag(
        name="v1.0",
        note="Release v1.0",
        change="test_change",
        timestamp=datetime(2023, 1, 20, 9, 0, 0),
        planner_name="Test User",
        planner_email="test@example.com",
    )


@pytest.fixture
def sample_plan(temp_project_dir: Path, plan_file: Path) -> Plan:
    """Create a sample plan object."""
    return Plan.from_file(plan_file)


@pytest.fixture
def mock_engine():
    """Create a mock database engine."""
    from sqlitch.engines.base import Engine

    engine = Mock(spec=Engine)
    engine.initialize_registry.return_value = None
    engine.deploy_change.return_value = None
    engine.revert_change.return_value = None
    engine.verify_change.return_value = True
    engine.current_state.return_value = None
    engine.deployed_changes.return_value = []
    return engine


@pytest.fixture
def mock_database_connection():
    """Create a mock database connection."""
    connection = Mock()
    connection.execute.return_value = None
    connection.fetchone.return_value = None
    connection.fetchall.return_value = []
    connection.commit.return_value = None
    connection.rollback.return_value = None
    connection.close.return_value = None
    return connection


@pytest.fixture(autouse=True)
def clean_environment():
    """Clean environment variables before each test."""
    # Store original environment
    original_env = dict(os.environ)

    # Clean sqlitch-related environment variables
    env_vars_to_clean = [
        "SQITCH_CONFIG",
        "SQITCH_USER_NAME",
        "SQITCH_USER_EMAIL",
        "SQITCH_TARGET",
        "SQITCH_ENGINE",
        "PGUSER",
        "PGPASSWORD",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "MYSQL_USER",
        "MYSQL_PASSWORD",
        "MYSQL_HOST",
        "MYSQL_PORT",
        "ORACLE_USER",
        "ORACLE_PASSWORD",
        "ORACLE_HOST",
        "ORACLE_PORT",
    ]

    for var in env_vars_to_clean:
        os.environ.pop(var, None)

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def isolated_filesystem(temp_dir: Path):
    """Change to a temporary directory for the test."""
    original_cwd = Path.cwd()
    os.chdir(temp_dir)
    try:
        yield temp_dir
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def mock_git_repo(temp_project_dir: Path):
    """Create a mock git repository."""
    git_dir = temp_project_dir / ".git"
    git_dir.mkdir()

    # Create basic git config
    config_file = git_dir / "config"
    config_file.write_text(
        """[user]
    name = Test User
    email = test@example.com
"""
    )

    return git_dir


@pytest.fixture(scope="session")
def docker_available() -> bool:
    """Check if Docker is available for integration tests."""
    try:
        import subprocess

        result = subprocess.run(["docker", "--version"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


@pytest.fixture
def skip_if_no_docker(docker_available: bool):
    """Skip test if Docker is not available."""
    if not docker_available:
        pytest.skip("Docker not available")


# Database-specific fixtures for integration tests
@pytest.fixture(scope="session")
def postgresql_container(docker_available: bool):
    """Start PostgreSQL container for integration tests."""
    if not docker_available:
        pytest.skip("Docker not available")

    try:
        import docker

        client = docker.from_env()

        # Start PostgreSQL container
        container = client.containers.run(
            "postgres:13",
            environment={"POSTGRES_PASSWORD": "test", "POSTGRES_DB": "sqlitch_test"},
            ports={"5432/tcp": None},
            detach=True,
            remove=True,
        )

        # Wait for container to be ready
        import time

        time.sleep(5)

        yield container

        # Cleanup
        container.stop()

    except ImportError:
        pytest.skip("Docker Python library not available")
    except Exception as e:
        pytest.skip(f"Could not start PostgreSQL container: {e}")


@pytest.fixture(scope="session")
def mysql_container(docker_available: bool):
    """Start MySQL container for integration tests."""
    if not docker_available:
        pytest.skip("Docker not available")

    try:
        import docker

        client = docker.from_env()

        container = client.containers.run(
            "mysql:8.0",
            environment={
                "MYSQL_ROOT_PASSWORD": "test",
                "MYSQL_DATABASE": "sqlitch_test",
            },
            ports={"3306/tcp": None},
            detach=True,
            remove=True,
        )

        # Wait for MySQL to be ready
        import time

        time.sleep(10)

        yield container

        container.stop()

    except ImportError:
        pytest.skip("Docker Python library not available")
    except Exception as e:
        pytest.skip(f"Could not start MySQL container: {e}")


# Performance testing fixtures
@pytest.fixture
def performance_timer():
    """Timer fixture for performance tests."""
    import time

    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.perf_counter()

        def stop(self):
            self.end_time = time.perf_counter()

        @property
        def elapsed(self) -> float:
            if self.start_time is None or self.end_time is None:
                return 0.0
            return self.end_time - self.start_time

    return Timer()


# Compatibility testing fixtures
@pytest.fixture
def perl_sqitch_available() -> bool:
    """Check if Perl sqitch is available for compatibility tests."""
    try:
        import subprocess

        result = subprocess.run(["sqitch", "--version"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


@pytest.fixture
def skip_if_no_perl_sqitch(perl_sqitch_available: bool):
    """Skip test if Perl sqitch is not available."""
    if not perl_sqitch_available:
        pytest.skip("Perl sqitch not available")


# Test data fixtures
@pytest.fixture
def sample_sql_files(temp_project_dir: Path) -> Dict[str, Path]:
    """Create sample SQL files for testing."""
    files = {}

    # Deploy script
    deploy_file = temp_project_dir / "deploy" / "test_change.sql"
    deploy_file.write_text(
        """-- Deploy test_change

BEGIN;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL
);

COMMIT;
"""
    )
    files["deploy"] = deploy_file

    # Revert script
    revert_file = temp_project_dir / "revert" / "test_change.sql"
    revert_file.write_text(
        """-- Revert test_change

BEGIN;

DROP TABLE users;

COMMIT;
"""
    )
    files["revert"] = revert_file

    # Verify script
    verify_file = temp_project_dir / "verify" / "test_change.sql"
    verify_file.write_text(
        """-- Verify test_change

SELECT id, name, email FROM users WHERE FALSE;
"""
    )
    files["verify"] = verify_file

    return files


# Logging fixtures
@pytest.fixture
def capture_logs():
    """Capture log output for testing."""
    import logging
    from io import StringIO

    log_capture = StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.DEBUG)

    # Add handler to sqlitch loggers
    loggers = [
        logging.getLogger("sqlitch"),
        logging.getLogger("sqlitch.core"),
        logging.getLogger("sqlitch.engines"),
        logging.getLogger("sqlitch.commands"),
    ]

    for logger in loggers:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    yield log_capture

    # Cleanup
    for logger in loggers:
        logger.removeHandler(handler)


# CLI testing fixtures
@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    from click.testing import CliRunner

    return CliRunner()


@pytest.fixture
def mock_cli_context():
    """Mock CLI context for testing commands."""
    import click

    ctx = Mock(spec=click.Context)
    ctx.obj = {
        "verbosity": 0,
        "config_files": None,
    }
    return ctx
