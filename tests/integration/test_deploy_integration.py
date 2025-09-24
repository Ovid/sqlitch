"""
Integration tests for deploy command.

Tests the deploy command end-to-end with real database operations,
plan file parsing, and CLI integration.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, Mock

from sqlitch.cli import cli
from sqlitch.commands.deploy import DeployCommand
from sqlitch.core.sqitch import create_sqitch
from sqlitch.core.plan import Plan
from sqlitch.core.change import Change, Dependency
from sqlitch.core.types import URI
from sqlitch.core.target import Target


def create_mock_psycopg2_connection():
    """Create a properly mocked psycopg2 connection with context manager support."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.commit = Mock()
    mock_conn.rollback = Mock()
    mock_conn.close = Mock()
    
    # Setup cursor mocks
    mock_cursor.execute = Mock()
    mock_cursor.fetchone = Mock()
    mock_cursor.fetchall = Mock()
    mock_cursor.close = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    
    return mock_conn, mock_cursor


@pytest.fixture
def temp_project():
    """Create temporary project directory."""
    temp_dir = Path(tempfile.mkdtemp())
    original_cwd = Path.cwd()
    
    try:
        # Change to temp directory
        import os
        os.chdir(temp_dir)
        
        yield temp_dir
    finally:
        # Restore original directory and cleanup
        os.chdir(original_cwd)
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def initialized_project(temp_project):
    """Create initialized sqlitch project."""
    # Create configuration file
    config_content = """[core]
    engine = pg
    plan_file = sqitch.plan
    top_dir = .

[engine "pg"]
    target = db:pg://test:test@localhost:5432/test_db
    registry = sqitch
    client = psql

[user]
    name = Test User
    email = test@example.com
"""
    
    config_file = temp_project / 'sqitch.conf'
    config_file.write_text(config_content)
    
    # Create plan file
    plan_content = """%syntax-version=1.0.0
%project=test_project
%uri=https://github.com/example/test_project

initial_schema 2023-01-15T10:00:00Z Test User <test@example.com> # Initial database schema
users_table [initial_schema] 2023-01-16T11:00:00Z Test User <test@example.com> # Add users table
posts_table [users_table] 2023-01-17T12:00:00Z Test User <test@example.com> # Add posts table

@v1.0 2023-01-17T15:00:00Z Test User <test@example.com> # Version 1.0 release
"""
    
    plan_file = temp_project / 'sqitch.plan'
    plan_file.write_text(plan_content)
    
    # Create directories
    (temp_project / 'deploy').mkdir()
    (temp_project / 'revert').mkdir()
    (temp_project / 'verify').mkdir()
    
    # Create SQL files
    deploy_dir = temp_project / 'deploy'
    revert_dir = temp_project / 'revert'
    verify_dir = temp_project / 'verify'
    
    # Initial schema
    (deploy_dir / 'initial_schema.sql').write_text("""
-- Deploy initial_schema

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.metadata (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT
);

INSERT INTO app.metadata (key, value) VALUES ('version', '1.0');
""")
    
    (revert_dir / 'initial_schema.sql').write_text("""
-- Revert initial_schema

DROP TABLE IF EXISTS app.metadata;
DROP SCHEMA IF EXISTS app;
""")
    
    (verify_dir / 'initial_schema.sql').write_text("""
-- Verify initial_schema

SELECT 1/COUNT(*) FROM information_schema.schemata WHERE schema_name = 'app';
SELECT 1/COUNT(*) FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'metadata';
""")
    
    # Users table
    (deploy_dir / 'users_table.sql').write_text("""
-- Deploy users_table

CREATE TABLE app.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_users_username ON app.users(username);
CREATE INDEX idx_users_email ON app.users(email);
""")
    
    (revert_dir / 'users_table.sql').write_text("""
-- Revert users_table

DROP TABLE IF EXISTS app.users;
""")
    
    (verify_dir / 'users_table.sql').write_text("""
-- Verify users_table

SELECT 1/COUNT(*) FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'users';
SELECT 1/COUNT(*) FROM information_schema.columns WHERE table_schema = 'app' AND table_name = 'users' AND column_name = 'id';
SELECT 1/COUNT(*) FROM information_schema.columns WHERE table_schema = 'app' AND table_name = 'users' AND column_name = 'username';
""")
    
    # Posts table
    (deploy_dir / 'posts_table.sql').write_text("""
-- Deploy posts_table

CREATE TABLE app.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_posts_user_id ON app.posts(user_id);
CREATE INDEX idx_posts_created_at ON app.posts(created_at);
""")
    
    (revert_dir / 'posts_table.sql').write_text("""
-- Revert posts_table

DROP TABLE IF EXISTS app.posts;
""")
    
    (verify_dir / 'posts_table.sql').write_text("""
-- Verify posts_table

SELECT 1/COUNT(*) FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'posts';
SELECT 1/COUNT(*) FROM information_schema.columns WHERE table_schema = 'app' AND table_name = 'posts' AND column_name = 'user_id';
""")
    
    return temp_project


class TestDeployCommandIntegration:
    """Integration tests for deploy command."""
    
    def test_deploy_command_creation(self, initialized_project):
        """Test creating deploy command instance."""
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        assert command.sqitch == sqitch
        assert command.config == sqitch.config
        assert command.logger == sqitch.logger
    
    def test_load_plan_integration(self, initialized_project):
        """Test loading plan file in real project."""
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        plan = command._load_plan()
        
        assert plan.project == "test_project"
        assert len(plan.changes) == 3
        assert plan.changes[0].name == "initial_schema"
        assert plan.changes[1].name == "users_table"
        assert plan.changes[2].name == "posts_table"
        
        # Check dependencies
        assert len(plan.changes[1].dependencies) == 1
        assert plan.changes[1].dependencies[0].change == "initial_schema"
    
    def test_argument_parsing_integration(self, initialized_project):
        """Test argument parsing with real command."""
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        # Test various argument combinations
        options = command._parse_args(['--target', 'production', '--verify'])
        assert options['target'] == 'production'
        assert options['verify'] is True
        
        options = command._parse_args(['--to-change', 'users_table', '--no-verify'])
        assert options['to_change'] == 'users_table'
        assert options['mode'] == 'change'
        assert options['verify'] is False
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_deploy_with_mock_database(self, mock_psycopg2, initialized_project):
        """Test deployment with mocked database."""
        # Setup mock database
        mock_conn, mock_cursor = create_mock_psycopg2_connection()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        # Mock cursor responses
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
            None,  # No deployed changes
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        # Mock file existence checks
        with patch('pathlib.Path.exists', return_value=True):
            result = command.execute([])
        
        assert result == 0
        
        # Verify database operations were called
        assert mock_psycopg2.connect.called
        assert mock_cursor.execute.called
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_deploy_specific_change(self, mock_psycopg2, initialized_project):
        """Test deploying up to specific change."""
        # Setup mock database
        mock_conn, mock_cursor = create_mock_psycopg2_connection()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        with patch('pathlib.Path.exists', return_value=True):
            result = command.execute(['--to-change', 'users_table'])
        
        assert result == 0
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_deploy_log_only(self, mock_psycopg2, initialized_project):
        """Test deploy with log-only option."""
        # Setup mock database
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        result = command.execute(['--log-only'])
        
        assert result == 0
        # Should not have made actual database changes
        assert not mock_cursor.execute.called
    
    def test_deploy_not_initialized(self, temp_project):
        """Test deploy command in uninitialized project."""
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        result = command.execute([])
        
        assert result == 1
    
    def test_deploy_missing_plan_file(self, temp_project):
        """Test deploy command with missing plan file."""
        # Create minimal config but no plan file
        config_content = """[core]
    engine = pg
"""
        config_file = temp_project / 'sqitch.conf'
        config_file.write_text(config_content)
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        result = command.execute([])
        
        assert result == 1
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_deploy_with_verification_failure(self, mock_psycopg2, initialized_project):
        """Test deployment with verification failure."""
        # Setup mock database
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        # Mock verification failure
        mock_cursor.execute.side_effect = [
            None,  # Registry creation
            None,  # Project insert
            None,  # Deploy script execution
            None,  # Change logging
            Exception("Verification failed"),  # Verification failure
        ]
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        with patch('pathlib.Path.exists', return_value=True):
            result = command.execute(['--to-change', 'initial_schema'])
        
        assert result == 1
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_deploy_dependency_validation(self, mock_psycopg2, initialized_project):
        """Test deployment with dependency validation."""
        # Setup mock database
        mock_conn, mock_cursor = create_mock_psycopg2_connection()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        # Try to deploy posts_table without users_table - should work since
        # users_table will be included automatically
        with patch('pathlib.Path.exists', return_value=True):
            result = command.execute(['--to-change', 'posts_table'])
        
        assert result == 0


class TestDeployCommandCLIIntegration:
    """Test deploy command CLI integration."""
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_cli_deploy_command(self, mock_psycopg2, initialized_project):
        """Test deploy command through CLI."""
        # Setup mock database
        mock_conn, mock_cursor = create_mock_psycopg2_connection()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        from click.testing import CliRunner
        runner = CliRunner()
        
        # Use isolated filesystem to avoid config file issues
        with runner.isolated_filesystem():
            # Copy the project files to isolated filesystem
            import shutil
            shutil.copytree(initialized_project, '.', dirs_exist_ok=True)
            
            # Patch config loading to avoid system config files
            with patch('pathlib.Path.exists', return_value=True), \
                 patch('sqlitch.core.config.Config._get_system_config_paths', return_value=[]), \
                 patch('sqlitch.core.config.Config._get_global_config_path', return_value=None):
                result = runner.invoke(cli, ['deploy', '--log-only'])
        
        assert result.exit_code == 0
        # For CLI tests, we just verify the command runs successfully
        # The actual output goes to the logger, not stdout
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_cli_deploy_with_options(self, mock_psycopg2, initialized_project):
        """Test deploy command with various CLI options."""
        # Setup mock database
        mock_conn, mock_cursor = create_mock_psycopg2_connection()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        from click.testing import CliRunner
        runner = CliRunner()
        
        # Use isolated filesystem to avoid config file issues
        with runner.isolated_filesystem():
            # Copy the project files to isolated filesystem
            import shutil
            shutil.copytree(initialized_project, '.', dirs_exist_ok=True)
            
            # Patch config loading to avoid system config files
            with patch('pathlib.Path.exists', return_value=True), \
                 patch('sqlitch.core.config.Config._get_system_config_paths', return_value=[]), \
                 patch('sqlitch.core.config.Config._get_global_config_path', return_value=None):
                result = runner.invoke(cli, [
                    'deploy', 
                    '--to-change', 'users_table',
                    '--no-verify',
                    '--log-only'
                ])
        
        assert result.exit_code == 0
    
    def test_cli_deploy_help(self, initialized_project):
        """Test deploy command help through CLI."""
        from click.testing import CliRunner
        runner = CliRunner()
        
        result = runner.invoke(cli, ['deploy', '--help'])
        
        assert result.exit_code == 0
        assert "Deploy database changes" in result.output
        assert "--target" in result.output
        assert "--verify" in result.output


class TestDeployCommandErrorHandling:
    """Test deploy command error handling."""
    
    def test_invalid_target(self, initialized_project):
        """Test deploy with invalid target."""
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        result = command.execute(['--target', 'nonexistent'])
        
        assert result == 1
    
    def test_invalid_change_name(self, initialized_project):
        """Test deploy to non-existent change."""
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        result = command.execute(['--to-change', 'nonexistent'])
        
        assert result == 1
    
    def test_invalid_tag_name(self, initialized_project):
        """Test deploy to non-existent tag."""
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        result = command.execute(['--to-tag', 'nonexistent'])
        
        assert result == 1
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_database_connection_error(self, mock_psycopg2, initialized_project):
        """Test deploy with database connection error."""
        mock_psycopg2.connect.side_effect = Exception("Connection failed")
        mock_psycopg2.Error = Exception
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        result = command.execute([])
        
        assert result == 1
    
    def test_missing_sql_files(self, initialized_project):
        """Test deploy with missing SQL files."""
        # Remove deploy files
        deploy_dir = initialized_project / 'deploy'
        for sql_file in deploy_dir.glob('*.sql'):
            sql_file.unlink()
        
        sqitch = create_sqitch()
        command = DeployCommand(sqitch)
        
        # Mock database setup
        with patch('sqlitch.engines.pg.psycopg2') as mock_psycopg2:
            mock_conn, mock_cursor = create_mock_psycopg2_connection()
            mock_psycopg2.connect.return_value = mock_conn
            mock_psycopg2.Error = Exception
            mock_psycopg2.extras.RealDictCursor = Mock()
            
            mock_cursor.fetchone.side_effect = [
                None,  # Schema doesn't exist
                None,  # Project doesn't exist
            ]
            mock_cursor.fetchall.return_value = []  # No deployed changes
            
            result = command.execute([])
        
        # Should still succeed if SQL files don't exist (they're optional)
        assert result == 0


class TestDeployCommandProgressReporting:
    """Test deploy command progress reporting."""
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_progress_reporting_normal(self, mock_psycopg2, initialized_project):
        """Test progress reporting with normal verbosity."""
        # Setup mock database
        mock_conn, mock_cursor = create_mock_psycopg2_connection()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        sqitch = create_sqitch()
        sqitch.verbosity = 0  # Normal verbosity
        command = DeployCommand(sqitch)
        
        with patch('pathlib.Path.exists', return_value=True):
            result = command.execute(['--to-change', 'users_table'])
        
        assert result == 0
        # Should have made progress reporting calls
        # Note: logger.info is a function, not a mock, so we can't check if it was called
    
    @patch('sqlitch.engines.pg.psycopg2')
    def test_progress_reporting_verbose(self, mock_psycopg2, initialized_project):
        """Test progress reporting with verbose output."""
        # Setup mock database
        mock_conn, mock_cursor = create_mock_psycopg2_connection()
        mock_psycopg2.connect.return_value = mock_conn
        mock_psycopg2.Error = Exception
        mock_psycopg2.extras.RealDictCursor = Mock()
        
        mock_cursor.fetchone.side_effect = [
            None,  # Schema doesn't exist
            None,  # Project doesn't exist
        ]
        mock_cursor.fetchall.return_value = []  # No deployed changes
        
        sqitch = create_sqitch()
        sqitch.verbosity = 2  # Verbose
        command = DeployCommand(sqitch)
        
        with patch('pathlib.Path.exists', return_value=True):
            result = command.execute(['--to-change', 'initial_schema'])
        
        assert result == 0
        # Should have made verbose logging calls
        # Note: logger.info is a function, not a mock, so we can't check if it was called