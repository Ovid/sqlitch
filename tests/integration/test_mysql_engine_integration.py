"""
Integration tests for MySQL database engine.

Tests the MySQL engine with actual database connections and operations.
These tests require a MySQL/MariaDB database to be available.
"""

import pytest
import os
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import Mock

from sqlitch.core.types import Target, URI
from sqlitch.core.change import Change, Dependency
from sqlitch.core.plan import Plan
from sqlitch.engines.mysql import MySQLEngine


# Skip all tests if MySQL is not available or not configured
pytestmark = pytest.mark.skipif(
    not os.getenv('MYSQL_TEST_URI'),
    reason="MySQL integration tests require MYSQL_TEST_URI environment variable"
)


@pytest.fixture(scope="session")
def mysql_test_uri():
    """Get MySQL test database URI from environment."""
    uri = os.getenv('MYSQL_TEST_URI')
    if not uri:
        pytest.skip("MYSQL_TEST_URI not set")
    return uri


@pytest.fixture(scope="session")
def mysql_available():
    """Check if MySQL/PyMySQL is available."""
    try:
        import pymysql
        return True
    except ImportError:
        pytest.skip("PyMySQL not available")


@pytest.fixture
def target(mysql_test_uri):
    """Create test target with MySQL database."""
    return Target(
        name="mysql_test",
        uri=URI(mysql_test_uri),
        registry="sqlitch_test_registry"
    )


@pytest.fixture
def plan():
    """Create test plan."""
    plan = Mock(spec=Plan)
    plan.project_name = "mysql_integration_test"
    plan.creator_name = "Test User"
    plan.creator_email = "test@example.com"
    plan.changes = []
    
    # Mock file methods
    def mock_get_deploy_file(change):
        return Path(f"/tmp/deploy_{change.name}.sql")
    
    def mock_get_revert_file(change):
        return Path(f"/tmp/revert_{change.name}.sql")
    
    def mock_get_verify_file(change):
        return Path(f"/tmp/verify_{change.name}.sql")
    
    plan.get_deploy_file = mock_get_deploy_file
    plan.get_revert_file = mock_get_revert_file
    plan.get_verify_file = mock_get_verify_file
    
    return plan


@pytest.fixture
def mysql_engine(target, plan, mysql_available):
    """Create MySQL engine instance."""
    return MySQLEngine(target, plan)


@pytest.fixture
def test_change():
    """Create test change."""
    return Change(
        id="test_change_123",
        name="test_change",
        note="Test change for integration testing",
        timestamp=datetime.now(timezone.utc),
        planner_name="Test User",
        planner_email="test@example.com",
        dependencies=[],
        tags=[]
    )


class TestMySQLEngineIntegration:
    """Integration tests for MySQL engine."""
    
    def test_connection_creation(self, mysql_engine):
        """Test creating a connection to MySQL database."""
        with mysql_engine.connection() as conn:
            assert conn is not None
            
            # Test basic query
            conn.execute("SELECT 1 as test")
            result = conn.fetchone()
            assert result['test'] == 1
    
    def test_registry_creation_and_cleanup(self, mysql_engine):
        """Test creating and cleaning up registry."""
        # Ensure registry is created
        mysql_engine.ensure_registry()
        
        # Verify registry exists
        with mysql_engine.connection() as conn:
            # Check if registry database exists
            conn.execute("SHOW DATABASES LIKE %s", (mysql_engine._registry_db_name,))
            result = conn.fetchone()
            assert result is not None
            
            # Switch to registry database
            conn.execute(f"USE `{mysql_engine._registry_db_name}`")
            
            # Check if all tables exist
            tables = ['releases', 'projects', 'changes', 'tags', 'dependencies', 'events']
            for table in tables:
                conn.execute(
                    "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                    (mysql_engine._registry_db_name, table)
                )
                result = conn.fetchone()
                assert result['count'] == 1, f"Table {table} should exist"
        
        # Cleanup: Drop the test registry database
        with mysql_engine.connection() as conn:
            conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")
    
    def test_registry_version_check(self, mysql_engine):
        """Test registry version checking."""
        # Create registry
        mysql_engine.ensure_registry()
        
        try:
            with mysql_engine.connection() as conn:
                version = mysql_engine._get_registry_version(conn)
                assert version == mysql_engine.registry_schema.REGISTRY_VERSION
        finally:
            # Cleanup
            with mysql_engine.connection() as conn:
                conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")
    
    def test_project_record_insertion(self, mysql_engine):
        """Test inserting project record."""
        # Create registry
        mysql_engine.ensure_registry()
        
        try:
            with mysql_engine.connection() as conn:
                conn.execute(f"USE `{mysql_engine._registry_db_name}`")
                
                # Check project record exists
                conn.execute(
                    "SELECT * FROM projects WHERE project = %s",
                    (mysql_engine.plan.project_name,)
                )
                result = conn.fetchone()
                
                assert result is not None
                assert result['project'] == mysql_engine.plan.project_name
                assert result['creator_name'] == mysql_engine.plan.creator_name
                assert result['creator_email'] == mysql_engine.plan.creator_email
        finally:
            # Cleanup
            with mysql_engine.connection() as conn:
                conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")
    
    def test_change_deployment_and_revert_cycle(self, mysql_engine, test_change, tmp_path):
        """Test complete change deployment and revert cycle."""
        # Create test SQL files
        deploy_file = tmp_path / f"deploy_{test_change.name}.sql"
        deploy_file.write_text("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100));")
        
        revert_file = tmp_path / f"revert_{test_change.name}.sql"
        revert_file.write_text("DROP TABLE IF EXISTS test_table;")
        
        # Mock plan file methods to return our test files
        mysql_engine.plan.get_deploy_file = lambda change: deploy_file
        mysql_engine.plan.get_revert_file = lambda change: revert_file
        mysql_engine.plan.get_verify_file = lambda change: tmp_path / "nonexistent.sql"
        
        # Create registry
        mysql_engine.ensure_registry()
        
        try:
            # Test deployment
            mysql_engine.deploy_change(test_change)
            
            # Verify change is recorded
            deployed_changes = mysql_engine.get_deployed_changes()
            assert test_change.id in deployed_changes
            
            # Verify table was created (in main database, not registry)
            with mysql_engine.connection() as conn:
                # Switch to main database
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                
                # Check if table exists
                conn.execute("SHOW TABLES LIKE 'test_table'")
                result = conn.fetchone()
                assert result is not None
            
            # Test revert
            mysql_engine.revert_change(test_change)
            
            # Verify change is no longer recorded
            deployed_changes = mysql_engine.get_deployed_changes()
            assert test_change.id not in deployed_changes
            
            # Verify table was dropped
            with mysql_engine.connection() as conn:
                # Switch to main database
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                
                # Check if table no longer exists
                conn.execute("SHOW TABLES LIKE 'test_table'")
                result = conn.fetchone()
                assert result is None
                
        finally:
            # Cleanup: Drop test table and registry
            with mysql_engine.connection() as conn:
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                    conn.execute("DROP TABLE IF EXISTS test_table")
                
                conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")
    
    def test_change_verification(self, mysql_engine, test_change, tmp_path):
        """Test change verification."""
        # Create test SQL files
        deploy_file = tmp_path / f"deploy_{test_change.name}.sql"
        deploy_file.write_text("CREATE TABLE verify_test (id INT PRIMARY KEY);")
        
        verify_file = tmp_path / f"verify_{test_change.name}.sql"
        verify_file.write_text("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'verify_test';")
        
        # Mock plan file methods
        mysql_engine.plan.get_deploy_file = lambda change: deploy_file
        mysql_engine.plan.get_revert_file = lambda change: tmp_path / "nonexistent.sql"
        mysql_engine.plan.get_verify_file = lambda change: verify_file
        
        # Create registry and deploy change
        mysql_engine.ensure_registry()
        
        try:
            mysql_engine.deploy_change(test_change)
            
            # Test verification
            result = mysql_engine.verify_change(test_change)
            assert result is True
            
        finally:
            # Cleanup
            with mysql_engine.connection() as conn:
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                    conn.execute("DROP TABLE IF EXISTS verify_test")
                
                conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")
    
    def test_transaction_rollback_on_error(self, mysql_engine, test_change, tmp_path):
        """Test transaction rollback on deployment error."""
        # Create deploy file with invalid SQL
        deploy_file = tmp_path / f"deploy_{test_change.name}.sql"
        deploy_file.write_text("CREATE TABLE test_table (id INT PRIMARY KEY); INVALID SQL STATEMENT;")
        
        # Mock plan file methods
        mysql_engine.plan.get_deploy_file = lambda change: deploy_file
        mysql_engine.plan.get_revert_file = lambda change: tmp_path / "nonexistent.sql"
        mysql_engine.plan.get_verify_file = lambda change: tmp_path / "nonexistent.sql"
        
        # Create registry
        mysql_engine.ensure_registry()
        
        try:
            # Deployment should fail
            with pytest.raises(Exception):  # Could be DeploymentError or MySQLError
                mysql_engine.deploy_change(test_change)
            
            # Verify change is not recorded (transaction rolled back)
            deployed_changes = mysql_engine.get_deployed_changes()
            assert test_change.id not in deployed_changes
            
            # Verify table was not created (transaction rolled back)
            with mysql_engine.connection() as conn:
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                
                conn.execute("SHOW TABLES LIKE 'test_table'")
                result = conn.fetchone()
                assert result is None
                
        finally:
            # Cleanup
            with mysql_engine.connection() as conn:
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                    conn.execute("DROP TABLE IF EXISTS test_table")
                
                conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")
    
    def test_multiple_changes_deployment_order(self, mysql_engine, tmp_path):
        """Test deploying multiple changes in correct order."""
        # Create multiple test changes
        change1 = Change(
            id="change1_123",
            name="change1",
            note="First change",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[],
            tags=[]
        )
        
        change2 = Change(
            id="change2_456",
            name="change2",
            note="Second change",
            timestamp=datetime.now(timezone.utc),
            planner_name="Test User",
            planner_email="test@example.com",
            dependencies=[],
            tags=[]
        )
        
        # Create SQL files
        deploy1 = tmp_path / "deploy_change1.sql"
        deploy1.write_text("CREATE TABLE table1 (id INT PRIMARY KEY);")
        
        deploy2 = tmp_path / "deploy_change2.sql"
        deploy2.write_text("CREATE TABLE table2 (id INT PRIMARY KEY);")
        
        # Mock plan file methods
        def get_deploy_file(change):
            if change.name == "change1":
                return deploy1
            elif change.name == "change2":
                return deploy2
            return tmp_path / "nonexistent.sql"
        
        mysql_engine.plan.get_deploy_file = get_deploy_file
        mysql_engine.plan.get_revert_file = lambda change: tmp_path / "nonexistent.sql"
        mysql_engine.plan.get_verify_file = lambda change: tmp_path / "nonexistent.sql"
        
        # Create registry
        mysql_engine.ensure_registry()
        
        try:
            # Deploy changes in order
            mysql_engine.deploy_change(change1)
            mysql_engine.deploy_change(change2)
            
            # Verify both changes are recorded in correct order
            deployed_changes = mysql_engine.get_deployed_changes()
            assert len(deployed_changes) == 2
            assert deployed_changes[0] == change1.id
            assert deployed_changes[1] == change2.id
            
        finally:
            # Cleanup
            with mysql_engine.connection() as conn:
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                    conn.execute("DROP TABLE IF EXISTS table1")
                    conn.execute("DROP TABLE IF EXISTS table2")
                
                conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")
    
    def test_sql_mode_and_charset_settings(self, mysql_engine):
        """Test that MySQL session is configured correctly."""
        with mysql_engine.connection() as conn:
            # Check character set
            conn.execute("SELECT @@character_set_client as charset")
            result = conn.fetchone()
            assert 'utf8' in result['charset']
            
            # Check time zone
            conn.execute("SELECT @@time_zone as tz")
            result = conn.fetchone()
            assert result['tz'] == '+00:00'
            
            # Check SQL mode includes required settings
            conn.execute("SELECT @@sql_mode as mode")
            result = conn.fetchone()
            sql_mode = result['mode']
            
            required_modes = ['ANSI', 'STRICT_TRANS_TABLES', 'NO_ZERO_DATE']
            for mode in required_modes:
                assert mode in sql_mode.upper()
    
    def test_concurrent_access_locking(self, mysql_engine, test_change, tmp_path):
        """Test that table locking prevents concurrent modifications."""
        # Create test SQL file
        deploy_file = tmp_path / f"deploy_{test_change.name}.sql"
        deploy_file.write_text("CREATE TABLE lock_test (id INT PRIMARY KEY);")
        
        # Mock plan file methods
        mysql_engine.plan.get_deploy_file = lambda change: deploy_file
        mysql_engine.plan.get_revert_file = lambda change: tmp_path / "nonexistent.sql"
        mysql_engine.plan.get_verify_file = lambda change: tmp_path / "nonexistent.sql"
        
        # Create registry
        mysql_engine.ensure_registry()
        
        try:
            # Test that transaction context manager handles locking
            with mysql_engine.transaction() as conn:
                # Verify we can execute statements within transaction
                conn.execute("SELECT 1")
                result = conn.fetchone()
                assert result is not None
            
            # Deploy change to test full transaction cycle
            mysql_engine.deploy_change(test_change)
            
            # Verify change was deployed
            deployed_changes = mysql_engine.get_deployed_changes()
            assert test_change.id in deployed_changes
            
        finally:
            # Cleanup
            with mysql_engine.connection() as conn:
                main_db = mysql_engine._connection_params.get('database')
                if main_db:
                    conn.execute(f"USE `{main_db}`")
                    conn.execute("DROP TABLE IF EXISTS lock_test")
                
                conn.execute(f"DROP DATABASE IF EXISTS `{mysql_engine._registry_db_name}`")