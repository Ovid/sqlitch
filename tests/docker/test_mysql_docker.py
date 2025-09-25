"""
Docker-based MySQL integration tests.

These tests use real MySQL Docker containers to verify full deployment
cycles, registry creation, change deployment, revert, and verification.
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from sqlitch.core.change import Change
from sqlitch.core.plan import Plan
from sqlitch.core.target import Target
from sqlitch.core.types import URI
from sqlitch.engines.mysql import MySQLEngine


@pytest.mark.integration
class TestMySQLDockerIntegration:
    """Docker-based integration tests for MySQL engine."""

    @pytest.fixture(scope="class")
    def mysql_uri(self, mysql_container):
        """Get MySQL connection URI from Docker container."""
        if not mysql_container:
            pytest.skip("MySQL container not available")

        # Get container port mapping
        port = mysql_container.ports.get("3306/tcp")
        if port:
            host_port = port[0]["HostPort"]
        else:
            host_port = "3306"

        return f"db:mysql://sqlitch:test@localhost:{host_port}/sqlitch_test"

    @pytest.fixture
    def mysql_target(self, mysql_uri):
        """Create MySQL target for testing."""
        return Target(
            name="test_mysql_docker",
            uri=URI(mysql_uri),
            registry="sqitch",
        )

    @pytest.fixture
    def test_plan(self, tmp_path):
        """Create a test plan with sample changes."""
        plan = Mock(spec=Plan)
        plan.project_name = "mysql_docker_test_project"
        plan.creator_name = "MySQL Docker Test User"
        plan.creator_email = "mysql.docker.test@example.com"

        # Create sample changes
        from datetime import datetime

        change1 = Change(
            name="create_products_table",
            note="Create products table for testing",
            timestamp=datetime(2023, 1, 15, 10, 30, 0),
            planner_name="MySQL Docker Test User",
            planner_email="mysql.docker.test@example.com",
        )

        change2 = Change(
            name="add_product_indexes",
            note="Add indexes to products table",
            timestamp=datetime(2023, 1, 16, 14, 20, 0),
            planner_name="MySQL Docker Test User",
            planner_email="mysql.docker.test@example.com",
        )

        plan.changes = [change1, change2]

        # Create SQL files for changes
        deploy_dir = tmp_path / "deploy"
        revert_dir = tmp_path / "revert"
        verify_dir = tmp_path / "verify"

        for dir_path in [deploy_dir, revert_dir, verify_dir]:
            dir_path.mkdir(exist_ok=True)

        # Create deploy scripts
        (deploy_dir / "create_products_table.sql").write_text(
            """
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""
        )

        (deploy_dir / "add_product_indexes.sql").write_text(
            """
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_created_at ON products(created_at);
"""
        )

        # Create revert scripts
        (revert_dir / "add_product_indexes.sql").write_text(
            """
DROP INDEX IF EXISTS idx_products_created_at ON products;
DROP INDEX IF EXISTS idx_products_price ON products;
DROP INDEX IF EXISTS idx_products_category ON products;
DROP INDEX IF EXISTS idx_products_name ON products;
"""
        )

        (revert_dir / "create_products_table.sql").write_text(
            """
DROP TABLE IF EXISTS products;
"""
        )

        # Create verify scripts
        (verify_dir / "create_products_table.sql").write_text(
            """
SELECT 1/COUNT(*) FROM information_schema.tables 
WHERE table_name = 'products' AND table_schema = DATABASE();
"""
        )

        (verify_dir / "add_product_indexes.sql").write_text(
            """
SELECT 1/COUNT(*) FROM information_schema.statistics 
WHERE table_name = 'products' AND index_name = 'idx_products_name' AND table_schema = DATABASE();

SELECT 1/COUNT(*) FROM information_schema.statistics 
WHERE table_name = 'products' AND index_name = 'idx_products_category' AND table_schema = DATABASE();

SELECT 1/COUNT(*) FROM information_schema.statistics 
WHERE table_name = 'products' AND index_name = 'idx_products_price' AND table_schema = DATABASE();

SELECT 1/COUNT(*) FROM information_schema.statistics 
WHERE table_name = 'products' AND index_name = 'idx_products_created_at' AND table_schema = DATABASE();
"""
        )

        # Mock file path methods
        def get_deploy_file(change):
            return deploy_dir / f"{change.name}.sql"

        def get_revert_file(change):
            return revert_dir / f"{change.name}.sql"

        def get_verify_file(change):
            return verify_dir / f"{change.name}.sql"

        plan.get_deploy_file = get_deploy_file
        plan.get_revert_file = get_revert_file
        plan.get_verify_file = get_verify_file

        return plan

    def test_registry_creation_and_initialization(
        self, mysql_target, test_plan, skip_if_no_docker
    ):
        """Test creating and initializing the sqitch registry."""
        engine = MySQLEngine(mysql_target, test_plan)

        # Test registry creation
        with engine.connection() as conn:
            # Registry should not exist initially
            assert not engine._registry_exists_in_db(conn)

            # Create registry
            engine._create_registry(conn)

            # Registry should now exist
            assert engine._registry_exists_in_db(conn)

            # Check that all required tables exist
            tables_query = """
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name LIKE 'sqitch_%'
            ORDER BY table_name
            """
            conn.execute(tables_query)
            tables = [row[0] for row in conn.fetchall()]

            expected_tables = [
                "sqitch_changes",
                "sqitch_dependencies",
                "sqitch_events",
                "sqitch_projects",
                "sqitch_releases",
                "sqitch_tags",
            ]
            assert all(table in tables for table in expected_tables)

            # Check registry version
            version = engine._get_registry_version(conn)
            assert version is not None
            assert version.startswith("1.")

    def test_full_deployment_cycle(self, mysql_target, test_plan, skip_if_no_docker):
        """Test complete deployment, verification, and revert cycle."""
        engine = MySQLEngine(mysql_target, test_plan)

        with engine.connection() as conn:
            # Ensure clean state
            if engine._registry_exists_in_db(conn):
                # Drop all sqitch tables
                conn.execute("DROP TABLE IF EXISTS sqitch_events")
                conn.execute("DROP TABLE IF EXISTS sqitch_dependencies")
                conn.execute("DROP TABLE IF EXISTS sqitch_tags")
                conn.execute("DROP TABLE IF EXISTS sqitch_changes")
                conn.execute("DROP TABLE IF EXISTS sqitch_projects")
                conn.execute("DROP TABLE IF EXISTS sqitch_releases")
                conn.commit()

            # Create registry
            engine._create_registry(conn)

            # Deploy first change
            change1 = test_plan.changes[0]
            engine.deploy_change(change1)

            # Verify deployment was recorded
            conn.execute(
                "SELECT COUNT(*) FROM sqitch_changes WHERE change_id = %s",
                (change1.id,),
            )
            assert conn.fetchone()[0] == 1

            # Verify table was created
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'products' AND table_schema = DATABASE()
            """
            )
            assert conn.fetchone()[0] == 1

            # Deploy second change
            change2 = test_plan.changes[1]
            engine.deploy_change(change2)

            # Verify indexes were created
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_name = 'products' AND index_name LIKE 'idx_products_%' 
                AND table_schema = DATABASE()
            """
            )
            assert (
                conn.fetchone()[0] >= 4
            )  # At least 4 index entries (some indexes may have multiple entries)

            # Test verification
            result = engine.verify_change(change1)
            assert result.success

            result = engine.verify_change(change2)
            assert result.success

            # Test revert
            engine.revert_change(change2)

            # Verify indexes were removed
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_name = 'products' AND index_name LIKE 'idx_products_%' 
                AND table_schema = DATABASE()
            """
            )
            assert conn.fetchone()[0] == 0

            # Verify change was removed from registry
            conn.execute(
                "SELECT COUNT(*) FROM sqitch_changes WHERE change_id = %s",
                (change2.id,),
            )
            assert conn.fetchone()[0] == 0

            # Revert first change
            engine.revert_change(change1)

            # Verify table was removed
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'products' AND table_schema = DATABASE()
            """
            )
            assert conn.fetchone()[0] == 0

    def test_mysql_specific_features(self, mysql_target, test_plan, skip_if_no_docker):
        """Test MySQL-specific features and SQL syntax."""
        engine = MySQLEngine(mysql_target, test_plan)

        # Create a change that uses MySQL-specific features
        from datetime import datetime

        mysql_change = Change(
            name="mysql_specific_features",
            note="Test MySQL-specific SQL features",
            timestamp=datetime(2023, 2, 1, 12, 0, 0),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Create temporary SQL file with MySQL-specific syntax
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(
                """
-- MySQL-specific features test
CREATE TABLE mysql_features_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert test data with JSON
INSERT INTO mysql_features_test (data) VALUES 
('{"name": "test1", "value": 100}'),
('{"name": "test2", "value": 200}');

-- Create a view
CREATE VIEW mysql_features_view AS 
SELECT id, JSON_EXTRACT(data, '$.name') as name, created_at 
FROM mysql_features_test;
"""
            )
            mysql_sql_file = Path(f.name)

        try:
            with engine.connection() as conn:
                # Ensure clean state
                if engine._registry_exists_in_db(conn):
                    # Drop all sqitch tables
                    conn.execute("DROP TABLE IF EXISTS sqitch_events")
                    conn.execute("DROP TABLE IF EXISTS sqitch_dependencies")
                    conn.execute("DROP TABLE IF EXISTS sqitch_tags")
                    conn.execute("DROP TABLE IF EXISTS sqitch_changes")
                    conn.execute("DROP TABLE IF EXISTS sqitch_projects")
                    conn.execute("DROP TABLE IF EXISTS sqitch_releases")
                    conn.commit()

                # Create registry
                engine._create_registry(conn)

                # Mock the plan to return our MySQL-specific SQL file
                original_get_deploy_file = test_plan.get_deploy_file
                test_plan.get_deploy_file = lambda change: (
                    mysql_sql_file
                    if change == mysql_change
                    else original_get_deploy_file(change)
                )

                # Deploy MySQL-specific change
                engine.deploy_change(mysql_change)

                # Verify table was created with correct engine and charset
                conn.execute(
                    """
                    SELECT engine, table_collation FROM information_schema.tables 
                    WHERE table_name = 'mysql_features_test' AND table_schema = DATABASE()
                """
                )
                result = conn.fetchone()
                assert result[0] == "InnoDB"
                assert "utf8mb4" in result[1]

                # Verify JSON data was inserted correctly
                conn.execute("SELECT COUNT(*) FROM mysql_features_test")
                assert conn.fetchone()[0] == 2

                # Verify view was created
                conn.execute(
                    """
                    SELECT COUNT(*) FROM information_schema.views 
                    WHERE table_name = 'mysql_features_view' AND table_schema = DATABASE()
                """
                )
                assert conn.fetchone()[0] == 1

                # Test JSON extraction through view
                conn.execute("SELECT name FROM mysql_features_view ORDER BY id")
                names = [row[0] for row in conn.fetchall()]
                assert names == [
                    '"test1"',
                    '"test2"',
                ]  # JSON_EXTRACT returns quoted strings

        finally:
            # Clean up temporary file
            mysql_sql_file.unlink(missing_ok=True)

    def test_transaction_isolation_and_locking(
        self, mysql_target, test_plan, skip_if_no_docker
    ):
        """Test MySQL transaction isolation and locking behavior."""
        engine1 = MySQLEngine(mysql_target, test_plan)
        engine2 = MySQLEngine(mysql_target, test_plan)

        with engine1.connection() as conn1:
            # Ensure clean state
            if engine1._registry_exists_in_db(conn1):
                # Drop all sqitch tables
                conn1.execute("DROP TABLE IF EXISTS sqitch_events")
                conn1.execute("DROP TABLE IF EXISTS sqitch_dependencies")
                conn1.execute("DROP TABLE IF EXISTS sqitch_tags")
                conn1.execute("DROP TABLE IF EXISTS sqitch_changes")
                conn1.execute("DROP TABLE IF EXISTS sqitch_projects")
                conn1.execute("DROP TABLE IF EXISTS sqitch_releases")
                conn1.commit()

            # Create registry
            engine1.create_registry(conn1)

            with engine2.connection() as conn2:
                # Both engines should see the registry
                assert engine1._registry_exists_in_db(conn1)
                assert engine2._registry_exists_in_db(conn2)

                # Start transaction in first connection
                conn1.execute("START TRANSACTION")

                # Deploy change with first engine (in transaction)
                change1 = test_plan.changes[0]
                engine1.deploy_change(change1)

                # Second engine should not see uncommitted change
                conn2.execute(
                    "SELECT COUNT(*) FROM sqitch_changes WHERE change_id = %s",
                    (change1.id,),
                )
                assert conn2.fetchone()[0] == 0

                # Commit transaction
                conn1.commit()

                # Now second engine should see the change
                conn2.execute(
                    "SELECT COUNT(*) FROM sqitch_changes WHERE change_id = %s",
                    (change1.id,),
                )
                assert conn2.fetchone()[0] == 1

    def test_error_handling_and_rollback(
        self, mysql_target, test_plan, skip_if_no_docker
    ):
        """Test error handling and transaction rollback."""
        engine = MySQLEngine(mysql_target, test_plan)

        # Create a change with invalid SQL
        from datetime import datetime

        bad_change = Change(
            name="bad_mysql_change",
            note="Change with invalid MySQL SQL",
            timestamp=datetime(2023, 2, 2, 15, 30, 0),
            planner_name="Test User",
            planner_email="test@example.com",
        )

        # Create temporary SQL file with invalid syntax
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("CREATE TABLE invalid_syntax_table ( invalid syntax here")
            bad_sql_file = Path(f.name)

        try:
            with engine.connection() as conn:
                # Ensure clean state
                if engine._registry_exists_in_db(conn):
                    # Drop all sqitch tables
                    conn.execute("DROP TABLE IF EXISTS sqitch_events")
                    conn.execute("DROP TABLE IF EXISTS sqitch_dependencies")
                    conn.execute("DROP TABLE IF EXISTS sqitch_tags")
                    conn.execute("DROP TABLE IF EXISTS sqitch_changes")
                    conn.execute("DROP TABLE IF EXISTS sqitch_projects")
                    conn.execute("DROP TABLE IF EXISTS sqitch_releases")
                    conn.commit()

                # Create registry
                engine._create_registry(conn)

                # Mock the plan to return our bad SQL file
                original_get_deploy_file = test_plan.get_deploy_file
                test_plan.get_deploy_file = lambda change: (
                    bad_sql_file
                    if change == bad_change
                    else original_get_deploy_file(change)
                )

                # Attempt to deploy bad change - should fail
                with pytest.raises(Exception):
                    engine.deploy_change(bad_change)

                # Verify the change was not recorded in registry
                conn.execute(
                    "SELECT COUNT(*) FROM sqitch_changes WHERE change_id = %s",
                    (bad_change.id,),
                )
                assert conn.fetchone()[0] == 0

                # Verify no partial state was left behind
                conn.execute(
                    """
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'invalid_syntax_table' AND table_schema = DATABASE()
                """
                )
                assert conn.fetchone()[0] == 0

        finally:
            # Clean up temporary file
            bad_sql_file.unlink(missing_ok=True)

    def test_charset_and_collation_handling(
        self, mysql_target, test_plan, skip_if_no_docker
    ):
        """Test proper handling of MySQL character sets and collations."""
        engine = MySQLEngine(mysql_target, test_plan)

        with engine.connection() as conn:
            # Ensure clean state
            if engine._registry_exists_in_db(conn):
                # Drop all sqitch tables
                conn.execute("DROP TABLE IF EXISTS sqitch_events")
                conn.execute("DROP TABLE IF EXISTS sqitch_dependencies")
                conn.execute("DROP TABLE IF EXISTS sqitch_tags")
                conn.execute("DROP TABLE IF EXISTS sqitch_changes")
                conn.execute("DROP TABLE IF EXISTS sqitch_projects")
                conn.execute("DROP TABLE IF EXISTS sqitch_releases")
                conn.commit()

            # Create registry
            engine._create_registry(conn)

            # Check that sqitch tables use proper charset
            conn.execute(
                """
                SELECT table_name, table_collation FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name LIKE 'sqitch_%'
                ORDER BY table_name
            """
            )

            for table_name, collation in conn.fetchall():
                # Should use utf8mb4 charset
                assert (
                    "utf8mb4" in collation
                ), f"Table {table_name} should use utf8mb4 charset, got {collation}"
