"""
Integration tests for template system.
"""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
import os

from sqlitch.utils.template import (
    create_template_engine,
    render_change_template,
    TemplateEngine,
    TemplateContext,
)
from sqlitch.core.types import EngineType, OperationType


class TestTemplateSystemIntegration:
    """Integration tests for the complete template system."""
    
    def test_end_to_end_template_rendering(self):
        """Test complete template rendering workflow."""
        # Test all supported engines and operations
        engines = ["pg", "mysql", "sqlite", "oracle", "snowflake", "vertica", "exasol", "firebird", "cockroach"]
        operations = ["deploy", "revert", "verify"]
        
        for engine in engines:
            for operation in operations:
                result = render_change_template(
                    operation=operation,
                    engine=engine,
                    project="testproject",
                    change="test_change",
                    requires=["dep1", "dep2"],
                    conflicts=["conf1"]
                )
                
                # Verify basic template structure
                assert f"{operation} testproject:test_change" in result.lower()
                
                # Only deploy templates include requires/conflicts
                if operation == "deploy":
                    assert "-- requires: dep1" in result
                    assert "-- requires: dep2" in result
                    assert "-- conflicts: conf1" in result
                
                # Verify engine-specific content for transaction-based engines
                if engine in ["pg", "mysql", "cockroach"]:
                    if operation in ["deploy", "revert"]:
                        assert "BEGIN;" in result
                        assert "COMMIT;" in result
                    elif operation == "verify":
                        assert "BEGIN;" in result
                        assert "ROLLBACK;" in result
    
    def test_custom_template_directory_integration(self):
        """Test integration with custom template directories."""
        with TemporaryDirectory() as temp_dir:
            # Set up custom template directory structure
            template_dir = Path(temp_dir) / "custom_templates"
            template_dir.mkdir()
            
            # Create custom templates for all operations
            for operation in ["deploy", "revert", "verify"]:
                op_dir = template_dir / operation
                op_dir.mkdir()
                
                # Create custom PostgreSQL template
                pg_template = op_dir / "pg.tmpl"
                template_content = f"""
-- Custom {operation.title()} {{{{ project }}}}:{{{{ change }}}} to {{{{ engine }}}}
{{% for req in requires %}}
-- requires: {{{{ req }}}}
{{% endfor %}}
{{% for conf in conflicts %}}
-- conflicts: {{{{ conf }}}}
{{% endfor %}}

-- Custom {operation} logic here
SELECT 'Custom {operation} for {{{{ change }}}}';
""".strip()
                pg_template.write_text(template_content)
            
            # Test rendering with custom templates
            result = render_change_template(
                operation="deploy",
                engine="pg",
                project="customproject",
                change="custom_change",
                requires=["req1"],
                conflicts=["conf1"],
                template_dirs=[template_dir]
            )
            
            assert "Custom Deploy customproject:custom_change to pg" in result
            assert "-- requires: req1" in result
            assert "-- conflicts: conf1" in result
            assert "Custom deploy logic here" in result
            assert "SELECT 'Custom deploy for custom_change';" in result
    
    def test_template_directory_fallback(self):
        """Test fallback to built-in templates when custom templates are missing."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "partial_templates"
            template_dir.mkdir()
            
            # Create only deploy template, leaving revert/verify to fall back to built-in
            deploy_dir = template_dir / "deploy"
            deploy_dir.mkdir()
            
            pg_deploy = deploy_dir / "pg.tmpl"
            pg_deploy.write_text("CUSTOM DEPLOY: {{ project }}/{{ change }}")
            
            # Test custom deploy template
            deploy_result = render_change_template(
                operation="deploy",
                engine="pg",
                project="testproject",
                change="test_change",
                template_dirs=[template_dir]
            )
            
            assert deploy_result == "CUSTOM DEPLOY: testproject/test_change"
            
            # Test fallback to built-in revert template
            revert_result = render_change_template(
                operation="revert",
                engine="pg",
                project="testproject",
                change="test_change",
                template_dirs=[template_dir]
            )
            
            assert "Revert testproject:test_change from pg" in revert_result
            assert "BEGIN;" in revert_result
            assert "COMMIT;" in revert_result
    
    def test_multiple_custom_directories_precedence(self):
        """Test precedence when multiple custom template directories are provided."""
        with TemporaryDirectory() as temp_dir:
            # Create two template directories
            template_dir1 = Path(temp_dir) / "templates1"
            template_dir1.mkdir()
            template_dir2 = Path(temp_dir) / "templates2"
            template_dir2.mkdir()
            
            # Create templates in both directories
            for i, tdir in enumerate([template_dir1, template_dir2], 1):
                deploy_dir = tdir / "deploy"
                deploy_dir.mkdir()
                
                pg_template = deploy_dir / "pg.tmpl"
                template_text = f"TEMPLATE DIR {i}: {{{{ project }}}}/{{{{ change }}}}"
                pg_template.write_text(template_text)
            
            # First directory should take precedence
            result = render_change_template(
                operation="deploy",
                engine="pg",
                project="testproject",
                change="test_change",
                template_dirs=[template_dir1, template_dir2]
            )
            
            assert result == "TEMPLATE DIR 1: testproject/test_change"
    
    def test_template_with_real_world_sql(self):
        """Test templates with realistic SQL content."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "sql_templates"
            template_dir.mkdir()
            
            # Create realistic SQL templates
            deploy_dir = template_dir / "deploy"
            deploy_dir.mkdir()
            
            pg_deploy = deploy_dir / "pg.tmpl"
            pg_deploy.write_text("""
-- Deploy {{ project }}:{{ change }} to {{ engine }}
{% for req in requires %}
-- requires: {{ req }}
{% endfor %}
{% for conf in conflicts %}
-- conflicts: {{ conf }}
{% endfor %}

BEGIN;

-- Create {{ change }} table
CREATE TABLE {{ change }} (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes
CREATE INDEX idx_{{ change }}_name ON {{ change }} (name);
CREATE INDEX idx_{{ change }}_email ON {{ change }} (email);

-- Add constraints
ALTER TABLE {{ change }} 
ADD CONSTRAINT chk_{{ change }}_email_format 
CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');

COMMIT;
""".strip())
            
            revert_dir = template_dir / "revert"
            revert_dir.mkdir()
            
            pg_revert = revert_dir / "pg.tmpl"
            pg_revert.write_text("""
-- Revert {{ project }}:{{ change }} from {{ engine }}

BEGIN;

-- Drop table and all associated objects
DROP TABLE IF EXISTS {{ change }} CASCADE;

COMMIT;
""".strip())
            
            verify_dir = template_dir / "verify"
            verify_dir.mkdir()
            
            pg_verify = verify_dir / "verify"
            pg_verify.write_text("""
-- Verify {{ project }}:{{ change }} on {{ engine }}

BEGIN;

-- Verify table exists
SELECT 1/COUNT(*) FROM information_schema.tables 
WHERE table_name = '{{ change }}' AND table_schema = 'public';

-- Verify required columns exist
SELECT 1/COUNT(*) FROM information_schema.columns 
WHERE table_name = '{{ change }}' 
  AND column_name IN ('id', 'name', 'email', 'created_at', 'updated_at');

-- Verify indexes exist
SELECT 1/COUNT(*) FROM pg_indexes 
WHERE tablename = '{{ change }}' 
  AND indexname IN ('idx_{{ change }}_name', 'idx_{{ change }}_email');

ROLLBACK;
""".strip())
            
            # Test deploy template
            deploy_result = render_change_template(
                operation="deploy",
                engine="pg",
                project="myapp",
                change="users",
                requires=["initial_schema"],
                template_dirs=[template_dir]
            )
            
            assert "Deploy myapp:users to pg" in deploy_result
            assert "-- requires: initial_schema" in deploy_result
            assert "CREATE TABLE users (" in deploy_result
            assert "CREATE INDEX idx_users_name ON users" in deploy_result
            assert "chk_users_email_format" in deploy_result
            
            # Test revert template
            revert_result = render_change_template(
                operation="revert",
                engine="pg",
                project="myapp",
                change="users",
                template_dirs=[template_dir]
            )
            
            assert "Revert myapp:users from pg" in revert_result
            assert "DROP TABLE IF EXISTS users CASCADE;" in revert_result
    
    def test_template_engine_with_nonexistent_directory(self):
        """Test template engine behavior with non-existent custom directories."""
        nonexistent_dir = Path("/nonexistent/template/directory")
        
        # Should not raise an error, just ignore the non-existent directory
        engine = TemplateEngine([nonexistent_dir])
        
        # Should still be able to render built-in templates
        context = TemplateContext(
            project="testproject",
            change="test_change",
            engine="pg"
        )
        
        result = engine.render_template('deploy/pg.tmpl', context)
        assert "Deploy testproject:test_change to pg" in result
    
    def test_template_listing_integration(self):
        """Test template listing with mixed built-in and custom templates."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "mixed_templates"
            template_dir.mkdir()
            
            # Create some custom templates
            deploy_dir = template_dir / "deploy"
            deploy_dir.mkdir()
            (deploy_dir / "custom1.tmpl").write_text("Custom 1")
            (deploy_dir / "custom2.tmpl").write_text("Custom 2")
            
            revert_dir = template_dir / "revert"
            revert_dir.mkdir()
            (revert_dir / "custom1.tmpl").write_text("Custom 1 revert")
            
            engine = TemplateEngine([template_dir])
            templates = engine.list_templates()
            
            # Should include both built-in and custom templates
            assert 'deploy/pg.tmpl' in templates  # Built-in
            assert 'deploy/mysql.tmpl' in templates  # Built-in
            assert 'deploy/custom1.tmpl' in templates  # Custom
            assert 'deploy/custom2.tmpl' in templates  # Custom
            assert 'revert/custom1.tmpl' in templates  # Custom
            
            # Should not have duplicates
            assert len(templates) == len(set(templates))
    
    def test_template_context_edge_cases(self):
        """Test template context with edge cases."""
        # Test with empty lists
        result = render_change_template(
            operation="deploy",
            engine="pg",
            project="testproject",
            change="test_change",
            requires=[],
            conflicts=[]
        )
        
        assert "Deploy testproject:test_change to pg" in result
        assert "-- requires:" not in result  # Should not appear if empty
        assert "-- conflicts:" not in result  # Should not appear if empty
        
        # Test with None values (should default to empty lists)
        result = render_change_template(
            operation="deploy",
            engine="pg",
            project="testproject",
            change="test_change",
            requires=None,
            conflicts=None
        )
        
        assert "Deploy testproject:test_change to pg" in result
        assert "-- requires:" not in result
        assert "-- conflicts:" not in result
    
    def test_template_with_special_characters(self):
        """Test templates with special characters in project/change names."""
        result = render_change_template(
            operation="deploy",
            engine="pg",
            project="my-project_v2",
            change="add_users-table_v1",
            requires=["schema-init_v1"],
            conflicts=["old-users_table"]
        )
        
        assert "Deploy my-project_v2:add_users-table_v1 to pg" in result
        assert "-- requires: schema-init_v1" in result
        assert "-- conflicts: old-users_table" in result
    
    def test_all_engine_templates_exist(self):
        """Test that all supported engines have the required templates."""
        engine = TemplateEngine()
        
        # Test engines that should have templates
        test_engines = ["pg", "mysql", "sqlite", "oracle", "snowflake", "vertica", "exasol", "firebird", "cockroach"]
        operations = ["deploy", "revert", "verify"]
        
        for db_engine in test_engines:
            for operation in operations:
                template_path = engine.get_template_path(operation, db_engine)
                assert engine.template_exists(template_path), \
                    f"Missing template: {template_path}"
                
                # Verify template can be rendered
                context = TemplateContext(
                    project="test",
                    change="test",
                    engine=db_engine
                )
                
                result = engine.render_template(template_path, context)
                assert len(result) > 0, f"Empty template: {template_path}"