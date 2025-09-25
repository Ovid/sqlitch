"""
Tests for template processing functionality.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest

from sqlitch.core.exceptions import SqlitchError
from sqlitch.core.types import EngineType, OperationType
from sqlitch.utils.template import (
    BuiltinTemplateLoader,
    TemplateContext,
    TemplateEngine,
    TemplateError,
    create_template_engine,
    render_change_template,
)


class TestTemplateContext:
    """Test TemplateContext class."""

    def test_init_with_defaults(self):
        """Test TemplateContext initialization with defaults."""
        context = TemplateContext(project="myproject", change="add_users", engine="pg")

        assert context.project == "myproject"
        assert context.change == "add_users"
        assert context.engine == "pg"
        assert context.requires == []
        assert context.conflicts == []

    def test_init_with_values(self):
        """Test TemplateContext initialization with values."""
        context = TemplateContext(
            project="myproject",
            change="add_users",
            engine="pg",
            requires=["initial_schema"],
            conflicts=["old_users"],
        )

        assert context.project == "myproject"
        assert context.change == "add_users"
        assert context.engine == "pg"
        assert context.requires == ["initial_schema"]
        assert context.conflicts == ["old_users"]

    def test_to_dict(self):
        """Test conversion to dictionary."""
        context = TemplateContext(
            project="myproject",
            change="add_users",
            engine="pg",
            requires=["initial_schema"],
            conflicts=["old_users"],
        )

        result = context.to_dict()
        expected = {
            "project": "myproject",
            "change": "add_users",
            "engine": "pg",
            "requires": ["initial_schema"],
            "conflicts": ["old_users"],
        }

        assert result == expected


class TestBuiltinTemplateLoader:
    """Test BuiltinTemplateLoader class."""

    def test_init(self):
        """Test loader initialization."""
        loader = BuiltinTemplateLoader()
        assert isinstance(loader.templates, dict)
        assert len(loader.templates) > 0

    def test_has_postgresql_templates(self):
        """Test that PostgreSQL templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/pg.tmpl" in loader.templates
        assert "revert/pg.tmpl" in loader.templates
        assert "verify/pg.tmpl" in loader.templates

    def test_has_mysql_templates(self):
        """Test that MySQL templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/mysql.tmpl" in loader.templates
        assert "revert/mysql.tmpl" in loader.templates
        assert "verify/mysql.tmpl" in loader.templates

    def test_has_sqlite_templates(self):
        """Test that SQLite templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/sqlite.tmpl" in loader.templates
        assert "revert/sqlite.tmpl" in loader.templates
        assert "verify/sqlite.tmpl" in loader.templates

    def test_has_oracle_templates(self):
        """Test that Oracle templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/oracle.tmpl" in loader.templates
        assert "revert/oracle.tmpl" in loader.templates
        assert "verify/oracle.tmpl" in loader.templates

    def test_has_snowflake_templates(self):
        """Test that Snowflake templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/snowflake.tmpl" in loader.templates
        assert "revert/snowflake.tmpl" in loader.templates
        assert "verify/snowflake.tmpl" in loader.templates

    def test_has_vertica_templates(self):
        """Test that Vertica templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/vertica.tmpl" in loader.templates
        assert "revert/vertica.tmpl" in loader.templates
        assert "verify/vertica.tmpl" in loader.templates

    def test_has_exasol_templates(self):
        """Test that Exasol templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/exasol.tmpl" in loader.templates
        assert "revert/exasol.tmpl" in loader.templates
        assert "verify/exasol.tmpl" in loader.templates

    def test_has_firebird_templates(self):
        """Test that Firebird templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/firebird.tmpl" in loader.templates
        assert "revert/firebird.tmpl" in loader.templates
        assert "verify/firebird.tmpl" in loader.templates

    def test_has_cockroach_templates(self):
        """Test that CockroachDB templates are available."""
        loader = BuiltinTemplateLoader()

        assert "deploy/cockroach.tmpl" in loader.templates
        assert "revert/cockroach.tmpl" in loader.templates
        assert "verify/cockroach.tmpl" in loader.templates

    def test_get_source_existing_template(self):
        """Test getting source for existing template."""
        loader = BuiltinTemplateLoader()

        source, uptodate, _ = loader.get_source(None, "deploy/pg.tmpl")

        assert isinstance(source, str)
        assert "{{ project }}" in source  # Should be converted from TT syntax
        assert "{{ change }}" in source
        assert "{{ engine }}" in source
        assert uptodate is None

    def test_get_source_nonexistent_template(self):
        """Test getting source for non-existent template."""
        loader = BuiltinTemplateLoader()

        with pytest.raises(TemplateError, match="Template not found"):
            loader.get_source(None, "nonexistent.tmpl")

    def test_convert_tt_to_jinja2_variables(self):
        """Test Template Toolkit to Jinja2 variable conversion."""
        loader = BuiltinTemplateLoader()

        tt_content = "Deploy [% project %]:[% change %] to [% engine %]"
        jinja2_content = loader._convert_tt_to_jinja2(tt_content)

        expected = "Deploy {{ project }}:{{ change }} to {{ engine }}"
        assert jinja2_content == expected

    def test_convert_tt_to_jinja2_foreach(self):
        """Test Template Toolkit to Jinja2 FOREACH conversion."""
        loader = BuiltinTemplateLoader()

        tt_content = """[% FOREACH item IN requires -%]
-- requires: [% item %]
[% END -%]"""

        jinja2_content = loader._convert_tt_to_jinja2(tt_content)

        expected = """{% for item in requires %}
-- requires: {{ item }}
{% endfor %}"""

        assert jinja2_content == expected


class TestTemplateEngine:
    """Test TemplateEngine class."""

    @patch("sqlitch.utils.template.JINJA2_AVAILABLE", False)
    def test_init_without_jinja2(self):
        """Test initialization without Jinja2."""
        with pytest.raises(TemplateError, match="Jinja2 is required"):
            TemplateEngine()

    def test_init_with_defaults(self):
        """Test initialization with defaults."""
        engine = TemplateEngine()

        assert engine.template_dirs == []
        assert engine.env is not None

    def test_init_with_custom_dirs(self):
        """Test initialization with custom template directories."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            engine = TemplateEngine([template_dir])

            assert engine.template_dirs == [template_dir]
            assert engine.env is not None

    def test_render_template_builtin(self):
        """Test rendering built-in template."""
        engine = TemplateEngine()
        context = TemplateContext(project="myproject", change="add_users", engine="pg")

        result = engine.render_template("deploy/pg.tmpl", context)

        assert "Deploy myproject:add_users to pg" in result
        assert "BEGIN;" in result
        assert "COMMIT;" in result

    def test_render_template_with_requires(self):
        """Test rendering template with requires."""
        engine = TemplateEngine()
        context = TemplateContext(
            project="myproject",
            change="add_users",
            engine="pg",
            requires=["initial_schema", "permissions"],
        )

        result = engine.render_template("deploy/pg.tmpl", context)

        assert "Deploy myproject:add_users to pg" in result
        assert "-- requires: initial_schema" in result
        assert "-- requires: permissions" in result

    def test_render_template_with_conflicts(self):
        """Test rendering template with conflicts."""
        engine = TemplateEngine()
        context = TemplateContext(
            project="myproject",
            change="add_users",
            engine="pg",
            conflicts=["old_users", "legacy_auth"],
        )

        result = engine.render_template("deploy/pg.tmpl", context)

        assert "Deploy myproject:add_users to pg" in result
        assert "-- conflicts: old_users" in result
        assert "-- conflicts: legacy_auth" in result

    def test_render_template_custom(self):
        """Test rendering custom template."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create custom template
            custom_template = template_dir / "deploy" / "custom.tmpl"
            custom_template.parent.mkdir()
            custom_template.write_text("Custom template for {{ project }}/{{ change }}")

            engine = TemplateEngine([template_dir])
            context = TemplateContext(
                project="myproject", change="add_users", engine="custom"
            )

            result = engine.render_template("deploy/custom.tmpl", context)

            assert result == "Custom template for myproject/add_users"

    def test_render_template_nonexistent(self):
        """Test rendering non-existent template."""
        engine = TemplateEngine()
        context = TemplateContext(project="myproject", change="add_users", engine="pg")

        with pytest.raises(TemplateError, match="Failed to render template"):
            engine.render_template("nonexistent.tmpl", context)

    def test_get_template_path(self):
        """Test getting template path."""
        engine = TemplateEngine()

        path = engine.get_template_path("deploy", "pg")
        assert path == "deploy/pg.tmpl"

        path = engine.get_template_path("revert", "mysql")
        assert path == "revert/mysql.tmpl"

        path = engine.get_template_path("verify", "sqlite")
        assert path == "verify/sqlite.tmpl"

    def test_template_exists_builtin(self):
        """Test checking if built-in template exists."""
        engine = TemplateEngine()

        assert engine.template_exists("deploy/pg.tmpl")
        assert engine.template_exists("revert/mysql.tmpl")
        assert engine.template_exists("verify/sqlite.tmpl")
        assert not engine.template_exists("nonexistent.tmpl")

    def test_template_exists_custom(self):
        """Test checking if custom template exists."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create custom template
            custom_template = template_dir / "deploy" / "custom.tmpl"
            custom_template.parent.mkdir()
            custom_template.write_text("Custom template")

            engine = TemplateEngine([template_dir])

            assert engine.template_exists("deploy/custom.tmpl")
            assert not engine.template_exists("deploy/missing.tmpl")

    def test_list_templates_builtin_only(self):
        """Test listing built-in templates only."""
        engine = TemplateEngine()

        templates = engine.list_templates()

        assert "deploy/pg.tmpl" in templates
        assert "revert/pg.tmpl" in templates
        assert "verify/pg.tmpl" in templates
        assert "deploy/mysql.tmpl" in templates
        assert "deploy/sqlite.tmpl" in templates
        assert len(templates) >= 9  # At least 3 engines × 3 operations

    def test_list_templates_with_custom(self):
        """Test listing templates with custom directory."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create custom templates
            (template_dir / "deploy").mkdir()
            (template_dir / "deploy" / "custom.tmpl").write_text("Custom deploy")
            (template_dir / "revert").mkdir()
            (template_dir / "revert" / "custom.tmpl").write_text("Custom revert")

            engine = TemplateEngine([template_dir])
            templates = engine.list_templates()

            assert "deploy/custom.tmpl" in templates
            assert "revert/custom.tmpl" in templates
            assert "deploy/pg.tmpl" in templates  # Built-in still available


class TestTemplateFunctions:
    """Test module-level template functions."""

    def test_create_template_engine_default(self):
        """Test creating template engine with defaults."""
        engine = create_template_engine()

        assert isinstance(engine, TemplateEngine)
        assert engine.template_dirs == []

    def test_create_template_engine_with_dirs(self):
        """Test creating template engine with custom directories."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            engine = create_template_engine([template_dir])

            assert isinstance(engine, TemplateEngine)
            assert engine.template_dirs == [template_dir]

    def test_render_change_template_deploy(self):
        """Test rendering change template for deploy."""
        result = render_change_template(
            operation="deploy", engine="pg", project="myproject", change="add_users"
        )

        assert "Deploy myproject:add_users to pg" in result
        assert "BEGIN;" in result
        assert "COMMIT;" in result

    def test_render_change_template_revert(self):
        """Test rendering change template for revert."""
        result = render_change_template(
            operation="revert", engine="pg", project="myproject", change="add_users"
        )

        assert "Revert myproject:add_users from pg" in result
        assert "BEGIN;" in result
        assert "COMMIT;" in result

    def test_render_change_template_verify(self):
        """Test rendering change template for verify."""
        result = render_change_template(
            operation="verify", engine="pg", project="myproject", change="add_users"
        )

        assert "Verify myproject:add_users on pg" in result
        assert "BEGIN;" in result
        assert "ROLLBACK;" in result

    def test_render_change_template_with_dependencies(self):
        """Test rendering change template with dependencies."""
        result = render_change_template(
            operation="deploy",
            engine="pg",
            project="myproject",
            change="add_users",
            requires=["initial_schema"],
            conflicts=["old_users"],
        )

        assert "Deploy myproject:add_users to pg" in result
        assert "-- requires: initial_schema" in result
        assert "-- conflicts: old_users" in result

    def test_render_change_template_mysql(self):
        """Test rendering change template for MySQL."""
        result = render_change_template(
            operation="deploy", engine="mysql", project="myproject", change="add_users"
        )

        assert "Deploy myproject:add_users to mysql" in result
        # MySQL templates use transactions
        assert "BEGIN;" in result
        assert "COMMIT;" in result

    def test_render_change_template_sqlite(self):
        """Test rendering change template for SQLite."""
        result = render_change_template(
            operation="deploy", engine="sqlite", project="myproject", change="add_users"
        )

        assert "Deploy myproject:add_users to sqlite" in result

    def test_render_change_template_with_custom_dirs(self):
        """Test rendering change template with custom directories."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create custom template that overrides built-in
            custom_template = template_dir / "deploy" / "pg.tmpl"
            custom_template.parent.mkdir()
            custom_template.write_text(
                "Custom PostgreSQL deploy for {{ project }}/{{ change }}"
            )

            result = render_change_template(
                operation="deploy",
                engine="pg",
                project="myproject",
                change="add_users",
                template_dirs=[template_dir],
            )

            assert result == "Custom PostgreSQL deploy for myproject/add_users"


class TestTemplateIntegration:
    """Integration tests for template system."""

    def test_template_directory_precedence(self):
        """Test that custom templates override built-in templates."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create custom template that overrides built-in
            custom_template = template_dir / "deploy" / "pg.tmpl"
            custom_template.parent.mkdir()
            custom_template.write_text("CUSTOM: {{ project }}/{{ change }}")

            engine = TemplateEngine([template_dir])
            context = TemplateContext(
                project="myproject", change="add_users", engine="pg"
            )

            result = engine.render_template("deploy/pg.tmpl", context)

            assert result == "CUSTOM: myproject/add_users"

    def test_multiple_template_directories(self):
        """Test multiple custom template directories."""
        with TemporaryDirectory() as temp_dir:
            template_dir1 = Path(temp_dir) / "templates1"
            template_dir1.mkdir()
            template_dir2 = Path(temp_dir) / "templates2"
            template_dir2.mkdir()

            # Create templates in different directories
            (template_dir1 / "deploy").mkdir()
            (template_dir1 / "deploy" / "custom1.tmpl").write_text(
                "Template 1: {{ change }}"
            )

            (template_dir2 / "deploy").mkdir()
            (template_dir2 / "deploy" / "custom2.tmpl").write_text(
                "Template 2: {{ change }}"
            )

            engine = TemplateEngine([template_dir1, template_dir2])
            context = TemplateContext(
                project="myproject", change="add_users", engine="custom"
            )

            result1 = engine.render_template("deploy/custom1.tmpl", context)
            result2 = engine.render_template("deploy/custom2.tmpl", context)

            assert result1 == "Template 1: add_users"
            assert result2 == "Template 2: add_users"

    def test_template_with_complex_jinja2_syntax(self):
        """Test template with complex Jinja2 syntax."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create template with complex Jinja2 syntax
            complex_template = template_dir / "deploy" / "complex.tmpl"
            complex_template.parent.mkdir()
            complex_template.write_text(
                """
-- Deploy {{ project }}:{{ change }}
{% if requires %}
-- Dependencies:
{% for req in requires %}
--   {{ req }}
{% endfor %}
{% endif %}
{% if conflicts %}
-- Conflicts:
{% for conf in conflicts %}
--   {{ conf }}
{% endfor %}
{% endif %}

CREATE TABLE {{ change }}_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
""".strip()
            )

            engine = TemplateEngine([template_dir])
            context = TemplateContext(
                project="myproject",
                change="add_users",
                engine="complex",
                requires=["schema", "permissions"],
                conflicts=["old_users"],
            )

            result = engine.render_template("deploy/complex.tmpl", context)

            assert "Deploy myproject:add_users" in result
            assert "-- Dependencies:" in result
            assert "--   schema" in result
            assert "--   permissions" in result
            assert "-- Conflicts:" in result
            assert "--   old_users" in result
            assert "CREATE TABLE add_users_table" in result


class TestTemplateErrorHandling:
    """Test template error handling."""

    def test_template_syntax_error(self):
        """Test handling of template syntax errors."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create template with syntax error
            bad_template = template_dir / "deploy" / "bad.tmpl"
            bad_template.parent.mkdir()
            bad_template.write_text("{{ unclosed_variable")

            engine = TemplateEngine([template_dir])
            context = TemplateContext(
                project="myproject", change="add_users", engine="bad"
            )

            with pytest.raises(TemplateError, match="Failed to render template"):
                engine.render_template("deploy/bad.tmpl", context)

    def test_missing_template_variable(self):
        """Test handling of missing template variables."""
        with TemporaryDirectory() as temp_dir:
            template_dir = Path(temp_dir) / "templates"
            template_dir.mkdir()

            # Create template with undefined variable - Jinja2 renders undefined as empty by default
            undefined_template = template_dir / "deploy" / "undefined.tmpl"
            undefined_template.parent.mkdir()
            undefined_template.write_text("{{ undefined_variable }}")

            engine = TemplateEngine([template_dir])
            context = TemplateContext(
                project="myproject", change="add_users", engine="undefined"
            )

            # This should not raise an error, just render empty string
            result = engine.render_template("deploy/undefined.tmpl", context)
            assert result == ""
