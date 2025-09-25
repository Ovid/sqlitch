"""
Unit tests for the verify command.

Tests the VerifyCommand class functionality including argument parsing,
verification execution, parallel processing, and error reporting.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from sqlitch.commands.verify import VerificationResult, VerifyCommand
from sqlitch.core.change import Change, Dependency
from sqlitch.core.exceptions import PlanError, SqlitchError
from sqlitch.core.plan import Plan


@pytest.fixture
def mock_sqitch():
    """Create mock Sqitch instance."""
    sqitch = Mock()
    sqitch.config = Mock()
    sqitch.logger = Mock()
    sqitch.verbosity = 0
    sqitch.get_plan_file.return_value = Path("sqitch.plan")
    sqitch.require_initialized.return_value = None
    return sqitch


@pytest.fixture
def verify_command(mock_sqitch):
    """Create VerifyCommand instance."""
    return VerifyCommand(mock_sqitch)


@pytest.fixture
def sample_plan():
    """Create sample plan with changes."""
    changes = [
        Change(
            name="users",
            note="Add users table",
            timestamp=datetime(2023, 1, 15, 10, 30, tzinfo=timezone.utc),
            planner_name="John Doe",
            planner_email="john@example.com",
            dependencies=[],
            tags=[],
        ),
        Change(
            name="posts",
            note="Add posts table",
            timestamp=datetime(2023, 1, 16, 14, 20, tzinfo=timezone.utc),
            planner_name="Jane Smith",
            planner_email="jane@example.com",
            dependencies=[Dependency(type="require", change="users")],
            tags=["v1.0"],
        ),
        Change(
            name="comments",
            note="Add comments table",
            timestamp=datetime(2023, 1, 17, 9, 15, tzinfo=timezone.utc),
            planner_name="Bob Wilson",
            planner_email="bob@example.com",
            dependencies=[Dependency(type="require", change="posts")],
            tags=[],
        ),
    ]

    plan = Mock(spec=Plan)
    plan.changes = changes
    plan.project_name = "test_project"
    return plan


@pytest.fixture
def mock_engine():
    """Create mock database engine."""
    engine = Mock()
    engine.target = Mock()
    engine.target.name = "test_db"
    engine.ensure_registry = Mock()
    engine.get_deployed_changes = Mock(return_value=["change1", "change2"])
    engine.verify_change = Mock(return_value=True)
    return engine


class TestVerifyCommand:
    """Test cases for VerifyCommand."""

    def test_init(self, mock_sqitch):
        """Test command initialization."""
        command = VerifyCommand(mock_sqitch)
        assert command.sqitch == mock_sqitch
        assert command.config == mock_sqitch.config
        assert command.logger == mock_sqitch.logger
        assert hasattr(command, "_lock")

    def test_parse_args_defaults(self, verify_command):
        """Test parsing with default arguments."""
        options = verify_command._parse_args([])

        assert options["target"] is None
        assert options["plan_file"] is None
        assert options["from_change"] is None
        assert options["to_change"] is None
        assert options["variables"] == {}
        assert options["parallel"] is True
        assert options["max_workers"] is None

    def test_parse_args_target(self, verify_command):
        """Test parsing target option."""
        options = verify_command._parse_args(["--target", "prod"])
        assert options["target"] == "prod"

        options = verify_command._parse_args(["-t", "dev"])
        assert options["target"] == "dev"

    def test_parse_args_plan_file(self, verify_command):
        """Test parsing plan file option."""
        options = verify_command._parse_args(["--plan-file", "custom.plan"])
        assert options["plan_file"] == Path("custom.plan")

    def test_parse_args_from_to_change(self, verify_command):
        """Test parsing from/to change options."""
        options = verify_command._parse_args(
            ["--from-change", "users", "--to-change", "posts"]
        )
        assert options["from_change"] == "users"
        assert options["to_change"] == "posts"

        options = verify_command._parse_args(["--from", "users", "--to", "posts"])
        assert options["from_change"] == "users"
        assert options["to_change"] == "posts"

    def test_parse_args_positional(self, verify_command):
        """Test parsing positional arguments."""
        options = verify_command._parse_args(["users", "posts"])
        assert options["from_change"] == "users"
        assert options["to_change"] == "posts"

    def test_parse_args_variables(self, verify_command):
        """Test parsing variable assignments."""
        options = verify_command._parse_args(
            ["--set", "key1=value1", "-s", "key2=value2"]
        )
        assert options["variables"] == {"key1": "value1", "key2": "value2"}

    def test_parse_args_parallel_options(self, verify_command):
        """Test parsing parallel options."""
        options = verify_command._parse_args(["--no-parallel"])
        assert options["parallel"] is False

        options = verify_command._parse_args(["--parallel"])
        assert options["parallel"] is True

        options = verify_command._parse_args(["--max-workers", "8"])
        assert options["max_workers"] == 8

    def test_parse_args_help(self, verify_command):
        """Test help option."""
        with pytest.raises(SystemExit):
            verify_command._parse_args(["--help"])

        with pytest.raises(SystemExit):
            verify_command._parse_args(["-h"])

    def test_parse_args_invalid_option(self, verify_command):
        """Test invalid option handling."""
        with pytest.raises(SqlitchError, match="Unknown option"):
            verify_command._parse_args(["--invalid"])

    def test_parse_args_missing_value(self, verify_command):
        """Test missing option values."""
        with pytest.raises(SqlitchError, match="--target requires a value"):
            verify_command._parse_args(["--target"])

        with pytest.raises(SqlitchError, match="--set requires a value"):
            verify_command._parse_args(["--set"])

        with pytest.raises(SqlitchError, match="--max-workers must be an integer"):
            verify_command._parse_args(["--max-workers", "invalid"])

    def test_parse_args_invalid_variable_format(self, verify_command):
        """Test invalid variable format."""
        with pytest.raises(SqlitchError, match="--set requires format key=value"):
            verify_command._parse_args(["--set", "invalid"])

    def test_load_plan_default(self, verify_command):
        """Test loading plan with default file."""
        with patch("sqlitch.core.plan.Plan.from_file") as mock_from_file:
            mock_plan = Mock()
            mock_from_file.return_value = mock_plan

            with patch("pathlib.Path.exists", return_value=True):
                result = verify_command._load_plan()

                assert result == mock_plan
                mock_from_file.assert_called_once_with(Path("sqitch.plan"))

    def test_load_plan_custom_file(self, verify_command):
        """Test loading plan with custom file."""
        custom_file = Path("custom.plan")

        with patch("sqlitch.core.plan.Plan.from_file") as mock_from_file:
            mock_plan = Mock()
            mock_from_file.return_value = mock_plan

            with patch("pathlib.Path.exists", return_value=True):
                result = verify_command._load_plan(custom_file)

                assert result == mock_plan
                mock_from_file.assert_called_once_with(custom_file)

    def test_load_plan_file_not_found(self, verify_command):
        """Test loading non-existent plan file."""
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(PlanError, match="Plan file not found"):
                verify_command._load_plan()

    def test_load_plan_parse_error(self, verify_command):
        """Test plan file parsing error."""
        with patch("pathlib.Path.exists", return_value=True):
            with patch(
                "sqlitch.core.plan.Plan.from_file", side_effect=Exception("Parse error")
            ):
                with pytest.raises(PlanError, match="Failed to load plan file"):
                    verify_command._load_plan()

    def test_determine_verification_range_defaults(self, verify_command, sample_plan):
        """Test determining verification range with defaults."""
        deployed_changes = sample_plan.changes[:2]  # First two changes
        options = {}

        from_idx, to_idx = verify_command._determine_verification_range(
            sample_plan, deployed_changes, options
        )

        assert from_idx == 0
        assert to_idx == 1

    def test_determine_verification_range_from_change(
        self, verify_command, sample_plan
    ):
        """Test determining verification range with from_change."""
        deployed_changes = sample_plan.changes
        options = {"from_change": "posts"}

        from_idx, to_idx = verify_command._determine_verification_range(
            sample_plan, deployed_changes, options
        )

        assert from_idx == 1  # posts is at index 1
        assert to_idx == 2  # last change

    def test_determine_verification_range_to_change(self, verify_command, sample_plan):
        """Test determining verification range with to_change."""
        deployed_changes = sample_plan.changes
        options = {"to_change": "posts"}

        from_idx, to_idx = verify_command._determine_verification_range(
            sample_plan, deployed_changes, options
        )

        assert from_idx == 0  # first change
        assert to_idx == 1  # posts is at index 1

    def test_determine_verification_range_change_not_deployed(
        self, verify_command, sample_plan
    ):
        """Test error when specified change is not deployed."""
        deployed_changes = sample_plan.changes[:1]  # Only first change deployed
        options = {"from_change": "posts"}  # posts not deployed

        with pytest.raises(SqlitchError, match='Change "posts" has not been deployed'):
            verify_command._determine_verification_range(
                sample_plan, deployed_changes, options
            )

    def test_determine_verification_range_change_not_found(
        self, verify_command, sample_plan
    ):
        """Test error when specified change is not found."""
        deployed_changes = sample_plan.changes
        options = {"from_change": "nonexistent"}

        with pytest.raises(SqlitchError, match='Cannot find "nonexistent"'):
            verify_command._determine_verification_range(
                sample_plan, deployed_changes, options
            )

    def test_verify_single_change_success(self, verify_command, sample_plan):
        """Test successful verification of single change."""
        change = sample_plan.changes[0]

        with patch.object(verify_command, "_verify_single_change") as mock_verify:
            mock_engine = Mock()
            mock_engine.verify_change.return_value = True

            result = VerificationResult(change, True)
            mock_verify.return_value = result

            actual_result = verify_command._verify_single_change(
                mock_engine, sample_plan, change, 0, {}
            )

            assert actual_result.change == change
            assert actual_result.success is True

    def test_verify_single_change_failure(self, verify_command, sample_plan):
        """Test failed verification of single change."""
        change = sample_plan.changes[0]

        mock_engine = Mock()
        mock_engine.verify_change.return_value = False

        result = verify_command._verify_single_change(
            mock_engine, sample_plan, change, 0, {}
        )

        assert result.change == change
        assert result.success is False
        assert result.error == "Verification script failed"

    def test_verify_single_change_exception(self, verify_command, sample_plan):
        """Test verification with exception."""
        change = sample_plan.changes[0]

        mock_engine = Mock()
        mock_engine.verify_change.side_effect = Exception("Database error")

        result = verify_command._verify_single_change(
            mock_engine, sample_plan, change, 0, {}
        )

        assert result.change == change
        assert result.success is False
        assert result.error == "Database error"

    def test_verify_single_change_not_in_plan(self, verify_command, sample_plan):
        """Test verification of change not in plan."""
        # Create a change with different ID
        change = Change(
            name="orphan",
            note="Orphaned change",
            timestamp=datetime.now(timezone.utc),
            planner_name="Unknown",
            planner_email="unknown@example.com",
        )

        mock_engine = Mock()
        mock_engine.verify_change.return_value = True

        result = verify_command._verify_single_change(
            mock_engine, sample_plan, change, 0, {}
        )

        assert result.not_in_plan is True
        assert result.has_errors is True

    def test_run_sequential_verifications(self, verify_command, sample_plan):
        """Test sequential verification execution."""
        changes = sample_plan.changes[:2]
        mock_engine = Mock()

        with patch.object(verify_command, "_verify_single_change") as mock_verify:
            with patch.object(verify_command, "_emit_verification_result") as mock_emit:
                # Mock verification results
                results = [
                    VerificationResult(changes[0], True),
                    VerificationResult(changes[1], True),
                ]
                mock_verify.side_effect = results

                actual_results = verify_command._run_sequential_verifications(
                    mock_engine, sample_plan, changes, {}, 10
                )

                assert len(actual_results) == 2
                assert mock_verify.call_count == 2
                assert mock_emit.call_count == 2

    def test_run_parallel_verifications(self, verify_command, sample_plan):
        """Test parallel verification execution."""
        changes = sample_plan.changes[:2]
        mock_engine = Mock()

        with patch.object(verify_command, "_verify_single_change") as mock_verify:
            with patch.object(verify_command, "_emit_verification_result"):
                # Mock verification results
                results = [
                    VerificationResult(changes[0], True),
                    VerificationResult(changes[1], True),
                ]
                mock_verify.side_effect = results

                actual_results = verify_command._run_parallel_verifications(
                    mock_engine, sample_plan, changes, {"max_workers": 2}, 10
                )

                assert len(actual_results) == 2
                assert mock_verify.call_count == 2
                # Note: emit might be called in different order due to threading

    def test_emit_verification_result_success(self, verify_command, sample_plan):
        """Test emitting successful verification result."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, True)

        with patch.object(verify_command, "info") as mock_info:
            verify_command._emit_verification_result(result, 10)

            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "users" in call_args
            assert "ok" in call_args
            assert "not ok" not in call_args

    def test_emit_verification_result_failure(self, verify_command, sample_plan):
        """Test emitting failed verification result."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, False, error="Script failed")

        with patch.object(verify_command, "info") as mock_info:
            verify_command._emit_verification_result(result, 10)

            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "users" in call_args
            assert "not ok" in call_args
            assert "Script failed" in call_args

    def test_emit_verification_result_out_of_order(self, verify_command, sample_plan):
        """Test emitting out of order verification result."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, True, out_of_order=True)

        with patch.object(verify_command, "info") as mock_info:
            verify_command._emit_verification_result(result, 10)

            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "users" in call_args
            assert "not ok" in call_args
            assert "Out of order" in call_args

    def test_check_undeployed_changes(self, verify_command, sample_plan):
        """Test checking for undeployed changes."""
        deployed_change_ids = {sample_plan.changes[0].id}  # Only first change deployed

        results = verify_command._check_undeployed_changes(
            sample_plan, deployed_change_ids, 0, 2
        )

        # Should find posts and comments as undeployed
        assert len(results) >= 1
        undeployed_names = [r.change.name for r in results if r.not_deployed]
        assert "posts" in undeployed_names or "comments" in undeployed_names

    def test_report_results_success(self, verify_command, sample_plan):
        """Test reporting successful results."""
        results = [
            VerificationResult(sample_plan.changes[0], True),
            VerificationResult(sample_plan.changes[1], True),
        ]

        with patch.object(verify_command, "info") as mock_info:
            exit_code = verify_command._report_results(results, sample_plan, 0, 1, {})

            assert exit_code == 0
            # Should call info with "Verify successful"
            success_calls = [
                call
                for call in mock_info.call_args_list
                if "Verify successful" in str(call)
            ]
            assert len(success_calls) == 1

    def test_report_results_failure(self, verify_command, sample_plan):
        """Test reporting failed results."""
        results = [
            VerificationResult(sample_plan.changes[0], True),
            VerificationResult(sample_plan.changes[1], False, error="Failed"),
        ]

        with patch.object(verify_command, "info") as mock_info:
            with pytest.raises(SqlitchError, match="Verify failed"):
                verify_command._report_results(results, sample_plan, 0, 1, {})

            # Should show summary report
            summary_calls = [
                call
                for call in mock_info.call_args_list
                if "Verify Summary Report" in str(call)
            ]
            assert len(summary_calls) == 1

    def test_execute_success(self, verify_command, sample_plan, mock_engine):
        """Test successful command execution."""
        with patch.object(verify_command, "_parse_args", return_value={}):
            with patch.object(verify_command, "_load_plan", return_value=sample_plan):
                with patch.object(verify_command, "get_target", return_value=Mock()):
                    with patch(
                        "sqlitch.engines.base.EngineRegistry.create_engine",
                        return_value=mock_engine,
                    ):
                        with patch.object(
                            verify_command, "_verify_changes", return_value=0
                        ):
                            result = verify_command.execute([])
                            assert result == 0

    def test_execute_sqlitch_error(self, verify_command):
        """Test command execution with SqlitchError."""
        with patch.object(
            verify_command, "_parse_args", side_effect=SqlitchError("Test error")
        ):
            result = verify_command.execute([])
            assert result == 1

    def test_execute_unexpected_error(self, verify_command):
        """Test command execution with unexpected error."""
        with patch.object(
            verify_command, "_parse_args", side_effect=Exception("Unexpected")
        ):
            result = verify_command.execute([])
            assert result == 2

    def test_verify_changes_no_deployed(self, verify_command, sample_plan, mock_engine):
        """Test verification with no deployed changes."""
        mock_engine.get_deployed_changes.return_value = []

        with patch.object(verify_command, "info") as mock_info:
            result = verify_command._verify_changes(mock_engine, sample_plan, {})

            assert result == 0
            mock_info.assert_called_with("No changes deployed")

    def test_verify_changes_no_plan(self, verify_command, mock_engine):
        """Test verification with deployed changes but no plan."""
        mock_engine.get_deployed_changes.return_value = ["change1"]
        empty_plan = Mock()
        empty_plan.changes = []

        with pytest.raises(
            SqlitchError, match="There are deployed changes, but none planned"
        ):
            verify_command._verify_changes(mock_engine, empty_plan, {})

    def test_show_help(self, verify_command, capsys):
        """Test help display."""
        verify_command._show_help()
        captured = capsys.readouterr()

        assert "Usage: sqlitch verify" in captured.out
        assert "Verify deployed database changes" in captured.out
        assert "--target" in captured.out
        assert "--parallel" in captured.out


class TestVerificationResult:
    """Test cases for VerificationResult."""

    def test_init_success(self, sample_plan):
        """Test successful result initialization."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, True)

        assert result.change == change
        assert result.success is True
        assert result.error is None
        assert result.has_errors is False

    def test_init_failure(self, sample_plan):
        """Test failed result initialization."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, False, error="Test error")

        assert result.change == change
        assert result.success is False
        assert result.error == "Test error"
        assert result.has_errors is True

    def test_has_errors_out_of_order(self, sample_plan):
        """Test has_errors with out of order flag."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, True, out_of_order=True)

        assert result.has_errors is True

    def test_has_errors_not_in_plan(self, sample_plan):
        """Test has_errors with not in plan flag."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, True, not_in_plan=True)

        assert result.has_errors is True

    def test_has_errors_not_deployed(self, sample_plan):
        """Test has_errors with not deployed flag."""
        change = sample_plan.changes[0]
        result = VerificationResult(change, True, not_deployed=True)

        assert result.has_errors is True
