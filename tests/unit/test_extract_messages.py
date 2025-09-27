"""
Unit tests for the message extraction functionality.

This module tests the AST-based message extraction system used to generate
POT files for internationalization.
"""

import ast
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlitch.i18n.extract_messages import (
    MessageExtractor,
    escape_po_string,
    extract_messages_from_codebase,
    find_python_files,
    generate_pot_file,
    main,
    update_po_file,
)


class TestMessageExtractor:
    """Test the MessageExtractor AST visitor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = MessageExtractor()

    def test_init(self):
        """Test MessageExtractor initialization."""
        assert self.extractor.messages == set()
        assert self.extractor.current_file == ""
        assert self.extractor.current_line == 0

    def test_extract_simple_translation(self, tmp_path):
        """Test extraction of simple __ translation calls."""
        test_file = tmp_path / "test.py"
        test_file.write_text(
            """
from sqlitch.i18n import __

def test_func():
    message = __("Simple message")
    return message
"""
        )

        self.extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in self.extractor.messages]
        assert "Simple message" in messages

    def test_extract_parameterized_translation(self, tmp_path):
        """Test extraction of __x parameterized translation calls."""
        test_file = tmp_path / "test.py"
        test_file.write_text(
            """
from sqlitch.i18n import __x

def test_func():
    message = __x("Hello {name}", name="World")
    return message
"""
        )

        self.extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in self.extractor.messages]
        assert "Hello {name}" in messages

    def test_extract_plural_translation(self, tmp_path):
        """Test extraction of __n plural translation calls."""
        test_file = tmp_path / "test.py"
        test_file.write_text(
            """
from sqlitch.i18n import __n

def test_func():
    message = __n("One item", "{count} items", 5, count=5)
    return message
"""
        )

        self.extractor.extract_from_file(test_file)

        # Should extract both singular and plural forms
        messages = list(self.extractor.messages)
        singular_plural = [msg for msg in messages if msg[1] is not None]
        assert len(singular_plural) > 0
        assert ("One item", "{count} items") in messages

    def test_extract_multiple_messages(self, tmp_path):
        """Test extraction of multiple different message types."""
        test_file = tmp_path / "test.py"
        test_file.write_text(
            """
from sqlitch.i18n import __, __x, __n

def test_func():
    msg1 = __("Simple message")
    msg2 = __x("Param message {param}", param="value")
    msg3 = __n("One file", "{count} files", 2, count=2)
    return msg1, msg2, msg3
"""
        )

        self.extractor.extract_from_file(test_file)

        assert len(self.extractor.messages) >= 3
        messages = [msg[0] for msg in self.extractor.messages]
        assert "Simple message" in messages
        assert "Param message {param}" in messages
        assert "One file" in messages

    def test_extract_non_string_arguments(self, tmp_path):
        """Test that non-string arguments are ignored."""
        test_file = tmp_path / "test.py"
        test_file.write_text(
            """
from sqlitch.i18n import __

def test_func():
    # These should be ignored
    message1 = __(variable_name)
    message2 = __(123)
    message3 = __("Valid message")
    return message1, message2, message3
"""
        )

        self.extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in self.extractor.messages]
        assert "Valid message" in messages
        assert len(messages) == 1  # Only the valid string message

    def test_extract_insufficient_arguments(self, tmp_path):
        """Test that calls with insufficient arguments are ignored."""
        test_file = tmp_path / "test.py"
        test_file.write_text(
            """
from sqlitch.i18n import __, __x, __n

def test_func():
    # These should be ignored due to insufficient arguments
    msg1 = __()
    msg2 = __x()
    msg3 = __n("Only one arg")

    # This should be extracted
    msg4 = __("Valid message")
    return msg1, msg2, msg3, msg4
"""
        )

        self.extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in self.extractor.messages]
        assert "Valid message" in messages
        assert len(messages) == 1

    def test_extract_from_syntax_error_file(self, tmp_path):
        """Test handling of files with syntax errors."""
        test_file = tmp_path / "bad_syntax.py"
        test_file.write_text(
            """
def invalid_syntax(
    # Missing closing parenthesis and colon
"""
        )

        # Should not raise exception, just print warning
        with patch("sys.stderr"):
            self.extractor.extract_from_file(test_file)

        # Should have no messages extracted
        assert len(self.extractor.messages) == 0

    def test_extract_from_unicode_error_file(self, tmp_path):
        """Test handling of files with unicode decode errors."""
        test_file = tmp_path / "bad_encoding.py"
        # Write invalid UTF-8 bytes
        test_file.write_bytes(b"\xff\xfe# Invalid UTF-8")

        # Should not raise exception, just print warning
        with patch("sys.stderr"):
            self.extractor.extract_from_file(test_file)

        # Should have no messages extracted
        assert len(self.extractor.messages) == 0

    def test_visit_call_with_non_name_func(self, tmp_path):
        """Test visit_Call with non-Name function calls."""
        test_file = tmp_path / "test.py"
        test_file.write_text(
            """
class TestClass:
    def method(self):
        # This should be ignored (method call, not function name)
        self.__("Not extracted")

        # This should be extracted
        from sqlitch.i18n import __
        __("Valid message")
"""
        )

        self.extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in self.extractor.messages]
        assert "Valid message" in messages
        assert "Not extracted" not in messages


class TestFindPythonFiles:
    """Test the find_python_files function."""

    def test_find_python_files_basic(self, tmp_path):
        """Test finding Python files in a directory."""
        # Create test files
        (tmp_path / "file1.py").write_text("# Python file 1")
        (tmp_path / "file2.py").write_text("# Python file 2")
        (tmp_path / "not_python.txt").write_text("Not a Python file")

        python_files = find_python_files(tmp_path)

        assert len(python_files) == 2
        file_names = [f.name for f in python_files]
        assert "file1.py" in file_names
        assert "file2.py" in file_names
        assert "not_python.txt" not in file_names

    def test_find_python_files_subdirectories(self, tmp_path):
        """Test finding Python files in subdirectories."""
        # Create nested structure
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        (tmp_path / "root.py").write_text("# Root file")
        (subdir / "sub.py").write_text("# Sub file")

        python_files = find_python_files(tmp_path)

        assert len(python_files) == 2
        file_paths = [str(f) for f in python_files]
        assert any("root.py" in path for path in file_paths)
        assert any("sub.py" in path for path in file_paths)

    def test_find_python_files_skip_hidden_dirs(self, tmp_path):
        """Test that hidden directories are skipped."""
        # Create hidden directory
        hidden_dir = tmp_path / ".hidden"
        hidden_dir.mkdir()

        (tmp_path / "visible.py").write_text("# Visible file")
        (hidden_dir / "hidden.py").write_text("# Hidden file")

        python_files = find_python_files(tmp_path)

        assert len(python_files) == 1
        assert python_files[0].name == "visible.py"

    def test_find_python_files_skip_pycache(self, tmp_path):
        """Test that __pycache__ directories are skipped."""
        # Create __pycache__ directory
        pycache_dir = tmp_path / "__pycache__"
        pycache_dir.mkdir()

        (tmp_path / "source.py").write_text("# Source file")
        (pycache_dir / "compiled.pyc").write_text("# Compiled file")

        python_files = find_python_files(tmp_path)

        assert len(python_files) == 1
        assert python_files[0].name == "source.py"


class TestExtractMessagesFromCodebase:
    """Test the extract_messages_from_codebase function."""

    def test_extract_messages_from_codebase(self, tmp_path):
        """Test extracting messages from a complete codebase."""
        # Create test codebase structure
        (tmp_path / "module1.py").write_text(
            """
from sqlitch.i18n import __
def func1():
    return __("Message from module1")
"""
        )

        (tmp_path / "module2.py").write_text(
            """
from sqlitch.i18n import __x
def func2():
    return __x("Message from {module}", module="module2")
"""
        )

        subdir = tmp_path / "subpackage"
        subdir.mkdir()
        (subdir / "module3.py").write_text(
            """
from sqlitch.i18n import __n
def func3():
    return __n("One message", "{count} messages", 2, count=2)
"""
        )

        with patch("builtins.print"):  # Suppress print output
            messages = extract_messages_from_codebase(tmp_path)

        assert len(messages) >= 3
        message_texts = [msg[0] for msg in messages]
        assert "Message from module1" in message_texts
        assert "Message from {module}" in message_texts
        assert "One message" in message_texts


class TestEscapePoString:
    """Test the escape_po_string function."""

    def test_escape_backslashes(self):
        """Test escaping backslashes."""
        result = escape_po_string("Path\\to\\file")
        assert result == "Path\\\\to\\\\file"

    def test_escape_quotes(self):
        """Test escaping double quotes."""
        result = escape_po_string('Message with "quotes"')
        assert result == 'Message with \\"quotes\\"'

    def test_escape_newlines(self):
        """Test escaping newlines."""
        result = escape_po_string("Line 1\nLine 2")
        assert result == "Line 1\\nLine 2"

    def test_escape_combined(self):
        """Test escaping multiple special characters."""
        result = escape_po_string('Path\\file with "quotes"\nand newlines')
        assert result == 'Path\\\\file with \\"quotes\\"\\nand newlines'

    def test_escape_empty_string(self):
        """Test escaping empty string."""
        result = escape_po_string("")
        assert result == ""

    def test_escape_no_special_chars(self):
        """Test string with no special characters."""
        result = escape_po_string("Simple message")
        assert result == "Simple message"


class TestGeneratePotFile:
    """Test the generate_pot_file function."""

    def test_generate_pot_file_basic(self, tmp_path):
        """Test generating a basic POT file."""
        messages = {
            ("Simple message", None),
            ("Parameterized {param}", None),
            ("One item", "{count} items"),
        }

        pot_file = tmp_path / "test.pot"

        with patch("sqlitch.i18n.extract_messages.datetime") as mock_datetime:
            mock_now = Mock()
            mock_now.strftime.return_value = "2023-12-25 15:30+0000"
            mock_datetime.now.return_value = mock_now
            generate_pot_file(messages, pot_file)

        content = pot_file.read_text(encoding="utf-8")

        # Check header
        assert "Project-Id-Version: sqlitch 1.0.0" in content
        assert "Content-Type: text/plain; charset=UTF-8" in content

        # Check messages
        assert 'msgid "Simple message"' in content
        assert 'msgid "Parameterized {param}"' in content
        assert 'msgid "One item"' in content
        assert 'msgid_plural "{count} items"' in content

        # Check that plural messages have both msgstr[0] and msgstr[1]
        assert 'msgstr[0] ""' in content
        assert 'msgstr[1] ""' in content

    def test_generate_pot_file_with_escaping(self, tmp_path):
        """Test generating POT file with strings that need escaping."""
        messages = {
            ('Message with "quotes"', None),
            ("Message with\nnewlines", None),
            ("Path\\to\\file", None),
        }

        pot_file = tmp_path / "test.pot"
        generate_pot_file(messages, pot_file)

        content = pot_file.read_text(encoding="utf-8")

        # Check escaped content
        assert 'msgid "Message with \\"quotes\\""' in content
        assert 'msgid "Message with\\nnewlines"' in content
        assert 'msgid "Path\\\\to\\\\file"' in content

    def test_generate_pot_file_empty_messages(self, tmp_path):
        """Test generating POT file with no messages."""
        messages = set()

        pot_file = tmp_path / "test.pot"
        generate_pot_file(messages, pot_file)

        content = pot_file.read_text(encoding="utf-8")

        # Should still have header
        assert "Project-Id-Version: sqlitch 1.0.0" in content
        assert "Content-Type: text/plain; charset=UTF-8" in content

        # Should not have any msgid entries (except the empty header one)
        lines = content.split("\n")
        msgid_lines = [
            line
            for line in lines
            if line.startswith("msgid ") and 'msgid ""' not in line
        ]
        assert len(msgid_lines) == 0


class TestUpdatePoFile:
    """Test the update_po_file function."""

    def test_update_po_file_exists(self, tmp_path):
        """Test updating an existing PO file."""
        pot_file = tmp_path / "test.pot"
        po_file = tmp_path / "test.po"

        pot_file.write_text("POT content")
        po_file.write_text("PO content")

        with patch("builtins.print") as mock_print:
            update_po_file(pot_file, po_file)

        # Should print update message
        mock_print.assert_called()
        call_args = [call[0][0] for call in mock_print.call_args_list]
        assert any("Would update" in arg for arg in call_args)

    def test_update_po_file_not_exists(self, tmp_path):
        """Test updating a non-existent PO file."""
        pot_file = tmp_path / "test.pot"
        po_file = tmp_path / "nonexistent.po"

        pot_file.write_text("POT content")

        with patch("builtins.print") as mock_print:
            update_po_file(pot_file, po_file)

        # Should print skip message
        mock_print.assert_called()
        call_args = [call[0][0] for call in mock_print.call_args_list]
        assert any("does not exist" in arg for arg in call_args)


class TestMainFunction:
    """Test the main function."""

    @patch("sqlitch.i18n.extract_messages.extract_messages_from_codebase")
    @patch("sqlitch.i18n.extract_messages.generate_pot_file")
    @patch("sqlitch.i18n.extract_messages.update_po_file")
    @patch("builtins.print")
    def test_main_function(
        self, mock_print, mock_update_po, mock_generate_pot, mock_extract
    ):
        """Test the main function workflow."""
        # Mock the extraction to return some test messages
        mock_extract.return_value = {
            ("Test message", None),
            ("Another message", None),
        }

        # Call main function
        main()

        # Verify the workflow
        mock_extract.assert_called_once()
        mock_generate_pot.assert_called_once()

        # Should call update_po_file for each language
        assert mock_update_po.call_count == 3  # de_DE, fr_FR, it_IT

        # Should print completion message
        mock_print.assert_called()

    def test_main_function_workflow(self):
        """Test that main function executes the complete workflow."""
        with patch(
            "sqlitch.i18n.extract_messages.extract_messages_from_codebase"
        ) as mock_extract:
            with patch(
                "sqlitch.i18n.extract_messages.generate_pot_file"
            ) as mock_generate:
                with patch(
                    "sqlitch.i18n.extract_messages.update_po_file"
                ) as mock_update:
                    with patch("builtins.print"):
                        mock_extract.return_value = {("Test message", None)}
                        main()

        # Verify the complete workflow was executed
        mock_extract.assert_called_once()
        mock_generate.assert_called_once()
        # Should call update for each language (de_DE, fr_FR, it_IT)
        assert mock_update.call_count == 3


class TestIntegrationScenarios:
    """Test integration scenarios and edge cases."""

    def test_complex_ast_structures(self, tmp_path):
        """Test extraction from complex AST structures."""
        test_file = tmp_path / "complex.py"
        test_file.write_text(
            """
from sqlitch.i18n import __, __x, __n

class TestClass:
    def method(self):
        # Nested function calls
        result = some_func(__("Nested message"))

        # Conditional expressions
        msg = __("True message") if condition else __("False message")

        # List comprehensions
        messages = [__("List message {}".format(i)) for i in range(3)]

        # Dictionary values
        config = {
            "error": __("Error message"),
            "success": __("Success message")
        }

        return result, msg, messages, config

def complex_function():
    # Multiple arguments with complex expressions
    return __x(
        "Complex message with {param1} and {param2}",
        param1=get_param1(),
        param2=calculate_param2()
    )
"""
        )

        extractor = MessageExtractor()
        extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in extractor.messages]

        # Should extract all the string literals
        assert "Nested message" in messages
        assert "True message" in messages
        assert "False message" in messages
        assert "Error message" in messages
        assert "Success message" in messages
        assert "Complex message with {param1} and {param2}" in messages

    def test_edge_case_function_names(self, tmp_path):
        """Test edge cases with function names."""
        test_file = tmp_path / "edge_cases.py"
        test_file.write_text(
            """
# These should NOT be extracted (wrong function names)
def test_func():
    msg1 = _("Wrong function name")
    msg2 = translate("Also wrong")
    msg3 = gettext("Still wrong")

    # These SHOULD be extracted
    from sqlitch.i18n import __, __x, __n
    msg4 = __("Correct simple")
    msg5 = __x("Correct {param}", param="value")
    msg6 = __n("Correct singular", "Correct plural", 2)

    return msg1, msg2, msg3, msg4, msg5, msg6
"""
        )

        extractor = MessageExtractor()
        extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in extractor.messages]

        # Should only extract the correct function calls
        assert "Wrong function name" not in messages
        assert "Also wrong" not in messages
        assert "Still wrong" not in messages

        assert "Correct simple" in messages
        assert "Correct {param}" in messages
        assert "Correct singular" in messages

    def test_malformed_translation_calls(self, tmp_path):
        """Test handling of malformed translation calls."""
        test_file = tmp_path / "malformed.py"
        test_file.write_text(
            """
from sqlitch.i18n import __, __x, __n

def test_func():
    # These should be ignored (malformed calls)
    msg1 = __(123)  # Non-string argument
    msg2 = __x(variable)  # Variable instead of string
    msg3 = __n("Only one", 2)  # Missing second string for plural
    msg4 = __n(123, "Second", 2)  # First arg not string
    msg5 = __n("First", 456, 2)  # Second arg not string

    # This should be extracted
    msg6 = __("Valid message")

    return msg1, msg2, msg3, msg4, msg5, msg6
"""
        )

        extractor = MessageExtractor()
        extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in extractor.messages]

        # Should only extract the valid message
        assert len(messages) == 1
        assert "Valid message" in messages

    def test_non_string_constant_arguments(self, tmp_path):
        """Test handling of non-string constant arguments."""
        test_file = tmp_path / "non_string_constants.py"
        test_file.write_text(
            """
from sqlitch.i18n import __, __x, __n

def test_func():
    # These should be ignored (non-string constants)
    msg1 = __x(123)  # Integer constant
    msg2 = __n(123, 456, 2)  # Both args are integers
    msg3 = __n("Valid", 456, 2)  # Second arg is integer
    msg4 = __n(123, "Valid", 2)  # First arg is integer

    # These should be extracted
    msg5 = __x("Valid parameterized")
    msg6 = __n("Valid singular", "Valid plural", 2)

    return msg1, msg2, msg3, msg4, msg5, msg6
"""
        )

        extractor = MessageExtractor()
        extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in extractor.messages]

        # Should extract only the valid messages
        assert "Valid parameterized" in messages
        assert "Valid singular" in messages
        # Should not extract the invalid ones
        assert len([m for m in messages if "Valid" in m]) == 2

    def test_mixed_constant_types_in_plural(self, tmp_path):
        """Test handling of mixed constant types in __n calls."""
        test_file = tmp_path / "mixed_constants.py"
        test_file.write_text(
            """
from sqlitch.i18n import __n

def test_func():
    # First arg is constant, second is not
    msg1 = __n("First constant", variable_name, 2)

    # First arg is not constant, second is
    msg2 = __n(variable_name, "Second constant", 2)

    # Valid case
    msg3 = __n("Valid singular", "Valid plural", 2)

    return msg1, msg2, msg3
"""
        )

        extractor = MessageExtractor()
        extractor.extract_from_file(test_file)

        messages = [msg[0] for msg in extractor.messages]

        # Should only extract the valid message
        assert "Valid singular" in messages
        assert "First constant" not in messages
        assert "Second constant" not in messages
        assert len(messages) == 1


if __name__ == "__main__":
    pytest.main([__file__])
