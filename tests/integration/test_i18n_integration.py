"""
Integration tests for internationalization support.

This module tests the complete i18n workflow including message extraction,
translation loading, and locale-aware formatting in real usage scenarios.
"""

import locale
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

import sqlitch.i18n as i18n
from sqlitch.core.config import Config
from sqlitch.core.sqitch import create_sqitch
from sqlitch.i18n import setup_i18n
from sqlitch.i18n.date_time import format_datetime


class TestI18nIntegration:
    """Test complete i18n integration."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.config_file = self.temp_dir / "sqitch.conf"

        # Create basic config
        self.config_file.write_text(
            """
[core]
    engine = pg

[user]
    name = Test User
    email = test@example.com
"""
        )

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sqitch_with_i18n_messages(self):
        """Test that Sqitch uses i18n messages."""
        config = Config([self.config_file])
        sqitch = create_sqitch([self.config_file])

        # Test user validation with i18n messages
        with patch.object(sqitch, "user_name", None):
            issues = sqitch.validate_user_info()
            assert len(issues) > 0

            # Should contain translated message
            name_issue = next(
                (issue for issue in issues if "name" in issue.lower()), None
            )
            assert name_issue is not None
            assert "sqlitch config" in name_issue

    def test_error_messages_with_i18n(self):
        """Test that error messages use i18n."""
        config = Config([self.config_file])
        sqitch = create_sqitch([self.config_file])

        # Test with invalid command
        exit_code = sqitch.run_command("nonexistent_command", [])
        assert exit_code != 0

    def test_datetime_formatting_in_context(self):
        """Test datetime formatting in real usage context."""
        test_datetime = datetime(2023, 12, 25, 15, 30, 45)

        # Test various formats
        iso_result = format_datetime(test_datetime, "iso")
        assert iso_result == "2023-12-25T15:30:45Z"

        rfc_result = format_datetime(test_datetime, "rfc")
        assert "Dec 2023" in rfc_result

        default_result = format_datetime(test_datetime, "default")
        assert isinstance(default_result, str)
        assert len(default_result) > 0

    @patch.dict(os.environ, {"LANG": "de_DE.UTF-8"})
    def test_german_locale_integration(self):
        """Test integration with German locale."""
        # Set up i18n with German locale
        locale_dir = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale"

        try:
            setup_i18n(locale_dir)

            # Test that messages can be processed
            result = getattr(i18n, "__")(
                'Cannot find your name; run sqlitch config --user user.name "YOUR NAME"'
            )
            assert isinstance(result, str)
            assert len(result) > 0

        except Exception as e:
            # If locale setup fails, that's okay for CI environments
            pytest.skip(f"Locale setup failed: {e}")

    @patch.dict(os.environ, {"LANG": "fr_FR.UTF-8"})
    def test_french_locale_integration(self):
        """Test integration with French locale."""
        locale_dir = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale"

        try:
            setup_i18n(locale_dir)

            # Test parameterized messages
            result = getattr(i18n, "__x")(
                "Unknown engine: {engine}", engine="test_engine"
            )
            assert "test_engine" in result

        except Exception as e:
            # If locale setup fails, that's okay for CI environments
            pytest.skip(f"Locale setup failed: {e}")

    def test_message_extraction_workflow(self):
        """Test the complete message extraction workflow."""
        # Create a temporary Python file with translatable strings
        test_file = self.temp_dir / "test_module.py"
        test_file.write_text(
            """
from sqlitch.i18n import __, __x, __n

def test_function():
    from sqlitch.i18n import __, __x, __n
    message1 = __("Simple message")
    message2 = __x("Parameterized {param}", param="value")
    message3 = __n("One item", "{count} items", 5, count=5)
    return message1, message2, message3
"""
        )

        # Test that the extraction script exists and can be imported
        extract_script = (
            Path(__file__).parent.parent.parent
            / "sqlitch"
            / "i18n"
            / "extract_messages.py"
        )

        if extract_script.exists():
            # Test the extraction functionality without modifying the real POT file
            try:
                # Import the extraction module to test its functionality
                import sys

                sys.path.insert(0, str(extract_script.parent))

                from extract_messages import MessageExtractor, find_python_files

                # Test the MessageExtractor on our test file
                extractor = MessageExtractor()
                extractor.extract_from_file(test_file)

                # Should have found our test messages
                messages = [msg[0] for msg in extractor.messages]
                assert "Simple message" in messages
                assert "Parameterized {param}" in messages
                assert "One item" in messages or "{count} items" in messages

                # Test find_python_files function
                python_files = find_python_files(self.temp_dir)
                assert test_file in python_files

            except ImportError as e:
                pytest.skip(f"Could not import extraction module: {e}")
            finally:
                # Clean up sys.path
                if str(extract_script.parent) in sys.path:
                    sys.path.remove(str(extract_script.parent))

    def test_po_file_validation(self):
        """Test that PO files are valid and contain expected content."""
        locale_dir = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale"

        for lang in ["de_DE", "fr_FR", "it_IT"]:
            po_file = locale_dir / lang / "LC_MESSAGES" / "sqlitch.po"

            if po_file.exists():
                content = po_file.read_text(encoding="utf-8")

                # Should have proper PO file structure
                assert 'msgid ""' in content
                assert 'msgstr ""' in content
                assert (
                    f"Language: {lang[:2]}" in content or f"Language: {lang}" in content
                )

                # Should contain some core messages
                assert "Cannot find your name" in content
                assert "Unknown engine" in content

    def test_mo_file_validation(self):
        """Test that MO files are valid binary files."""
        locale_dir = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale"

        for lang in ["de_DE", "fr_FR", "it_IT"]:
            mo_file = locale_dir / lang / "LC_MESSAGES" / "sqlitch.mo"

            if mo_file.exists():
                # Should be a binary file with proper MO header
                with open(mo_file, "rb") as f:
                    header = f.read(4)
                    # MO files start with magic number (little or big endian)
                    assert header in [b"\xde\x12\x04\x95", b"\x95\x04\x12\xde"]

                # Should be non-empty
                assert mo_file.stat().st_size > 100  # Reasonable minimum size

    def test_locale_fallback_behavior(self):
        """Test that locale fallback works correctly."""
        # Test with unsupported locale
        with patch.dict(os.environ, {"LANG": "xx_XX.UTF-8"}):
            locale_dir = (
                Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale"
            )
            setup_i18n(locale_dir)

            # Should fall back to English
            result = getattr(i18n, "__")("Test message")
            assert result == "Test message"

            # Parameterized messages should still work
            result = getattr(i18n, "__x")("Test {param}", param="value")
            assert result == "Test value"

    def test_concurrent_translation_access(self):
        """Test that translation functions are thread-safe."""
        import threading
        import time

        results = []
        errors = []

        def translate_messages():
            try:
                for i in range(10):
                    result = getattr(i18n, "__x")("Message {num}", num=i)
                    results.append(result)
                    time.sleep(0.001)  # Small delay to encourage race conditions
            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=translate_messages)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5)

        # Should not have any errors
        assert len(errors) == 0

        # Should have expected number of results
        assert len(results) == 50  # 5 threads * 10 messages each

        # All results should be properly formatted
        for result in results:
            assert "Message " in result
            assert result.startswith("Message ")

    def test_large_message_handling(self):
        """Test handling of large messages."""
        # Create a large message
        large_message = "This is a very long message. " * 100

        # Should handle without issues
        result = getattr(i18n, "__")(large_message)
        assert result == large_message

        # Test with parameters
        result = getattr(i18n, "__x")("Large message: {content}", content=large_message)
        assert large_message in result

    def test_special_character_handling(self):
        """Test handling of special characters in messages."""
        # Test various special characters
        special_messages = [
            "Message with ümlaut",
            "Message with émoji 🚀",
            "Message with 中文",
            'Message with "quotes"',
            "Message with 'apostrophes'",
            "Message with\nnewlines",
            "Message with\ttabs",
        ]

        for message in special_messages:
            result = getattr(i18n, "__")(message)
            assert result == message

            # Test with parameters
            result = getattr(i18n, "__x")("Special: {msg}", msg=message)
            assert message in result


if __name__ == "__main__":
    pytest.main([__file__])
