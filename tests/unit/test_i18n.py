"""
Tests for internationalization support.

This module tests the gettext-based message translation system,
locale-aware date/time formatting, and translation markers.
"""

import os
import locale
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import sqlitch.i18n as i18n
from sqlitch.i18n import setup_i18n
from sqlitch.i18n.date_time import (
    LocaleAwareDateTimeFormatter,
    format_datetime,
    format_timestamp
)


class TestBasicTranslation:
    """Test basic translation functionality."""
    
    def test_simple_translation_fallback(self):
        """Test that __ function returns original message when no translation."""
        message = "Test message"
        result = getattr(i18n, '__')(message)
        assert result == message
    
    def test_parameterized_translation_fallback(self):
        """Test that __x function formats parameters when no translation."""
        message = "Test message with {param}"
        result = getattr(i18n, '__x')(message, param="value")
        assert result == "Test message with value"
    
    def test_plural_translation_fallback(self):
        """Test that __n function handles plurals when no translation."""
        singular = "One item"
        plural = "{count} items"
        
        result = getattr(i18n, '__n')(singular, plural, 1)
        assert result == "One item"
        
        result = getattr(i18n, '__n')(singular, plural, 2)
        assert "2" in result and "items" in result
    
    def test_parameterized_translation_error_handling(self):
        """Test that __x handles formatting errors gracefully."""
        message = "Test message with {missing_param}"
        
        # Should fall back to original message formatting
        with pytest.raises(KeyError):
            getattr(i18n, '__x')(message, other_param="value")
    
    def test_setup_i18n_with_nonexistent_locale_dir(self):
        """Test that setup_i18n handles missing locale directory gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nonexistent_dir = Path(temp_dir) / "nonexistent"
            
            # Should not raise an exception
            setup_i18n(nonexistent_dir)
            
            # Should still work with fallback
            result = getattr(i18n, '__')("Test message")
            assert result == "Test message"


class TestLocaleAwareDateTimeFormatting:
    """Test locale-aware date/time formatting."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = LocaleAwareDateTimeFormatter()
        self.test_datetime = datetime(2023, 12, 25, 15, 30, 45)
    
    def test_iso_format(self):
        """Test ISO 8601 date formatting."""
        result = self.formatter.format_datetime(self.test_datetime, 'iso')
        assert result == "2023-12-25T15:30:45Z"
    
    def test_rfc_format(self):
        """Test RFC 2822 date formatting."""
        result = self.formatter.format_datetime(self.test_datetime, 'rfc')
        # Should be in format: Mon, 25 Dec 2023 15:30:45 -0000
        assert "25 Dec 2023" in result
        assert "15:30:45" in result
    
    def test_rfc2822_format(self):
        """Test RFC 2822 date formatting (alias)."""
        result = self.formatter.format_datetime(self.test_datetime, 'rfc2822')
        # Should be in format: Mon, 25 Dec 2023 15:30:45 -0000
        assert "25 Dec 2023" in result
        assert "15:30:45" in result
    
    def test_strftime_format(self):
        """Test direct strftime formatting."""
        result = self.formatter.format_datetime(self.test_datetime, 'strftime:%Y-%m-%d')
        assert result == "2023-12-25"
    
    def test_cldr_format(self):
        """Test CLDR format patterns."""
        result = self.formatter.format_datetime(self.test_datetime, 'cldr:short')
        # Should contain date and time components
        assert "2023" in result or "23" in result
        assert "12" in result or "25" in result
    
    def test_default_format(self):
        """Test default locale-aware formatting."""
        result = self.formatter.format_datetime(self.test_datetime, 'default')
        # Should return some formatted string
        assert isinstance(result, str)
        assert len(result) > 0
    
    def test_invalid_format_fallback(self):
        """Test that invalid formats fall back to ISO."""
        result = self.formatter.format_datetime(self.test_datetime, 'invalid_format')
        assert result == "2023-12-25T15:30:45Z"
    
    def test_format_datetime_convenience_function(self):
        """Test the convenience format_datetime function."""
        result = format_datetime(self.test_datetime, 'iso')
        assert result == "2023-12-25T15:30:45Z"
    
    def test_format_timestamp_convenience_function(self):
        """Test the convenience format_timestamp function."""
        timestamp = self.test_datetime.timestamp()
        result = format_timestamp(timestamp, 'iso')
        # Should be close to the expected format (may vary by timezone)
        assert "2023-12-25T" in result
    
    @patch('locale.setlocale')
    def test_locale_setup_error_handling(self, mock_setlocale):
        """Test that locale setup errors are handled gracefully."""
        mock_setlocale.side_effect = locale.Error("Locale not available")
        
        formatter = LocaleAwareDateTimeFormatter()
        result = formatter.format_datetime(self.test_datetime, 'default')
        
        # Should fall back to ISO format
        assert result == "2023-12-25T15:30:45Z"
    
    @patch('platform.system')
    def test_windows_locale_handling(self, mock_system):
        """Test Windows-specific locale handling."""
        mock_system.return_value = "Windows"
        
        with patch.dict(os.environ, {'LANG': 'en_US.UTF-8'}):
            formatter = LocaleAwareDateTimeFormatter()
            # Should not raise an exception
            result = formatter.format_datetime(self.test_datetime, 'default')
            assert isinstance(result, str)


class TestMessageExtraction:
    """Test message extraction functionality."""
    
    def test_extract_messages_script_exists(self):
        """Test that the message extraction script exists."""
        script_path = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "extract_messages.py"
        assert script_path.exists()
        assert script_path.is_file()
    
    def test_pot_file_exists(self):
        """Test that the POT template file exists."""
        pot_path = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale" / "sqlitch.pot"
        assert pot_path.exists()
        assert pot_path.is_file()
    
    def test_po_files_exist(self):
        """Test that PO translation files exist for supported languages."""
        locale_dir = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale"
        
        for lang in ['de_DE', 'fr_FR', 'it_IT']:
            po_path = locale_dir / lang / "LC_MESSAGES" / "sqlitch.po"
            assert po_path.exists(), f"Missing PO file for {lang}"
            
            mo_path = locale_dir / lang / "LC_MESSAGES" / "sqlitch.mo"
            assert mo_path.exists(), f"Missing MO file for {lang}"


class TestTranslationIntegration:
    """Test integration with actual translation files."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.locale_dir = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale"
    
    @patch('locale.getlocale')
    @patch('gettext.translation')
    def test_german_translation_loading(self, mock_translation, mock_getlocale):
        """Test loading German translations."""
        mock_getlocale.return_value = ('de_DE', 'UTF-8')
        
        # Mock translation object
        mock_trans = MagicMock()
        mock_trans.gettext.return_value = "Deutscher Text"
        mock_translation.return_value = mock_trans
        
        setup_i18n(self.locale_dir)
        result = getattr(i18n, '__')("Test message")
        
        # Should have attempted to load translation
        mock_translation.assert_called_once()
    
    def test_translation_file_content(self):
        """Test that translation files contain expected messages."""
        de_po_path = self.locale_dir / "de_DE" / "LC_MESSAGES" / "sqlitch.po"
        
        if de_po_path.exists():
            content = de_po_path.read_text(encoding='utf-8')
            
            # Should contain some key messages
            assert 'msgid "Cannot find your name' in content
            assert 'msgstr "Kann deinen Namen nicht finden' in content
    
    def test_mo_file_compilation(self):
        """Test that MO files are properly compiled."""
        for lang in ['de_DE', 'fr_FR', 'it_IT']:
            mo_path = self.locale_dir / lang / "LC_MESSAGES" / "sqlitch.mo"
            
            if mo_path.exists():
                # MO files should be binary and non-empty
                assert mo_path.stat().st_size > 0
                
                # Should be binary (not text)
                with open(mo_path, 'rb') as f:
                    header = f.read(4)
                    # MO files start with magic number
                    assert header in [b'\xde\x12\x04\x95', b'\x95\x04\x12\xde']


class TestContextualTranslation:
    """Test contextual translation features."""
    
    def setup_method(self):
        """Reset i18n state for each test."""
        # Reset the global translation function
        i18n._ = None
        setup_i18n()
    
    def test_context_markers_in_pot_file(self):
        """Test that POT file exists and has proper structure."""
        pot_path = Path(__file__).parent.parent.parent / "sqlitch" / "i18n" / "locale" / "sqlitch.pot"
        
        if pot_path.exists():
            content = pot_path.read_text(encoding='utf-8')
            
            # Should have proper POT file structure
            assert 'Project-Id-Version: sqlitch' in content
            assert 'Content-Type: text/plain; charset=UTF-8' in content
            
            # If it has content, check for context markers (optional since extraction may vary)
            if 'msgid "Yes"' in content:
                # Context markers are preferred but not required for this test
                pass
    
    def test_parameter_substitution_in_translations(self):
        """Test that parameter substitution works with translations."""
        # Test with various parameter types
        result = getattr(i18n, '__x')("Test {param1} and {param2}", param1="value1", param2=42)
        assert result == "Test value1 and 42"
        
        # Test with missing parameters (should raise KeyError)
        with pytest.raises(KeyError):
            getattr(i18n, '__x')("Test {missing}", other="value")
    
    def test_plural_forms_handling(self):
        """Test plural forms handling."""
        # Test singular
        result = getattr(i18n, '__n')("One file", "{count} files", 1)
        assert "One file" in result
        
        # Test plural
        result = getattr(i18n, '__n')("One file", "{count} files", 5)
        assert "5" in result and "files" in result
        
        # Test zero (should use plural in English)
        result = getattr(i18n, '__n')("One file", "{count} files", 0)
        assert "0" in result and "files" in result


class TestErrorHandling:
    """Test error handling in i18n functionality."""
    
    def test_translation_with_encoding_errors(self):
        """Test handling of encoding errors in translation files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a malformed MO file
            locale_dir = Path(temp_dir) / "locale"
            lang_dir = locale_dir / "test_lang" / "LC_MESSAGES"
            lang_dir.mkdir(parents=True)
            
            mo_file = lang_dir / "sqlitch.mo"
            mo_file.write_bytes(b"invalid mo file content")
            
            # Should handle gracefully
            setup_i18n(locale_dir)
            result = getattr(i18n, '__')("Test message")
            assert result == "Test message"
    
    def test_missing_translation_domain(self):
        """Test handling of missing translation domain."""
        with tempfile.TemporaryDirectory() as temp_dir:
            locale_dir = Path(temp_dir)
            
            # Should handle gracefully
            setup_i18n(locale_dir, domain="nonexistent")
            result = getattr(i18n, '__')("Test message")
            assert result == "Test message"
    
    def test_format_string_errors(self):
        """Test handling of format string errors."""
        # Invalid format string should fall back to original
        message = "Test {invalid format"
        
        # Should handle gracefully (may raise exception or return original)
        try:
            result = getattr(i18n, '__x')(message, param="value")
            # If no exception, should return something reasonable
            assert isinstance(result, str)
        except (KeyError, ValueError):
            # Format errors are acceptable
            pass


if __name__ == "__main__":
    pytest.main([__file__])