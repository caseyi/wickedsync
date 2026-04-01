"""
Tests for Gumroad utility functions that don't require a live browser.
"""
import pytest
from app.gumroad import _parse_cookies, _extract_filename_from_url


class TestParseCookies:
    def test_single_cookie(self):
        result = _parse_cookies("_gumroad_guid=abc123")
        assert len(result) == 1
        assert result[0]["name"] == "_gumroad_guid"
        assert result[0]["value"] == "abc123"

    def test_multiple_cookies(self):
        result = _parse_cookies("_gumroad_app_session=sess; _gumroad_guid=guid")
        assert len(result) == 2
        names = {c["name"] for c in result}
        assert names == {"_gumroad_app_session", "_gumroad_guid"}

    def test_domain_set(self):
        result = _parse_cookies("key=val")
        assert result[0]["domain"] == ".gumroad.com"

    def test_empty_string(self):
        result = _parse_cookies("")
        assert result == []

    def test_cookie_with_equals_in_value(self):
        # Base64-encoded values contain = signs
        result = _parse_cookies("_session=abc==xyz")
        assert result[0]["value"] == "abc==xyz"


class TestExtractFilename:
    def test_cloudfront_url(self):
        url = (
            "https://d2dw6lv4z9w0e2.cloudfront.net/attachments/abc123/def456/original/"
            "Wicked%20-%20Blade%20Sculpture%20%28Non%20Supported%29.zip"
            "?Expires=1775030667&Signature=xyz"
        )
        name = _extract_filename_from_url(url)
        assert name == "Wicked - Blade Sculpture (Non Supported).zip"

    def test_files_gumroad_url(self):
        url = (
            "https://files.gumroad.com/attachments/5453867803493/d7de3ba5/"
            "original/Wicked%20-%20Blade%20%28Non%20Supported%29.zip"
            "?response-content-disposition=attachment&cache_key=bdc2"
        )
        name = _extract_filename_from_url(url)
        assert name == "Wicked - Blade (Non Supported).zip"

    def test_unknown_url_returns_fallback(self):
        name = _extract_filename_from_url("https://example.com/noextension")
        # Should return something, not crash
        assert isinstance(name, str)
        assert len(name) > 0
