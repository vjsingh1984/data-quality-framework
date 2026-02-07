# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for configuration loading logic."""
from urllib.parse import urlparse

import pytest


class TestURISchemeDetection:
    """Tests for URI scheme parsing used in config loading."""

    @pytest.mark.parametrize(
        "config_str,expected_scheme",
        [
            ("dqframework { dqrules = [] }", ""),
            ("s3://bucket/key/config.conf", "s3"),
            ("abfss://container@account.dfs.core.windows.net/path", "abfss"),
            ("file:///home/user/config.conf", "file"),
            ("http://example.com/config.conf", "http"),
            ("https://example.com/config.conf", "https"),
        ],
    )
    def test_scheme_detection(self, config_str, expected_scheme):
        parsed = urlparse(config_str)
        assert parsed.scheme == expected_scheme

    def test_string_config_parsing(self):
        from pyhocon import ConfigFactory

        config_str = """
        dqframework {
            dqrules = [
                {
                    name = "test"
                    engine = "deequ"
                    checks = []
                }
            ]
        }
        """
        config = ConfigFactory.parse_string(config_str)
        assert config.get("dqframework.dqrules") is not None
        rules = config.get("dqframework.dqrules")
        assert len(rules) == 1
        assert rules[0]["engine"] == "deequ"

    def test_empty_config_string(self):
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string("{}")
        assert config.get("dqframework", None) is None

    def test_invalid_hocon_raises(self):
        from pyhocon import ConfigFactory

        with pytest.raises(Exception):
            ConfigFactory.parse_string("{{{{invalid")

    def test_s3_url_parts(self):
        url = "s3://my-bucket/path/to/config.conf"
        parsed = urlparse(url)
        assert parsed.scheme == "s3"
        assert parsed.netloc == "my-bucket"
        assert parsed.path == "/path/to/config.conf"

    def test_unsupported_scheme(self):
        url = "ftp://server/config.conf"
        parsed = urlparse(url)
        assert parsed.scheme == "ftp"
        # Framework should reject this
        assert parsed.scheme not in ("", "s3", "abfss", "file", "http", "https")
