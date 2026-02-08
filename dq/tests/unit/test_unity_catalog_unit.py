# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Unity Catalog identifier escaping and provider methods."""
from unittest.mock import MagicMock

import pytest

from dq.catalog.unity_catalog import UnityCatalogProvider, _escape_identifier


class TestEscapeIdentifier:
    """Tests for the _escape_identifier helper."""

    def test_simple_name(self):
        assert _escape_identifier("my_table") == "`my_table`"

    def test_dotted_name(self):
        assert (
            _escape_identifier("catalog.schema.table") == "`catalog`.`schema`.`table`"
        )

    def test_two_part_name(self):
        assert _escape_identifier("schema.table") == "`schema`.`table`"

    def test_backtick_in_name(self):
        assert _escape_identifier("my`cat.schema") == "`my``cat`.`schema`"

    def test_spaces_in_name(self):
        assert (
            _escape_identifier("my catalog.my schema.my table")
            == "`my catalog`.`my schema`.`my table`"
        )

    def test_injection_attempt(self):
        malicious = "table; DROP TABLE users--"
        assert _escape_identifier(malicious) == "`table; DROP TABLE users--`"

    def test_dotted_injection_attempt(self):
        malicious = "cat.schema.table; DROP TABLE users--"
        assert (
            _escape_identifier(malicious)
            == "`cat`.`schema`.`table; DROP TABLE users--`"
        )

    def test_empty_part(self):
        # edge case: ".schema.table" → empty first part
        assert _escape_identifier(".schema.table") == "``.`schema`.`table`"

    def test_single_backtick_name(self):
        assert _escape_identifier("`") == "````"


class TestUnityCatalogProviderSQL:
    """Tests that provider methods pass escaped identifiers to spark.sql()."""

    @pytest.fixture
    def provider(self):
        spark = MagicMock()
        return UnityCatalogProvider(spark)

    def test_table_exists_escapes_identifier(self, provider):
        provider._spark.sql.return_value.collect.return_value = []
        provider.table_exists("catalog.schema.table")
        sql_arg = provider._spark.sql.call_args[0][0]
        assert sql_arg == "DESCRIBE TABLE `catalog`.`schema`.`table`"

    def test_list_schemas_escapes_catalog(self, provider):
        provider._spark.sql.return_value.collect.return_value = []
        provider.list_schemas("my_catalog")
        sql_arg = provider._spark.sql.call_args[0][0]
        assert sql_arg == "SHOW SCHEMAS IN `my_catalog`"

    def test_list_schemas_no_catalog(self, provider):
        provider._spark.sql.return_value.collect.return_value = []
        provider.list_schemas()
        sql_arg = provider._spark.sql.call_args[0][0]
        assert sql_arg == "SHOW SCHEMAS"

    def test_list_tables_escapes_schema(self, provider):
        provider._spark.sql.return_value.collect.return_value = []
        provider.list_tables("my_schema", "my_catalog")
        sql_arg = provider._spark.sql.call_args[0][0]
        assert sql_arg == "SHOW TABLES IN `my_catalog`.`my_schema`"

    def test_set_current_catalog_escapes(self, provider):
        provider.set_current_catalog("my_catalog")
        sql_arg = provider._spark.sql.call_args[0][0]
        assert sql_arg == "USE CATALOG `my_catalog`"

    def test_table_exists_with_injection(self, provider):
        provider._spark.sql.return_value.collect.return_value = []
        provider.table_exists("x; DROP TABLE y--")
        sql_arg = provider._spark.sql.call_args[0][0]
        assert sql_arg == "DESCRIBE TABLE `x; DROP TABLE y--`"

    def test_get_dataframe_does_not_escape(self, provider):
        """spark.table() uses the catalog API — no SQL escaping needed."""
        provider._spark.table.return_value = MagicMock()
        provider.get_dataframe("catalog.schema.table")
        provider._spark.table.assert_called_once_with("catalog.schema.table")
        provider._spark.sql.assert_not_called()
