# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for CatalogFactory extensibility (D8 fix)."""
from unittest.mock import MagicMock

import pytest

from dq.catalog.base import CatalogProvider
from dq.catalog.catalog_factory import CatalogFactory


class CustomCatalogProvider(CatalogProvider):
    """Mock custom catalog provider for testing."""

    def __init__(self, spark_session):
        super().__init__(spark_session)
        self.test_attr = "custom"

    def get_dataframe(self, table_name):
        raise NotImplementedError("Test mock")

    def get_table_schema(self, table_name):
        raise NotImplementedError("Test mock")

    def table_exists(self, table_reference, database=None):
        raise NotImplementedError("Test mock")

    def list_tables(self, database=None):
        raise NotImplementedError("Test mock")

    def list_schemas(self, catalog=None):
        raise NotImplementedError("Test mock")

    def set_current_catalog(self, catalog_name):
        raise NotImplementedError("Test mock")


class TestCatalogFactoryExtensibility:
    """Tests for CatalogFactory extensibility features."""

    def test_register_custom_provider(self):
        """Test that custom catalog providers can be registered."""
        # Register a custom provider
        CatalogFactory.register_provider("my_system", CustomCatalogProvider)

        # Verify it's in the supported types
        supported = CatalogFactory.supported_types()
        assert "my_system" in supported

    def test_get_custom_provider(self):
        """Test that registered custom provider can be instantiated."""
        # Register a custom provider
        CatalogFactory.register_provider("custom", CustomCatalogProvider)

        # Get the provider
        mock_spark = MagicMock()
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="custom")

        # Verify it's an instance of our custom class
        assert isinstance(provider, CustomCatalogProvider)
        assert provider.test_attr == "custom"

    def test_register_provider_validates_inheritance(self):
        """Test that register_provider validates inheritance."""

        class NotAProvider:
            """A class that doesn't inherit from CatalogProvider."""

            pass

        # Should raise TypeError for non-Provider class
        with pytest.raises(TypeError, match="must inherit from CatalogProvider"):
            CatalogFactory.register_provider("invalid", NotAProvider)

    def test_register_provider_thread_safety(self):
        """Test that provider registration is thread-safe."""
        import concurrent.futures

        class ThreadSafeCatalogProvider(CatalogProvider):
            def __init__(self, spark_session):
                super().__init__(spark_session)
                self.registration_count = 0

            def get_dataframe(self, table_name):
                raise NotImplementedError()

            def get_table_schema(self, table_name):
                raise NotImplementedError()

            def table_exists(self, table_reference, database=None):
                raise NotImplementedError()

            def list_tables(self, database=None):
                raise NotImplementedError()

            def list_schemas(self, catalog=None):
                raise NotImplementedError()

            def set_current_catalog(self, catalog_name):
                raise NotImplementedError()

        def register_many_providers():
            for i in range(10):
                CatalogFactory.register_provider(
                    f"thread_safe_{i}", ThreadSafeCatalogProvider
                )

        # Register providers from multiple threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(register_many_providers) for _ in range(5)]
            concurrent.futures.wait(futures)

        # Verify all registrations succeeded
        for i in range(10):
            assert f"thread_safe_{i}" in CatalogFactory.supported_types()

    def test_builtin_providers_still_work(self):
        """Test that built-in providers are still registered."""
        supported = CatalogFactory.supported_types()

        # Verify all built-in types are available
        assert "spark" in supported
        assert "hive" in supported
        assert "unity" in supported
        assert "delta" in supported
        assert "glue" in supported

    def test_can_get_builtin_providers_after_registration(self):
        """Test that built-in providers still work after custom registration."""
        # Register a custom provider
        CatalogFactory.register_provider("custom", CustomCatalogProvider)

        # Verify built-in providers still work
        mock_spark = MagicMock()
        for catalog_type in ["spark", "unity", "glue"]:
            provider = CatalogFactory.get_provider(
                mock_spark, catalog_type=catalog_type
            )
            assert provider is not None
            # Verify it's the correct type
            assert hasattr(provider, "get_dataframe")

    def test_register_provider_case_insensitive(self):
        """Test that provider names are normalized to lowercase."""
        # Register with mixed case
        CatalogFactory.register_provider("MySystem", CustomCatalogProvider)

        # Should be accessible with lowercase
        supported = CatalogFactory.supported_types()
        assert "mysystem" in supported

        # Should be retrievable with lowercase
        mock_spark = MagicMock()
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="MySystem")
        assert isinstance(provider, CustomCatalogProvider)

    def test_unknown_catalog_type_gives_helpful_error(self):
        """Test that unknown catalog types give helpful error messages."""
        mock_spark = MagicMock()

        with pytest.raises(
            ValueError, match="Unknown catalog_type.*unknown_catalog"
        ) as exc_info:
            CatalogFactory.get_provider(mock_spark, catalog_type="unknown_catalog")

        # Error message should list available types
        error_msg = str(exc_info.value)
        assert "spark" in error_msg
        assert "unity" in error_msg
        assert "glue" in error_msg
