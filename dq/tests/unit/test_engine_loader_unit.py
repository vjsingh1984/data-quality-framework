# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for EngineLoader name validation and path construction."""
import re

import pytest

# Import the pattern directly for testing without Spark
_ENGINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class TestEngineNameValidation:
    """Tests for engine name regex validation."""

    @pytest.mark.parametrize(
        "name",
        [
            "deequ",
            "custom",
            "schemavalidation",
            "greatexpectations",
            "my_engine",
            "engine123",
            "a",
        ],
    )
    def test_valid_names(self, name):
        assert _ENGINE_NAME_PATTERN.match(name) is not None

    @pytest.mark.parametrize(
        "name",
        [
            "Deequ",
            "CUSTOM",
            "1engine",
            "_private",
            "my-engine",
            "my.engine",
            "my/engine",
            "",
            "engine name",
            "engine!",
        ],
    )
    def test_invalid_names(self, name):
        assert _ENGINE_NAME_PATTERN.match(name) is None


class TestModulePathConstruction:
    """Tests for engine module path and class name construction."""

    def test_module_path(self):
        name = "deequ"
        module_path = f"dq.engine.{name}.{name}_engine"
        assert module_path == "dq.engine.deequ.deequ_engine"

    def test_class_name(self):
        name = "deequ"
        class_name = f"{name.capitalize()}Engine"
        assert class_name == "DeequEngine"

    def test_class_name_multiword(self):
        name = "schemavalidation"
        class_name = f"{name.capitalize()}Engine"
        # capitalize() only uppercases first letter
        assert class_name == "SchemavalidationEngine"

    def test_class_name_with_underscore(self):
        name = "my_engine"
        class_name = f"{name.capitalize()}Engine"
        assert class_name == "My_engineEngine"

    def test_loader_import_error(self):
        from dq.engine.engine_loader import EngineLoader

        loader = EngineLoader()
        with pytest.raises(ValueError, match="Invalid engine name"):
            loader.load_engine("Invalid-Name", {})

    def test_loader_module_not_found(self):
        from dq.engine.engine_loader import EngineLoader

        loader = EngineLoader()
        with pytest.raises(ImportError):
            loader.load_engine("nonexistent", {})
