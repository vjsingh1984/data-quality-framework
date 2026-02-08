# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for EngineLoader name validation and path construction."""
import re

import pytest

# Import the pattern directly for testing without Spark
_ENGINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _build_class_name(name: str) -> str:
    """Helper function that matches the new engine class name logic."""
    from dq.utils.string_utils import split_compound_word

    if "_" in name:
        # "schema_validation" -> ["Schema", "Validation"] -> "SchemaValidationEngine"
        return "".join(word.capitalize() for word in name.split("_")) + "Engine"
    else:
        # "schemavalidation" -> ["schema", "validation"] -> "SchemaValidationEngine"
        words = split_compound_word(name)
        return "".join(word.capitalize() for word in words) + "Engine"


class TestEngineNameValidation:
    """Tests for engine name regex validation."""

    @pytest.mark.parametrize(
        "name",
        [
            "deequ",
            "custom",
            "schemavalidation",
            "greatexpectations",
            "drules",
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
        # capitalize() only uppercases first letter (old broken behavior)
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
        """Test that loading a non-existent engine raises ImportError."""
        from dq.engine.engine_registry import EngineRegistry

        # Try to get a non-existent engine
        with pytest.raises(ImportError, match="not found via registry"):
            EngineRegistry.get_engine_class("nonexistent")


class TestImprovedClassNameConstruction:
    """Tests for the improved class name construction logic with compound word support."""

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("deequ", "DeequEngine"),
            ("custom", "CustomEngine"),  # Class name building produces CustomEngine
            ("drules", "DrulesEngine"),
            ("my_engine", "MyEngineEngine"),
            ("schema_validation", "SchemaValidationEngine"),
            ("great_expectations", "GreatExpectationsEngine"),
            ("schemavalidation", "SchemaValidationEngine"),
            ("greatexpectations", "GreatExpectationsEngine"),
            ("a", "AEngine"),
            ("multi_part_engine", "MultiPartEngineEngine"),
        ],
    )
    def test_class_name_new_logic(self, name, expected):
        """Test that underscores are removed and each word is capitalized."""
        assert _build_class_name(name) == expected

    def test_compound_word_splitting(self):
        """Test compound word splitting without underscores."""
        from dq.utils.string_utils import split_compound_word

        # Test known compound words
        assert split_compound_word("schemavalidation") == ["schema", "validation"]
        assert split_compound_word("greatexpectations") == ["great", "expectations"]

        # Test underscore splitting still works
        assert split_compound_word("schema_validation") == ["schema", "validation"]
        assert split_compound_word("great_expectations") == ["great", "expectations"]

        # Test simple words pass through
        assert split_compound_word("deequ") == ["deequ"]
        assert split_compound_word("custom") == ["custom"]

    def test_old_capitalize_was_broken(self):
        """Demonstrate that the old capitalize() logic was broken."""
        # Old logic: f"{name.capitalize()}Engine"
        # "my_engine" -> "My_engineEngine" ❌
        # "schemavalidation" -> "SchemavalidationEngine" ❌ (not proper PascalCase)

        assert "my_engine".capitalize() == "My_engine"
        assert "schemavalidation".capitalize() == "Schemavalidation"

        # New logic handles this correctly - each word is capitalized
        assert _build_class_name("my_engine") == "MyEngineEngine"
        assert _build_class_name("schemavalidation") == "SchemaValidationEngine"


class TestEngineDiscoveryValidation:
    """Tests for engine discovery validation (D5 fix)."""

    def test_convention_import_validates_apply_method(self):
        """Test that convention import validates required 'apply' method."""
        from dq.engine.engine_registry import EngineRegistry

        # Try to load known good engines - they should all have the apply method
        for engine_name in ["deequ", "custom", "drules"]:
            result = EngineRegistry._try_convention_import(engine_name)
            if result is not None:
                assert hasattr(
                    result, "apply"
                ), f"{engine_name} should have apply method"

    def test_real_engines_load_with_new_logic(self):
        """Test that real engines can be loaded with the new class name logic."""
        from dq.engine.engine_registry import EngineRegistry

        # These engines should load successfully with the new logic
        for engine_name in ["deequ", "custom", "drules", "schemavalidation"]:
            result = EngineRegistry.get_engine_class(engine_name)
            assert result is not None, f"Failed to load {engine_name}"
            # Verify it has the required apply method
            assert hasattr(result, "apply"), f"{engine_name} missing apply method"
