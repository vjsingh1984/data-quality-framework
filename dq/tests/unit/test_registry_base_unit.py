# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for GenericRegistry base class (D3)."""

import pytest

from dq.utils.registry_base import GenericRegistry


class _TestItem:
    """Test item for registry."""

    def __init__(self, name: str):
        self.name = name


class _TestRegistry(GenericRegistry[_TestItem]):
    """Test registry for GenericRegistry."""

    _registry: dict = {}


class TestGenericRegistry:
    """Tests for GenericRegistry base class."""

    def setup_method(self):
        """Clean registry before each test."""
        _TestRegistry.clear()

    def test_register_and_get(self):
        """Test registering and retrieving an item."""
        item = _TestItem("test_item")
        _TestRegistry.register("MyItem", item)

        retrieved = _TestRegistry.get("MyItem")
        assert retrieved is item

    def test_register_case_insensitive(self):
        """Test that names are case-insensitive."""
        item = _TestItem("test")
        _TestRegistry.register("MyItem", item)

        # Retrieve with different case
        assert _TestRegistry.get("myitem") is item
        assert _TestRegistry.get("MYITEM") is item
        assert _TestRegistry.get("MyItem") is item

    def test_get_unknown_raises_key_error(self):
        """Test that getting unknown item raises KeyError."""
        with pytest.raises(KeyError, match="Unknown 'unknown'"):
            _TestRegistry.get("unknown")

    def test_error_message_includes_available(self):
        """Test that error message lists available items."""
        _TestRegistry.register("Item1", _TestItem("a"))
        _TestRegistry.register("Item2", _TestItem("b"))

        with pytest.raises(KeyError) as exc_info:
            _TestRegistry.get("unknown")

        error_msg = str(exc_info.value)
        assert "item1" in error_msg or "Item1" in error_msg
        assert "item2" in error_msg or "Item2" in error_msg

    def test_unregister(self):
        """Test unregistering an item."""
        item = _TestItem("test")
        _TestRegistry.register("MyItem", item)

        assert _TestRegistry.is_registered("MyItem")
        _TestRegistry.unregister("MyItem")
        assert not _TestRegistry.is_registered("MyItem")

    def test_unregister_nonexistent_no_error(self):
        """Test that unregistering nonexistent item doesn't raise."""
        # Should not raise
        _TestRegistry.unregister("nonexistent")

    def test_is_registered(self):
        """Test checking if item is registered."""
        item = _TestItem("test")

        assert not _TestRegistry.is_registered("MyItem")
        _TestRegistry.register("MyItem", item)
        assert _TestRegistry.is_registered("MyItem")

    def test_list_items(self):
        """Test listing all registered items."""
        _TestRegistry.register("Item1", _TestItem("a"))
        _TestRegistry.register("Item2", _TestItem("b"))
        _TestRegistry.register("Item3", _TestItem("c"))

        items = _TestRegistry.list_items()
        assert len(items) == 3
        # Names should be lowercase
        assert "item1" in items
        assert "item2" in items
        assert "item3" in items

    def test_clear(self):
        """Test clearing all items."""
        _TestRegistry.register("Item1", _TestItem("a"))
        _TestRegistry.register("Item2", _TestItem("b"))

        assert len(_TestRegistry.list_items()) == 2
        _TestRegistry.clear()
        assert len(_TestRegistry.list_items()) == 0

    def test_thread_safety(self):
        """Test that registry is thread-safe."""
        import concurrent.futures

        results = []

        def register_many():
            for i in range(10):
                name = f"item_{i}"
                _TestRegistry.register(name, _TestItem(name))
                results.append(name)

        # Register from multiple threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(register_many) for _ in range(5)]
            concurrent.futures.wait(futures)

        # All should have registered successfully
        assert len(results) == 50
        for i in range(10):
            assert _TestRegistry.is_registered(f"item_{i}")

    def test_overwrite(self):
        """Test that registering same name twice overwrites."""
        item1 = _TestItem("first")
        item2 = _TestItem("second")

        _TestRegistry.register("MyItem", item1)
        assert _TestRegistry.get("MyItem").name == "first"

        _TestRegistry.register("MyItem", item2)
        assert _TestRegistry.get("MyItem").name == "second"
