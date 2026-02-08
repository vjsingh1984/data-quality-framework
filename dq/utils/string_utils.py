# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""String utilities for naming conventions."""

import re
from typing import List


# Common compound word patterns for framework names
_COMPOUND_WORD_PATTERNS = {
    "schema": ["schema"],
    "validation": ["validation"],
    "schemavalidation": ["schema", "validation"],
    "great": ["great"],
    "expectations": ["expectations"],
    "greatexpectations": ["great", "expectations"],
    "data": ["data"],
    "quality": ["quality"],
    "framework": ["framework"],
}


def split_compound_word(word: str) -> List[str]:
    """Split a compound word into its component words for proper capitalization.

    This function handles common compound words in the framework and provides
    intelligent splitting. For unknown words, it falls back to a simple
    capitalization strategy.

    Args:
        word: The compound word to split (e.g., "schemavalidation").

    Returns:
        List of component words (e.g., ["schema", "validation"]).

    Examples:
        >>> split_compound_word("schemavalidation")
        ["schema", "validation"]
        >>> split_compound_word("greatexpectations")
        ["great", "expectations"]
        >>> split_compound_word("myengine")
        ["myengine"]  # Unknown pattern, return as-is
    """
    # If it has underscores, split on them first (most common case)
    if "_" in word:
        # "schema_validation" -> ["schema", "validation"]
        return word.split("_")

    # Check if it's a known compound word pattern
    word_lower = word.lower()

    if word_lower in _COMPOUND_WORD_PATTERNS:
        return _COMPOUND_WORD_PATTERNS[word_lower]

    # Try to find known sub-patterns within the word
    for pattern, parts in _COMPOUND_WORD_PATTERNS.items():
        if pattern in word_lower and pattern != word_lower:
            # Split the word around the known pattern
            parts_list = []
            remaining = word_lower

            # Find prefix
            if remaining.startswith(pattern):
                parts_list.extend(parts)
                remaining = remaining[len(pattern):]
            else:
                # Check if pattern is in the middle
                idx = remaining.find(pattern)
                if idx > 0:
                    parts_list.append(remaining[:idx])
                    remaining = remaining[idx:]

                parts_list.extend(parts)
                remaining = remaining[len(pattern):]

            # Handle remaining suffix
            if remaining:
                parts_list.append(remaining)

            return parts_list

    # Fallback: return as-is for unknown words
    return [word]
