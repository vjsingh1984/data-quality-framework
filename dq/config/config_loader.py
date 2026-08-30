# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration loading abstractions and implementations."""
import logging
from abc import ABC, abstractmethod
from typing import Dict, Type
from urllib.parse import urlparse

from pyhocon import ConfigFactory, ConfigTree

from dq.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

# Registry of scheme -> loader class
_SCHEME_LOADERS: Dict[str, Type["ConfigLoader"]] = {}


def register_loader(scheme: str, loader_class: Type["ConfigLoader"]) -> None:
    """Register a config loader for a URI scheme.

    Args:
        scheme: URI scheme (e.g. "s3", "abfss").
        loader_class: ConfigLoader subclass to handle that scheme.
    """
    _SCHEME_LOADERS[scheme.lower()] = loader_class


class ConfigLoader(ABC):
    """Abstract base class for configuration loaders."""

    @abstractmethod
    def load(self, source: str) -> ConfigTree:
        """Load and parse a HOCON configuration.

        Args:
            source: Configuration source (string content, file path, or URI).

        Returns:
            Parsed ConfigTree.

        Raises:
            ConfigurationError: If configuration cannot be loaded or parsed.
        """


class StringConfigLoader(ConfigLoader):
    """Loads HOCON configuration from an inline string."""

    def load(self, source: str) -> ConfigTree:
        try:
            return ConfigFactory.parse_string(source)
        except Exception as e:
            raise ConfigurationError(f"Failed to parse config string: {e}") from e


class FileConfigLoader(ConfigLoader):
    """Loads HOCON configuration from a local file."""

    def load(self, source: str) -> ConfigTree:
        file_path = (
            source.replace("file://", "") if source.startswith("file://") else source
        )
        try:
            return ConfigFactory.parse_file(file_path)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load config file '{file_path}': {e}"
            ) from e


class S3ConfigLoader(ConfigLoader):
    """Loads HOCON configuration from an S3 bucket."""

    def load(self, source: str) -> ConfigTree:
        from dq.utils import config_utils

        parsed = urlparse(source)
        try:
            content = config_utils.load_from_s3(bucket=parsed.netloc, key=parsed.path)
            return ConfigFactory.parse_string(content)
        except ConfigurationError:
            raise
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load config from S3 '{source}': {e}"
            ) from e


class ADLSConfigLoader(ConfigLoader):
    """Loads HOCON configuration from Azure Data Lake Storage."""

    def load(self, source: str) -> ConfigTree:
        from dq.utils import config_utils

        try:
            content = config_utils.load_from_adls(source)
            return ConfigFactory.parse_string(content)
        except ConfigurationError:
            raise
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load config from ADLS '{source}': {e}"
            ) from e


class HttpConfigLoader(ConfigLoader):
    """Loads HOCON configuration from an HTTP/HTTPS URL."""

    def load(self, source: str) -> ConfigTree:
        from dq.utils import config_utils

        try:
            content = config_utils.load_from_uri(source)
            if content is None:
                raise ConfigurationError(
                    f"HTTP request returned no content for '{source}'"
                )
            return ConfigFactory.parse_string(content)
        except ConfigurationError:
            raise
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load config from URL '{source}': {e}"
            ) from e


class AutoConfigLoader(ConfigLoader):
    """Automatically selects the appropriate loader based on URI scheme.

    Supports extensibility via ``register_loader(scheme, loader_class)``.
    """

    # Default scheme -> loader mapping
    _DEFAULT_LOADERS: Dict[str, Type[ConfigLoader]] = {
        "": StringConfigLoader,
        "file": FileConfigLoader,
        "s3": S3ConfigLoader,
        "abfss": ADLSConfigLoader,
        "http": HttpConfigLoader,
        "https": HttpConfigLoader,
    }

    def load(self, source: str) -> ConfigTree:
        parsed = urlparse(source)
        scheme = parsed.scheme.lower()

        # Check custom registry first, then defaults
        loader_class = _SCHEME_LOADERS.get(scheme) or self._DEFAULT_LOADERS.get(scheme)

        if loader_class is None:
            raise ConfigurationError(f"Unsupported config scheme: {scheme}")

        loader = loader_class()
        return loader.load(source)


# Register default loaders in the global registry
for _scheme, _cls in AutoConfigLoader._DEFAULT_LOADERS.items():
    register_loader(_scheme, _cls)
