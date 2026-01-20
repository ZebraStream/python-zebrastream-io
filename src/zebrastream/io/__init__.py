# SPDX-License-Identifier: MIT
"""ZebraStream IO package for file-like interfaces."""

from importlib.metadata import PackageNotFoundError, version

from ._core import DEFAULT_ZEBRASTREAM_CONNECT_API_URL

try:
    __version__ = version("zebrastream-io")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "DEFAULT_ZEBRASTREAM_CONNECT_API_URL",
]
