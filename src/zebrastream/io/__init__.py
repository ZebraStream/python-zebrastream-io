# SPDX-License-Identifier: MIT
"""ZebraStream IO package for file-like interfaces."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("zebrastream-io")
except PackageNotFoundError:
    __version__ = "unknown"
