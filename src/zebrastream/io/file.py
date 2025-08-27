# SPDX-License-Identifier: MIT
"""
Synchronous file-like wrappers for ZebraStream I/O.
This module provides synchronous `Reader` and `Writer` classes that wrap the asynchronous
ZebraStream protocol implementations, allowing seamless integration with code expecting
standard file-like interfaces. The wrappers use AnyIO's blocking portal to bridge between
sync and async code, supporting context management and typical file operations.
"""

import atexit
import io
import logging
import threading
import weakref
from collections.abc import Awaitable, Callable
from typing import Any, BinaryIO, TextIO, TypeVar, overload

import anyio

from ._core import AsyncReader, AsyncWriter

logger = logging.getLogger(__name__)
T = TypeVar('T')

class _PortalManager:
    """Manages anyio blocking portal lifecycle."""
    
    # Class-level type annotations - use WeakSet to avoid reference leaks
    _instances: weakref.WeakSet['_PortalManager'] = weakref.WeakSet()
    _instances_lock: threading.Lock = threading.Lock()
    
    _blocking_portal: Any  # FIX: AnyIO type
    _blocking_portal_cm: Any  # FIX: AnyIO type

    def __init__(self) -> None:
        """Initialize and start the blocking portal."""
        logger.debug("Initializing PortalManager")
        
        # Register for cleanup - WeakSet doesn't keep strong references
        with self._instances_lock:
            self._instances.add(self)
        
        # If this succeeds, object is guaranteed to be fully initialized
        self._open_blocking_portal()

    def _open_blocking_portal(self) -> None:
        """Start the anyio blocking portal."""
        self._blocking_portal = anyio.from_thread.start_blocking_portal("asyncio")
        self._blocking_portal_cm = self._blocking_portal.__enter__()

    def _close_blocking_portal(self) -> None:
        """Stop the anyio blocking portal."""
        self._blocking_portal.__exit__(None, None, None)
        del self._blocking_portal_cm
        del self._blocking_portal

    @overload  
    def call(self, callable: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T: ...
    
    @overload
    def call(self, callable: Callable[..., T], *args: Any, **kwargs: Any) -> T: ...

    def call(self, callable: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run a callable in the blocking portal."""
        return self._blocking_portal_cm.call(callable, *args, **kwargs)

    def __del__(self) -> None:
        """Clean up portal when object is destroyed."""
        try:
            logger.debug("Cleaning up PortalManager in destructor")
            self._close_blocking_portal()
            
            # No need to manually unregister - WeakSet handles it automatically
                
        except Exception:
            logger.exception("Error during PortalManager cleanup")


class _AsyncInstanceManager:
    """Manages async instance lifecycle using a portal manager."""
    
    # Instance-level type annotations
    portal: _PortalManager
    instance: AsyncReader | AsyncWriter
    _owns_portal: bool

    def __init__(self, async_factory: Callable[[], AsyncReader | AsyncWriter], portal_manager: _PortalManager | None = None) -> None:
        """
        Initialize async instance manager.
        
        Args:
            async_factory: Function that creates the async instance
            portal_manager: Portal manager to use (creates new one if None)
        """
        logger.debug("Initializing AsyncInstanceManager")
        
        # Use provided portal or create new one
        if portal_manager is None:
            self.portal = _PortalManager()
            self._owns_portal = True
        else:
            self.portal = portal_manager
            self._owns_portal = False
        
        # If this succeeds, object is guaranteed to be fully initialized
        self.instance = self.portal.call(async_factory)
        self.portal.call(self.instance.start)

    def __del__(self) -> None:
        """Clean up async instance when object is destroyed."""
        try:
            logger.debug("Cleaning up AsyncInstanceManager in destructor")
            
            # Stop async instance
            try:
                self.portal.call(self.instance.stop)
            except Exception:
                logger.exception("Error stopping async instance")
        
            # Clean up owned portal
            if self._owns_portal:
                try:
                    del self.portal
                except Exception:
                    logger.exception("Error cleaning up portal")
                
        except Exception:
            logger.exception("Error during AsyncInstanceManager cleanup")


@atexit.register
def _cleanup_portal_instances():
    """Clean up any remaining instances at exit."""
    while _PortalManager._instances:
        with _PortalManager._instances_lock:
            try:
                instance = _PortalManager._instances.pop()
            except KeyError:
                break  # Set became empty (shouldn't happen due to while condition)
        
        # Cleanup outside lock
        try:
            logger.debug(f"Emergency cleanup of {instance.__class__.__name__}")
            del instance  # Triggers __del__() properly
        except Exception:
            logger.exception("Error cleaning up instance during shutdown")


def open(mode: str, encoding: str = "utf-8", **kwargs: Any) -> "TextIO | BinaryIO":
    """
    Open a ZebraStream stream path for reading or writing.

    Args:
        mode (str): Mode to open the stream. 'r'/'rt'/'rb' for reading, 'w'/'wt'/'wb' for writing.
        encoding (str): Text encoding. Only used for text modes. Default: 'utf-8'.
        **kwargs: Additional arguments passed to the corresponding Reader or Writer class.
        These may include:
        stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
        access_token (str, optional): Access token for authentication.
        content_type (str, optional): Content type for the stream.
        connect_timeout (int, optional): Timeout in seconds for the connect operation.

    Returns:
        TextIO or BinaryIO: Text wrapper for text modes, binary Reader/Writer for binary modes.
        
    Note:
        Data may be buffered internally for efficiency. For immediate transmission in write modes,
        call flush() after write() operations.

    Raises:
        ValueError: If mode is not supported.
    """
    logger.debug(f"Opening ZebraStream in mode '{mode}'")
    
    # Normalize mode
    if mode in ("r", "rt"):
        # Text read mode
        binary_reader = Reader(**kwargs)
        return io.TextIOWrapper(binary_reader, encoding=encoding)
    elif mode == "rb":
        # Binary read mode
        return Reader(**kwargs)
    elif mode in ("w", "wt"):
        # Text write mode
        binary_writer = Writer(**kwargs)
        return io.TextIOWrapper(binary_writer, encoding=encoding)
    elif mode == "wb":
        # Binary write mode
        return Writer(**kwargs)
    else:
        logger.error(f"Unsupported mode: {mode!r}")
        raise ValueError(f"Unsupported mode: {mode!r}. Supported: 'r', 'rt', 'rb', 'w', 'wt', 'wb'.")


class _BinaryIOBase(BinaryIO):
    """Base class that implements BinaryIO interface for ZebraStream objects."""
    
    def __enter__(self) -> BinaryIO:
        """Return self as BinaryIO for context manager."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """Exit the runtime context and close the stream."""
        try:
            self.close()
        except Exception as close_error:
            # Log but don't mask original exception
            if exc_type is None:
                # No original exception, re-raise our close error
                raise close_error
            else:
                # There was an original exception, just log ours
                logger.exception("Error during context manager exit (original exception will be raised)")
                # Original exception will be re-raised automatically

    # Implement required BinaryIO methods that can be shared
    def readline(self, size: int = -1) -> bytes:
        """Read a line from the stream."""
        result = b""
        while True:
            char = self.read(1)
            if not char or char == b'\n':
                break
            result += char
            if size > 0 and len(result) >= size:
                break
        return result
    
    def readlines(self, hint: int = -1) -> list[bytes]:
        """Read lines from the stream."""
        lines = []
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
        return lines
    
    def writelines(self, lines) -> None:
        """Write lines to the stream."""
        for line in lines:
            self.write(line)
    
    # Unsupported operations for streams
    def seek(self, offset: int, whence: int = 0) -> int:
        raise io.UnsupportedOperation("seek")
    
    def tell(self) -> int:
        raise io.UnsupportedOperation("tell")
    
    def truncate(self, size: int | None = None) -> int:
        raise io.UnsupportedOperation("truncate")


class Writer(_BinaryIOBase):
    """
    Synchronous writer for ZebraStream data streams.
    
    Note: Data may be buffered internally. Use flush() for immediate transmission.
    """
    
    # Instance-level type annotation
    _async_manager: _AsyncInstanceManager | None

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Writer for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncWriter (e.g., stream_path, access_token, content_type, connect_timeout).
        """
        self._async_manager = _AsyncInstanceManager(lambda: AsyncWriter(**kwargs))

    def read(self, size: int = -1) -> bytes:
        """Writers don't support reading."""
        raise io.UnsupportedOperation("not readable")
    
    def write(self, data: bytes) -> int:
        """
        Write bytes. Data may be buffered - use flush() for immediate transmission.
        """
        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")
            
        logger.debug(f"Writing {len(data)} bytes")
        self._async_manager.portal.call(self._async_manager.instance.write, data)
        return len(data)
    
    def readable(self) -> bool:
        return False  # General capability - never changes
    
    def writable(self) -> bool:
        return True   # General capability - never changes
    
    def seekable(self) -> bool:
        return False  # General capability - never changes
    
    def flush(self) -> None:
        """Flush buffered data for immediate transmission."""
        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")
        self._async_manager.portal.call(self._async_manager.instance.flush)
    
    def close(self) -> None:
        """Close the writer and release all resources."""
        if self._async_manager is not None:
            del self._async_manager  # Triggers immediate cleanup via __del__
            self._async_manager = None
    
    @property 
    def closed(self) -> bool:
        """Required by BinaryIO interface."""
        return self._async_manager is None


class Reader(_BinaryIOBase):
    """Synchronous reader for ZebraStream data streams."""
    
    # Instance-level type annotation
    _async_manager: _AsyncInstanceManager | None

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Reader for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncReader (e.g., stream_path, access_token, content_type, connect_timeout).
        """
        self._async_manager = _AsyncInstanceManager(lambda: AsyncReader(**kwargs))

    def write(self, data: bytes) -> int:
        """Readers don't support writing."""
        raise io.UnsupportedOperation("not writable")
    
    def read(self, size: int = -1) -> bytes:
        """Read bytes from the ZebraStream data stream."""
        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")
            
        logger.debug(f"Reading up to {size} bytes")
        if size == 0:
            return b""
        if size < 0:
            return self._async_manager.portal.call(self._async_manager.instance.read_all)
        return self._async_manager.portal.call(self._async_manager.instance.read_variable_block, size)
    
    def readable(self) -> bool:
        return True   # General capability - never changes
    
    def writable(self) -> bool:
        return False  # General capability - never changes
    
    def seekable(self) -> bool:
        return False  # General capability - never changes
    
    def flush(self) -> None:
        pass  # No-op for readers
    
    def close(self) -> None:
        """Close the reader and release all resources."""
        if self._async_manager is not None:
            del self._async_manager  # Triggers immediate cleanup via __del__
            self._async_manager = None
    
    @property
    def closed(self) -> bool:
        """Return True if the reader is closed."""
        return self._async_manager is None
