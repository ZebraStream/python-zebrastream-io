# SPDX-License-Identifier: MIT
"""
Synchronous file-like wrappers for ZebraStream I/O.
This module provides synchronous `Reader` and `Writer` classes that wrap the asynchronous
ZebraStream protocol implementations, allowing seamless integration with code expecting
standard file-like interfaces. The wrappers use AnyIO's blocking portal to bridge between
sync and async code, supporting context management and typical file operations.
"""

import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, overload

import anyio

from ._core import AsyncReader, AsyncWriter

logger = logging.getLogger(__name__)
T = TypeVar('T')

# TODO: portal owning base class for sync wrappers


def open(mode: str, **kwargs: Any) -> "Reader | Writer":
    """
    Open a ZebraStream stream path for reading or writing.

    Args:
        mode (str): Mode to open the stream. 'rb' for reading, 'wb' for writing.
        **kwargs: Additional arguments passed to the corresponding Reader or Writer class.
        These may include:
        stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
        access_token (str, optional): Access token for authentication.
        content_type (str, optional): Content type for the stream.
        connect_timeout (int, optional): Timeout in seconds for the connect operation.

    Returns:
        Reader or Writer: An instance of Reader (for 'rb') or Writer (for 'wb').

    Raises:
        ValueError: If mode is not 'rb' or 'wb'.
    """
    logger.debug(f"Opening ZebraStream in mode '{mode}'")
    if mode == "rb":
        return Reader(**kwargs)
    elif mode == "wb":
        return Writer(**kwargs)
    else:
        logger.error(f"Unsupported mode: {mode!r}")
        raise ValueError(f"Unsupported mode: {mode!r}. Only 'rb' and 'wb' are supported.")

# TODO: use AnyIO async context manager wrapper for simpler code, avoid calling private methods
class Writer:
    _async_writer_factory: Callable[[], AsyncWriter]
    _async_writer: AsyncWriter
    _blocking_portal: Any  # FIX: AnyIO type
    _blocking_portal_cm: Any  # FIX: AnyIO type
    _is_open: bool

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Writer for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncWriter (e.g., stream_path, access_token, content_type, connect_timeout).
        """
        logger.debug("Initializing sync Writer")
        self._async_writer_factory = lambda: AsyncWriter(**kwargs)
        self._is_open = False
        self._open()
    
    def _start_blocking_portal(self) -> None:
        """Start the anyio blocking portal."""
        assert not hasattr(self, "_blocking_portal"), "Portal is already started"  # TODO: remove assert, cannot happen internally
        self._blocking_portal = anyio.from_thread.start_blocking_portal("asyncio")
        self._blocking_portal_cm = self._blocking_portal.__enter__()
    
    def _stop_blocking_portal(self) -> None:
        """Stop the anyio blocking portal."""
        assert hasattr(self, "_blocking_portal"), "Portal is not started"  # TODO: remove assert, cannot happen internally
        del self._blocking_portal_cm  # TODO: needed?
        self._blocking_portal.__exit__(None, None, None)

    @overload  
    def _call_async(self, callable: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T: ...
    
    @overload
    def _call_async(self, callable: Callable[..., T], *args: Any, **kwargs: Any) -> T: ...

    def _call_async(self, callable: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run a callable in the blocking portal."""
        assert hasattr(self, "_blocking_portal"), "Portal is not started"
        return self._blocking_portal_cm.call(callable, *args, **kwargs)
    
    def _create_async_writer(self) -> None:
        """Create an AsyncWriter instance."""
        assert not hasattr(self, "_async_writer"), "AsyncZebraStreamWriter is already created"  # TODO: remove assert, cannot happen internally
        self._async_writer = self._call_async(self._async_writer_factory)
        self._call_async(self._async_writer.start)  # TODO: use async context manager instead?
    
    def _destroy_async_writer(self) -> None:
        """Destroy the AsyncWriter instance."""
        self._call_async(self._async_writer.stop)
        del self._async_writer  # TODO: needed?

    def _open(self) -> None:
        # TODO: merge into init?
        assert not self._is_open, "Writer is already open" # TODO: remove, cannot happen internally
        logger.debug("Opening sync Writer")
        self._start_blocking_portal()
        self._create_async_writer()
        self._is_open = True

    def write(self, data: bytes) -> None:
        """
        Write bytes to the ZebraStream data stream.

        Args:
            data (bytes): The data to write.
        Raises:
            RuntimeError: If the writer is not open.
        """
        if not self._is_open:
            raise RuntimeError("Writer is not open")
        logger.debug(f"Writing {len(data)} bytes")
        self._call_async(self._async_writer.write, data)

    def close(self) -> None:
        """
        Close the writer and release all resources.
        """
        if not self._is_open:
            raise RuntimeError("Writer is not open")
        logger.debug("Closing sync Writer")
        self._destroy_async_writer()
        self._stop_blocking_portal()
        self._is_open = False

    def writable(self) -> bool:
        """
        Return True if the stream supports writing.
        """
        return True

    def readable(self) -> bool:
        """
        Return True if the stream supports reading.
        """
        return False

    def seekable(self) -> bool:
        """
        Return True if the stream supports random access.
        """
        return False

    def flush(self) -> None:
        """
        Flush the write buffer, ensuring all data is sent to the stream.
        """
        if not self._is_open:
            raise RuntimeError("Writer is not open")
        self._call_async(self._async_writer.flush)

    @property
    def closed(self) -> bool:
        """
        Return True if the writer is closed.
        """
        return not self._is_open

    def __enter__(self) -> "Writer":
        """
        Enter the runtime context related to this object.
        Returns:
            Writer: self
        """
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the runtime context and close the writer.
        """
        self.close()


# TODO: use AnyIO async context manager wrapper for simpler code, avoid calling private methods
class Reader:
    _async_reader_factory: Callable[[], AsyncReader]
    _async_reader: AsyncReader
    _blocking_portal: Any  # FIX: AnyIO type
    _blocking_portal_cm: Any  # FIX: AnyIO type
    _is_open: bool

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Reader for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncReader (e.g., stream_path, access_token, content_type, connect_timeout).
        """
        logger.debug("Initializing sync Reader")
        self._async_reader_factory = lambda: AsyncReader(**kwargs)
        self._is_open = False
        self._open()
    
    def _start_blocking_portal(self) -> None:
        """Start the anyio blocking portal."""
        assert not hasattr(self, "_blocking_portal"), "Portal is already started"  # TODO: remove assert, cannot happen internally
        self._blocking_portal = anyio.from_thread.start_blocking_portal("asyncio")
        self._blocking_portal_cm = self._blocking_portal.__enter__()
    
    def _stop_blocking_portal(self) -> None:
        """Stop the anyio blocking portal."""
        assert hasattr(self, "_blocking_portal"), "Portal is not started"  # TODO: remove assert, cannot happen internally
        del self._blocking_portal_cm  # TODO: needed?
        self._blocking_portal.__exit__(None, None, None)
    
    @overload  
    def _call_async(self, callable: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T: ...
    
    @overload
    def _call_async(self, callable: Callable[..., T], *args: Any, **kwargs: Any) -> T: ...

    def _call_async(self, callable: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run a callable in the blocking portal."""
        assert hasattr(self, "_blocking_portal"), "Portal is not started"
        return self._blocking_portal_cm.call(callable, *args, **kwargs)
    
    def _create_async_reader(self) -> None:
        """Create an AsyncReader instance."""
        assert not hasattr(self, "_async_reader"), "AsyncZebraStreamReader is already created"  # TODO: remove assert, cannot happen internally
        self._async_reader = self._call_async(self._async_reader_factory)
        self._call_async(self._async_reader.start)  # TODO: use async context manager instead?
    
    def _destroy_async_reader(self) -> None:
        """Destroy the AsyncReader instance."""
        self._call_async(self._async_reader.stop)
        del self._async_reader  # TODO: needed?

    def _open(self) -> None:
        # TODO: merge into init?
        assert not self._is_open, "Reader is already open" # TODO: remove, cannot happen internally
        logger.debug("Opening sync Reader")
        self._start_blocking_portal()
        self._create_async_reader()
        self._is_open = True

    def read(self, size: int = -1) -> bytes:
        """
        Read bytes from the ZebraStream data stream.

        Args:
            size (int): Number of bytes to read. Default is -1 (read until EOF or available data).
        Returns:
            bytes: The data read from the stream.
        Raises:
            RuntimeError: If the reader is not open.
        """
        if not self._is_open:
            raise RuntimeError("Reader is not open")
        logger.debug(f"Reading up to {size} bytes")
        return self._call_async(self._async_reader.read_exactly, size)

    def close(self) -> None:
        """
        Close the reader and release all resources.
        """
        if not self._is_open:
            raise RuntimeError("Reader is not open")
        logger.debug("Closing sync Reader")
        self._destroy_async_reader()
        self._stop_blocking_portal()
        self._is_open = False

    def readable(self) -> bool:
        """
        Return True if the stream supports reading.
        """
        return True

    def writable(self) -> bool:
        """
        Return True if the stream supports writing.
        """
        return False

    def seekable(self) -> bool:
        """
        Return True if the stream supports random access.
        """
        return False

    def flush(self) -> None:
        """
        No-op flush for Reader (for API compatibility).
        """
        pass

    @property
    def closed(self) -> bool:
        """
        Return True if the reader is closed.
        """
        return not self._is_open

    def __enter__(self) -> "Reader":
        """
        Enter the runtime context related to this object.
        Returns:
            Reader: self
        """
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the runtime context and close the reader.
        """
        self.close()
