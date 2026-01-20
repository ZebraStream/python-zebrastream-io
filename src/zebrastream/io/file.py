# SPDX-License-Identifier: MIT
"""
Synchronous file-like wrappers for ZebraStream I/O.
This module provides synchronous `Reader` and `Writer` classes that wrap the asynchronous
ZebraStream protocol implementations, allowing seamless integration with code expecting
standard file-like interfaces. The wrappers use AnyIO's blocking portal to bridge between
sync and async code, supporting context management and typical file operations.
"""

import atexit
import inspect
import io
import logging
import os
import tempfile
import threading
import weakref
from collections.abc import Awaitable, Callable
from concurrent.futures import CancelledError
from contextlib import contextmanager
from typing import Any, BinaryIO, Generator, TextIO, TypeVar, overload

import anyio

from ._core import AsyncReader, AsyncWriter
from ._exceptions import (
    AuthenticationError,
    ConnectionFailedError,
    ConnectionTimeoutError,
    DownloadError,
    NotStartedError,
    PeerDisconnectedError,
    ProtocolError,
    StreamClosedError,
    UploadError,
)

logger = logging.getLogger(__name__)
T = TypeVar('T')

class _PortalManager:
    """Manages anyio blocking portal lifecycle with cancellation support."""
    
    # Class-level type annotations - use WeakSet to avoid reference leaks
    _instances: weakref.WeakSet['_PortalManager'] = weakref.WeakSet()
    _instances_lock: threading.Lock = threading.Lock()
    
    _blocking_portal: Any  # FIX: AnyIO type
    _blocking_portal_cm: Any  # FIX: AnyIO type
    _cancel_scope: anyio.CancelScope | None
    _is_closed: bool

    def __init__(self) -> None:
        """Initialize and start the blocking portal."""
        logger.debug("Initializing PortalManager")
        
        self._is_closed = False
        self._cancel_scope = None
        
        # Register for cleanup - WeakSet doesn't keep strong references
        with self._instances_lock:
            self._instances.add(self)
        
        try:
            # If this succeeds, object is guaranteed to be fully initialized
            self._open_blocking_portal()
        except Exception:
            self._is_closed = True
            raise

    def _open_blocking_portal(self) -> None:
        """Start the anyio blocking portal."""
        self._blocking_portal = anyio.from_thread.start_blocking_portal("asyncio")
        self._blocking_portal_cm = self._blocking_portal.__enter__()


    def _close_blocking_portal(self) -> None:
        """Stop the anyio blocking portal."""
        self._blocking_portal.__exit__(None, None, None)
        del self._blocking_portal_cm
        del self._blocking_portal
    
    def close(self) -> None:
        """
        Close the portal and release resources (idempotent).
        
        This method is safe to call multiple times.
        """
        if self._is_closed:
            return
        
        logger.debug("Closing PortalManager")
        self._is_closed = True
        
        # Cancel any ongoing operations
        if self._cancel_scope is not None:
            try:
                self._cancel_scope.cancel()
            except Exception:
                logger.exception("Error cancelling scope during close")
        
        # Close the portal
        try:
            self._close_blocking_portal()
        except Exception:
            logger.exception("Error closing blocking portal")

    @overload  
    def call(self, callable: Callable[..., Awaitable[T]], cancellable: bool, *args: Any, **kwargs: Any) -> T: ...
    
    @overload
    def call(self, callable: Callable[..., T], cancellable: bool, *args: Any, **kwargs: Any) -> T: ...

    def call(self, callable: Callable[..., Any], cancellable: bool, *args: Any, **kwargs: Any) -> Any:
        """
        Run a callable in the blocking portal with cancellation support.
        
        For async callables, wraps in a cancellation scope that can be triggered
        by any exception from the calling thread (including KeyboardInterrupt).
        
        This provides proper cancellation semantics, ensuring async tasks are
        cleaned up even when the sync side encounters errors.
        
        Args:
            callable: Sync or async callable to run in the event loop
            cancellable: Whether to make the call cancellable (if async)
            *args: Positional arguments for the callable
            **kwargs: Keyword arguments for the callable
            
        Returns:
            The return value of the callable
            
        Raises:
            Any exception raised by the callable is propagated after cleanup
        """
        # Make cancellable if async
        if cancellable and inspect.iscoroutinefunction(callable):
            # Async callable - wrap in cancellation scope for proper cancellation
            async def _with_cancellation() -> Any:
                with anyio.CancelScope() as scope:
                    self._cancel_scope = scope
                    try:
                        return await callable(*args, **kwargs)
                    finally:
                        self._cancel_scope = None
            
            try:
                return self._blocking_portal_cm.call(_with_cancellation)
            finally:
                # Always cancel if scope still exists (means abnormal exit)
                if self._cancel_scope is not None:
                    logger.debug("Abnormal exit detected, cancelling async operation")
                    try:
                        # cancel() is thread-safe, can be called directly
                        self._cancel_scope.cancel()
                    except Exception:
                        # Best effort - original exception will be raised
                        logger.exception("Failed to cancel scope during cleanup")
        else:
            # Run directly (no cancellation scope needed)
            return self._blocking_portal_cm.call(callable, *args, **kwargs)

    def __del__(self) -> None:
        """Clean up portal when object is destroyed."""
        try:
            logger.debug("Cleaning up PortalManager in destructor")
            self.close()
        except Exception:
            logger.exception("Error during PortalManager cleanup")


class _AsyncInstanceManager:
    """Manages async instance lifecycle using a portal manager."""
    
    # Instance-level type annotations
    portal: _PortalManager
    instance: AsyncReader | AsyncWriter | None
    _owns_portal: bool
    _is_closed: bool

    def __init__(self, async_factory: Callable[[], AsyncReader | AsyncWriter], portal_manager: _PortalManager | None = None) -> None:
        """
        Initialize async instance manager.
        
        Args:
            async_factory: Function that creates the async instance
            portal_manager: Portal manager to use (creates new one if None)
        """
        logger.debug("Initializing AsyncInstanceManager")
        
        self._is_closed = False
        
        # Use provided portal or create new one
        if portal_manager is None:
            self.portal = _PortalManager()
            self._owns_portal = True
        else:
            self.portal = portal_manager
            self._owns_portal = False
        
        self.instance = None  # type: ignore[assignment]
        
        try:
            # If this succeeds, object is guaranteed to be fully initialized
            self.instance = self.portal.call(
                callable=async_factory,
                cancellable=False,
            )
            self.portal.call(
                callable=self.instance.start,
                cancellable=True,
            )
        except (KeyboardInterrupt, Exception):
            # Clean up on any failure (including interrupt)
            logger.debug("Initialization failed or interrupted, cleaning up")
            self.close()
            raise

    def close(self) -> None:
        """
        Close the manager and release resources (idempotent).
        
        This method is safe to call multiple times.
        """
        if self._is_closed:
            return
        
        logger.debug("Closing AsyncInstanceManager")
        self._is_closed = True
        
        # Stop instance if it was created (stop() is idempotent, safe to call anytime)
        if self.instance is not None:
            try:
                self.portal.call(
                    callable=self.instance.stop,
                    cancellable=False, # probalby slightly better choice, but True also works
                )
            finally:
                self.instance = None
        
        # Close portal if we own it
        if self._owns_portal:
            try:
                self.portal.close()
            except Exception:
                logger.exception("Error closing portal during cleanup")

    def __del__(self) -> None:
        """Clean up async instance when object is destroyed."""
        try:
            logger.debug("Cleaning up AsyncInstanceManager in destructor")
            self.close()
        except Exception:
            logger.exception("Error during AsyncInstanceManager cleanup")


@atexit.register
def _cleanup_portal_instances() -> None:
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
            # Explicitly call close() for cleanup (idempotent)
            instance.close()
        except Exception:
            logger.exception("Error cleaning up instance during shutdown")


@contextmanager
def seekable_from_stream(stream: BinaryIO, chunk_size: int = 1024 * 1024) -> Generator[BinaryIO, None, None]:
    """
    Create a seekable file-like object from a sequential stream by buffering to disk.
    
    Args:
        stream: Sequential binary stream to buffer
        chunk_size: Size of chunks to read at a time (default: 1MB)
        
    Yields:
        BinaryIO: Seekable file object backed by temporary file
        
    Note:
        The temporary file is automatically cleaned up after the context exits.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.dat', mode='w+b')
    tmp_name = tmp.name
    try:
        logger.debug(f"Buffering stream to temporary file: {tmp_name}")
        while chunk := stream.read(chunk_size):
            tmp.write(chunk)
        tmp.flush()
        tmp.seek(0)
        logger.debug(f"Stream buffered successfully ({tmp.tell()} bytes)")
        yield tmp
    finally:
        tmp.close()
        try:
            os.unlink(tmp_name)
            logger.debug(f"Temporary file cleaned up: {tmp_name}")
        except OSError:
            logger.warning(f"Failed to clean up temporary file: {tmp_name}")


@contextmanager
def _create_buffered_reader(binary_reader: BinaryIO, encoding: str | None, download_chunk_size: int) -> Generator[TextIO | BinaryIO, None, None]:
    """
    Create a buffered, seekable reader from a sequential binary reader.
    
    Args:
        binary_reader: Sequential binary reader to buffer
        encoding: Text encoding (None for binary mode)
        download_chunk_size: Size of chunks to read when buffering
        
    Yields:
        TextIO or BinaryIO: Seekable file object (text or binary)
    """
    with binary_reader as sequential:
        with seekable_from_stream(sequential, download_chunk_size) as seekable:
            if encoding is not None:
                yield io.TextIOWrapper(seekable, encoding=encoding)
            else:
                yield seekable


def open(mode: str, encoding: str = "utf-8", random_read: bool = False, download_chunk_size: int = 1024 * 1024, **kwargs: Any) -> "TextIO | BinaryIO":
    """
    Open a ZebraStream stream path for reading or writing.

    Args:
        mode (str): Mode to open the stream. 'r'/'rt'/'rb' for reading, 'w'/'wt'/'wb' for writing.
        encoding (str): Text encoding. Only used for text modes. Default: 'utf-8'.
        random_read (bool): If True, buffers stream to temp file (1MB chunks) for seek/tell support.
            Supports pandas.read_parquet(), PyArrow, ZIP, etc. Only applicable for read modes.
        download_chunk_size (int): Size of chunks for streaming when random_read=True (default: 1MB).
        **kwargs: Additional arguments passed to the corresponding Reader or Writer class.
        These may include:
        stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
        access_token (str, optional): Access token for authentication.
        content_type (str, optional): Content type for the stream.
        connect_timeout (int, optional): Timeout in seconds for the connect operation.
        connect_api_url (str, optional): Base URL for the ZebraStream Connect API.
            Defaults to the public ZebraStream cloud service.

    Returns:
        TextIO or BinaryIO: Text wrapper for text modes, binary Reader/Writer for binary modes.
        When random_read=True, returns a context manager yielding a seekable file.
        
    Note:
        Data may be buffered internally for efficiency. For immediate transmission in write modes,
        call flush() after write() operations.

    Raises:
        ValueError: If mode is not supported or random_read=True with write mode.
        OSError: If connection fails or authentication fails.
        TimeoutError: If connection times out.
    """
    logger.debug(f"Opening ZebraStream in mode '{mode}', random_read={random_read}")
    
    # Validate random_read usage
    if random_read and mode not in ("r", "rt", "rb"):
        logger.error("random_read=True only supported for read modes")
        raise ValueError("random_read=True only supported for read modes ('r', 'rt', 'rb')")
    
    try:
        # Normalize mode
        if mode in ("r", "rt"):
            # Text read mode
            binary_reader = Reader(**kwargs)
            if random_read:
                return _create_buffered_reader(binary_reader, encoding, download_chunk_size)
            return io.TextIOWrapper(binary_reader, encoding=encoding)
        if mode == "rb":
            # Binary read mode
            binary_reader = Reader(**kwargs)
            if random_read:
                return _create_buffered_reader(binary_reader, None, download_chunk_size)
            return binary_reader
        if mode in ("w", "wt"):
            # Text write mode
            binary_writer = Writer(**kwargs)
            return io.TextIOWrapper(binary_writer, encoding=encoding)
        if mode == "wb":
            # Binary write mode
            return Writer(**kwargs)
        logger.error(f"Unsupported mode: {mode!r}")
        raise ValueError(f"Unsupported mode: {mode!r}. Supported: 'r', 'rt', 'rb', 'w', 'wt', 'wb'.")
    except ConnectionTimeoutError as e:
        raise TimeoutError(f"open failed: {e.message}") from e
    except (ConnectionFailedError, AuthenticationError) as e:
        raise OSError(f"open failed: {e.message}") from e


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
        
        Raises:
            TypeError: If data is not bytes.
            ValueError: If file is closed.
            OSError: If stream not started or generic I/O failure.
            BrokenPipeError: If peer disconnected during write.
            TimeoutError: If write operation timed out.
        """
        if not isinstance(data, bytes):
            raise TypeError(f"a bytes-like object is required, not '{type(data).__name__}'")
        
        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")
            
        logger.debug(f"Writing {len(data)} bytes")
        try:
            self._async_manager.portal.call(
                callable=self._async_manager.instance.write,
                cancellable=True,
                data=data,
            )
        except StreamClosedError as e:
            raise ValueError("I/O operation on closed file") from e
        except NotStartedError as e:
            raise OSError(f"write failed: stream not started") from e
        except PeerDisconnectedError as e:
            raise BrokenPipeError(f"write failed: peer disconnected") from e
        except UploadError as e:
            raise OSError(f"write failed: {e.message}") from e
        except ConnectionTimeoutError as e:
            raise TimeoutError(f"write failed: {e.message}") from e
        except (ConnectionFailedError, AuthenticationError, ProtocolError) as e:
            raise OSError(f"write failed: {e.message}") from e
        
        return len(data)
    
    def readable(self) -> bool:
        return False  # General capability - never changes
    
    def writable(self) -> bool:
        return True   # General capability - never changes
    
    def seekable(self) -> bool:
        return False  # General capability - never changes
    
    def flush(self) -> None:
        """
        Flush buffered data for immediate transmission.
        
        Raises:
            ValueError: If file is closed.
            OSError: If background upload has failed.
            BrokenPipeError: If peer disconnected during upload.
            TimeoutError: If background upload timed out.
        """
        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")
        
        try:
            self._async_manager.portal.call(
                callable=self._async_manager.instance.flush,
                cancellable=True
            )
        except StreamClosedError as e:
            raise ValueError("I/O operation on closed file") from e
        except NotStartedError as e:
            raise OSError("flush failed: stream not started") from e
        except PeerDisconnectedError as e:
            raise BrokenPipeError("flush failed: peer disconnected") from e
        except UploadError as e:
            raise OSError(f"flush failed: {e.message}") from e
        except ConnectionTimeoutError as e:
            raise TimeoutError(f"flush failed: {e.message}") from e
        except (ConnectionFailedError, AuthenticationError, ProtocolError) as e:
            raise OSError(f"flush failed: {e.message}") from e
    
    def close(self) -> None:
        """
        Close the writer and release all resources.
        
        Note: This method is more lenient than other methods.
        State errors (already closed) are ignored. Only serious errors
        (transfer failures, connection issues) may propagate as OSError.
        
        Raises:
            OSError: If cleanup encounters serious errors.
        """
        if self._async_manager is not None:
            try:
                self._async_manager.close()
            except (UploadError, DownloadError, ConnectionFailedError, AuthenticationError, ProtocolError) as e:
                # Serious errors during cleanup should propagate
                logger.error(f"Error during close: {e.message}")
                raise OSError(f"close failed: {e.message}") from e
            except (StreamClosedError, NotStartedError, PeerDisconnectedError):
                # State errors and peer disconnects during cleanup are logged but not raised
                # These are expected in various shutdown scenarios
                pass
            except Exception as e:
                # Unexpected errors are logged but not raised to ensure cleanup completes
                logger.warning(f"Unexpected error during close: {e}")
                pass
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
        """
        Read bytes from the ZebraStream data stream.
        
        Raises:
            ValueError: If file is closed.
            OSError: If stream not started or generic I/O failure.
            BrokenPipeError: If peer disconnected during read.
            TimeoutError: If read operation timed out.
        """
        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")
            
        logger.debug(f"Reading up to {size} bytes")
        if size == 0:
            return b""
        
        try:
            if size < 0:
                return self._async_manager.portal.call(
                    callable=self._async_manager.instance.read_all,
                    cancellable=True,
                )
            return self._async_manager.portal.call(
                callable=self._async_manager.instance.read_variable_block,
                cancellable=True,
                n=size,
            )
        except StreamClosedError as e:
            raise ValueError("I/O operation on closed file") from e
        except NotStartedError as e:
            raise OSError("read failed: stream not started") from e
        except PeerDisconnectedError as e:
            raise BrokenPipeError("read failed: peer disconnected") from e
        except DownloadError as e:
            raise OSError(f"read failed: {e.message}") from e
        except ConnectionTimeoutError as e:
            raise TimeoutError(f"read failed: {e.message}") from e
        except (ConnectionFailedError, AuthenticationError, ProtocolError) as e:
            raise OSError(f"read failed: {e.message}") from e
    
    def readable(self) -> bool:
        return True   # General capability - never changes
    
    def writable(self) -> bool:
        return False  # General capability - never changes
    
    def seekable(self) -> bool:
        return False  # General capability - never changes
    
    def flush(self) -> None:
        pass  # No-op for readers
    
    def close(self) -> None:
        """
        Close the reader and release all resources.
        
        Note: This method is more lenient than other methods.
        State errors (already closed) are ignored. Only serious errors
        (transfer failures, connection issues) may propagate as OSError.
        
        Raises:
            OSError: If cleanup encounters serious errors.
        """
        if self._async_manager is not None:
            try:
                self._async_manager.close()
            except (UploadError, DownloadError, ConnectionFailedError, AuthenticationError, ProtocolError) as e:
                # Serious errors during cleanup should propagate
                logger.error(f"Error during close: {e.message}")
                raise OSError(f"close failed: {e.message}") from e
            except (StreamClosedError, NotStartedError, PeerDisconnectedError):
                # State errors and peer disconnects during cleanup are logged but not raised
                # These are expected in various shutdown scenarios
                pass
            except Exception as e:
                # Unexpected errors are logged but not raised to ensure cleanup completes
                logger.warning(f"Unexpected error during close: {e}")
                pass
            finally:
                self._async_manager = None
    
    @property
    def closed(self) -> bool:
        """Return True if the reader is closed."""
        return self._async_manager is None
