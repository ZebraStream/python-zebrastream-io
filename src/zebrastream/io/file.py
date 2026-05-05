# SPDX-License-Identifier: MIT
"""
Synchronous file-like wrappers for ZebraStream I/O.

This module provides synchronous `Reader` and `Writer` classes that wrap the asynchronous
ZebraStream protocol implementations, allowing seamless integration with code expecting
standard file-like interfaces. The wrappers use AnyIO's blocking portal to bridge between
sync and async code, supporting context management and typical file operations.

Basic Usage
-----------

::

    # Text or binary streaming
    with open(mode='r', stream_path='/logs') as f:
        for line in f:
            print(line)

    # Random access (pandas, PyArrow, ZIP files)
    with open(mode='rb', random_read=True, stream_path='/data.parquet') as f:
        df = pd.read_parquet(f)

See `open()` for detailed examples and all supported modes.

Key Concepts
------------

- **Context managers**: Always use `with` statements for automatic resource cleanup
- **Blocking I/O**: All operations block until complete (sync wrapper over async)
- **Buffering**: Data may be buffered internally; call `flush()` for immediate transmission
- **Random access**: Use `random_read=True` to buffer entire stream to temporary file
"""

import atexit
import inspect
import io
import logging
import os
import sys
import tempfile
import threading
import weakref
from collections.abc import Awaitable, Callable
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

__all__ = ["Reader", "Writer", "open", "seekable_from_stream"]

logger = logging.getLogger(__name__)
T = TypeVar("T")


class _PortalManager:
    """Manages anyio blocking portal lifecycle with cancellation support.

    This is the sync/async bridge: creates an event loop in a background thread
    and provides thread-safe access to run async code from sync context.
    WeakSet tracking enables reliable cleanup at interpreter shutdown.
    """

    # Class-level type annotations - use WeakSet to avoid reference leaks
    _instances: weakref.WeakSet["_PortalManager"] = weakref.WeakSet()
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
                        # Best effort — original exception will be raised
                        logger.debug("Failed to cancel scope during cleanup", exc_info=True)
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
    """Manages async instance lifecycle using a portal manager.

    Wraps AsyncReader/AsyncWriter instances and coordinates their lifecycle
    with the portal. Handles initialization (factory + start), cleanup (stop),
    and portal ownership (shared vs dedicated).
    """

    # Instance-level type annotations
    portal: _PortalManager
    instance: AsyncReader | AsyncWriter | None
    _owns_portal: bool
    _is_closed: bool

    def __init__(
        self, async_factory: Callable[[], AsyncReader | AsyncWriter], portal_manager: _PortalManager | None = None
    ) -> None:
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

        try:
            # Stop instance if it was created (stop() is idempotent, safe to call anytime)
            if self.instance is not None:
                try:
                    self.portal.call(
                        callable=self.instance.stop,
                        cancellable=False,  # probalby slightly better choice, but True also works
                    )
                finally:
                    self.instance = None
        finally:
            # Close portal if we own it — must always run, even if stop() raised,
            # otherwise the background event loop thread never terminates.
            if self._owns_portal:
                try:
                    self.portal.close()
                except Exception:
                    logger.exception("Error closing portal during cleanup")

    def abort(self) -> None:
        """
        Abort the managed writer without sending EOF (idempotent).

        Forwards to :meth:`AsyncWriter.abort` so the upload task is cancelled
        in-flight.  Falls back to a plain :meth:`close` if the underlying
        instance has no ``abort`` method (e.g. it is an :class:`AsyncReader`).
        """
        if self._is_closed:
            return

        logger.debug("Aborting AsyncInstanceManager")
        self._is_closed = True

        try:
            if self.instance is not None:
                try:
                    abort_fn = getattr(self.instance, "abort", None)
                    if abort_fn is not None:
                        self.portal.call(callable=abort_fn, cancellable=False)
                    else:
                        self.portal.call(callable=self.instance.stop, cancellable=False)
                finally:
                    self.instance = None
        finally:
            # Close portal if we own it — must always run, even if abort raised.
            if self._owns_portal:
                try:
                    self.portal.close()
                except Exception:
                    logger.exception("Error closing portal during abort cleanup")

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

    This is a context manager that buffers the entire stream into a temporary file,
    enabling random access for libraries like pandas, PyArrow, and ZIP readers.

    Args:
        stream: Sequential binary stream to buffer
        chunk_size: Size of chunks to read at a time (default: 1MB)

    Yields:
        BinaryIO: Seekable file object backed by temporary file

    Example::

        with seekable_from_stream(my_reader) as seekable_file:
            df = pd.read_parquet(seekable_file)
        # Temporary file is automatically cleaned up here

    Note:
        - Must be used as a context manager (with statement)
        - Buffers the entire stream to disk before yielding
        - Temporary file is automatically cleaned up on context exit
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".dat", mode="w+b")
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
def _create_buffered_reader(
    binary_reader: BinaryIO, encoding: str | None, download_chunk_size: int
) -> Generator[TextIO | BinaryIO, None, None]:
    """
    Create a buffered, seekable reader from a sequential binary reader (internal).

    This context manager buffers the entire stream to a temporary file,
    wrapping it in TextIOWrapper if encoding is specified.

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


def open(
    mode: str, encoding: str = "utf-8", random_read: bool = False, download_chunk_size: int = 1024 * 1024, **kwargs: Any
) -> "TextIO | BinaryIO":
    """
    Open a ZebraStream stream path for reading or writing.

    Args:
        mode (str): Mode to open the stream. 'r'/'rt'/'rb' for reading, 'w'/'wt'/'wb' for writing.
        encoding (str): Text encoding. Only used for text modes. Default: 'utf-8'.
        random_read (bool): If True, buffers entire stream to temporary file for random access.
            Returns a context manager (must use 'with' statement).
            Required for: pandas.read_parquet(), PyArrow, ZIP files, etc.
            Only applicable for read modes. Warning: Loads entire stream into temp file.
        download_chunk_size (int): Size of chunks when buffering with random_read (default: 1MB).
        **kwargs (Any): Stream configuration passed to Reader or Writer. These may include:

            - stream_path (str, required): The ZebraStream path (e.g., '/my-stream')
            - access_token (str): Access token for authentication
            - content_type (str): Content type for the stream
            - connect_timeout (int): Connection timeout in seconds
            - write_buffer_size (int): Optional write buffer size in bytes (default: 64 KiB). Only for write modes.
            - transfer_buffer_multiplier (int): Transfer buffer size multiplier (default: 10). Only for write modes.
            - auto_flush_delay (int): Automatic flush delay in seconds (default: None, disabled). Only for write modes.
              Set to 0 for instant flush on every write, 1+ for timer-based flushing. When enabled with a value >= 1,
              buffered data is flushed at most N seconds after first write.
            - connect_api_url (str): ZebraStream Connect API URL
              (defaults to public cloud service)

    Returns:
        TextIO | BinaryIO: File-like object for the stream.

        - Text modes ('r', 'rt', 'w', 'wt'): Returns TextIOWrapper
        - Binary modes ('rb', 'wb'): Returns Reader or Writer
        - With random_read=True: Returns context manager (must use 'with' statement)

    Examples:
        Sequential text read::

            with open(mode='r', stream_path='/logs') as f:
                for line in f:
                    print(line)

        Real-time log streaming with timer-based auto-flush::

            with open(mode='w', stream_path='/logs', auto_flush_delay=5) as f:
                for log_line in generate_logs():
                    f.write(log_line)  # Auto-flushed within 5 seconds

        Instant flush for low-latency (alerts, metrics)::

            with open(mode='w', stream_path='/alerts', auto_flush_delay=0) as f:
                f.write('CRITICAL: system down\\n')  # Flushed immediately

        Binary write with manual flush::

            with open(mode='wb', stream_path='/data') as f:
                f.write(b'urgent data')
                f.flush()  # Send immediately

        Random access for pandas (requires 'with' statement):

            with open(mode='rb', random_read=True, stream_path='/data.parquet') as f:
                df = pd.read_parquet(f)

    Note:
        - Data may be buffered internally. Call flush() for immediate transmission.
        - random_read=True loads the entire stream into a temporary file.
        - random_read=True REQUIRES 'with' statement (returns context manager).
        - All I/O operations block until complete.

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
            text_wrapper = io.TextIOWrapper(binary_reader, encoding=encoding)
            text_wrapper.mode = "r"  # type: ignore[misc]  # TextIOWrapper doesn't officially support mode assignment
            return text_wrapper
        if mode == "rb":
            # Binary read mode
            binary_reader = Reader(**kwargs)
            if random_read:
                return _create_buffered_reader(binary_reader, None, download_chunk_size)
            return binary_reader
        if mode in ("w", "wt"):
            # Text write mode
            binary_writer = Writer(**kwargs)
            text_wrapper = io.TextIOWrapper(binary_writer, encoding=encoding)
            text_wrapper.mode = "w"  # type: ignore[misc]  # TextIOWrapper doesn't officially support mode assignment
            return text_wrapper
        if mode == "wb":
            # Binary write mode
            return Writer(**kwargs)
        logger.error(f"Unsupported mode: {mode!r}")
        raise ValueError(f"Unsupported mode: {mode!r}. Supported: 'r', 'rt', 'rb', 'w', 'wt', 'wb'.")
    except ConnectionTimeoutError as e:
        raise TimeoutError(f"open failed: {e.message}") from e
    except (ConnectionFailedError, AuthenticationError) as e:
        raise OSError(f"open failed: {e.message}") from e


class Writer(io.BufferedIOBase):
    """
    Synchronous writer for ZebraStream data streams.

    Provides a file-like interface for writing to ZebraStream endpoints. Data may be
    buffered internally for efficiency. Use flush() for immediate transmission, or
    configure auto_flush_delay for automatic flushing (0=instant, 1+=timer-based).

    Examples:
        Basic usage with manual flush::

            writer = Writer(stream_path='/data', access_token='token')
            writer.write(b'data')
            writer.flush()  # Send immediately
            writer.close()

        Instant flush for low-latency::

            writer = Writer(stream_path='/alerts', access_token='token', auto_flush_delay=0)
            writer.write(b'CRITICAL\\n')  # Flushed immediately on every write

        Real-time log streaming with timer-based auto-flush::

            writer = Writer(stream_path='/logs', access_token='token', auto_flush_delay=5)
            for log_line in logs:
                writer.write(log_line.encode())  # Auto-flushed within 5 seconds
            writer.close()
    """

    # Instance-level type annotation
    _async_manager: _AsyncInstanceManager | None

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Writer for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncWriter (e.g., stream_path, access_token, content_type, connect_timeout, write_buffer_size, transfer_buffer_multiplier).
        """
        super().__init__()
        self._async_manager = _AsyncInstanceManager(lambda: AsyncWriter(**kwargs))

    def write(self, data: bytes | bytearray | memoryview) -> int:  # type: ignore[override]  # ReadableBuffer in typeshed is broader
        """
        Write bytes. Data may be buffered - use flush() for immediate transmission.

        Raises:
            TypeError: If data is not bytes-like.
            ValueError: If file is closed.
            OSError: If stream not started or generic I/O failure.
            BrokenPipeError: If peer disconnected during write.
            TimeoutError: If write operation timed out.
        """
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"a bytes-like object is required, not '{type(data).__name__}'")

        # Ensure bytes for the async layer
        raw = bytes(data) if not isinstance(data, bytes) else data

        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")

        logger.debug(f"Writing {len(data)} bytes")
        try:
            self._async_manager.portal.call(
                callable=self._async_manager.instance.write,
                cancellable=True,
                data=raw,
            )
        except StreamClosedError as e:
            raise ValueError("I/O operation on closed file") from e
        except NotStartedError as e:
            raise OSError("write failed: stream not started") from e
        except PeerDisconnectedError as e:
            raise BrokenPipeError("write failed: peer disconnected") from e
        except UploadError as e:
            raise OSError(f"write failed: {e.message}") from e
        except ConnectionTimeoutError as e:
            raise TimeoutError(f"write failed: {e.message}") from e
        except (ConnectionFailedError, AuthenticationError, ProtocolError) as e:
            raise OSError(f"write failed: {e.message}") from e

        return len(data)

    def readable(self) -> bool:
        """Return whether the stream supports reading.

        Returns:
            bool: Always False for Writer instances (write-only).
        """
        return False  # General capability - never changes

    def writable(self) -> bool:
        """Return whether the stream supports writing.

        Returns:
            bool: Always True for Writer instances.
        """
        return True  # General capability - never changes

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
                callable=self._async_manager.instance.flush, cancellable=True, wait_upload=True
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
                # Serious errors during cleanup should propagate.
                # Log at DEBUG only — the raised OSError carries the message to the caller,
                # so logging at ERROR here would duplicate whatever the caller logs.
                logger.debug(f"Error during close: {e.message}")
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

    def _abort(self) -> None:
        """
        Abort the writer without sending EOF.

        Cancels the in-flight upload so the relay sees an incomplete stream
        rather than a clean finish.  Called by :meth:`__exit__` when an
        exception is propagating.
        """
        if self._async_manager is not None:
            try:
                self._async_manager.abort()
            except Exception:
                logger.warning("Error during writer abort", exc_info=True)
            finally:
                self._async_manager = None

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the context manager.

        Calls :meth:`_abort` if an exception is propagating so the consumer
        receives an incomplete upload (error) instead of a clean EOF that
        would falsely indicate success.  Otherwise falls through to
        :meth:`close` for a clean finish.
        """
        if exc_type is not None:
            self._abort()
        else:
            self.close()

    @property
    def closed(self) -> bool:
        """Return True if the writer is closed."""
        return self._async_manager is None

    @property
    def mode(self) -> str:
        """File open mode (file-like compatibility).

        Reports underlying access mode ('rb', 'wb', etc.).
        Used by TextIOWrapper delegation and introspection code.
        Matches io.FileIO/BufferedReader semantics.
        """
        return "wb"

    @property
    def name(self) -> str:
        """Return the stream path identifier.

        Synthetic name for virtual streams ('<virtual>', etc.).
        Used by repr() and TextIOWrapper delegation.
        Matches standard file object behavior.
        """
        if self._async_manager is None or self._async_manager.instance is None:
            return "<zebrastream-closed-writer>"
        return self._async_manager.instance.stream_path


class Reader(io.BufferedIOBase):
    """Synchronous reader for ZebraStream data streams."""

    # Instance-level type annotation
    _async_manager: _AsyncInstanceManager | None

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Reader for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncReader (e.g., stream_path, access_token, content_type, connect_timeout).
        """
        super().__init__()
        self._async_manager = _AsyncInstanceManager(lambda: AsyncReader(**kwargs))

    def read(self, size: int | None = -1) -> bytes:
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
        if size == 0:
            return b""

        try:
            if size is None or size < 0:
                logger.debug("Reading all bytes")
                return self._async_manager.portal.call(
                    callable=self._async_manager.instance.read_all,
                    cancellable=True,
                )
            logger.debug(f"Reading up to {size} bytes")
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
        """Return whether the stream supports reading.

        Returns:
            bool: Always True for Reader instances.
        """
        return True  # General capability - never changes

    def writable(self) -> bool:
        """Return whether the stream supports writing.

        Returns:
            bool: Always False for Reader instances (read-only).
        """
        return False  # General capability - never changes

    def read1(self, size: int = -1) -> bytes:
        """Read up to size bytes with at most one underlying read operation.

        Unlike read(), this method performs at most one call to the underlying
        stream and may return fewer bytes than requested. This is useful for:
        - Reducing latency when partial data is acceptable
        - Performance optimization by avoiding small read loops
        - Implementing custom buffering strategies

        Note: This operation will block waiting for data from the network.
        Use read() with size parameter for guaranteed byte counts.

        Args:
            size: Maximum number of bytes to read. If negative, reads one
                  chunk of DEFAULT_BUFFER_SIZE (typically 8192 bytes).

        Returns:
            bytes: Data read from a single network operation (may be less than requested,
                   empty if stream ended).

        Raises:
            ValueError: If file is closed.
            OSError: If stream not started or generic I/O failure.
            BrokenPipeError: If peer disconnected during read.
            TimeoutError: If read operation timed out.
        """
        if self._async_manager is None:
            raise ValueError("I/O operation on closed file")
        if size == 0:
            return b""
        if size < 0:
            size = io.DEFAULT_BUFFER_SIZE
        try:
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

    def flush(self) -> None:
        """Flush write buffers (no-op for readers).

        This method exists for file-like API compatibility but has no
        effect on read-only streams. Safe to call.
        """
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
                # Serious errors during cleanup should propagate.
                # Log at DEBUG only — the raised OSError carries the message to the caller,
                # so logging at ERROR here would duplicate whatever the caller logs.
                logger.debug(f"Error during close: {e.message}")
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

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the context manager.

        Calls :meth:`_abort` if an exception is propagating so cleanup is fast
        and the original exception (:class:`KeyboardInterrupt`, etc.) reaches
        the caller intact.  On a clean exit, calls :meth:`close` for normal
        protocol validation.
        """
        if exc_type is not None:
            self._abort()
        else:
            self.close()

    def _abort(self) -> None:
        """
        Abort the reader without protocol validation.

        Cancels the in-flight download task immediately.  Called by
        :meth:`__exit__` when an exception is already propagating.
        """
        if self._async_manager is not None:
            try:
                self._async_manager.abort()
            except Exception:
                logger.warning("Error during reader abort", exc_info=True)
            finally:
                self._async_manager = None

    @property
    def closed(self) -> bool:
        """Return True if the reader is closed."""
        return self._async_manager is None

    @property
    def mode(self) -> str:
        """File open mode (file-like compatibility).

        Reports underlying access mode ('rb', 'wb', etc.).
        Used by TextIOWrapper delegation and introspection code.
        Matches io.FileIO/BufferedReader semantics.
        """
        return "rb"

    @property
    def name(self) -> str:
        """Return the stream path identifier.

        Synthetic name for virtual streams ('<virtual>', etc.).
        Used by repr() and TextIOWrapper delegation.
        Matches standard file object behavior.
        """
        if self._async_manager is None or self._async_manager.instance is None:
            return "<zebrastream-closed-reader>"
        return self._async_manager.instance.stream_path
