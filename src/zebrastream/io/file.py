# SPDX-License-Identifier: MIT
"""
Synchronous file-like wrappers for ZebraStream I/O.
This module provides synchronous `Reader` and `Writer` classes that wrap the asynchronous
ZebraStream protocol implementations, allowing seamless integration with code expecting
standard file-like interfaces. The wrappers use AnyIO's blocking portal to bridge between
sync and async code, supporting context management and typical file operations.
"""

import atexit
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, overload

import anyio

from ._core import AsyncReader, AsyncWriter

logger = logging.getLogger(__name__)
T = TypeVar('T')

class _SyncWrapperBase:
    """Base class for synchronous ZebraStream wrappers."""
    
    _instances = set()
    _instances_lock = threading.Lock()
    
    _async_instance: AsyncReader | AsyncWriter
    _blocking_portal: Any  # FIX: AnyIO type
    _blocking_portal_cm: Any  # FIX: AnyIO type
    _is_open: bool

    def __init__(self, async_factory: Callable[[], AsyncReader | AsyncWriter]) -> None:
        """
        Initialize a synchronous ZebraStream wrapper.
        
        Args:
            async_factory: Function that creates the async instance
        """
        logger.debug(f"Initializing sync {self.__class__.__name__}")
        self._async_factory = async_factory
        self._is_open = False  # Set early for __del__ safety
        
        # Register for cleanup
        with self._instances_lock:
            self._instances.add(self)
        
        try:
            self._start_blocking_portal()
            self._create_async_instance()
            self._is_open = True
        except:
            # Clean up any partial initialization
            self._cleanup_on_error()
            raise

    def _start_blocking_portal(self) -> None:
        """Start the anyio blocking portal."""
        self._blocking_portal = anyio.from_thread.start_blocking_portal("asyncio")
        self._blocking_portal_cm = self._blocking_portal.__enter__()

    def _stop_blocking_portal(self) -> None:
        """Stop the anyio blocking portal."""
        self._blocking_portal.__exit__(None, None, None)
        del self._blocking_portal_cm
        del self._blocking_portal

    @overload  
    def _call_async(self, callable: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T: ...
    
    @overload
    def _call_async(self, callable: Callable[..., T], *args: Any, **kwargs: Any) -> T: ...

    def _call_async(self, callable: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run a callable in the blocking portal."""
        return self._blocking_portal_cm.call(callable, *args, **kwargs)

    def _create_async_instance(self) -> None:
        """Create an async instance."""
        self._async_instance = self._call_async(self._async_factory)
        self._call_async(self._async_instance.start)

    def _destroy_async_instance(self) -> None:
        """Destroy the async instance."""
        self._call_async(self._async_instance.stop)
        del self._async_instance

    def _cleanup_on_error(self) -> None:
        """Clean up any partial initialization on error."""
        errors = []
        
        # Clean up async instance if it exists
        if hasattr(self, "_async_instance"):
            try:
                self._destroy_async_instance()
            except Exception as e:
                errors.append(e)
        
        # Clean up portal if it exists
        if hasattr(self, "_blocking_portal"):
            try:
                self._stop_blocking_portal()
            except Exception as e:
                errors.append(e)
        
        # Log errors but don't raise (we're already in error handling)
        for error in errors:
            logger.exception("Error during cleanup: %s", error)

    def close(self) -> None:
        """Close the stream and release all resources."""
        if not self._is_open:
            return  # Already closed, no-op
            
        logger.debug(f"Closing sync {self.__class__.__name__}")
        errors = []
        
        # Clean up in reverse order of creation
        try:
            self._destroy_async_instance()
        except Exception as e:
            errors.append(e)
            logger.exception("Error stopping async instance")
        
        try:
            self._stop_blocking_portal()
        except Exception as e:
            errors.append(e)
            logger.exception("Error stopping blocking portal")
        
        self._is_open = False
        
        # Unregister from cleanup
        with self._instances_lock:
            self._instances.discard(self)
        
        # Re-raise first error if any occurred
        if errors:
            raise errors[0]

    @property
    def closed(self) -> bool:
        """
        Return True if the stream is closed.
        """
        return not self._is_open

    def __enter__(self) -> "_SyncWrapperBase":
        """
        Enter the runtime context related to this object.
        Returns:
            _SyncWrapperBase: self
        """
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the runtime context and close the stream.
        
        Ensures cleanup happens even if there was an exception in the context.
        """
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

    def __del__(self) -> None:
        """Ensure resources are cleaned up if object is garbage collected."""
        try:
            if getattr(self, "_is_open", False):
                logger.warning(f"Emergency cleanup of {self.__class__.__name__} in __del__")
                self.close()
        except Exception:
            # Can't raise in __del__, just log if possible
            try:
                logger.exception("Error during emergency cleanup in __del__")
            except:
                pass  # Even logging might fail during shutdown


@atexit.register
def _cleanup_all_instances():
    """Clean up any remaining instances at exit."""
    with _SyncWrapperBase._instances_lock:
        instances = list(_SyncWrapperBase._instances)
    
    for instance in instances:
        try:
            if instance._is_open:
                logger.info(f"Cleaning up {instance.__class__.__name__} during shutdown")
                instance.close()
        except Exception:
            logger.exception(f"Error cleaning up {instance.__class__.__name__} during shutdown")


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

class Writer(_SyncWrapperBase):
    """
    Synchronous writer for ZebraStream data streams.
    """
    
    _async_instance: AsyncWriter  # More specific type for better IDE support

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Writer for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncWriter (e.g., stream_path, access_token, content_type, connect_timeout).
        """
        super().__init__(lambda: AsyncWriter(**kwargs))

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
        self._call_async(self._async_instance.write, data)

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
        self._call_async(self._async_instance.flush)


class Reader(_SyncWrapperBase):
    """
    Synchronous reader for ZebraStream data streams.
    """
    
    _async_instance: AsyncReader  # More specific type for better IDE support

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a synchronous Reader for ZebraStream.

        Args:
            **kwargs: Arguments passed to the underlying AsyncReader (e.g., stream_path, access_token, content_type, connect_timeout).
        """
        super().__init__(lambda: AsyncReader(**kwargs))

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
        if size == 0:
            # Read zero bytes, return empty bytes
            return b""
        if size < 0:
            # Read until EOF
            return self._call_async(self._async_instance.read_all)
        return self._call_async(self._async_instance.read_variable_block, size)

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
