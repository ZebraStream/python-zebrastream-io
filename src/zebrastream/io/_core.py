# SPDX-License-Identifier: MIT
"""
Asynchronous ZebraStream I/O core implementation using aiohttp.
This module provides core asynchronous classes and functions for interacting with ZebraStream data streams
over HTTP using the ZebraStream Connect API.
"""

# TODO: an asyncio.StreamWriter-compatible wrapper class for AsyncZebraStreamWriter
# TODO: an anyio.ByteStream-compatible wrapper class for AsyncZebraStreamWriter

import asyncio
import logging
import typing
from enum import Enum

import aiohttp

from ._age_rt import AgeRTError
from ._byte_buffer import AsyncByteBuffer
from ._exceptions import (
    AlreadyStartedError,
    AuthenticationError,
    ConnectionFailedError,
    ConnectionTimeoutError,
    DecryptionError,
    DownloadError,
    EncryptionError,
    NotStartedError,
    PeerDisconnectedError,
    ProtocolError,
    StreamClosedError,
    UploadError,
)

logger = logging.getLogger(__name__)

# Default ZebraStream Connect API URL
DEFAULT_ZEBRASTREAM_CONNECT_API_URL = "https://connect.zebrastream.io/v0/"

# age encryption chunk size (64 KiB)
# The original age format uses fixed 64 KiB chunks for optimal encryption performance
AGE_CHUNK_SIZE = 64 * 1024  # 65536 bytes

# Minimum transfer buffer size (prevents deadlock when write_buffer_size=0)
# Set to AGE_CHUNK_SIZE to maintain efficient async operation
MIN_TRANSFER_BUFFER_SIZE = AGE_CHUNK_SIZE  # 65536 bytes


class _ErrorAction(Enum):
    """Actions to take when handling connection errors."""
    RAISE = 1        # Raise immediately (non-retryable error)
    WRAP_AUTH = 2    # Wrap as AuthenticationError and raise
    RETRY = 3        # Retry with backoff (transient error)


def _categorize_error(e: Exception, had_stable_connection: bool) -> _ErrorAction:
    """
    Categorize connection error and determine action to take.
    
    Args:
        e: The exception that was raised
        had_stable_connection: Whether we've had a stable connection before
        
    Returns:
        ErrorAction indicating how to handle the error
    """
    # Always raise these - non-retryable by nature
    if isinstance(e, (asyncio.TimeoutError, asyncio.CancelledError)):
        return _ErrorAction.RAISE
    
    # Authentication errors - wrap and raise (never retry)
    if isinstance(e, aiohttp.ClientResponseError) and e.status in (401, 403):
        return _ErrorAction.WRAP_AUTH
    
    # Client errors (4xx) - always raise (configuration problem)
    if isinstance(e, aiohttp.ClientResponseError) and 400 <= e.status < 500:
        return _ErrorAction.RAISE
    
    # Server errors (5xx) - retry only if we had a stable connection before
    if isinstance(e, aiohttp.ClientResponseError) and e.status >= 500:
        return _ErrorAction.RETRY if had_stable_connection else _ErrorAction.RAISE
    
    # Transient network errors - retry only if we had a stable connection before
    if isinstance(e, (aiohttp.ClientOSError, aiohttp.ServerConnectionError, aiohttp.ServerTimeoutError)):
        return _ErrorAction.RETRY if had_stable_connection else _ErrorAction.RAISE
    
    # Everything else (DNS, SSL, URL parsing, etc.) - always raise (configuration problem)
    return _ErrorAction.RAISE


async def _connect(
    stream_path: str,
    mode: str,
    access_token: str | None = None,
    connect_timeout: int | None = None,
    connect_api_url: str = DEFAULT_ZEBRASTREAM_CONNECT_API_URL,
) -> str:
    """
    Establish a connection to the ZebraStream Connect API and get the data stream URL.

    Args:
        stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
        mode (str): Connection mode ('await-reader' or 'await-writer').
        access_token (str, optional): Access token for authorization.
        connect_timeout (int, optional): Timeout in seconds for the connect operation.
        connect_api_url (str, optional): Base URL for the ZebraStream Connect API.
            Defaults to the public ZebraStream cloud service.

    Returns:
        str: The data stream URL for subsequent requests.

    Raises:
        ConnectionTimeoutError: If the overall operation exceeds the timeout.
        AuthenticationError: If authentication/authorization fails (401/403).
        ConnectionFailedError: If connection fails after retries.
    """
    if mode not in {"await-reader", "await-writer"}:
        raise ValueError("Invalid mode specified. Use 'await-reader' or 'await-writer'.")

    # Construct the full connect URL from the base URL and stream path
    connect_url = connect_api_url.rstrip("/") + stream_path

    headers = {}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    params = {"mode": mode}
    if connect_timeout is not None:
        # we set the server-side timeout to be slightly longer than the client-side timeout to ensure the client-side timeout triggers first
        params["timeout"] = str(
            connect_timeout + 1
        )

    # Always set explicit connection timeout for fast failure on DNS/network errors
    # Apply timeout per-request rather than per-session to avoid cleanup delays
    TCP_CONNECT_TIMEOUT = 10  # seconds - fail fast on DNS/connection errors
    
    # Build timeout for individual requests
    if connect_timeout is not None:
        request_timeout = aiohttp.ClientTimeout(
            total=connect_timeout,
            sock_connect=TCP_CONNECT_TIMEOUT,
        )
    else:
        # No overall timeout, but still enforce connection timeout for fail-fast behavior
        request_timeout = aiohttp.ClientTimeout(sock_connect=TCP_CONNECT_TIMEOUT)

    # Retry strategy constants
    MIN_STABLE_DURATION = 30  # seconds - connection must last this long to be considered stable
    MAX_FAILURE_DURATION = 1800  # seconds - 30 minutes cumulative backoff before giving up

    retry_count = 0
    cumulative_backoff = 0
    had_stable_connection = False
    last_error: Exception | None = None

    async def _connect_attempt_loop() -> str:
        nonlocal retry_count, cumulative_backoff, had_stable_connection, last_error
        # Create session without default timeout - apply timeout per-request instead
        async with aiohttp.ClientSession() as session:
            while True:
                request_start_time = asyncio.get_event_loop().time()
                
                try:
                    async with session.get(
                        connect_url, params=params, headers=headers, timeout=request_timeout
                    ) as resp:
                        resp.raise_for_status()
                        data_stream_url = (await resp.text()).strip()
                        
                        # Reset counters after stable connection
                        request_duration = asyncio.get_event_loop().time() - request_start_time
                        if request_duration >= MIN_STABLE_DURATION:
                            retry_count = 0
                            cumulative_backoff = 0
                            had_stable_connection = True
                            logger.debug("Stable connection established, reset retry counters")
                        
                        return data_stream_url
                        
                except Exception as e:
                    # Categorize the error and determine action
                    action = _categorize_error(e, had_stable_connection)
                    
                    match action:
                        case _ErrorAction.RAISE:
                            # Non-retryable error - log and raise immediately
                            if not had_stable_connection:
                                logger.debug(f"Initial connection attempt failed: {e}")
                            else:
                                logger.debug(f"Non-retryable connection error: {e}")
                            raise
                        
                        case _ErrorAction.WRAP_AUTH:
                            # Authentication error - wrap and raise
                            logger.debug(f"Authentication failed: {e}")
                            if isinstance(e, aiohttp.ClientResponseError):
                                raise AuthenticationError(
                                    status_code=e.status,
                                    stream_path=stream_path,
                                    connect_api_url=connect_api_url,
                                    original_error=e,
                                ) from e
                            raise  # Shouldn't happen, but handle gracefully
                        
                        case _ErrorAction.RETRY:
                            # Transient error - retry with linear backoff
                            retry_count += 1
                            backoff = retry_count  # Linear backoff: 1s, 2s, 3s, ...
                            cumulative_backoff += backoff
                            
                            if cumulative_backoff > MAX_FAILURE_DURATION:
                                logger.error(
                                    "Maximum failure duration exceeded (%d seconds), giving up after %d retries",
                                    MAX_FAILURE_DURATION,
                                    retry_count,
                                )
                                raise
                            
                            last_error = e
                            logger.warning(
                                "Connection attempt %d failed: %s, retrying in %ds (cumulative: %ds)",
                                retry_count,
                                e,
                                backoff,
                                cumulative_backoff,
                            )
                            await asyncio.sleep(backoff)

    try:
        if connect_timeout is None:
            return await _connect_attempt_loop()
        return await asyncio.wait_for(_connect_attempt_loop(), timeout=connect_timeout)
    except asyncio.TimeoutError as e:
        logger.debug("Connection attempt timed out after %s seconds", connect_timeout)
        raise ConnectionTimeoutError(
            timeout_seconds=connect_timeout or 0,
            stream_path=stream_path,
            connect_api_url=connect_api_url,
            original_error=e,
        ) from e
    except AuthenticationError:
        # Re-raise authentication errors as-is
        raise
    except asyncio.CancelledError:
        # Re-raise cancellation as-is
        raise
    except Exception as e:
        # Wrap any other exception as ConnectionFailedError
        logger.debug("Connection failed after %d attempts", retry_count)
        raise ConnectionFailedError(
            retries=retry_count,
            last_error=last_error or e,
            stream_path=stream_path,
            connect_api_url=connect_api_url,
        ) from e


class AsyncWriter:
    """
    An asynchronous writer for ZebraStream data streams using HTTP PUT and aiohttp.

    This class provides an async interface for sending data to a ZebraStream endpoint. It manages connection setup,
    buffering, and upload tasks, and supports use as an async context manager.

    Buffering Behavior:
        Data written via write() may be buffered internally for efficiency. This buffering can occur at multiple
        levels (text wrapper, internal queues, HTTP layer) and helps optimize network performance for bulk transfers.

        For applications requiring immediate data transmission (e.g., real-time logging, streaming), use one of:
        - auto_flush_delay=0 for instant flush on every write (lowest latency)
        - auto_flush_delay=N for time-based automatic flushing (N seconds)
        - Explicit flush() calls after write() for manual control

    Examples:
        # Instant flush for low-latency (e.g., alerts, metrics):
        async with AsyncWriter(stream_path="/alerts", access_token="token", auto_flush_delay=0) as writer:
            await writer.write(b"CRITICAL: system down\\n")  # Flushed immediately

        # Real-time log streaming with timer-based flush:
        async with AsyncWriter(stream_path="/logs", access_token="token", auto_flush_delay=5) as writer:
            for log_line in log_stream:
                await writer.write(log_line)  # Auto-flushed within 5 seconds

        # Manual flush for precise control:
        async with AsyncWriter(stream_path="/events", access_token="token") as writer:
            await writer.write(b"event data\\n")
            await writer.flush()  # Explicit flush when needed

        # Bulk transfer - let buffering optimize performance:
        async with AsyncWriter(stream_path="/data", access_token="token") as writer:
            for data in large_dataset:
                await writer.write(data)  # Buffered for efficiency
            # Implicit flush() on context exit
    """

    _CONNECT_MODE: str = "await-reader"
    _stream_path: str
    _access_token: str | None
    _content_type: str | None
    _connect_timeout: int | None
    _connect_api_url: str
    _write_buffer: bytearray
    write_buffer_size: int
    _transfer_buffer: AsyncByteBuffer
    _write_failed: bool
    _upload_task: asyncio.Task[None] | None
    _auto_flush_task: asyncio.Task[None] | None
    _data_stream_url: str | None
    is_started: bool
    _closed: bool
    _eof_sent: bool
    _passphrase: str | None
    _auto_flush_delay: int | None

    @property
    def stream_path(self) -> str:
        """Return the stream path identifier."""
        return self._stream_path

    def __init__(
        self,
        stream_path: str,
        access_token: str | None = None,
        content_type: str | None = None,
        connect_timeout: int | None = None,
        connect_api_url: str | None = None,
        passphrase: str | None = None,
        write_buffer_size: int = 65536,
        transfer_buffer_multiplier: int = 10,
        auto_flush_delay: int | None = None,
    ) -> None:
        """
        Initialize an asynchronous ZebraStream writer.

        Args:
            stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
            access_token (str, optional): Access token for authorization.
            content_type (str, optional): Content-Type for the HTTP request.
            connect_timeout (int, optional): Server-side timeout in seconds for the connect operation.
            connect_api_url (str, optional): Base URL for the ZebraStream Connect API.
                If None, uses the default public ZebraStream cloud service.
            passphrase (str, optional): Passphrase for symmetric encryption.
                If provided, all data written will be encrypted using age-rt.
                If None (default), data is transmitted unencrypted.
            write_buffer_size (int, optional): Size threshold for automatic buffer flushing in bytes.
                Data is accumulated until the buffer reaches this size, then automatically flushed.
                Due to write accumulation, the buffer may temporarily exceed this size by up to
                the size of a single write (typically much less than 2x in practice).
                Default is 65536 (64 KiB). Set to 0 to disable buffering (each write goes directly to transfer buffer).
            transfer_buffer_multiplier (int, optional): Multiplier for transfer buffer size.
                Transfer buffer size = write_buffer_size × transfer_buffer_multiplier.
                Controls backpressure and memory usage. When buffer is full, write operations will block.
                Must be >= 1. Default is 10 (640 KiB transfer buffer for 64 KiB write buffer).
                Total memory: ~write_buffer_size × (1 + transfer_buffer_multiplier).
                Note: A minimum transfer buffer size is enforced to ensure async operation works correctly.
            auto_flush_delay (int, optional): Automatic flush delay in seconds.
                When set, buffered data is flushed at most N seconds after the first write
                following a flush. Set to 0 for instant flush on every write (disables write buffering).
                Set to 1+ for timer-based flushing. Set to None (default) to disable time-based flushing.
                Note: auto_flush_delay=0 is the most efficient instant-flush mechanism.
        """
        self._stream_path = stream_path
        self._access_token = access_token
        self._content_type = content_type
        self._connect_timeout = connect_timeout
        self._connect_api_url = connect_api_url or DEFAULT_ZEBRASTREAM_CONNECT_API_URL
        self._passphrase = passphrase
        if transfer_buffer_multiplier < 1:
            raise ValueError(f"transfer_buffer_multiplier must be >= 1, got {transfer_buffer_multiplier}")
        if auto_flush_delay is not None and auto_flush_delay < 0:
            raise ValueError(
                f"auto_flush_delay must be >= 0 (seconds) or None to disable, got {auto_flush_delay}. "
                "Use 0 for instant flush on every write, 1+ for timer-based flushing."
            )
        
        # Configure instant flush mode: auto_flush_delay=0 disables write buffering
        # This leverages existing size-check logic without additional overhead
        if auto_flush_delay == 0:
            self.write_buffer_size = 0
            self._auto_flush_delay = None  # No timer needed for instant flush
        else:
            self.write_buffer_size = write_buffer_size
            self._auto_flush_delay = auto_flush_delay
        
        self._write_buffer = bytearray()
        
        # Ensure minimum transfer buffer size to prevent deadlock when write_buffer_size=0
        transfer_size = max(
            MIN_TRANSFER_BUFFER_SIZE,
            self.write_buffer_size * transfer_buffer_multiplier
        )
        self._transfer_buffer = AsyncByteBuffer(transfer_size)
        self._write_failed = False
        self.is_started = False
        self._closed = False
        self._eof_sent = False

        # Initialize attributes that are set later in start()
        self._upload_task: asyncio.Task[None] | None = None
        self._data_stream_url: str | None = None
        self._auto_flush_task: asyncio.Task[None] | None = None

    async def _start_connect(self) -> None:
        self._data_stream_url = await _connect(
            stream_path=self._stream_path,
            mode=self._CONNECT_MODE,
            access_token=self._access_token,
            connect_timeout=self._connect_timeout,
            connect_api_url=self._connect_api_url,
        )

    def _start_send(self) -> None:
        """Start the upload task to send data to the ZebraStream Data API."""
        if self._data_stream_url is None:
            raise NotStartedError(operation="start_send")
        if self._upload_task is not None:
            raise AlreadyStartedError(stream_path=self._stream_path)
        self._upload_task = asyncio.create_task(self._upload())

    async def start(self) -> None:
        """
        Start the writer and wait for a peer to connect.

        Raises:
            AlreadyStartedError: If the writer is already started.
            ConnectionTimeoutError: If connection times out.
            AuthenticationError: If authentication fails.
            ConnectionFailedError: If connection fails after retries.
        """
        if self.is_started:
            raise AlreadyStartedError(stream_path=self._stream_path)
        logger.debug("Starting AsyncWriter")
        await self._start_connect()
        self._start_send()
        self.is_started = True
        logger.debug("AsyncWriter started successfully")

    async def stop(self) -> None:
        """
        Stop the writer and wait for the upload task to finish.

        Flushes any remaining buffered data, sends EOF if not already sent, then waits for
        the upload task to complete. Any errors during stop (including PeerDisconnectedError)
        will be raised to indicate that the stream may not have been properly closed and data
        may not have been delivered.

        This method is idempotent and safe to call multiple times or even if
        start() was never called or didn't complete successfully.
        """
        if self._closed:
            logger.debug("AsyncWriter.stop() called but already closed")
            return

        if not self.is_started:
            logger.debug("AsyncWriter.stop() called but writer not started")

        logger.debug("Stopping AsyncWriter")

        # Send EOF if not already sent
        if not self._eof_sent:
            await self._cancel_auto_flush_timer()  # Cancel timer before flushing
            await self._flush()  # Flush any remaining buffered data
            self._transfer_buffer.close()  # Signal EOF to upload task
            self._eof_sent = True

        # Wait for upload task to complete
        # Note: We do NOT suppress PeerDisconnectedError or other errors here.
        # If an error occurs during stop, it indicates the stream was not properly
        # closed and the receiver may not have gotten all data.
        if self._upload_task is not None:
            try:
                await self._upload_task
            except asyncio.CancelledError:
                pass  # Expected during forced shutdown
            except Exception as e:
                if not self._write_failed:
                    logger.debug("Upload task failed: %s", e)
                    raise e
        self.is_started = False
        self._closed = True
        logger.debug("AsyncWriter stopped")

    async def abort(self) -> None:
        """
        Abort the writer immediately without sending EOF.

        Cancels the upload task in-flight so the HTTP PUT body terminates
        without a final chunk. The relay will see an incomplete upload and
        should signal an error to any waiting consumer.

        Call this instead of :meth:`stop` when the producer has failed and
        sending a clean EOF would be misleading to the consumer.

        This method is idempotent and safe to call multiple times or even if
        start() was never called.
        """
        if self._closed:
            logger.debug("AsyncWriter.abort() called but already closed")
            return

        logger.debug("Aborting AsyncWriter (no EOF will be sent)")

        await self._cancel_auto_flush_timer()  # Cancel timer before aborting

        if self._upload_task is not None:
            self._upload_task.cancel()
            try:
                await self._upload_task
            except (asyncio.CancelledError, Exception):
                pass

        self.is_started = False
        self._closed = True
        logger.debug("AsyncWriter aborted")

    async def write(self, data: bytes | None) -> None:
        """
        Write bytes to the ZebraStream data stream asynchronously.

        Data is accumulated in an internal buffer for efficiency. When the buffer reaches
        write_buffer_size bytes, it is automatically flushed to the transfer queue. For immediate
        transmission, call flush() after write().

        Args:
            data (bytes | None): The data to write. Pass None to signal EOF (end of stream).
        Raises:
            StreamClosedError: If the writer is closed.
            NotStartedError: If the writer is not started.
            UploadError: If the upload task failed.
        """
        if self._closed:
            raise StreamClosedError(operation="write", stream_path=self._stream_path)
        # Check if the upload task has failed and propagate the exception
        if self._upload_task and self._upload_task.done():
            exc = self._upload_task.exception()
            if exc is not None:
                self._write_failed = True
                raise exc
        if not self.is_started:
            raise NotStartedError(operation="write", stream_path=self._stream_path)

        # Handle EOF signal: flush buffer, send EOF, then mark flag
        if data is None:
            await self._cancel_auto_flush_timer()  # Cancel timer before EOF
            await self._flush()
            self._transfer_buffer.close()
            self._eof_sent = True
            return

        # Accumulate data in buffer
        self._write_buffer.extend(data)

        # Start auto-flush timer on first write after flush (if enabled)
        self._start_auto_flush_timer()

        # Auto-flush when buffer reaches threshold
        if len(self._write_buffer) >= self.write_buffer_size:
            await self._flush()

    async def flush(self, wait_upload: bool = True) -> None:
        """
        Flush write buffer to transfer queue.

        Moves accumulated data from the internal buffer to the transfer queue.
        By default, waits for the upload task to consume the queued data (backpressure).

        Args:
            wait_upload (bool, optional): If True (default), waits for the upload task
                to consume all queued data. If False, returns immediately after queueing.

        Use the default (True) when you need confirmation that data has been handed off
        for transmission. Use False for fire-and-forget buffering with overlapped I/O.

        Examples::

            # Backpressure control (default):
            await writer.write(data)
            await writer.flush()  # Waits for upload task

            # Fire-and-forget:
            await writer.write(log_line)
            await writer.flush(wait_upload=False)  # Return immediately

            # Overlapped I/O:
            await writer.write(data)
            await writer.flush(wait_upload=False)  # Start upload
            result = await compute()  # Do other work
            await writer.flush()  # Sync point
        """
        await self._flush()
        if wait_upload:
            await self._drain()

    async def _flush(self) -> None:
        """Internal: Move write buffer to transfer buffer."""
        await self._cancel_auto_flush_timer()  # Cancel timer when flushing
        if not self._write_buffer:
            return
        try:
            await self._transfer_buffer.write(bytes(self._write_buffer))
        except ValueError:
            # Transfer buffer was force-closed (upload task died mid-write).
            # Re-raise the real upload error if available, otherwise re-raise ValueError.
            if self._upload_task is not None and self._upload_task.done() and not self._upload_task.cancelled():
                exc = self._upload_task.exception()
                if exc is not None:
                    self._write_failed = True
                    raise exc
            raise
        self._write_buffer.clear()

    async def _drain(self) -> None:
        """Internal: Wait for transfer buffer to be consumed by upload task."""
        await self._transfer_buffer.drain()

    def _start_auto_flush_timer(self) -> None:
        """Start auto-flush timer if enabled and not already running."""
        if self._auto_flush_delay is None:
            return  # Timer disabled
        if self._auto_flush_task is not None:
            return  # Timer already running
        self._auto_flush_task = asyncio.create_task(self._auto_flush_timer_loop())

    async def _cancel_auto_flush_timer(self) -> None:
        """Cancel auto-flush timer if running."""
        if self._auto_flush_task is None:
            return  # No timer running
        self._auto_flush_task.cancel()
        try:
            await self._auto_flush_task
        except asyncio.CancelledError:
            pass  # Expected when cancelling
        self._auto_flush_task = None

    async def _auto_flush_timer_loop(self) -> None:
        """Timer loop that flushes buffer after delay."""
        try:
            await asyncio.sleep(self._auto_flush_delay)
            await self._flush()
        finally:
            # Clear task reference when timer completes or is cancelled
            self._auto_flush_task = None

    async def _upload(self) -> None:
        async def plaintext_chunks() -> typing.AsyncGenerator[bytes, None]:
            """Yield plaintext chunks from application writes."""
            while True:
                # Limit chunk size to age's standard 64 KiB for optimal encryption
                chunk = await self._transfer_buffer.consume_available(AGE_CHUNK_SIZE)
                if not chunk:  # EOF (buffer closed and empty)
                    break
                yield chunk

        # Wrap with encryption if passphrase provided
        if self._passphrase:
            try:
                from ._age_rt import aiter_encode

                data_chunks = aiter_encode(plaintext_chunks(), self._passphrase)
            except AgeRTError as e:
                raise EncryptionError(
                    message=f"Failed to initialize encryption: {e}",
                    stream_path=self._stream_path,
                    original_error=e,
                ) from e
        else:
            data_chunks = plaintext_chunks()

        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        if self._content_type:
            headers["Content-Type"] = self._content_type

        # Prepend empty byte for aiohttp
        async def wire_data():
            yield b""  # keeps aiohttp waiting for data instead of closing connection immediately
            async for chunk in data_chunks:
                yield chunk

        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(self._data_stream_url, headers=headers, data=wire_data()) as resp:
                    line_str = ""
                    async for line in resp.content:
                        # TODO: validate entire control message sequence
                        line_str = line.decode(errors="replace").rstrip()
                        logger.debug("Server response: %s", line_str)
                        if line_str == "[ERROR:RECEIVER_PREMATURE_DISCONNECT]":
                            logger.warning("Receiver disconnected prematurely")
                            raise PeerDisconnectedError(
                                peer_role="reader",
                                phase="upload",
                                stream_path=self._stream_path,
                            )
                    resp.raise_for_status()

                if line_str != "[STATE:TRANSFER_SUCCESSFUL]":
                    raise ProtocolError(
                        message="Server did not confirm successful transfer",
                        phase="upload",
                        expected="[STATE:TRANSFER_SUCCESSFUL]",
                        actual=line_str,
                        stream_path=self._stream_path,
                    )
        except (PeerDisconnectedError, ProtocolError):
            # Re-raise our own exceptions
            raise
        except AgeRTError as e:
            # Wrap age-rt encryption errors
            raise EncryptionError(
                message=f"Encryption failed during upload: {e}",
                stream_path=self._stream_path,
                original_error=e,
            ) from e
        except aiohttp.ClientError as e:
            # Wrap aiohttp exceptions
            raise UploadError(
                message=f"Upload failed: {e}",
                stream_path=self._stream_path,
                original_error=e,
            ) from e
        except Exception as e:
            # Wrap any other exception
            raise UploadError(
                message=f"Upload failed unexpectedly: {e}",
                stream_path=self._stream_path,
                original_error=e,
            ) from e
        finally:
            # Closing the transfer buffer unblocks any concurrent _flush() / _drain()
            # calls waiting for buffer space or drain completion. This is idempotent.
            self._transfer_buffer.close()

    async def __aenter__(self) -> "AsyncWriter":
        """
        Enter the async context manager, starting the writer.

        Returns:
            AsyncWriter: self
        """
        await self.start()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the async context manager.

        Calls :meth:`abort` if an exception is in flight (so the consumer
        receives an incomplete upload rather than a clean EOF), otherwise calls
        :meth:`stop` for a clean finish.
        """
        if exc_type is not None:
            await self.abort()
        else:
            await self.stop()


class AsyncReader:
    """
    An asynchronous reader for ZebraStream data streams using HTTP GET and aiohttp.

    This class provides an async interface for receiving data from a ZebraStream endpoint. It manages connection setup,
    buffering, and download tasks, and supports use as an async context manager.
    """

    _CONNECT_MODE: str = "await-writer"

    _stream_path: str
    _access_token: str | None
    _content_type: str | None
    _connect_timeout: int | None
    _connect_api_url: str
    _block_size: int
    _buffer: bytearray
    _read_event: asyncio.Event
    _download_task: asyncio.Task[None] | None
    _data_stream_url: str | None
    is_started: bool
    _eof: bool
    _eof_consumed: bool
    _exception: Exception | None
    _closed: bool
    _passphrase: str | None

    @property
    def stream_path(self) -> str:
        """Return the stream path identifier."""
        return self._stream_path

    def __init__(
        self,
        stream_path: str,
        access_token: str | None = None,
        content_type: str | None = None,
        connect_timeout: int | None = None,
        block_size: int = 4096,
        connect_api_url: str | None = None,
        passphrase: str | None = None,
    ) -> None:
        """
        Initialize an asynchronous ZebraStream reader.

        Args:
            stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
            access_token (str, optional): Access token for authorization.
            content_type (str, optional): Content-Type for the HTTP request.
            connect_timeout (int, optional): Timeout in seconds for the connect operation.
            block_size (int, optional): Size of data blocks to read from the HTTP response stream.
                Smaller values (e.g., 1024) provide lower latency for real-time data like log lines.
                Larger values (e.g., 32768) provide better throughput for bulk data transfers.
                Default is 4096 bytes, which balances latency and throughput for most use cases.
            connect_api_url (str, optional): Base URL for the ZebraStream Connect API.
                If None, uses the default public ZebraStream cloud service.
            passphrase (str, optional): Passphrase for symmetric encryption.
                If provided, all data read will be decrypted using age-rt.
                If None (default), data is read unencrypted.
        """
        self._stream_path = stream_path
        self._access_token = access_token
        self._content_type = content_type
        self._connect_timeout = connect_timeout
        self._connect_api_url = connect_api_url or DEFAULT_ZEBRASTREAM_CONNECT_API_URL
        self._passphrase = passphrase
        self._block_size = block_size
        self._buffer = bytearray()
        self._read_event = asyncio.Event()
        self.is_started = False
        self._eof = False
        self._eof_consumed = False
        self._exception = None
        self._closed = False

        # Initialize attributes that are set later in start()
        self._download_task: asyncio.Task[None] | None = None
        self._data_stream_url: str | None = None

    async def _start_connect(self) -> None:
        self._data_stream_url = await _connect(
            stream_path=self._stream_path,
            mode=self._CONNECT_MODE,
            access_token=self._access_token,
            connect_timeout=self._connect_timeout,
            connect_api_url=self._connect_api_url,
        )

    def _start_download(self) -> None:
        if self._data_stream_url is None:
            raise NotStartedError(operation="start_download")
        if self._download_task is not None:
            raise AlreadyStartedError(stream_path=self._stream_path)
        self._download_task = asyncio.create_task(self._download())

    async def start(self) -> None:
        """
        Start the reader and wait for a peer to connect.

        Raises:
            AlreadyStartedError: If the reader is already started.
            ConnectionTimeoutError: If connection times out.
            AuthenticationError: If authentication fails.
            ConnectionFailedError: If connection fails after retries.
        """
        if self.is_started:
            raise AlreadyStartedError(stream_path=self._stream_path)
        logger.debug("Starting AsyncReader")
        await self._start_connect()
        self._start_download()
        self.is_started = True
        logger.debug("AsyncReader started successfully")

    async def stop(self) -> None:
        """
        Stop the reader and release resources.

        This method always cancels the download task and closes the connection.
        After cleanup, it validates that the protocol was followed correctly:
        - EOF must have been received from peer
        - EOF must have been consumed by application

        If protocol validation fails, ProtocolError is raised AFTER cleanup,
        ensuring no resource leaks even when protocol is violated.

        Note: aiohttp closes the HTTP connection immediately after consuming
        the response body, so canceling the download task doesn't affect
        connection state (connection is already closed). The validation ensures
        application followed the protocol correctly before calling stop().

        Raises:
            ProtocolError: If EOF not received or not consumed by application.
        """
        if self._closed:
            logger.debug("AsyncReader.stop() called but already closed")
            return

        if not self.is_started:
            logger.debug("AsyncReader.stop() called but reader not started")

        # Capture state before cancellation for validation
        eof_received = self._eof
        eof_consumed = self._eof_consumed

        logger.debug("Stopping AsyncReader (eof_received=%s, eof_consumed=%s)", eof_received, eof_consumed)

        # Always cancel download task (cleanup happens regardless of validation)
        if self._download_task is not None:
            self._download_task.cancel()
            try:
                await self._download_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning("Download task raised exception during cancellation: %s", e)

        self.is_started = False
        self._closed = True

        # Validate protocol after cleanup
        # If validation fails, resources are still cleaned up but error is raised
        if not eof_received:
            raise ProtocolError(
                message="EOF not received",
                phase="stop",
                stream_path=self._stream_path,
            )

        if not eof_consumed:
            raise ProtocolError(
                message="EOF not consumed",
                phase="stop",
                stream_path=self._stream_path,
            )

        logger.debug("AsyncReader stopped")

    async def read_fixed_block(self, n: int) -> bytes:
        """
        Read a fixed block of n bytes from the ZebraStream data stream asynchronously.

        This method attempts to read exactly n bytes. It will return fewer than n bytes
        (or zero bytes) only if the stream reaches EOF before n bytes are available.

        Args:
            n (int): Number of bytes to read. Must be > 0.
        Returns:
            bytes: The data read from the stream. Returns exactly n bytes unless EOF is
                   reached, in which case it returns all remaining bytes (possibly fewer
                   than n, or empty bytes if no data remains).
        Raises:
            StreamClosedError: If the reader is closed.
            ValueError: If n <= 0.
            DownloadError: If an error occurs during reading.
        """
        if self._closed:
            raise StreamClosedError(operation="read", stream_path=self._stream_path)
        if n <= 0:
            raise ValueError("n must be positive")

        while len(self._buffer) < n and not self._eof:
            if self._exception:
                raise self._exception
            await self._read_event.wait()
            self._read_event.clear()
        if not self._buffer and self._eof:
            return b""
        data = bytes(memoryview(self._buffer)[:n])
        del self._buffer[:n]
        return data

    async def read_variable_block(self, n: int) -> bytes:
        """
        Read up to n bytes of available data from the ZebraStream data stream asynchronously.

        This method returns immediately with whatever data is available, up to n bytes.
        When this method returns empty bytes (b''), it indicates EOF has been reached.
        This signals to the download task that the application has acknowledged the
        end of stream, allowing the connection to close cleanly.

        Args:
            n (int): Maximum number of bytes to read. Must be > 0.
        Returns:
            bytes: The data read from the stream. Returns up to n bytes of available data,
               or empty bytes if EOF is reached.
        Raises:
            StreamClosedError: If the reader is closed.
            ValueError: If n <= 0.
            DownloadError: If an error occurs during reading.
        """
        if self._closed:
            raise StreamClosedError(operation="read", stream_path=self._stream_path)
        if n <= 0:
            raise ValueError("n must be positive")

        # Wait until we have some data or reach EOF
        while not self._buffer and not self._eof:
            if self._exception:
                raise self._exception
            await self._read_event.wait()
            self._read_event.clear()

        # Check for exception one more time after waiting
        if self._exception:
            raise self._exception

        # If EOF reached with no data, mark as consumed
        if not self._buffer and self._eof:
            self._eof_consumed = True
            return b""

        data = bytes(memoryview(self._buffer)[:n])
        del self._buffer[:n]
        return data

    async def read_all(self) -> bytes:
        """
        Read all remaining data until EOF.

        This method consumes all data including EOF, marking the stream
        as fully consumed.

        Returns:
            bytes: All remaining data in the stream.
        Raises:
            Exception: If an error occurs during reading.
        """
        # Wait until EOF is reached and all data is buffered
        while not self._eof:
            if self._exception:
                raise self._exception
            await self._read_event.wait()
            self._read_event.clear()

        # Mark EOF as consumed
        self._eof_consumed = True

        # Return all buffered data
        if not self._buffer:
            return b""

        data = bytes(self._buffer)
        self._buffer.clear()
        return data

    async def _download(self) -> None:
        """
        Download data from the stream.

        Consumes the HTTP response body and marks EOF when complete.
        Note: aiohttp closes the connection immediately after consuming
        the response body, so there is no way to keep the connection open
        until application consumes EOF. The _eof_consumed flag provides
        deterministic error detection in stop() but doesn't affect connection
        lifecycle.
        """
        # TODO: match content type, or raise exception
        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self._data_stream_url, headers=headers) as resp:
                    resp.raise_for_status()

                    # Wrap iterator to terminate on empty chunks
                    async def data_chunks():
                        async for chunk in resp.content.iter_chunked(self._block_size):
                            if not chunk:
                                break
                            yield chunk

                    # Wrap with decryption if passphrase provided
                    if self._passphrase:
                        try:
                            from ._age_rt import aiter_decode_chunks

                            plaintext_chunks = aiter_decode_chunks(data_chunks(), self._passphrase)
                        except AgeRTError as e:
                            raise DecryptionError(
                                message=f"Failed to initialize decryption: {e}",
                                stream_path=self._stream_path,
                                original_error=e,
                            ) from e
                    else:
                        plaintext_chunks = data_chunks()

                    # Consume all plaintext chunks.
                    # On cancellation, abort the transport before the CancelledError
                    # propagates through resp.__aexit__ / session.__aexit__.
                    # Without this, aiohttp calls transport.close() (graceful SSL
                    # shutdown) on the live connection, which waits for the peer to
                    # acknowledge the close_notify — causing the event-loop thread to
                    # hang when the relay is still alive.
                    try:
                        async for chunk in plaintext_chunks:
                            self._buffer.extend(chunk)
                            self._read_event.set()
                    except asyncio.CancelledError:
                        if resp.connection is not None and resp.connection.protocol is not None:
                            resp.connection.protocol.abort()
                        raise

                    # Mark EOF and wake any waiting readers
                    self._eof = True
                    self._read_event.set()
                    logger.debug("EOF reached, all data buffered")

        except DecryptionError:
            # Re-raise our own exception
            raise
        except AgeRTError as e:
            # Wrap age-rt decryption errors
            # Log at DEBUG — the exception is stored in self._exception and will be
            # re-raised at the read site, where the caller (e.g. CLI) logs at ERROR.
            logger.debug("Decryption failed: %s", e)
            self._exception = DecryptionError(
                message=f"Decryption failed: {e}",
                stream_path=self._stream_path,
                original_error=e,
            )
        except aiohttp.ClientError as e:
            logger.debug("Download failed: %s", e)
            self._exception = DownloadError(
                message=f"Download failed: {e}",
                stream_path=self._stream_path,
                original_error=e,
            )
        except Exception as e:
            logger.debug("Download failed unexpectedly: %s", e)
            self._exception = DownloadError(
                message=f"Download failed unexpectedly: {e}",
                stream_path=self._stream_path,
                original_error=e,
            )
        finally:
            # Always wake up waiting readers, even on exception
            self._read_event.set()

    async def __aenter__(self) -> "AsyncReader":
        """
        Enter the async context manager, starting the reader.

        Returns:
            AsyncReader: self
        """
        await self.start()
        return self

    async def abort(self) -> None:
        """
        Abort the reader immediately without protocol validation.

        Cancels the download task in-flight and returns without waiting for it
        to finish.  The subsequent portal shutdown (via
        :meth:`_AsyncInstanceManager.abort`) will stop the event loop and let
        anyio cancel any remaining tasks as part of its normal teardown.

        Call this instead of :meth:`stop` when an exception is already
        propagating (e.g. :class:`KeyboardInterrupt`) and a fast exit is
        required.  Skips ``eof_received`` / ``eof_consumed`` validation.

        This method is idempotent and safe to call multiple times.
        """
        if self._closed:
            logger.debug("AsyncReader.abort() called but already closed")
            return

        logger.debug("Aborting AsyncReader (no validation)")

        if self._download_task is not None:
            self._download_task.cancel()
            # Do not await — portal teardown cancels and joins remaining tasks.
            # Register callback to suppress 'Future exception was never retrieved'.
            self._download_task.add_done_callback(
                lambda t: t.exception() if not t.cancelled() else None
            )

        # Unblock any portal.call(read_*) task currently waiting on _read_event
        # inside the BlockingPortal task group.  portal.stop(cancel_remaining=False)
        # does not cancel that group, so without this the event-loop thread hangs
        # indefinitely in BlockingPortal.__aexit__.
        if self._exception is None:
            self._exception = StreamClosedError(
                operation="read", stream_path=self._stream_path
            )
        self._read_event.set()

        self.is_started = False
        self._closed = True
        logger.debug("AsyncReader aborted")

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the async context manager.

        Calls :meth:`abort` if an exception is in flight so cleanup is fast,
        otherwise calls :meth:`stop` for clean protocol validation.
        """
        if exc_type is not None:
            await self.abort()
        else:
            await self.stop()
