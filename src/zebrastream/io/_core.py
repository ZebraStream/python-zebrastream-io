# SPDX-License-Identifier: MIT
"""
Asynchronous ZebraStream I/O core implementation using aiohttp.
This module provides core asynchronous classes and functions for interacting with ZebraStream data streams
over HTTP using the ZebraStream Connect API.
"""

# TODO: an asyncio.StreamWriter-compatible wrapper class for AsyncZebraStreamWriter
# TODO: an anyio.ByteStream-compatible wrapper class for AsyncZebraStreamWriter
# TODO: check what happens, if the HTTP requests fail
# TODO: control queue capacity/size --> keep external for now (like StreamWriter)
# TODO: better (library-independent) exceptions
# TODO: use exponential backoff for connect procedure
# TODO: add aiohttp TCP connect timeout?
# TODO: signal premature disconnect as exception to user code

import asyncio
import logging
import typing

import aiohttp

logger = logging.getLogger(__name__)

ZEBRASTREAM_CONNECT_API_URL = "https://connect.zebrastream.io/v0/"

async def _connect(stream_path: str, mode: str, access_token: str | None = None, connect_timeout: int | None = None) -> str:
    """
    Establish a connection to the ZebraStream Connect API and get the data stream URL.
    
    Args:
        stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
        access_token (str, optional): Access token for authorization.
        connect_timeout (int, optional): Timeout in seconds for the connect operation.
    
    Returns:
        str: The data stream URL for subsequent requests.
    
    Raises:
        asyncio.TimeoutError: If the overall operation exceeds the client timeout.
    """
    assert mode in {"await-reader", "await-writer"}, "Invalid mode specified. Use 'await-reader' or 'await-writer'."
    
    # Construct the full connect URL from the base URL and stream path
    connect_url = ZEBRASTREAM_CONNECT_API_URL.rstrip('/') + stream_path
    
    headers = {}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    params = {"mode": mode}
    if connect_timeout is not None:
        params["timeout"] = str(connect_timeout+1)  # we prefer the client-side timeout to fire instead of the server-side timeout

    client_timeout = None
    if connect_timeout is not None:
        client_timeout = aiohttp.ClientTimeout(total=connect_timeout)

    async def _connect_attempt_loop() -> str:
        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            while True:
                try:
                    async with session.get(connect_url, params=params, headers=headers) as resp:
                        resp.raise_for_status()
                        data_stream_url = (await resp.text()).strip()
                        return data_stream_url
                except asyncio.TimeoutError:
                    # Don't catch TimeoutError here - let it bubble up to the outer try/except
                    raise
                except Exception as e:
                    # Only retry on non-timeout exceptions (network errors, HTTP errors, etc.)
                    logger.warning("Connection attempt failed: %s", e)
                    await asyncio.sleep(1)
    
    try:
        if connect_timeout is None:
            return await _connect_attempt_loop()
        return await asyncio.wait_for(_connect_attempt_loop(), timeout=connect_timeout)
    except asyncio.TimeoutError:
        logger.error("Connection attempt timed out after %s seconds", connect_timeout)
        raise


class AsyncWriter:
    """
    An asynchronous writer for ZebraStream data streams using HTTP PUT and aiohttp.

    This class provides an async interface for sending data to a ZebraStream endpoint. It manages connection setup,
    buffering, and upload tasks, and supports use as an async context manager.
    
    Buffering Behavior:
        Data written via write() may be buffered internally for efficiency. This buffering can occur at multiple 
        levels (text wrapper, internal queues, HTTP layer) and helps optimize network performance for bulk transfers.
        
        For applications requiring immediate data transmission (e.g., real-time logging, streaming), call flush() 
        after write() to ensure data is sent immediately without waiting for internal buffers to fill.
        
    Examples:
        # Real-time streaming - immediate transmission:
        async with AsyncWriter(stream, access_token="my_token", mode="wt") as writer:
            await writer.write("URGENT: system error\\n")
            await writer.flush()  # Guarantees immediate sending
            
        # Bulk transfer - let buffering optimize performance:
        async with AsyncWriter(stream, access_token="my_token", mode="wb") as writer:
            for data in large_dataset:
                await writer.write(data)  # Buffered for efficiency
            # Implicit flush() on context exit
    """

    _CONNECT_MODE: str = "await-reader"
    _stream_path: str
    _access_token: str | None
    _content_type: str | None
    _connect_timeout: int | None
    _buffer: asyncio.Queue[bytes | None]
    _write_failed: bool
    _upload_task: asyncio.Task[None]
    _data_stream_url: str
    is_started: bool

    def __init__(self, stream_path: str, access_token: str | None = None, content_type: str | None = None, connect_timeout: int | None = None) -> None:
        """
        Initialize an asynchronous ZebraStream writer.

        Args:
            stream_path (str): The ZebraStream stream path (e.g., '/my-stream').
            access_token (str, optional): Access token for authorization.
            content_type (str, optional): Content-Type for the HTTP request.
            connect_timeout (int, optional): Server-side timeout in seconds for the connect operation.
        """
        self._stream_path = stream_path
        self._access_token = access_token
        self._content_type = content_type
        self._connect_timeout = connect_timeout
        self._buffer = asyncio.Queue()
        self._write_failed = False
        self.is_started = False

    async def _start_connect(self) -> None:
        self._data_stream_url = await _connect(
            stream_path=self._stream_path,
            mode=self._CONNECT_MODE,
            access_token=self._access_token, 
            connect_timeout=self._connect_timeout
        )
    
    def _start_send(self) -> None:
        """Start the upload task to send data to the ZebraStream Data API."""
        assert hasattr(self, "_data_stream_url"), "Client is not connected"  # TODO: remove (expect correct handling)
        assert not hasattr(self, "_upload_task"), "Upload task is already running"  # TODO: remove (expect correct handling)
        self._upload_task = asyncio.create_task(self._upload())
        # TODO: check upload task for errors before continuing

    async def start(self) -> None:
        """
        Start the writer and wait for a peer to connect.

        Raises:
            RuntimeError: If the writer is already started.
        """
        if self.is_started:
            raise RuntimeError("Writer is already started")
        logger.debug("Starting AsyncWriter")
        await self._start_connect()
        self._start_send()
        self.is_started = True
        logger.debug("AsyncWriter started successfully")
    
    async def stop(self) -> None:
        """
        Stop the writer and wait for the upload task to finish.

        Raises:
            RuntimeError: If the writer is not started.
        """
        if not self.is_started:
            raise RuntimeError("Writer is not started")
        logger.debug("Stopping AsyncWriter")
        await self._buffer.put(None)
        # await self.flush()  # Ensure all data is processed --> blocks!
        try:
            await self._upload_task
        except Exception as e:
            if not self._write_failed:
                logger.error("Upload task failed: %s", e)
                raise e
        self.is_started = False
        logger.debug("AsyncWriter stopped")

    async def write(self, data: bytes) -> None:
        """
        Write bytes to the ZebraStream data stream asynchronously.
        
        Note: Data may be buffered internally for efficiency. For immediate transmission,
        call flush() after write() to ensure data is sent without delay.

        Args:
            data (bytes): The data to write.
        Raises:
            RuntimeError: If the writer is not started.
            Exception: If the upload task failed.
        """
        # Check if the upload task has failed and propagate the exception
        if self._upload_task and self._upload_task.done():
            exc = self._upload_task.exception()
            if exc is not None:
                self._write_failed = True
                raise exc
        if not self.is_started:  # TODO: avoid check
            raise RuntimeError("Writer is not started")
        await self._buffer.put(data)
        
    async def flush(self) -> None:
        """
        Ensure all buffered data is sent immediately.
        
        This method waits until all data in the transfer queue has been processed by the upload task,
        forcing any internal buffers to be transmitted. Only flush() guarantees immediate sending -
        without it, data may be buffered for efficiency until internal thresholds are reached.
        
        Use flush() after write() for real-time applications where immediate data transmission
        is required (e.g., log streaming, alerts, interactive applications).
        """
        await self._buffer.join()

    async def _upload(self) -> None:
        async def chunks() -> typing.AsyncGenerator[bytes, None]:
            yield b""  # keep aiohttp waiting
            while True:
                # TODO: peek and remove only after uploading?
                chunk = await self._buffer.get()
                if chunk is None:
                    break
                yield chunk
                self._buffer.task_done()  # Mark the chunk as processed
        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        if self._content_type:
            headers["Content-Type"] = self._content_type
        async with aiohttp.ClientSession() as session:
            async with session.put(self._data_stream_url, headers=headers, data=chunks()) as resp:
                async for line in resp.content:
                    # TODO: validate entire control message sequence
                    line_str = line.decode(errors="replace").rstrip()
                    logger.debug("Server response: %s", line_str)
                    if line_str == "[ERROR:RECEIVER_PREMATURE_DISCONNECT]":
                        logger.warning("Receiver disconnected prematurely")
                        raise RuntimeError("Receiver disconnected prematurely")
                resp.raise_for_status()
            assert line_str == "[STATE:TRANSFER_SUCCESSFUL]", "Server did not confirm successful transfer"

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
        Exit the async context manager, stopping the writer.
        """
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
    _block_size: int
    _buffer: bytearray
    _read_event: asyncio.Event
    _download_task: asyncio.Task[None]
    _data_stream_url: str
    is_started: bool
    _eof: bool
    _exception: Exception | None

    def __init__(self, stream_path: str, access_token: str | None = None, content_type: str | None = None, connect_timeout: int | None = None, block_size: int = 4096) -> None:
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
        """
        self._stream_path = stream_path
        self._access_token = access_token
        self._content_type = content_type
        self._connect_timeout = connect_timeout
        self._block_size = block_size
        self._buffer = bytearray()
        self._read_event = asyncio.Event()
        self.is_started = False
        self._eof = False
        self._exception = None

    async def _start_connect(self) -> None:
        self._data_stream_url = await _connect(
            stream_path=self._stream_path,
            mode=self._CONNECT_MODE,
            access_token=self._access_token, 
            connect_timeout=self._connect_timeout
        )
    
    def _start_download(self) -> None:
        assert hasattr(self, "_data_stream_url"), "Client is not connected"  # TODO: remove (expect correct handling)
        assert not hasattr(self, "_download_task"), "Download task is already running"  # TODO: remove (expect correct handling)
        self._download_task = asyncio.create_task(self._download())

    async def start(self) -> None:
        """
        Start the reader and wait for a peer to connect.

        Raises:
            RuntimeError: If the reader is already started.
        """
        if self.is_started:
            raise RuntimeError("Reader is already started")
        logger.debug("Starting AsyncReader")
        await self._start_connect()
        self._start_download()
        self.is_started = True
        logger.debug("AsyncReader started successfully")

    async def stop(self) -> None:
        """
        Stop the reader and wait for the download task to finish.

        Raises:
            RuntimeError: If the reader is not started.
        """
        if not self.is_started:
            raise RuntimeError("Reader is not started")
        logger.debug("Stopping AsyncReader")
        if self._download_task:
            self._download_task.cancel()
            try:
                await self._download_task
            except Exception:
                pass
        self.is_started = False
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
            ValueError: If n <= 0.
            Exception: If an error occurs during reading.
        """
        if n <= 0:
            raise ValueError("n must be positive")
            
        while len(self._buffer) < n and not self._eof:
            if self._exception:
                raise self._exception
            await self._read_event.wait()
            self._read_event.clear()
        if not self._buffer and self._eof:
            return b''
        data = bytes(memoryview(self._buffer)[:n])
        del self._buffer[:n] 
        return data

    async def read_variable_block(self, n: int) -> bytes:
        """
        Read up to n bytes of available data from the ZebraStream data stream asynchronously.
        
        This method returns immediately with whatever data is available, up to n bytes.
        
        Args:
            n (int): Maximum number of bytes to read. Must be > 0.
        Returns:
            bytes: The data read from the stream. Returns up to n bytes of available data,
               or empty bytes if EOF is reached.
        Raises:
            ValueError: If n <= 0.
            Exception: If an error occurs during reading.
        """
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
        
        # Return whatever data we have available, up to n bytes
        if not self._buffer:
            return b''
            
        data = bytes(memoryview(self._buffer)[:n])
        del self._buffer[:n] 
        return data

    async def read_all(self) -> bytes:
        """
        Read all remaining data until EOF.
        
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


        # Return all buffered data
        if not self._buffer:
            return b''

        data = bytes(self._buffer)
        self._buffer.clear()
        return data
    
    async def _download(self) -> None:
        # TODO: match content type, ot raise exception
        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self._data_stream_url, headers=headers) as resp:
                    async for chunk in resp.content.iter_chunked(self._block_size):
                        if not chunk:
                            break
                        self._buffer.extend(chunk)
                        self._read_event.set()
                    self._eof = True
                    self._read_event.set()
        except Exception as e:
            logger.error("Download failed: %s", e)
            self._exception = e
            self._read_event.set()

    async def __aenter__(self) -> "AsyncReader":
        """
        Enter the async context manager, starting the reader.

        Returns:
            AsyncReader: self
        """
        await self.start()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        """
        Exit the async context manager, stopping the reader.
        """
        await self.stop()
