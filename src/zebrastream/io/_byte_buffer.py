# SPDX-License-Identifier: MIT
"""Async byte buffer with ring buffer backend for efficient streaming."""

import asyncio


class SimpleRingBuffer:
    """Simple ring buffer using pre-allocated bytearray.

    Provides O(1) push/pop operations with automatic wraparound.
    Pure Python implementation suitable for most use cases where
    network I/O is the bottleneck.

    Not thread-safe. Designed for async usage where one or more tasks write
    and one or more tasks read (coordination handled by AsyncByteBuffer wrapper).
    """

    def __init__(self, capacity: int):
        """Initialize ring buffer with fixed capacity.

        Args:
            capacity: Maximum number of bytes the buffer can hold.
        """
        self._buffer = bytearray(capacity)
        self._capacity = capacity
        self._read_pos = 0
        self._write_pos = 0
        self._size = 0

    def push(self, data: bytes) -> None:
        """Push data into ring buffer.

        Caller must ensure sufficient space is available.

        Args:
            data: Bytes to push. len(data) must be <= (capacity - len(self)).
        """
        n = len(data)

        if self._write_pos + n <= self._capacity:
            # Contiguous write - no wraparound
            self._buffer[self._write_pos : self._write_pos + n] = data
        else:
            # Wraparound write - split into two parts
            first_part = self._capacity - self._write_pos
            self._buffer[self._write_pos :] = data[:first_part]
            self._buffer[: n - first_part] = data[first_part:]

        self._write_pos = (self._write_pos + n) % self._capacity
        self._size += n

    def pop(self, n: int) -> bytes:
        """Pop n bytes from ring buffer.

        Caller must ensure sufficient data is available.

        Args:
            n: Number of bytes to pop. Must be <= len(self).

        Returns:
            Exactly n bytes removed from the buffer.
        """
        if self._read_pos + n <= self._capacity:
            # Contiguous read - no wraparound
            result = bytes(self._buffer[self._read_pos : self._read_pos + n])
        else:
            # Wraparound read - split into two parts
            first_part = self._capacity - self._read_pos
            result = bytes(self._buffer[self._read_pos :]) + bytes(self._buffer[: n - first_part])

        self._read_pos = (self._read_pos + n) % self._capacity
        self._size -= n
        return result

    def __len__(self) -> int:
        """Return current number of bytes stored."""
        return self._size

    @property
    def capacity(self) -> int:
        """Return maximum capacity in bytes."""
        return self._capacity


class AsyncByteBuffer:
    """Async byte buffer with backpressure control.

    Wraps a SimpleRingBuffer to provide async write/consume operations
    with automatic blocking when buffer is full (backpressure) or empty.

    Designed for streaming data between async tasks (e.g., buffering before
    encryption). Supports multiple producers and/or consumers.

    Memory usage: Pre-allocated to max_bytes capacity for O(1) operations.

    Note: For zero-copy peek operations, could add:
        def peek(self, n: int) -> memoryview:
            return memoryview(self._backend._buffer)[start:end]
    """

    def __init__(self, max_bytes: int):
        """Initialize async byte buffer.

        Args:
            max_bytes: Maximum buffer capacity in bytes.
        """
        self._backend = SimpleRingBuffer(max_bytes)
        self._space_available = asyncio.Event()
        self._data_available = asyncio.Event()
        self._buffer_empty = asyncio.Event()
        self._space_available.set()  # Initially has space
        self._buffer_empty.set()  # Initially empty
        self._closed = False

    async def write(self, data: bytes) -> None:
        """Write data to buffer, blocking if full (backpressure).

        Automatically chunks data if it exceeds buffer capacity, writing
        one chunk at a time as space becomes available. Uses memoryview
        for efficient zero-copy slicing of large writes.

        Args:
            data: Bytes to write.

        Raises:
            ValueError: If buffer is closed.
        """
        if self._closed:
            raise ValueError("Cannot write to closed buffer")

        # Use memoryview for zero-copy chunking of large writes
        view = memoryview(data)
        offset = 0

        while offset < len(view):
            # Calculate how much space is available
            available = self._backend.capacity - len(self._backend)

            if available == 0:
                # Buffer full, wait for space
                self._space_available.clear()
                await self._space_available.wait()
                if self._closed:
                    raise ValueError("Buffer closed while waiting for space")
                continue

            # Write as much as fits (copy only when pushing to backend)
            chunk_size = min(available, len(view) - offset)
            chunk_view = view[offset : offset + chunk_size]
            self._backend.push(bytes(chunk_view))
            offset += chunk_size

            self._data_available.set()
            if len(self._backend) > 0:
                self._buffer_empty.clear()

    async def consume_available(self, n: int | None = None) -> bytes:
        """Wait for data, then consume what's available (up to n bytes).

        Blocks until at least some data is available or EOF is reached.
        Returns all available data once data arrives (natural batching).

        Under normal conditions: returns write_buffer_size chunks.
        Under backpressure: may return larger batches (multiple accumulated flushes).

        Args:
            n: Maximum bytes to consume. If None, consume all available data.

        Returns:
            1+ bytes when data becomes available (up to n if specified).
            Empty bytes b'' only on EOF when buffer is empty.

        This is the primary consumption method for streaming scenarios
        where you want to process data as soon as it's available without
        waiting for specific chunk sizes.
        """
        return await self._consume(min_bytes=1, max_bytes=n)

    async def consume_exact(self, n: int) -> bytes:
        """Wait for exactly n bytes, then consume.

        Blocks until exactly n bytes are available or EOF is reached.

        Args:
            n: Exact number of bytes to consume (must be > 0).

        Returns:
            Exactly n bytes if available.
            Fewer than n bytes if EOF reached before n bytes accumulated.
            Empty bytes b'' only if EOF and buffer is empty.

        Use this when you need fixed-size chunks (e.g., for encryption
        algorithms that require specific block sizes).

        Raises:
            ValueError: If n <= 0
        """
        if n <= 0:
            raise ValueError(f"n must be > 0, got {n}")
        return await self._consume(min_bytes=n, max_bytes=n)

    async def _consume(self, min_bytes: int, max_bytes: int | None) -> bytes:
        """Internal consume implementation.

        Args:
            min_bytes: Minimum bytes needed before returning (blocks until available).
            max_bytes: Maximum bytes to consume (None = no limit).

        Returns:
            Consumed bytes, or b'' on EOF.
        """
        # Wait for minimum bytes or EOF
        while len(self._backend) < min_bytes and not self._closed:
            self._data_available.clear()
            await self._data_available.wait()

        # Snapshot current state after waiting
        available = len(self._backend)
        if available == 0:  # EOF
            return b""

        # Calculate how much to consume
        if max_bytes is None:
            actual_n = available
        else:
            actual_n = min(max_bytes, available)

        # Consume and update state
        result = self._backend.pop(actual_n)
        self._space_available.set()
        if actual_n == available:  # Consumed everything
            self._buffer_empty.set()
        return result

    def close(self) -> None:
        """Signal EOF or force-close the buffer.

        Wakes all waiters so they can handle the closed state:
        - ``consume_available`` / ``_consume`` waiting for data
        - ``drain`` waiting for the buffer to empty
        - ``write`` waiting for buffer space (backpressure)

        Safe to call multiple times.
        """
        self._closed = True
        self._data_available.set()   # wake waiting readers
        self._buffer_empty.set()     # wake waiting drain() calls
        self._space_available.set()  # wake waiting write() calls

    async def drain(self) -> None:
        """Wait until all data has been consumed (buffer is empty).

        Provides backpressure by blocking until the buffer drains.
        Similar to asyncio.StreamWriter.drain().
        """
        if len(self._backend) == 0:
            return
        self._buffer_empty.clear()
        await self._buffer_empty.wait()

    def __len__(self) -> int:
        """Return number of bytes currently buffered."""
        return len(self._backend)
