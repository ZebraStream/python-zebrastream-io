"""
age-rt: age-based real-time encryption for streaming data

This module implements the age-rt v0.2 protocol, providing authenticated
encryption for streaming data using the age v1 format with passphrase-based
key derivation (scrypt).

age-rt v0.2 features:
- Variable-length chunks with authentication
- ChaCha20-Poly1305 AEAD encryption
- HKDF-based payload key derivation (info="payload")
- age v1 PSK header format with scrypt
- Truncation detection via final flag in nonce

Wire format:
    [age header][16-byte nonce][length-prefixed chunks]

Example (encoding):
    # Simple: encode to file
    with open('data.age', 'wb') as f:
        encode_file([b"chunk1", b"chunk2"], f, "secret")

    # Or: iterate over wire chunks
    for wire_chunk in iter_encode([b"chunk1", b"chunk2"], "secret"):
        output.write(wire_chunk)

Example (decoding):
    # Simple: decode from file
    with open('data.age', 'rb') as f:
        for plaintext in decode_file(f, "secret"):
            process(plaintext)

    # Or: decode from read function
    with open("data.age", "rb") as f:
        for plaintext in iter_decode_callable(f.read, "secret"):
            process(plaintext)

Example (async decoding):
    async for plaintext in aiter_decode_callable(reader.read_fixed_block, "secret"):
        await process(plaintext)

Requires: cryptography>=41.0.0
"""

from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from typing import Optional, Iterator, Callable, Awaitable, AsyncIterator, Iterable, AsyncIterable, BinaryIO
from enum import Enum, auto
import io
import struct
import os
import base64


# ============================================================================


# ============================================================================
# Exception Classes
# ============================================================================


class AgeRTError(Exception):
    """Base exception for age-rt errors."""

    pass


class DecodeError(AgeRTError):
    """Base class for decoding errors."""

    pass


class HeaderParseError(DecodeError):
    """Raised when age header parsing fails."""

    pass


class ChunkAuthenticationError(DecodeError):
    """Raised when chunk authentication fails."""

    pass


class InsufficientDataError(DecodeError):
    """Decoder received insufficient data in feed()."""

    pass


class StreamTruncatedError(DecodeError):
    """I/O stream ended before decoding complete (factory-level)."""

    pass


# ============================================================================
# Internal Helper Functions
# ============================================================================


def _derive_file_key_from_passphrase(passphrase: str, salt: bytes) -> bytes:
    """
    Derive 32-byte file key from passphrase using scrypt (age v1 parameters).

    Args:
        passphrase: User passphrase
        salt: 16-byte salt for scrypt

    Returns:
        32-byte file key
    """
    if len(salt) != 16:
        raise ValueError("Salt must be 16 bytes")

    kdf = Scrypt(
        salt=salt,
        length=32,
        n=2**18,  # Age v1 uses 2^18 for scrypt
        r=8,
        p=1,
    )
    return kdf.derive(passphrase.encode("utf-8"))


def _wrap_file_key(file_key: bytes, passphrase: str, scrypt_salt: bytes) -> bytes:
    """
    Wrap file key using passphrase-derived wrapping key.

    Args:
        file_key: 32-byte file key to wrap
        passphrase: User passphrase
        scrypt_salt: 16-byte salt for scrypt

    Returns:
        Wrapped file key (ciphertext + tag, 48 bytes)
    """
    wrap_key = _derive_file_key_from_passphrase(passphrase, scrypt_salt)
    cipher = ChaCha20Poly1305(wrap_key)
    nonce = b"\x00" * 12  # Zero nonce for wrapping
    wrapped = cipher.encrypt(nonce, file_key, None)
    return wrapped


def _unwrap_file_key(wrapped: bytes, passphrase: str, scrypt_salt: bytes) -> bytes:
    """
    Unwrap file key using passphrase-derived wrapping key.

    Args:
        wrapped: Wrapped file key (48 bytes)
        passphrase: User passphrase
        scrypt_salt: 16-byte salt for scrypt

    Returns:
        Unwrapped 32-byte file key

    Raises:
        ChunkAuthenticationError: If authentication fails (wrong passphrase)
    """
    wrap_key = _derive_file_key_from_passphrase(passphrase, scrypt_salt)
    cipher = ChaCha20Poly1305(wrap_key)
    nonce = b"\x00" * 12  # Zero nonce for wrapping
    try:
        file_key = cipher.decrypt(nonce, wrapped, None)
        return file_key
    except Exception as e:
        raise ChunkAuthenticationError(f"Failed to unwrap file key (wrong passphrase?): {e}")


def _encode_age_psk_header(passphrase: str, file_key: bytes, scrypt_salt: bytes) -> bytes:
    """
    Encode age v1 PSK header with scrypt stanza.

    Format:
        age-encryption.org/v1
        -> scrypt <base64-salt> 18
        <base64-wrapped-file-key>
        ---

    Args:
        passphrase: User passphrase
        file_key: 32-byte file key to wrap
        scrypt_salt: 16-byte salt for scrypt

    Returns:
        Header as bytes (with trailing newline)
    """
    wrapped_key = _wrap_file_key(file_key, passphrase, scrypt_salt)

    # Encode to base64 (age uses standard base64)
    salt_b64 = base64.b64encode(scrypt_salt).decode("ascii")
    wrapped_b64 = base64.b64encode(wrapped_key).decode("ascii")

    # Build header
    header_lines = ["age-encryption.org/v1", f"-> scrypt {salt_b64} 18", wrapped_b64, "---"]

    return "\n".join(header_lines).encode("utf-8") + b"\n"


def _decode_age_psk_header(header_bytes: bytes, passphrase: str) -> bytes:
    """
    Decode age v1 PSK header and unwrap file key.

    Args:
        header_bytes: Header bytes (with newlines)
        passphrase: User passphrase

    Returns:
        Unwrapped 32-byte file key

    Raises:
        HeaderParseError: If header format is invalid
        ChunkAuthentication Error: If authentication fails (wrong passphrase)
    """
    lines = header_bytes.decode("utf-8").strip().split("\n")

    # Validate format
    if len(lines) < 4:
        raise HeaderParseError("Invalid age header: too few lines")

    if lines[0] != "age-encryption.org/v1":
        raise HeaderParseError(f"Invalid age header: expected 'age-encryption.org/v1', got '{lines[0]}'")

    if not lines[1].startswith("-> scrypt "):
        raise HeaderParseError("Invalid age header: expected scrypt stanza")

    if lines[-1] != "---":
        raise HeaderParseError("Invalid age header: missing '---' footer")

    # Parse scrypt stanza
    stanza_parts = lines[1].split()
    if len(stanza_parts) != 4:
        raise HeaderParseError("Invalid scrypt stanza format")

    salt_b64 = stanza_parts[2]
    log_n = int(stanza_parts[3])

    if log_n != 18:
        raise HeaderParseError(f"Unsupported scrypt work factor: 2^{log_n}")

    # Decode base64
    try:
        scrypt_salt = base64.b64decode(salt_b64)
        wrapped_key = base64.b64decode(lines[2])
    except Exception as e:
        raise HeaderParseError(f"Failed to decode base64: {e}")

    # Unwrap file key (may raise ChunkAuthenticationError)
    file_key = _unwrap_file_key(wrapped_key, passphrase, scrypt_salt)

    return file_key


def _make_aead_nonce(chunk_index: int, is_final: bool) -> bytes:
    """
    Create 12-byte AEAD nonce: 11-byte counter + 1-byte final flag.

    Args:
        chunk_index: Chunk sequence number
        is_final: Whether this is the final chunk

    Returns:
        12-byte nonce for ChaCha20-Poly1305
    """
    last_chunk_flag = 0x01 if is_final else 0x00
    return chunk_index.to_bytes(11, "big") + bytes([last_chunk_flag])


# Encoder
# ============================================================================


class AgeRTEncoder:
    """
    age-rt v0.2 stream encoder.

    Encodes plaintext chunks into age-rt wire format with authentication.
    Supports passphrase-based encryption with age v1 PSK headers.

    Do not instantiate directly. Use factory methods:
    - AgeRTEncoder.from_passphrase("secret")

    Example:
        encoder = AgeRTEncoder.from_passphrase("secret")
        output.write(encoder.get_header())
        for chunk in chunks:
            output.write(encoder.encode_chunk(chunk))
        output.write(encoder.encode_chunk(b'', is_final=True))
    """

    def __init__(self, _file_key: bytes, _age_header: bytes):
        """
        Internal constructor. Use from_passphrase() instead.

        Args:
            _file_key: 32-byte file key (internal)
            _age_header: Pre-encoded age header bytes (internal)

        Raises:
            ValueError: If file_key is not 32 bytes
        """
        if len(_file_key) != 32:
            raise ValueError("File key must be 32 bytes")

        self._file_key = _file_key
        self._age_header = _age_header

        # Generate 16-byte nonce (payload nonce, HKDF salt)
        self._nonce = os.urandom(16)

        # Derive payload key using age v1 HKDF
        payload_key = HKDF(algorithm=hashes.SHA256(), length=32, salt=self._nonce, info=b"payload").derive(_file_key)

        self._cipher = ChaCha20Poly1305(payload_key)
        self._chunk_index = 0
        self._finalized = False

    @classmethod
    def from_passphrase(cls, passphrase: str) -> "AgeRTEncoder":
        """
        Create encoder from passphrase with age PSK header.

        Generates random file key and scrypt salt internally.

        Args:
            passphrase: User passphrase

        Returns:
            New AgeRTEncoder instance
        """
        file_key = os.urandom(32)
        scrypt_salt = os.urandom(16)
        age_header = _encode_age_psk_header(passphrase, file_key, scrypt_salt)
        return cls(_file_key=file_key, _age_header=age_header)

    def get_header(self) -> bytes:
        """
        Get complete stream header (age header + nonce).

        Returns:
            Stream header bytes
        """
        return self._age_header + self._nonce

    def encode_chunk(self, plaintext: bytes, is_final: bool = False) -> bytes:
        """
        Encode a single chunk into wire format.

        Wire format: [4-byte length][ciphertext + tag]

        Args:
            plaintext: Plaintext data to encode
            is_final: Whether this is the final chunk (required for truncation detection)

        Returns:
            Encoded chunk in wire format

        Raises:
            RuntimeError: If stream already finalized
        """
        if self._finalized:
            raise RuntimeError("Stream already finalized")

        aead_nonce = _make_aead_nonce(self._chunk_index, is_final)
        ciphertext = self._cipher.encrypt(aead_nonce, plaintext, None)
        self._chunk_index += 1

        if is_final:
            self._finalized = True

        return struct.pack(">I", len(ciphertext)) + ciphertext


# ============================================================================
# Decoder State Machine
# ============================================================================


class _DecoderState(Enum):
    """Internal decoder states."""

    HEADER_SCAN = auto()
    NONCE = auto()
    CHUNK_LENGTH = auto()
    CHUNK_DATA = auto()
    DONE = auto()


class AgeRTDecoder:
    """
    age-rt v0.2 stateful decoder.

    Push-based decoder that announces data needs and processes incrementally.
    Decouples I/O from parsing/crypto logic.

    Usage pattern:
        decoder = AgeRTDecoder(passphrase)
        while needed := decoder.bytes_needed:
            data = source.read(needed)
            for chunk in decoder.feed(data):
                process(chunk)

    For most use cases, use factory functions instead:
        - iter_decode_callable(): Sync from read function
        - aiter_decode_callable(): Async from read function
        - iter_decode_chunks(): Sync from byte iterable
        - aiter_decode_chunks(): Async from byte iterable
        - decode_file(): From file path
        - decode_bytes(): From bytes object
    """

    def __init__(self, passphrase: str, **kwargs):
        """
        Initialize decoder with passphrase.

        Args:
            passphrase: Passphrase for decryption
            **kwargs: Reserved for future extensions
        """
        self._passphrase = passphrase
        self._state = _DecoderState.HEADER_SCAN
        self._header_buffer = bytearray()
        self._chunk_index = 0
        self._finalized = False
        self._cipher: Optional[ChaCha20Poly1305] = None
        self._next_chunk_length: Optional[int] = None
        self._bytes_needed = 1  # Start by reading header byte-by-byte

    @property
    def bytes_needed(self) -> int:
        """
        Number of bytes needed for next operation.

        Returns 0 when decoding is complete.
        """
        return self._bytes_needed

    def is_done(self) -> bool:
        """
        Check if decoding is complete.

        Equivalent to checking if bytes_needed == 0.
        """
        return self._bytes_needed == 0

    def feed(self, data: bytes) -> Iterator[bytes]:
        """
        Feed exactly bytes_needed bytes to decoder.

        Yields decrypted plaintext chunks as they become available.

        Args:
            data: Must be exactly bytes_needed bytes

        Yields:
            Decrypted plaintext chunks

        Raises:
            InsufficientDataError: If wrong amount of data provided
            HeaderParseError: If header is invalid
            ChunkAuthenticationError: If authentication fails
        """
        if len(data) != self._bytes_needed:
            raise InsufficientDataError(f"Expected {self._bytes_needed} bytes, got {len(data)}")

        if self._state == _DecoderState.HEADER_SCAN:
            # Accumulate header byte-by-byte
            self._header_buffer.extend(data)
            if self._header_buffer.endswith(b"---\n"):
                # Header complete, transition to nonce
                if len(self._header_buffer) > 1024:
                    raise HeaderParseError("Header too large")
                self._state = _DecoderState.NONCE
                self._bytes_needed = 16
            # else: still scanning, keep _bytes_needed = 1
            return

        elif self._state == _DecoderState.NONCE:
            # Parse header and initialize cipher
            header_bytes = bytes(self._header_buffer)
            file_key = _decode_age_psk_header(header_bytes, self._passphrase)

            payload_key = HKDF(
                algorithm=hashes.SHA256(),
                length=32,
                salt=data,  # data is the 16-byte nonce
                info=b"payload",
            ).derive(file_key)

            self._cipher = ChaCha20Poly1305(payload_key)
            self._state = _DecoderState.CHUNK_LENGTH
            self._bytes_needed = 4
            return

        elif self._state == _DecoderState.CHUNK_LENGTH:
            # Parse chunk length
            length = struct.unpack(">I", data)[0]

            if length < 16 or length > 16 * 1024 * 1024:
                raise ChunkAuthenticationError(f"Invalid chunk length: {length}")

            self._next_chunk_length = length
            self._state = _DecoderState.CHUNK_DATA
            self._bytes_needed = length
            return

        elif self._state == _DecoderState.CHUNK_DATA:
            # Decrypt chunk
            ciphertext = data

            # Try non-final, then final
            try:
                plaintext = self._cipher.decrypt(_make_aead_nonce(self._chunk_index, False), ciphertext, None)
                self._chunk_index += 1
                self._state = _DecoderState.CHUNK_LENGTH
                self._bytes_needed = 4
                yield plaintext

            except Exception:
                try:
                    plaintext = self._cipher.decrypt(_make_aead_nonce(self._chunk_index, True), ciphertext, None)
                    self._chunk_index += 1
                    self._state = _DecoderState.DONE
                    self._bytes_needed = 0
                    yield plaintext  # Always yield, even if empty

                except Exception as e:
                    raise ChunkAuthenticationError(f"Auth failed at chunk {self._chunk_index}: {e}")

        elif self._state == _DecoderState.DONE:
            raise RuntimeError("Decoder already complete")


# ============================================================================
# Factory Functions
# ============================================================================


def iter_encode(chunks: Iterable[bytes], passphrase: str, **kwargs) -> Iterator[bytes]:
    """
    Encode chunks as iterator with automatic finalization.

    Automatically handles header and final flag on last chunk.

    Args:
        chunks: Iterable of plaintext chunks
        passphrase: User passphrase
        **kwargs: Reserved for future extensions

    Yields:
        Encoded wire format chunks (header first, then encrypted chunks)

    Example:
        >>> for wire_chunk in iter_encode(plaintext_chunks, "secret"):
        ...     output.write(wire_chunk)
    """
    encoder = AgeRTEncoder.from_passphrase(passphrase)
    yield encoder.get_header()

    for chunk in chunks:
        yield encoder.encode_chunk(chunk, is_final=False)

    yield encoder.encode_chunk(b"", is_final=True)


async def aiter_encode(chunks: AsyncIterable[bytes], passphrase: str, **kwargs) -> AsyncIterator[bytes]:
    """
    Encode chunks from async iterable with automatic finalization.

    Automatically handles header and final flag on last chunk.

    Args:
        chunks: Async iterable of plaintext chunks
        passphrase: User passphrase
        **kwargs: Reserved for future extensions

    Yields:
        Encoded wire format chunks (header first, then encrypted chunks)

    Example:
        >>> async for wire_chunk in aiter_encode(async_chunks, "secret"):
        ...     await output.write(wire_chunk)
    """
    encoder = AgeRTEncoder.from_passphrase(passphrase)
    yield encoder.get_header()

    async for chunk in chunks:
        yield encoder.encode_chunk(chunk, is_final=False)

    yield encoder.encode_chunk(b"", is_final=True)


def iter_decode_callable(read_func: Callable[[int], bytes], passphrase: str, **kwargs) -> Iterator[bytes]:
    """
    Decode from synchronous read function.

    Args:
        read_func: Function(n: int) -> bytes that reads exactly n bytes
        passphrase: Decryption passphrase
        **kwargs: Additional arguments passed to AgeRTDecoder

    Yields:
        Decrypted plaintext chunks

    Raises:
        StreamTruncatedError: If read returns fewer bytes than requested
        HeaderParseError: If age header is invalid
        ChunkAuthenticationError: If chunk authentication fails

    Example:
        >>> with open('encrypted.age', 'rb') as f:
        ...     for chunk in iter_decode_callable(f.read, "secret"):
        ...         print(chunk)
    """
    decoder = AgeRTDecoder(passphrase, **kwargs)

    while not decoder.is_done():
        needed = decoder.bytes_needed
        data = read_func(needed)

        if len(data) != needed:
            raise StreamTruncatedError(f"Stream ended: got {len(data)} bytes, needed {needed}")

        yield from decoder.feed(data)


async def aiter_decode_callable(
    read_func: Callable[[int], Awaitable[bytes]], passphrase: str, **kwargs
) -> AsyncIterator[bytes]:
    """
    Decode from asynchronous read function.

    Args:
        read_func: Async function(n: int) -> bytes that reads exactly n bytes
        passphrase: Decryption passphrase
        **kwargs: Additional arguments passed to AgeRTDecoder

    Yields:
        Decrypted plaintext chunks

    Raises:
        StreamTruncatedError: If read returns fewer bytes than requested
        HeaderParseError: If age header is invalid
        ChunkAuthenticationError: If chunk authentication fails

    Example:
        >>> async for chunk in aiter_decode_callable(reader.read_fixed_block, "secret"):
        ...     await process(chunk)
    """
    decoder = AgeRTDecoder(passphrase, **kwargs)

    while not decoder.is_done():
        needed = decoder.bytes_needed
        data = await read_func(needed)

        if len(data) != needed:
            raise StreamTruncatedError(f"Stream ended: got {len(data)} bytes, needed {needed}")

        for chunk in decoder.feed(data):
            yield chunk


def iter_decode_chunks(data_source: Iterable[bytes], passphrase: str, **kwargs) -> Iterator[bytes]:
    """
    Decode from iterable of byte chunks.

    Handles buffering internally when chunks don't align with decoder needs.

    Args:
        data_source: Iterable yielding bytes chunks (not individual bytes!)
        passphrase: Decryption passphrase
        **kwargs: Additional arguments passed to AgeRTDecoder

    Yields:
        Decrypted plaintext chunks

    Raises:
        StreamTruncatedError: If source exhausted before decoding complete
        HeaderParseError: If age header is invalid
        ChunkAuthenticationError: If chunk authentication fails

    Example:
        >>> chunks = [b'part1', b'part2', b'part3']
        >>> for plaintext in iter_decode_chunks(chunks, "secret"):
        ...     print(plaintext)
    """
    decoder = AgeRTDecoder(passphrase, **kwargs)
    buffer = bytearray()
    source_iter = iter(data_source)

    while not decoder.is_done():
        needed = decoder.bytes_needed

        # Fill buffer to needed amount
        while len(buffer) < needed:
            try:
                chunk = next(source_iter)
                buffer.extend(chunk)
            except StopIteration:
                raise StreamTruncatedError("Source exhausted before decoding complete")

        # Feed exact amount
        data = bytes(buffer[:needed])
        del buffer[:needed]

        yield from decoder.feed(data)


async def aiter_decode_chunks(data_source: AsyncIterable[bytes], passphrase: str, **kwargs) -> AsyncIterator[bytes]:
    """
    Decode from async iterable of byte chunks.

    Handles buffering internally when chunks don't align with decoder needs.

    Args:
        data_source: Async iterable yielding bytes chunks
        passphrase: Decryption passphrase
        **kwargs: Additional arguments passed to AgeRTDecoder

    Yields:
        Decrypted plaintext chunks

    Raises:
        StreamTruncatedError: If source exhausted before decoding complete
        HeaderParseError: If age header is invalid
        ChunkAuthenticationError: If chunk authentication fails

    Example:
        >>> async def fetch_chunks():
        ...     for i in range(10):
        ...         yield await fetch_data(i)
        >>> async for plaintext in aiter_decode_chunks(fetch_chunks(), "secret"):
        ...     await process(plaintext)
    """
    decoder = AgeRTDecoder(passphrase, **kwargs)
    buffer = bytearray()
    source_iter = aiter(data_source)

    while not decoder.is_done():
        needed = decoder.bytes_needed

        # Fill buffer to needed amount
        while len(buffer) < needed:
            try:
                chunk = await anext(source_iter)
                buffer.extend(chunk)
            except StopAsyncIteration:
                raise StreamTruncatedError("Source exhausted before decoding complete")

        # Feed exact amount
        data = bytes(buffer[:needed])
        del buffer[:needed]

        for chunk in decoder.feed(data):
            yield chunk


# ============================================================================
# Convenience Wrappers
# ============================================================================


def encode_file(chunks: Iterable[bytes], file: BinaryIO, passphrase: str, **kwargs) -> None:
    """
    Encode chunks to file-like object.

    Args:
        chunks: Iterable of plaintext chunks
        file: Binary file-like object with write() method
        passphrase: Encryption passphrase
        **kwargs: Reserved for future extensions

    Example:
        >>> with open('data.age', 'wb') as f:
        ...     encode_file([b'chunk1', b'chunk2'], f, "secret")
    """
    for wire_chunk in iter_encode(chunks, passphrase, **kwargs):
        file.write(wire_chunk)


def encode_bytes(chunks: Iterable[bytes], passphrase: str, **kwargs) -> bytes:
    """
    Encode chunks to bytes object.

    Args:
        chunks: Iterable of plaintext chunks
        passphrase: Encryption passphrase
        **kwargs: Reserved for future extensions

    Returns:
        Encrypted data as bytes

    Example:
        >>> encrypted = encode_bytes([b'chunk1', b'chunk2'], "secret")
    """
    stream = io.BytesIO()
    for wire_chunk in iter_encode(chunks, passphrase, **kwargs):
        stream.write(wire_chunk)
    return stream.getvalue()


def decode_file(file: BinaryIO, passphrase: str, **kwargs) -> Iterator[bytes]:
    """
    Decode from file-like object.

    Args:
        file: Binary file-like object with read() method
        passphrase: Decryption passphrase
        **kwargs: Additional arguments passed to AgeRTDecoder

    Yields:
        Decrypted plaintext chunks

    Example:
        >>> with open('data.age', 'rb') as f:
        ...     for chunk in decode_file(f, "secret"):
        ...         print(chunk)
    """
    yield from iter_decode_callable(file.read, passphrase, **kwargs)


def decode_bytes(data: bytes, passphrase: str, **kwargs) -> Iterator[bytes]:
    """
    Decode from bytes object.

    Args:
        data: Encrypted bytes
        passphrase: Decryption passphrase
        **kwargs: Additional arguments passed to AgeRTDecoder

    Yields:
        Decrypted plaintext chunks

    Example:
        >>> encrypted = b'...'
        >>> for chunk in decode_bytes(encrypted, "secret"):
        ...     print(chunk)
    """
    stream = io.BytesIO(data)
    yield from iter_decode_callable(stream.read, passphrase, **kwargs)
