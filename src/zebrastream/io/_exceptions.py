"""
Exception hierarchy for zebrastream-io.

This module defines the core exception hierarchy used by AsyncReader and AsyncWriter.
File-like interfaces (Reader, Writer) translate these to stdlib exceptions for BinaryIO compatibility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


# ============================================================================
# Base Exception
# ============================================================================


class ZebraStreamError(Exception):
    """Base exception for all zebrastream errors.
    
    Attributes:
        message: Human-readable error message
        stream_path: The stream path involved (if applicable)
        original_error: Wrapped underlying exception (if any)
    """
    
    def __init__(
        self,
        message: str,
        stream_path: str | None = None,
        original_error: Exception | None = None,
    ):
        self.message = message
        self.stream_path = stream_path
        self.original_error = original_error
        super().__init__(message)


# ============================================================================
# Connection Exceptions
# ============================================================================


class ZebraStreamConnectionError(ZebraStreamError):
    """Base for connection establishment failures.
    
    Additional Attributes:
        connect_api_url: The connect API URL used
    """
    
    def __init__(
        self,
        message: str,
        stream_path: str | None = None,
        original_error: Exception | None = None,
        connect_api_url: str | None = None,
    ):
        super().__init__(message, stream_path, original_error)
        self.connect_api_url = connect_api_url


class ConnectionTimeoutError(ZebraStreamConnectionError):
    """Connection attempt timed out.
    
    Additional Attributes:
        timeout_seconds: The timeout value that was exceeded
    """
    
    def __init__(
        self,
        timeout_seconds: float,
        stream_path: str | None = None,
        **kwargs: Any,
    ):
        message = f"Connection timed out after {timeout_seconds}s"
        if stream_path:
            message += f" for stream '{stream_path}'"
        super().__init__(message, stream_path, **kwargs)
        self.timeout_seconds = timeout_seconds


class ConnectionFailedError(ZebraStreamConnectionError):
    """Connection failed after all retries exhausted.
    
    Additional Attributes:
        retries: Number of retry attempts made
        last_error: Last error that caused failure
    """
    
    def __init__(
        self,
        retries: int,
        last_error: Exception | None = None,
        **kwargs: Any,
    ):
        message = f"Connection failed after {retries} retries"
        if last_error:
            message += f": {last_error}"
        super().__init__(message, original_error=last_error, **kwargs)
        self.retries = retries
        self.last_error = last_error


class AuthenticationError(ZebraStreamConnectionError):
    """Authentication or authorization failed.
    
    Additional Attributes:
        status_code: HTTP status code (401, 403)
    """
    
    def __init__(
        self,
        status_code: int,
        stream_path: str | None = None,
        **kwargs: Any,
    ):
        message = f"Authentication failed with status {status_code}"
        if stream_path:
            message += f" for stream '{stream_path}'"
        super().__init__(message, stream_path, **kwargs)
        self.status_code = status_code


# ============================================================================
# Transfer Exceptions
# ============================================================================


class ZebraStreamTransferError(ZebraStreamError):
    """Base for data transfer failures.
    
    Additional Attributes:
        phase: "upload" or "download"
        bytes_transferred: Bytes successfully transferred before failure
    """
    
    def __init__(
        self,
        message: str,
        phase: str,
        bytes_transferred: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(message, **kwargs)
        self.phase = phase
        self.bytes_transferred = bytes_transferred


class PeerDisconnectedError(ZebraStreamTransferError):
    """Peer disconnected prematurely during transfer.
    
    Additional Attributes:
        peer_role: "reader" or "writer" (who disconnected)
    """
    
    def __init__(
        self,
        peer_role: str,
        phase: str,
        **kwargs: Any,
    ):
        message = f"Peer {peer_role} disconnected during {phase}"
        super().__init__(message, phase, **kwargs)
        self.peer_role = peer_role


class UploadError(ZebraStreamTransferError):
    """Upload (PUT) operation failed."""
    
    def __init__(
        self,
        message: str,
        bytes_transferred: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(message, phase="upload", bytes_transferred=bytes_transferred, **kwargs)


class DownloadError(ZebraStreamTransferError):
    """Download (GET) operation failed."""
    
    def __init__(
        self,
        message: str,
        bytes_transferred: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(message, phase="download", bytes_transferred=bytes_transferred, **kwargs)


class ProtocolError(ZebraStreamTransferError):
    """Protocol violation (malformed control messages, unexpected responses).
    
    Additional Attributes:
        expected: Expected message/state
        actual: Actual message/state received
    """
    
    def __init__(
        self,
        message: str,
        phase: str,
        expected: str | None = None,
        actual: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(message, phase, **kwargs)
        self.expected = expected
        self.actual = actual


# ============================================================================
# State Exceptions
# ============================================================================


class ZebraStreamStateError(ZebraStreamError):
    """Base for lifecycle/state management errors.
    
    Additional Attributes:
        current_state: Current state of the stream
        operation: Operation that was attempted
    """
    
    def __init__(
        self,
        message: str,
        current_state: str | None = None,
        operation: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(message, **kwargs)
        self.current_state = current_state
        self.operation = operation


class AlreadyStartedError(ZebraStreamStateError):
    """Attempted to start an already-started stream."""
    
    def __init__(
        self,
        **kwargs: Any,
    ):
        super().__init__(
            "Stream already started",
            current_state="started",
            operation="start",
            **kwargs
        )


class NotStartedError(ZebraStreamStateError):
    """Attempted operation on a not-yet-started stream."""
    
    def __init__(
        self,
        operation: str | None = None,
        **kwargs: Any,
    ):
        if operation:
            message = f"Stream not started (attempted: {operation})"
        else:
            message = "Stream not started"
        super().__init__(
            message,
            current_state="not_started",
            operation=operation,
            **kwargs
        )


class StreamClosedError(ZebraStreamStateError):
    """Attempted operation on a closed stream."""
    
    def __init__(
        self,
        operation: str | None = None,
        **kwargs: Any,
    ):
        if operation:
            message = f"Stream closed (attempted: {operation})"
        else:
            message = "Stream closed"
        super().__init__(
            message,
            current_state="closed",
            operation=operation,
            **kwargs
        )

