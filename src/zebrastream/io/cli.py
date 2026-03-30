# SPDX-License-Identifier: MIT
"""
ZebraStream CLI - Command-line interface for ZebraStream streaming.

This module provides a thin wrapper around the zebrastream-io SDK for streaming
binary data between Unix pipelines and ZebraStream streams.
"""

import logging
import subprocess
import sys
from dataclasses import dataclass
from enum import StrEnum
from typing import Optional

import typer
from typing_extensions import Annotated

# Import SDK components
import zebrastream.io.file as zsfile
from zebrastream.io._exceptions import ( 
    AuthenticationError,
    ConnectionFailedError,
    ConnectionTimeoutError,
    DownloadError,
    PeerDisconnectedError,
    ProtocolError,
    UploadError,
    ZebraStreamError,
)

# Exit codes as per spec
EXIT_SUCCESS = 0
EXIT_GENERIC_ERROR = 1
EXIT_USAGE_ERROR = 2
EXIT_AUTH_ERROR = 3
EXIT_CONNECT_TIMEOUT = 4
EXIT_PEER_DISCONNECTED = 5
EXIT_PRODUCER_FAILED = 6
EXIT_CONSUMER_FAILED = 7

# Chunk size for streaming (64KB, matches SDK AGE_CHUNK_SIZE)
CHUNK_SIZE = 65536


class StreamMode(StrEnum):
    READ = "read"
    WRITE = "write"

# Initialize Typer app
app = typer.Typer(
    name="zebrastream",
    help="Stream data between Unix pipelines and ZebraStream streams",
    add_completion=False,
    invoke_without_command=True,
)

# Global logger
logger = logging.getLogger("zebrastream")


@dataclass
class GlobalOptions:
    """Container for global CLI options."""

    config_name: Optional[str]
    config_file: Optional[str]
    log_level: str


# Shared option definitions to avoid duplication between read/write commands
StreamPathOption = Annotated[
    Optional[str],
    typer.Option("-s", "--stream-path", help="Stream path (e.g., /org/team/feed). Optional if specified in config."),
]

ConnectUrlOption = Annotated[
    Optional[str],
    typer.Option("--connect-url", help="Override connect API URL"),
]

ConnectTimeoutOption = Annotated[
    Optional[int],
    typer.Option("--connect-timeout", help="Connect timeout in seconds"),
]

AccessTokenOption = Annotated[
    Optional[str],
    typer.Option("--access-token", help="Access token for authentication"),
]

PassphraseOption = Annotated[
    Optional[str],
    typer.Option("--passphrase", help="Passphrase for symmetric encryption"),
]


@dataclass
class StreamArgs:
    """CLI arguments that configure a stream connection."""

    stream_path: Optional[str] = None
    connect_url: Optional[str] = None
    connect_timeout: Optional[int] = None
    access_token: Optional[str] = None
    passphrase: Optional[str] = None
    content_type: Optional[str] = None  # write-only


class StderrFormatter(logging.Formatter):
    """Simple formatter for CLI stderr output."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as [LEVEL] message."""
        return f"[{record.levelname}] {record.getMessage()}"


def setup_logging(log_level: str) -> None:
    """Configure logging to stderr with the specified level."""
    level_map = {
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }
    
    level = level_map.get(log_level.lower(), logging.ERROR)
    
    # Configure the zebrastream logger
    logger.setLevel(level)
    
    # Remove any existing handlers
    logger.handlers.clear()
    
    # Add stderr handler with custom formatter
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(StderrFormatter())
    logger.addHandler(handler)
    
    # Don't propagate to root logger
    logger.propagate = False


def load_config(config_name: Optional[str], config_file: Optional[str]) -> dict:
    """Load configuration from named config or explicit file using confuse.
    
    Automatically maps ZEBRASTREAM_* environment variables to config keys.
    Environment variables take precedence over config file values.
    For example, ZEBRASTREAM_ACCESS_TOKEN maps to config['access_token'].
    
    Args:
        config_name: Name of the config file (without extension) in ~/.config/zebrastream/streams/
        config_file: Explicit path to a config file (takes precedence over config_name)
        
    Returns:
        Dictionary of configuration values (includes environment variables)
    """
    try:
        import confuse
        from pathlib import Path
        
        # Create configuration with XDG support
        config = confuse.Configuration("zebrastream", modname=__name__)
        
        # Load from file first if specified
        if config_file:
            # Explicit file path
            config_path = Path(config_file)
            if not config_path.exists():
                logger.error(f"Config file not found: {config_file}")
                sys.exit(EXIT_GENERIC_ERROR)
            config.set_file(str(config_path))
        elif config_name:
            # Named config from standard location
            stream_config_path = Path(config.config_dir()) / "streams" / f"{config_name}.yaml"
            if stream_config_path.exists():
                config.set_file(str(stream_config_path))
            else:
                # Show user-friendly message with ~ instead of absolute path
                home = str(Path.home())
                display_path = str(stream_config_path).replace(home, "~", 1)
                logger.warning(f"Config '{config_name}' not found. Expected location: {display_path}")
        
        # Enable automatic environment variable mapping AFTER loading file
        # This ensures env vars override config file values (correct precedence)
        # ZEBRASTREAM_ACCESS_TOKEN -> access_token, ZEBRASTREAM_PASSPHRASE -> passphrase, etc.
        config.set_env()
        
        return config.flatten()
            
    except ImportError:
        logger.error("confuse library not installed. Install with: pip install zebrastream-io[cli]")
        sys.exit(EXIT_GENERIC_ERROR)
    except Exception as e:
        source = config_file if config_file else config_name
        logger.error(f"Failed to load config '{source}': {e}")
        sys.exit(EXIT_GENERIC_ERROR)


def _resolve_with_warning(
    cli_value: Optional[str],
    config: dict,
    config_key: str,
    cli_flag: str,
    env_var: str,
) -> Optional[str]:
    """Resolve a parameter from CLI or config, with security warning for CLI usage.
    
    Args:
        cli_value: Value from CLI flag (if provided)
        config: Configuration dictionary (includes environment variables)
        config_key: Key to look up in config
        cli_flag: Name of CLI flag (for warning message)
        env_var: Name of environment variable (for warning message)
        
    Returns:
        Resolved value or None
    """
    if cli_value:
        logger.warning(
            f"Using {cli_flag} is insecure on shared systems. "
            f"Use {env_var} environment variable or config file instead."
        )
    return cli_value or config.get(config_key)


def map_exception_to_exit_code(exc: Exception) -> int:
    """Map SDK exception to CLI exit code.
    
    Args:
        exc: Exception from SDK
        
    Returns:
        Exit code integer
    """
    if isinstance(exc, AuthenticationError):
        return EXIT_AUTH_ERROR
    if isinstance(exc, ConnectionTimeoutError):
        return EXIT_CONNECT_TIMEOUT
    if isinstance(exc, PeerDisconnectedError):
        return EXIT_PEER_DISCONNECTED
    if isinstance(exc, (ConnectionFailedError, UploadError, DownloadError, ProtocolError, ZebraStreamError)):
        return EXIT_GENERIC_ERROR
    return EXIT_GENERIC_ERROR


@app.callback()
def global_options(
    ctx: typer.Context,
    config_name: Annotated[
        Optional[str],
        typer.Option("--config-name", help="Named configuration from ~/.config/zebrastream/streams/NAME.yaml"),
    ] = None,
    config_file: Annotated[
        Optional[str],
        typer.Option("--config-file", help="Explicit path to a config file (takes precedence over --config-name)"),
    ] = None,
    log_level: Annotated[
        str,
        typer.Option("--log-level", help="Log level: error, warn, info, debug"),
    ] = "error",
) -> None:
    """Global options for all zebrastream commands."""
    setup_logging(log_level)
    ctx.obj = GlobalOptions(
        config_name=config_name,
        config_file=config_file,
        log_level=log_level,
    )
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


def prepare_stream_kwargs(
    args: StreamArgs,
    global_opts: GlobalOptions,
    mode: StreamMode,
) -> tuple[str, dict]:
    """Prepare kwargs for SDK stream open call.
    
    Args:
        args: Stream connection arguments from CLI
        global_opts: Global CLI options (config name/file, log level)
        mode: The subcommand being run (read or write)
        
    Returns:
        Tuple of (resolved_stream_path, kwargs_dict) for zsfile.open()
    """
    # Load configuration (config_file takes precedence over config_name)
    config = load_config(global_opts.config_name, global_opts.config_file)
    
    # If a config is in use, its mode must be present and match the subcommand.
    # This prevents accidentally using a write config with 'read' or vice versa.
    if global_opts.config_name or global_opts.config_file:
        config_mode = config.get("mode")
        if not config_mode:
            source = f"--config-file {global_opts.config_file}" if global_opts.config_file else f"--config-name {global_opts.config_name}"
            logger.error(
                f"Config ({source}) is missing required 'mode' field. "
                f"Add 'mode: {mode}' to the config file."
            )
            sys.exit(EXIT_USAGE_ERROR)
        if config_mode != mode:
            logger.error(
                f"Subcommand '{mode}' conflicts with config mode '{config_mode}'. "
                f"Use 'zebrastream {config_mode}' to match the config."
            )
            sys.exit(EXIT_USAGE_ERROR)
    
    # Resolve stream path: CLI argument takes precedence over config
    resolved_stream_path = args.stream_path or config.get("stream_path")
    if not resolved_stream_path:
        logger.error(
            "No stream path provided. Specify as argument or in config file with 'stream_path' field."
        )
        sys.exit(EXIT_USAGE_ERROR)
    
    # Resolve access token (with security warning if provided via CLI)
    resolved_token = _resolve_with_warning(
        args.access_token, config, "access_token", "--access-token", "ZEBRASTREAM_ACCESS_TOKEN"
    )
    if not resolved_token:
        logger.error(
            "No access token provided. Set ZEBRASTREAM_ACCESS_TOKEN environment variable "
            "or use --config-name/--config-file with a config file containing access_token."
        )
        sys.exit(EXIT_AUTH_ERROR)
    
    # Build SDK kwargs with CLI args taking precedence over config
    kwargs = {
        "stream_path": resolved_stream_path,
        "access_token": resolved_token,
    }
    
    # Optional parameters: CLI arg > config > omit
    # Only add to kwargs if there's an actual value (don't pass None to SDK)
    
    if value := (args.connect_url or config.get("connect_url")):
        kwargs["connect_api_url"] = value

    # connect_timeout=0 is falsy but valid, so use explicit None check
    timeout = args.connect_timeout if args.connect_timeout is not None else config.get("connect_timeout")
    if timeout is not None:
        kwargs["connect_timeout"] = timeout
    
    if resolved_passphrase := _resolve_with_warning(
        args.passphrase, config, "passphrase", "--passphrase", "ZEBRASTREAM_PASSPHRASE"
    ):
        kwargs["passphrase"] = resolved_passphrase
    
    if value := (args.content_type or config.get("content_type")):
        kwargs["content_type"] = value
    
    return resolved_stream_path, kwargs


def stream_stdin_to_writer(writer) -> None:
    """Stream data from stdin to the writer.
    
    Args:
        writer: ZebraStream writer object (binary mode)
    """
    stdin_binary = sys.stdin.buffer
    
    try:
        while chunk := stdin_binary.read(CHUNK_SIZE):
            writer.write(chunk)
    except BrokenPipeError:
        # Consumer disconnected - let SDK handle it
        logger.debug("stdin pipe broken")
        raise
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        raise


def stream_subprocess_to_writer(writer, command: list[str]) -> int:
    """Stream data from subprocess stdout to the writer.
    
    Args:
        writer: ZebraStream writer object (binary mode)
        command: Command and arguments to execute
        
    Returns:
        Exit code of subprocess
        
    Raises:
        Exception: If subprocess fails or stream fails
    """
    logger.debug(f"Starting producer subprocess: {command}")
    
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=None,  # Inherit stderr from CLI
        stdin=None,  # Inherit stdin from CLI
        shell=False,
        bufsize=0,  # Unbuffered for binary streams
    )
    
    try:
        # Stream subprocess stdout to writer
        while chunk := process.stdout.read(CHUNK_SIZE):
            writer.write(chunk)
        
        # Wait for subprocess to complete
        returncode = process.wait()
        
        if returncode != 0:
            logger.error(f"Producer subprocess exited with code {returncode}")
            raise RuntimeError(f"Producer subprocess failed with exit code {returncode}")
        
        return returncode
        
    except Exception as e:
        # Kill subprocess if stream fails
        logger.debug(f"Terminating subprocess due to error: {e}")
        process.terminate()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise


@app.command()
def write(
    ctx: typer.Context,
    stream_path: StreamPathOption = None,
    connect_url: ConnectUrlOption = None,
    connect_timeout: ConnectTimeoutOption = None,
    access_token: AccessTokenOption = None,
    passphrase: PassphraseOption = None,
    content_type: Annotated[
        Optional[str],
        typer.Option("--content-type", help="Content-Type HTTP header for the stream"),
    ] = None,
    producer_cmd: Annotated[
        Optional[list[str]],
        typer.Argument(help="Producer command and arguments (use -- before command)"),
    ] = None,
) -> None:
    """Send data to a ZebraStream stream.

    Reads from stdin by default, or from a producer command if specified after '--'.

    Examples:
        zebrastream write -s /my-stream < data.txt
        zebrastream write -s /my-stream -- pg_dump mydb
        zebrastream write -- sh -c "cat file.txt | gzip"  # stream_path from config
        zebrastream --config-name myconfig write  # if config has stream_path
    """
    global_opts: GlobalOptions = ctx.obj
    
    # producer_cmd is None or empty list for stdin mode, populated list for producer mode
    if producer_cmd is None:
        producer_cmd = []
    
    # Prepare SDK kwargs using global options (resolves stream_path from config if needed)
    resolved_stream_path, kwargs = prepare_stream_kwargs(
        StreamArgs(stream_path, connect_url, connect_timeout, access_token, passphrase, content_type),
        global_opts,
        StreamMode.WRITE,
    )

    logger.debug(f"Stream path: {resolved_stream_path}, Producer command: {producer_cmd}")
    logger.info(f"Opening stream for writing: {resolved_stream_path}")

    try:
        # Open stream for writing (binary mode)
        with zsfile.open(mode="wb", **kwargs) as writer:
            logger.info("Stream connected successfully")

            if producer_cmd:
                # Stream from subprocess
                logger.debug(f"Using producer command: {producer_cmd}")
                stream_subprocess_to_writer(writer, producer_cmd)
            else:
                # Stream from stdin
                logger.debug("Reading from stdin")
                stream_stdin_to_writer(writer)

            # Implicit flush on context exit
            logger.info("Data transfer completed successfully")

    except RuntimeError as e:
        # Producer subprocess failed
        if "Producer subprocess failed" in str(e):
            logger.error(str(e))
            sys.exit(EXIT_PRODUCER_FAILED)
        else:
            logger.error(f"Write failed: {e}")
            sys.exit(EXIT_GENERIC_ERROR)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(EXIT_GENERIC_ERROR)
    except Exception as e:
        exit_code = map_exception_to_exit_code(e)
        logger.error(f"Write failed: {e}")
        sys.exit(exit_code)


def stream_reader_to_subprocess(reader, command: list[str]) -> int:
    """Stream data from reader to subprocess stdin.

    Args:
        reader: ZebraStream reader object (binary mode)
        command: Command and arguments to execute

    Returns:
        Exit code of subprocess

    Raises:
        Exception: If subprocess fails or stream fails
    """
    logger.debug(f"Starting consumer subprocess: {command}")

    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=None,   # Inherit stdout from CLI
        stderr=None,   # Inherit stderr from CLI
        shell=False,
        bufsize=0,     # Unbuffered for binary streams
    )

    try:
        while chunk := reader.read(CHUNK_SIZE):
            try:
                process.stdin.write(chunk)
            except BrokenPipeError:
                logger.debug("Consumer subprocess stdin pipe broken")
                break

        process.stdin.close()
        returncode = process.wait()

        if returncode != 0:
            logger.error(f"Consumer subprocess exited with code {returncode}")
            raise RuntimeError(f"Consumer subprocess failed with exit code {returncode}")

        return returncode

    except Exception as e:
        logger.debug(f"Terminating consumer subprocess due to error: {e}")
        process.terminate()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise


@app.command()
def read(
    ctx: typer.Context,
    stream_path: StreamPathOption = None,
    connect_url: ConnectUrlOption = None,
    connect_timeout: ConnectTimeoutOption = None,
    access_token: AccessTokenOption = None,
    passphrase: PassphraseOption = None,
    consumer_cmd: Annotated[
        Optional[list[str]],
        typer.Argument(help="Consumer command and arguments (use -- before command)"),
    ] = None,
) -> None:
    """Receive data from a ZebraStream stream.

    Writes to stdout by default, or pipes into a consumer command if specified after '--'.

    Examples:
        zebrastream read -s /my-stream > output.txt
        zebrastream read --stream-path /my-stream | tar -xz
        zebrastream read -s /my-stream -- tar -xz
        zebrastream --config-name myconfig read -- python process.py
    """
    global_opts: GlobalOptions = ctx.obj

    if consumer_cmd is None:
        consumer_cmd = []

    # Prepare SDK kwargs using global options (resolves stream_path from config if needed)
    resolved_stream_path, kwargs = prepare_stream_kwargs(
        StreamArgs(stream_path, connect_url, connect_timeout, access_token, passphrase),
        global_opts,
        StreamMode.READ,
    )

    logger.debug(f"Stream path: {resolved_stream_path}, Consumer command: {consumer_cmd}")
    logger.info(f"Opening stream for reading: {resolved_stream_path}")

    try:
        # Open stream for reading (binary mode)
        with zsfile.open(mode="rb", **kwargs) as reader:
            logger.info("Stream connected successfully")

            if consumer_cmd:
                # Pipe stream data into subprocess stdin
                logger.debug(f"Using consumer command: {consumer_cmd}")
                stream_reader_to_subprocess(reader, consumer_cmd)
            else:
                # Write stream data to stdout
                logger.debug("Writing to stdout")
                stdout_binary = sys.stdout.buffer
                while chunk := reader.read(CHUNK_SIZE):
                    try:
                        stdout_binary.write(chunk)
                    except BrokenPipeError:
                        logger.debug("stdout pipe broken, stopping read")
                        break

            logger.info("Data transfer completed successfully")

    except RuntimeError as e:
        if "Consumer subprocess failed" in str(e):
            logger.error(str(e))
            sys.exit(EXIT_CONSUMER_FAILED)
        else:
            logger.error(f"Read failed: {e}")
            sys.exit(EXIT_GENERIC_ERROR)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(EXIT_GENERIC_ERROR)
    except Exception as e:
        exit_code = map_exception_to_exit_code(e)
        logger.error(f"Read failed: {e}")
        sys.exit(exit_code)


def main() -> None:
    """Main entry point for the CLI."""
    try:
        app()
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(EXIT_GENERIC_ERROR)


if __name__ == "__main__":
    main()
