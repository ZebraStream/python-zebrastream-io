#!/usr/bin/env python3
"""
ZebraStream Async Stdin Send Example

This script demonstrates how to asynchronously stream data from stdin to a ZebraStream
endpoint using the low-level AsyncWriter class. It reads data in configurable blocks
and streams it in real-time without buffering.

This example showcases:
- Async/await pattern for non-blocking I/O
- Direct use of the AsyncWriter core class
- Configurable block size for performance tuning
- Real-time streaming without intermediate buffering

Usage:
    python stdin-send-async.py <stream_path> --access-token <token> [OPTIONS]

Example:
    echo "Hello World" | python stdin-send-async.py "/my-stream" --access-token "abc123"
    cat large-file.txt | python stdin-send-async.py "/data-stream" --access-token "abc123" --block-size 8192
"""

import asyncio
import logging
import sys

import anyio
import typer

from zebrastream.io._core import AsyncWriter

app = typer.Typer()

@app.command()
def main(
    stream_path: str = typer.Argument(..., help="ZebraStream stream path (e.g., '/my-stream')"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    content_type: str = typer.Option("text/plain", help="Content-Type for the HTTP request"),
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read data from stdin and stream it to ZebraStream using AsyncWriter."""
    asyncio.run(async_main(stream_path, access_token, content_type, block_size, timeout))

async def async_main(stream_path, access_token, content_type, block_size, timeout):
    try:
        async with (
            AsyncWriter(stream_path=stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout) as writer,
            anyio.wrap_file(sys.stdin.buffer) as astdin
        ):
            while data := await astdin.read(block_size):
                await writer.write(data)
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)

if __name__ == "__main__":
    app()
