#!/usr/bin/env python3
"""
ZebraStream Async Receive to Stdout Example

This script demonstrates how to asynchronously receive data from a ZebraStream
endpoint and write it to stdout using the low-level AsyncReader class. It reads
data in configurable blocks and outputs it in real-time.

This example showcases:
- Async/await pattern for non-blocking data consumption
- Direct use of the AsyncReader core class
- Configurable block size for performance tuning
- Real-time data reception without buffering delays

Usage:
    python stdout-receive-async.py <stream_path> --access-token <token> [OPTIONS]

Example:
    python stdout-receive-async.py "/my-stream" --access-token "abc123" > output.txt
    python stdout-receive-async.py "/data-stream" --access-token "abc123" --block-size 8192 | grep "ERROR"
"""

import asyncio
import logging
import sys

import anyio
import typer

from zebrastream.io._core import AsyncReader

app = typer.Typer()

@app.command()
def main(
    stream_path: str = typer.Argument(..., help="ZebraStream stream path (e.g., '/my-stream')"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read data from stdin and stream it to ZebraStream using AsyncZebraStreamWriter."""
    asyncio.run(async_main(stream_path, access_token, block_size, timeout))

async def async_main(stream_path, access_token, block_size, timeout):
    try:
        async with (
            AsyncReader(stream_path=stream_path, access_token=access_token, connect_timeout=timeout) as reader,
            anyio.wrap_file(sys.stdout.buffer) as f
        ):
            while data := await reader.read_variable_block(block_size):
                await f.write(data)
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)

if __name__ == "__main__":
    app()
