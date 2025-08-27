#!/usr/bin/env python3
"""
ZebraStream Async Line-by-Line Send Example

This script demonstrates how to asynchronously stream data from stdin line-by-line
to a ZebraStream endpoint using the AsyncWriter class. It reads lines one at a time
and can limit the number of lines processed.

This example showcases:
- Line-oriented async streaming
- Configurable line count limits
- Real-time line-by-line processing
- Direct use of AsyncWriter for async operations

Usage:
    python stdin-send-lines-async.py <stream_path> --access-token <token> [OPTIONS]

Example:
    cat log-file.txt | python stdin-send-lines-async.py "/log-stream" --access-token "abc123"
    tail -f application.log | python stdin-send-lines-async.py "/live-logs" --access-token "abc123" --number-lines 100
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
    number_lines: int = typer.Option(0, help="Number of lines to read from stdin (default: 0, read until EOF)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read lines from stdin and stream them to ZebraStream using AsyncWriter."""
    asyncio.run(async_main(stream_path, access_token, content_type, number_lines, timeout))

async def async_main(stream_path, access_token, content_type, number_lines, timeout):
    try:
        async with (
            AsyncWriter(stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout) as writer,
            anyio.wrap_file(sys.stdin) as astdin
        ):
            i = 1
            async for line in astdin:
                await writer.write(line.encode())
                if i == number_lines:
                    break
                i += 1
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)

if __name__ == "__main__":
    app()
