#!/usr/bin/env python3
"""
ZebraStream Gzip Compression Send Example

This script demonstrates how to stream gzip-compressed data from stdin to a ZebraStream
endpoint. It uses the synchronous Writer with Python's GzipFile to compress data
on-the-fly while streaming.

This example showcases:
- Real-time gzip compression during streaming
- Integration with Python's gzip module
- Bandwidth-efficient data transmission
- Synchronous streaming with compression

Usage:
    python stdin-send-gzip.py <stream_path> --access-token <token> [OPTIONS]

Example:
    cat large-log.txt | python stdin-send-gzip.py "/compressed-logs" --access-token "abc123"
    echo "Hello World" | python stdin-send-gzip.py "/my-stream" --access-token "abc123" --content-type "application/gzip"
"""

import sys
import logging
from gzip import GzipFile

import typer

from zebrastream.io.file import Writer

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
    
    try:
        with Writer(stream_path=stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout) as writer:
            with GzipFile(fileobj=writer, mode="wb") as fz:
                while data := sys.stdin.buffer.read(block_size):
                    fz.write(data)
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)

if __name__ == "__main__":
    app()
