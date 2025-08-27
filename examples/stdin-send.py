#!/usr/bin/env python3
"""
ZebraStream Basic Stdin Send Example

This script demonstrates the most basic way to stream data from stdin to a ZebraStream
endpoint using the file-like interface. It reads data in configurable blocks and
streams it efficiently with minimal code complexity.

This example showcases:
- File-like interface for familiar Python I/O patterns
- Configurable block size for performance tuning
- Simple streaming with automatic resource management
- Basic binary mode streaming

Usage:
    python stdin-send.py <stream_path> --access-token <token> [OPTIONS]

Example:
    echo "Hello World" | python stdin-send.py "/my-stream" --access-token "abc123"
    cat document.pdf | python stdin-send.py "/files/document" --access-token "abc123" --content-type "application/pdf"
"""

import sys

import typer

import zebrastream.io.file as zsfile

app = typer.Typer()


@app.command()
def main(
    stream_path: str = typer.Argument(..., help="ZebraStream stream path (e.g., '/my-stream')"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    content_type: str = typer.Option("text/plain", help="Content-Type for the HTTP request"),
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read data from stdin and stream it to ZebraStream using AsyncZebraStreamWriter."""
    with zsfile.open(mode="wb", stream_path=stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout) as f:
        while data := sys.stdin.buffer.read(block_size):
            f.write(data)

if __name__ == "__main__":
    app()
