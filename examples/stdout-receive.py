#!/usr/bin/env python3
"""
ZebraStream Basic Stdout Receive Example

This script demonstrates the most basic way to receive data from a ZebraStream
endpoint and write it to stdout using the file-like interface. It reads data
in configurable blocks and outputs it efficiently with minimal code complexity.

This example showcases:
- File-like interface for familiar Python I/O patterns
- Configurable block size for performance tuning
- Simple streaming with automatic resource management
- Basic binary mode reception

Usage:
    python stdout-receive.py <stream_path> --access-token <token> [OPTIONS]

Example:
    python stdout-receive.py "/my-stream" --access-token "abc123" > output.txt
    python stdout-receive.py "/files/document" --access-token "abc123" > document.pdf
"""

import logging
import sys

import typer

import zebrastream.io.file as zsfile

app = typer.Typer()


@app.command()
def main(
    stream_path: str = typer.Argument(..., help="ZebraStream stream path (e.g., '/my-stream')"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read data from ZebraStream and write it to stdout."""
    
    try:
        with zsfile.open(mode="rb", stream_path=stream_path, access_token=access_token, connect_timeout=timeout) as f:
            while data := f.read(block_size):
                sys.stdout.buffer.write(data)
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)

if __name__ == "__main__":
    app()
