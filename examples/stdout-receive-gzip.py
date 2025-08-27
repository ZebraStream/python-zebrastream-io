#!/usr/bin/env python3
"""
ZebraStream Gzip Decompression Receive Example

This script demonstrates how to receive gzip-compressed data from a ZebraStream
endpoint and decompress it on-the-fly while writing to stdout. It uses the 
synchronous Reader with Python's GzipFile to handle compressed streams.

This example showcases:
- Real-time gzip decompression during streaming
- Integration with Python's gzip module
- Bandwidth-efficient data reception
- Transparent decompression for compressed streams

Usage:
    python stdout-receive-gzip.py <stream_path> --access-token <token> [OPTIONS]

Example:
    python stdout-receive-gzip.py "/compressed-logs" --access-token "abc123" > decompressed.txt
    python stdout-receive-gzip.py "/my-stream" --access-token "abc123" | tail -f
"""

import logging
import sys
from gzip import GzipFile

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
        with zsfile.open(mode="rb", stream_path=stream_path, access_token=access_token, connect_timeout=timeout) as reader:
            with GzipFile(fileobj=reader, mode="rb") as fz:
                while data := fz.read(block_size):
                    sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)

if __name__ == "__main__":
    app()
