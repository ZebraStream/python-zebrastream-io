#!/usr/bin/env python3
"""
ZebraStream Line-by-Line Receive Example

This script demonstrates how to receive text data from a ZebraStream endpoint
line-by-line and write it to stdout. It uses text mode for automatic line
iteration, making it ideal for log monitoring and text stream processing.

This example showcases:
- Text mode streaming with automatic line splitting
- Line-by-line processing for real-time text analysis
- Simple integration with text processing pipelines
- File-like interface in text mode

Usage:
    python stdout-receive-lines.py <stream_path> --access-token <token> [OPTIONS]

Example:
    python stdout-receive-lines.py "/log-stream" --access-token "abc123" | grep "ERROR"
    python stdout-receive-lines.py "/live-logs" --access-token "abc123" > processed.log
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
        with zsfile.open(mode="rt", stream_path=stream_path, access_token=access_token, connect_timeout=timeout, block_size=block_size) as f:
            for line in f:
                print(line, end="")
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)

if __name__ == "__main__":
    app()
