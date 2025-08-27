#!/usr/bin/env python3
"""
ZebraStream Synchronous Line-by-Line Send Example

This script demonstrates how to stream data from stdin line-by-line to a ZebraStream
endpoint using the synchronous Writer wrapper. It reads lines one at a time and can
limit the number of lines processed, making it ideal for log streaming scenarios.

This example showcases:
- Synchronous line-oriented streaming
- Configurable line count limits
- Simple integration with text processing pipelines
- Writer class usage for sync operations

Usage:
    python stdin-send-lines.py <stream_path> --access-token <token> [OPTIONS]

Example:
    cat log-file.txt | python stdin-send-lines.py "/log-stream" --access-token "abc123"
    tail -f application.log | python stdin-send-lines.py "/live-logs" --access-token "abc123" --number-lines 1000
"""

import itertools
import sys
import logging

import typer

import zebrastream.io.file as zsfile

app = typer.Typer()

@app.command()
def main(
    stream_path: str = typer.Argument(..., help="ZebraStream stream path (e.g., '/my-stream')"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    content_type: str = typer.Option("text/plain", help="Content-Type for the HTTP request"),
    number_lines: int = typer.Option(0, help="Number of lines to read from stdin (default: 0, read until EOF)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read lines from stdin and stream them to ZebraStream using Writer."""
    
    # Force line buffering for immediate line availability
    # sys.stdin.reconfigure(line_buffering=True)

    with zsfile.open(mode="wt", stream_path=stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout) as f:
        lines_gen = sys.stdin if number_lines <= 0 else itertools.islice(sys.stdin, number_lines)
        try:
            for line in lines_gen:
                # sys.stderr.write(f"Sending line: {line}")
                f.write(line)
                f.flush()
        except Exception as e:
            logging.error(f"Broken stream: {e}")
            sys.exit(1)

if __name__ == "__main__":
    app()
