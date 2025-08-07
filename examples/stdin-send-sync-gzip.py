#!/usr/bin/env python3
import sys
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
    with GzipFile(fileobj=Writer(stream_path=stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout), mode="wb") as fz:
        while data := sys.stdin.buffer.read(block_size):
            print(f"Read {len(data)} bytes from stdin")
            fz.write(data)
        fz.flush()  # needed?

if __name__ == "__main__":
    app()
