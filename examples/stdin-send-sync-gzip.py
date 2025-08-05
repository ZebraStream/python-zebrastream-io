#!/usr/bin/env python3
import sys
from gzip import GzipFile

import typer

from zebrastream.io.file import Writer

app = typer.Typer()

@app.command()
def main(
    connect_url: str = typer.Argument(..., help="ZebraStream Connect API URL"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    content_type: str = typer.Option("text/plain", help="Content-Type for the HTTP request"),
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
):
    """Read data from stdin and stream it to ZebraStream using AsyncWriter."""
    sync_main(connect_url, access_token, content_type, block_size)

def sync_main(connect_url, access_token, content_type, block_size):
    with GzipFile(fileobj=Writer(connect_url=connect_url, access_token=access_token, content_type=content_type), mode="wb") as fz:
        while data := sys.stdin.buffer.read(block_size):
            print(f"Read {len(data)} bytes from stdin")
            fz.write(data)
        fz.flush()  # needed?

if __name__ == "__main__":
    app()
