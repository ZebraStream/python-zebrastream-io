#!/usr/bin/env python3
import sys

import typer

import zebrastream.io.file as zsfile

app = typer.Typer()


@app.command()
def main(
    connect_url: str = typer.Argument(..., help="ZebraStream Connect API URL"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read data from ZebraStream and write it to stdout."""
    with zsfile.open(mode="rb", connect_url=connect_url, access_token=access_token, connect_timeout=timeout) as f:
        while data := f.read(block_size):
            sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()

if __name__ == "__main__":
    app()
