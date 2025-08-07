#!/usr/bin/env python3
import itertools
import sys

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
    sync_main(stream_path, access_token, content_type, number_lines, timeout)

def sync_main(stream_path, access_token, content_type, number_lines, timeout):
    with zsfile.open(mode="wb", stream_path=stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout) as f:
        lines_gen = sys.stdin if number_lines <= 0 else itertools.islice(sys.stdin, number_lines)
        for line in lines_gen:
            f.write(line.encode())

if __name__ == "__main__":
    app()
