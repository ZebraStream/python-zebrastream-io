#!/usr/bin/env python3
import asyncio
import logging
import sys

import anyio
import typer

from zebrastream.io._core import AsyncWriter


app = typer.Typer()

@app.command()
def main(
    connect_url: str = typer.Argument(..., help="ZebraStream Connect API URL"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    content_type: str = typer.Option("text/plain", help="Content-Type for the HTTP request"),
    number_lines: int = typer.Option(0, help="Number of lines to read from stdin (default: 0, read until EOF)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Read lines from stdin and stream them to ZebraStream using AsyncWriter."""
    asyncio.run(async_main(connect_url, access_token, content_type, number_lines, timeout))

async def async_main(url, access_token, content_type, number_lines, timeout):
    try:
        async with (
            AsyncWriter(url, access_token=access_token, content_type=content_type, connect_timeout=timeout) as writer,
            anyio.wrap_file(sys.stdin) as astdin
        ):
            i = 1
            async for line in astdin:
                await writer.write(line.encode())
                if i == number_lines:
                    break
                i += 1
    except Exception as e:
        logging.error(f"Broken stream: {e}")

if __name__ == "__main__":
    app()
