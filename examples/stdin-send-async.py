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
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
):
    """Read data from stdin and stream it to ZebraStream using AsyncWriter."""
    asyncio.run(async_main(connect_url, access_token, content_type, block_size))

async def async_main(connect_url, access_token, content_type, block_size):
    try:
        async with (
            AsyncWriter(connect_url=connect_url, access_token=access_token, content_type=content_type) as writer,
            anyio.wrap_file(sys.stdin.buffer) as astdin
        ):
                while data := await astdin.read(block_size):
                    # print(f"Read {len(data)} bytes from stdin")
                    await writer.write(data)
    except Exception as e:
        logging.error(f"Broken stream: {e}")

if __name__ == "__main__":
    app()
