#!/usr/bin/env python3
import asyncio
import logging
import sys

import anyio
import typer

from zebrastream.io._core import AsyncReader

app = typer.Typer()

@app.command()
def main(
    connect_url: str = typer.Argument(..., help="ZebraStream Connect API URL"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    block_size: int = typer.Option(4096, help="Size of data blocks to read from stdin (default: 4096 bytes)"),
):
    """Read data from stdin and stream it to ZebraStream using AsyncZebraStreamWriter."""
    asyncio.run(async_main(connect_url, access_token, block_size))

async def async_main(connect_url, access_token, block_size):
    try:
        async with (
            AsyncReader(connect_url=connect_url, access_token=access_token) as reader,
            anyio.wrap_file(sys.stdout.buffer) as f
        ):
                while data := await reader.read_exactly(block_size):
                    # print(f"Read {len(data)} bytes from reader")
                    await f.write(data)
    except Exception as e:
        logging.error(f"Broken stream: {e}")

if __name__ == "__main__":
    app()
