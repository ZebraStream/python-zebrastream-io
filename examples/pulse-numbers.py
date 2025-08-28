#!/usr/bin/env python3
"""
ZebraStream Pulse Numbers Example

This script demonstrates how to stream sequential numbered events to a ZebraStream
endpoint with configurable delays between events. It uses the zebrastream.io.file
module to write events to a stream in real-time.

The script sends events in the format "Event {number}" where the number increments
from 0. It can run indefinitely or for a specified number of events.

Usage:
    python pulse-numbers.py <stream_path> --access-token <token> [OPTIONS]

Example:
    python pulse-numbers.py "/my-stream" --access-token "abc123" --delay-seconds 1.0 --number-events 10
"""

import itertools
import logging
import sys
import time

import typer

import zebrastream.io.file as zsfile

app = typer.Typer()

@app.command()
def main(
    stream_path: str = typer.Argument(..., help="ZebraStream stream path (e.g., '/my-stream')"),
    access_token: str = typer.Option(..., help="Access token for Authorization header"),
    content_type: str = typer.Option("text/plain", help="Content-Type for the HTTP request"),
    number_events: int = typer.Option(0, help="Number of events to pulse (default: 0, endless)"),
    delay_seconds: float = typer.Option(2.0, help="Delay between events in seconds (default: 2.0)"),
    timeout: int = typer.Option(None, help="Connect timeout in seconds (default: None)"),
):
    """Stream events with delay to ZebraStream using Writer."""

    try:
        with zsfile.open(mode="wt", stream_path=stream_path, access_token=access_token, content_type=content_type, connect_timeout=timeout) as f:
            
            counter = itertools.islice(itertools.count(), number_events) if number_events > 0 else itertools.count()
            
            for i in counter:
                sys.stderr.write(f"Sending event: {i}\n")
                f.write(f"Event {i}\n")
                f.flush()
                time.sleep(delay_seconds)
    except Exception as e:
        logging.error(f"Broken stream: {e}")
        sys.exit(1)
        
if __name__ == "__main__":
    app()
