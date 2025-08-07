# zebrastream-io

Python IO interface for ZebraStream data streaming services.

> **Disclaimer:**  
> The code in this package is considered **pre-production quality**. APIs and functionality may change without notice. Use with caution in production environments.

## Features
- File-like synchronous interface for ZebraStream data streams
- Async interface (internal, subject to change)
- Easily extensible for other IO interfaces

## Installation

```bash
pip install zebrastream-io
```

## Usage

### Synchronous file-like interface

The synchronous interface provides a familiar, file-like API for reading from and writing to ZebraStream data streams. This design allows you to interact with remote streams using standard Python file IO, making integration with existing codebases straightforward. The goal is to offer a simple and reliable way to handle streaming data without requiring knowledge of asynchronous programming or custom protocols.

#### Producer

```python
import zebrastream.io.file as zsfile

with zsfile.open(mode="wb", stream_path="/my-stream", access_token=token) as f:
    f.write(b"Hello ZebraStream!")
```

#### Consumer

```python
import zebrastream.io.file as zsfile

with zsfile.open(mode="rb", stream_path="/my-stream", access_token=token) as f:
    data = f.read(1024)  # read 1024 bytes
    # do something with the data
```

### Async interface (internal)

Async interface for performing network operations using the asyncio event loop.

This interface is currently non-public and subject to change, as it is under active development. The primary goal is to provide an internal, robust reference implementation for ZebraStream, leveraging Python's async/await syntax. At present, the implementation exclusively supports execution within the asyncio event loop, as it relies on the `httpio` library — the only request library currently offering reliable, full-duplex communication required for complete ZebraStream protocol support.

Future plans include stabilizing the API and exposing standard async streaming interfaces such as asyncio `StreamReader`/`StreamWriter`.

#### Producer

```python
from zebrastream.io._core import AsyncWriter
import asyncio

async def main():
    async with AsyncWriter(stream_path="/my-stream", access_token=token) as writer:
        await writer.write(b"Hello ZebraStream!")

asyncio.run(main())
```

#### Consumer

```python
from zebrastream.io._core import AsyncReader
import asyncio

async def main():
    async with AsyncReader(stream_path="/my-stream", access_token=token) as reader:
        data = await reader.read(1024)
        # do something with the data

asyncio.run(main())
```

## Documentation
See [ZebraStream documentation](https://help.zebrastream.io/) for more details.

## License
MIT License. See [LICENSE](./LICENSE) for details.

## See also
- [zebrastream-cli](https://github.com/zebrastream/zebrastream-cli): Command-line tools for ZebraStream cloud service
