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
import time

with zsfile.open(mode="w", stream_path="/my-stream", access_token=token) as f:
    f.write("Hello!")
    f.flush()  # force send buffer
    time.sleep(10)
    f.write("This is ZebraStream")
```

#### Consumer

```python
import zebrastream.io.file as zsfile

with zsfile.open(mode="r", stream_path="/my-stream", access_token=token) as f:
    for line in f:
        print(line, end="")
```

### End-to-End Encryption

> **⚠️ Experimental:** End-to-end encryption support is currently experimental and subject to change.

ZebraStream supports passphrase-based end-to-end encryption using an encryption scheme derived from [age](https://age-encryption.org/), a simple and secure file encryption format. When encryption is enabled, data is encrypted on the sender side before transmission and can only be decrypted by receivers with the correct passphrase. Follow the general security descriptions of the age project.

```python
import zebrastream.io.file as zsfile
import time

# Producer - encrypt data before sending
with zsfile.open(mode="w", stream_path="/my-stream", 
                 access_token=token, 
                 passphrase="secret") as f:
    f.write("This is")
    f.flush()
    time.sleep(10)
    f.write("encrypted data")

# Consumer - decrypt data after receiving
with zsfile.open(mode="r", stream_path="/my-stream", 
                 access_token=token,
                 passphrase="secret") as f:
    for line in f:
        print(line)
```

### Async interface (unstable)

Async interface for performing network operations using the asyncio event loop.

This interface is currently non-public and subject to change, as it is under active development. The primary goal is to provide an internal, robust reference implementation for ZebraStream, leveraging Python's async/await syntax. At present, the implementation exclusively supports execution within the asyncio event loop, as it relies on the `httpio` library — the only request library currently offering reliable, full-duplex communication required for complete ZebraStream protocol support.

Future plans include stabilizing the API and exposing standard async streaming interfaces such as asyncio `StreamReader`/`StreamWriter`.

#### Producer

```python
from zebrastream.io._core import AsyncWriter
import asyncio

async def main():
    async with AsyncWriter(stream_path="/my-stream", access_token=token) as writer:
        await writer.write(b"Hello!")
        await writer.flush()
        await asyncio.sleep(10)
        await writer.write("This is ZebraStream")

asyncio.run(main())
```

#### Consumer

```python
from zebrastream.io._core import AsyncReader
import asyncio

async def main():
    async with AsyncReader(stream_path="/my-stream", access_token=token) as reader:
        while data := await reader.read_variable_block(4096):
            print(data.decode(), end="")

asyncio.run(main())
```

### Command-Line Interface

A `zebrastream` CLI is included as an optional extra for streaming data between Unix pipelines and ZebraStream streams. The CLI serves as a **reference implementation** showcasing the Python SDK's capabilities, with a focus on **correctness and protocol compliance** rather than maximum performance.

#### Installation

```bash
pip install zebrastream-io[cli]
```

#### Usage

The CLI provides `write` and `read` subcommands with global options:

```bash
# Write from stdin
echo "Hello ZebraStream" | zebrastream write -s /my-stream

# Write from a producer command
zebrastream write -s /my-stream -- pg_dump mydb
zebrastream write --stream-path /my-stream -- sh -c "cat data.txt | gzip"

# Read to stdout
zebrastream read -s /my-stream > output.txt
zebrastream read --stream-path /my-stream | tar -xz

# Pipe into a consumer command
zebrastream read -s /my-stream -- tar -xz
zebrastream read -s /my-stream -- python process.py

# Global options (--log-level, --config-name, --config-file) come before subcommand
zebrastream --log-level info write -s /my-stream --connect-timeout 30 < data.bin
zebrastream --config-name production read -s /my-stream | jq .

# Stream path can come from config
zebrastream --config-name production write < data.txt

# Using explicit config file path
zebrastream --config-file ~/my-zebrastream-config.yaml write -s /my-stream

# Using environment variable for authentication
ZEBRASTREAM_ACCESS_TOKEN='your_token_here' zebrastream write -s /my-stream < file.txt
```

**Configuration Files:** Named configuration files should use the `.yaml` extension and be stored in `~/.config/zebrastream/streams/` for reusable settings. You can also specify an explicit file path with `--config-file`. When both are provided, `--config-file` takes precedence.

Config files must include a `mode` field (`read` or `write`) that matches the subcommand used — this prevents accidentally using a write config with `read` or vice versa. If `stream_path` is included, the `-s/--stream-path` CLI option can be omitted.

```yaml
# ~/.config/zebrastream/streams/my-feed.yaml
# Use with: zebrastream --config-name my-feed read

# Required: must match the subcommand (read or write)
mode: read

# Stream path (optional if provided on command line)
stream_path: /userspace/project/my-stream

# Access token — prefer ZEBRASTREAM_ACCESS_TOKEN env var to keep it out of the file
access_token: YOUR_ACCESS_TOKEN

# Passphrase for symmetric end-to-end encryption (optional)
# Both sender and receiver must use the same passphrase
# Prefer ZEBRASTREAM_PASSPHRASE env var
# passphrase: your-secret-passphrase

# Content-Type header (optional, write mode only)
# content_type: application/octet-stream

# Override connect API URL (optional, defaults to ZebraStream cloud)
# connect_url: https://connect.zebrastream.io/v0/

# Connection timeout in seconds (optional, default: no timeout)
# connect_timeout: 30
```

For more details on configuration, authentication, and advanced options:

```bash
zebrastream --help
zebrastream write --help
zebrastream read --help
```

## Documentation
See [ZebraStream documentation](https://help.zebrastream.io/) for more details.

## License
MIT License. See [LICENSE](./LICENSE) for details.

## See also
- [zebrastream-cli](https://github.com/zebrastream/zebrastream-cli): Command-line tools for ZebraStream cloud service
