# SPDX-License-Identifier: MIT
"""
Basic tests for the ZebraStream CLI.

These tests verify basic CLI functionality without requiring network access.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest


def test_cli_module_imports():
    """Test that CLI module can be imported."""
    from zebrastream.io import cli
    
    assert cli.app is not None
    assert cli.CHUNK_SIZE == 65536


def test_exit_codes_defined():
    """Test that all exit codes are defined as per spec."""
    from zebrastream.io import cli
    
    assert cli.EXIT_SUCCESS == 0
    assert cli.EXIT_GENERIC_ERROR == 1
    assert cli.EXIT_USAGE_ERROR == 2
    assert cli.EXIT_AUTH_ERROR == 3
    assert cli.EXIT_CONNECT_TIMEOUT == 4
    assert cli.EXIT_PEER_DISCONNECTED == 5
    assert cli.EXIT_PRODUCER_FAILED == 6
    assert cli.EXIT_CONSUMER_FAILED == 7


def test_exception_mapping():
    """Test that exceptions map to correct exit codes."""
    from zebrastream.io import cli
    from zebrastream.io._exceptions import (
        AuthenticationError,
        ConnectionTimeoutError,
        PeerDisconnectedError,
        UploadError,
    )
    
    # Test specific mappings
    assert cli.map_exception_to_exit_code(
        AuthenticationError(status_code=401)
    ) == cli.EXIT_AUTH_ERROR
    
    assert cli.map_exception_to_exit_code(
        ConnectionTimeoutError(timeout_seconds=10)
    ) == cli.EXIT_CONNECT_TIMEOUT
    
    assert cli.map_exception_to_exit_code(
        PeerDisconnectedError(peer_role="reader", phase="upload")
    ) == cli.EXIT_PEER_DISCONNECTED
    
    assert cli.map_exception_to_exit_code(
        UploadError("test error")
    ) == cli.EXIT_GENERIC_ERROR


def test_resolve_access_token_precedence():
    """Test access token resolution follows correct precedence.
    
    Note: Environment variables are now handled by load_config() via confuse.set_env(),
    not by _resolve_with_warning(). This test verifies CLI vs config precedence.
    """
    from zebrastream.io import cli
    
    # CLI token takes precedence over config
    config = {"access_token": "config_token"}
    result = cli._resolve_with_warning(
        "cli_token", config, "access_token", "--access-token", "ZEBRASTREAM_ACCESS_TOKEN"
    )
    assert result == "cli_token"
    
    # Config token used when no CLI token provided
    config = {"access_token": "config_token"}
    result = cli._resolve_with_warning(
        None, config, "access_token", "--access-token", "ZEBRASTREAM_ACCESS_TOKEN"
    )
    assert result == "config_token"
    
    # None when nothing provided
    result = cli._resolve_with_warning(
        None, {}, "access_token", "--access-token", "ZEBRASTREAM_ACCESS_TOKEN"
    )
    assert result is None


def test_resolve_passphrase_precedence():
    """Test passphrase resolution follows correct precedence.
    
    Note: Environment variables are now handled by load_config() via confuse.set_env(),
    not by _resolve_with_warning(). This test verifies CLI vs config precedence.
    """
    from zebrastream.io import cli
    
    # CLI passphrase takes precedence over config
    config = {"passphrase": "config_pass"}
    result = cli._resolve_with_warning(
        "cli_pass", config, "passphrase", "--passphrase", "ZEBRASTREAM_PASSPHRASE"
    )
    assert result == "cli_pass"
    
    # Config passphrase used when no CLI passphrase provided
    config = {"passphrase": "config_pass"}
    result = cli._resolve_with_warning(
        None, config, "passphrase", "--passphrase", "ZEBRASTREAM_PASSPHRASE"
    )
    assert result == "config_pass"
    
    # None when nothing provided (encryption is optional)
    result = cli._resolve_with_warning(
        None, {}, "passphrase", "--passphrase", "ZEBRASTREAM_PASSPHRASE"
    )
    assert result is None


def test_setup_logging():
    """Test logging configuration."""
    from zebrastream.io import cli
    
    # Test different log levels
    cli.setup_logging("error")
    assert cli.logger.level == 40  # ERROR
    
    cli.setup_logging("warn")
    assert cli.logger.level == 30  # WARNING
    
    cli.setup_logging("info")
    assert cli.logger.level == 20  # INFO
    
    cli.setup_logging("debug")
    assert cli.logger.level == 10  # DEBUG


def test_load_config_missing_file():
    """Test config loading with non-existent file."""
    from zebrastream.io import cli
    import logging
    from io import StringIO
    
    # Capture log output
    log_capture = StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.WARNING)
    cli.logger.addHandler(handler)
    cli.logger.setLevel(logging.WARNING)
    
    try:
        # Should return empty dict for missing config
        config = cli.load_config("this_config_does_not_exist_12345", None)
        assert config == {}
        
        # Check that warning message is user-friendly
        log_output = log_capture.getvalue()
        assert "this_config_does_not_exist_12345" in log_output
        assert "Expected location:" in log_output
        assert "~/.config/zebrastream/streams/" in log_output or ".config/zebrastream/streams/" in log_output
    finally:
        cli.logger.removeHandler(handler)


def test_load_config_none():
    """Test that load_config returns empty dict for None."""
    from zebrastream.io import cli
    
    # Should return empty dict when no config name provided
    config = cli.load_config(None, None)
    assert config == {}


def test_config_file_precedence():
    """Test that config_file takes precedence over config_name."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path
    
    # Create a temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("access_token: from_file\n")
        f.write("stream_path: /file-stream\n")
        temp_file = f.name
    
    try:
        # Load from explicit file path
        config = cli.load_config(None, temp_file)
        assert config.get("access_token") == "from_file"
        assert config.get("stream_path") == "/file-stream"
    finally:
        # Clean up
        Path(temp_file).unlink(missing_ok=True)


def test_config_with_mode():
    """Test that config mode must match the subcommand."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path

    # Matching mode passes validation
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /my-stream\n")
        f.write("access_token: test_token\n")
        temp_file = f.name

    try:
        config = cli.load_config(None, temp_file)
        assert config.get("mode") == "read"
        assert config.get("stream_path") == "/my-stream"

        global_opts = cli.GlobalOptions(config_name=None, config_file=temp_file, log_level="error")

        # Matching mode should succeed
        path, kwargs, _ = cli.prepare_stream_kwargs(cli.StreamArgs(), global_opts, cli.StreamMode.READ)
        assert path == "/my-stream"

        # Mismatched mode should exit with usage error
        with pytest.raises(SystemExit) as exc_info:
            cli.prepare_stream_kwargs(cli.StreamArgs(), global_opts, cli.StreamMode.WRITE)
        assert exc_info.value.code == cli.EXIT_USAGE_ERROR
    finally:
        Path(temp_file).unlink(missing_ok=True)

    # Missing mode in config should also exit with usage error
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("stream_path: /my-stream\n")
        f.write("access_token: test_token\n")
        temp_file = f.name

    try:
        global_opts = cli.GlobalOptions(config_name=None, config_file=temp_file, log_level="error")
        with pytest.raises(SystemExit) as exc_info:
            cli.prepare_stream_kwargs(cli.StreamArgs(), global_opts, cli.StreamMode.READ)
        assert exc_info.value.code == cli.EXIT_USAGE_ERROR
    finally:
        Path(temp_file).unlink(missing_ok=True)


def test_prepare_stream_kwargs_from_config():
    """Test that prepare_stream_kwargs resolves stream_path from config."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path
    
    # Create a config with stream_path
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /config-stream\n")
        f.write("access_token: test_token\n")
        temp_file = f.name
    
    try:
        global_opts = cli.GlobalOptions(
            config_name=None,
            config_file=temp_file,
            log_level="error",
        )
        
        # Call without stream_path - should get it from config
        resolved_path, kwargs, _ = cli.prepare_stream_kwargs(cli.StreamArgs(), global_opts, cli.StreamMode.READ)
        assert resolved_path == "/config-stream"
        assert kwargs["stream_path"] == "/config-stream"
        assert kwargs["access_token"] == "test_token"
        
        # Call with stream_path - should override config
        resolved_path, kwargs, _ = cli.prepare_stream_kwargs(cli.StreamArgs(stream_path="/cli-stream"), global_opts, cli.StreamMode.READ)
        assert resolved_path == "/cli-stream"
        assert kwargs["stream_path"] == "/cli-stream"
    finally:
        Path(temp_file).unlink(missing_ok=True)


def test_passphrase_encryption_support():
    """Test that passphrase is correctly passed to SDK for encryption."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path
    
    # Test 1: Passphrase from config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /encrypted-stream\n")
        f.write("access_token: test_token\n")
        f.write("passphrase: config_passphrase\n")
        temp_file = f.name
    
    try:
        global_opts = cli.GlobalOptions(
            config_name=None,
            config_file=temp_file,
            log_level="error",
        )
        
        # Should use passphrase parameter (works for both read and write)
        resolved_path, kwargs, _ = cli.prepare_stream_kwargs(cli.StreamArgs(), global_opts, cli.StreamMode.READ)
        assert kwargs["passphrase"] == "config_passphrase"
        
        # Test 2: Passphrase from CLI argument
        resolved_path, kwargs, _ = cli.prepare_stream_kwargs(
            cli.StreamArgs(passphrase="cli_passphrase"), global_opts, cli.StreamMode.READ
        )
        assert kwargs["passphrase"] == "cli_passphrase"
        
        # Test 3: Passphrase from environment variable
        with patch.dict("os.environ", {"ZEBRASTREAM_PASSPHRASE": "env_passphrase"}):
            resolved_path, kwargs, _ = cli.prepare_stream_kwargs(cli.StreamArgs(), global_opts, cli.StreamMode.READ)
            assert kwargs["passphrase"] == "env_passphrase"
    finally:
        Path(temp_file).unlink(missing_ok=True)
    
    # Test 4: No passphrase - should not be in kwargs
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /plain-stream\n")
        f.write("access_token: test_token\n")
        temp_file = f.name
    
    try:
        global_opts = cli.GlobalOptions(
            config_name=None,
            config_file=temp_file,
            log_level="error",
        )
        
        with patch.dict("os.environ", {}, clear=True):
            resolved_path, kwargs, _ = cli.prepare_stream_kwargs(cli.StreamArgs(), global_opts, cli.StreamMode.READ)
            assert "passphrase" not in kwargs
    finally:
        Path(temp_file).unlink(missing_ok=True)


def test_write_producer_command_parsing():
    """Test that write command correctly parses producer commands after --."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path
    import sys
    
    # Create a config with mode, stream_path and access_token
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: write\n")
        f.write("stream_path: /test-stream\n")
        f.write("access_token: test_token\n")
        f.write("connect_url: http://fake-url\n")
        temp_file = f.name
    
    try:
        # Mock zsfile.open to avoid actual network calls
        with patch('zebrastream.io.cli.zsfile.open') as mock_open:
            mock_writer = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_writer
            
            # Mock subprocess.Popen
            with patch('zebrastream.io.cli.subprocess.Popen') as mock_popen:
                mock_process = MagicMock()
                mock_process.stdout.read.side_effect = [b'test data', b'']
                mock_process.wait.return_value = 0
                mock_popen.return_value = mock_process
                
                # Mock sys.argv to test: zebrastream --config-file FILE write -- seq 10
                original_argv = sys.argv
                try:
                    sys.argv = ['zebrastream', '--config-file', temp_file, 'write', '--', 'seq', '10']
                    
                    # Call main() which will preprocess sys.argv and run the command
                    try:
                        cli.main()
                    except SystemExit as e:
                        assert e.code == 0, f"Command failed with exit code {e.code}"
                finally:
                    sys.argv = original_argv
                
                # Verify subprocess was called with correct command
                mock_popen.assert_called_once()
                call_args = mock_popen.call_args[0][0]
                assert call_args == ['seq', '10']
                
                # Verify zsfile.open was called with correct stream path from config
                mock_open.assert_called_once()
                kwargs = mock_open.call_args[1]
                assert kwargs['stream_path'] == '/test-stream'
                assert kwargs['access_token'] == 'test_token'
    finally:
        Path(temp_file).unlink(missing_ok=True)


def test_read_consumer_command_parsing():
    """Test that read command correctly pipes stream data into consumer subprocess after --."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path
    import sys

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /test-stream\n")
        f.write("access_token: test_token\n")
        f.write("connect_url: http://fake-url\n")
        temp_file = f.name

    try:
        with patch('zebrastream.io.cli.zsfile.open') as mock_open:
            mock_reader = MagicMock()
            mock_reader.read.side_effect = [b'test data', b'']
            mock_open.return_value.__enter__.return_value = mock_reader

            with patch('zebrastream.io.cli.subprocess.Popen') as mock_popen:
                mock_process = MagicMock()
                mock_process.stdin.write.return_value = None
                mock_process.wait.return_value = 0
                mock_popen.return_value = mock_process

                original_argv = sys.argv
                try:
                    sys.argv = ['zebrastream', '--config-file', temp_file, 'read', '--', 'cat']
                    try:
                        cli.main()
                    except SystemExit as e:
                        assert e.code == 0, f"Command failed with exit code {e.code}"
                finally:
                    sys.argv = original_argv

                mock_popen.assert_called_once()
                call_args = mock_popen.call_args[0][0]
                assert call_args == ['cat']

                # stdin must be PIPE so we can write stream data into it
                assert mock_popen.call_args[1]['stdin'] == __import__('subprocess').PIPE

                mock_open.assert_called_once()
                kwargs = mock_open.call_args[1]
                assert kwargs['stream_path'] == '/test-stream'
                assert kwargs['access_token'] == 'test_token'
    finally:
        Path(temp_file).unlink(missing_ok=True)


def test_write_producer_command_without_explicit_subcommand():
    """Test that mode in config must match the explicit subcommand."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path
    import sys

    # Config with mode: write
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: write\n")
        f.write("stream_path: /test-stream\n")
        f.write("access_token: test_token\n")
        f.write("connect_url: http://fake-url\n")
        temp_file = f.name

    try:
        original_argv = sys.argv
        try:
            # Using 'read' with a write config should fail
            sys.argv = ['zebrastream', '--config-file', temp_file, 'read']
            with pytest.raises(SystemExit) as exc_info:
                cli.main()
            assert exc_info.value.code == cli.EXIT_USAGE_ERROR
        finally:
            sys.argv = original_argv
    finally:
        Path(temp_file).unlink(missing_ok=True)


def test_command_in_config():
    """Test that command can be specified in config and CLI command takes precedence."""
    from zebrastream.io import cli
    import tempfile
    from pathlib import Path

    # Test 1: Config with producer command for write mode
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: write\n")
        f.write("stream_path: /test-stream\n")
        f.write("access_token: test_token\n")
        f.write("command: pg_dump mydb\n")
        temp_file = f.name

    try:
        global_opts = cli.GlobalOptions(
            config_name=None,
            config_file=temp_file,
            log_level="error",
        )

        # Config command should be parsed from string
        resolved_path, kwargs, config_command = cli.prepare_stream_kwargs(
            cli.StreamArgs(), global_opts, cli.StreamMode.WRITE
        )
        assert config_command == ["pg_dump", "mydb"]

        # resolve_command should use config command when no CLI command provided
        final_command = cli.resolve_command([], config_command, "producer")
        assert final_command == ["pg_dump", "mydb"]

        # CLI command should take precedence over config command
        cli_command = ["gzip"]
        final_command = cli.resolve_command(cli_command, config_command, "producer")
        assert final_command == ["gzip"]

    finally:
        Path(temp_file).unlink(missing_ok=True)

    # Test 2: Config with consumer command for read mode
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /test-stream\n")
        f.write("access_token: test_token\n")
        f.write("command: tar -xz -C /output\n")
        temp_file = f.name

    try:
        global_opts = cli.GlobalOptions(
            config_name=None,
            config_file=temp_file,
            log_level="error",
        )

        # Config command should be parsed with shell quoting
        resolved_path, kwargs, config_command = cli.prepare_stream_kwargs(
            cli.StreamArgs(), global_opts, cli.StreamMode.READ
        )
        assert config_command == ["tar", "-xz", "-C", "/output"]

        # resolve_command should use config command when no CLI command provided
        final_command = cli.resolve_command([], config_command, "consumer")
        assert final_command == ["tar", "-xz", "-C", "/output"]

    finally:
        Path(temp_file).unlink(missing_ok=True)

    # Test 3: Config without command should return None
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /test-stream\n")
        f.write("access_token: test_token\n")
        temp_file = f.name

    try:
        global_opts = cli.GlobalOptions(
            config_name=None,
            config_file=temp_file,
            log_level="error",
        )

        resolved_path, kwargs, config_command = cli.prepare_stream_kwargs(
            cli.StreamArgs(), global_opts, cli.StreamMode.READ
        )
        assert config_command is None

        # resolve_command should return empty list for stdin/stdout mode
        final_command = cli.resolve_command([], config_command, "consumer")
        assert final_command == []

    finally:
        Path(temp_file).unlink(missing_ok=True)

    # Test 4: Invalid command type should exit with usage error
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("mode: read\n")
        f.write("stream_path: /test-stream\n")
        f.write("access_token: test_token\n")
        f.write("command: 123\n")  # Invalid: should be string
        temp_file = f.name

    try:
        global_opts = cli.GlobalOptions(
            config_name=None,
            config_file=temp_file,
            log_level="error",
        )

        with pytest.raises(SystemExit) as exc_info:
            cli.prepare_stream_kwargs(
                cli.StreamArgs(), global_opts, cli.StreamMode.READ
            )
        assert exc_info.value.code == cli.EXIT_USAGE_ERROR

    finally:
        Path(temp_file).unlink(missing_ok=True)
