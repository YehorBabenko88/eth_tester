"""QA tests never create outbound connections to an exchange or Telegram."""
import socket

import pytest


@pytest.fixture(autouse=True)
def prohibit_external_tcp(monkeypatch):
    original = socket.socket.connect

    def guarded_connect(sock, address):
        if isinstance(address, tuple):
            host = address[0]
            if host not in ("localhost", "127.0.0.1", "::1"):
                raise AssertionError(f"QA blocked external TCP connection to {host}")
        return original(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
