"""Auto click a button on Coupang runner.html via Chrome DevTools Protocol."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import socket
import ssl
import struct
import time
import urllib.parse
import urllib.request
from typing import Any
import urllib.error

logger = logging.getLogger(__name__)

RUNNER_URL_SUFFIX = "/runner.html"
RUNNER_URL_FALLBACK = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html"
DEFAULT_BUTTON_ID = "topHalfBtn"
DEBUG_ENDPOINT = "http://127.0.0.1:9222"
TIMEOUT_SECONDS = 60
POLL_INTERVAL_SECONDS = 0.5


def _http_json(url: str, method: str = "GET") -> Any:
    try:
        req = urllib.request.Request(url, method=method, data=b"" if method != "GET" else None)
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = resp.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"DevTools endpoint unreachable: {url}") from exc
    return json.loads(payload)


def _fetch_tabs() -> list[dict]:
    return _http_json(f"{DEBUG_ENDPOINT}/json")


def _find_runner_url() -> str:
    """extension ID를 동적으로 탐색해 runner.html URL을 반환."""
    tabs = _fetch_tabs()
    for tab in tabs:
        url = str(tab.get("url", ""))
        if url.startswith("chrome-extension://") and url.endswith(RUNNER_URL_SUFFIX):
            return url
    return RUNNER_URL_FALLBACK


def _ensure_runner_tab() -> str:
    runner_url = RUNNER_URL_FALLBACK
    start = time.time()
    while time.time() - start < 10:
        tabs = _fetch_tabs()
        for tab in tabs:
            url = str(tab.get("url", ""))
            if url.startswith("chrome-extension://") and url.endswith(RUNNER_URL_SUFFIX):
                ws_url = tab.get("webSocketDebuggerUrl")
                if ws_url:
                    logger.info("existing runner tab found: %s", url)
                    return ws_url
        time.sleep(POLL_INTERVAL_SECONDS)

    runner_url = _find_runner_url()

    logger.info("runner tab not found, create one: %s", runner_url)
    encoded = urllib.parse.quote(runner_url, safe="")
    new_tab = _http_json(f"{DEBUG_ENDPOINT}/json/new?{encoded}", method="PUT")
    ws_url = new_tab.get("webSocketDebuggerUrl") if isinstance(new_tab, dict) else None
    if ws_url:
        return ws_url

    raise RuntimeError("Unable to open runner.html tab with websocket endpoint")


def _recv_exact(sock: socket.socket, n: int, timeout: float = 5.0) -> bytes:
    sock.settimeout(timeout)
    chunks = []
    remain = n
    while remain > 0:
        chunk = sock.recv(remain)
        if not chunk:
            raise RuntimeError("socket closed while receiving websocket frame")
        chunks.append(chunk)
        remain -= len(chunk)
    return b"".join(chunks)


def _build_websocket_frame(payload: bytes) -> bytes:
    mask = os.urandom(4)
    length = len(payload)

    first = 0x81
    if length <= 125:
        second = 0x80 | length
        header = bytes([first, second])
    elif length <= 65535:
        second = 0x80 | 126
        header = bytes([first, second]) + struct.pack("!H", length)
    else:
        second = 0x80 | 127
        header = bytes([first, second]) + struct.pack("!Q", length)

    masked_payload = bytes(payload[i] ^ mask[i % 4] for i in range(length))
    return header + mask + masked_payload


def _parse_websocket_frame(sock: socket.socket) -> tuple[int, bytes]:
    header = _recv_exact(sock, 2)
    b1, b2 = header[0], header[1]
    opcode = b1 & 0x0F
    length = b2 & 0x7F

    if length == 126:
        length = struct.unpack("!H", _recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _recv_exact(sock, 8))[0]

    if b2 & 0x80:
        _recv_exact(sock, 4)  # mask ignored for incoming frames in this flow

    payload = _recv_exact(sock, int(length)) if length else b""
    return opcode, payload


class _RawWebSocket:
    def __init__(self, url: str) -> None:
        parsed = urllib.parse.urlparse(url)
        if not parsed.hostname:
            raise RuntimeError(f"Invalid websocket url: {url}")

        self._url = url
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        self._path = (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else "")
        self._ssl = parsed.scheme == "wss"
        self._next_id = 1

        self._sock: socket.socket | None = socket.create_connection((self._host, self._port), timeout=10)
        if not self._sock:
            raise RuntimeError("failed to connect websocket socket")

        if self._ssl:
            ctx = ssl.create_default_context()
            self._sock = ctx.wrap_socket(self._sock, server_hostname=self._host)

        self._handshake()

    def _handshake(self) -> None:
        if self._sock is None:
            raise RuntimeError("websocket socket missing")

        key = base64.b64encode(os.urandom(16)).decode()
        req_lines = [
            f"GET {self._path} HTTP/1.1",
            f"Host: {self._host}:{self._port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            "",
            "",
        ]
        self._sock.sendall("\r\n".join(req_lines).encode("utf-8"))

        response = b""
        while b"\r\n\r\n" not in response:
            response += self._sock.recv(4096)
        headers = response.split(b"\r\n\r\n", 1)[0].decode("utf-8", errors="replace")
        if " 101 " not in headers.splitlines()[0]:
            raise RuntimeError(f"websocket handshake failed: {headers.splitlines()[0]}")

    def send(self, payload: dict) -> int:
        if self._sock is None:
            raise RuntimeError("websocket socket missing")
        current_id = self._next_id
        self._next_id += 1
        payload["id"] = current_id
        frame = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._sock.sendall(_build_websocket_frame(frame))
        return current_id

    def recv_json(self, timeout: float = 5.0) -> dict | None:
        if self._sock is None:
            return None
        end = time.time() + timeout
        while True:
            now = time.time()
            if now >= end:
                return None
            per_call_timeout = max(0.5, end - now)
            try:
                if self._sock is not None:
                    self._sock.settimeout(per_call_timeout)
                opcode, payload = _parse_websocket_frame(self._sock)
            except TimeoutError:
                return None
            except socket.timeout:
                return None

            if opcode == 8:
                return None
            if opcode != 1:
                continue
            text = payload.decode("utf-8", errors="replace")
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                logger.warning("invalid websocket payload (ignore): %s", text)
                continue

    def call(self, method: str, params: dict | None = None, timeout: float = 10.0) -> dict:
        if params is None:
            params = {}
        request_id = self.send({"method": method, "params": params})
        deadline = time.time() + timeout

        while time.time() < deadline:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            message = self.recv_json(timeout=remaining)
            if not message:
                break
            if message.get("id") == request_id:
                return message
        raise TimeoutError(f"CDP call timeout: {method}")

    def close(self) -> None:
        if self._sock is None:
            return
        try:
            self._sock.close()
        finally:
            self._sock = None


def _evaluate_button_state(ws: _RawWebSocket, button_id: str) -> tuple[bool, bool, bool]:
    script = (
        "(()=>{"
        f"const b=document.querySelector('#{button_id}');"
        "const stop=document.querySelector('#stopBtn');"
        "const alreadyRunning=!!(stop && !stop.disabled);"
        "if(!b){return {exists:false,enabled:false,alreadyRunning};}"
        "return {exists:true,enabled:!b.disabled,alreadyRunning};"
        "})()"
    )
    result = ws.call(
        "Runtime.evaluate",
        {
            "expression": script,
            "returnByValue": True,
            "awaitPromise": True,
        },
        timeout=5.0,
    )
    if "error" in result:
        raise RuntimeError(f"CDP evaluate failed: {result['error']}")

    value = (result.get("result") or {}).get("result") or {}
    parsed = value.get("value") or {}
    return (
        bool(parsed.get("exists")),
        bool(parsed.get("enabled")),
        bool(parsed.get("alreadyRunning")),
    )


def _click_button(ws: _RawWebSocket, button_id: str) -> None:
    result = ws.call(
        "Runtime.evaluate",
        {
            "expression": f"(()=>{{document.getElementById('{button_id}').click(); return true;}})()",
            "returnByValue": True,
        },
        timeout=5.0,
    )
    if "error" in result:
        raise RuntimeError(f"button click failed: {result['error']}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto click a button on Coupang runner.html")
    parser.add_argument(
        "--button-id",
        default=DEFAULT_BUTTON_ID,
        help=f"runner.html button id to click (default: {DEFAULT_BUTTON_ID})",
    )
    return parser.parse_args()


def run_autoclick(button_id: str = DEFAULT_BUTTON_ID) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    ws_url = _ensure_runner_tab()
    logger.info("connected runner tab: %s", ws_url)
    logger.info("target button id: %s", button_id)

    ws = _RawWebSocket(ws_url)
    try:
        ws.call("Runtime.enable")
        start = time.time()
        while time.time() - start < TIMEOUT_SECONDS:
            exists, enabled, already_running = _evaluate_button_state(ws, button_id)
            if not exists:
                logger.info("%s not found yet, waiting", button_id)
            elif already_running:
                logger.info("runner batch is already running; treating autoclick as successful")
                return 0
            elif enabled:
                logger.info("%s is enabled. clicking", button_id)
                _click_button(ws, button_id)
                logger.info("clicked %s", button_id)
                return 0
            else:
                logger.info("%s exists but disabled", button_id)
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        ws.close()

    logger.error("timeout waiting %s enabled for %ss", button_id, TIMEOUT_SECONDS)
    return 1


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(run_autoclick(button_id=args.button_id))
