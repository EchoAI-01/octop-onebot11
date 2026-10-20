"""Minimal OneBot 11 mock server (WebSocket + HTTP) for offline verification.

Runs inside the Octop container (or anywhere with ``websockets`` installed):

    python mock_napcat.py --host 0.0.0.0 --port 3001 --token dev-token \
        --logged-in --send-test-after 5 --http-port 3000

It implements just enough of the OneBot 11 API for the Octop channel:
``get_login_info``, ``get_status``, ``get_forward_msg``, ``send_private_msg``,
``send_group_msg``, ``send_group_forward_msg`` and the request-approval
actions.  Use ``--logged-in`` to simulate a logged-in NapCat; omit it to
verify that Octop creates and keeps the channel while QQ is not logged in.
``--http-port`` exposes the same actions over the OneBot 11 HTTP API
(``POST /<action>``) so the channel's HTTP fallback path can be exercised.
``--ack-delay`` / ``--no-ack-send`` simulate a slow or silent NapCat: the
``send_*`` actions are still executed (and logged, i.e. the message counts as
delivered) but their responses arrive late or never, which is exactly the
"delivered but not acknowledged" situation the outbox has to survive.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import websockets

SELF_ID = "10000"
_counter = 1000
_out_log: Any = None


def _log(message: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {message}"
    print(line, flush=True)
    if _out_log is not None:
        _out_log.write(line + "\n")
        _out_log.flush()


def _action_result(
    action: str,
    params: dict[str, Any],
    *,
    logged_in: bool,
) -> tuple[bool, Any, int, str]:
    """Shared action switch for both the WS and HTTP transports."""
    global _counter
    if action == "get_login_info":
        if logged_in:
            return True, {"user_id": int(SELF_ID), "nickname": "MockBot"}, 0, ""
        return False, None, 1404, "not logged in"
    if action == "get_status":
        return True, {"online": bool(logged_in), "good": True}, 0, ""
    if action == "get_forward_msg":
        return True, [
            {
                "sender": {"user_id": 10001, "nickname": "转发用户A"},
                "message": [{"type": "text", "data": {"text": "转发内容第一条"}}],
            },
            {
                "sender": {"user_id": 10002, "nickname": "转发用户B"},
                "message": [{"type": "text", "data": {"text": "转发内容第二条"}}],
            },
        ], 0, ""
    if action in ("set_friend_add_request", "set_group_add_request"):
        _log(f"{action} flag={params.get('flag')} approve={params.get('approve')}")
        return True, None, 0, ""
    if action in ("send_private_msg", "send_group_msg", "send_group_forward_msg"):
        _counter += 1
        target = params.get("group_id") or params.get("user_id")
        payload = params.get("message")
        if payload is None:
            payload = params.get("messages")
        _log(f"{action} target={target} message={json.dumps(payload, ensure_ascii=False)}")
        return True, {"message_id": _counter}, 0, ""
    return False, None, 1400, f"unsupported action {action}"


def _response(echo: Any, *, ok: bool = True, data: Any = None, retcode: int = 0, wording: str = "") -> str:
    return json.dumps(
        {
            "status": "ok" if ok else "failed",
            "retcode": retcode,
            "data": data,
            "echo": echo,
            **({"wording": wording} if wording else {}),
        },
        ensure_ascii=False,
    )


async def _send_test_event(ws: Any) -> None:
    await asyncio.sleep(4)
    event = {
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "message_id": 9001,
        "group_id": 20002,
        "user_id": 10001,
        "self_id": SELF_ID,
        "time": int(time.time()),
        "message": [
            {"type": "at", "data": {"qq": SELF_ID}},
            {"type": "text", "data": {"text": " 你好，这是一条离线联调测试消息。"}},
        ],
        "raw_message": f"[CQ:at,qq={SELF_ID}] 你好，这是一条离线联调测试消息。",
        "sender": {"user_id": 10001, "nickname": "测试用户", "card": "测试用户"},
    }
    await ws.send(json.dumps(event, ensure_ascii=False))
    _log("sent test group message (message_id=9001, group_id=20002)")


async def _send_later(ws: Any, delay: float, frame: str) -> None:
    try:
        await asyncio.sleep(delay)
        await ws.send(frame)
    except websockets.ConnectionClosed:
        pass


async def handler(ws: Any, args: argparse.Namespace) -> None:
    if args.token:
        auth = ws.request.headers.get("Authorization", "")
        if auth != f"Bearer {args.token}":
            _log(f"rejected client: bad Authorization={auth!r}")
            await ws.close(code=1008, reason="invalid token")
            return
    _log(f"client connected: {ws.remote_address}")
    test_task = None
    if args.send_test_after and args.logged_in:
        test_task = asyncio.create_task(_send_test_event(ws))
    try:
        async for raw in ws:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                _log(f"non-JSON frame: {raw!r}")
                continue
            action = payload.get("action")
            params = payload.get("params") or {}
            echo = payload.get("echo")
            ok, data, retcode, wording = _action_result(action, params, logged_in=args.logged_in)
            if retcode == 1400:
                _log(f"unsupported action={action} params={params}")
            frame = _response(echo, ok=ok, data=data, retcode=retcode, wording=wording)
            is_send = str(action).startswith("send_")
            if is_send and args.no_ack_send:
                continue  # executed and logged above, but never acknowledged
            if is_send and args.ack_delay > 0:
                asyncio.create_task(_send_later(ws, args.ack_delay, frame))
                continue
            await ws.send(frame)
    except websockets.ConnectionClosed:
        _log("client disconnected")
    finally:
        if test_task is not None:
            test_task.cancel()


def _make_http_handler(args: argparse.Namespace) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a: Any) -> None:  # silence default access log
            pass

        def do_POST(self) -> None:  # noqa: N802 - http.server naming
            try:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length > 0 else b"{}"
                params = json.loads(body or b"{}")
            except Exception:
                self.send_response(400)
                self.end_headers()
                return
            if args.token and self.headers.get("Authorization", "") != f"Bearer {args.token}":
                self.send_response(401)
                self.end_headers()
                return
            action = self.path.split("?", 1)[0].strip("/")
            ok, data, retcode, wording = _action_result(action, params, logged_in=args.logged_in)
            payload = json.dumps(
                {
                    "status": "ok" if ok else "failed",
                    "retcode": retcode,
                    "data": data,
                    **({"wording": wording} if wording else {}),
                },
                ensure_ascii=False,
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return Handler


def start_http_server(args: argparse.Namespace) -> ThreadingHTTPServer | None:
    if not args.http_port:
        return None
    server = ThreadingHTTPServer((args.host, args.http_port), _make_http_handler(args))
    threading.Thread(target=server.serve_forever, daemon=True, name="mock-napcat-http").start()
    _log(f"mock napcat http api listening on http://{args.host}:{args.http_port}")
    return server


async def main() -> None:
    global _out_log
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3001)
    parser.add_argument("--http-port", type=int, default=0)
    parser.add_argument("--token", default="")
    parser.add_argument("--logged-in", action="store_true")
    parser.add_argument("--send-test-after", type=int, default=0)
    parser.add_argument("--log-file", default="")
    parser.add_argument(
        "--ack-delay",
        type=float,
        default=0.0,
        help="seconds to wait before answering send_* actions (the message is still delivered)",
    )
    parser.add_argument(
        "--no-ack-send",
        action="store_true",
        help="execute send_* actions but never answer them",
    )
    args = parser.parse_args()
    if args.log_file:
        _out_log = open(args.log_file, "a", encoding="utf-8")  # noqa: SIM115

    start_http_server(args)
    async with websockets.serve(
        lambda ws: handler(ws, args),
        args.host,
        args.port,
        ping_interval=20,
        ping_timeout=20,
    ):
        _log(
            f"mock napcat listening on ws://{args.host}:{args.port} "
            f"logged_in={args.logged_in} token={'yes' if args.token else 'no'}"
        )
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
