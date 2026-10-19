"""Minimal OneBot 11 forward-WebSocket mock server for offline verification.

Runs inside the Octop container (or anywhere with ``websockets`` installed):

    python mock_napcat.py --host 0.0.0.0 --port 3001 --token dev-token \
        --logged-in --send-test-after 5

It implements just enough of the OneBot 11 action API for the Octop channel:
``get_login_info``, ``get_status``, ``send_private_msg`` and
``send_group_msg``.  Use ``--logged-in`` to simulate a logged-in NapCat; omit
it to verify that Octop creates and keeps the channel while QQ is not logged
in.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any

import websockets

SELF_ID = "10000"
_counter = 1000
_out_log: Any = None


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


def _log(message: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {message}"
    print(line, flush=True)
    if _out_log is not None:
        _out_log.write(line + "\n")
        _out_log.flush()


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


async def handler(ws: Any, args: argparse.Namespace) -> None:
    global _counter
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
            if action == "get_login_info":
                if args.logged_in:
                    await ws.send(_response(echo, data={"user_id": int(SELF_ID), "nickname": "MockBot"}))
                else:
                    await ws.send(_response(echo, ok=False, retcode=1404, wording="not logged in"))
            elif action == "get_status":
                await ws.send(
                    _response(
                        echo,
                        data={"online": bool(args.logged_in), "good": True},
                    ),
                )
            elif action in ("send_private_msg", "send_group_msg"):
                _counter += 1
                target = params.get("group_id") or params.get("user_id")
                _log(f"{action} target={target} message={json.dumps(params.get('message'), ensure_ascii=False)}")
                await ws.send(_response(echo, data={"message_id": _counter}))
            else:
                _log(f"unsupported action={action} params={params}")
                await ws.send(_response(echo, ok=False, retcode=1400, wording=f"unsupported action {action}"))
    except websockets.ConnectionClosed:
        _log("client disconnected")
    finally:
        if test_task is not None:
            test_task.cancel()


async def main() -> None:
    global _out_log
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3001)
    parser.add_argument("--token", default="")
    parser.add_argument("--logged-in", action="store_true")
    parser.add_argument("--send-test-after", type=int, default=0)
    parser.add_argument("--log-file", default="")
    args = parser.parse_args()
    if args.log_file:
        _out_log = open(args.log_file, "a", encoding="utf-8")  # noqa: SIM115

    async with websockets.serve(
        lambda ws: handler(ws, args),
        args.host,
        args.port,
        ping_interval=20,
        ping_timeout=20,
    ):
        _log(
            f"mock napcat listening on ws://{args.host}:{args.port} "
            f"logged_in={args.logged_in} token={'yes' if args.token else 'no'}",
        )
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
