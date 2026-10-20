"""Agent tools for proactive OneBot 11 / NapCat messages.

The channel plugin (`octop-onebot11`) keeps a process-local registry of live
channel instances.  These tools resolve that instance and call its strict
``push_segments()`` / ``push_forward()`` paths, which park messages in the
retry outbox while the transport is down (instead of failing) and raise on
real send failures.

Tool call path:
    Agent -> StructuredTool -> onebot11_send_group()
          -> OneBot11Channel.push_segments()
          -> send_group_msg action over the NapCat WebSocket (or HTTP fallback)
"""

import json
import sys
from typing import Any

_CHANNEL_MODULE = "harness_plugin_octop_onebot11"


def _channel_module() -> Any | None:
    module = sys.modules.get(_CHANNEL_MODULE)
    if module is not None:
        return module
    try:
        import importlib

        return importlib.import_module(_CHANNEL_MODULE)
    except Exception:
        return None


def _resolve_channel(channel_id: str = "") -> tuple[Any | None, str]:
    """Find a live channel instance.

    ``is_connected`` is deliberately not required here: when the transport is
    down the strict push path either goes through the HTTP fallback or parks
    the message in the outbox and retries after NapCat reconnects.  A missing
    instance or an outdated channel plugin still fails the call.
    """
    module = _channel_module()
    if module is None:
        return None, "OneBot 通道插件未加载"
    channel = module.get_live_channel(channel_id)
    if channel is None:
        return None, "没有可用的 OneBot 11 通道实例"
    if not hasattr(channel, "push_segments"):
        return None, "通道插件版本过旧（缺少 push_segments），请升级 octop-onebot11"
    return channel, ""


def _push_note(channel: Any) -> str:
    if getattr(channel, "is_connected", False):
        return ""
    config = getattr(channel, "_config", None)
    if str(getattr(config, "http_api_url", "") or "").strip():
        return ""  # delivered through the HTTP fallback despite the WS being down
    detail = getattr(channel, "runtime_error", None) or "通道未连接"
    return f"（通道当前状态：{detail}；消息已进入待发队列，重连后自动补发）"


def _build_segments(
    text: str,
    image_url: str,
    at_user_id: str = "",
    reply_to_message_id: str = "",
) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    if reply_to_message_id:
        segments.append({"type": "reply", "data": {"id": str(reply_to_message_id)}})
    if at_user_id:
        segments.append({"type": "at", "data": {"qq": at_user_id}})
    if image_url:
        segments.append({"type": "image", "data": {"file": image_url}})
    if text:
        segments.append({"type": "text", "data": {"text": text}})
    return segments


def _build_forward_nodes(messages_json: str, fallback_uin: str) -> list[dict[str, Any]]:
    parsed = json.loads(messages_json)
    if not isinstance(parsed, list) or not parsed:
        raise ValueError("messages_json 必须是非空 JSON 数组")
    nodes: list[dict[str, Any]] = []
    for item in parsed:
        if isinstance(item, str):
            name, text = "通知", item
        elif isinstance(item, dict):
            name = str(item.get("name") or "通知")
            text = str(item.get("text") or "")
        else:
            raise ValueError("数组元素必须是 {\"name\": ..., \"text\": ...} 或纯字符串")
        if not text:
            continue
        nodes.append(
            {
                "type": "node",
                "data": {
                    "name": name,
                    "uin": fallback_uin,
                    "content": [{"type": "text", "data": {"text": text}}],
                },
            }
        )
    if not nodes:
        raise ValueError("没有可发送的转发内容")
    return nodes


async def onebot11_send_group(
    group_id: str,
    text: str = "",
    image_url: str = "",
    at_user_id: str = "",
    reply_to_message_id: str = "",
    account: str = "",
    channel_id: str = "",
) -> str:
    """主动向指定 QQ 群发送消息，可带图片、@ 成员和引用。

    Args:
        group_id: QQ 群号，例如 "123456789"。
        text: 要发送的纯文本内容，与 image_url 至少提供一个。
        image_url: 图片地址（http/https URL 或服务器本地文件路径），可选。
        at_user_id: 需要 @ 的成员 QQ 号，可选；@ 段会放在消息最前面。
        reply_to_message_id: 要引用回复的消息 ID，可选。
        account: 可选，多账号模式下指定用哪个机器人 QQ 号发送；留空用默认账号。
        channel_id: 可选，指定 Octop 通道 ID；留空则使用第一个已连接的 OneBot 通道。

    Returns:
        发送结果说明；失败时以 "发送失败:" 开头。通道断线时消息进入待发队列，
        重连后自动补发，此时返回值会注明排队状态。
    """
    gid = str(group_id or "").strip()
    body = str(text or "").strip()
    image = str(image_url or "").strip()
    at = str(at_user_id or "").strip()
    reply_to = str(reply_to_message_id or "").strip()
    sender_account = str(account or "").strip()
    if not gid:
        return "发送失败: group_id 不能为空"
    if not body and not image:
        return "发送失败: text 与 image_url 至少提供一个"

    channel, error = _resolve_channel(channel_id)
    if channel is None:
        return f"发送失败: {error}"

    module = _channel_module()
    try:
        subject = module.build_group_subject(gid, sender_account)
        await channel.push_segments(subject, _build_segments(body, image, at, reply_to))
    except Exception as exc:  # noqa: BLE001 - surface the platform error to the agent
        return f"发送失败: {exc}"
    return f"已发送到群 {gid}{_push_note(channel)}"


async def onebot11_send_private(
    user_id: str,
    text: str = "",
    image_url: str = "",
    reply_to_message_id: str = "",
    account: str = "",
    channel_id: str = "",
) -> str:
    """主动向指定 QQ 用户发送私聊消息，可带图片和引用。

    Args:
        user_id: QQ 号，例如 "10001"。
        text: 要发送的纯文本内容，与 image_url 至少提供一个。
        image_url: 图片地址（http/https URL 或服务器本地文件路径），可选。
        reply_to_message_id: 要引用回复的消息 ID，可选。
        account: 可选，多账号模式下指定用哪个机器人 QQ 号发送；留空用默认账号。
        channel_id: 可选，指定 Octop 通道 ID；留空则使用第一个已连接的 OneBot 通道。

    Returns:
        发送结果说明；失败时以 "发送失败:" 开头。通道断线时消息进入待发队列，
        重连后自动补发，此时返回值会注明排队状态。
    """
    uid = str(user_id or "").strip()
    body = str(text or "").strip()
    image = str(image_url or "").strip()
    reply_to = str(reply_to_message_id or "").strip()
    sender_account = str(account or "").strip()
    if not uid:
        return "发送失败: user_id 不能为空"
    if not body and not image:
        return "发送失败: text 与 image_url 至少提供一个"

    channel, error = _resolve_channel(channel_id)
    if channel is None:
        return f"发送失败: {error}"

    module = _channel_module()
    try:
        subject = module.build_private_subject(uid, sender_account)
        await channel.push_segments(subject, _build_segments(body, image, "", reply_to))
    except Exception as exc:  # noqa: BLE001
        return f"发送失败: {exc}"
    return f"已发送给用户 {uid}{_push_note(channel)}"


async def onebot11_send_forward_group(
    group_id: str,
    messages_json: str,
    account: str = "",
    channel_id: str = "",
) -> str:
    """主动向指定 QQ 群发送合并转发消息（把多条文本打包成一条转发卡片）。

    Args:
        group_id: QQ 群号，例如 "123456789"。
        messages_json: JSON 数组字符串，每项为 {"name": 显示昵称, "text": 文本}，
            也可以是纯字符串数组（昵称默认为“通知”）。
        account: 可选，多账号模式下指定用哪个机器人 QQ 号发送；留空用默认账号。
        channel_id: 可选，指定 Octop 通道 ID；留空则使用第一个已连接的 OneBot 通道。

    Returns:
        发送结果说明；失败时以 "发送失败:" 开头。
    """
    gid = str(group_id or "").strip()
    if not gid:
        return "发送失败: group_id 不能为空"
    raw = str(messages_json or "").strip()
    if not raw:
        return "发送失败: messages_json 不能为空"
    sender_account = str(account or "").strip()

    channel, error = _resolve_channel(channel_id)
    if channel is None:
        return f"发送失败: {error}"

    module = _channel_module()
    try:
        uin = sender_account or str(getattr(channel, "_self_id", "") or "10000")
        nodes = _build_forward_nodes(raw, uin)
        subject = module.build_group_subject(gid, sender_account)
        await channel.push_forward(subject, nodes)
    except Exception as exc:  # noqa: BLE001
        return f"发送失败: {exc}"
    return f"已发送合并转发到群 {gid}{_push_note(channel)}"


def setup(ctx: Any) -> None:
    """Register proactive send tools with Octop's plugin registry."""
    ctx.tool(
        "onebot11_send_group",
        onebot11_send_group,
        description=(
            "主动向指定 QQ 群发送文本消息，可附带图片、@ 成员和引用回复"
            "（image_url / at_user_id / reply_to_message_id）。"
            "用于用户明确要求“发到某个群/通知某个群”的场景；不要求该群此前与机器人有过会话。"
            "多账号通道可用 account 指定发送的机器人 QQ 号。"
            "通道暂时断线时消息会进入待发队列，重连后自动补发。"
        ),
    )
    ctx.tool(
        "onebot11_send_private",
        onebot11_send_private,
        description=(
            "主动向指定 QQ 用户发送私聊文本消息，可附带图片和引用回复"
            "（image_url / reply_to_message_id）。"
            "用于用户明确要求私聊通知某个 QQ 号的场景。"
            "多账号通道可用 account 指定发送的机器人 QQ 号。"
            "通道暂时断线时消息会进入待发队列，重连后自动补发。"
        ),
    )
    ctx.tool(
        "onebot11_send_forward_group",
        onebot11_send_forward_group,
        description=(
            "主动向指定 QQ 群发送合并转发消息：把多条文本打包成一条转发卡片。"
            "用于“把这份清单/日志/多段内容合并发到群里”的场景，比连续发多条消息更整洁。"
        ),
    )
