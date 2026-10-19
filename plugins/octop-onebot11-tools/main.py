"""Agent tools for proactive OneBot 11 / NapCat messages.

The channel plugin (`octop-onebot11`) keeps a process-local registry of live
channel instances.  These tools resolve that instance and call its
``push_text()`` path, which raises on real send failures (unlike inbound reply
delivery, which is best-effort).

Tool call path:
    Agent -> StructuredTool -> onebot11_send_group()
          -> OneBot11Channel.push_text()
          -> send_group_msg action over the NapCat WebSocket
"""

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
    module = _channel_module()
    if module is None:
        return None, "OneBot 通道插件未加载"
    channel = module.get_live_channel(channel_id)
    if channel is None:
        return None, "没有可用的 OneBot 11 通道实例"
    if not channel.is_connected:
        detail = getattr(channel, "runtime_error", None) or "NapCat 未登录或 WebSocket 未连接"
        return None, f"OneBot 通道未连接：{detail}"
    return channel, ""


async def onebot11_send_group(group_id: str, text: str, channel_id: str = "") -> str:
    """主动向指定 QQ 群发送文本消息。

    Args:
        group_id: QQ 群号，例如 "123456789"。
        text: 要发送的纯文本内容。
        channel_id: 可选，指定 Octop 通道 ID；留空则使用第一个已连接的 OneBot 通道。

    Returns:
        发送结果说明；失败时以 "发送失败:" 开头。
    """
    gid = str(group_id or "").strip()
    body = str(text or "").strip()
    if not gid:
        return "发送失败: group_id 不能为空"
    if not body:
        return "发送失败: text 不能为空"

    channel, error = _resolve_channel(channel_id)
    if channel is None:
        return f"发送失败: {error}"

    module = _channel_module()
    try:
        subject = module.build_group_subject(gid)
        await channel.push_text(subject, body)
    except Exception as exc:  # noqa: BLE001 - surface the platform error to the agent
        return f"发送失败: {exc}"
    return f"已发送到群 {gid}"


async def onebot11_send_private(user_id: str, text: str, channel_id: str = "") -> str:
    """主动向指定 QQ 用户发送私聊文本消息。

    Args:
        user_id: QQ 号，例如 "10001"。
        text: 要发送的纯文本内容。
        channel_id: 可选，指定 Octop 通道 ID；留空则使用第一个已连接的 OneBot 通道。

    Returns:
        发送结果说明；失败时以 "发送失败:" 开头。
    """
    uid = str(user_id or "").strip()
    body = str(text or "").strip()
    if not uid:
        return "发送失败: user_id 不能为空"
    if not body:
        return "发送失败: text 不能为空"

    channel, error = _resolve_channel(channel_id)
    if channel is None:
        return f"发送失败: {error}"

    module = _channel_module()
    try:
        subject = module.build_private_subject(uid)
        await channel.push_text(subject, body)
    except Exception as exc:  # noqa: BLE001
        return f"发送失败: {exc}"
    return f"已发送给用户 {uid}"


def setup(ctx: Any) -> None:
    """Register proactive send tools with Octop's plugin registry."""
    ctx.tool(
        "onebot11_send_group",
        onebot11_send_group,
        description=(
            "主动向指定 QQ 群发送文本消息。用于用户明确要求“发到某个群/通知某个群”的场景；"
            "不要求该群此前与机器人有过会话。"
        ),
    )
    ctx.tool(
        "onebot11_send_private",
        onebot11_send_private,
        description=(
            "主动向指定 QQ 用户发送私聊文本消息。用于用户明确要求私聊通知某个 QQ 号的场景。"
        ),
    )
