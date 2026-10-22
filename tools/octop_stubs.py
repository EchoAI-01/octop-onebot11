"""Shared stand-ins for the ``octop_gateway`` contracts used by the plugin.

The plugin imports ``octop_gateway`` at module level, but the Octop runtime is
not available on a workstation or in CI.  ``install()`` registers small fakes
that follow the real contracts closely enough for the wire-format, routing and
policy logic to run.  ``test_plugin_core_integration.py`` runs against the real
package where it is installed.

Differences from the real ``BaseChannel`` that tests may rely on:

* ``enqueue()`` appends to ``channel.enqueued`` instead of calling the manager;
* ``_acquire_rate_slot()`` only counts calls in ``channel.rate_slots``.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CHANNEL_MODULE = "harness_plugin_octop_onebot11"
TOOLS_MODULE = "harness_plugin_octop_onebot11_tools"


@dataclass
class GroupContextConfig:
    enabled: bool = False
    visibility: str = "auto"
    activation: str = "mention"
    history: str = "recent"
    history_limit: int = 10
    history_ttl_seconds: float = 300.0
    clear_after_reply: bool = True
    groups: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data):
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


@dataclass
class ChannelConfig:
    channel_id: str | None = None
    tenant_id: str | None = None
    show_thinking: bool = False
    show_tool_hints: bool = True
    group_context: GroupContextConfig = field(default_factory=GroupContextConfig)

    @classmethod
    def from_dict(cls, data):
        """Same behaviour as the real implementation (strip, aliases, group_context)."""

        def _clean(value):
            return value.strip() if isinstance(value, str) else value

        known = {f.name for f in fields(cls)}
        out = {k: _clean(v) for k, v in data.items() if k in known}
        group_context = out.get("group_context")
        if isinstance(group_context, dict):
            out["group_context"] = GroupContextConfig.from_dict(group_context)
        for alias, canonical in cls.field_aliases.items():
            if canonical in known and not out.get(canonical) and data.get(alias):
                out[canonical] = _clean(data[alias])
        return cls(**out)

    def missing_credentials(self):
        return [name for name in self.required_credentials if not getattr(self, name, None)]


ChannelConfig.field_aliases = {}
ChannelConfig.required_credentials = ()


@dataclass
class ChannelConstraints:
    reply_timeout: float = 0.0
    send_rate_limit: tuple[int, float] | None = None
    typing_keepalive_interval: float = 0.0
    show_thinking: bool = False
    show_tool_hints: bool = True


class BaseChannel:
    channel_type = "base"

    def __init__(self, processor, *, channel_id=None, tenant_id=None, config=None, **kwargs):
        self._processor = processor
        self._channel_id = channel_id or "test-channel"
        self._tenant_id = tenant_id
        self._config = config
        self.enqueued: list[Any] = []
        self.rate_slots = 0
        # Mirror the real BaseChannel: constraints are resolved during
        # __init__, before subclass constructors assign their own attributes.
        self._default_constraints()

    @property
    def channel_id(self):
        return self._channel_id

    @property
    def tenant_id(self):
        return self._tenant_id

    def enqueue(self, payload):
        self.enqueued.append(payload)

    async def _acquire_rate_slot(self):
        self.rate_slots += 1

    async def _preprocess_inbound(self, message):
        """Core hook (default: nothing to do)."""

    async def handle_inbound(self, raw_payload):
        """The part of the core's inbound template the plugin hooks into."""
        message = self.parse_inbound(raw_payload)
        await self._preprocess_inbound(message)

    def resolve_push_subject(self, subject):
        return subject

    async def push_text(self, subject, text):
        await self._send_text(subject, text)

    async def load_media_bytes(self, part):
        return b"", "application/octet-stream"

    @staticmethod
    def _get_media_label(part):
        return "媒体"


@dataclass
class TextContent:
    text: str
    type: str = "text"


@dataclass
class ImageContent:
    url: str = ""
    alt_text: str = ""
    data: str | None = None
    mime_type: str | None = None
    size: int | None = None
    local_path: str | None = None
    type: str = "image"


@dataclass
class AudioContent:
    url: str = ""
    mime_type: str | None = None
    size: int | None = None
    local_path: str | None = None
    data: str | None = None
    type: str = "audio"


@dataclass
class VideoContent:
    url: str = ""
    type: str = "video"


@dataclass
class FileContent:
    url: str = ""
    filename: str = ""
    type: str = "file"


@dataclass
class ChannelSubject:
    subject_id: str
    chat_type: str = ""
    display_name: str = ""
    metadata: dict = field(default_factory=dict)
    first_seen: float = 0
    last_seen: float = 0


@dataclass
class InboundMessage:
    channel_id: str
    channel_type: str
    tenant_id: str | None
    channel_subject: ChannelSubject
    channel_session_id: str
    content: list
    metadata: dict = field(default_factory=dict)
    timestamp: float = 0

    @property
    def text(self):
        return "\n".join(part.text for part in self.content if isinstance(part, TextContent) and part.text)


def install() -> types.SimpleNamespace:
    """Register the fake ``octop_gateway`` modules (idempotent)."""
    namespace = types.SimpleNamespace(
        BaseChannel=BaseChannel,
        ChannelConfig=ChannelConfig,
        ChannelConstraints=ChannelConstraints,
        GroupContextConfig=GroupContextConfig,
        TextContent=TextContent,
        ImageContent=ImageContent,
        AudioContent=AudioContent,
        VideoContent=VideoContent,
        FileContent=FileContent,
        ChannelSubject=ChannelSubject,
        InboundMessage=InboundMessage,
    )
    if "octop_gateway" in sys.modules and getattr(sys.modules["octop_gateway"], "_octop_stub", False):
        return namespace

    pkg = types.ModuleType("octop_gateway")
    pkg.__path__ = []
    pkg._octop_stub = True
    sys.modules["octop_gateway"] = pkg

    channel_mod = types.ModuleType("octop_gateway.channel")
    channel_mod.BaseChannel = BaseChannel
    channel_mod.ChannelConfig = ChannelConfig
    channel_mod.MessageProcessor = object
    sys.modules["octop_gateway.channel"] = channel_mod

    constraints_mod = types.ModuleType("octop_gateway.constraints")
    constraints_mod.ChannelConstraints = ChannelConstraints
    sys.modules["octop_gateway.constraints"] = constraints_mod

    group_mod = types.ModuleType("octop_gateway.group_context")
    group_mod.GroupContextConfig = GroupContextConfig
    sys.modules["octop_gateway.group_context"] = group_mod

    models_mod = types.ModuleType("octop_gateway.models")
    for name in (
        "TextContent",
        "ImageContent",
        "AudioContent",
        "VideoContent",
        "FileContent",
        "ChannelSubject",
        "InboundMessage",
    ):
        setattr(models_mod, name, getattr(namespace, name))
    models_mod.ContentPart = object
    sys.modules["octop_gateway.models"] = models_mod
    return namespace


def _load(path: Path, module_name: str) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_plugin(module_name: str = CHANNEL_MODULE) -> types.ModuleType:
    """Load the channel plugin under the module name the tools plugin imports.

    ``ONEBOT11_PLUGIN_MAIN`` points the tests at another copy of ``main.py``
    (e.g. an older revision, to prove a regression test really fails there).
    """
    override = os.environ.get("ONEBOT11_PLUGIN_MAIN")
    path = Path(override) if override else ROOT / "plugins" / "octop-onebot11" / "main.py"
    return _load(path, module_name)


def load_tools_plugin(module_name: str = TOOLS_MODULE) -> types.ModuleType:
    return _load(ROOT / "plugins" / "octop-onebot11-tools" / "main.py", module_name)
