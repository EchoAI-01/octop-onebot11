"""Inbound media helpers for the OneBot 11 channel (R11).

Everything here is free of Octop imports so it can be tested on its own:
the URL policy for fetching media on behalf of a chat peer, a size-capped
download, format sniffing, normalisation of NapCat's ``get_image`` /
``get_record`` replies, and a client for OpenAI-compatible speech-to-text
services (``POST {base_url}/audio/transcriptions``).

Trust model: image and voice URLs come from the chat platform, but the peer is
still a stranger in a group.  The plugin therefore only fetches hosts it has
reason to trust (QQ's CDN domains, the NapCat host itself, plus whatever the
operator lists) and never follows a redirect off that list.
"""

from __future__ import annotations

import base64
import binascii
import ipaddress
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlsplit

USER_AGENT = "Mozilla/5.0 (compatible; Octop-OneBot11/1.0)"

# Host names (and their sub-domains) that QQ serves chat media from.
DEFAULT_TRUSTED_HOSTS: tuple[str, ...] = (
    "qq.com",
    "qq.com.cn",
    "qpic.cn",
    "qlogo.cn",
    "gtimg.cn",
    "gtimg.com",
    "tencent.com",
    "myqcloud.com",
    "tencent-cloud.com",
    "weiyun.com",
)

_CHUNK = 64 * 1024
_ERROR_SNIPPET = 160


class MediaError(Exception):
    """A media step failed; the message is one readable line for the log."""


def parse_host_list(text: Any) -> tuple[str, ...]:
    """``"a.com, B.org"`` -> ``("a.com", "b.org")`` (blank and duplicate entries dropped)."""
    seen: dict[str, None] = {}
    for item in str(text or "").replace(";", ",").replace("\n", ",").split(","):
        host = item.strip().lower().rstrip(".")
        if host:
            seen.setdefault(host, None)
    return tuple(seen)


def host_of(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower().rstrip(".")
    except ValueError:
        return ""


def host_allowed(url: str, extra_hosts: Sequence[str] = ()) -> bool:
    """True for http(s) URLs whose host is a trusted QQ domain or listed by the operator.

    ``example.com`` matches itself and every sub-domain, never ``evilexample.com``.
    """
    value = str(url or "").strip()
    try:
        parts = urlsplit(value)
    except ValueError:
        return False
    if parts.scheme not in ("http", "https"):
        return False
    host = (parts.hostname or "").lower().rstrip(".")
    if not host:
        return False
    for entry in (*DEFAULT_TRUSTED_HOSTS, *extra_hosts):
        if host == entry:
            return True
        if host.endswith("." + entry) and not _is_ip_literal(entry):
            return True
    return False


def _is_ip_literal(value: str) -> bool:
    try:
        ipaddress.ip_address(value.strip("[]"))
    except ValueError:
        return False
    return True


_IMAGE_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
    (b"BM", "image/bmp"),
)


def sniff_image_mime(data: bytes) -> str:
    """Image MIME type from the leading bytes ('' when it is not a known image)."""
    head = bytes(data[:16])
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    for magic, mime in _IMAGE_MAGIC:
        if head.startswith(magic):
            return mime
    return ""


def sniff_audio_mime(data: bytes) -> str:
    """Audio MIME type from the leading bytes ('' when unknown).

    QQ voice notes are SILK or AMR; both are recognised so callers can tell
    "needs transcoding" from "ready for a speech-to-text service".
    """
    head = bytes(data[:16])
    if head[:4] == b"RIFF" and head[8:12] == b"WAVE":
        return "audio/wav"
    if head.startswith(b"ID3") or (len(head) >= 2 and head[0] == 0xFF and head[1] & 0xE0 == 0xE0):
        return "audio/mpeg"
    if head.startswith(b"OggS"):
        return "audio/ogg"
    if head.startswith(b"fLaC"):
        return "audio/flac"
    if head.startswith(b"#!AMR"):
        return "audio/amr"
    if b"#!SILK" in head:
        return "audio/silk"
    if head[4:8] == b"ftyp":
        return "audio/mp4"
    return ""


_EXTENSIONS = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
    "audio/wav": ".wav",
    "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
    "audio/flac": ".flac",
    "audio/amr": ".amr",
    "audio/silk": ".silk",
    "audio/mp4": ".m4a",
}

# Formats OpenAI-style transcription services accept.
ASR_ACCEPTED_MIME = frozenset({"audio/wav", "audio/mpeg", "audio/ogg", "audio/flac", "audio/mp4"})


def extension_for(mime: str, default: str = ".bin") -> str:
    return _EXTENSIONS.get(str(mime or "").split(";", 1)[0].strip().lower(), default)


@dataclass
class MediaInfo:
    """What a NapCat media action (``get_image`` / ``get_record`` / ``get_file``) handed back."""

    base64: str = ""
    url: str = ""
    file: str = ""
    name: str = ""
    size: int = 0


def parse_media_info(result: Any) -> MediaInfo:
    """Normalise the reply shapes seen across OneBot implementations.

    Accepts the bare payload or one wrapped in ``{"data": {...}}``; ``base64``
    may carry a ``base64://`` or ``data:...;base64,`` prefix; ``file`` is
    whatever the server calls the file (often a path on *its* machine).
    """
    if isinstance(result, dict) and isinstance(result.get("data"), dict) and "file" not in result:
        result = result["data"]
    if isinstance(result, str):
        result = {"file": result}
    if not isinstance(result, dict):
        return MediaInfo()
    encoded = str(result.get("base64") or "").strip()
    file_value = str(result.get("file") or "").strip()
    if not encoded and file_value.lower().startswith("base64://"):
        encoded, file_value = file_value, ""
    if encoded.lower().startswith("base64://"):
        encoded = encoded[len("base64://") :]
    elif encoded.lower().startswith("data:") and "," in encoded:
        encoded = encoded.split(",", 1)[1]
    url = str(result.get("url") or "").strip()
    try:
        size = int(str(result.get("file_size") or result.get("size") or 0))
    except ValueError:
        size = 0
    return MediaInfo(
        base64=encoded,
        url=url if url.lower().startswith(("http://", "https://")) else "",
        file=file_value,
        name=str(result.get("file_name") or result.get("name") or ""),
        size=max(0, size),
    )


def decode_base64_capped(value: str, max_bytes: int) -> bytes:
    """Decode base64 text, refusing payloads whose decoded size would exceed ``max_bytes``."""
    text = "".join(str(value or "").split())
    if (len(text) * 3) // 4 > max_bytes + 3:
        raise MediaError(f"媒体数据超过 {max_bytes} 字节上限")
    try:
        data = base64.b64decode(text, validate=False)
    except (binascii.Error, ValueError) as exc:
        raise MediaError(f"媒体 base64 无法解码: {exc}") from exc
    if not data:
        raise MediaError("媒体数据为空")
    if len(data) > max_bytes:
        raise MediaError(f"媒体数据超过 {max_bytes} 字节上限")
    return data


async def download(
    http: Any,
    url: str,
    *,
    max_bytes: int,
    timeout: float,
    allowed: Callable[[str], bool],
    headers: dict[str, str] | None = None,
    max_redirects: int = 3,
) -> tuple[bytes, str]:
    """GET ``url`` and return ``(bytes, content_type)``.

    The host policy ``allowed`` is applied to the first URL and to every
    redirect target; the body is streamed and aborted once it passes
    ``max_bytes``.
    """
    import aiohttp  # noqa: PLC0415 - optional at import time (offline stubs)

    merged = {"User-Agent": USER_AGENT, **(headers or {})}
    current = url
    for _ in range(max_redirects + 1):
        if not allowed(current):
            raise MediaError(f"地址不在可信媒体主机范围内: {host_of(current) or current[:60]}")
        async with http.get(
            current,
            headers=merged,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            if resp.status in (301, 302, 303, 307, 308):
                location = resp.headers.get("Location", "")
                if not location:
                    raise MediaError(f"重定向缺少 Location (HTTP {resp.status})")
                current = urljoin(current, location)
                continue
            if resp.status != 200:
                raise MediaError(f"下载失败 HTTP {resp.status}")
            declared = resp.content_length
            if declared is not None and declared > max_bytes:
                raise MediaError(f"媒体 {declared} 字节，超过 {max_bytes} 字节上限")
            body = bytearray()
            async for chunk in resp.content.iter_chunked(_CHUNK):
                body.extend(chunk)
                if len(body) > max_bytes:
                    raise MediaError(f"媒体超过 {max_bytes} 字节上限")
            if not body:
                raise MediaError("媒体内容为空")
            content_type = (resp.content_type or "").lower()
            return bytes(body), content_type
    raise MediaError("重定向次数过多")


@dataclass(frozen=True)
class AsrConfig:
    """OpenAI-compatible speech-to-text endpoint."""

    base_url: str
    api_key: str = ""
    model: str = "whisper-1"
    language: str = "zh"
    timeout: float = 30.0

    @property
    def endpoint(self) -> str:
        base = self.base_url.strip().rstrip("/")
        return base if base.endswith("/audio/transcriptions") else base + "/audio/transcriptions"


def _error_snippet(body: bytes) -> str:
    text = body[:_ERROR_SNIPPET * 4].decode("utf-8", errors="replace")
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = None
    if isinstance(parsed, dict):
        err = parsed.get("error")
        text = str(err.get("message") if isinstance(err, dict) else err or parsed.get("message") or text)
    return " ".join(text.split())[:_ERROR_SNIPPET]


async def transcribe(http: Any, config: AsrConfig, data: bytes, *, filename: str, mime: str) -> str:
    """Send audio to an OpenAI-style transcription endpoint and return the text.

    The API key is only ever placed in the request header; error messages never
    include it.
    """
    import aiohttp  # noqa: PLC0415

    form = aiohttp.FormData()
    form.add_field("file", data, filename=filename, content_type=mime or "application/octet-stream")
    form.add_field("model", config.model)
    if config.language:
        form.add_field("language", config.language)
    form.add_field("response_format", "json")
    headers = {"Authorization": f"Bearer {config.api_key}"} if config.api_key else {}
    try:
        async with http.post(
            config.endpoint,
            data=form,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=config.timeout),
        ) as resp:
            body = await resp.content.read(1024 * 1024)
            status = resp.status
            content_type = (resp.content_type or "").lower()
    except aiohttp.ClientError as exc:
        raise MediaError(f"语音识别服务不可达: {type(exc).__name__}") from exc
    if status >= 400:
        raise MediaError(f"语音识别服务返回 HTTP {status}: {_error_snippet(body)}")
    text = ""
    if "json" in content_type or body.lstrip()[:1] in (b"{", b"["):
        try:
            parsed = json.loads(body.decode("utf-8", errors="replace"))
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            text = str(parsed.get("text") or "")
    else:
        text = body.decode("utf-8", errors="replace")
    text = " ".join(text.split())
    if not text:
        raise MediaError("语音识别结果为空")
    return text
