"""Agent skills for the OneBot 11 (NapCat) channel.

Documentation only: Octop copies every ``skills/<name>/SKILL.md`` registered
here into the agent workspace, where the agent reads it on demand.  The tools
these skills talk about are provided by ``octop-onebot11-tools``.
"""

from __future__ import annotations

from typing import Any


def setup(ctx: Any) -> None:
    ctx.skills("skills")
