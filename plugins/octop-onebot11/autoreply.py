"""Autonomous reply gate for group chats (R8).

``group_reply_mode = "auto"`` lets the bot join a conversation it was not
addressed in.  The plugin cannot (and should not) call a model by itself, so
this module only decides which *unaddressed* messages are worth handing to the
agent; the agent keeps the last word and may answer with the skip token when
it has nothing to add.

The gate is a small, deterministic scorer so its behaviour can be explained,
tuned and tested:

* evidence that the message wants an answer raises the score (a question, a
  plea for help, a follow-up soon after the bot itself spoke);
* evidence that jumping in would be noise lowers it (a few characters, a
  chat that is already moving fast);
* a message is handed over only when the score reaches the threshold *and*
  the group's cooldown and hourly budget allow it.

Costs are bounded by construction: at most ``max_per_hour`` model calls per
group, never closer together than ``cooldown_seconds``.  State is in memory and
capped, so an idle process holds nothing and a hostile group cannot grow it.
"""

from __future__ import annotations

import re
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field

_MAX_GROUPS = 256

_QUESTION_RE = re.compile(
    r"[?？]|怎么|怎样|如何|为什么|为啥|是不是|能不能|可不可以|有没有|谁知道|哪个|哪里|多少|吗\s*$|呢\s*$"
)
_HELP_RE = re.compile(r"求助|请问|求问|有人知道|有没有人|谁能|谁会|帮忙|帮我|救命|\bhelp\b", re.IGNORECASE)
_MEANINGFUL_RE = re.compile(r"[\w一-鿿]")

W_QUESTION = 0.30
W_HELP = 0.35
W_LENGTH = 0.10  # at least LONG_CHARS meaningful characters
W_FOLLOW_UP = 0.40  # scaled down linearly over FOLLOW_UP_WINDOW
W_REPLY_TO_BOT = 0.70  # the message quotes something the bot itself said
P_SHORT = 0.30  # fewer than SHORT_CHARS meaningful characters
P_BUSY = 0.30  # more than BUSY_COUNT messages inside BUSY_WINDOW

LONG_CHARS = 8
SHORT_CHARS = 4
FOLLOW_UP_WINDOW = 90.0
BUSY_WINDOW = 60.0
BUSY_COUNT = 8
HOUR = 3600.0


@dataclass(frozen=True)
class Decision:
    """Outcome of one evaluation (``blocked`` says why a candidate was held back)."""

    engage: bool
    score: float
    signals: tuple[str, ...] = ()
    blocked: str = ""  # "" | "threshold" | "cooldown" | "hourly-cap"

    @property
    def reason(self) -> str:
        return "auto:" + "+".join(self.signals) if self.signals else "auto"


@dataclass
class _GroupState:
    arrivals: deque[float] = field(default_factory=deque)
    engaged: deque[float] = field(default_factory=deque)
    bot_spoke_at: float | None = None


def score_text(text: str) -> tuple[float, list[str]]:
    """Content-only evidence: ``(score, signals)`` before any group state is applied."""
    body = str(text or "").strip()
    meaningful = len(_MEANINGFUL_RE.findall(body))
    score = 0.0
    signals: list[str] = []
    if meaningful < SHORT_CHARS:
        return -P_SHORT, ["short"]
    if _QUESTION_RE.search(body):
        score += W_QUESTION
        signals.append("question")
    if _HELP_RE.search(body):
        score += W_HELP
        signals.append("help")
    if meaningful >= LONG_CHARS:
        score += W_LENGTH
    return score, signals


class AutoReplyGate:
    """Per-group engagement budget plus the scorer above."""

    def __init__(
        self,
        *,
        threshold: float = 0.6,
        cooldown_seconds: float = 120.0,
        max_per_hour: int = 6,
    ) -> None:
        self.threshold = float(threshold)
        self.cooldown_seconds = max(0.0, float(cooldown_seconds))
        self.max_per_hour = max(1, int(max_per_hour))
        self._groups: OrderedDict[str, _GroupState] = OrderedDict()

    def _state(self, group_id: str) -> _GroupState:
        state = self._groups.get(group_id)
        if state is None:
            state = self._groups[group_id] = _GroupState()
            while len(self._groups) > _MAX_GROUPS:
                self._groups.popitem(last=False)
        else:
            self._groups.move_to_end(group_id)
        return state

    def arrive(self, group_id: str, *, now: float | None = None) -> None:
        """Record that a group message arrived (feeds the "chat is busy" penalty)."""
        moment = time.monotonic() if now is None else now
        arrivals = self._state(group_id).arrivals
        arrivals.append(moment)
        while arrivals and moment - arrivals[0] > BUSY_WINDOW:
            arrivals.popleft()

    def note_bot_spoke(self, group_id: str, *, now: float | None = None) -> None:
        """The bot said something in this group (a quick reply after it is likely a follow-up)."""
        self._state(group_id).bot_spoke_at = time.monotonic() if now is None else now

    def evaluate(
        self,
        group_id: str,
        text: str,
        *,
        now: float | None = None,
        reply_to_bot: bool = False,
    ) -> Decision:
        """Score an unaddressed message and apply cooldown and hourly budget.

        ``reply_to_bot`` means the message quotes one of the bot's own messages:
        it is effectively addressed to the bot, so it skips the short-text
        penalty and the cooldown (the hourly budget still applies).
        """
        moment = time.monotonic() if now is None else now
        self.arrive(group_id, now=moment)
        state = self._state(group_id)
        score, signals = score_text(text)
        if reply_to_bot:
            score = max(score, 0.0) + W_REPLY_TO_BOT
            signals = [sig for sig in signals if sig != "short"] + ["reply-to-bot"]
        if "short" not in signals:
            if state.bot_spoke_at is not None:
                age = moment - state.bot_spoke_at
                if 0 <= age < FOLLOW_UP_WINDOW:
                    score += W_FOLLOW_UP * (1.0 - age / FOLLOW_UP_WINDOW)
                    signals.append("follow-up")
            if len(state.arrivals) > BUSY_COUNT:
                score -= P_BUSY
                signals.append("busy")
        score = round(score, 3)
        if score < self.threshold:
            return Decision(False, score, tuple(signals), "threshold")
        while state.engaged and moment - state.engaged[0] > HOUR:
            state.engaged.popleft()
        if not reply_to_bot and state.engaged and moment - state.engaged[-1] < self.cooldown_seconds:
            return Decision(False, score, tuple(signals), "cooldown")
        if len(state.engaged) >= self.max_per_hour:
            return Decision(False, score, tuple(signals), "hourly-cap")
        state.engaged.append(moment)
        return Decision(True, score, tuple(signals))
