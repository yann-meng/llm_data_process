from __future__ import annotations

import re

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?86[- ]?)?(?:1[3-9]\d{9}|\d{3}[- ]?\d{3}[- ]?\d{4})(?!\d)")
ID_CARD_RE = re.compile(r"(?<!\d)(?:\d{15}|\d{17}[\dXx])(?!\d)")


def redact_pii(
    text: str,
    replace_token: str = "[REDACTED]",
    email: bool = True,
    phone: bool = True,
    id_card: bool = True,
) -> str:
    out = text
    if email:
        out = EMAIL_RE.sub(replace_token, out)
    if phone:
        out = PHONE_RE.sub(replace_token, out)
    if id_card:
        out = ID_CARD_RE.sub(replace_token, out)
    return out
