from __future__ import annotations

import math
import re
from dataclasses import dataclass


@dataclass(slots=True)
class QualityScore:
    rule_score: float
    perplexity: float | None
    passed: bool


def rule_based_score(text: str) -> float:
    if not text:
        return 0.0

    length_term = min(len(text) / 2000.0, 1.0)
    line_count = max(1, text.count("\n") + 1)
    avg_line_len = len(text) / line_count
    structure_term = 1.0 - min(abs(avg_line_len - 80.0) / 160.0, 1.0)
    bad_pattern_penalty = 0.2 if re.search(r"(lorem ipsum|asdfasdf|测试测试测试)", text.lower()) else 0.0

    symbol_ratio = sum(1 for ch in text if not ch.isalnum() and not ch.isspace()) / max(1, len(text))
    symbol_term = 1.0 - min(symbol_ratio / 0.5, 1.0)

    score = 0.5 * length_term + 0.3 * structure_term + 0.2 * symbol_term - bad_pattern_penalty
    return max(0.0, min(score, 1.0))


def mock_perplexity(text: str) -> float:
    tokens = re.findall(r"\S+", text)
    if not tokens:
        return math.inf

    unique_ratio = len(set(tokens)) / len(tokens)
    return max(20.0, 1400.0 * (1.0 - unique_ratio))


def quality_gate(
    text: str,
    min_rule_score: float,
    max_perplexity: float,
    ppl_backend: str = "mock",
) -> QualityScore:
    rule_score = rule_based_score(text)

    perplexity: float | None
    if ppl_backend == "none":
        perplexity = None
    else:
        perplexity = mock_perplexity(text)

    ppl_pass = True if perplexity is None else perplexity <= max_perplexity
    passed = rule_score >= min_rule_score and ppl_pass

    return QualityScore(rule_score=rule_score, perplexity=perplexity, passed=passed)
