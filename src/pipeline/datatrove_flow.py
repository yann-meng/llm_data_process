from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable

from .config import CleanConfig, DedupConfig, PiiConfig, QualityConfig
from .markdown_parse import ParsedDoc
from .pii import redact_pii
from .quality import quality_gate

from .config import CleanConfig, DedupConfig
from .markdown_parse import ParsedDoc



def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def pass_filters(text: str, cfg: CleanConfig) -> bool:
    if len(text) < cfg.min_chars:
        return False

    symbol_chars = sum(1 for ch in text if not ch.isalnum() and not ch.isspace())
    symbol_ratio = symbol_chars / max(1, len(text))
    if symbol_ratio > cfg.max_ratio_symbol:
        return False

    has_zh = any("\u4e00" <= ch <= "\u9fff" for ch in text)
    has_en = bool(re.search(r"[A-Za-z]", text))

    allowed = set(cfg.language_allow)
    return ("zh" in allowed and has_zh) or ("en" in allowed and has_en)


def _exact_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def deduplicate(docs: Iterable[ParsedDoc], cfg: DedupConfig) -> Iterable[ParsedDoc]:
    """
    Framework-level dedup stage.
    - exact: uses SHA1 fingerprint, memory-efficient and deterministic.
    - minhash: placeholder hook for swapping to Datatrove's large-scale fuzzy dedup stage.
    """
    seen: set[str] = set()

    for doc in docs:
        fp = _exact_hash(doc.text)
        if fp in seen:
            continue
        seen.add(fp)
        yield doc


def run_clean_filter_dedup(
    docs: Iterable[ParsedDoc],
    clean_cfg: CleanConfig,
    dedup_cfg: DedupConfig,
    quality_cfg: QualityConfig,
    pii_cfg: PiiConfig,
    docs: Iterable[ParsedDoc], clean_cfg: CleanConfig, dedup_cfg: DedupConfig

) -> Iterable[ParsedDoc]:
    cleaned = []
    for doc in docs:
        new_text = clean_text(doc.text)
        if not pass_filters(new_text, clean_cfg):
            continue

        if pii_cfg.enable:
            new_text = redact_pii(
                new_text,
                replace_token=pii_cfg.replace_token,
                email=pii_cfg.email,
                phone=pii_cfg.phone,
                id_card=pii_cfg.id_card,
            )

        if quality_cfg.enable:
            q = quality_gate(
                new_text,
                min_rule_score=quality_cfg.min_rule_score,
                max_perplexity=quality_cfg.max_perplexity,
                ppl_backend=quality_cfg.ppl_backend,
            )
            if not q.passed:
                continue

        cleaned.append(
            ParsedDoc(
                doc_id=doc.doc_id,
                source_path=doc.source_path,
                markdown=doc.markdown,
                text=new_text,
                headings=doc.headings,
                code_blocks=doc.code_blocks,
            )
        )

    return deduplicate(cleaned, dedup_cfg)
