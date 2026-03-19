from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha1

from datasketch import MinHash, MinHashLSH

from .config import DedupConfig
from .markdown_parse import ParsedDoc


def _shingles(text: str, k: int) -> set[str]:
    if len(text) <= k:
        return {text}
    return {text[i : i + k] for i in range(len(text) - k + 1)}


def _minhash(text: str, k: int, num_perm: int) -> MinHash:
    mh = MinHash(num_perm=num_perm)
    for sh in _shingles(text, k):
        mh.update(sh.encode("utf-8", errors="ignore"))
    return mh


def global_minhash_lsh_dedup(docs: Iterable[ParsedDoc], cfg: DedupConfig) -> list[ParsedDoc]:
    """跨 shard 近似去重。

    基于 MinHash + LSH：同桶候选中保留首条文档。
    """
    if cfg.strategy == "exact":
        out: list[ParsedDoc] = []
        seen: set[str] = set()
        for doc in docs:
            fp = sha1(doc.text.encode("utf-8", errors="ignore")).hexdigest()
            if fp in seen:
                continue
            seen.add(fp)
            out.append(doc)
        return out

    lsh = MinHashLSH(threshold=cfg.lsh_threshold, num_perm=cfg.num_perm)
    kept: list[ParsedDoc] = []

    for doc in docs:
        mh = _minhash(doc.text, cfg.shingle_size, cfg.num_perm)
        candidates = lsh.query(mh)
        if candidates:
            continue
        lsh.insert(doc.doc_id, mh)
        kept.append(doc)

    return kept
