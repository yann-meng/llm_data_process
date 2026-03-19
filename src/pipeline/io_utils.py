from __future__ import annotations

from pathlib import Path
from typing import Iterable

import orjson

from .markdown_parse import ParsedDoc


def iter_input_files(root: Path, patterns: list[str], ignore_patterns: list[str]) -> Iterable[Path]:
    ignored = set()
    for pat in ignore_patterns:
        ignored.update(root.glob(pat))

    for pat in patterns:
        for path in root.glob(pat):
            if path.is_file() and path not in ignored:
                yield path


def write_jsonl(records: Iterable[ParsedDoc], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as f:
        for item in records:
            payload = {
                "id": item.doc_id,
                "source": item.source_path,
                "text": item.text,
                "headings": item.headings,
                "code_blocks": item.code_blocks,
            }
            f.write(orjson.dumps(payload))
            f.write(b"\n")
