from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import trafilatura
from unstructured.partition.auto import partition


@dataclass(slots=True)
class MarkdownDoc:
    doc_id: str
    source_path: str
    ext: str
    markdown: str


def extract_html_with_trafilatura(path: Path) -> str:
    html = path.read_text(encoding="utf-8", errors="ignore")
    md = trafilatura.extract(
        html,
        output_format="markdown",
        include_comments=False,
        include_links=True,
        include_images=False,
    )
    return md or ""


def extract_with_unstructured(path: Path) -> str:
    elements = partition(filename=str(path))
    lines = [e.text.strip() for e in elements if getattr(e, "text", None)]
    return "\n\n".join(lines)


def to_markdown(path: Path, html_engine: str = "trafilatura") -> MarkdownDoc:
    ext = path.suffix.lower()
    if ext in {".html", ".htm"} and html_engine == "trafilatura":
        markdown = extract_html_with_trafilatura(path)
    else:
        markdown = extract_with_unstructured(path)

    return MarkdownDoc(
        doc_id=f"{path.stem}-{abs(hash(str(path)))}",
        source_path=str(path),
        ext=ext,
        markdown=markdown,
    )
