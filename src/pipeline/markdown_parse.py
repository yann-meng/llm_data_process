from __future__ import annotations

from dataclasses import dataclass

from markdown_it import MarkdownIt

from .extract import MarkdownDoc


@dataclass(slots=True)
class ParsedDoc:
    doc_id: str
    source_path: str
    markdown: str
    text: str
    headings: list[str]
    code_blocks: int


_MD = MarkdownIt("commonmark", {"breaks": False, "html": False})


def parse_markdown(doc: MarkdownDoc) -> ParsedDoc:
    tokens = _MD.parse(doc.markdown)
    headings: list[str] = []
    text_chunks: list[str] = []
    code_blocks = 0

    for i, token in enumerate(tokens):
        if token.type == "heading_open":
            inline_token = tokens[i + 1] if i + 1 < len(tokens) else None
            if inline_token and inline_token.type == "inline":
                headings.append(inline_token.content.strip())
        elif token.type == "inline" and token.content.strip():
            text_chunks.append(token.content.strip())
        elif token.type == "fence":
            code_blocks += 1
            if token.content.strip():
                text_chunks.append(token.content)

    return ParsedDoc(
        doc_id=doc.doc_id,
        source_path=doc.source_path,
        markdown=doc.markdown,
        text="\n".join(text_chunks),
        headings=headings,
        code_blocks=code_blocks,
    )
