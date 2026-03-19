from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class InputConfig(BaseModel):
    input_root: Path = Field(description="原始文件根目录")
    glob_patterns: list[str] = Field(
        default=["**/*.html", "**/*.htm", "**/*.md", "**/*.txt", "**/*.pdf", "**/*.docx", "**/*.pptx"],
        description="扫描输入文件的匹配模式",
    )
    ignore_patterns: list[str] = Field(default=["**/~$*", "**/.DS_Store"])


class ExtractConfig(BaseModel):
    html_engine: Literal["trafilatura", "unstructured"] = "trafilatura"
    workers: int = 16
    max_chars_per_doc: int = 4_000_000


class ParseConfig(BaseModel):
    keep_token_types: list[str] = Field(default=["paragraph_open", "heading_open", "fence", "inline"])


class CleanConfig(BaseModel):
    min_chars: int = 200
    max_ratio_symbol: float = 0.35
    language_allow: list[str] = Field(default=["zh", "en"])


class DedupConfig(BaseModel):
    strategy: Literal["exact", "minhash"] = "exact"
    shingle_size: int = 13
    num_perm: int = 128


class OutputConfig(BaseModel):
    output_jsonl: Path = Field(default=Path("data/output/corpus.jsonl"))
    write_batch_size: int = 10_000


class PipelineConfig(BaseModel):
    input: InputConfig
    extract: ExtractConfig = ExtractConfig()
    parse: ParseConfig = ParseConfig()
    clean: CleanConfig = CleanConfig()
    dedup: DedupConfig = DedupConfig()
    output: OutputConfig = OutputConfig()
