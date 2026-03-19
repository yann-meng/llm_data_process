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
    shard_size: int = Field(default=2_000_000, description="每个 shard 最多处理的文件数")



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
    strategy: Literal["exact", "minhash"] = "minhash"
    shingle_size: int = 13
    num_perm: int = 128
    lsh_threshold: float = 0.85
    lsh_bands: int = 32


class QualityConfig(BaseModel):
    enable: bool = True
    min_rule_score: float = 0.45
    max_perplexity: float = 1200.0
    ppl_backend: Literal["none", "mock", "transformers"] = "mock"


class PiiConfig(BaseModel):
    enable: bool = True
    replace_token: str = "[REDACTED]"
    email: bool = True
    phone: bool = True
    id_card: bool = True

    strategy: Literal["exact", "minhash"] = "exact"
    shingle_size: int = 13
    num_perm: int = 128



class OutputConfig(BaseModel):
    output_jsonl: Path = Field(default=Path("data/output/corpus.jsonl"))
    output_parquet_dir: Path = Field(default=Path("data/output/parquet"))
    write_batch_size: int = 10_000


class LakehouseConfig(BaseModel):
    enable_delta: bool = False
    delta_uri: Path = Path("data/lakehouse/delta/corpus")
    enable_iceberg: bool = False
    iceberg_catalog_uri: str = "sqlite:///data/lakehouse/iceberg/catalog.db"
    iceberg_namespace: str = "default"
    iceberg_table: str = "corpus"


class DatatroveConfig(BaseModel):
    enable_native: bool = False
    task: Literal["extract", "dedup", "all"] = "all"



    write_batch_size: int = 10_000


class PipelineConfig(BaseModel):
    input: InputConfig
    extract: ExtractConfig = ExtractConfig()
    parse: ParseConfig = ParseConfig()
    clean: CleanConfig = CleanConfig()
    dedup: DedupConfig = DedupConfig()
    quality: QualityConfig = QualityConfig()
    pii: PiiConfig = PiiConfig()
    output: OutputConfig = OutputConfig()
    lakehouse: LakehouseConfig = LakehouseConfig()
    datatrove: DatatroveConfig = DatatroveConfig()
    output: OutputConfig = OutputConfig()
