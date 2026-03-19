from __future__ import annotations
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from itertools import islice
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from tqdm import tqdm

from .config import PipelineConfig
from .datatrove_flow import run_clean_filter_dedup
from .datatrove_native import run_datatrove_native_pipeline
from .dedup_cross_shard import global_minhash_lsh_dedup
from .extract import to_markdown
from .io_utils import iter_input_files, to_record, write_jsonl
from .lakehouse import write_delta, write_iceberg, write_parquet
from .markdown_parse import ParsedDoc, parse_markdown


def _chunked(items: Iterable, size: int):
    it = iter(items)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def _process_shard(files: list, cfg: PipelineConfig) -> list[ParsedDoc]:
    extract_fn = partial(to_markdown, html_engine=cfg.extract.html_engine)
    with ProcessPoolExecutor(max_workers=cfg.extract.workers) as executor:
        markdown_docs = list(executor.map(extract_fn, files))

    parsed_docs = [parse_markdown(d) for d in markdown_docs]
    return list(
        run_clean_filter_dedup(
            parsed_docs,
            cfg.clean,
            cfg.dedup,
            cfg.quality,
            cfg.pii,
        )
    )


def run_pipeline(cfg: PipelineConfig) -> None:
    files_iter = iter_input_files(cfg.input.input_root, cfg.input.glob_patterns, cfg.input.ignore_patterns)

    shard_outputs: list[ParsedDoc] = []
    for idx, shard in enumerate(tqdm(_chunked(files_iter, cfg.input.shard_size), desc="shards"), start=1):
        docs = _process_shard(shard, cfg)
        shard_outputs.extend(docs)
        print(f"[shard-{idx}] input={len(shard)} kept={len(docs)}")

    final_docs = global_minhash_lsh_dedup(shard_outputs, cfg.dedup)
    write_jsonl(final_docs, cfg.output.output_jsonl)

    records = [to_record(d) for d in final_docs]
    write_parquet(records, cfg.output.output_parquet_dir)

    if cfg.lakehouse.enable_delta:
        write_delta(records, cfg.lakehouse.delta_uri)

    if cfg.lakehouse.enable_iceberg:
        write_iceberg(
            records,
            catalog_uri=cfg.lakehouse.iceberg_catalog_uri,
            namespace=cfg.lakehouse.iceberg_namespace,
            table_name=cfg.lakehouse.iceberg_table,
        )

    if cfg.datatrove.enable_native:
        run_datatrove_native_pipeline(cfg, cfg.output.output_jsonl, cfg.output.output_jsonl)
from .extract import to_markdown
from .io_utils import iter_input_files, write_jsonl
from .markdown_parse import parse_markdown


def run_pipeline(cfg: PipelineConfig) -> None:
    files = list(iter_input_files(cfg.input.input_root, cfg.input.glob_patterns, cfg.input.ignore_patterns))

    extract_fn = partial(to_markdown, html_engine=cfg.extract.html_engine)
    with ProcessPoolExecutor(max_workers=cfg.extract.workers) as executor:
        markdown_docs = list(
            tqdm(
                executor.map(extract_fn, files),
                total=len(files),
                desc="extract->markdown",
            )
        )

    parsed_docs = [parse_markdown(d) for d in tqdm(markdown_docs, desc="markdown-it parse")]

    final_docs = run_clean_filter_dedup(parsed_docs, cfg.clean, cfg.dedup)

    write_jsonl(final_docs, cfg.output.output_jsonl)
