from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from functools import partial

from tqdm import tqdm

from .config import PipelineConfig
from .datatrove_flow import run_clean_filter_dedup
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
