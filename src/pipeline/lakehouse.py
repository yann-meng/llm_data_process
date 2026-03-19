from __future__ import annotations

from pathlib import Path


def write_parquet(records: list[dict], output_dir: Path) -> Path:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("写 parquet 需要安装 pyarrow。") from exc

    output_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    out = output_dir / "corpus.parquet"
    pq.write_table(table, out)
    return out


def write_delta(records: list[dict], uri: Path) -> None:
    try:
        from deltalake.writer import write_deltalake
    except ImportError as exc:
        raise RuntimeError("写 Delta 需要安装 deltalake。") from exc

    write_deltalake(str(uri), records, mode="append")


def write_iceberg(
    records: list[dict],
    catalog_uri: str,
    namespace: str,
    table_name: str,
) -> None:
    try:
        import pyarrow as pa
        from pyiceberg.catalog import load_catalog
    except ImportError as exc:
        raise RuntimeError("写 Iceberg 需要安装 pyiceberg + pyarrow。") from exc

    catalog = load_catalog("default", uri=catalog_uri)
    ns_tuple = tuple(namespace.split("."))
    if not catalog.namespace_exists(ns_tuple):
        catalog.create_namespace(ns_tuple)

    identifier = (*ns_tuple, table_name)
    if not catalog.table_exists(identifier):
        schema = pa.Table.from_pylist(records).schema
        catalog.create_table(identifier, schema=schema)

    table = catalog.load_table(identifier)
    arrow_table = pa.Table.from_pylist(records)
    table.append(arrow_table)
