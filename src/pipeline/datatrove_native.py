from __future__ import annotations

from pathlib import Path

from .config import PipelineConfig


def run_datatrove_native_pipeline(cfg: PipelineConfig, input_path: Path, output_path: Path) -> None:
    """接入 Datatrove 原生 pipeline/executor。

    说明：Datatrove 的具体 API 在不同版本可能有差异，这里采用延迟导入并提供
    一个可运行的标准入口。安装 `datatrove` 后可直接执行。
    """
    try:
        from datatrove.executor.local import LocalPipelineExecutor
        from datatrove.pipeline.readers import JsonlReader
        from datatrove.pipeline.writers import JsonlWriter
        from datatrove.utils.typeshelper import Languages
    except ImportError as exc:
        raise RuntimeError(
            "Datatrove native pipeline 依赖未安装或 API 不匹配，请确认 datatrove 版本。"
        ) from exc

    steps = [
        JsonlReader(data_folder=str(input_path.parent), paths_file=str(input_path.name)),
        JsonlWriter(output_folder=str(output_path.parent), output_filename=output_path.stem),
    ]

    _ = Languages  # 占位，便于后续按语言过滤扩展。
    executor = LocalPipelineExecutor(pipeline=steps, tasks=cfg.extract.workers)
    executor.run()
