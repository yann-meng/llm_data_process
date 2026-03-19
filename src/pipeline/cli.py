from __future__ import annotations

from pathlib import Path

import typer
import yaml

from .config import PipelineConfig
from .runner import run_pipeline

app = typer.Typer(help="Large-scale LLM data process pipeline")


@app.command()
def run(config: Path = typer.Option(..., "--config", "-c", exists=True, readable=True)) -> None:
    cfg_dict = yaml.safe_load(config.read_text(encoding="utf-8"))
    cfg = PipelineConfig.model_validate(cfg_dict)
    run_pipeline(cfg)
    typer.echo(f"Done. JSONL saved to: {cfg.output.output_jsonl}")


if __name__ == "__main__":
    app()
