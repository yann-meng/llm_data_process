#!/usr/bin/env bash
set -euo pipefail

python -m pipeline.cli run -c configs/pipeline.yaml
