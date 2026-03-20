#!/usr/bin/env bash
set -euo pipefail

python -m pipeline.cli -c configs/pipeline.yaml
