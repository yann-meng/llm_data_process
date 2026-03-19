# LLM Data Process（Trafilatura / Unstructured -> Markdown -> markdown-it-py -> Datatrove -> JSONL/Parquet/Lakehouse）

面向 10B 级语料的数据处理工程，覆盖以下完整链路并扩展了生产能力：

1. Trafilatura / Unstructured 抽取
2. Markdown 中间层
3. markdown-it-py 结构解析
4. Datatrove 风格清洗 + 过滤 + 去重
5. 跨 shard MinHash + LSH 全局去重
6. 质量评分（规则分 + 困惑度代理）和阈值裁剪
7. PII 清洗（邮箱 / 手机号 / 证件号）
8. JSONL + Parquet 输出
9. Delta / Iceberg 数据湖写入

---

## 1) 工程结构

```text
.
├── configs/
│   └── pipeline.yaml
├── scripts/
│   └── run_pipeline.sh
├── src/pipeline/
│   ├── __init__.py
│   ├── cli.py
│   ├── config.py
│   ├── datatrove_flow.py
│   ├── datatrove_native.py
│   ├── dedup_cross_shard.py
│   ├── extract.py
│   ├── io_utils.py
│   ├── lakehouse.py
│   ├── markdown_parse.py
│   ├── pii.py
│   ├── quality.py
│   └── runner.py
├── pyproject.toml
└── README.md
```

---

## 2) 核心能力映射

### A. Datatrove 原生 pipeline/executor 接入
- 文件：`src/pipeline/datatrove_native.py`
- 提供 `run_datatrove_native_pipeline(...)` 入口；通过延迟导入方式适配不同环境。
- 在 `runner.py` 中通过 `datatrove.enable_native` 开关触发。

### B. 跨 shard MinHash + LSH 去重
- 文件：`src/pipeline/dedup_cross_shard.py`
- 使用 `datasketch.MinHash` + `MinHashLSH` 做近似去重。
- `dedup.strategy=minhash` 时走全局近似去重；`exact` 时回落到 SHA1 精确去重。

### C. 质量打分 + 阈值裁剪
- 文件：`src/pipeline/quality.py`
- `rule_based_score`：长度、结构、符号比例等规则分。
- `mock_perplexity`：默认困惑度代理（可替换真实 LM 困惑度）。
- `quality_gate`：统一阈值决策。

### D. PII 清洗
- 文件：`src/pipeline/pii.py`
- 默认支持邮箱、手机号、中式证件号正则脱敏。

### E. Parquet / Delta / Iceberg
- 文件：`src/pipeline/lakehouse.py`
- `write_parquet`：落 parquet 文件
- `write_delta`：写 Delta Lake
- `write_iceberg`：写 Iceberg catalog/table

---

## 3) 配置说明（`configs/pipeline.yaml`）

新增关键配置：
- `input.shard_size`：shard 切分大小
- `dedup.*`：MinHash/LSH 参数
- `quality.*`：质量筛选参数
- `pii.*`：脱敏开关
- `output.output_parquet_dir`：parquet 输出目录
- `lakehouse.*`：Delta/Iceberg 写入参数
- `datatrove.enable_native`：Datatrove 原生执行开关

---

## 4) 运行

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

# 如果要开启 parquet/delta/iceberg
pip install -e .[lakehouse]

./scripts/run_pipeline.sh
# 或
python -m pipeline.cli run -c configs/pipeline.yaml
```

---

## 5) 10B 级落地建议

1. 单 shard 先做抽取/清洗/质量/PII/局部去重，产出中间 parquet。
2. 统一做跨 shard MinHash + LSH，全局去重后再写最终 JSONL。
3. 下游训练与检索尽量直接消费 parquet/delta/iceberg，减少 JSON 反序列化开销。
4. 困惑度建议切换为真实 LM（例如小模型批量打分），当前 `mock` 仅作框架位。
5. 建议将 `runner.py` 调度层迁移到 Ray/Spark，以便稳定承载 10B 级任务。
