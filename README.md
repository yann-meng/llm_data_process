
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

## 1 工程结构
# LLM Data Process（Trafilatura / Unstructured -> Markdown -> markdown-it-py -> Datatrove -> JSONL）

这是一个面向**超大规模语料（10B 量级）**的数据工程框架，重点覆盖你给出的处理链路：

1. Trafilatura / Unstructured
2. Markdown 统一中间层
3. markdown-it-py 解析
4. Datatrove 风格清洗 + 过滤 + 去重
5. JSONL 输出

> 适配数据类型：Markdown、PDF、HTML、PPT、Word、TXT 等；中英文混合（中文占比高、英文较少）以及代码文本。


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
│   ├── extract.py
│   ├── io_utils.py
│   ├── markdown_parse.py
│   └── runner.py
├── pyproject.toml
└── README.md
```


## 2 核心能力映射

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



## 3 配置说明（`configs/pipeline.yaml`）

新增关键配置：
- `input.shard_size`：shard 切分大小
- `dedup.*`：MinHash/LSH 参数
- `quality.*`：质量筛选参数
- `pii.*`：脱敏开关
- `output.output_parquet_dir`：parquet 输出目录
- `lakehouse.*`：Delta/Iceberg 写入参数
- `datatrove.enable_native`：Datatrove 原生执行开关



## 3. 安装与运行

### 安装

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .


# 如果要开启 parquet/delta/iceberg
pip install -e .[lakehouse]

```

### 配置

编辑 `configs/pipeline.yaml`：
- `input.input_root` 指向原始语料目录
- `extract.workers` 按机器核数调整
- `clean.language_allow` 建议保持 `["zh", "en"]`

### 执行

```bash

./scripts/run_pipeline.sh
# 或
python -m pipeline.cli -c configs/pipeline.yaml
```


## 4. 10B 规模建议（落地重点）

1. **分片执行（强烈建议）**
   - 按目录/时间/来源做 shard（例如每 shard 5M~20M 文档）。
   - 每个 shard 产出独立 JSONL + 统计文件。

2. **两阶段去重**
   - 阶段1：shard 内 exact dedup（当前已支持）。
   - 阶段2：全局 near-dedup（MinHash/LSH，建议接 Datatrove 分布式组件）。

3. **分布式调度**
   - 当前 `runner.py` 使用本机 `ProcessPoolExecutor`，适合单机/单节点。
   - 10B 建议迁移到 Ray / Spark / Slurm 批处理，并保持本仓库的模块边界不变。

4. **中英文与代码混合语料**
   - 中文：避免仅英文规则过滤，当前逻辑已支持 CJK 检测。
   - 代码：保留 fenced code 与行内文本，避免误删技术文档。

5. **可观测性**
   - 为每个 stage 增加指标：输入数、通过率、平均长度、去重率。
   - 建议接 Prometheus + Grafana 或至少 CSV 指标汇总。


