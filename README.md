# LLM Data Process（Trafilatura / Unstructured -> Markdown -> markdown-it-py -> Datatrove -> JSONL）

这是一个面向**超大规模语料（10B 量级）**的数据工程框架，重点覆盖你给出的处理链路：

1. Trafilatura / Unstructured
2. Markdown 统一中间层
3. markdown-it-py 解析
4. Datatrove 风格清洗 + 过滤 + 去重
5. JSONL 输出

> 适配数据类型：Markdown、PDF、HTML、PPT、Word、TXT 等；中英文混合（中文占比高、英文较少）以及代码文本。

---

## 1. 工程结构

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
│   ├── extract.py
│   ├── io_utils.py
│   ├── markdown_parse.py
│   └── runner.py
├── pyproject.toml
└── README.md
```

---

## 2. 处理流程映射

### Stage A: 多格式抽取 -> Markdown
- `extract.py`
  - HTML/HTM 优先用 `trafilatura` 提取为 Markdown。
  - 其它格式（pdf/docx/pptx/md/txt）通过 `unstructured.partition.auto` 统一提取。

### Stage B: Markdown 结构解析
- `markdown_parse.py`
  - 用 `markdown-it-py` 把 Markdown 解析成 token。
  - 抽取结构信息：标题、正文、代码块数量。

### Stage C: 清洗 + 过滤 + 去重
- `datatrove_flow.py`
  - `clean_text`：去 NUL、合并空行、压缩空白。
  - `pass_filters`：最小长度、符号比例、中英语言门控。
  - `deduplicate`：默认 exact hash（SHA1）。
  - 预留 `minhash` 策略接口，后续可替换为 Datatrove 的大规模模糊去重。

### Stage D: JSONL 落盘
- `io_utils.py`
  - 按行写出 JSONL，字段包含：
    - `id`
    - `source`
    - `text`
    - `headings`
    - `code_blocks`

---

## 3. 安装与运行

### 安装

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
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
python -m pipeline.cli run -c configs/pipeline.yaml
```

---

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

---

## 5. 下一步可扩展项

- [ ] 接入 Datatrove 原生 pipeline/executor（本框架已留出 `datatrove_flow.py` 入口）。
- [ ] 增加 MinHash + LSH 的跨 shard 去重任务。
- [ ] 增加质量打分（困惑度/规则分）并做阈值裁剪。
- [ ] 增加 PII 清洗（邮箱/电话/证件号等）。
- [ ] 增加 parquet 输出与 Delta/Iceberg 数据湖写入。

