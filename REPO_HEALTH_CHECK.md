# 仓库检查报告（2026-03-19）

本次检查聚焦四类问题：**格式（format）**、**编码（encoding）**、**内容（content）**、**排版（layout）**。

## 1) 格式（Format）

### 1.1 `pyproject.toml` 存在重复键（已修复）
- `[project]` 下的 `version` 与 `description` 被重复定义，导致 TOML 解析失败，`ruff` 无法执行。
- 已保留较新的一组配置（`version = "0.2.0"` 与带 Lakehouse 描述的 `description`），删除旧重复项。

影响：工具链（lint/格式检查）会直接中断。

### 1.2 Ruff 检查仍存在多项问题（未在本次批量修复）
`ruff check .` 仍报错，主要包括：
- `runner.py` 中重复导入、重复函数定义、导入位置错误（E402/F811/I001）。
- 多处长行超限（E501）。
- `cli.py` 的 `typer.Option` 默认参数模式触发 B008。
- `io_utils.py` 的 `typing.Iterable` 触发 UP035（建议改 `collections.abc.Iterable`）。

## 2) 编码（Encoding）

使用脚本扫描仓库文本文件后：
- 未发现非 UTF-8 编码文件。
- 未发现 CRLF 行结尾。
- 未发现末尾缺失换行。
- 未发现尾随空格与 Tab 缩进。

结论：编码与基础文本规范整体良好。

## 3) 内容（Content）

### 3.1 `datatrove_flow.py` 存在明显“合并污染”痕迹（已修复关键阻断项）
- 文件内曾出现重复 import。
- `run_clean_filter_dedup` 函数参数曾被重复拼接（`docs/clean_cfg/dedup_cfg` 重复），造成语法错误。

已修复为单一、合法函数签名，恢复可解释性与可维护性。

### 3.2 `runner.py` 仍有内容一致性问题（待后续）
- 同一文件中存在两版 `run_pipeline` 定义。
- 模块尾部出现重复导入语句。
- 这些问题虽然不一定触发语法错误，但会导致语义混乱、覆盖行为不透明。

## 4) 排版（Layout）

- 主要问题体现在代码结构层面的“排版/组织”：导入顺序、重复块、函数重定义。
- 文档排版（README）本次未发现阻断性问题，但建议后续补充“开发检查命令”章节（如 `ruff check .`、`pytest -q`、`python -m compileall -q src`）。

## 建议优先级

1. **P0**：清理 `src/pipeline/runner.py` 的重复定义与重复导入，保证单一实现来源。
2. **P1**：统一执行 `ruff check . --fix`（先自动修复，再人工处理 B008/E501 等）。
3. **P2**：补充最小测试集（当前 `pytest -q` 显示无测试）。
4. **P3**：在 README 新增“本地质量门禁”章节，降低回归风险。

