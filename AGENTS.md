# AstraTune — Agent 工作指南

> 供后续 Agent 快速初始化上下文。细节与进度以 `docs/过程文档.md` 为准；机制以 `docs/架构说明书.md` 为准；准确率以 `docs/spec_acc.md` + `docs/flash200评测报告.md` 为准。P0/P1 历史规格已收敛进过程文档与架构说明书（仓库内不再单独维护 `spec_p0.md` / `spec_p1.md`）。

## 项目是什么

AstraTune：基于大模型与多智能体协同的数据库慢查询（及更广 SQL 问题）诊断与优化系统。研究叙事面向卫星互联网，**实现上泛化为通用 SQL 问题**；实验基准为 **BIRD-CRITIC**。

## 总目标（GOAL）

1. **诊断智能体**（ReAct + 沙箱）— **已调通**
2. **修复多智能体**（Generate–Verify，无 Reflect）— **已完成**
3. **BIRD-CRITIC 评测跑批** — Flash 200 全链路 + 准确率 **≥50% 已达标（51.00%）**；PG 530 / Open-MySQL 全量仍延后
4. **WebUI**（单条 SQL：诊断→优化→`pred_sqls`）— **未实现（P2）**

## 已砍掉 / 冻结（勿再实现）

| 项 | 决策 |
| ---- | ---- |
| 微调数据集 / Qwen3-32B 微调 | 砍掉；用基座 API（当前 `deepseek-v4-flash`，可换） |
| RAG / 经验库 | 砍掉；勿恢复 `rag_tool` |
| Reflect 智能体 / 知识沉淀 | **砍掉**（失败原因改由 Verify 回灌 Generate） |
| 达梦 DM | 冻结；`sandbox/dm_executor.py` 仅 `NotImplementedError` |
| 卫星互联网领域限定 | 泛化为通用 SQL |
| Verify 阶段 DDL/DML 硬拦 | **已取消**；安全靠沙箱隔离，只做 Soft EX / QEP / 超时 |

## 架构要点（易混）

- **沙箱**（AstraTune 内部）：Agent 诊断/优化时操作的 DB；从「生产库」采样 + 统计克隆。**`sandbox_fidelity` 已对 Diag/Generate/Verify 透明**（`utils/sandbox_fidelity.py`）。
- **Verify**：**Verify Agent + 规则信号**（`verify_mode=agent+rules`）；规则层见 `optimization/verify.py`。采样沙箱下 **QEP 不硬否决**。
- **BIRD-CRITIC 评测**（外部）：ephemeral copy 上执行 `pred_sqls` + `test_cases`；**Agent 不直接写评测库**。
- **批跑鲁棒性**：默认 `--isolate subprocess`（`scripts/run_bird_worker.py`），单题超时强杀；有界 `workers` 在飞；`pred` 有则不清空；输出 slim + 流式去重。
- 端到端：`诊断报告` → **Generate–Verify** → `pred_sqls` → merge sol → 官方 wrapper 评测（`--limit` 见 `docs/评测对接.md` §2.1）。

## BIRD 数据位置（本机）

| 用途 | 路径 |
| ---- | ---- |
| Schema/题面 | `benchmark/baseline/data/{post,open,flash}_schema.jsonl` |
| PG gold | `benchmark/evaluation/pg_sol/pg_sol.jsonl` |
| Open gold | `benchmark/evaluation/open_sol/open_sol.jsonl` |
| Flash gold | `benchmark/evaluation/flash_sol/` |
| PG dumps | `benchmark/evaluation/postgre_table_dumps/`（及 `docker/postgre_table_dumps/`） |
| MySQL dumps | `benchmark/evaluation/mysql_table_dumps/` |
| 官方评测 | `benchmark/evaluation/src/wrapper_evaluation_*.py` |

**推理时不要把** `sol_sql` **喂给 Agent**；仅评测合并时使用。

## 当前代码地图

| 路径 | 说明 |
| ---- | ---- |
| `diagnosis/` | 诊断 Agent（LangGraph ReAct），入口 `scripts/diag.py` |
| `optimization/` | Generate–Verify（无 Reflect），单例入口 `scripts/opt.py` |
| `scripts/infer.py` | 单例端到端：诊断 → 优化 → `pred_sqls` |
| `scripts/run_bird_critic.py` | BIRD 批量推理（默认 subprocess 隔离） |
| `scripts/run_bird_worker.py` | 单题子进程 worker |
| `scripts/slim_pred_jsonl.py` | pred JSONL 去重 + slim |
| `sandbox/` | PG/MySQL 执行器、`SandboxManager`、`statistics.py`、模板缓存 |
| `tools/sandbox_tool.py` | LangChain `execute_sql` |
| `config/*.json` | agents / sandbox / target_db |
| `docker/` | PG target:5432、PG sandbox:5433、MySQL target:3306、MySQL sandbox:3307、API:8010 |
| `benchmark/` | BIRD-CRITIC 源码、dumps、sol、评测 |
| `data/eff_subset.jsonl` | 30 条 PG Efficiency 调试子集 |
| `at_webui.py` | 空，P2 |
| `docs/环境搭建.md` | 跨平台启动 |
| `docs/spec_acc.md` | **准确率规格（Flash ≥50% 已达标；val28 ≥60% 已过）** |
| `docs/flash200评测报告.md` | **Flash 200 官方评测（51.00%）** |
| `docs/flash_val28评测报告.md` | val28 官方评测（71.43%） |
| `docs/flash_hard40评测报告.md` | hard40 官方评测（60.00%） |
| `docs/架构说明书.md` | **模块实现与机制（权威）** |
| `docs/评测对接.md` | merge sol + 官方 wrapper；**`--limit` 必读 §2.1** |

## 开发优先级

- **P0 / P1**：已完成（Generate–Verify、Flash 全链路工程、并发/对齐/消融）
- **准确率**：**Flash 200 51.00%（102/200）已过 ≥50%**；val28 71.43% 已过 ≥60% — 见 `docs/flash200评测报告.md`
- **后续**：Query/Pers 断言失败聚类；val28 全量漂移题；PG/Open-MySQL 全量延后
- **P2**：WebUI、compose 彻底统一

## 约定

- 回答与文档：中文（用户规则另有口吻要求时从其规则）。
- 勿擅自提交 git / push，除非用户明确要求。
- 勿引入 RAG、微调管线、达梦实现。
- 诊断结论标记：`【诊断结论】`；评测交付字段：`pred_sqls`（`list[str]`）。
- LLM：`config/agents.json` + **`OPENAI_API_KEY` / `OPENAI_API_KEYS`**（`;` 分隔）；当前 `llm_key_mode=auto`，`llm_workers=4`。批跑沙箱 `--workers` 与 LLM 池独立。默认 `--isolate subprocess`。
- 单条示例见 `data/测试数据.md`；`--db-id` 可覆盖目标库名。

## 环境速查

```bash
conda activate at
cd docker && docker compose --env-file stack.defaults.env up -d
cd scripts
python diag.py --db-id financial --sql "..." --user-prompt "..."
python opt.py --db-id financial --sql "..." --diagnosis "【诊断结论】..."
python infer.py --db-id financial --sql "..." --user-prompt "..."
```

Flash 200 复现：

```bash
python scripts/run_bird_critic.py \
  --schema benchmark/baseline/data/flash_schema.jsonl \
  --input benchmark/baseline/data/flash/flash-00000-of-00001.jsonl \
  --dialect postgresql --output outputs/flash200_pred.jsonl \
  --resume --workers 4 --mode diag+opt --force-batch
python scripts/merge_pred_with_sol.py \
  --pred outputs/flash200_pred.jsonl \
  --sol benchmark/evaluation/flash_sol/bird-critic-1.0-flash_w_sol.jsonl \
  --output outputs/flash200_eval_ready.jsonl --require-pred
python scripts/run_official_eval.py \
  --dialect postgresql --jsonl outputs/flash200_eval_ready.jsonl --num_threads 2
```

样例命令见 `data/测试数据.md`。目标库默认 `root/123123@5432`（PG）/ `3306`（MySQL）；沙箱 PG `5433`、MySQL `3307`。

## Docker 磁盘（必读，防中断）

本机 Docker Desktop 数据盘在 **D:**，虚拟磁盘路径：

`D:\Docker_wsl\DockerDesktopWSL\disk\docker_data.vhdx`

**教训**：大批量 Flash 评测后 vhdx 会虚胖；`docker system df` 可能低估。跑批前建议 D: 保留 **≥30–50GB**。

压缩步骤：`compose down` → 可选 `scripts/reclaim_sandbox_disk.py` → 退出 Docker Desktop → `wsl --shutdown` → 管理员 `scripts/compact_docker_vhdx.ps1` → 再 `compose up -d`。

细节见 `docs/flash_val28评测报告.md` 附录 D。
