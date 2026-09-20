# AstraTune 准确率规格（PG 530）

> **状态**：**PG 530 正式全量 51.13%（271/530）**；**Effi-SQL 全量评测已完成**（见 `docs/effi300评测报告.md`）；机制见 `docs/架构说明书.md`  
> **日期**：2026-09-07（Effi 评测收口同步）  
> **关联**：`docs/pg530评测报告.md`；`docs/effi300评测报告.md`；`docs/架构说明书.md`；`AGENTS.md`

---

## 0. 目标

| 项 | 内容 |
|----|------|
| GOAL | BIRD-CRITIC **PG 530** 官方 Overall Accuracy（现行正式成绩 **51.13%**） |
| 禁区 | 不向 Agent 喂 `sol_sql` / 官方 `test_cases`；不恢复 RAG/Reflect/微调/达梦 |

---

## 1. 验收集

| 集合 | 文件 | 说明 |
|------|------|------|
| **PG 530** | `outputs/pg530_r2_*`；`data/pg530_r2_pass/fail_ids.txt` | **51.13%（271/530）**；报告 `docs/pg530评测报告.md` |

---

## 2. 已落地工程改动（对照现行代码）

1. **沙箱 PG**：序列同步、列名 quote（含 `%`）、统计注入、PostGIS fail-open、`lock_timeout`
2. **抽表门禁**：`resolve_and_validate_tables` + `original_schema` 回退
3. **Soft EX**：仅 Efficiency 硬要求与 `issue_sql` 集合等价
4. **Verify Agent + fidelity**：`verify_mode=agent+rules`；采样下 **QEP 不硬否决**
5. **智能 reset**、模板缓存、`batch_preflight`
6. **批跑鲁棒性**：`--isolate subprocess`、有界在飞、`instance_timeout_s` 强杀、有 pred 不清空、slim/去重
7. **PG530 机制**：Intent Spec 类目 goal 权威、输出列契约、诊断换表约束、Pers 代表行、jsonb ARRAY（见 `docs/架构说明书.md` §4.8）

细节见 `docs/架构说明书.md`。

---

## 3. 验收

| 项 | 状态 |
|----|------|
| PG 530 全量官方 Overall | **51.13%（271/530）** |
| 评测报告 | `docs/pg530评测报告.md` |

---

## 4. 命令备忘（PG 530）

```bash
python scripts/run_bird_critic.py \
  --schema benchmark_data/schema/post_schema.jsonl \
  --input benchmark_data/input/post/pg-00000-of-00001.jsonl \
  --dialect postgresql --output outputs/pg530_pred.jsonl \
  --resume --workers 4 --mode diag+opt --force-batch

python scripts/merge_pred_with_sol.py \
  --pred outputs/pg530_pred.jsonl \
  --sol benchmark_data/sol/pg_sol/pg_sol.jsonl \
  --output outputs/pg530_eval_ready.jsonl --require-pred

# 省略 --limit = 全量 530
python scripts/run_official_eval.py \
  --dialect postgresql --jsonl outputs/pg530_eval_ready.jsonl --num_threads 2
```

`--limit` 约定见 `docs/评测对接.md` §2.1。

---

## 5. 剩余缺口与下一步

| 类 | 方向 | 代表 |
|----|------|------|
| Query / Pers | 断言失败 → 机制层优化（少 prompt 注入） | `docs/架构说明书.md` §4.8；`data/pg530_r2_fail_ids.txt` |
| Management | 相对较高，仍有断言/执行失败 | 同上 |
| **Effi-SQL** | **全量评测已完成**（match 91.7% / R-VES 0.7642 / Coverage@2x 55.67% / GMS 4.0814） | `docs/effi300评测报告.md`；链路 `docs/评测对接.md` §4 |
| **后续** | Effi 未 match dig（25 题，尤 tpch） | 同上报告 §3 |
| 延后 | Open-MySQL 全量 | — |
| P2 | WebUI 已实现 | `docs/spec_webui.md`；`python at_webui.py` |
