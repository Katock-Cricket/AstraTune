# Effi-SQL 16 题小规模官方评测报告

> **正式评测**：2026-08-25 20:25（UTC+8）— **match_rate 93.8%（15/16）**；**R-VES 0.7500**  
> **链路**：`run_effi_sql.py`（`--function efficiency --mode diag+opt`，`--isolate subprocess`）→ Diff-SQL `eval_postgresql_execution.py`（`EVAL_SQL_MODE=end2end`）→ `_aggregate_metrics.py`  
> **评测范围**：跨 4 库各 4 题，共 **16** 条（非全量 300）  
> **数据路径**：`benchmark_data/effi/input/effi-sql-pg.jsonl`；评测库 Diff-SQL `:7001` / `:5438`  
> **配置**：`llm_key_mode=single`；批跑 `--workers 6`（每库至多 1 路在飞）；子进程各 1 LLM 槽

**题号**：`1,2,3,4`（tpch） / `229,231,232,233`（solar_panel） / `230,234,235,241`（robot_fault_prediction） / `239,245,246,247`（polar_equipment）

---

## 1. 总览

| 指标 | 结果 |
| ---- | ---- |
| 实例数 | 16 |
| **match_rate** | **93.8%（15/16）** |
| **R-VES** | **0.7500** |
| Reward（官方计数） | 13 |
| SQL Extract Failed | 1 |
| 结果集不匹配 | 0 |
| 推理侧 `inference_ok` | 13/16 |
| 推理侧有 `prediction` | 15/16 |

### 按测评库（官方结果回放）

| db | 条数 | Match | match_rate | Reward 合计 | R-VES（库内均值） |
| ---- | ---- | ---- | ---- | ---- | ---- |
| solar_panel | 4 | 4 | **100%** | 4 | 0.8125 |
| robot_fault_prediction | 4 | 4 | **100%** | 3 | 0.7500 |
| polar_equipment | 4 | 4 | **100%** | 3 | 0.7500 |
| tpch | 4 | 3 | **75%** | 3 | 0.6875 |
| **Total** | **16** | **15** | **93.8%** | **13** | **0.7500** |

说明：R-VES 按 Diff-SQL `_aggregate_metrics.r_ves_score(speedup)`，以 **processed=16** 为分母；Reward 与 R-VES 分段阈值不完全相同（如 match 且 speedup∈[1,1.2) 可有 R-VES=0.5 而 Reward=0）。

---

## 2. 产物

| 文件 | 说明 |
| ---- | ---- |
| `outputs/effi16_pred.jsonl` | 16 题推理 pred（含 `prediction` / `pred_sqls`） |
| `outputs/effi16_pred_v3.log` / `effi16_v3_console.log` | 最终 resume 批跑日志 |
| `Diff-SQL/data/benchmark/effi-sql/astratune_effi16_pred.jsonl` | 送入 Diff-SQL 的评测输入副本 |
| `Diff-SQL/outputs/postgres/astratune_effi16_pred.jsonl` | 官方 end2end 逐题结果 |
| `outputs/effi16_eval_console.log` | 评测控制台（Processed/Match/Reward） |

---

## 3. 逐题摘要

| id | db | Match | Reward | speedup | R-VES | 备注 |
| -- | -- | ---- | ---- | ---- | ---- | ---- |
| 1 | tpch | ✗ | 0 | — | 0 | `sql_extract_failed`（推理 hard_fail / 空 pred；曾被 OOM 强杀后未恢复） |
| 2 | tpch | ✓ | 1 | 156.72 | 1.00 | Soft EX 截断跳过后仍产出可用 pred |
| 3 | tpch | ✓ | 1 | 2.17 | 1.00 | 推理 `fallback=True`，评测仍 match |
| 4 | tpch | ✓ | 1 | 1.68 | 0.75 | |
| 229 | solar_panel | ✓ | 1 | 1.43 | 0.75 | |
| 230 | robot_fault_prediction | ✓ | 0 | 1.03 | 0.50 | match 但加速不足 Reward 阈值 |
| 231 | solar_panel | ✓ | 1 | 1.50 | 0.75 | |
| 232 | solar_panel | ✓ | 1 | 1.61 | 0.75 | |
| 233 | solar_panel | ✓ | 1 | 4.19 | 1.00 | |
| 234 | robot_fault_prediction | ✓ | 1 | 10.44 | 1.00 | |
| 235 | robot_fault_prediction | ✓ | 1 | 1.55 | 0.75 | |
| 239 | polar_equipment | ✓ | 1 | 1.86 | 0.75 | |
| 241 | robot_fault_prediction | ✓ | 1 | 1.38 | 0.75 | |
| 245 | polar_equipment | ✓ | 0 | 1.17 | 0.50 | 同 230 |
| 246 | polar_equipment | ✓ | 1 | 1.24 | 0.75 | 推理 `fallback=True` |
| 247 | polar_equipment | ✓ | 1 | 9.42 | 1.00 | |

---

## 4. 推理 / 工程侧异常

| 项 | 现象 |
| ---- | ---- |
| EffiSQL_1 | TPC-H 大结果 Soft EX 曾单题吃到数十 GB；强杀后 hard_fail、无 `prediction` → 官方 `sql_extract_failed` |
| 批跑 | 已改为默认 `--isolate subprocess` + 每库 1 路 + `max_fetch_rows` 截断后 `rollback`（见 `docs/过程文档.md` Effi 踩坑） |
| CamelCase | `polar_equipment` 表名需 `::regclass` 内嵌双引号（已修） |
| 评测入口 | 本机 `bash` 为 WSL 启动器时 `run_effi_eval.py` 易挂；本次用 `docker exec` 直跑 |

---

## 5. 简要结论

1. **小规模 16 题官方 match 93.8%（15/16），R-VES 0.7500**；唯一失配是空 pred 的 tpch#1。
2. 三套 smalldb（solar / robot / polar）**全部 match**；tpch 3/4，短板仍是超大结果集题的推理稳定性。
3. Reward（13）低于 Match（15）：两题 match 但 speedup 仅略大于 1（#230、#245），符合 Diff-SQL 计分分段。
4. **全量已完成**：见 [`docs/effi300评测报告.md`](effi300评测报告.md)（match 91.7% / R-VES 0.7642）。
