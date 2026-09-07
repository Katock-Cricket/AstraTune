# Effi-SQL 全量 300 官方评测报告

> **正式评测**：2026-08-27 15:07（UTC+8）— **match_rate 91.7%（275/300）**；**R-VES 0.7642（76.42%）**；**Coverage@2x 55.67%**；**GMS 4.0814**  
> **链路**：`run_effi_sql.py`（`--function efficiency --mode diag+opt`，`--isolate subprocess`）→ Diff-SQL `eval_postgresql_execution.py`（`EVAL_SQL_MODE=end2end`）→ `_aggregate_metrics.py`（R-VES）+ 同产物回放 Coverage@2x / GMS（BIRD 榜单口径）  
> **评测范围**：JSONL **全量 300 条**（`benchmark_data/effi/input/effi-sql-pg.jsonl`）  
> **评测库**：Diff-SQL smalldb `:7001` / TPC-H `:5438`  
> **配置**：`llm_key_mode=single`；批跑 `--workers 4`（每库至多 1 路在飞）；子进程各 1 LLM 槽；`instance_timeout_s=2400`（空 pred 补跑加长至 4800）

---

## 1. 总览

| 指标 | 结果 |
| ---- | ---- |
| 实例数 | 300 |
| **match_rate** | **91.7%（275/300）** |
| **R-VES** | **0.7642（榜单写法 76.42%）** |
| **Coverage@2x** | **55.67%（167/300）** |
| **GMS** | **4.0814** |
| Reward（官方计数） | 205 |
| 推理侧有 `prediction` | **300/300** |
| 推理侧 `inference_ok` | 196/300 |
| 推理侧 fallback（仍有 pred） | 104 |

### 官方 error_type 分布

| error_type | 条数 |
| ---- | ---- |
| ok（match） | 275 |
| pred_mismatch_content | 14 |
| pred_mismatch_rowcount | 5 |
| pred_result_exceeds_gt | 5 |
| gt_timeout | 1 |

### 按测评库

| db | 条数 | Match | match_rate | Reward 合计 | R-VES（库内均值） | Coverage@2x | GMS |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| polar_equipment | 14 | 14 | **100%** | 10 | 0.8036 | 50.00%（7/14） | 2.1479 |
| robot_fault_prediction | 22 | 22 | **100%** | 16 | 0.7386 | 36.36%（8/22） | 1.8661 |
| solar_panel | 26 | 25 | **96.2%** | 19 | 0.7692 | 46.15%（12/26） | 2.2194 |
| tpch | 238 | 214 | **89.9%** | 160 | 0.7637 | 58.82%（140/238） | 4.9533 |
| **Total** | **300** | **275** | **91.7%** | **205** | **0.7642** | **55.67%（167/300）** | **4.0814** |

说明（与 [BIRD Effi-SQL 榜](https://bird-critic.github.io/) 一致）：

- **R-VES**：\(m_i\cdot g(\tau_i)\) 全量平均；\(g\) 见 Diff-SQL `_aggregate_metrics.r_ves_score`（\(\tau\ge2\to1\)，\([1.2,2)\to0.75\)，\([1,1.2)\to0.5\)，\([0.5,1)\to0.25\)，否则 0）；非 match 计 0。报告小数 0.7642 ≡ 榜单百分数 76.42%。
- **Coverage@2x**：全量中「match 且 \(\tau\ge2\)」占比。
- **GMS**：仅在 match 成功集 \(C\) 上对 \(\tau\) 取几何平均；\(\tau\le0\) 不计。
- Reward 与 R-VES 分段阈值不完全相同。

---

## 2. 产物

| 文件 | 说明 |
| ---- | ---- |
| `outputs/effi300_pred.jsonl` | **正式 pred**（300 条，均含 `prediction` / `pred_sqls`） |
| `outputs/effi300_pred.log` / `effi300_console.log` | 全量批跑日志 |
| `outputs/effi300_rerun.log` | `#24/#175/#209` 超时补跑日志 |
| `Diff-SQL/data/benchmark/effi-sql/astratune_effi300_pred.jsonl` | 送入 Diff-SQL 的评测输入副本 |
| `Diff-SQL/outputs/postgres/astratune_effi300_pred.jsonl` | 官方 end2end 逐题结果 |
| `outputs/effi300_eval_console.log` | 评测控制台（Processed/Match/Reward） |
| `outputs/effi300_aggregate.log` | `_aggregate_metrics` 汇总 |
| `outputs/effi300_eval_summary.json` | 本报告用的结构化摘要 |

---

## 3. 未 Match 清单（25）

| id | db | error_type |
| -- | -- | ---- |
| 256 | solar_panel | pred_mismatch_content |
| 6 | tpch | pred_mismatch_content |
| 7 | tpch | pred_mismatch_content |
| 19 | tpch | pred_mismatch_content |
| 28 | tpch | pred_mismatch_rowcount |
| 33 | tpch | pred_mismatch_content |
| 34 | tpch | pred_mismatch_content |
| 38 | tpch | pred_result_exceeds_gt |
| 51 | tpch | pred_result_exceeds_gt |
| 72 | tpch | pred_mismatch_rowcount |
| 77 | tpch | pred_mismatch_rowcount |
| 96 | tpch | pred_mismatch_rowcount |
| 112 | tpch | pred_result_exceeds_gt |
| 123 | tpch | pred_mismatch_content |
| 135 | tpch | pred_result_exceeds_gt |
| 146 | tpch | pred_mismatch_content |
| 150 | tpch | pred_mismatch_content |
| 153 | tpch | gt_timeout |
| 159 | tpch | pred_mismatch_content |
| 175 | tpch | pred_mismatch_content |
| 179 | tpch | pred_mismatch_rowcount |
| 181 | tpch | pred_mismatch_content |
| 203 | tpch | pred_result_exceeds_gt |
| 220 | tpch | pred_mismatch_content |
| 223 | tpch | pred_mismatch_content |

---

## 4. 推理 / 工程侧备注

| 项 | 现象 |
| ---- | ---- |
| 首轮空 pred | `#24/#175/#209`（tpch）子进程墙钟 2400s 超时；加长至 4800s 补跑后均产出 pred |
| Soft EX / 内存 | `config/effi_db.json`：`max_fetch_rows` + 截断后 `ROLLBACK`；批跑默认 `--isolate subprocess`、同库串行 |
| 评测入口 | 本机 `bash` 为 WSL 启动器时 `run_effi_eval.py` 易挂；本次用 `docker exec so_eval_env` 直跑 |
| 小规模对照 | 16 题见 [`docs/effi16评测报告.md`](effi16评测报告.md)（match 93.8% / R-VES 0.7500） |

---

## 5. 简要结论

1. **正式全量 match 91.7%（275/300），R-VES 0.7642（76.42%），Coverage@2x 55.67%，GMS 4.0814**；Reward 205。
2. **polar / robot 两库 100% match**；solar 96.2%；主失分在 **tpch（214/238，89.9%）**，以结果内容/行数不匹配与 `pred_result_exceeds_gt` 为主。tpch 虽 match 略低，但 Coverage@2x / GMS 最高（大加速题更集中）。
3. 推理侧 Verify 通过率 65.3%（196/300），但 fallback 仍大多能在官方 end2end 上 match——评测交付靠 `prediction` 完整性，不要求 `inference_ok`。
4. **下一步**：针对 25 道未 match（尤其 tpch 语义漂移 / 超额结果）做 failure dig；Open-MySQL 全量仍延后。
