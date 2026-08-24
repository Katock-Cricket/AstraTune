# AstraTune 准确率优化规格（Flash ≥50%）

> **状态**：**Flash 200 全量已达标**（**51.00%（102/200）**）；val28 门禁 **71.43%（20/28）** 已过  
> **日期**：2026-08-24（Flash 200 全量官方评测）  
> **关联**：`docs/flash200评测报告.md`；`docs/flash_val28评测报告.md`；`docs/flash_hard40评测报告.md`；`docs/架构说明书.md`；`AGENTS.md`

---

## 0. 目标

| 项 | 内容 |
|----|------|
| GOAL | Flash 200 官方 Overall Accuracy **≥ 50%** — **已达标 51.00%** |
| 子集门禁 | **val28 ≥ 60%（≥17/28）** — **已过 71.43%** |
| 节奏 | hard40 → val28 → 全量 Flash — **主链路完成** |
| 禁区 | 不向 Agent 喂 `sol_sql` / 官方 `test_cases`；不恢复 RAG/Reflect/微调/达梦 |

---

## 1. 验证集

| 集合 | 文件 | 说明 |
|------|------|------|
| Flash 200 | `outputs/flash200_*`；`data/flash200_pass/fail_ids.txt` | **51.00%（102/200）** |
| hard40 | `data/flash_hard40_*` | **60.00%（24/40）**；报告 `docs/flash_hard40评测报告.md` |
| val28 | `data/flash_val28_*` | **71.43%（20/28）**；报告 `docs/flash_val28评测报告.md` |

---

## 2. 已落地工程改动（对照现行代码）

1. **沙箱 PG**：序列同步、列名 quote（含 `%`）、统计注入、PostGIS fail-open、`lock_timeout`
2. **抽表门禁**：`resolve_and_validate_tables` + `original_schema` 回退
3. **Soft EX**：仅 Efficiency 硬要求与 `issue_sql` 集合等价
4. **Verify Agent + fidelity**：`verify_mode=agent+rules`；采样下 **QEP 不硬否决**
5. **智能 reset**、模板缓存、`batch_preflight`
6. **批跑鲁棒性**：`--isolate subprocess`、有界在飞、`instance_timeout_s` 强杀、有 pred 不清空、slim/去重

细节见 `docs/架构说明书.md`。

---

## 3. 验收

| 项 | 状态 |
|----|------|
| hard40 / val28 可 `--ids` 复跑 | 已有报告 |
| 全量 Flash Overall **≥ 50%** | **51.00% — 已过** |
| 评测报告 | `docs/flash200评测报告.md` |

---

## 4. 命令备忘（Flash 200）

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

# 省略 --limit = 全量
python scripts/run_official_eval.py \
  --dialect postgresql --jsonl outputs/flash200_eval_ready.jsonl --num_threads 2
```

`--limit` 约定见 `docs/评测对接.md` §2.1。

---

## 5. 达标后剩余缺口（非阻塞）

| 类 | 方向 | 代表 |
|----|------|------|
| Query / Pers | 断言失败聚类（机制层） | 见 `data/flash200_fail_ids.txt` |
| val28 漂移 | 全量相对子集退化 | 5, 55, 68, 129 |
| Efficiency | 全量超时 | 62 |
| 延后 | PG 530 / Open-MySQL | — |
