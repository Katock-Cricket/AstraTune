# Flash hard40 官方评测报告

> **评测时间**：2026-08-23 12:46（UTC+8）  
> **环境**：Docker 栈正常；**ChatAnywhere**（`api.chatanywhere.tech`）**多 key**（`OPENAI_API_KEYS` 4 路 + `llm_key_mode=auto`）；沙箱 `--workers` 4  
> **链路**：`diag+opt` 全量 40 条 → `merge_pred_with_sol` → 官方 `wrapper_evaluation_postgresql.py`  
> **评测范围**：JSONL **全量 40 条**（`Official eval scope: 全量 (40)`）

---

## 1. 总览

| 指标 | 数值 |
|------|------|
| 实例数 | 40 |
| **Overall Accuracy** | **60.00%（24/40）** |
| Execution Error | 0 |
| Timeout | 0 |
| Assertion Error | 16 |
| 总失败 | 16 |
| inference_ok（Verify 通过） | **36/40** |
| fallback | 3 |
| hard_fail（空 pred） | 4 |
| 平均耗时 | 314.4s / 题（p50 272.7s） |

### 与上一轮对比

| 时点 | 准确率 | 通过数 | 备注 |
|------|--------|--------|------|
| P1 / 首轮 hard40 | 42.50% | 17/40 | spec_acc 基线 |
| **本轮（ChatAnywhere 多 key）** | **60.00%** | **24/40** | **hard40 ≥40% 门禁已过** |

### 子集门禁（spec_acc）

| 门禁 | 要求 | 本轮 | 差距 |
|------|------|------|------|
| hard40 | ≥40%（≥16/40） | **60.00%（24/40）** | **已达标（+8 题缓冲）** |

---

## 2. 按 Category（官方 Test Case 行）

| Category | 题数 | 准确率 |
|----------|------|--------|
| Query | 11 | **36.36%** |
| Management | 11 | **72.73%** |
| Personalization | 12 | **66.67%** |
| Efficiency | 6 | **66.67%** |
| **Total** | 40 | **60.00%** |

---

## 3. 逐题结果

| instance_id | 结果 | inference | 失败阶段 / 说明 |
|-------------|------|-----------|-----------------|
| 0 | ✅ | ok | |
| 1 | ✅ | ok | |
| 4 | ✅ | ok | ON CONFLICT 三列唯一索引 |
| 5 | ✅ | ok | UPDATE RETURNING JOIN |
| 9 | ❌ | ok | Assertion（两表 COUNT 行语义） |
| 12 | ❌ | ok | Assertion（GROUP BY 全列 / DISTINCT ON） |
| 13 | ✅ | ok | |
| 14 | ✅ | ok | |
| 15 | ❌ | ok | Assertion（UNION 去重） |
| 17 | ✅ | ok | |
| 19 | ✅ | ok | |
| 20 | ✅ | ok | ON CONFLICT RETURNING id |
| 21 | ✅ | ok | UPSERT xmax 判别 |
| 25 | ❌ | ok | Assertion（FK 引用元数据查询） |
| 26 | ❌ | ok | Assertion（IGNORE NULLS 窗口替代） |
| 28 | ❌ | ok | Assertion（跨年聚合 UNION） |
| 29 | ✅ | ok | |
| 30 | ❌ | ok | Assertion（jsonb_agg 合并，3 test） |
| 31 | ✅ | ok | |
| 34 | ❌ | ok | Assertion（DISTINCT ON 最新行） |
| 35 | ❌ | ok | Assertion（ALTER TYPE + 脏数据） |
| 37 | ✅ | ok | Efficiency 索引 |
| 41 | ✅ | ok | |
| 64 | ✅ | ok | |
| 66 | ❌ | ok | Assertion（窗口累计分段） |
| 77 | ❌ | ok | Assertion（ROUND 到 10 位） |
| 91 | ❌ | **空 pred** | 推理异常 `tuple index out of range` |
| 94 | ✅ | ok | Efficiency UNION 合并 |
| 101 | ❌ | **fallback** | Verify 否决 ffill 聚合；官方 Assertion |
| 105 | ✅ | ok | |
| 108 | ✅ | ok | |
| 111 | ✅ | ok | |
| 117 | ❌ | **fallback** | Verify QEP 未优于 issue；官方 Assertion |
| 120 | ✅ | ok | |
| 125 | ✅ | ok | |
| 150 | ✅ | ok | streak 时区 |
| 153 | ❌ | ok | Assertion（多角色 string_agg） |
| 158 | ✅ | ok | |
| 168 | ✅ | ok | |
| 189 | ❌ | **fallback** | PostGIS 扩展沙箱缺失；官方 Assertion |

---

## 4. 推理批次说明

| 批次 | 条数 | 说明 |
|------|------|------|
| 主跑 | 32/40 | 11:37–12:20；进程在 id=101 附近中断，已落盘 32 条 |
| 补跑 | 8/40 | 12:23–12:44；`--ids 101,117,125,150,153,158,168,189` |
| 合并 | 40/40 | `outputs/flash_hard40_pred.jsonl` 去重后 40 条 |

**产物路径**

| 文件 | 说明 |
|------|------|
| `outputs/flash_hard40_pred.jsonl` | 推理 pred |
| `outputs/flash_hard40_eval_ready.jsonl` | merge sol 后评测输入 |
| `outputs/flash_hard40_eval_ready_report.txt` | 官方汇总 |
| `outputs/flash_hard40_eval_ready_output_with_status.jsonl` | 逐题 status |
| `data/flash_hard40_pass_ids.txt` / `fail_ids.txt` | 通过/失败锚点 |

---

## 5. 主要缺口（冲 Flash 200 ≥50%）

1. **Query（4/11）**：元数据/FK 查询、IGNORE NULLS、跨年聚合、条件计数仍弱。
2. **推理 hard_fail（4 题）**：91 运行时异常；101/117 Verify 过严或 QEP 门禁；189 PostGIS 沙箱与扩展。
3. **Pers 断言（4 题未过）**：12/15/77/66 等与 val28 重叠模式，可复用 Verify 启发式。

---

## 6. 复现命令

```bash
cd scripts
python run_bird_critic.py \
  --input ../data/flash_hard40.jsonl \
  --dialect postgresql \
  --output ../outputs/flash_hard40_pred.jsonl \
  --resume --workers 4 --mode diag+opt --force-batch

python merge_pred_with_sol.py \
  --pred ../outputs/flash_hard40_pred.jsonl \
  --sol ../benchmark/evaluation/flash_sol/bird-critic-1.0-flash_w_sol.jsonl \
  --output ../outputs/flash_hard40_eval_ready.jsonl --require-pred

python run_official_eval.py \
  --dialect postgresql \
  --jsonl ../outputs/flash_hard40_eval_ready.jsonl \
  --num_threads 2
```

`--limit` 约定见 `docs/评测对接.md` §2.1（全量请省略 `--limit`）。
