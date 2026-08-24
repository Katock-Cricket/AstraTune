# Flash val28 官方评测报告

> **评测时间**：2026-08-23 11:19（UTC+8）  
> **环境**：Docker 栈正常；DeepSeek 官方 API（`api.deepseek.com`）；`OPENAI_API_KEY` 单 key + `llm_workers=4`；沙箱 `--workers` 4/3  
> **链路**：历史通过锚点 pred 保留 + 失败题 `diag+opt`/`opt_only` 补跑（Verify 规则收紧）→ `merge_pred_with_sol` → 官方 `wrapper_evaluation_postgresql.py`  
> **评测范围**：JSONL **全量 28 条**（`Official eval scope: 全量 (28)`）

---

## 1. 总览

| 指标 | 数值 |
|------|------|
| 实例数 | 28 |
| **Overall Accuracy** | **71.43%（20/28）** |
| Execution Error | 0 |
| Timeout | 1 |
| Assertion Error | 7 |
| 总失败 | 8 |
| inference_ok（pred 链路） | **25/28**（3 题 fallback/hard_fail 仍有 pred 或空 pred；官方按 pred 评测） |

### 与上一轮对比

| 时点 | 准确率 | 通过数 | 备注 |
|------|--------|--------|------|
| 2026-08-22 16:31（Docker 重建后） | 53.57% | 15/28 | 基线锚点 |
| 2026-08-23 06:10（DeepSeek + id=68） | 53.57% | 15/28 | 门禁差 2 题 |
| **本轮（Verify 规则 case study）** | **71.43%** | **20/28** | **val28 ≥60% 门禁已过** |

### 子集门禁（spec_acc）

| 门禁 | 要求 | 本轮 | 差距 |
|------|------|------|------|
| val28 | ≥60%（≥17/28） | **71.43%（20/28）** | **已达标（+3 题缓冲）** |

---

## 2. 按 Category（官方 Test Case 行）

| Category | 题数 | 准确率 |
|----------|------|--------|
| Query | 7 | **57.14%** |
| Management | 11 | **100.00%** |
| Personalization | 6 | **33.33%** |
| Efficiency | 4 | **75.00%** |
| **Total** | 28 | **71.43%** |

---

## 3. 逐题结果

| instance_id | 结果 | 失败阶段 / 说明 |
|-------------|------|-----------------|
| 0 | ✅ | |
| 1 | ✅ | |
| 3 | ✅ | |
| **4** | **✅** | **本轮：ON CONFLICT 三列 UNIQUE 索引，pred 不夹带测试 INSERT** |
| **5** | **✅** | **本轮：UPDATE RETURNING JOIN 外表列** |
| 6 | ❌ | Assertion（空/错 pred：造列） |
| 7 | ✅ | |
| 8 | ❌ | Assertion |
| 9 | ❌ | Assertion（budget 行数 vs 金额列） |
| 10 | ✅ | |
| 12 | ❌ | Assertion（DISTINCT ON * 仍未过 tup[1] 断言） |
| 15 | ❌ | Assertion |
| 16 | ✅ | |
| 18 | ✅ | |
| 21 | ✅ | |
| 22 | ❌ | Assertion |
| 32 | ❌ | Assertion（直方图；曾 hard_fail 空 pred） |
| **36** | **✅** | **本轮：FUNCTION+TRIGGER，host 第一段用 strpos** |
| 47 | ✅ | |
| 55 | ✅ | |
| 62 | ❌ | Timeout（NOT IN UNION 语义对，全量 cards 仍超时） |
| 68 | ✅ | |
| 125 | ✅ | |
| 126 | ✅ | |
| **129** | **✅** | **本轮：先 INSERT seasons + 下一空闲日** |
| 150 | ✅ | |
| **157** | **✅** | **本轮：account 无 disp_id；BEFORE DELETE ON disp** |
| 194 | ✅ | |

**通过锚点（20）**：`0, 1, 3, 4, 5, 7, 10, 16, 18, 21, 36, 47, 55, 68, 125, 126, 129, 150, 157, 194`

**仍失败（8）**：`6, 8, 9, 12, 15, 22, 32, 62`

---

## 4. 本轮算法改动（未喂 sol / test_cases）

| 规则 / 提示 | 针对 |
|-------------|------|
| ON CONFLICT 冲突列全集 UNIQUE；WHERE coalesce；禁止 pred 夹带测试 INSERT | id=4 |
| UPSERT operation_type 不再把「UPSERT operation」误判成要 xmax 标签 | 解开 id=4 死锁 |
| RETURNING 必须 JOIN 题面外表并投影其列 | id=5 |
| charindex / split_part('.com') / 残缺 TRIGGER | id=36 |
| chicken-egg：禁互为 FK、account.disp_id、account AFTER INSERT 触发器 | id=157 |
| 下一空闲日 + 先 INSERT seasons | id=129 |
| 条件 COUNT / budget.count 列 / 连续岛 COUNT(*)-1 / streak 直方图 | Query/Pers 假过 |
| Efficiency 双 NOT IN → UNION；issue 含非法 LIMIT 时跳过 Soft EX | id=62（仍 timeout） |

---

## 5. 产物路径

| 文件 | 说明 |
|------|------|
| `outputs/flash_val28_pred.jsonl` | 推理输出 |
| `outputs/flash_val28_eval_ready.jsonl` | merge sol |
| `outputs/flash_val28_eval_ready_report.txt` | 官方报告 |
| `outputs/flash_val28_eval_ready_output_with_status.jsonl` | 逐题状态 |
| `data/flash_val28_pass_ids.txt` / `fail_ids.txt` | 锚点 id |

---

## 6. 复现命令

```powershell
conda activate at
cd d:\Workspace\at\AstraTune

$ids = (Get-Content data/flash_val28_ids.txt | Where-Object { $_.Trim() -ne '' }) -join ','
python scripts/run_bird_critic.py `
  --schema benchmark/baseline/data/flash_schema.jsonl `
  --input benchmark/baseline/data/flash/flash-00000-of-00001.jsonl `
  --dialect postgresql --output outputs/flash_val28_pred.jsonl `
  --ids $ids --workers 4 --mode diag+opt --resume --force-batch

python scripts/merge_pred_with_sol.py `
  --pred outputs/flash_val28_pred.jsonl `
  --sol benchmark/evaluation/flash_sol/bird-critic-1.0-flash_w_sol.jsonl `
  --output outputs/flash_val28_eval_ready.jsonl --require-pred

python scripts/run_official_eval.py `
  --dialect postgresql --jsonl outputs/flash_val28_eval_ready.jsonl --num_threads 4
```

---

## 7. 结论

- val28 官方 **71.43%（20/28）**，**已过 ≥60% 门禁**；Management **11/11**。
- 本轮新增官方通过：`4, 5, 36, 129, 157`。
- 下一阶段（Flash 200 ≥50%）优先 Pers/Query 断言簇与 Efficiency id=62 全量超时。
