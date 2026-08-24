# Flash 200 官方评测报告

> **评测时间**：2026-08-24 11:30（UTC+8）  
> **环境**：Docker 栈正常；ChatAnywhere API（`config/agents.json`）；`llm_key_mode=auto` + `llm_workers=4`；批跑 `--workers 4 --isolate subprocess`  
> **链路**：`run_bird_critic.py`（diag+opt）→ `merge_pred_with_sol` → 官方 `wrapper_evaluation_postgresql.py`  
> **评测范围**：JSONL **全量 200 条**（`Official eval scope: 全量 (200)`）

---

## 1. 总览


| 指标                      | 数值                                   |
| ----------------------- | ------------------------------------ |
| 实例数                     | 200                                  |
| **Overall Accuracy**    | **51.00%（102/200）**                  |
| Execution Error         | 0                                    |
| Timeout                 | 1                                    |
| Assertion Error         | 97                                   |
| 总失败                     | 98                                   |
| pred 链路（200 条均有 pred）   | **200/200**（`hard_fail=0`，`empty=0`） |
| Verify 通过（inference_ok） | **185/200**                          |
| fallback 交付             | **15/200**（其中官方仍过 **5** 题）           |




### 与 spec_acc 门禁


| 门禁        | 要求                 | 本轮                       | 结论              |
| --------- | ------------------ | ------------------------ | --------------- |
| Flash 200 | **≥50%（≥100/200）** | **51.00%（102/200）**      | **已达标（+2 题缓冲）** |
| val28（历史） | ≥60%（≥17/28）       | 71.43%（20/28，见 val28 报告） | 已过              |




### 与 P1 基线对比


| 时点                   | 准确率        | 通过数         | 备注                 |
| -------------------- | ---------- | ----------- | ------------------ |
| P1 工程验收基线            | ~10%       | ~20/200     | 全链路打通              |
| val28 子集（2026-08-23） | 71.43%     | 20/28       | 机制迭代锚点             |
| **本轮 Flash 200 全量**  | **51.00%** | **102/200** | **spec_acc 主门禁达标** |


---



## 2. 按 Category（官方 Test Case 行）


| Category        | 题数  | 准确率        |
| --------------- | --- | ---------- |
| Query           | 64  | **39.06%** |
| Management      | 50  | **68.00%** |
| Personalization | 64  | **48.44%** |
| Efficiency      | 22  | **54.55%** |
| **Total**       | 200 | **51.00%** |


**观察**：Management 最强；Query 仍是主要失分来源（64 题中仅 25 题全过 test case）。

---



## 3. 推理链路 vs 官方结果


| 维度                 | 数量  | 说明                     |
| ------------------ | --- | ---------------------- |
| Verify 通过 + 官方通过   | 97  | 主路径                    |
| Verify 通过 + 官方失败   | 88  | 沙箱/Soft EX 与官方断言仍有 gap |
| fallback + 官方通过    | 5   | id=32,72,81,134,174    |
| fallback + 官方失败    | 10  | 含 timeout/强杀残留 pred    |
| hard_fail / 空 pred | 0   | 本轮链路交付完整               |


**fallback 15 题**：`6, 25, 32, 68, 72, 80, 81, 91, 129, 134, 135, 140, 174, 189, 191`

---



## 4. val28 锚点在全量中的漂移


| 类型            | instance_id                                     | 说明           |
| ------------- | ----------------------------------------------- | ------------ |
| val28 通过且全量仍过 | 0,1,3,4,7,10,16,18,21,36,47,125,126,150,157,194 | **16/20** 稳定 |
| val28 通过、全量退化 | **5,55,68,129**                                 | 需 case study |
| val28 失败、全量修复 | **9,32**                                        | 子集门禁外泛化      |


---



## 5. 失败形态（Top）


| 失败类型            | 数量  | 代表 id                         |
| --------------- | --- | ----------------------------- |
| Assertion Error | 97  | Query/Pers 语义、行数、列值           |
| Timeout         | 1   | **62**（Efficiency，与 val28 一致） |
| Execution Error | 0   | —                             |


部分多 test case 题仅部分通过（计为失败）：如 id=41(1/3)、107(0/3)、112(1/2)、147(1/3) 等。

**后续算法方向**（少 prompt / 单题规则注入）：见 [`docs/flash200优化方案.md`](flash200优化方案.md)。

---



## 6. 产物路径


| 文件                                                     | 说明                       |
| ------------------------------------------------------ | ------------------------ |
| `outputs/flash200_pred.jsonl`                          | 推理输出（200 条，~1.0 MB slim） |
| `outputs/flash200_eval_ready.jsonl`                    | merge sol                |
| `outputs/flash200_eval_ready_report.txt`               | 官方报告                     |
| `outputs/flash200_eval_ready_output_with_status.jsonl` | 逐题状态                     |
| `data/flash200_pass_ids.txt` / `fail_ids.txt`          | 通过/失败锚点                  |


---



## 7. 复现命令

```powershell
conda activate at
cd d:\Workspace\at\AstraTune

python scripts\_flash200_status.py

python scripts\merge_pred_with_sol.py `
  --pred outputs/flash200_pred.jsonl `
  --sol benchmark/evaluation/flash_sol/bird-critic-1.0-flash_w_sol.jsonl `
  --output outputs/flash200_eval_ready.jsonl `
  --require-pred

python scripts\run_official_eval.py `
  --dialect postgresql `
  --jsonl outputs/flash200_eval_ready.jsonl `
  --num_threads 2
```

